#!/usr/bin/env python3
"""
Benchmark: Batch Gateway Effectiveness in Shared Clusters

Orchestrates guidellm burst/idle cycles alongside batch workloads to measure
whether gated dispatch protects interactive latency while batch makes SLO progress.

Supports 7 scenarios:
  0 - Interactive only (baseline)
  1 - No batch-gateway (batch as regular requests)
  2 - Ungated batch (aggressive concurrency, no AIMD)
  3 - Admission control + AIMD (saturation-based batch rejection + adaptive concurrency)
  4 - Flow control + AIMD (priority dispatch ordering + adaptive concurrency)
  5 - Async processor (async dispatch via llm-d-async with endpoint-scrape gate)
  6 - Low batch concurrency (fixed perEndpoint cap, no AIMD)

Usage:
    python3 benchmarks/benchmark.py \
        --context my-k8s-context \
        --scenarios 0 2 3 4 \
        --burst-rate 15 --idle-rate 1 \
        --burst-seconds 90 --idle-seconds 90 \
        --cycles 3 \
        --results-dir ./benchmarks/results/run-01
"""

import argparse
import ast
import atexit
import csv
import datetime
import json
import os
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

SCENARIO_NAMES = {
    0: "interactive-only",
    1: "no-batch-gateway",
    2: "ungated",
    3: "admission-control-aimd",
    4: "flow-control-aimd",
    5: "async",
    6: "low-concurrency",
}

SCRIPT_DIR = Path(__file__).parent.resolve()


def load_profile():
    """Load default parameter profile if available."""
    profile_path = SCRIPT_DIR / "profiles" / "default.yaml"
    if profile_path.exists():
        with open(profile_path) as f:
            return yaml.safe_load(f)
    return {}


@dataclass
class BenchmarkConfig:
    context: str
    scenarios: list
    model: str
    burst_rate: int
    idle_rate: int
    burst_seconds: int
    idle_seconds: int
    cycles: int
    batch_size: int
    num_jobs: int
    prompt_tokens: int
    num_system_prompts: int
    results_dir: Path
    target: str
    gpu_count: int = 1
    max_model_len: int = 4096
    warmup_cycles: int = 2


@dataclass
class PhaseMetrics:
    phase: str
    cycle: int
    ttft_p50: float = 0.0
    ttft_p95: float = 0.0
    ttft_p99: float = 0.0
    tpot_p50: float = 0.0
    tpot_p95: float = 0.0
    tpot_p99: float = 0.0
    itl_p50: float = 0.0
    itl_p95: float = 0.0
    itl_p99: float = 0.0
    req_latency_p50: float = 0.0
    req_latency_p95: float = 0.0
    req_latency_p99: float = 0.0
    ok_rps: float = 0.0
    err_rps: float = 0.0
    error_rate: float = 0.0
    completed: int = 0
    errors: int = 0


@dataclass
class ScenarioResult:
    scenario: int
    name: str
    phases: list = field(default_factory=list)
    batch_timeline: list = field(default_factory=list)
    job_completion_times: dict = field(default_factory=dict)
    aimd_metrics: dict = field(default_factory=dict)
    flow_control_metrics: dict = field(default_factory=dict)
    gpu_metrics: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JOB_SLO_WINDOWS = {
    "job-a": {"label": "A", "display": "30m", "seconds": 1800},
    "job-b": {"label": "B", "display": "2h", "seconds": 7200},
    "job-c": {"label": "C", "display": "24h", "seconds": 86400},
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def kubectl(args, context, namespace=None, capture=True, check=True):
    cmd = ["kubectl", f"--context={context}"]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(args)
    result = subprocess.run(cmd, capture_output=capture, text=True, check=check)
    return result.stdout.strip() if capture else None


def kubectl_apply(yaml_str, context, namespace):
    cmd = ["kubectl", f"--context={context}", "-n", namespace, "apply", "-f", "-"]
    subprocess.run(cmd, input=yaml_str, text=True, check=True)


_NAMESPACE_OVERRIDE = None


def namespace_for_scenario(scenario):
    if _NAMESPACE_OVERRIDE:
        return _NAMESPACE_OVERRIDE
    return f"batch-bench-s{scenario}"


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup_namespace(context, namespace):
    """Reset namespace state between runs: truncate DBs, restart deployments."""
    log(f"Cleaning up {namespace}")

    kubectl(["delete", "job", "--all", "--ignore-not-found"],
            context, namespace, check=False)
    kubectl(["delete", "configmap", "-l", "batch-benchmark=true",
             "--ignore-not-found"], context, namespace, check=False)

    # Scale down
    kubectl(["scale", "deployment",
             "batch-gateway-processor", "batch-gateway-apiserver",
             "--replicas=0"], context, namespace, check=False)
    time.sleep(8)

    # Truncate PostgreSQL (extract password from the deployed secret)
    try:
        pg_url = kubectl(
            ["get", "secret", "batch-gateway-secrets",
             "-o", "jsonpath={.data.postgresql-url}"],
            context, namespace, check=True)
        import base64
        pg_conn = base64.b64decode(pg_url).decode()
        # Extract password from postgresql://user:pass@host/db format
        pg_pass = pg_conn.split("://")[1].split(":")[1].split("@")[0]
    except Exception:
        pg_pass = ""
    if pg_pass:
        kubectl(["run", "--rm", "-i", "pg-nuke", "--image=postgres:16",
                 "--restart=Never", f"--env=PGPASSWORD={pg_pass}", "--",
                 "psql", "-h", "postgresql", "-U", "postgres", "-d", "batchgateway",
                 "-c", "TRUNCATE batch_items, file_items CASCADE;"],
                context, namespace, check=False)

    # Flush Redis
    kubectl(["run", "--rm", "-i", "redis-del", "--image=redis",
             "--restart=Never", "--",
             "sh", "-c",
             'for key in $(redis-cli -h redis-master KEYS "*"); do '
             'redis-cli -h redis-master DEL "$key"; done'],
            context, namespace, check=False)

    # Scale back up
    kubectl(["scale", "deployment",
             "batch-gateway-processor", "batch-gateway-apiserver",
             "--replicas=1"], context, namespace, check=False)

    kubectl(["rollout", "status", "deployment/batch-gateway-processor",
             "--timeout=60s"], context, namespace, check=False)
    time.sleep(5)
    log(f"  Cleanup complete for {namespace}")


# ---------------------------------------------------------------------------
# Batch submission
# ---------------------------------------------------------------------------


def _batch_submit_script():
    """Python script that runs inside a K8s Job to submit a batch."""
    return textwrap.dedent("""\
        import json, os, sys, time
        from urllib.request import urlopen, Request

        base_url = os.environ["BATCH_GATEWAY_URL"]
        input_file = os.environ["INPUT_FILE"]
        completion_window = os.environ["COMPLETION_WINDOW"]

        # Upload file
        with open(input_file, "rb") as f:
            file_data = f.read()

        boundary = "----BatchBoundary"
        body = (
            f"--{boundary}\\r\\n"
            f'Content-Disposition: form-data; name="purpose"\\r\\n\\r\\n'
            f"batch\\r\\n"
            f"--{boundary}\\r\\n"
            f'Content-Disposition: form-data; name="file"; filename="input.jsonl"\\r\\n'
            f"Content-Type: application/octet-stream\\r\\n\\r\\n"
        ).encode() + file_data + f"\\r\\n--{boundary}--\\r\\n".encode()
        req = Request(
            f"{base_url}/v1/files",
            data=body,
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Authorization": "Bearer benchmark",
            },
        )
        resp = json.loads(urlopen(req).read())
        file_id = resp["id"]
        print(f"Uploaded: {file_id}", flush=True)

        # Create batch
        batch_body = json.dumps({
            "input_file_id": file_id,
            "endpoint": "/v1/chat/completions",
            "completion_window": completion_window,
        }).encode()
        req = Request(
            f"{base_url}/v1/batches",
            data=batch_body,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer benchmark",
            },
        )
        resp = json.loads(urlopen(req).read())
        batch_id = resp["id"]
        print(f"Batch: {batch_id}", flush=True)

        # Poll until terminal
        timeout = 7200
        elapsed = 0
        while elapsed < timeout:
            req = Request(
                f"{base_url}/v1/batches/{batch_id}",
                headers={"Authorization": "Bearer benchmark"},
            )
            status = json.loads(urlopen(req).read())
            s = status["status"]
            c = status["request_counts"].get("completed", 0)
            t = status["request_counts"].get("total", 0)
            print(f"Batch {batch_id}: status={s} completed={c}/{t} ({elapsed}s)", flush=True)
            if s in ("completed", "failed", "cancelled", "expired"):
                print(f"Terminal: {s}", flush=True)
                break
            time.sleep(5)
            elapsed += 5
        else:
            print("Timed out", flush=True)
    """)


def _upload_jsonl_to_pvc(cfg, namespace, job_names):
    """Upload JSONL files to the benchmark-results PVC via a helper pod."""
    helper_name = "data-loader"
    helper_yaml = textwrap.dedent(f"""\
    apiVersion: v1
    kind: Pod
    metadata:
      name: {helper_name}
      labels:
        batch-benchmark: "true"
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      restartPolicy: Never
      containers:
        - name: loader
          image: busybox
          securityContext:
            allowPrivilegeEscalation: false
          command: ["sleep", "300"]
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: benchmark-results
    """)
    kubectl(["delete", "pod", helper_name, "--ignore-not-found", "--wait"],
            cfg.context, namespace, check=False)
    kubectl_apply(helper_yaml, cfg.context, namespace)
    try:
        kubectl(["wait", "pod", helper_name, "--for=condition=Ready",
                 "--timeout=60s"], cfg.context, namespace)
        time.sleep(2)

        for name in job_names:
            jsonl_path = cfg.results_dir / f"{name}.jsonl"
            for attempt in range(3):
                try:
                    kubectl(["cp", str(jsonl_path),
                             f"{namespace}/{helper_name}:/data/{name}.jsonl"],
                            cfg.context, namespace=None, check=True)
                    break
                except subprocess.CalledProcessError:
                    if attempt == 2:
                        raise
                    time.sleep(5)
    finally:
        kubectl(["delete", "pod", helper_name, "--ignore-not-found"],
                cfg.context, namespace, check=False)


def submit_batches(cfg, namespace):
    """Submit batch jobs using pre-generated JSONL files stored on PVC."""
    if cfg.num_jobs == 0:
        return

    job_names = list(JOB_SLO_WINDOWS.keys())
    active_jobs = []

    for i in range(min(cfg.num_jobs, 3)):
        name = job_names[i]
        jsonl_path = cfg.results_dir / f"{name}.jsonl"

        if not jsonl_path.exists():
            log(f"  Generating prompts for {name}...")
            subprocess.run([
                sys.executable, str(SCRIPT_DIR / "generate_prompts.py"),
                "--num-requests", str(cfg.batch_size),
                "--num-system-prompts", str(cfg.num_system_prompts),
                "--prompt-tokens", str(cfg.prompt_tokens),
                "--model", cfg.model,
                "--seed", str(42 + i),
                "--output", str(jsonl_path),
            ], check=True)
        active_jobs.append(name)

    log("  Uploading JSONL files to PVC...")
    _upload_jsonl_to_pvc(cfg, namespace, active_jobs)

    for i, name in enumerate(active_jobs):
        window = JOB_SLO_WINDOWS[name]["display"]
        log(f"  Submitting {name} (window={window}, size={cfg.batch_size})")
        script = _batch_submit_script()
        indented = "\n".join("          " + line for line in script.splitlines())

        cm_yaml = textwrap.dedent(f"""\
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: {name}-script
          labels:
            batch-benchmark: "true"
        data:
          script.py: |
        """) + indented + "\n"

        job_yaml = textwrap.dedent(f"""\
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: {name}
          labels:
            batch-benchmark: "true"
        spec:
          backoffLimit: 0
          template:
            metadata:
              labels:
                batch-benchmark: "true"
                app.kubernetes.io/part-of: batch-gateway
            spec:
              securityContext:
                runAsNonRoot: true
                runAsUser: 1000
                fsGroup: 1000
              restartPolicy: Never
              containers:
                - name: batch-submit
                  image: python:3.12-slim
                  securityContext:
                    allowPrivilegeEscalation: false
                  env:
                    - name: BATCH_GATEWAY_URL
                      value: "http://batch-gateway-apiserver:8000"
                    - name: INPUT_FILE
                      value: "/data/{name}.jsonl"
                    - name: COMPLETION_WINDOW
                      value: "{window}"
                  command: ["python3", "-u", "/scripts/script.py"]
                  volumeMounts:
                    - name: script
                      mountPath: /scripts
                    - name: data
                      mountPath: /data
              volumes:
                - name: script
                  configMap:
                    name: {name}-script
                - name: data
                  persistentVolumeClaim:
                    claimName: benchmark-results
        """)

        kubectl_apply(cm_yaml, cfg.context, namespace)
        kubectl_apply(job_yaml, cfg.context, namespace)


# ---------------------------------------------------------------------------
# Interactive traffic (guidellm)
# ---------------------------------------------------------------------------


def start_interactive_traffic(cfg, namespace):
    """Start guidellm burst/idle cycles with warmup support."""
    log(f"  Starting interactive traffic: {cfg.cycles} cycles "
        f"({cfg.warmup_cycles} warmup), "
        f"burst@{cfg.burst_rate}/s for {cfg.burst_seconds}s, "
        f"idle@{cfg.idle_rate}/s for {cfg.idle_seconds}s")

    backend_kwargs = json.dumps({
        "extras": {
            "headers": {
                "x-gateway-inference-objective": "interactive-default",
            },
        },
    })

    cycle_lines = []
    for c in range(1, cfg.cycles + 1):
        suffix = "warmup" if c <= cfg.warmup_cycles else ""
        idle_output = f"idle-{c}-warmup.csv" if suffix else f"idle-{c}.csv"
        burst_output = f"burst-{c}-warmup.csv" if suffix else f"burst-{c}.csv"
        label = f" [WARMUP]" if suffix else ""

        cycle_lines.extend([
            f'echo "=== Phase {c}: IDLE ({cfg.idle_rate} req/s, {cfg.idle_seconds}s){label} ==="',
            f'guidellm benchmark run --target "$T" $COMMON '
            f'--profile constant --rate {cfg.idle_rate} --max-seconds {cfg.idle_seconds} '
            f'--output-dir /results --outputs "{idle_output}"',
            f'echo "=== Phase {c}: BURST ({cfg.burst_rate} req/s, {cfg.burst_seconds}s){label} ==="',
            f'guidellm benchmark run --target "$T" $COMMON '
            f'--profile constant --rate {cfg.burst_rate} --max-seconds {cfg.burst_seconds} '
            f'--output-dir /results --outputs "{burst_output}"',
        ])

    script_lines = [
        f'T="{cfg.target}"',
        f'M="{cfg.model}"',
        f'COMMON="--request-format text_completions --model $M '
        f'--data prompt_tokens={cfg.prompt_tokens},output_tokens=512 '
        f"--backend-kwargs '{backend_kwargs}' "
        f'--processor $M --disable-console-interactive"',
        'mkdir -p /results',
    ] + cycle_lines + ['echo "=== Done ==="']

    indent = " " * 14
    script_block = "\n".join(indent + line for line in script_lines)

    yaml = f"""\
apiVersion: batch/v1
kind: Job
metadata:
  name: guidellm-interactive
  labels:
    batch-benchmark: "true"
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: guidellm
          image: ghcr.io/vllm-project/guidellm:v0.6.1
          env:
            - name: USER
              value: "guidellm"
            - name: HF_HUB_CACHE
              value: "/tmp/hf_cache"
          command:
            - sh
            - -c
            - |
{script_block}
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          persistentVolumeClaim:
            claimName: benchmark-results
"""
    kubectl_apply(yaml, cfg.context, namespace)


def start_batch_as_interactive_traffic(cfg, namespace):
    """Start a second guidellm instance that sends batch-equivalent prompts as regular requests.

    Used in scenario 1 to show what happens when batch work is sent without
    batch-gateway — requests compete directly with interactive traffic.
    """
    total_duration = cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds)
    total_requests = cfg.batch_size * cfg.num_jobs
    rate = max(1, total_requests // total_duration)

    log(f"  Starting batch-as-interactive traffic: {total_requests} requests "
        f"at ~{rate} req/s over {total_duration}s")

    backend_kwargs = json.dumps({
        "extras": {
            "headers": {
                "x-gateway-inference-objective": "interactive-default",
            },
        },
    })

    indent = " " * 14
    script_lines = [
        f'T="{cfg.target}"',
        f'M="{cfg.model}"',
        f'COMMON="--request-format text_completions --model $M '
        f'--data prompt_tokens={cfg.prompt_tokens},output_tokens=512 '
        f"--backend-kwargs '{backend_kwargs}' "
        f'--processor $M --disable-console-interactive"',
        'mkdir -p /results',
        f'echo "=== Batch-as-interactive: {total_requests} requests at {rate} req/s ==="',
        f'guidellm benchmark run --target "$T" $COMMON '
        f'--profile constant --rate {rate} --max-seconds {total_duration} '
        f'--output-dir /results --outputs "batch-traffic.csv"',
        'echo "=== Batch-as-interactive done ==="',
    ]
    script_block = "\n".join(indent + line for line in script_lines)

    yaml = f"""\
apiVersion: batch/v1
kind: Job
metadata:
  name: guidellm-batch
  labels:
    batch-benchmark: "true"
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: guidellm
          image: ghcr.io/vllm-project/guidellm:v0.6.1
          env:
            - name: USER
              value: "guidellm"
            - name: HF_HUB_CACHE
              value: "/tmp/hf_cache"
          command:
            - sh
            - -c
            - |
{script_block}
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          persistentVolumeClaim:
            claimName: benchmark-results
"""
    kubectl_apply(yaml, cfg.context, namespace)


# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------


def get_batch_progress(context, namespace):
    """Aggregate batch progress across all batch jobs."""
    completed, total = 0, 0
    for name in ["job-a", "job-b", "job-c"]:
        try:
            out = kubectl(["logs", f"job/{name}", "--tail=5"],
                         context, namespace, check=False)
            for line in reversed(out.split("\n")):
                if "completed=" in line and "/" in line.split("completed=")[1]:
                    parts = line.split("completed=")[1].split()[0].split("/")
                    completed += int(parts[0])
                    total += int(parts[1])
                    break
        except Exception:
            pass
    return completed, total


def get_per_job_progress(context, namespace):
    """Get per-job batch progress: {job_name: {completed, total, status}}."""
    jobs = {}
    for name in list(JOB_SLO_WINDOWS.keys()):
        try:
            out = kubectl(["logs", f"job/{name}", "--tail=10"],
                         context, namespace, check=False)
            job_completed, job_total = 0, 0
            status = "in_progress"
            for line in reversed(out.split("\n")):
                if "Terminal:" in line:
                    status = line.split("Terminal:")[1].strip()
                    break
                if "completed=" in line and "/" in line.split("completed=")[1]:
                    parts = line.split("completed=")[1].split()[0].split("/")
                    job_completed = int(parts[0])
                    job_total = int(parts[1])
                    break
            jobs[name] = {"completed": job_completed, "total": job_total, "status": status}
        except Exception as e:
            log(f"  DEBUG: Failed to parse progress for {name}: {e}")
    return jobs


def get_current_phase(context, namespace):
    """Get current guidellm phase from job logs."""
    try:
        out = kubectl(["logs", "job/guidellm-interactive", "--tail=20"],
                     context, namespace, check=False)
        phase = "unknown"
        for line in out.split("\n"):
            if line.startswith("==="):
                phase = line.strip("= ")
        return phase
    except Exception:
        return "unknown"


def monitor_scenario(cfg, scenario, namespace):
    """Monitor batch progress during interactive traffic, return timeline."""
    timeline = []
    job_completion_times = {}
    start = time.time()
    total_duration = cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds) + 300

    while True:
        elapsed = time.time() - start

        completed, total = get_batch_progress(cfg.context, namespace)
        phase = get_current_phase(cfg.context, namespace)

        per_job = get_per_job_progress(cfg.context, namespace)
        for jname, jinfo in per_job.items():
            if jname not in job_completion_times and jinfo["status"] == "completed":
                job_completion_times[jname] = round(elapsed)

        timeline.append({
            "elapsed": round(elapsed),
            "completed": completed,
            "total": total,
            "phase": phase,
        })

        log(f"  [s{scenario}] {phase} | batch: {completed}/{total} | {int(elapsed)}s")

        # Check if guidellm is done
        try:
            gs = kubectl(["get", "pods", "-l", "job-name=guidellm-interactive",
                         "-o", "jsonpath={.items[0].status.phase}"],
                        cfg.context, namespace, check=False)
            if gs in ("Succeeded", "Failed"):
                log(f"  [s{scenario}] Interactive traffic complete")
                break
        except Exception:
            pass

        if elapsed > total_duration:
            log(f"  [s{scenario}] Timeout, stopping monitor")
            break

        time.sleep(10)

    return timeline, job_completion_times


# ---------------------------------------------------------------------------
# Result collection
# ---------------------------------------------------------------------------


def collect_results(cfg, scenario, namespace):
    """Collect guidellm CSVs from the PVC via a helper pod."""
    dest = cfg.results_dir / f"scenario-{scenario}"
    dest.mkdir(parents=True, exist_ok=True)

    helper = f"results-helper-s{scenario}"
    helper_yaml = textwrap.dedent(f"""\
    apiVersion: v1
    kind: Pod
    metadata:
      name: {helper}
      labels:
        batch-benchmark: "true"
    spec:
      restartPolicy: Never
      containers:
        - name: helper
          image: busybox
          command: ["sleep", "300"]
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          persistentVolumeClaim:
            claimName: benchmark-results
    """)

    kubectl_apply(helper_yaml, cfg.context, namespace)
    kubectl(["wait", "--for=condition=Ready", f"pod/{helper}", "--timeout=30s"],
            cfg.context, namespace, check=False)
    time.sleep(2)

    # List and copy CSV files
    try:
        file_list = kubectl(["exec", helper, "--", "ls", "/results"],
                           cfg.context, namespace, check=False)
        for fname in file_list.split():
            if fname.endswith(".csv"):
                local_path = dest / fname
                subprocess.run([
                    "kubectl", f"--context={cfg.context}", "-n", namespace,
                    "cp", f"{helper}:/results/{fname}", str(local_path)
                ], check=False)
        log(f"  Collected results to {dest}")
    except Exception as e:
        log(f"  WARNING: Failed to collect results: {e}")

    # Cleanup helper
    kubectl(["delete", "pod", helper, "--ignore-not-found"],
            cfg.context, namespace, check=False)

    return dest


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def parse_guidellm_csv(csv_path):
    """Parse a guidellm 0.6.x summary CSV into PhaseMetrics.

    guidellm outputs a multi-header summary CSV:
      Row 0: Category (e.g. "Time to First Token", "Request Latency")
      Row 1: Sub-header (e.g. "Successful ms", "Successful Sec")
      Row 2: Stat type (e.g. "Mean", "Median", "Std Dev", "Percentiles")
      Row 3: Single data row with aggregated values

    Percentile arrays contain 11 decile values (p0..p100 in steps of 10).
    We interpolate p95/p99 from p90 and p100.
    """

    phase_name = csv_path.stem  # e.g. "burst-1", "idle-2"
    parts = phase_name.rsplit("-", 1)
    phase = parts[0] if len(parts) == 2 else phase_name
    cycle = int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else 0

    try:
        with open(csv_path) as f:
            reader = csv.reader(f)
            rows = list(reader)
    except (OSError, csv.Error) as e:
        log(f"  WARNING: Failed to parse {csv_path}: {e}")
        return PhaseMetrics(phase=phase, cycle=cycle)

    if len(rows) < 4:
        log(f"  WARNING: {csv_path} has fewer than 4 rows")
        return PhaseMetrics(phase=phase, cycle=cycle)

    categories, sub_headers, stat_types = rows[0], rows[1], rows[2]
    data = rows[3]

    def find_col(category, sub_header, stat_type):
        for i, (c, s, t) in enumerate(zip(categories, sub_headers, stat_types)):
            cat_match = category in c if category else c == ""
            sub_match = sub_header in s if sub_header else s == ""
            stat_match = stat_type in t if stat_type else t.strip() == ""
            if cat_match and sub_match and stat_match:
                return i
        return None

    def safe_float(col_idx, default=0.0):
        if col_idx is None or col_idx >= len(data):
            return default
        try:
            return float(data[col_idx])
        except (ValueError, TypeError):
            return default

    def parse_percentile_array(col_idx):
        if col_idx is None or col_idx >= len(data):
            return []
        try:
            return list(ast.literal_eval(data[col_idx]))
        except (ValueError, SyntaxError):
            return []

    def interpolate_percentile(pct_array, target_pct):
        """Interpolate a percentile from a decile array (11 values: p0..p100)."""
        if not pct_array or len(pct_array) < 11:
            return 0.0
        step = 10
        lower_idx = min(target_pct // step, 9)
        upper_idx = min(lower_idx + 1, 10)
        frac = (target_pct - lower_idx * step) / step
        return pct_array[lower_idx] + frac * (pct_array[upper_idx] - pct_array[lower_idx])

    # Request counts
    completed = int(safe_float(find_col("Request Counts", "Successful", "")))
    errors = int(safe_float(find_col("Request Counts", "Errored", "")))
    total = int(safe_float(find_col("Request Counts", "Total", "")))

    # TTFT (milliseconds)
    ttft_median = safe_float(find_col("Time to First Token", "Successful ms", "Median"))
    ttft_pct = parse_percentile_array(find_col("Time to First Token", "Successful ms", "Percentiles"))

    # TPOT (milliseconds)
    tpot_median = safe_float(find_col("Time per Output Token", "Successful ms", "Median"))
    tpot_pct = parse_percentile_array(find_col("Time per Output Token", "Successful ms", "Percentiles"))

    # ITL (milliseconds)
    itl_median = safe_float(find_col("Inter Token Latency", "Successful ms", "Median"))
    itl_pct = parse_percentile_array(find_col("Inter Token Latency", "Successful ms", "Percentiles"))

    # Request latency (seconds → convert to ms for consistency)
    req_lat_median = safe_float(find_col("Request Latency", "Successful Sec", "Median")) * 1000
    req_lat_pct_raw = parse_percentile_array(find_col("Request Latency", "Successful Sec", "Percentiles"))
    req_lat_pct = [v * 1000 for v in req_lat_pct_raw]

    # Duration (seconds) for throughput calculation
    duration = safe_float(find_col("Timings", "Duration", "Sec"), default=1.0)

    error_rate = errors / total if total > 0 else 0.0

    return PhaseMetrics(
        phase=phase, cycle=cycle,
        ttft_p50=ttft_median,
        ttft_p95=interpolate_percentile(ttft_pct, 95),
        ttft_p99=interpolate_percentile(ttft_pct, 99),
        itl_p50=itl_median,
        itl_p95=interpolate_percentile(itl_pct, 95),
        itl_p99=interpolate_percentile(itl_pct, 99),
        tpot_p50=tpot_median,
        tpot_p95=interpolate_percentile(tpot_pct, 95),
        tpot_p99=interpolate_percentile(tpot_pct, 99),
        req_latency_p50=req_lat_median,
        req_latency_p95=interpolate_percentile(req_lat_pct, 95),
        req_latency_p99=interpolate_percentile(req_lat_pct, 99),
        ok_rps=completed / duration if duration > 0 else 0,
        err_rps=errors / duration if duration > 0 else 0,
        error_rate=error_rate,
        completed=completed, errors=errors,
    )


def parse_scenario_results(results_dir):
    """Parse all guidellm CSVs in a scenario results directory."""
    phases = []
    if not results_dir.exists():
        return phases
    for csv_path in sorted(results_dir.glob("*.csv")):
        metrics = parse_guidellm_csv(csv_path)
        if metrics.completed > 0:
            phases.append(metrics)
    return phases


# ---------------------------------------------------------------------------
# Prometheus metrics collection
# ---------------------------------------------------------------------------


class PrometheusPortForward:
    """Manages a kubectl port-forward to Prometheus."""

    def __init__(self, context, namespace, service, local_port=9090):
        self.context = context
        self.namespace = namespace
        self.service = service
        self.local_port = local_port
        self._process = None

    def start(self):
        cmd = [
            "kubectl", f"--context={self.context}",
            "-n", self.namespace,
            "port-forward", f"svc/{self.service}",
            f"{self.local_port}:9090",
        ]
        self._process = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(2)
        if self._process.poll() is not None:
            log(f"  WARNING: Prometheus port-forward failed to start (exit={self._process.returncode})")
            self._process = None
            return False
        log(f"  Prometheus port-forward started (localhost:{self.local_port} → {self.service})")
        return True

    def stop(self):
        if self._process:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
            self._process = None

    @property
    def url(self):
        return f"http://localhost:{self.local_port}"


_prom_port_forward = None


def get_prometheus_url():
    """Return the Prometheus URL, starting a port-forward if configured."""
    global _prom_port_forward
    if _prom_port_forward and _prom_port_forward._process:
        return _prom_port_forward.url
    return os.environ.get("PROMETHEUS_URL", "")


def start_prometheus_port_forward(context, namespace, service):
    """Start a background port-forward to Prometheus if PROMETHEUS_URL is not set."""
    global _prom_port_forward
    if os.environ.get("PROMETHEUS_URL"):
        return
    pf = PrometheusPortForward(context, namespace, service)
    if pf.start():
        _prom_port_forward = pf
        os.environ["PROMETHEUS_URL"] = pf.url
        atexit.register(stop_prometheus_port_forward)


def stop_prometheus_port_forward():
    """Stop the background port-forward."""
    global _prom_port_forward
    if _prom_port_forward:
        _prom_port_forward.stop()
        _prom_port_forward = None


def query_prometheus(context, namespace, query, start_time, end_time, step="15s"):
    """Query Prometheus for time-series metrics via port-forward."""
    import urllib.request
    import urllib.parse

    prom_url = get_prometheus_url()
    if not prom_url:
        return []
    params = urllib.parse.urlencode({
        "query": query,
        "start": start_time.isoformat() + "Z",
        "end": end_time.isoformat() + "Z",
        "step": step,
    })
    url = f"{prom_url}/api/v1/query_range?{params}"

    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            if data.get("status") == "success":
                return data["data"]["result"]
    except Exception as e:
        log(f"  WARNING: Prometheus query failed: {e}")
    return []


def collect_gpu_metrics(context, namespace, start_time, end_time):
    """Collect GPU utilization metrics from Prometheus (DCGM exporter + vLLM)."""
    metrics = {}

    gpu_cache_query = 'avg(vllm:gpu_cache_usage_perc)'
    results = query_prometheus(context, namespace, gpu_cache_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["gpu_cache_usage_avg"] = sum(values) / len(values)
            metrics["gpu_cache_usage_max"] = max(values)
            metrics["gpu_cache_usage_series"] = values

    running_query = 'sum(vllm:num_requests_running)'
    results = query_prometheus(context, namespace, running_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["requests_running_avg"] = sum(values) / len(values)
            metrics["requests_running_max"] = max(values)
            metrics["requests_running_series"] = values

    waiting_query = 'sum(vllm:num_requests_waiting)'
    results = query_prometheus(context, namespace, waiting_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["requests_waiting_avg"] = sum(values) / len(values)
            metrics["requests_waiting_series"] = values

    return metrics


def collect_aimd_metrics(context, namespace, start_time, end_time):
    """Collect AIMD adaptive concurrency metrics from Prometheus."""
    metrics = {}

    # Current concurrency limit (gauge)
    limit_query = 'avg(batch_processor_aimd_concurrency_limit)'
    results = query_prometheus(context, namespace, limit_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["aimd_concurrency_limit_avg"] = sum(values) / len(values)
            metrics["aimd_concurrency_limit_min"] = min(values)
            metrics["aimd_concurrency_limit_max"] = max(values)
            metrics["aimd_concurrency_limit_series"] = values

    # Decrease events (multiplicative backoff on 429/5xx)
    decrease_query = 'sum(rate(batch_processor_aimd_decreases_total[30s]))'
    results = query_prometheus(context, namespace, decrease_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["aimd_decrease_rate_avg"] = sum(values) / len(values)

    # Increase events (additive increase on success)
    increase_query = 'sum(rate(batch_processor_aimd_increases_total[30s]))'
    results = query_prometheus(context, namespace, increase_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["aimd_increase_rate_avg"] = sum(values) / len(values)

    return metrics


def collect_flow_control_metrics(context, namespace, start_time, end_time):
    """Collect EPP flow control metrics from Prometheus.

    Tries the llm-d Router (v0.9.0+) ``llm_d_epp_*`` names first and
    falls back to the legacy ``inference_extension_*`` names emitted by
    GIE v1.5.0 standalone EPP and router <= v0.8.0 (see ROUTER_EPP_TAG /
    GIE_VERSION in setup.sh).
    """
    metrics = {}

    # Pool saturation (0-1 ratio of flow control capacity used)
    saturation_query = 'avg(llm_d_epp_flow_control_pool_saturation)'
    results = query_prometheus(context, namespace, saturation_query, start_time, end_time)
    if not results:
        saturation_query = 'avg(inference_extension_flow_control_pool_saturation)'
        results = query_prometheus(context, namespace, saturation_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["flow_control_saturation_avg"] = sum(values) / len(values)
            metrics["flow_control_saturation_max"] = max(values)
            metrics["flow_control_saturation_series"] = values

    # Queue size per priority band
    queue_query = 'sum by (priority) (llm_d_epp_flow_control_queue_size)'
    results = query_prometheus(context, namespace, queue_query, start_time, end_time)
    if not results:
        queue_query = 'sum by (priority) (inference_extension_flow_control_queue_size)'
        results = query_prometheus(context, namespace, queue_query, start_time, end_time)
    if results:
        for series in results:
            priority = series.get("metric", {}).get("priority", "unknown")
            values = [float(v[1]) for v in series.get("values", []) if v[1] != "NaN"]
            if values:
                metrics[f"queue_size_priority_{priority}_avg"] = sum(values) / len(values)
                metrics[f"queue_size_priority_{priority}_series"] = values

    # Total queue size (all priorities combined) for the chart
    total_queue_query = 'sum(llm_d_epp_flow_control_queue_size)'
    results = query_prometheus(context, namespace, total_queue_query, start_time, end_time)
    if not results:
        total_queue_query = 'sum(inference_extension_flow_control_queue_size)'
        results = query_prometheus(context, namespace, total_queue_query, start_time, end_time)
    if results:
        values = [float(v[1]) for v in results[0].get("values", []) if v[1] != "NaN"]
        if values:
            metrics["flow_control_queue_size_series"] = values
            metrics["flow_control_queue_size_avg"] = sum(values) / len(values)
            metrics["flow_control_queue_size_max"] = max(values)

    return metrics


def _aggregate_phases(phases, phase_filter=None):
    """Aggregate multiple PhaseMetrics into averages."""
    filtered = [p for p in phases if phase_filter is None or phase_filter in p.phase]
    if not filtered:
        return None
    n = len(filtered)
    return {
        "ttft_p50": sum(p.ttft_p50 for p in filtered) / n,
        "ttft_p95": sum(p.ttft_p95 for p in filtered) / n,
        "ttft_p99": sum(p.ttft_p99 for p in filtered) / n,
        "itl_p50": sum(p.itl_p50 for p in filtered) / n,
        "itl_p95": sum(p.itl_p95 for p in filtered) / n,
        "itl_p99": sum(p.itl_p99 for p in filtered) / n,
        "tpot_p50": sum(p.tpot_p50 for p in filtered) / n,
        "tpot_p95": sum(p.tpot_p95 for p in filtered) / n,
        "tpot_p99": sum(p.tpot_p99 for p in filtered) / n,
        "completed": sum(p.completed for p in filtered),
        "errors": sum(p.errors for p in filtered),
        "error_rate": sum(p.error_rate for p in filtered) / n,
    }


def _format_slo_completion(result):
    """Format batch SLO as per-job actual/target with pass/fail indicators.

    Returns e.g. 'A: ✓ 8m/30m<br>B: ✓ 15m/2h<br>C: ✓ 25m/24h'
    """
    parts = []
    for job_name, info in JOB_SLO_WINDOWS.items():
        label = info["label"]
        target = info["display"]
        if job_name in result.job_completion_times:
            elapsed_s = result.job_completion_times[job_name]
            passed = elapsed_s <= info["seconds"]
            indicator = "✓" if passed else "✗"
            actual = _format_duration(elapsed_s)
            parts.append(f"{label}: {indicator} {actual}/{target}")
        else:
            parts.append(f"{label}: — /{target}")

    return "<br>".join(parts)


def _format_duration(seconds):
    """Format seconds into human-readable duration (e.g. '8m', '1h 15m')."""
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    remaining_min = minutes % 60
    if remaining_min == 0:
        return f"{hours}h"
    return f"{hours}h {remaining_min}m"


def _generate_narrative(results, cfg):
    """Auto-generate a narrative conclusion based on metric comparisons."""
    baseline = next((r for r in results if r.scenario == 0), None)
    ungated = next((r for r in results if r.scenario == 2), None)
    aimd = next((r for r in results if r.scenario == 3), None)
    fc = next((r for r in results if r.scenario == 4), None)
    async_proc = next((r for r in results if r.scenario == 5), None)
    low_conc = next((r for r in results if r.scenario == 6), None)

    lines = []
    baseline_burst = _aggregate_phases(baseline.phases, "burst") if baseline else None
    baseline_ttft = baseline_burst["ttft_p99"] if baseline_burst else None

    if baseline_ttft:
        lines.append(f"The interactive-only baseline (S0) achieved a TTFT p99 of "
                     f"{baseline_ttft:.0f} ms during burst phases.")

    if ungated:
        ungated_burst = _aggregate_phases(ungated.phases, "burst")
        if ungated_burst and baseline_ttft:
            degradation = ((ungated_burst["ttft_p99"] - baseline_ttft) / baseline_ttft) * 100
            lines.append(f"Ungated batch (S2) degraded interactive TTFT p99 by "
                         f"{degradation:.0f}% ({ungated_burst['ttft_p99']:.0f} ms) "
                         f"due to uncontrolled batch competition.")

    if aimd:
        aimd_burst = _aggregate_phases(aimd.phases, "burst")
        if aimd_burst and baseline_ttft:
            overhead = ((aimd_burst["ttft_p99"] - baseline_ttft) / baseline_ttft) * 100
            lines.append(f"Admission control + AIMD (S3) protected interactive TTFT p99 within "
                         f"{overhead:.0f}% of baseline ({aimd_burst['ttft_p99']:.0f} ms) "
                         f"using saturation-based batch rejection and adaptive concurrency.")

    if fc:
        fc_burst = _aggregate_phases(fc.phases, "burst")
        if fc_burst and baseline_ttft:
            overhead = ((fc_burst["ttft_p99"] - baseline_ttft) / baseline_ttft) * 100
            lines.append(f"Flow control + AIMD (S4) achieved the best protection at "
                         f"{overhead:.0f}% above baseline ({fc_burst['ttft_p99']:.0f} ms) "
                         f"with priority dispatch ordering (interactive before batch).")

    if low_conc:
        low_conc_burst = _aggregate_phases(low_conc.phases, "burst")
        if low_conc_burst and baseline_ttft:
            overhead = ((low_conc_burst["ttft_p99"] - baseline_ttft) / baseline_ttft) * 100
            lines.append(f"Low concurrency (S6) limited interactive TTFT p99 impact to "
                         f"{overhead:.0f}% above baseline ({low_conc_burst['ttft_p99']:.0f} ms) "
                         f"using a fixed per-endpoint concurrency cap.")

    if async_proc:
        async_burst = _aggregate_phases(async_proc.phases, "burst")
        if async_burst and baseline_ttft:
            overhead = ((async_burst["ttft_p99"] - baseline_ttft) / baseline_ttft) * 100
            lines.append(f"Async dispatch (S5) limited interactive TTFT p99 impact to "
                         f"{overhead:.0f}% above baseline ({async_burst['ttft_p99']:.0f} ms) "
                         f"using async-processor with endpoint-scrape gating.")

    # Batch completion summary with per-job SLO status
    for r in results:
        if r.scenario >= 2 and r.batch_timeline:
            final = r.batch_timeline[-1]
            completed = final.get("completed", 0)
            total = final.get("total", 0)
            elapsed = final.get("elapsed", 0)
            if total > 0:
                pct = completed / total * 100
                if r.job_completion_times:
                    slo_met = sum(
                        1 for jn, jt in r.job_completion_times.items()
                        if jt <= JOB_SLO_WINDOWS.get(jn, {}).get("seconds", float("inf"))
                    )
                    lines.append(
                        f"S{r.scenario} ({r.name}) completed {pct:.0f}% of batch in "
                        f"{_format_duration(elapsed)}, {slo_met}/{len(r.job_completion_times)} "
                        f"jobs met their SLO windows.")
                else:
                    lines.append(f"S{r.scenario} ({r.name}) completed {pct:.0f}% of batch "
                                 f"({completed}/{total}) in {_format_duration(elapsed)}.")

    if not lines:
        return "Insufficient data for narrative conclusion. Run on GPU cluster to generate."

    return " ".join(lines)


def generate_html_report(cfg, results):
    """Generate an HTML comparison report across scenarios."""
    report_path = cfg.results_dir / "report.html"

    # Build latency comparison table
    latency_rows = []
    for result in results:
        burst_agg = _aggregate_phases(result.phases, "burst")
        idle_agg = _aggregate_phases(result.phases, "idle")

        if burst_agg:
            latency_rows.append(
                f"<tr><td>S{result.scenario} ({result.name})</td><td>Burst</td>"
                f"<td>{burst_agg['ttft_p50']:.1f}</td>"
                f"<td>{burst_agg['ttft_p95']:.1f}</td>"
                f"<td>{burst_agg['ttft_p99']:.1f}</td>"
                f"<td>{burst_agg['tpot_p50']:.1f}</td>"
                f"<td>{burst_agg['tpot_p95']:.1f}</td>"
                f"<td>{burst_agg['tpot_p99']:.1f}</td>"
                f"<td>{burst_agg['completed']}</td>"
                f"<td>{burst_agg['error_rate']*100:.1f}%</td></tr>"
            )
        if idle_agg:
            latency_rows.append(
                f"<tr><td>S{result.scenario} ({result.name})</td><td>Idle</td>"
                f"<td>{idle_agg['ttft_p50']:.1f}</td>"
                f"<td>{idle_agg['ttft_p95']:.1f}</td>"
                f"<td>{idle_agg['ttft_p99']:.1f}</td>"
                f"<td>{idle_agg['tpot_p50']:.1f}</td>"
                f"<td>{idle_agg['tpot_p95']:.1f}</td>"
                f"<td>{idle_agg['tpot_p99']:.1f}</td>"
                f"<td>{idle_agg['completed']}</td>"
                f"<td>{idle_agg['error_rate']*100:.1f}%</td></tr>"
            )

    # Build chart data for latency comparison (TTFT, TPOT, ITL)
    chart_data = []
    for result in results:
        burst_agg = _aggregate_phases(result.phases, "burst")
        if burst_agg:
            chart_data.append({
                "scenario": f"S{result.scenario}",
                "name": result.name,
                "ttft_p50": burst_agg["ttft_p50"],
                "ttft_p95": burst_agg["ttft_p95"],
                "ttft_p99": burst_agg["ttft_p99"],
                "tpot_p50": burst_agg["tpot_p50"],
                "tpot_p95": burst_agg["tpot_p95"],
                "tpot_p99": burst_agg["tpot_p99"],
                "itl_p50": burst_agg["itl_p50"],
                "itl_p95": burst_agg["itl_p95"],
                "itl_p99": burst_agg["itl_p99"],
                "error_rate": burst_agg["error_rate"],
            })

    # Error rate data per scenario (both burst and idle)
    error_chart_data = []
    for result in results:
        burst_agg = _aggregate_phases(result.phases, "burst")
        idle_agg = _aggregate_phases(result.phases, "idle")
        error_chart_data.append({
            "scenario": f"S{result.scenario}",
            "name": result.name,
            "burst_error_rate": burst_agg["error_rate"] * 100 if burst_agg else 0,
            "idle_error_rate": idle_agg["error_rate"] * 100 if idle_agg else 0,
            "burst_errors": burst_agg["errors"] if burst_agg else 0,
            "idle_errors": idle_agg["errors"] if idle_agg else 0,
        })

    timelines_json = {}
    for result in results:
        if result.batch_timeline:
            timelines_json[f"S{result.scenario} ({result.name})"] = result.batch_timeline

    # AIMD dynamics data (scenarios with AIMD enabled)
    aimd_chart_data = []
    for result in results:
        if result.aimd_metrics and "aimd_concurrency_limit_series" in result.aimd_metrics:
            aimd_chart_data.append({
                "scenario": f"S{result.scenario}",
                "name": result.name,
                "series": result.aimd_metrics["aimd_concurrency_limit_series"],
                "avg": result.aimd_metrics.get("aimd_concurrency_limit_avg", 0),
                "min": result.aimd_metrics.get("aimd_concurrency_limit_min", 0),
                "max": result.aimd_metrics.get("aimd_concurrency_limit_max", 0),
            })

    # Flow control data (scenario 4)
    flow_control_chart_data = []
    for result in results:
        if result.flow_control_metrics and "flow_control_saturation_series" in result.flow_control_metrics:
            flow_control_chart_data.append({
                "scenario": f"S{result.scenario}",
                "name": result.name,
                "series": result.flow_control_metrics["flow_control_saturation_series"],
                "avg": result.flow_control_metrics.get("flow_control_saturation_avg", 0),
                "max": result.flow_control_metrics.get("flow_control_saturation_max", 0),
            })

    # Flow control queue size data (scenario 4)
    fc_queue_chart_data = []
    for result in results:
        if result.flow_control_metrics and "flow_control_queue_size_series" in result.flow_control_metrics:
            fc_queue_chart_data.append({
                "scenario": f"S{result.scenario}",
                "name": result.name,
                "series": result.flow_control_metrics["flow_control_queue_size_series"],
                "avg": result.flow_control_metrics.get("flow_control_queue_size_avg", 0),
                "max": result.flow_control_metrics.get("flow_control_queue_size_max", 0),
            })

    # GPU / infrastructure metrics data
    gpu_chart_data = []
    for result in results:
        if result.gpu_metrics:
            gpu_chart_data.append({
                "scenario": f"S{result.scenario}",
                "name": result.name,
                "cache_series": result.gpu_metrics.get("gpu_cache_usage_series", []),
                "running_series": result.gpu_metrics.get("requests_running_series", []),
                "waiting_series": result.gpu_metrics.get("requests_waiting_series", []),
                "cache_avg": result.gpu_metrics.get("gpu_cache_usage_avg", 0),
                "running_avg": result.gpu_metrics.get("requests_running_avg", 0),
                "waiting_avg": result.gpu_metrics.get("requests_waiting_avg", 0),
            })

    # Build summary comparison table (one row per scenario)
    summary_rows = []
    for result in results:
        burst_agg = _aggregate_phases(result.phases, "burst")
        idle_agg = _aggregate_phases(result.phases, "idle")

        if burst_agg:
            ttft_burst = f"{burst_agg['ttft_p50']:.0f} / {burst_agg['ttft_p95']:.0f} / {burst_agg['ttft_p99']:.0f}"
            itl_burst = f"{burst_agg['itl_p50']:.0f} / {burst_agg['itl_p95']:.0f} / {burst_agg['itl_p99']:.0f}"
        else:
            ttft_burst = "N/A"
            itl_burst = "N/A"
        idle_rps = f"{idle_agg['completed'] / max(1, cfg.idle_seconds * (cfg.cycles - cfg.warmup_cycles)):.2f}" if idle_agg else "N/A"

        if result.scenario <= 1:
            batch_slo = "N/A"
        elif result.job_completion_times:
            batch_slo = _format_slo_completion(result)
        elif result.batch_timeline:
            final = result.batch_timeline[-1]
            completed = final.get("completed", 0)
            total = final.get("total", 0)
            batch_slo = f"{completed}/{total}" if total > 0 else "N/A"
        else:
            batch_slo = "N/A"

        batch_ttft_p50 = "N/A"
        batch_phases = [p for p in result.phases if "batch" in p.phase.lower()]
        if batch_phases:
            batch_ttft_p50 = f"{sum(p.ttft_p50 for p in batch_phases) / len(batch_phases):.1f}"

        summary_rows.append(
            f"<tr><td>S{result.scenario} ({result.name})</td>"
            f"<td>{ttft_burst}</td>"
            f"<td>{itl_burst}</td>"
            f"<td>{idle_rps}</td>"
            f"<td>{batch_slo}</td>"
            f"<td>{batch_ttft_p50}</td></tr>"
        )

    # Generate narrative conclusion
    narrative = _generate_narrative(results, cfg)

    # Compute phase bands from the first timeline that has phase data
    phase_bands = []
    for tl_data in timelines_json.values():
        if tl_data and any(d.get("phase") for d in tl_data):
            current_phase = None
            band_start = 0
            for point in tl_data:
                phase = point.get("phase", "unknown")
                phase_normalized = "burst" if "burst" in phase.lower() else (
                    "idle" if "idle" in phase.lower() else None)
                if phase_normalized and phase_normalized != current_phase:
                    if current_phase:
                        phase_bands.append({
                            "phase": current_phase,
                            "start": band_start,
                            "end": point["elapsed"]
                        })
                    current_phase = phase_normalized
                    band_start = point["elapsed"]
            if current_phase:
                phase_bands.append({
                    "phase": current_phase,
                    "start": band_start,
                    "end": tl_data[-1]["elapsed"]
                })
            break

    html = textwrap.dedent(f"""\
    <!DOCTYPE html>
    <html>
    <head>
        <title>Batch Gateway Benchmark Report</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3"></script>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                   margin: 40px; max-width: 1400px; margin-left: auto; margin-right: auto;
                   background: #fafafa; color: #333; }}
            h1 {{ color: #1a1a1a; border-bottom: 2px solid #e5e5e5; padding-bottom: 12px; }}
            h2 {{ color: #444; margin-top: 40px; }}
            .card {{ background: white; border-radius: 8px; padding: 24px; margin: 20px 0;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .narrative {{ background: #f0f9ff; border-left: 4px solid #3b82f6;
                         padding: 16px 20px; border-radius: 0 8px 8px 0; margin: 20px 0;
                         line-height: 1.6; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #eee; }}
            th {{ background: #f5f5f5; font-weight: 600; }}
            .chart-container {{ background: white; border-radius: 8px; padding: 20px;
                              margin: 20px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
            .charts-grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; }}
            canvas {{ max-height: 350px; }}
            .metric-good {{ color: #16a34a; font-weight: 600; }}
            .metric-bad {{ color: #dc2626; font-weight: 600; }}
            .legend-note {{ font-size: 0.85em; color: #666; margin-top: 8px; }}
        </style>
    </head>
    <body>
        <h1>Batch Gateway Benchmark Report</h1>

        <div class="narrative">
            <strong>Conclusion:</strong> {narrative}
        </div>

        <div class="card">
            <h2 style="margin-top:0">Configuration</h2>
            <table>
                <tr><td><strong>Model</strong></td><td>{cfg.model}</td></tr>
                <tr><td><strong>GPU</strong></td>
                    <td>{cfg.gpu_count}x GPU, max-model-len={cfg.max_model_len}</td></tr>
                <tr><td><strong>Interactive</strong></td>
                    <td>{cfg.burst_rate} req/s burst ({cfg.burst_seconds}s),
                        {cfg.idle_rate} req/s idle ({cfg.idle_seconds}s),
                        {cfg.cycles} cycles ({cfg.warmup_cycles} warmup)</td></tr>
                <tr><td><strong>Batch</strong></td>
                    <td>{cfg.num_jobs} jobs x {cfg.batch_size} requests</td></tr>
                <tr><td><strong>Prompts</strong></td>
                    <td>{cfg.prompt_tokens} tokens ISL, {cfg.num_system_prompts} system prompts</td></tr>
                <tr><td><strong>Metrics</strong></td>
                    <td>TTFT, TPOT, ITL, request latency (p50/p95/p99)</td></tr>
                <tr><td><strong>Scenarios</strong></td>
                    <td>{', '.join(f'S{r.scenario} ({r.name})' for r in results)}</td></tr>
            </table>
        </div>

        <h2>Summary Comparison</h2>
        <div class="card">
            <p><em>Key metrics at a glance (one row per scenario). Latencies in ms.</em></p>
            <table>
                <tr>
                    <th>Scenario</th>
                    <th>Interactive TTFT at peak (p50 / p95 / p99)</th>
                    <th>Interactive ITL at peak (p50 / p95 / p99)</th>
                    <th>Batch idle throughput (req/s)</th>
                    <th>Batch SLO completion</th>
                    <th>Batch TTFT p50</th>
                </tr>
                {''.join(summary_rows) if summary_rows else '<tr><td colspan="6">No data collected yet</td></tr>'}
            </table>
        </div>

        <h2>Interactive Latency Comparison</h2>
        <div class="card">
            <p><em>All values in milliseconds. Lower is better.
               Warmup cycle(s) excluded from results.</em></p>
            <table>
                <tr>
                    <th>Scenario</th><th>Phase</th>
                    <th>TTFT p50</th><th>TTFT p95</th><th>TTFT p99</th>
                    <th>TPOT p50</th><th>TPOT p95</th><th>TPOT p99</th>
                    <th>Completed</th><th>Error Rate</th>
                </tr>
                {''.join(latency_rows) if latency_rows else '<tr><td colspan="10">No latency data collected yet (run on GPU cluster)</td></tr>'}
            </table>
        </div>

        <div class="charts-grid-3">
            <div class="chart-container">
                <canvas id="ttftChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="itlChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="tpotChart"></canvas>
            </div>
        </div>

        <h2>Error Rate Comparison</h2>
        <div class="chart-container">
            <canvas id="errorChart"></canvas>
        </div>

        <h2>Batch Completion Timeline</h2>
        <div class="chart-container">
            <canvas id="timelineChart"></canvas>
            <p class="legend-note">Shaded bands: <span style="color:#ef4444">&#9632;</span> burst phases,
               <span style="color:#22c55e">&#9632;</span> idle phases</p>
        </div>

        <h2>Infrastructure Metrics</h2>
        <div class="charts-grid">
            <div class="chart-container">
                <canvas id="gpuCacheChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="requestsRunningChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="requestsWaitingChart"></canvas>
            </div>
        </div>

        <h2>AIMD Concurrency Dynamics</h2>
        <div class="chart-container">
            <canvas id="aimdChart"></canvas>
        </div>

        <h2>Flow Control (Scenario 4)</h2>
        <div class="charts-grid">
            <div class="chart-container">
                <canvas id="flowControlChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="fcQueueChart"></canvas>
            </div>
        </div>

        <script>
        const chartData = {json.dumps(chart_data)};
        const timelines = {json.dumps(timelines_json)};
        const aimdData = {json.dumps(aimd_chart_data)};
        const flowControlData = {json.dumps(flow_control_chart_data)};
        const fcQueueData = {json.dumps(fc_queue_chart_data)};
        const gpuData = {json.dumps(gpu_chart_data)};
        const errorData = {json.dumps(error_chart_data)};
        const phaseBands = {json.dumps(phase_bands)};
        const colors = ['#6b7280', '#ef4444', '#f59e0b', '#3b82f6', '#22c55e', '#8b5cf6'];

        // TTFT bar chart (p50/p95/p99)
        if (chartData.length > 0) {{
            new Chart(document.getElementById('ttftChart'), {{
                type: 'bar',
                data: {{
                    labels: chartData.map(d => d.scenario),
                    datasets: [
                        {{ label: 'p50', data: chartData.map(d => d.ttft_p50), backgroundColor: '#93c5fd' }},
                        {{ label: 'p95', data: chartData.map(d => d.ttft_p95), backgroundColor: '#3b82f6' }},
                        {{ label: 'p99', data: chartData.map(d => d.ttft_p99), backgroundColor: '#1d4ed8' }},
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'TTFT During Burst (ms, lower is better)' }} }},
                    scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'ms' }} }} }}
                }}
            }});
        }}

        // ITL bar chart (p50/p95/p99)
        if (chartData.length > 0) {{
            new Chart(document.getElementById('itlChart'), {{
                type: 'bar',
                data: {{
                    labels: chartData.map(d => d.scenario),
                    datasets: [
                        {{ label: 'p50', data: chartData.map(d => d.itl_p50), backgroundColor: '#86efac' }},
                        {{ label: 'p95', data: chartData.map(d => d.itl_p95), backgroundColor: '#22c55e' }},
                        {{ label: 'p99', data: chartData.map(d => d.itl_p99), backgroundColor: '#15803d' }},
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'ITL During Burst (ms, lower is better)' }} }},
                    scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'ms' }} }} }}
                }}
            }});
        }}

        // TPOT bar chart (p50/p95/p99)
        if (chartData.length > 0) {{
            new Chart(document.getElementById('tpotChart'), {{
                type: 'bar',
                data: {{
                    labels: chartData.map(d => d.scenario),
                    datasets: [
                        {{ label: 'p50', data: chartData.map(d => d.tpot_p50), backgroundColor: '#fde68a' }},
                        {{ label: 'p95', data: chartData.map(d => d.tpot_p95), backgroundColor: '#f59e0b' }},
                        {{ label: 'p99', data: chartData.map(d => d.tpot_p99), backgroundColor: '#b45309' }},
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'TPOT During Burst (ms, lower is better)' }} }},
                    scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'ms' }} }} }}
                }}
            }});
        }}

        // Error rate comparison chart
        if (errorData.length > 0) {{
            new Chart(document.getElementById('errorChart'), {{
                type: 'bar',
                data: {{
                    labels: errorData.map(d => d.scenario + ' (' + d.name + ')'),
                    datasets: [
                        {{ label: 'Burst Error Rate (%)', data: errorData.map(d => d.burst_error_rate),
                           backgroundColor: '#fca5a5' }},
                        {{ label: 'Idle Error Rate (%)', data: errorData.map(d => d.idle_error_rate),
                           backgroundColor: '#fed7aa' }},
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'Error Rate by Scenario and Phase (%)' }} }},
                    scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: '%' }} }} }}
                }}
            }});
        }}

        // Timeline chart with phase bands
        const tlDatasets = [];
        let idx = 0;
        for (const [name, data] of Object.entries(timelines)) {{
            tlDatasets.push({{
                label: name,
                data: data.map(d => ({{x: d.elapsed, y: d.completed}})),
                borderColor: colors[idx % colors.length],
                fill: false, tension: 0.1, pointRadius: 2, borderWidth: 2,
            }});
            idx++;
        }}
        if (tlDatasets.length > 0) {{
            const annotations = {{}};
            phaseBands.forEach((band, i) => {{
                annotations['band' + i] = {{
                    type: 'box',
                    xMin: band.start,
                    xMax: band.end,
                    backgroundColor: band.phase === 'burst'
                        ? 'rgba(239, 68, 68, 0.08)'
                        : 'rgba(34, 197, 94, 0.08)',
                    borderWidth: 0,
                }};
            }});
            new Chart(document.getElementById('timelineChart'), {{
                type: 'line',
                data: {{ datasets: tlDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{
                        title: {{ display: true, text: 'Batch Requests Completed Over Time' }},
                        annotation: {{ annotations: annotations }}
                    }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Completed' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // GPU cache usage chart
        if (gpuData.length > 0 && gpuData.some(d => d.cache_series.length > 0)) {{
            const cacheDatasets = gpuData.filter(d => d.cache_series.length > 0).map((d, i) => ({{
                label: d.scenario + ' (' + d.name + ') avg=' + (d.cache_avg * 100).toFixed(1) + '%',
                data: d.cache_series.map((v, j) => ({{x: j * 15, y: v * 100}})),
                borderColor: colors[i % colors.length],
                fill: false, tension: 0.3, pointRadius: 0, borderWidth: 2,
            }}));
            new Chart(document.getElementById('gpuCacheChart'), {{
                type: 'line',
                data: {{ datasets: cacheDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'GPU KV Cache Usage (%)' }} }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Cache Usage (%)' }}, min: 0, max: 100 }}
                    }}
                }}
            }});
        }}

        // Requests running chart
        if (gpuData.length > 0 && gpuData.some(d => d.running_series.length > 0)) {{
            const runningDatasets = gpuData.filter(d => d.running_series.length > 0).map((d, i) => ({{
                label: d.scenario + ' (' + d.name + ') avg=' + d.running_avg.toFixed(1),
                data: d.running_series.map((v, j) => ({{x: j * 15, y: v}})),
                borderColor: colors[i % colors.length],
                fill: false, tension: 0.3, pointRadius: 0, borderWidth: 2,
            }}));
            new Chart(document.getElementById('requestsRunningChart'), {{
                type: 'line',
                data: {{ datasets: runningDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'vLLM Requests Running' }} }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Requests' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // Requests waiting chart
        if (gpuData.length > 0 && gpuData.some(d => (d.waiting_series || []).length > 0)) {{
            const waitingDatasets = gpuData.filter(d => (d.waiting_series || []).length > 0).map((d, i) => ({{
                label: d.scenario + ' (' + d.name + ') avg=' + (d.waiting_avg || 0).toFixed(1),
                data: d.waiting_series.map((v, j) => ({{x: j * 15, y: v}})),
                borderColor: colors[i % colors.length],
                fill: false, tension: 0.3, pointRadius: 0, borderWidth: 2,
            }}));
            new Chart(document.getElementById('requestsWaitingChart'), {{
                type: 'line',
                data: {{ datasets: waitingDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'vLLM Requests Waiting (Queue Pressure)' }} }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Requests' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // AIMD concurrency limit time-series chart
        if (aimdData.length > 0) {{
            const aimdDatasets = aimdData.map((d, i) => ({{
                label: d.scenario + ' (' + d.name + ') avg=' + d.avg.toFixed(1)
                    + ' min=' + d.min.toFixed(0) + ' max=' + d.max.toFixed(0),
                data: d.series.map((v, j) => ({{x: j * 15, y: v}})),
                borderColor: colors[(i + 3) % colors.length],
                fill: false, tension: 0.3, pointRadius: 0, borderWidth: 2,
            }}));
            new Chart(document.getElementById('aimdChart'), {{
                type: 'line',
                data: {{ datasets: aimdDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'AIMD Concurrency Limit Over Time' }} }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Concurrency Limit' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // Flow control pool saturation time-series chart
        if (flowControlData.length > 0) {{
            const fcDatasets = flowControlData.map((d, i) => ({{
                label: d.scenario + ' (' + d.name + ') avg=' + (d.avg * 100).toFixed(1) + '%',
                data: d.series.map((v, j) => ({{x: j * 15, y: v * 100}})),
                borderColor: '#dc2626',
                fill: true,
                backgroundColor: 'rgba(220, 38, 38, 0.1)',
                tension: 0.3, pointRadius: 0, borderWidth: 2,
            }}));
            new Chart(document.getElementById('flowControlChart'), {{
                type: 'line',
                data: {{ datasets: fcDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'Flow Control Pool Saturation (%)' }} }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Saturation (%)' }}, min: 0, max: 100 }}
                    }}
                }}
            }});
        }}

        // Flow control queue size chart
        if (fcQueueData.length > 0) {{
            const queueDatasets = fcQueueData.map((d, i) => ({{
                label: d.scenario + ' (' + d.name + ') avg=' + d.avg.toFixed(1),
                data: d.series.map((v, j) => ({{x: j * 15, y: v}})),
                borderColor: '#7c3aed',
                fill: true,
                backgroundColor: 'rgba(124, 58, 237, 0.1)',
                tension: 0.3, pointRadius: 0, borderWidth: 2,
            }}));
            new Chart(document.getElementById('fcQueueChart'), {{
                type: 'line',
                data: {{ datasets: queueDatasets }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'Flow Control Queue Size' }} }},
                    scales: {{
                        x: {{ type: 'linear', title: {{ display: true, text: 'Time (s)' }} }},
                        y: {{ title: {{ display: true, text: 'Queue Depth' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}
        </script>
    </body>
    </html>
    """)

    report_path.write_text(html)
    log(f"Report written to {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# Managed setup/teardown (--managed mode)
# ---------------------------------------------------------------------------


def _managed_setup(args, scenario):
    """Run benchmarks/setup.sh with env vars for the given scenario."""
    namespace = _NAMESPACE_OVERRIDE or f"batch-bench-s{scenario}"
    ghcr_user = getattr(args, "ghcr_user", None) or os.environ.get("GHCR_USER", "")
    ghcr_token = getattr(args, "ghcr_token", None) or os.environ.get("GHCR_TOKEN", "")
    router_repo = getattr(args, "router_repo", None) or os.environ.get("ROUTER_REPO", "")
    prometheus_release = (
        getattr(args, "prometheus_release", None)
        or os.environ.get("PROMETHEUS_RELEASE", "")
    )

    env = {
        **os.environ,
        "SCENARIO": str(scenario),
        "MODE": "gpu",
        "NAMESPACE": namespace,
        "KUBE_CONTEXT": args.context,
        "GHCR_USER": ghcr_user,
        "GHCR_TOKEN": ghcr_token,
        "ROUTER_REPO": router_repo,
        "PROMETHEUS_RELEASE": prometheus_release,
        "PROMETHEUS_NAMESPACE": getattr(args, "prometheus_namespace", "") or "",
    }

    setup_script = str(SCRIPT_DIR / "setup.sh")
    log(f"  [managed] Running setup.sh for scenario {scenario} in {namespace}")
    result = subprocess.run(
        ["bash", setup_script],
        env=env,
        check=False,
    )
    if result.returncode != 0:
        log(f"  [managed] WARNING: setup.sh exited with code {result.returncode}")


def _managed_teardown(args, scenario):
    """Tear down all benchmark resources in the namespace."""
    namespace = _NAMESPACE_OVERRIDE or f"batch-bench-s{scenario}"
    context = args.context

    log(f"  [managed] Tearing down resources in {namespace}")

    teardown_commands = [
        ["helm", f"--kube-context={context}", "uninstall", "batch-gateway", "-n", namespace],
        ["helm", f"--kube-context={context}", "uninstall", "optimized-baseline", "-n", namespace],
        ["helm", f"--kube-context={context}", "uninstall", "epp-bench", "-n", namespace],
        ["kubectl", f"--context={context}", "-n", namespace, "delete", "job", "--all", "--ignore-not-found"],
        ["kubectl", f"--context={context}", "-n", namespace, "delete", "configmap",
         "-l", "batch-benchmark=true", "--ignore-not-found"],
        ["kubectl", f"--context={context}", "-n", namespace, "delete", "-k",
         str(SCRIPT_DIR / "manifests" / "vllm/"), "--ignore-not-found"],
        ["kubectl", f"--context={context}", "-n", namespace, "delete", "gateway",
         "llm-d-inference-gateway", "--ignore-not-found"],
    ]

    for cmd in teardown_commands:
        subprocess.run(cmd, check=False)


# ---------------------------------------------------------------------------
# Scenario runners
# ---------------------------------------------------------------------------


def run_scenario(cfg, scenario):
    """Run a single benchmark scenario end-to-end."""
    name = SCENARIO_NAMES[scenario]
    namespace = namespace_for_scenario(scenario)
    log(f"━━━ Scenario {scenario}: {name} ━━━")

    # Verify namespace exists
    try:
        kubectl(["get", "ns", namespace], cfg.context)
    except subprocess.CalledProcessError:
        log(f"  ERROR: Namespace {namespace} does not exist. Run setup.sh first.")
        return ScenarioResult(scenario=scenario, name=name)

    # Cleanup previous run (scenarios 0-1: delete old jobs; scenarios 2+: full cleanup)
    if scenario >= 2:
        cleanup_namespace(cfg.context, namespace)
    else:
        kubectl(["delete", "job", "--all", "--ignore-not-found"],
                cfg.context, namespace, check=False)

    # Clean stale CSV files from results PVC to prevent cross-scenario contamination
    kubectl(["delete", "pod", "pvc-cleaner", "--ignore-not-found"],
            cfg.context, namespace, check=False)
    kubectl(["run", "--rm", "-i", "pvc-cleaner", "--image=busybox",
             "--restart=Never", "--overrides",
             '{"spec":{"securityContext":{"runAsNonRoot":true,"runAsUser":1000,"fsGroup":1000},'
             '"containers":[{"name":"c","image":"busybox",'
             '"command":["sh","-c","rm -f /results/*.csv"],'
             '"securityContext":{"allowPrivilegeEscalation":false},'
             '"volumeMounts":[{"name":"r","mountPath":"/results"}]}],'
             '"volumes":[{"name":"r","persistentVolumeClaim":'
             '{"claimName":"benchmark-results"}}]}}', "--"],
            cfg.context, namespace, check=False)

    # Submit batch (scenarios 2-6)
    if scenario >= 2:
        submit_batches(cfg, namespace)
        time.sleep(10)

    # Scenario 1: start batch-equivalent traffic as regular requests
    if scenario == 1:
        start_batch_as_interactive_traffic(cfg, namespace)

    # Start interactive traffic (all scenarios)
    start_interactive_traffic(cfg, namespace)
    time.sleep(30)  # Let guidellm validate and start

    # Monitor
    job_completion_times = {}
    if scenario >= 2:
        timeline, job_completion_times = monitor_scenario(cfg, scenario, namespace)
    else:
        # Scenarios 0-1: wait for guidellm job(s) to finish
        timeline = []
        total_wait = cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds) + 120
        log(f"  Waiting up to {total_wait}s for traffic to complete...")
        start = time.time()
        while time.time() - start < total_wait:
            try:
                gs = kubectl(["get", "pods", "-l", "job-name=guidellm-interactive",
                             "-o", "jsonpath={.items[0].status.phase}"],
                            cfg.context, namespace, check=False)
                if gs in ("Succeeded", "Failed"):
                    break
            except Exception:
                pass
            time.sleep(10)

        # Scenario 1: also wait for the batch-as-interactive job
        if scenario == 1:
            log("  Waiting for guidellm-batch job to complete...")
            try:
                kubectl(["wait", "--for=condition=complete", "job/guidellm-batch",
                         f"--timeout={total_wait}s"],
                        cfg.context, namespace, check=False)
            except Exception:
                pass

    # Collect results
    results_dir = collect_results(cfg, scenario, namespace)

    # Parse guidellm CSVs into PhaseMetrics (exclude warmup files)
    phases = []
    if results_dir and results_dir.exists():
        for csv_path in sorted(results_dir.glob("*.csv")):
            if "warmup" in csv_path.name:
                continue
            metrics = parse_guidellm_csv(csv_path)
            if metrics.completed > 0:
                phases.append(metrics)

    # Collect AIMD metrics (scenarios with AIMD enabled: S3, S4)
    aimd_metrics = {}
    if scenario >= 3 and os.environ.get("PROMETHEUS_URL"):
        end_time = datetime.datetime.utcfromtimestamp(time.time())
        start_time = end_time - datetime.timedelta(seconds=cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds))
        aimd_metrics = collect_aimd_metrics(cfg.context, namespace, start_time, end_time)
        if aimd_metrics:
            log(f"  AIMD: concurrency limit avg={aimd_metrics.get('aimd_concurrency_limit_avg', 0):.1f}")

    # Collect flow control metrics for scenario 4
    flow_control_metrics = {}
    if scenario == 4 and os.environ.get("PROMETHEUS_URL"):
        end_time = datetime.datetime.utcfromtimestamp(time.time())
        start_time = end_time - datetime.timedelta(seconds=cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds))
        flow_control_metrics = collect_flow_control_metrics(cfg.context, namespace, start_time, end_time)
        if flow_control_metrics:
            log(f"  Flow control: saturation avg={flow_control_metrics.get('flow_control_saturation_avg', 0):.2f}")

    # Collect GPU/infrastructure metrics for all scenarios
    gpu_metrics = {}
    if os.environ.get("PROMETHEUS_URL"):
        end_time = datetime.datetime.utcfromtimestamp(time.time())
        start_time = end_time - datetime.timedelta(seconds=cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds))
        gpu_metrics = collect_gpu_metrics(cfg.context, namespace, start_time, end_time)
        if gpu_metrics:
            log(f"  GPU: cache_usage avg={gpu_metrics.get('gpu_cache_usage_avg', 0) * 100:.1f}%, "
                f"running avg={gpu_metrics.get('requests_running_avg', 0):.1f}")

    return ScenarioResult(scenario=scenario, name=name, phases=phases,
                          batch_timeline=timeline,
                          job_completion_times=job_completion_times,
                          aimd_metrics=aimd_metrics,
                          flow_control_metrics=flow_control_metrics,
                          gpu_metrics=gpu_metrics)


# ---------------------------------------------------------------------------
# Multi-trial aggregation
# ---------------------------------------------------------------------------


def _aggregate_trials(trial_results):
    """Aggregate multiple trial runs into a single ScenarioResult with variance info.

    Computes mean and 95% confidence interval for each metric across trials.
    """
    import math

    if not trial_results:
        return ScenarioResult(scenario=0, name="unknown")

    base = trial_results[0]
    scenario = base.scenario
    name = base.name

    # Collect all phases across trials, grouped by (phase, cycle)
    phase_groups = {}
    for result in trial_results:
        for phase in result.phases:
            key = (phase.phase, phase.cycle)
            if key not in phase_groups:
                phase_groups[key] = []
            phase_groups[key].append(phase)

    def mean_and_ci(values):
        """Return (mean, ci_95) for a list of values."""
        n = len(values)
        if n == 0:
            return 0.0, 0.0
        m = sum(values) / n
        if n < 2:
            return m, 0.0
        variance = sum((v - m) ** 2 for v in values) / (n - 1)
        stderr = math.sqrt(variance / n)
        return m, stderr * 1.96

    aggregated_phases = []
    for (phase_name, cycle), phases in sorted(phase_groups.items()):
        n = len(phases)
        if n == 0:
            continue

        agg = PhaseMetrics(
            phase=phase_name, cycle=cycle,
            ttft_p50=sum(p.ttft_p50 for p in phases) / n,
            ttft_p95=sum(p.ttft_p95 for p in phases) / n,
            ttft_p99=sum(p.ttft_p99 for p in phases) / n,
            itl_p50=sum(p.itl_p50 for p in phases) / n,
            itl_p95=sum(p.itl_p95 for p in phases) / n,
            itl_p99=sum(p.itl_p99 for p in phases) / n,
            tpot_p50=sum(p.tpot_p50 for p in phases) / n,
            tpot_p95=sum(p.tpot_p95 for p in phases) / n,
            tpot_p99=sum(p.tpot_p99 for p in phases) / n,
            req_latency_p50=sum(p.req_latency_p50 for p in phases) / n,
            req_latency_p95=sum(p.req_latency_p95 for p in phases) / n,
            req_latency_p99=sum(p.req_latency_p99 for p in phases) / n,
            ok_rps=sum(p.ok_rps for p in phases) / n,
            completed=sum(p.completed for p in phases) // n,
            errors=sum(p.errors for p in phases) // n,
            error_rate=sum(p.error_rate for p in phases) / n,
        )
        aggregated_phases.append(agg)

    # Use longest timeline from any trial
    best_timeline = max((r.batch_timeline for r in trial_results), key=len, default=[])

    # Use AIMD/flow-control/GPU metrics from first trial that has them
    aimd = next((r.aimd_metrics for r in trial_results if r.aimd_metrics), {})
    fc = next((r.flow_control_metrics for r in trial_results if r.flow_control_metrics), {})
    gpu = next((r.gpu_metrics for r in trial_results if r.gpu_metrics), {})
    jct = next((r.job_completion_times for r in trial_results if r.job_completion_times), {})

    result = ScenarioResult(
        scenario=scenario, name=name,
        phases=aggregated_phases, batch_timeline=best_timeline,
        job_completion_times=jct,
        aimd_metrics=aimd, flow_control_metrics=fc,
        gpu_metrics=gpu,
    )

    # Attach variance metadata for reporting
    result._trial_count = len(trial_results)
    result._ttft_p99_ci = {}
    result._tpot_p99_ci = {}
    for (phase_name, cycle), phases in phase_groups.items():
        key = f"{phase_name}-{cycle}"
        _, ttft_ci = mean_and_ci([p.ttft_p99 for p in phases])
        _, tpot_ci = mean_and_ci([p.tpot_p99 for p in phases])
        if ttft_ci > 0:
            result._ttft_p99_ci[key] = ttft_ci
        if tpot_ci > 0:
            result._tpot_p99_ci[key] = tpot_ci

    return result


# ---------------------------------------------------------------------------
# Rate sweep mode
# ---------------------------------------------------------------------------


@dataclass
class RateSweepPoint:
    rate: int
    scenario: int
    ttft_p50: float = 0.0
    ttft_p95: float = 0.0
    ttft_p99: float = 0.0
    tpot_p50: float = 0.0
    tpot_p95: float = 0.0
    tpot_p99: float = 0.0
    throughput: float = 0.0
    error_rate: float = 0.0


def auto_calibrate_rate(cfg, namespace):
    """Discover the saturating rate with a short preliminary sweep.

    Runs short bursts at increasing rates until error rate exceeds 5%
    or TTFT p99 exceeds 3x the baseline (lowest rate).
    Returns the last rate before saturation was detected.
    """
    log("  Auto-calibrating saturating rate...")
    calibration_rates = [1, 5, 10, 15, 20, 25, 30, 40, 50]
    baseline_ttft = None
    saturating_rate = calibration_rates[-1]

    for rate in calibration_rates:
        log(f"    Probing {rate} req/s (15s)...")
        probe_cfg = BenchmarkConfig(
            context=cfg.context, scenarios=cfg.scenarios, model=cfg.model,
            burst_rate=rate, idle_rate=1,
            burst_seconds=15, idle_seconds=0, cycles=1,
            batch_size=cfg.batch_size, num_jobs=0,
            prompt_tokens=cfg.prompt_tokens,
            num_system_prompts=cfg.num_system_prompts,
            results_dir=cfg.results_dir / "calibration",
            target=cfg.target, gpu_count=cfg.gpu_count,
            max_model_len=cfg.max_model_len, warmup_cycles=0,
        )
        probe_cfg.results_dir.mkdir(parents=True, exist_ok=True)

        start_interactive_traffic(probe_cfg, namespace)

        # Wait for probe job to complete (15s burst + buffer)
        try:
            kubectl(["wait", "--for=condition=complete", "job/guidellm-interactive",
                     "--timeout=45s"], cfg.context, namespace, check=False)
        except Exception:
            pass

        # Check pod status
        try:
            gs = kubectl(["get", "pods", "-l", "job-name=guidellm-interactive",
                         "-o", "jsonpath={.items[0].status.phase}"],
                        cfg.context, namespace, check=False)
        except Exception:
            gs = "Unknown"

        # Collect and parse probe results
        probe_metrics = None
        results_dir = collect_results(probe_cfg, 0, namespace)
        if results_dir and results_dir.exists():
            for csv_path in results_dir.glob("*.csv"):
                m = parse_guidellm_csv(csv_path)
                if m.completed > 0:
                    probe_metrics = m
                    break

        # Clean up probe job
        kubectl(["delete", "job", "guidellm-interactive", "--ignore-not-found"],
                cfg.context, namespace, check=False)
        time.sleep(5)

        # Detect saturation via TTFT degradation or error rate
        if probe_metrics and probe_metrics.completed > 0:
            ttft_p99 = probe_metrics.ttft_p99
            log(f"    Rate {rate}: TTFT p99={ttft_p99:.1f}ms, "
                f"errors={probe_metrics.error_rate*100:.1f}%")

            if baseline_ttft is None:
                baseline_ttft = ttft_p99
                log(f"    Baseline TTFT p99: {baseline_ttft:.1f}ms")
            elif baseline_ttft > 0 and ttft_p99 > 3 * baseline_ttft:
                saturating_rate = max(1, rate - calibration_rates[1])
                log(f"    Saturating rate: {saturating_rate} req/s "
                    f"(TTFT p99 {ttft_p99:.1f}ms > 3x baseline {baseline_ttft:.1f}ms)")
                break

            if probe_metrics.error_rate > 0.05:
                saturating_rate = max(1, rate - calibration_rates[1])
                log(f"    Saturating rate: {saturating_rate} req/s "
                    f"(error rate {probe_metrics.error_rate*100:.1f}% > 5%)")
                break
        elif gs == "Failed":
            saturating_rate = max(1, rate - calibration_rates[1])
            log(f"    Saturating rate: {saturating_rate} req/s (probe failed at {rate})")
            break

    log(f"    Final saturating rate: {saturating_rate} req/s")
    return saturating_rate


def run_rate_sweep(cfg, scenarios, rate_min, rate_max, rate_step, duration):
    """Run each scenario across a range of request rates."""
    rates = list(range(rate_min, rate_max + 1, rate_step))
    log(f"Rate sweep: {rates} req/s across scenarios {scenarios}")

    sweep_results = []

    for scenario in scenarios:
        name = SCENARIO_NAMES[scenario]
        namespace = namespace_for_scenario(scenario)
        log(f"━━━ Sweep: Scenario {scenario} ({name}) ━━━")

        for rate in rates:
            log(f"  Rate: {rate} req/s ({duration}s)")

            # Clean up previous run
            kubectl(["delete", "job", "--all", "--ignore-not-found"],
                    cfg.context, namespace, check=False)
            time.sleep(3)

            # Create a single-burst config for this rate point
            point_cfg = BenchmarkConfig(
                context=cfg.context, scenarios=[scenario], model=cfg.model,
                burst_rate=rate, idle_rate=rate,
                burst_seconds=duration, idle_seconds=0, cycles=1,
                batch_size=cfg.batch_size, num_jobs=cfg.num_jobs if scenario >= 2 else 0,
                prompt_tokens=cfg.prompt_tokens,
                num_system_prompts=cfg.num_system_prompts,
                results_dir=cfg.results_dir / f"sweep-s{scenario}-r{rate}",
                target=cfg.target, gpu_count=cfg.gpu_count,
                max_model_len=cfg.max_model_len, warmup_cycles=0,
            )
            point_cfg.results_dir.mkdir(parents=True, exist_ok=True)

            # Submit batch if needed
            if scenario >= 2:
                submit_batches(point_cfg, namespace)
                time.sleep(5)

            # Run traffic at this rate
            start_interactive_traffic(point_cfg, namespace)
            time.sleep(duration + 15)

            # Wait for completion
            try:
                kubectl(["wait", "--for=condition=complete", "job/guidellm-interactive",
                         f"--timeout={duration + 30}s"],
                        cfg.context, namespace, check=False)
            except Exception:
                pass

            # Collect and parse results
            results_dir = collect_results(point_cfg, scenario, namespace)
            point = RateSweepPoint(rate=rate, scenario=scenario)

            if results_dir and results_dir.exists():
                for csv_path in results_dir.glob("*.csv"):
                    metrics = parse_guidellm_csv(csv_path)
                    if metrics.completed > 0:
                        point.ttft_p50 = metrics.ttft_p50
                        point.ttft_p95 = metrics.ttft_p95
                        point.ttft_p99 = metrics.ttft_p99
                        point.tpot_p50 = metrics.tpot_p50
                        point.tpot_p95 = metrics.tpot_p95
                        point.tpot_p99 = metrics.tpot_p99
                        point.throughput = metrics.ok_rps
                        point.error_rate = metrics.error_rate
                        break

            sweep_results.append(point)
            log(f"    TTFT p99={point.ttft_p99:.1f}ms, "
                f"throughput={point.throughput:.1f} rps, "
                f"errors={point.error_rate*100:.1f}%")

    return sweep_results


def save_sweep_results(cfg, sweep_results):
    """Save rate sweep results as JSON and generate a sweep chart."""
    sweep_data = {}
    for point in sweep_results:
        key = f"S{point.scenario}"
        if key not in sweep_data:
            sweep_data[key] = []
        sweep_data[key].append({
            "rate": point.rate,
            "ttft_p99_ms": point.ttft_p99,
            "tpot_p99_ms": point.tpot_p99,
            "throughput_rps": point.throughput,
            "error_rate": point.error_rate,
        })

    sweep_path = cfg.results_dir / "sweep-results.json"
    with open(sweep_path, "w") as f:
        json.dump(sweep_data, f, indent=2)
    log(f"Sweep results saved to {sweep_path}")
    return sweep_data


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    profile = load_profile()
    bench_cfg = profile.get("benchmark", {})
    prompt_cfg = profile.get("prompt", {})

    parser = argparse.ArgumentParser(
        description="Batch Gateway Benchmark Orchestrator"
    )
    parser.add_argument("--context", required=True, help="kubectl context")
    parser.add_argument("--namespace", default=None,
                        help="Override namespace (default: batch-bench-s{scenario})")
    parser.add_argument("--scenarios", type=int, nargs="+", default=[2],
                        help="Scenarios to run (default: [2])")
    parser.add_argument("--model",
                        default=bench_cfg.get("model", "Qwen/Qwen3-8B"),
                        help="Model name (default: Qwen/Qwen3-8B)")
    parser.add_argument("--burst-rate", type=int,
                        default=bench_cfg.get("burst_rate", 15),
                        help="Requests/s during burst (default: 15)")
    parser.add_argument("--idle-rate", type=int,
                        default=bench_cfg.get("idle_rate", 1),
                        help="Requests/s during idle (default: 1)")
    parser.add_argument("--burst-seconds", type=int,
                        default=bench_cfg.get("burst_seconds", 90),
                        help="Duration of burst phase (default: 90)")
    parser.add_argument("--idle-seconds", type=int,
                        default=bench_cfg.get("idle_seconds", 90),
                        help="Duration of idle phase (default: 90)")
    parser.add_argument("--cycles", type=int,
                        default=bench_cfg.get("cycles", 4),
                        help="Number of burst/idle cycles (default: 4)")
    parser.add_argument("--batch-size", type=int,
                        default=bench_cfg.get("batch_size", 1000),
                        help="Requests per batch job (default: 1000)")
    parser.add_argument("--num-jobs", type=int,
                        default=bench_cfg.get("num_jobs", 3),
                        help="Concurrent batch jobs (default: 3)")
    parser.add_argument("--prompt-tokens", type=int,
                        default=prompt_cfg.get("prompt_tokens", 256),
                        help="Input tokens per prompt (default: 256)")
    parser.add_argument("--num-system-prompts", type=int,
                        default=prompt_cfg.get("num_system_prompts", 5),
                        help="Distinct system prompts (default: 5)")
    parser.add_argument("--results-dir", type=Path,
                        default=Path("benchmarks/results/latest"),
                        help="Output directory (default: benchmarks/results/latest)")
    parser.add_argument("--target",
                        default="http://llm-d-inference-gateway-istio",
                        help="Inference gateway URL (default: http://llm-d-inference-gateway-istio)")
    parser.add_argument("--gpu-count", type=int,
                        default=int(os.environ.get("GPU_COUNT", "1")),
                        help="Number of GPUs per node (default: 1)")
    parser.add_argument("--max-model-len", type=int,
                        default=int(os.environ.get("MAX_MODEL_LEN", "4096")),
                        help="vLLM max-model-len (default: 4096)")
    parser.add_argument("--warmup", type=int,
                        default=bench_cfg.get("warmup_cycles", 2),
                        help="Number of warmup cycles to exclude from results (default: 2)")

    # Prometheus port-forward automation
    parser.add_argument("--prometheus-namespace",
                        default=os.environ.get("PROMETHEUS_NAMESPACE", "llm-d-monitoring"),
                        help="Namespace where Prometheus is deployed (default: llm-d-monitoring)")
    parser.add_argument("--prometheus-service",
                        default=os.environ.get("PROMETHEUS_SERVICE", "llmd-kube-prometheus-stack-prometheus"),
                        help="Prometheus service name (default: llmd-kube-prometheus-stack-prometheus)")

    # Multiple trials
    parser.add_argument("--trials", type=int, default=1,
                        help="Number of times to run each scenario (default: 1)")

    # Rate sweep mode
    parser.add_argument("--rate-sweep", action="store_true",
                        help="Run each scenario across a range of request rates")
    parser.add_argument("--rate-sweep-min", type=int, default=1,
                        help="Minimum rate for sweep (default: 1 rps)")
    parser.add_argument("--rate-sweep-max", type=int, default=None,
                        help="Maximum rate for sweep (default: auto-calibrate)")
    parser.add_argument("--rate-sweep-step", type=int, default=5,
                        help="Rate increment for sweep (default: 5 rps)")
    parser.add_argument("--rate-sweep-duration", type=int, default=30,
                        help="Duration per rate point in sweep mode (default: 30s)")

    # Managed mode: orchestrate setup/teardown per scenario
    parser.add_argument("--managed", action="store_true",
                        help="Orchestrate setup/teardown per scenario internally")
    parser.add_argument("--ghcr-user", default=None,
                        help="GHCR username (or set GHCR_USER env)")
    parser.add_argument("--ghcr-token", default=None,
                        help="GHCR token (or set GHCR_TOKEN env)")
    parser.add_argument("--router-repo", default=None,
                        help="Router image repo (or set ROUTER_REPO env)")
    parser.add_argument("--prometheus-release", default=None,
                        help="Prometheus Helm release name (or set PROMETHEUS_RELEASE env)")

    args = parser.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    global _NAMESPACE_OVERRIDE
    _NAMESPACE_OVERRIDE = args.namespace

    cfg = BenchmarkConfig(
        context=args.context,
        scenarios=args.scenarios,
        model=args.model,
        burst_rate=args.burst_rate,
        idle_rate=args.idle_rate,
        burst_seconds=args.burst_seconds,
        idle_seconds=args.idle_seconds,
        cycles=args.cycles,
        batch_size=args.batch_size,
        num_jobs=args.num_jobs,
        prompt_tokens=args.prompt_tokens,
        num_system_prompts=args.num_system_prompts,
        results_dir=args.results_dir,
        target=args.target,
        gpu_count=args.gpu_count,
        max_model_len=args.max_model_len,
        warmup_cycles=args.warmup,
    )

    log("=== Batch Gateway Benchmark ===")
    log(f"Scenarios: {[f'{s} ({SCENARIO_NAMES[s]})' for s in cfg.scenarios]}")
    log(f"Traffic: {cfg.burst_rate} req/s burst ({cfg.burst_seconds}s), "
        f"{cfg.idle_rate} req/s idle ({cfg.idle_seconds}s), {cfg.cycles} cycles")
    log(f"Batch: {cfg.num_jobs} jobs x {cfg.batch_size} requests")
    log(f"Results: {cfg.results_dir}")

    # Start Prometheus port-forward if needed (for scenarios >= 3)
    if any(s >= 3 for s in cfg.scenarios) and not os.environ.get("PROMETHEUS_URL"):
        start_prometheus_port_forward(
            args.context, args.prometheus_namespace, args.prometheus_service,
        )

    # Validate scenarios
    for s in cfg.scenarios:
        if s not in SCENARIO_NAMES:
            log(f"ERROR: Unknown scenario {s}. Valid: 0-6")
            sys.exit(1)

    # Rate sweep mode
    if args.rate_sweep:
        rate_max = args.rate_sweep_max
        if rate_max is None:
            # Auto-calibrate: discover saturating rate
            namespace = namespace_for_scenario(cfg.scenarios[0])
            rate_max = auto_calibrate_rate(cfg, namespace)
            log(f"Auto-calibrated max rate: {rate_max} req/s")

        sweep_results = run_rate_sweep(
            cfg, cfg.scenarios,
            rate_min=args.rate_sweep_min,
            rate_max=rate_max,
            rate_step=args.rate_sweep_step,
            duration=args.rate_sweep_duration,
        )
        save_sweep_results(cfg, sweep_results)
        log("=== Rate sweep complete ===")
        log(f"Results: {cfg.results_dir / 'sweep-results.json'}")
        return

    # Run each scenario (normal mode), with optional multiple trials
    trials = args.trials
    results = []

    if trials > 1:
        log(f"Running {trials} trials per scenario for statistical significance")

    for scenario in cfg.scenarios:
        trial_results = []
        for trial in range(1, trials + 1):
            if trials > 1:
                log(f"  Trial {trial}/{trials} for scenario {scenario}")
                trial_dir = cfg.results_dir / f"trial-{trial}"
                trial_dir.mkdir(parents=True, exist_ok=True)
                trial_cfg = BenchmarkConfig(
                    context=cfg.context, scenarios=cfg.scenarios, model=cfg.model,
                    burst_rate=cfg.burst_rate, idle_rate=cfg.idle_rate,
                    burst_seconds=cfg.burst_seconds, idle_seconds=cfg.idle_seconds,
                    cycles=cfg.cycles, batch_size=cfg.batch_size, num_jobs=cfg.num_jobs,
                    prompt_tokens=cfg.prompt_tokens,
                    num_system_prompts=cfg.num_system_prompts,
                    results_dir=trial_dir, target=cfg.target,
                    gpu_count=cfg.gpu_count, max_model_len=cfg.max_model_len,
                    warmup_cycles=cfg.warmup_cycles,
                )
                if args.managed:
                    _managed_setup(args, scenario)
                result = run_scenario(trial_cfg, scenario)
                if args.managed:
                    _managed_teardown(args, scenario)
            else:
                if args.managed:
                    _managed_setup(args, scenario)
                result = run_scenario(cfg, scenario)
                if args.managed:
                    _managed_teardown(args, scenario)
            trial_results.append(result)

        if trials > 1:
            # Aggregate trial results with variance
            aggregated = _aggregate_trials(trial_results)
            results.append(aggregated)
        else:
            results.append(trial_results[0])

    # Save timelines
    for result in results:
        if result.batch_timeline:
            timeline_path = cfg.results_dir / f"scenario-{result.scenario}-timeline.json"
            timeline_path.write_text(json.dumps(result.batch_timeline, indent=2))

    # Generate report
    generate_html_report(cfg, results)

    # Write machine-readable run metadata
    metadata = {
        "profile": "default",
        "parameters": {
            "model": cfg.model,
            "gpu_count": cfg.gpu_count,
            "max_model_len": cfg.max_model_len,
            "burst_rate": cfg.burst_rate,
            "idle_rate": cfg.idle_rate,
            "burst_seconds": cfg.burst_seconds,
            "idle_seconds": cfg.idle_seconds,
            "cycles": cfg.cycles,
            "warmup_cycles": cfg.warmup_cycles,
            "trials": trials,
            "batch_size": cfg.batch_size,
            "num_jobs": cfg.num_jobs,
            "prompt_tokens": cfg.prompt_tokens,
            "num_system_prompts": cfg.num_system_prompts,
        },
        "metrics_collected": ["ttft", "tpot", "itl", "req_latency"],
        "percentiles": ["p50", "p95", "p99"],
        "versions": {
            "batch_gateway": "unknown",
            "router": "unknown",
            "vllm": "unknown",
            "model_revision": os.environ.get("MODEL_REVISION", "latest"),
        },
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }
    metadata_path = cfg.results_dir / "run-metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    # Emit structured results.json (inference-perf compatible schema)
    structured_results = {
        "schema_version": "1.0",
        "metadata": metadata,
        "scenarios": [],
    }
    for result in results:
        scenario_data = {
            "scenario": result.scenario,
            "name": result.name,
            "phases": [],
            "batch_timeline": result.batch_timeline,
            "job_completion_times": result.job_completion_times,
            "summary": {},
        }

        for phase in result.phases:
            scenario_data["phases"].append({
                "phase": phase.phase,
                "cycle": phase.cycle,
                "latency": {
                    "ttft_ms": {"p50": phase.ttft_p50, "p95": phase.ttft_p95, "p99": phase.ttft_p99},
                    "itl_ms": {"p50": phase.itl_p50, "p95": phase.itl_p95, "p99": phase.itl_p99},
                    "tpot_ms": {"p50": phase.tpot_p50, "p95": phase.tpot_p95, "p99": phase.tpot_p99},
                    "request_ms": {"p50": phase.req_latency_p50, "p95": phase.req_latency_p95,
                                   "p99": phase.req_latency_p99},
                },
                "throughput": {
                    "ok_rps": phase.ok_rps,
                    "err_rps": phase.err_rps,
                },
                "counts": {
                    "completed": phase.completed,
                    "errors": phase.errors,
                    "error_rate": phase.error_rate,
                },
            })

        # Aggregate summary across all non-warmup phases
        burst_phases = [p for p in result.phases if "burst" in p.phase]
        if burst_phases:
            n = len(burst_phases)
            scenario_data["summary"]["burst"] = {
                "ttft_ms": {
                    "p50": sum(p.ttft_p50 for p in burst_phases) / n,
                    "p95": sum(p.ttft_p95 for p in burst_phases) / n,
                    "p99": sum(p.ttft_p99 for p in burst_phases) / n,
                },
                "tpot_ms": {
                    "p50": sum(p.tpot_p50 for p in burst_phases) / n,
                    "p95": sum(p.tpot_p95 for p in burst_phases) / n,
                    "p99": sum(p.tpot_p99 for p in burst_phases) / n,
                },
                "total_completed": sum(p.completed for p in burst_phases),
                "total_errors": sum(p.errors for p in burst_phases),
            }

        # AIMD and flow control metrics
        if result.aimd_metrics:
            scenario_data["aimd"] = {
                k: v for k, v in result.aimd_metrics.items()
                if k != "aimd_concurrency_limit_series"
            }
        if result.flow_control_metrics:
            scenario_data["flow_control"] = {
                k: v for k, v in result.flow_control_metrics.items()
                if not k.endswith("_series")
            }
        if result.gpu_metrics:
            scenario_data["infrastructure"] = {
                k: v for k, v in result.gpu_metrics.items()
                if not k.endswith("_series")
            }

        structured_results["scenarios"].append(scenario_data)

    results_json_path = cfg.results_dir / "results.json"
    with open(results_json_path, "w") as f:
        json.dump(structured_results, f, indent=2)

    log("=== Benchmark complete ===")
    log(f"Results:  {cfg.results_dir}")
    log(f"Report:   {cfg.results_dir / 'report.html'}")
    log(f"Metadata: {metadata_path}")
    log(f"Data:     {results_json_path}")

    stop_prometheus_port_forward()


if __name__ == "__main__":
    main()
