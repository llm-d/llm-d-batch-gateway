import unittest
from unittest import mock

from benchmarks import benchmark


class FlowControlMetricsTest(unittest.TestCase):
    def test_metric_families_in_shared_prometheus(self):
        namespace = "batch-bench-s4"
        selector = f'{{namespace="{namespace}"}}'
        queries = {
            prefix: [
                f"avg({prefix}_flow_control_pool_saturation{selector})",
                f"sum by (priority) ({prefix}_flow_control_queue_size{selector})",
                f"sum({prefix}_flow_control_queue_size{selector})",
            ]
            for prefix in ("llm_d_epp", "inference_extension")
        }
        current = queries["llm_d_epp"]
        legacy = queries["inference_extension"]

        def series(value):
            return [{"values": [[0, str(value)]]}]

        for current_in_namespace in (True, False):
            with self.subTest(current_in_namespace=current_in_namespace):
                # Both families exist in the shared Prometheus. In the fallback
                # case, the current family is only in another namespace.
                responses = {
                    current[0]: series(0.25) if current_in_namespace else [],
                    current[1]: (
                        [{"metric": {"priority": "-1"}, "values": [[0, "3"]]}]
                        if current_in_namespace else []
                    ),
                    current[2]: series(4) if current_in_namespace else [],
                    legacy[0]: series(0.75),
                    legacy[1]: [{"metric": {"priority": "-1"}, "values": [[0, "30"]]}],
                    legacy[2]: series(40),
                }
                with mock.patch.object(benchmark, "query_prometheus") as query:
                    query.side_effect = lambda _ctx, _ns, promql, _start, _end: responses[promql]
                    metrics = benchmark.collect_flow_control_metrics(
                        "test-context", namespace, mock.sentinel.start, mock.sentinel.end
                    )

                selected = current if current_in_namespace else legacy
                expected_queries = selected if current_in_namespace else [current[0], *legacy]
                self.assertEqual(expected_queries, [call.args[2] for call in query.call_args_list])
                self.assertEqual(
                    0.25 if current_in_namespace else 0.75,
                    metrics["flow_control_saturation_avg"],
                )
                self.assertEqual(
                    3 if current_in_namespace else 30,
                    metrics["queue_size_priority_-1_avg"],
                )
                self.assertEqual(
                    4 if current_in_namespace else 40,
                    metrics["flow_control_queue_size_avg"],
                )

    @mock.patch.object(benchmark, "query_prometheus", return_value=[])
    def test_defaults_to_current_family_when_saturation_is_missing(self, query):
        benchmark.collect_flow_control_metrics(
            "test-context", "batch-bench-s4", mock.sentinel.start, mock.sentinel.end
        )
        self.assertEqual(
            [
                'avg(llm_d_epp_flow_control_pool_saturation{namespace="batch-bench-s4"})',
                'avg(inference_extension_flow_control_pool_saturation{namespace="batch-bench-s4"})',
                'sum by (priority) (llm_d_epp_flow_control_queue_size{namespace="batch-bench-s4"})',
                'sum(llm_d_epp_flow_control_queue_size{namespace="batch-bench-s4"})',
            ],
            [call.args[2] for call in query.call_args_list],
        )
