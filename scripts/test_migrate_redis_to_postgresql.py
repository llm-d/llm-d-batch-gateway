# Copyright 2026 The llm-d Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import json
import os
import unittest
import uuid
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("migrate", Path(__file__).with_name("migrate-redis-to-postgresql.py"))
migrate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migrate)


def record(item_type, item_id, *, status="completed", **fields):
    values = {
        "ver": "1",
        "ID": item_id,
        "tenantID": "tenant-a",
        "expiry": "1790000000",
        "tags": "",
        "status": json.dumps({"status": status}),
        "spec": json.dumps({"name": item_id}),
    }
    if item_type == "file":
        values["purpose"] = "batch"
    values.update(fields)
    return f"llmd_batch:store:{item_type}:{item_id}", values


class FakeRedis:
    """The calls the script makes, over a dict of hashes with lifetimes in ms."""

    def __init__(self, *records, ttl=60 * 24 * 3600 * 1000):
        self.hashes = dict(records)
        self.ttls = {key: ttl for key in self.hashes}
        self.scans = 0
        self.on_second_read = None

    def scan_iter(self, match, count, _type):
        self.scans += 1
        if self.scans == 3 and self.on_second_read:  # two scans per read: batches, then files
            self.on_second_read(self)
        prefix = match.rstrip("*")
        return [key for key in list(self.hashes) if key.startswith(prefix)]

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def pttl(self, key):
        return self.ttls.get(key, -2)


class FakePostgres:
    """Rows by table, with INSERT ... ON CONFLICT DO NOTHING and commit/rollback.

    The tables have the gateway's columns unless a test changes self.columns.
    The "does the existing row differ" SELECT compares values as plain strings;
    RealPostgresTest checks it with PostgreSQL's own jsonb comparison.
    """

    def __init__(self, **tables):
        self.committed = {"batch_items": {}, "file_items": {}}
        self.committed.update(tables)
        self.pending = {name: dict(rows) for name, rows in self.committed.items()}
        self.columns = {
            table: [(column, migrate.COLUMN_TYPES[column], column in ("id", "tenant_id")) for column in migrate.COLUMNS[item_type]]
            for item_type, table in migrate.TABLES.items()
        }
        self.rowcount = 0
        self.result = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        if "information_schema" in sql:
            self.result = self.columns.get(params[0], [])
            return
        table = "batch_items" if "batch_items" in sql else "file_items"
        columns = migrate.COLUMNS["batch" if table == "batch_items" else "file"]
        row = dict(zip(columns, params or []))
        if sql.startswith("INSERT"):
            self.rowcount = 0 if row["id"] in self.pending[table] else 1
            self.pending[table].setdefault(row["id"], row)
        elif sql.startswith("SELECT"):
            self.result = (1,) if self.pending[table].get(row["id"]) != row else None

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result

    def commit(self):
        self.committed = {name: dict(rows) for name, rows in self.pending.items()}

    def rollback(self):
        self.pending = {name: dict(rows) for name, rows in self.committed.items()}


class Clock:
    """A monotonic clock that advances one second per call."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        self.now += 1.0
        return self.now


def run(client, connection, **options):
    messages = []
    options = {"dry_run": False, "clock": Clock(), **options}
    return migrate.migrate(client, connection, report=messages.append, **options), messages


class ToRowTest(unittest.TestCase):
    def test_row_matches_what_the_gateway_stores_in_postgresql(self):
        key, values = record("file", "file_1", tags='{"batch_id": "batch_1"}', purpose="batch_output")
        self.assertEqual(
            migrate.to_row("file", key, values),
            {
                "id": "file_1",
                "tenant_id": "tenant-a",
                "expiry": 1790000000,
                "tags": '{"batch_id": "batch_1"}',
                "purpose": "batch_output",
                "spec": '{"name": "file_1"}',
                "status": '{"status": "completed"}',
            },
        )

    def test_item_without_tags_is_stored_as_an_empty_object(self):
        key, values = record("batch", "batch_1")
        self.assertEqual(migrate.to_row("batch", key, values)["tags"], "{}")

    def test_records_that_cannot_be_copied_faithfully_are_rejected(self):
        cases = {
            "unknown version": {"ver": "2"},
            "ID differs from the key": {"ID": "batch_other"},
            "no tenant": {"tenantID": ""},
            "expiry is not a number": {"expiry": "soon"},
            "spec is not JSON": {"spec": "{"},
            "NUL in a JSON string": {"spec": '{"metadata": {"note": "a\\u0000b"}}'},
            "NUL in a JSON key": {"tags": '{"k\\u0000": "v"}'},
            "NUL in the tenant ID": {"tenantID": "tenant\x00a"},
        }
        for name, fields in cases.items():
            with self.subTest(name):
                key, values = record("batch", "batch_1", **fields)
                with self.assertRaises(migrate.RecordError):
                    migrate.to_row("batch", key, values)

    def test_escaped_backslash_before_u0000_is_not_a_nul(self):
        # The JSON text \\u0000 is a backslash followed by "u0000", which PostgreSQL stores fine.
        key, values = record("batch", "batch_1", spec='{"prompt": "escape it as \\\\u0000"}')
        self.assertEqual(migrate.to_row("batch", key, values)["spec"], values["spec"])


class MigrateTest(unittest.TestCase):
    def test_copies_batches_and_files_and_is_safe_to_run_again(self):
        client = FakeRedis(record("batch", "batch_1"), record("batch", "batch_2", status="failed"), record("file", "file_1"))
        postgres = FakePostgres()

        code, _ = run(client, postgres)
        self.assertEqual(code, 0)
        self.assertEqual(sorted(postgres.committed["batch_items"]), ["batch_1", "batch_2"])
        self.assertEqual(postgres.committed["file_items"]["file_1"]["purpose"], "batch")

        before = json.dumps(postgres.committed, sort_keys=True)
        code, messages = run(client, postgres)
        self.assertEqual(code, 0)
        self.assertEqual(json.dumps(postgres.committed, sort_keys=True), before)
        self.assertIn("batch_items: 2 in Redis, 0 copied, 2 already in PostgreSQL", messages)

    def test_dry_run_writes_nothing(self):
        postgres = FakePostgres()
        code, _ = run(FakeRedis(record("batch", "batch_1")), postgres, dry_run=True)
        self.assertEqual(code, 0)
        self.assertEqual(postgres.committed["batch_items"], {})

    def test_unfinished_batch_is_reported_and_left_out(self):
        client = FakeRedis(record("batch", "batch_done"), record("batch", "batch_running", status="in_progress"))
        postgres = FakePostgres()

        code, messages = run(client, postgres)
        self.assertEqual(code, 0)
        self.assertEqual(sorted(postgres.committed["batch_items"]), ["batch_done"])
        self.assertTrue(any(m.startswith("WARNING") and "batch_running" in m for m in messages))

    def test_empty_redis_is_an_error_not_a_successful_copy(self):
        # Usually the wrong database number: the gateway's records are elsewhere.
        code, messages = run(FakeRedis(), FakePostgres())
        self.assertEqual(code, 1)
        self.assertTrue(any("--redis-db" in message for message in messages))

    def test_record_whose_lifetime_ends_during_the_copy_is_not_a_change(self):
        client = FakeRedis(record("batch", "batch_1"), record("file", "file_1"))
        client.ttls["llmd_batch:store:file:file_1"] = 500  # expires before the second read
        client.on_second_read = lambda c: c.hashes.pop("llmd_batch:store:file:file_1")
        postgres = FakePostgres()
        code, _ = run(client, postgres)
        self.assertEqual(code, 0)
        self.assertEqual(sorted(postgres.committed["file_items"]), ["file_1"])

    def test_record_that_expires_right_after_it_is_read_is_not_a_change(self):
        key = "llmd_batch:store:file:file_1"

        class ExpiringRedis(FakeRedis):
            def hgetall(self, name):
                values = super().hgetall(name)
                if name == key:  # gone before anything else can be asked about it
                    self.hashes.pop(name, None)
                    self.ttls.pop(name, None)
                return values

        client = ExpiringRedis(record("batch", "batch_1"), record("file", "file_1"))
        client.ttls[key] = 500
        postgres = FakePostgres()
        code, _ = run(client, postgres)
        self.assertEqual(code, 0)
        self.assertEqual(sorted(postgres.committed["file_items"]), ["file_1"])

    def test_record_that_vanishes_between_scan_and_read_is_skipped(self):
        key = "llmd_batch:store:file:file_1"

        class VanishingRedis(FakeRedis):
            def hgetall(self, name):
                if name == key:  # expired after SCAN listed it
                    self.hashes.pop(name, None)
                return super().hgetall(name)

        postgres = FakePostgres()
        code, _ = run(VanishingRedis(record("batch", "batch_1"), record("file", "file_1")), postgres)
        self.assertEqual(code, 0)
        self.assertEqual((sorted(postgres.committed["batch_items"]), postgres.committed["file_items"]), (["batch_1"], {}))

    def test_one_bad_record_stops_the_whole_copy(self):
        postgres = FakePostgres()
        code, _ = run(FakeRedis(record("batch", "batch_ok"), record("file", "file_bad", ver="2")), postgres)
        self.assertEqual(code, 1)
        self.assertEqual(postgres.committed, {"batch_items": {}, "file_items": {}})

    def test_tables_that_cannot_hold_the_records_stop_the_copy(self):
        def drop(columns, name):
            return [c for c in columns if c[0] != name]

        cases = {
            "no table": lambda t: t.update(batch_items=[]),
            "missing column": lambda t: t.update(file_items=drop(t["file_items"], "purpose")),
            "different type": lambda t: t.update(batch_items=[(n, "text" if n == "expiry" else ty, r) for n, ty, r in t["batch_items"]]),
            "new required column": lambda t: t["batch_items"].append(("owner", "text", True)),
        }
        for name, change in cases.items():
            with self.subTest(name):
                postgres = FakePostgres()
                change(postgres.columns)
                for dry_run in (True, False):
                    code, messages = run(FakeRedis(record("batch", "batch_1"), record("file", "file_1")), postgres, dry_run=dry_run)
                    self.assertEqual(code, 1)
                    self.assertEqual(postgres.committed, {"batch_items": {}, "file_items": {}})
                    self.assertTrue(any("cannot hold these records" in message for message in messages))

    def test_new_optional_column_is_fine(self):
        postgres = FakePostgres()
        postgres.columns["batch_items"].append(("note", "text", False))
        self.assertEqual(run(FakeRedis(record("batch", "batch_1")), postgres)[0], 0)

    def test_record_changed_during_the_copy_means_the_gateway_is_running(self):
        changes = {
            "upload": lambda c: c.hashes.update([record("file", "file_late")]),
            "delete": lambda c: c.hashes.pop("llmd_batch:store:file:file_1"),
            "update": lambda c: c.hashes["llmd_batch:store:batch:batch_1"].update(status='{"status": "expired"}'),
        }
        for name, change in changes.items():
            with self.subTest(name):
                client = FakeRedis(record("batch", "batch_1"), record("file", "file_1"))
                client.on_second_read = change
                postgres = FakePostgres()
                code, messages = run(client, postgres)
                self.assertEqual(code, 1)
                self.assertEqual(postgres.committed, {"batch_items": {}, "file_items": {}})
                self.assertTrue(any("still running" in message for message in messages))


try:
    import redis
except ImportError:
    redis = None


class WithDbTest(unittest.TestCase):
    URLS = [
        "redis://:pw@redis:6379/0",
        "rediss://user:pw@redis:6380",
        "redis://redis:6379/5?protocol=3",
        "redis://redis:6379/0?db=0",
        "redis://redis:6379?db=5&protocol=3",
    ]

    def test_database_number_replaces_the_one_in_the_url(self):
        self.assertEqual(migrate.with_db("redis://redis:6379/5?db=5&protocol=3", 2), "redis://redis:6379/2?protocol=3")

    @unittest.skipUnless(redis, "needs redis-py")
    def test_redis_client_uses_the_given_database(self):
        for url in self.URLS:
            with self.subTest(url):
                pool = redis.Redis.from_url(migrate.with_db(url, 2)).connection_pool
                self.assertEqual(pool.connection_kwargs["db"], 2)
                self.assertEqual(pool.connection_kwargs.get("password"), redis.Redis.from_url(url).connection_pool.connection_kwargs.get("password"))


try:
    import psycopg
except ImportError:
    psycopg = None

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")


@unittest.skipUnless(psycopg and POSTGRES_URL, "needs psycopg and TEST_POSTGRES_URL")
class RealPostgresTest(unittest.TestCase):
    """The schema check, INSERT, conflict handling and rollback against a real
    PostgreSQL, with the tables created from the gateway's own schema files."""

    SCHEMA_DIR = Path(__file__).resolve().parent.parent / "internal" / "database" / "postgresql"

    def setUp(self):
        self.schema = f"migrate_test_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(POSTGRES_URL, autocommit=True) as admin:
            admin.execute(f"CREATE SCHEMA {self.schema}")
        self.addCleanup(self.drop_schema)
        with self.sql() as gateway:
            for name in ("batch_schema.sql", "file_schema.sql"):
                gateway.execute((self.SCHEMA_DIR / name).read_text())
        self.connection = self.sql()
        self.addCleanup(self.connection.close)

    def sql(self):
        return psycopg.connect(POSTGRES_URL, options=f"-c search_path={self.schema}")

    def drop_schema(self):
        with psycopg.connect(POSTGRES_URL, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {self.schema} CASCADE")

    def ids(self, table):
        with self.sql() as reader:
            return sorted(row[0] for row in reader.execute(f"SELECT id FROM {table}"))

    def assert_schema_stops_the_copy(self, ddl):
        with self.sql() as gateway:
            gateway.execute(ddl)
        code, messages = run(FakeRedis(record("file", "file_1")), self.connection)
        self.assertEqual(code, 1)
        self.assertEqual(self.ids("file_items"), [])
        return messages

    def test_missing_table_stops_the_copy(self):
        messages = self.assert_schema_stops_the_copy("DROP TABLE batch_items CASCADE")
        self.assertTrue(any("start the gateway on PostgreSQL once" in message for message in messages))

    def test_new_required_column_stops_the_copy(self):
        self.assert_schema_stops_the_copy("ALTER TABLE file_items ADD COLUMN owner TEXT NOT NULL")

    def test_copies_and_a_second_run_changes_nothing(self):
        client = FakeRedis(record("batch", "batch_1"), record("file", "file_1", tags='{"k": "v"}'))
        self.assertEqual(run(client, self.connection)[0], 0)
        self.assertEqual((self.ids("batch_items"), self.ids("file_items")), (["batch_1"], ["file_1"]))

        code, messages = run(client, self.connection)
        self.assertEqual(code, 0)
        self.assertIn("file_items: 1 in Redis, 0 copied, 1 already in PostgreSQL", messages)

    def test_existing_row_that_differs_is_kept_and_the_rest_is_rolled_back(self):
        key, values = record("batch", "batch_1")
        self.assertEqual(run(FakeRedis((key, values)), self.connection)[0], 0)
        with psycopg.connect(POSTGRES_URL, options=f"-c search_path={self.schema}") as writer:
            writer.execute("UPDATE batch_items SET status = %s WHERE id = 'batch_1'", ['{"status": "failed"}'])

        code, _ = run(FakeRedis((key, values), record("batch", "batch_2"), record("file", "file_1")), self.connection)
        self.assertEqual(code, 1)
        self.assertEqual((self.ids("batch_items"), self.ids("file_items")), (["batch_1"], []))
        with psycopg.connect(POSTGRES_URL, options=f"-c search_path={self.schema}") as reader:
            status = reader.execute("SELECT status->>'status' FROM batch_items WHERE id = 'batch_1'").fetchone()[0]
        self.assertEqual(status, "failed")

    def test_existing_row_is_compared_as_jsonb_compares_it(self):
        cases = {
            "true and 1": ("true", "1", 1),
            "numbers a float would round to the same value": ("9007199254740992.0", "9007199254740993.0", 1),
            "1 and 1.0": ("1", "1.0", 0),
        }
        for name, (in_redis, in_postgres, expected) in cases.items():
            with self.subTest(name):
                key, values = record("batch", f"batch_{uuid.uuid4().hex[:8]}", spec='{"value": %s}' % in_redis)
                self.assertEqual(run(FakeRedis((key, values)), self.connection)[0], 0)
                with psycopg.connect(POSTGRES_URL, options=f"-c search_path={self.schema}") as writer:
                    writer.execute("UPDATE batch_items SET spec = %s::jsonb WHERE id = %s", ['{"value": %s}' % in_postgres, values["ID"]])

                self.assertEqual(run(FakeRedis((key, values)), self.connection)[0], expected)

    def test_change_during_the_copy_rolls_everything_back(self):
        client = FakeRedis(record("batch", "batch_1"), record("file", "file_1"))
        client.on_second_read = lambda c: c.hashes.update([record("file", "file_late")])
        self.assertEqual(run(client, self.connection)[0], 1)
        self.assertEqual((self.ids("batch_items"), self.ids("file_items")), ([], []))


if __name__ == "__main__":
    unittest.main()
