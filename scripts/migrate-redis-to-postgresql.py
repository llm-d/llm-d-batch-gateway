#!/usr/bin/env python3
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
#
# /// script
# requires-python = ">=3.10"
# dependencies = ["redis>=5", "psycopg[binary]>=3.1"]
# ///
"""Copy Batch Gateway job records from Redis or Valkey to PostgreSQL.

Batch Gateway releases up to v0.5.0 can keep their batch and file records in
Redis or Valkey; from v0.6.0 the gateway reads them from PostgreSQL only. Run
this after the gateway has been switched to PostgreSQL, has started once (it
creates its own tables) and has been stopped again. That can be before the
upgrade to v0.6.0 or after it. The script does not create tables: it checks
that the gateway's tables can hold the Redis records and copies nothing if
they cannot. See scripts/README.md for the procedure.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse

KEY_PREFIX = "llmd_batch:store:"
RECORD_VERSION = "1"
TERMINAL_BATCH_STATUSES = {"completed", "failed", "expired", "cancelled"}

# The columns the Redis records fill, with their PostgreSQL types. The tables
# themselves are created by the gateway, so a newer gateway may have more.
COLUMN_TYPES = {
    "id": "text",
    "tenant_id": "text",
    "expiry": "bigint",
    "tags": "jsonb",
    "purpose": "text",
    "spec": "jsonb",
    "status": "jsonb",
}
TABLES = {"batch": "batch_items", "file": "file_items"}
COLUMNS = {
    "batch": ("id", "tenant_id", "expiry", "tags", "spec", "status"),
    "file": ("id", "tenant_id", "expiry", "tags", "purpose", "spec", "status"),
}
JSON_COLUMNS = {"tags", "spec", "status"}


class RecordError(ValueError):
    """A Redis record that cannot be copied as it is."""


def to_row(item_type, key, record):
    """Turn one Redis hash into the PostgreSQL row the gateway would have written."""
    version = record.get("ver")
    if version != RECORD_VERSION:
        raise RecordError(f"{key}: record version {version!r} is not supported (expected {RECORD_VERSION!r})")
    item_id = record.get("ID") or ""
    if key != f"{KEY_PREFIX}{item_type}:{item_id}":
        raise RecordError(f"{key}: record ID {item_id!r} does not match its key")
    if not record.get("tenantID"):
        raise RecordError(f"{key}: record has no tenant ID")
    try:
        expiry = int(record.get("expiry") or 0)
    except ValueError as error:
        raise RecordError(f"{key}: expiry {record.get('expiry')!r} is not a number") from error
    row = {
        "id": item_id,
        "tenant_id": record["tenantID"],
        "expiry": expiry,
        # The gateway stores an item without tags as an empty object.
        "tags": record.get("tags") or "{}",
        "spec": record.get("spec") or None,
        "status": record.get("status") or None,
    }
    if item_type == "file":
        row["purpose"] = record.get("purpose") or None
    # PostgreSQL rejects a NUL character in text as well as in jsonb.
    for column in ("id", "tenant_id", "purpose"):
        if "\x00" in (row.get(column) or ""):
            raise RecordError(f"{key}: {column} contains a NUL character, which PostgreSQL cannot store")
    for column in JSON_COLUMNS:
        if row[column] is not None:
            try:
                value = json.loads(row[column])
            except ValueError as error:
                raise RecordError(f"{key}: {column} is not valid JSON") from error
            # PostgreSQL jsonb rejects \u0000, which JSON itself allows.
            if contains_nul(value):
                raise RecordError(f"{key}: {column} contains a NUL character, which PostgreSQL cannot store")
    return row


def contains_nul(value):
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(contains_nul(k) or contains_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(contains_nul(v) for v in value)
    return False


def batch_status(row):
    if row["status"] is None:
        return None
    status = json.loads(row["status"])
    return status.get("status") if isinstance(status, dict) else None


def read_records(client, item_type):
    """Yield (key, (hash, ttl in ms)) for every record of one type."""
    for key in client.scan_iter(match=f"{KEY_PREFIX}{item_type}:*", count=500, _type="hash"):
        # Lifetime first: read after the hash, a record that expires in between
        # would report "no such key" and look deleted rather than expired.
        ttl = client.pttl(key)
        record = client.hgetall(key)
        if record:  # an expired key can vanish between SCAN and HGETALL
            yield key, (record, ttl)


def read_all(client):
    """Return {item type: {key: (record, ttl in ms)}} for every batch and file record."""
    return {item_type: dict(read_records(client, item_type)) for item_type in TABLES}


def changed_since(before, after, elapsed_ms):
    """Keys whose records were added, changed or removed between two reads.

    The record store sets a lifetime on every record, so a record whose
    lifetime ran out between the reads is expected to be gone and does not
    count as a change.
    """
    changed = []
    for item_type, records in before.items():
        later = after[item_type]
        for key, (record, ttl) in records.items():
            if key in later:
                if later[key][0] != record:
                    changed.append(key)
            elif not 0 <= ttl <= elapsed_ms:
                changed.append(key)
        changed += [key for key in later if key not in records]
    return changed


def schema_problems(connection):
    """Why the gateway's tables cannot hold the Redis records, if they cannot."""
    problems = []
    with connection.cursor() as cursor:
        for item_type, table in TABLES.items():
            cursor.execute(
                "SELECT column_name, data_type, is_nullable = 'NO' AND column_default IS NULL"
                " FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s",
                [table],
            )
            found = {name: (data_type, required) for name, data_type, required in cursor.fetchall()}
            if not found:
                problems.append(f"table {table} does not exist; start the gateway on PostgreSQL once so it creates its tables, then stop it")
                continue
            for column in COLUMNS[item_type]:
                if column not in found:
                    problems.append(f"{table}.{column} does not exist")
                elif found[column][0] != COLUMN_TYPES[column]:
                    problems.append(f"{table}.{column} is {found[column][0]}, expected {COLUMN_TYPES[column]}")
            for column, (_, required) in found.items():
                if required and column not in COLUMNS[item_type]:
                    problems.append(f"{table}.{column} is required but the Redis records have no value for it")
    return problems


def migrate(client, connection, *, dry_run, report, clock=time.monotonic):
    started = clock()
    source = read_all(client)
    if problems := schema_problems(connection):
        for problem in problems:
            report(f"ERROR: {problem}")
        report("The gateway's tables cannot hold these records; nothing was written")
        return 1
    if not any(source.values()):
        report("ERROR: no batch or file records found in Redis")
        report("Check that --redis-db matches the gateway's dbClient.redis.db; nothing was written")
        return 1
    rows = {"batch": [], "file": []}
    problems = []
    for item_type, records in source.items():
        for key, (record, _) in records.items():
            try:
                rows[item_type].append(to_row(item_type, key, record))
            except RecordError as error:
                problems.append(str(error))
    if problems:
        for problem in problems:
            report(f"ERROR: {problem}")
        report(f"{len(problems)} record(s) cannot be copied; nothing was written")
        return 1

    # A batch still running when the old gateway stopped can never finish: the
    # new gateway would run it again from the start, so it is left out.
    unfinished = sorted(row["id"] for row in rows["batch"] if batch_status(row) not in TERMINAL_BATCH_STATUSES)
    if unfinished:
        report(f"WARNING: {len(unfinished)} batch(es) had not finished and are not copied: {', '.join(unfinished)}")
        rows["batch"] = [row for row in rows["batch"] if batch_status(row) in TERMINAL_BATCH_STATUSES]

    failed = False
    for item_type, items in rows.items():
        table, columns = TABLES[item_type], COLUMNS[item_type]
        if dry_run:
            report(f"{table}: {len(items)} record(s) in Redis; dry run, nothing written")
            continue
        values = ["%s::jsonb" if column in JSON_COLUMNS else "%s" for column in columns]
        insert = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)}) ON CONFLICT (id) DO NOTHING"
        # PostgreSQL compares the existing row, so jsonb equality rules apply (true is not 1, 1 is 1.0).
        differs = f"SELECT 1 FROM {table} WHERE id = %s AND ({', '.join(columns[1:])}) IS DISTINCT FROM ({', '.join(values[1:])})"
        copied, differing = 0, []
        with connection.cursor() as cursor:
            for row in items:
                params = [row[column] for column in columns]
                cursor.execute(insert, params)
                if cursor.rowcount:
                    copied += 1
                    continue
                cursor.execute(differs, params)
                if cursor.fetchone():
                    differing.append(row["id"])
        report(f"{table}: {len(items)} in Redis, {copied} copied, {len(items) - copied} already in PostgreSQL")
        if differing:
            failed = True
            report(f"ERROR: {table}: {len(differing)} existing row(s) differ from Redis and were left as they are: {', '.join(sorted(differing)[:10])}")
    if failed:
        connection.rollback()
        report("Nothing was written")
        return 1
    if not dry_run:
        changed = changed_since(source, read_all(client), (clock() - started) * 1000)
        if changed:
            connection.rollback()
            report(f"ERROR: {len(changed)} record(s) in Redis changed while they were being copied, so the gateway is still running: {', '.join(sorted(changed)[:10])}")
            report("Stop the API server, processor and garbage collector, then run this again; nothing was written")
            return 1
    connection.commit()
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL"), help="Redis or Valkey URL holding the job records (default: $REDIS_URL)")
    parser.add_argument(
        "--redis-db",
        type=int,
        default=0,
        help="Redis database number to read, replacing any number in the URL, as the gateway does with its dbClient.redis.db setting (default: 0, the chart default)",
    )
    parser.add_argument("--postgresql-url", default=os.environ.get("POSTGRESQL_URL"), help="PostgreSQL URL the gateway will use (default: $POSTGRESQL_URL)")
    parser.add_argument("--dry-run", action="store_true", help="Check the records and report counts without writing to PostgreSQL")
    args = parser.parse_args()
    if not args.redis_url or not args.postgresql_url:
        parser.error("--redis-url and --postgresql-url (or REDIS_URL and POSTGRESQL_URL) are required")

    def report(message):
        print(message, file=sys.stderr if message.startswith("ERROR") else sys.stdout)

    # Imported here so the record handling can be tested without the drivers.
    import psycopg
    import redis

    try:
        with (
            redis.Redis.from_url(with_db(args.redis_url, args.redis_db), decode_responses=True) as client,
            psycopg.connect(args.postgresql_url) as connection,
        ):
            return migrate(client, connection, dry_run=args.dry_run, report=report)
    except (redis.RedisError, psycopg.Error) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except UnicodeDecodeError as error:
        print(f"ERROR: a record in Redis is not valid UTF-8 and cannot be copied: {error}", file=sys.stderr)
        return 1


def with_db(url, db):
    """Return url with its database number replaced by db.

    redis-py takes the number from a db query parameter before the path, so
    that parameter is removed too.
    """
    parts = urllib.parse.urlsplit(url)
    query = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True) if k != "db"]
    return urllib.parse.urlunsplit(parts._replace(path=f"/{db}", query=urllib.parse.urlencode(query)))


if __name__ == "__main__":
    sys.exit(main())
