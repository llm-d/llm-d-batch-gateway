# Scripts

## Development


| Script          | Description                                                                                                                                      |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `dev-deploy.sh` | Local development deployment. Builds images, creates kind cluster, installs Redis/PostgreSQL/Jaeger, deploys batch-gateway with TLS and tracing. |


## Release


| Script                  | Description                                                                                                                                               |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `generate-release.sh`   | Creates a `v*.*.*` tag on GitHub from a given commit SHA via the `gh` CLI, plus a `release-<tag>` branch for final releases (no local git changes). Triggers the Release workflow. See [managing releases](../docs/guides/managing-releases.md). |
| `publish-helm-chart.sh` | Packages the helm chart for a tag and pushes it to `oci://ghcr.io/llm-d/charts` (invoked as `make publish-helm-chart` in release CI).          |


## Migration


| Script                           | Description |
| -------------------------------- | ----------- |
| `migrate-redis-to-postgresql.py` | Copies batch and file records from Redis or Valkey to PostgreSQL, for deployments that used the Redis record store (releases up to v0.5.0) and are moving to a release that only reads PostgreSQL (v0.6.0 and later). |

### Migrating job records from Redis to PostgreSQL

The gateway does not move existing records when you switch the record store. Without this step, batches and files created before the switch are no longer found through the API, and their objects stay in file storage because the garbage collector only removes files it has a record for.

The script does not create tables: it copies into the tables the new gateway creates when it starts on PostgreSQL, and first checks that they can hold the Redis records. The gateway must be stopped while the records are copied, so that nothing changes the records on either side during the copy.

1. Let running batches finish, or cancel them. A batch that has not reached a terminal state (`completed`, `failed`, `expired` or `cancelled`) when the old gateway stops is not copied, because the new gateway would run it again from the start; the script lists these batches.
2. Upgrade to v0.6.0 or later on PostgreSQL (`global.dbClient.type: postgresql`, with a `postgresql-url` key in the secret) and let the gateway start once, so it creates its tables. Keep the Redis or Valkey instance: the script reads the records from it.
3. Stop the API server, the processor and the garbage collector, and wait for their pods to go. With the Helm chart, set `apiserver.replicaCount`, `processor.replicaCount` and `gc.replicaCount` to `0`.
4. Check the tables and the records without writing anything. Pass the URLs as environment variables so the passwords do not show up in the process list:

    ```bash
    export REDIS_URL='redis://:<password>@<redis-host>:6379'
    export POSTGRESQL_URL='postgres://<user>:<password>@<postgres-host>:5432/<database>'
    uv run scripts/migrate-redis-to-postgresql.py --redis-db <db> --dry-run
    ```

    `--redis-db` must be the old gateway's `global.dbClient.redis.db` (default `0`). The gateway used that number, not the one in `redis-url`, and so does the script. If the script finds no records at all, it stops with an error, which usually means the number is wrong.

5. Run it again without `--dry-run`.
6. Start the API server, the processor and the garbage collector again. Do not run the script after this: it would bring back records that users have deleted since.

Without `uv`: `pip install "redis>=5" "psycopg[binary]>=3.1"`, then run the script with `python3`. The URLs can also be passed as `--redis-url` and `--postgresql-url`. The script needs Redis 6.0 or later, or Valkey.

What to expect:

- The script only reads from Redis and never overwrites a row that is already in PostgreSQL, so it is safe to run again while the gateway is stopped.
- It writes nothing if the gateway's tables are missing or cannot hold the records (a column is missing or has another type, or a newer gateway added a required column the Redis records have no value for), if a record cannot be stored as it is (malformed, or JSON containing a NUL character, which PostgreSQL does not accept), if an existing PostgreSQL row differs from Redis, or if a record changes in Redis while it runs (an old gateway component is still running). A record whose Redis lifetime runs out during the copy does not count as a change.
- `--dry-run` checks the tables and the Redis records. It does not compare the records with rows that are already in PostgreSQL, so a run without `--dry-run` can still stop on a row that differs.
- The Redis record store expires a record 60 days after it is created, so older records are no longer in Redis and cannot be migrated. After the move, records follow their own `expiry` instead; a record without one (`0`) is kept until it is deleted.
