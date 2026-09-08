# Database Schema Migrations

The PostgreSQL schema is owned by the `migrate` package
(`internal/database/postgresql/migrate`). The apiserver, processor, and gc
never run DDL. On startup they read the `schema_migrations` table and refuse
to start if it is missing or behind the version they were built with.

## How the schema gets applied

Every binary has a `migrate` subcommand:

```bash
batch-gateway-apiserver migrate                      # URL from /etc/.secrets/postgresql-url
batch-gateway-gc migrate --postgresql-url 'postgres://user:pass@host:5432/batch'
```

The Helm chart renders it as an initContainer on each Deployment when
`global.dbClient.type` is `postgresql`, using the pod's own image. The
operator renders the same chart, so both install paths get it without any
extra configuration.

A run takes a transaction-scoped advisory lock before it does anything else,
creates `schema_migrations` if needed, applies every migration newer than the
recorded version, and commits. Concurrent runs (several replicas starting at
once, or several pods in a rollout) queue on the lock; the first one applies
the migrations and the rest find nothing to do. A failed migration rolls back
completely, so the database is never left half-migrated.

Upgrading a deployment that predates this mechanism needs nothing special.
Migration `0001` uses `IF NOT EXISTS`, so it adopts the existing tables and
records version 1.

## Writing a migration

1. Add `internal/database/postgresql/migrate/migrations/NNNN_short_name.sql`.
   `NNNN` is the next number in sequence, zero-padded to four digits. The name
   is lowercase letters, digits, and underscores. `TestLoad` fails on gaps,
   duplicates, or misnamed files.
2. Keep it transactional. The whole run happens in one transaction, so
   `CREATE INDEX CONCURRENTLY` and other non-transactional statements are not
   allowed.
3. Keep it compatible with the previous release. Rolling updates run the new
   pods' migration while old pods are still serving, and a rollback runs old
   binaries against the new schema. Expand first (add columns, add tables,
   backfill), and contract (drop, rename, tighten constraints) only in a later
   release once nothing reads the old shape.
4. Write plain DDL, no `IF NOT EXISTS` guards. Migration `0001` uses them only
   to adopt databases created before versioning existed. From `0002` on, the
   version table is authoritative and a statement that finds its object
   already present is a bug worth failing on.
5. Bump nothing else. The expected version is the number of embedded files.

## Running locally

Point `TEST_POSTGRESQL_URL` at a disposable PostgreSQL to run the package tests
against a real database:

```bash
podman run -d --name pg -e POSTGRES_PASSWORD=pg -p 5432:5432 docker.io/library/postgres:16
TEST_POSTGRESQL_URL='postgres://postgres:pg@localhost:5432/postgres?sslmode=disable' \
    go test ./internal/database/postgresql/migrate/
```

Without the variable the database-backed tests skip. The e2e suite
(`make test-e2e`) checks the deployed database version and runs the
subcommand concurrently against a fresh database inside the cluster.
