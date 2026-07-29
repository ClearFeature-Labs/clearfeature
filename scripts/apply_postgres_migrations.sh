#!/usr/bin/env bash
#
# Apply infra/postgres/*.sql to the running Compose Postgres, in filename order.
#
# Safe to re-run: every migration uses CREATE/ALTER ... IF NOT EXISTS (verified per file
# before this helper was added). Use this when a Postgres VOLUME predates a migration —
# the compose initdb mount applies schemas only on the FIRST init of an EMPTY volume.
#
# Usage:
#   docker compose up -d --wait postgres
#   bash scripts/apply_postgres_migrations.sh

set -euo pipefail

cd "$(dirname "$0")/.."

if ! docker compose ps -q postgres | grep -q .; then
    echo "postgres container is not running; start it first:" >&2
    echo "  docker compose up -d --wait postgres" >&2
    exit 1
fi

for migration in infra/postgres/*.sql; do
    echo "==> applying ${migration}"
    docker compose exec -T postgres \
        sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
        < "$migration"
done

echo "All migrations applied (idempotent — re-running is a no-op)."
