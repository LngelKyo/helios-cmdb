#!/usr/bin/env bash
# Restore helios_cmdb from a pg_dump backup file.
#
# Usage: ./ops/restore.sh <backup.dump> [target_db_url]
#
# What this script does:
#   1. Parses target DB URL (defaults to CMDB_DATABASE_URL).
#   2. Connects to the parent database (postgres) and DROPs + CREATEs the
#      target database fresh — pg_dump custom format can't create the DB
#      for us.
#   3. Runs pg_restore to load tables, extensions, data.
#   4. Re-applies AGE graph setup via `cmdb migrate` (AGE graph state is
#      stored in ag_catalog internals that aren't always reliably captured
#      by pg_dump; re-running migration 0003 recreates the graph + backfills
#      from entities).
#
# Requires: pg_restore, psql, cmdb on PATH.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <backup.dump> [target_db_url]" >&2
  exit 1
fi

DUMP="$1"
DB_URL="${2:-${CMDB_DATABASE_URL:-${DATABASE_URL:-}}}"
if [[ -z "$DB_URL" ]]; then
  echo "error: target DB URL not given and CMDB_DATABASE_URL is unset" >&2
  exit 1
fi

# Derive parent URL (replace /dbname with /postgres) for CREATE DATABASE.
PARENT_URL="$(echo "$DB_URL" | sed -E 's|/[^/?]+$|/postgres|')"

echo "target: $DB_URL"
echo "parent: $PARENT_URL"
echo "dump:   $DUMP"
echo ""
echo "this will DROP the target database. Continue? (y/N)"
read -r confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "aborted."
  exit 1
fi

# Parse DB name out of URL for DROP/CREATE.
DB_NAME="$(echo "$DB_URL" | sed -E 's|.*/([^/?]+)(\?.*)?$|\1|')"
echo "dropping + recreating $DB_NAME ..."
psql "$PARENT_URL" -c "DROP DATABASE IF EXISTS $DB_NAME;"
psql "$PARENT_URL" -c "CREATE DATABASE $DB_NAME;"

echo "restoring data..."
if ! pg_restore --no-owner --no-privileges --dbname="$DB_URL" "$DUMP"; then
  echo "(pg_restore reported errors — this is common when AGE objects can't be restored; continuing)"
fi

echo "re-running migrations to recreate AGE graph + idempotent schema setup..."
export CMDB_DATABASE_URL="$DB_URL"
if command -v cmdb >/dev/null 2>&1; then
    cmdb migrate || echo "(migration had errors; manual intervention may be needed)"
else
    echo "(cmdb not on PATH — skipping graph rebuild; run 'cmdb migrate' manually)"
fi

echo "restore complete."
