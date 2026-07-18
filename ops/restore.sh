#!/usr/bin/env bash
# Restore helios_cmdb from a pg_dump backup file.
#
# Usage: ./ops/restore.sh <backup.dump> [target_db_url]
#
# WARNING: this overwrites the target database. By default it restores to
# the URL in CMDB_DATABASE_URL. Pass an explicit URL as $2 to restore elsewhere
# (recommended for verification before swapping).
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

echo "RESTORING $DUMP -> $DB_URL"
echo "this will DROP and recreate the database. Continue? (y/N)"
read -r confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "aborted."
  exit 1
fi

pg_restore --clean --if-exists --no-owner --no-privileges \
  --dbname="$DB_URL" "$DUMP"
echo "restore complete."
