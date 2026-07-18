#!/usr/bin/env bash
# Backup helios_cmdb to a timestamped file.
#
# Usage: ./ops/backup.sh [output_dir]
#
# Requires: pg_dump on PATH, DATABASE_URL or CMDB_DATABASE_URL set.
set -euo pipefail

DB_URL="${CMDB_DATABASE_URL:-${DATABASE_URL:-}}"
if [[ -z "$DB_URL" ]]; then
  echo "error: CMDB_DATABASE_URL (or DATABASE_URL) must be set" >&2
  exit 1
fi

OUT_DIR="${1:-./backups}"
mkdir -p "$OUT_DIR"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUT_DIR/helios_cmdb-$TS.dump"

echo "backing up to $OUT ..."
pg_dump --format=custom --no-owner --no-privileges --dbname="$DB_URL" --file="$OUT"
SIZE=$(du -h "$OUT" | cut -f1)
echo "done: $OUT ($SIZE)"
echo ""
echo "to restore:"
echo "  ./ops/restore.sh $OUT"
