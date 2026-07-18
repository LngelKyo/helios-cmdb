#!/usr/bin/env bash
# Regression smoke test for the AGE sync path. Run after `cmdb migrate`.
#
# What this checks:
#   1. put-entity creates exactly one vertex per entity (no duplicates
#      on repeated upsert).
#   2. relate creates an edge between vertices.
#   3. Vertex entity_id matches the entities-table UUID form (not ULID).
#   4. Re-running `cmdb migrate` is idempotent (no error).
#
# Usage: ./ops/regression-age.sh
# Requires: cmdb + psql on PATH, CMDB_DATABASE_URL set, AGE+pgvector loaded.
set -euo pipefail

BIN="${CMDB_BIN:-cmdb}"
PSQL="${PSQL:-psql}"
URL="${CMDB_DATABASE_URL:?CMDB_DATABASE_URL must be set}"

# Inject search_path if caller didn't.
if [[ "$URL" != *"options="* ]]; then
    PSQL_URL="${URL}?options=-c%20search_path%3Dpublic%2Cag_catalog"
else
    PSQL_URL="$URL"
fi

echo "=== migration idempotency ==="
$BIN migrate
$BIN migrate

echo ""
echo "=== put same entity 3 times → should have exactly 1 vertex ==="
# Use cc.fleet because cmdb-cli's parse_ref() hardcodes that namespace for
# name-based refs (not configurable per-invocation).
NS="cc.fleet"
$BIN put-entity --type fleet.host --name h1-reg --attrs '{"os":"debian"}' > /dev/null
$BIN put-entity --type fleet.host --name h1-reg --attrs '{"os":"debian","role":"control-plane"}' > /dev/null
$BIN put-entity --type fleet.host --name h1-reg --attrs '{"os":"debian","role":"control-plane","cpus":4}' > /dev/null

COUNT=$($PSQL "$PSQL_URL" -t -c "SELECT * FROM cypher('cmdb_graph', \$\$ MATCH (n:Entity) WHERE n.name = 'h1-reg' RETURN count(n) \$\$) AS (c agtype)" | tr -d ' \n"')
echo "vertex count after 3 upserts: $COUNT (expected 1)"
[[ "$COUNT" == "1" ]] || { echo "FAIL"; exit 1; }

echo ""
echo "=== entity_id is UUID form (matches entities.id) ==="
UUID=$($PSQL "$URL" -t -c "SELECT id::text FROM entities WHERE name='h1-reg' LIMIT 1" | tr -d ' \n')
GID=$($PSQL "$PSQL_URL" -t -c "SELECT * FROM cypher('cmdb_graph', \$\$ MATCH (n:Entity {name: 'h1-reg'}) RETURN n.entity_id \$\$) AS (id agtype)" | tr -d ' \n"')
echo "entities.id:    $UUID"
echo "graph entity_id: $GID"
[[ "$UUID" == "$GID" ]] || { echo "FAIL: entity_id mismatch"; exit 1; }

echo ""
echo "=== relate → edge appears ==="
$BIN put-entity --type fleet.agent --name a1-reg > /dev/null
$BIN relate fleet.agent:a1-reg runs_on fleet.host:h1-reg > /dev/null
sleep 1
EDGES=$($PSQL "$PSQL_URL" -t -c "SELECT * FROM cypher('cmdb_graph', \$\$ MATCH (a:Entity {name: 'a1-reg'})-[:Relation]->(:Entity {name: 'h1-reg'}) RETURN count(*) \$\$) AS (c agtype)" | tr -d ' \n"')
echo "edges: $EDGES (expected >= 1)"
[[ "$EDGES" -ge 1 ]] || { echo "FAIL: no edges"; exit 1; }

echo ""
echo "ALL CHECKS PASSED"
