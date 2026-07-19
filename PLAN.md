# helios-cmdb — Plan

> Agent-native CMDB for the ANA fleet. Rust + Postgres + pgvector + Apache
> AGE + MCP-first. Designed so LLM agents (Claude Code, the `ana` fleet on
> NATS) are first-class operators, not an afterthought.

## Status

**P0–P5 complete.** Production-ready for single-node deployment against a
Postgres+AGE+pgvector backend. Latest commit on `main` is the source of
truth; this document is the snapshot.

| Phase | Status | Notes |
|---|---|---|
| P0 core slice | ✅ done | workspace + 4 tables + CLI migrate/put/get/list/relate/query |
| P1 MCP + ana bridge + ssh-facts | ✅ done | 12 MCP tools, stdio + HTTP; bus subscribes `cc.fleet.>` and answers `cmdb.query.>` RPC; ssh-facts fish-safe via `sh -s` stdin |
| P2 REST + GraphQL + pgvector + collectors | ✅ done | 11 REST endpoints, full GraphQL schema, semantic search via Ollama nomic-embed-text, k8s-observe + docker-socket collectors |
| P3 Apache AGE + TUI | ✅ done | graph `cmdb_graph`, dual-write MERGE+SET, Cypher translator fallback, ratatui browser |
| P4 Tokens + governance + Web UI | ✅ done | scoped API tokens, propose/approve-type workflow, embedded HTML/JS Web UI with vis-network topology |
| P5 Auth enforcement + changebus + metrics + stress + backup | ✅ done | scope-aware middleware, PG LISTEN/NOTIFY → NATS, Prometheus `/metrics`, 1M stress (60k inserts/sec), `ops/backup.sh` + `ops/restore.sh` |

## Critical fixes baked in (from QA rounds)

| Issue | Root cause | Fix |
|---|---|---|
| AGE dual-write 100% silent fail | connection pool `search_path` defaulted to `"$user",public` — never included `ag_catalog`. Caused `agtype` not to resolve, MERGE operator missing, WHERE cast to fail. Two rounds misdiagnosed this as "AGE 1.7.0-rc0 missing `@>`". | `normalize_pg_url()` injects `options=-c search_path=public,ag_catalog` into every connection URL at `PgStore::connect`. |
| Migrate non-idempotent | graph named `helios` collided with DB user `helios` (AGE creates a same-named schema for each graph). On 2nd migrate, search_path put graph schema first; `_sqlx_migrations` got written into `helios` schema, public one stayed empty. | Renamed graph `helios` → `cmdb_graph`. Migration 0003 now wraps `create_graph`/`create_vlabel`/`create_elabel` in DO blocks with `IF NOT EXISTS` checks. Backfill uses MERGE. |
| entity_id format drift in graph | backfill wrote UUID form (`r.id::text`); dual-write wrote ULID form (`EntityId::Display`). Same entity had two different keys. | Both paths now use UUID form via `e.id.as_uuid().to_string()`. Regression-tested in `ops/regression-age.sh`. |
| Auth "authentication without authorization" | middleware only checked token validity, not scope. `--require-auth` also 401-ed healthz/metrics/UI (broke k8s probes). | Public routes (healthz, metrics, /, /ui/*, /graphql/playground) bypass auth. Write methods require `write` or `admin` in op_scope. |
| http_requests_total always 0 | counter struct declared but no increment sites. | New `metrics_middleware` increments per request, buckets on 4xx/5xx. |
| restore.sh failed on fresh target | didn't create database; AGE graph internals not preserved by pg_dump. | restore.sh now drops + creates target DB, runs pg_restore, then re-runs `cmdb migrate` to rebuild graph. |

## Architecture (as built)

### 13-crate workspace

```
cmdb-core           domain model + Store trait + InMemoryStore (zero IO)
cmdb-store-pg       sqlx impl: entities/relations/facts/changes + AGE dual-write
cmdb-provenance     (placeholder for P5+ decay logic)
cmdb-embedding      Embedder trait + Ollama / OpenAI / Noop + from_env()
cmdb-auth           TokenManager + Principal + scope logic + axum middleware
cmdb-ana-bridge     subscribe cc.fleet.> + serve cmdb.query.> RPC + changebus
cmdb-mcp            MCP server (stdio + HTTP/SSE), 12 tools
cmdb-http           REST + GraphQL + Web UI + Prometheus metrics
cmdb-collectors     ssh-facts + k8s-observe + docker-socket
cmdb-tui            ratatui three-pane browser
cmdb-cli            `cmdb` binary: all subcommands
cmdb-server         (placeholder for multi-process binary; use cmdb serve all)
```

### Data model

- **Entity** (`entities` table): `id UUID, namespace, type, name, attrs JSONB, tags TEXT[], version`
- **Relation** (`relations`): directed edge; `from_id, to_id, type, props JSONB`
- **Fact** (`facts`): versioned attribute observation with provenance; effective = newest non-expired with highest confidence
- **Change** (`changes`): append-only event log; partitioned monthly at scale (P5+ polish item)
- **MetaModel** (`entity_types`, `relation_types`): schema-as-data
- **entity_embeddings** (`pgvector`): 768-dim cosine embeddings for semantic search
- **api_tokens**: scoped identity tokens (SHA-256 hashed secrets)

### AGE graph mirror

Every entity/relation is dual-written to the `cmdb_graph` AGE graph as
`:Entity` vertex / `:Relation` edge. Best-effort: failure logs WARN but
doesn't block the primary write. The `entity_id` property is the UUID form
of the entity's primary key, matching the migration backfill.

The Cypher query path tries AGE native first (canonical); the SQL
translator is a fallback for patterns AGE 1.7.0-rc0 doesn't support. When
AGE stabilizes in a future release, the translator becomes dead code and
can be removed in a one-line PR (delete the `Err(age_err) => ...` branch
in `PgStore::cypher`).

### Transports

| # | interface | what | who |
|---|---|---|---|
| 1 | MCP (stdio) | 12 tools | Claude Code, Cursor (local) |
| 2 | MCP (HTTP/SSE) | same tools over HTTP | remote fleet agents |
| 3 | NATS bus (ana) | `cc.fleet.cmdb.query.>` RPC + auto-ingest `discovery`/`pulse` | ana fleet (zero new deps) |
| 4 | REST + GraphQL | `/api/v1/*`, `/graphql` | k8s operators, dashboards, scripts |
| 5 | TUI | ratatui browser | operator terminal |
| 6 | Web UI | embedded HTML + vanila JS + vis-network | browser |
| 7 | Prometheus | `/metrics` text exposition | scraper |

`cmdb serve all` runs HTTP + bus + changebus in a single process. MCP-stdio
is excluded (owns stdin/stdout) — run it separately for editor integration.

## Operational notes

### Required PG extensions

```sql
CREATE EXTENSION age;       -- must be in shared_preload_libraries
CREATE EXTENSION pgvector;  -- for semantic search
CREATE EXTENSION pg_trgm;   -- for fuzzy text (auto by migration 0001)
CREATE EXTENSION pgcrypto;  -- for gen_random_uuid fallback
```

In NixOS:
```nix
postgresql_17.withPackages (p: [ p.age p.pgvector ])
# plus: shared_preload_libraries = "age" in postgresql.conf
```

### Connection URL

`PgStore::connect` auto-injects `options=-c search_path=public,ag_catalog`
if not already present. You don't need to set this manually — but if you
connect via psql for debugging, set it yourself or AGE operators fail:

```bash
psql "postgres://helios@host/db?options=-c%20search_path%3Dpublic%2Cag_catalog"
```

### Smoke / regression tests

| Test | What it covers |
|---|---|
| `cargo test --workspace` | 26 unit tests (domain logic, Principal scope, ssh-facts parser, Cypher translator) |
| `ops/regression-age.sh` | End-to-end: migrate idempotency, AGE vertex dedup, entity_id form consistency, edge lands after relate |
| `ops/backup.sh` + `ops/restore.sh` | Round-trip; restore rebuilds AGE graph |

## Known caveats / Future work

- **changes table partitioning**: PLAN.md original called for monthly
  partitions + pg_partman automation. Not implemented; for P5 1M-entity
  load test, flat table was fine. Add pg_partman if `changes` grows
  beyond ~10M rows.
- **AGE 1.7.0 stable**: when released, drop the Cypher translator
  fallback in `PgStore::cypher` (one-line removal). Verify `MERGE ... ON
  CREATE SET` syntax support.
- **Backup/restore + AGE graph**: `pg_dump` / `pg_restore` does NOT
  reliably capture AGE graph internals (vertices/edges stored in
  `_ag_label_*` tables with graphid OIDs that may not survive the
  round-trip). `ops/restore.sh` handles this by re-running `cmdb migrate`
  after the data load — migration 0003 is idempotent and the backfill
  DO blocks use MERGE so existing data is preserved while the graph is
  rebuilt. If you restore manually (without restore.sh), always run
  `cmdb migrate` after `pg_restore`.
- **docker-socket collector**: requires `helios` user in `docker` group
  (or rootless docker). Without that, the collector logs EACCES and
  exits.
- **Web UI**: vis-network bundled locally (no CDN dependency). Could
  add: token login flow, edit-in-place, multi-namespace switcher,
  property editor for metamodel.proposal.
- **Multi-node PG**: read replica support not tested. Streaming
  replication should work but `LISTEN/NOTIFY` (for changebus) doesn't
  replicate — would need to either run changebus per-replica or use
  logical decoding.
- **MCP `cypher` tool**: returns agtype-encoded JSON strings (with
  quoted values). A future polish could decode agtype → JSON natively.
- **`normalize_pg_url` on user-provided URLs**: if a caller provides an
  `options=` with their own `search_path` that doesn't include
  `ag_catalog`, we log a WARN but respect their choice. This could
  silently resurrect AGE errors — operators should ensure either
  `ag_catalog` is in their custom search_path or omit `options=` from
  the URL.

## Bug history (lessons learned)

### The "AGE 1.7.0-rc0 missing `@>`" misdiagnosis (two rounds)

**Symptom**: AGE dual-write was 100% silently failing. `MERGE (n:Entity {entity_id: 'X'})` errored with `operator does not exist: ag_catalog.agtype @> ag_catalog.agtype`. WHERE on agtype failed with `cannot cast agtype string to boolean`.

**First diagnosis (P3)**: AGE 1.7.0-rc0 is buggy, missing the `@>` operator that openCypher MERGE needs for property matching. Worked around with MATCH-then-CREATE pattern.

**Second diagnosis (P5)**: MATCH-then-CREATE also failed; double-down on the rc0-bug theory, added Cypher→SQL translator as primary path.

**Real root cause (post-P5 QA)**: PG connection's `search_path` was `"$user",public`. `ag_catalog` was never in it. Without the schema, `agtype` type itself doesn't resolve, so any operator on it appears "missing". The rc0 theory was wrong.

**One-line fix**: `normalize_pg_url()` injects `options=-c search_path=public,ag_catalog`. Three rounds of confusion collapsed into one config line.

**Lesson**: When a "library bug" is silent (best-effort failure logged at WARN only), and your smoke tests use a fallback path (SQL translator) that doesn't touch the broken code path, suspect environment/config first. Test the actual code path under RUST_LOG=info before blaming the library.

## Decision log

- **2026-07-19** P0: stack = Rust + Postgres + closure-table-then-AGE
- **2026-07-19** P0: identity = string, trust via NATS ACL/mTLS, sig slot reserved for ana v0.2
- **2026-07-19** P0: namespace = first-class, mirrors ana prefix
- **2026-07-19** P0: CMDB runs as ana agent identity `cmdb`, fleet queries via existing `ana` CLI
- **2026-07-19** P0: shared k3s PG, independent database `helios_cmdb`
- **2026-07-19** P0: collectors run as `cmdb collector run <name>` subcommands
- **2026-07-19** P1: hand-rolled MCP JSON-RPC (~400 lines) instead of SDK
- **2026-07-19** P1: ssh-facts uses `ssh host sh -s` + stdin pipe (POSIX-shell-guaranteed)
- **2026-07-19** P2: default embedder = Ollama nomic-embed-text (768 dims)
- **2026-07-19** P3: AGE graph name `helios` renamed → `cmdb_graph` to avoid user/schema collision
- **2026-07-19** P3: Cypher translator added as fallback after AGE 1.7.0-rc0 edge-traversal bugs; translator first, AGE fallback
- **2026-07-19** P4: rust-embed for Web UI (no build step); vis-network from CDN
- **2026-07-19** P5: auth enforcement via `from_fn` + closure capturing `Arc<AppState>` (axum 0.8 workaround)
- **2026-07-19** P5: connection URL injection of `search_path=public,ag_catalog` — root cause of three rounds of AGE bugs
- **2026-07-19** P5: AGE-native first, translator as fallback (reversed from P3). When AGE 1.7.0 stable lands, translator becomes dead code removable in one PR.
- **2026-07-19** P5: vis-network bundled locally (644KB embedded) — no more CDN dependency, no mixed-content blocking

## Quick start

See [README.md](./README.md). TL;DR:

```bash
nix develop  # or any PG 17 with age + pgvector extensions
createdb helios_cmdb
export CMDB_DATABASE_URL=postgres://helios@localhost/helios_cmdb
cmdb migrate
cmdb put-entity --type fleet.host --name h1 --attrs '{"os":"nixos"}'
cmdb serve http        # Web UI at http://localhost:8766/
cmdb serve all         # HTTP + bus + changebus in one process
```

## License

MIT.
