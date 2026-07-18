# helios-cmdb — Plan

An agent-native CMDB for the ANA fleet. Rust + Postgres + MCP-first. Designed so
LLM agents (Claude Code + the `ana` fleet on NATS) are first-class operators,
not an afterthought.

Status: **P0 in progress** (workspace + schema + CLI slice).

## Why this exists

Existing OSS CMDBs (NetBox / CMDBuild / iTop / Ralph / GLPI / OneCMDB) are
designed for human DCIM operators: UI-first, rigid schema, weak API,
process-heavy. They are bad at:

- letting an LLM **introspect** the data model and **propose** new types,
- treating **relationships** as first-class with graph traversal,
- recording **provenance** per fact (which agent observed it? how confident?
  how stale?),
- being **driven** by tools (MCP / RPC), not by clicking forms,
- surviving **bad data** (agent hallucinations) without corrupting truth.

`helios-cmdb` flips the defaults: tool-first, schema-as-data, facts are
versioned with provenance + decay, the CMDB itself is just another agent on
the ana bus.

## Architecture

### Data model (4-tuple + metamodel)

| concept | fields | notes |
|---|---|---|
| **Entity** | `id(ulid), namespace, type, name, attrs(jsonb), tags[], version` | one CI; type defined in metamodel |
| **Relation** | `id, namespace, from_id, to_id, type, props(jsonb)` | directed edge; type from metamodel |
| **Fact** | `id, namespace, entity_id, key, value(jsonb), source(jsonb), confidence, observed_at, ttl, superseded_by` | versioned attribute observation; multiple coexist; effective = newest-non-expired & highest-confidence |
| **Change** | `id, ts, namespace, actor, op, target_type, target_id, before, after, reason` | append-only event log; partitioned by month |
| **MetaModel** | `entity_types[], relation_types[]` | schema-as-data; agent can introspect & propose new types |

### Provenance

Every Fact carries a `Source`:

```rust
struct Source {
    kind: SourceKind,         // Collector | Agent | Human | Inferred
    identity: String,         // "cc.fleet.e15" | "collector.ssh.host1" | "user:helios"
    transport: Transport,     // Nats | Mcp | Http | Cli | Internal
    nats_subject: Option<String>,
    observed_at: DateTime<Utc>,
    confidence: f32,          // 0.0..1.0; defaults: human=1.0 collector=0.9 agent=0.7 inferred=0.5
    ttl: Option<Duration>,
    sig: Option<Signature>,   // reserved for ana v0.2 ed25519
    evidence_ref: Option<EvidenceRef>,
}
```

Trust comes from transport (NATS ACL / mTLS / MCP auth), not envelope signatures
— ana v0.1 has none. The `sig` slot is reserved so v0.2 is non-breaking.

### Identity / namespace

- `source.identity: String` — matches the agent's `from` field on the ana bus.
- NATS subject ACLs (`publish: cc.fleet.<self>.>`) make forgery expensive.
- `namespace` is first-class on every table, mirrors the ana `prefix` (default
  `cc.fleet`). Multi-tenant = multiple prefixes or multiple CMDB instances.

### The CMDB is itself an ana agent

```
identity = "cmdb"

subscribe:
  cc.fleet.>.discovery     → upsert fleet.agent
  cc.fleet.>.pulse         → update agent activity + runs_on host
  cc.fleet.>.alert         → record event + emit change notification
  cc.fleet.cmdb.query.>    → RPC entrypoint (any agent can query)
  cc.fleet.cmdb.command.>  → RPC entrypoint (privileged writes)

publish:
  cc.fleet.cmdb.alert.entity_changed.<id>
  cc.fleet.cmdb.alert.fact_decayed.<id>.<key>
  cc.fleet.broadcast.alert.cmdb.schema_changed
```

Fleet agents query using the existing ana CLI:
```bash
ana --identity e15 send query --to cmdb \
    --query 'list agents running on host miraku-home' --topic agent.query
```

No new client SDK needed.

## Stack (locked)

| layer | choice | rationale |
|---|---|---|
| language | Rust | single binary, fleet-grade perf, type safety |
| storage | Postgres on shared k3s, independent db `helios_cmdb` | isolation via separate DB; no extra component to run |
| graph query | closure table + recursive CTE (P0-P2) → Apache AGE (P3) | AGE still incubating; defer |
| full-text / fuzzy | `pg_trgm` + `pgvector` (P2) | same DB; no ES |
| eventing | core NATS (ana bus) + JetStream optional for durability | matches fleet |
| MCP | `rmcp` crate, stdio + HTTP/SSE dual transport | Claude Code local + fleet remote |
| HTTP | axum + async-graphql | P2 |
| CLI | clap | P0 |
| TUI | ratatui | P3 |

## Crate layout

```
helios-cmdb/
├─ Cargo.toml                      workspace
├─ flake.nix                       NixOS first-class
├─ migrations/                     sqlx::migrate!
├─ crates/
│  ├─ cmdb-core/                   domain model + traits (zero IO)
│  ├─ cmdb-store-pg/               sqlx + closure table (AGE in P3)
│  ├─ cmdb-provenance/             confidence decay / ttl / effective fact
│  ├─ cmdb-ana-bridge/             ana protocol client (sub / pub / RPC)
│  ├─ cmdb-mcp/                    MCP server, stdio + http
│  ├─ cmdb-http/                   axum REST + async-graphql
│  ├─ cmdb-cli/                    clap main binary
│  ├─ cmdb-tui/                    ratatui browser
│  ├─ cmdb-collectors/             ssh_facts / k8s_observe / docker_socket
│  └─ cmdb-server/                 main binary, wires transports together
└─ tests/                          integration tests (testcontainers)
```

## Interface priority

1. **MCP server** (stdio + HTTP/SSE) — agent native
2. **CLI** (`cmdb` + `cmdb collector run <name>`)
3. **NATS** — fleet RPC + change events
4. **REST + GraphQL** — external integrations
5. **TUI** — operator browsing
6. **Web UI** — topology viz (P4)

## Scale targets

- 1M+ entities, 10M+ relations
- `changes` partitioned monthly; >6mo archived to cold storage
- effective-fact lookup: ms via `(namespace, entity_id, key, observed_at DESC)` index
- attrs GIN index (`jsonb_path_ops`) for ad-hoc structural queries

## Bootstrap entity types (out of the box)

From ana envelopes (P1 collector that just subscribes to the bus):
- `fleet.agent` (from `discovery`)
- `fleet.host` (from `host` field + ssh-facts)
- `fleet.cluster` (from `discovery.cluster`)

Infra (P2 collectors):
- `infra.vm`, `infra.container`, `infra.pod`, `infra.service`, `infra.volume`

Service catalog (manual/CI):
- `app.service`, `app.component`
- `secret.ref` (path + rotation metadata, never the value)
- `kb.runbook` (URL + applicable entities)

## Roadmap

| phase | weeks | deliverable | done when |
|---|---|---|---|
| **P0** | 1-2 | workspace + schema + CLI 3 commands | can put/get/relate/traverse locally |
| **P1** | 3-5 | MCP (12 tools, stdio+http) + ana bridge auto-ingest `cc.fleet.>` + ssh-facts collector + full provenance | Claude Code sees live fleet; `ana send query --to cmdb` works |
| **P2** | 2 | REST + GraphQL + k8s/docker collectors + pgvector semantic search | k8s cron auto-syncs pods |
| **P3** | 2 | Apache AGE graph + TUI | Cypher 5-hop impact queries |
| **P4** | 3 | governance (entity-type proposals/approvals) + Web UI + mTLS/scoped tokens | agent proposals routed through approve |
| **P5** | continuous | backup/restore, partition automation, prom metrics, 1M load test | prod-hardened |

## P0 concrete steps

1. Cargo workspace + 10 crate skeletons + `flake.nix`
2. `cmdb-core` domain models + `Store` trait (mock impl + unit tests)
3. `migrations/0001_init.sql` — 5 tables + closure table + metamodel tables + indexes
4. `cmdb-store-pg` sqlx impl + testcontainers integration tests
5. `cmdb-cli` MVP: `put-entity`, `get`, `relate`, `query --traverse`, plus `migrate`

P0 done = local `cmdb` binary against a `helios_cmdb` Postgres database, can
model 10 hosts with relations and traverse neighbors. MVP milestone.

## Decision log

- **2026-07-19** stack = Rust + Postgres + closure-table-then-AGE
- **2026-07-19** identity = string, trust via NATS ACL/mTLS, sig slot reserved
- **2026-07-19** namespace = first-class, mirrors ana prefix
- **2026-07-19** CMDB runs as ana agent identity `cmdb`, fleet queries via existing `ana` CLI
- **2026-07-19** shared k3s PG, independent database `helios_cmdb`
- **2026-07-19** collectors run as `cmdb collector run <name>` subcommands
