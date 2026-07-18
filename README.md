# helios-cmdb

Agent-native CMDB for the ANA fleet. Rust + Postgres + pgvector + Apache AGE
+ MCP-first. Designed so LLM agents (Claude Code, the `ana` fleet on NATS)
are first-class operators, not an afterthought.

See [PLAN.md](./PLAN.md) for the full design.

## Status

P0–P4 done. P5 (auth enforcement, NATS change events, partition
automation, backup/restore, prometheus metrics, 1M load test) in progress.

## Quick start

```bash
# 1. Enter dev shell (PG with age+pgvector, sqlx-cli, psql, cargo)
nix develop
# (or use any PG 16/17 + age + pgvector you have)

# 2. Run a local Postgres with the required extensions.
pg_ctl -D /tmp/cmdb-pgdata -o "-p 55432 -k /tmp" -l /tmp/pg.log start
createdb -p 55432 helios_cmdb

# 3. Apply migrations (creates api_tokens / AGE graph 'helios' / entity_types / etc.)
export DATABASE_URL=postgres://helios@127.0.0.1:55432/helios_cmdb
cmdb migrate

# 4. Configure semantic embeddings via Ollama (optional, default = noop):
export CMDB_OLLAMA_URL=http://127.0.0.1:11434
export CMDB_EMBED_MODEL=nomic-embed-text

# 5. Use
cmdb put-entity --type fleet.host --name miraku-home --attrs '{"os":"nixos"}'
cmdb list --type fleet.host
cmdb relate fleet.agent:helios-agent runs_on fleet.host:miraku-home
cmdb query --traverse --from fleet.host:miraku-home --depth 3
cmdb query --cypher "MATCH (a:fleet.agent)-[:runs_on]->(h:fleet.host) RETURN a.name, h.name"

# 6. Web UI (browser): http://your-host:8766/
cmdb serve http
#   REST:    http://your-host:8766/api/v1/
#   GraphQL: http://your-host:8766/graphql  (playground at /graphql/playground)

# 7. MCP server (Claude Code / Cursor)
cmdb serve mcp --transport stdio      # for local editor
cmdb serve mcp --transport http       # for remote fleet agents

# 8. Run as an ana agent on the NATS bus (any fleet agent can query it
#    via `ana probe cmdb --query '...'`)
cmdb serve bus --nats-url nats://127.0.0.1:31222 --identity cmdb

# 9. Collectors
cmdb collector run ssh-facts --targets host1,host2 --ssh-user you --interval 60
cmdb collector run k8s-observe --targets default,kube-system
cmdb collector run docker-socket

# 10. Governance + tokens
cmdb token create --identity agent:e15 --ns-scope cc.fleet --ops read,write
cmdb type propose --name fleet.cronjob --schema '{"type":"object"}' --by agent:e15
cmdb type approve <proposal-id>

# 11. TUI
cmdb tui
```

## CLI flag names (common gotchas)

| want | flag |
|---|---|
| Postgres URL | `--database-url` (or `CMDB_DATABASE_URL`) |
| NATS URL (for `serve bus`) | `--nats-url` (NOT `--nats`) |
| Collector targets | `--targets h1,h2` (NOT `--target`) |
| SSH user / port (ssh-facts) | `--ssh-user` / `--ssh-port` |
| HTTP bind | `--addr 0.0.0.0:8766` (default) |
| Token namespace scope | `--ns-scope ns1,ns2` (NOT `--namespace`; that clashes with the global arg) |
| Token ops | `--ops read,write,admin` |

## Project layout

13-crate Rust workspace; see [PLAN.md](./PLAN.md) § "Crate layout".

## License

MIT
