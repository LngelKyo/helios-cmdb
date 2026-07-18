# helios-cmdb

Agent-native CMDB for the ANA fleet. Rust + Postgres + MCP-first.

See [PLAN.md](./PLAN.md) for the full design.

## Status

**P0**: workspace + schema + CLI slice. In progress.

## Quick start (P0)

```bash
# 1. Enter dev shell (gives cargo, sqlx-cli, psql, docker for testcontainers)
nix develop

# 2. Run migrations against your Postgres
export DATABASE_URL=postgres://user:pass@host:5432/helios_cmdb
sqlx migrate run

# 3. Use
cmdb put-entity --type fleet.host --name miraku-home --attrs '{"os":"nixos"}'
cmdb get --type fleet.host --name miraku-home
cmdb relate miraku-home runs_on e15-host
cmdb query --traverse --from <id> --depth 3
```

## Project layout

10-crate Rust workspace; see [PLAN.md](./PLAN.md) § "Crate layout".

## License

MIT
