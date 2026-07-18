//! CLI command implementations.

use crate::output;
use crate::{Cli, Command};
use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::fact::FactInput;
use cmdb_core::id::EntityId;
use cmdb_core::relation::RelationInput;
use cmdb_core::source::Source;
use cmdb_core::store::{Direction, QueryFilter, TraverseStep};
use cmdb_core::Store;
use cmdb_store_pg::PgStore;
use serde_json::{json, Value};
use std::str::FromStr;

pub async fn dispatch(cli: Cli) -> Result<()> {
    let namespace = cli.namespace.clone();
    let actor = cli.actor.clone();
    let json_mode = cli.json;
    let store = open_store(&cli).await?;

    match cli.command {
        Command::Migrate => migrate(&store).await?,
        Command::PutEntity(args) => put_entity(&namespace, &actor, &store, args).await?,
        Command::Get(args) => get(&store, args).await?,
        Command::List(args) => list(&namespace, &store, args, json_mode).await?,
        Command::Relate(args) => relate(&namespace, &store, args).await?,
        Command::Query(args) => query(&store, args, json_mode).await?,
        Command::Serve(args) => serve(&namespace, &actor, &store, args).await?,
        Command::Collector(args) => run_collector(&namespace, &actor, &store, args).await?,
        Command::Tui => {
            let store_arc: std::sync::Arc<dyn Store> = std::sync::Arc::new(store.clone());
            cmdb_tui::run(store_arc, namespace.clone()).await?;
        }
        Command::Token(args) => token_cmd(&store, args).await?,
        Command::Type(args) => type_cmd(&namespace, &actor, &store, args).await?,
        Command::Stress(args) => stress(&store, args).await?,
    }
    Ok(())
}

async fn open_store(cli: &Cli) -> Result<PgStore> {
    let url = cli
        .database_url
        .as_ref()
        .ok_or_else(|| anyhow!("--database-url or CMDB_DATABASE_URL is required"))?;
    let mut store = PgStore::connect(url).await?;
    if !cli.no_embed {
        let embedder: std::sync::Arc<dyn cmdb_embedding::Embedder> =
            std::sync::Arc::from(cmdb_embedding::from_env());
        store = store.with_embedder(embedder);
    }
    Ok(store)
}

async fn migrate(store: &PgStore) -> Result<()> {
    print!("applying migrations... ");
    store.run_migrations().await?;
    println!("ok");
    Ok(())
}

#[derive(Args, Debug)]
pub struct PutEntityArgs {
    #[arg(long)]
    pub r#type: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_name = "JSON")]
    pub attrs: Option<String>,
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub fact: bool,
}

async fn put_entity(
    namespace: &str,
    actor: &str,
    store: &PgStore,
    args: PutEntityArgs,
) -> Result<()> {
    let attrs: Value = match &args.attrs {
        Some(s) => serde_json::from_str(s)?,
        None => Value::Object(Default::default()),
    };
    let input = EntityInput::new(namespace, &args.r#type, &args.name)
        .with_attrs(attrs.clone())
        .with_tags(args.tags.clone().into_iter());
    let source = Source::new_cli(actor);

    let entity = store.put_entity(input, source).await?;
    output::entity(&entity);

    if args.fact {
        let _ = store
            .add_fact(FactInput {
                namespace: namespace.to_string(),
                entity: EntityRef::by_id(entity.id),
                key: "_attrs".into(),
                value: attrs,
                source: Source::new_cli(actor),
            })
            .await?;
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct GetArgs {
    #[arg(long)]
    pub r#type: Option<String>,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long, default_value_t = false)]
    pub facts: bool,
}

async fn get(store: &PgStore, args: GetArgs) -> Result<()> {
    let entity = if let Some(id_str) = &args.id {
        let id = EntityId::from_str(id_str)?;
        store.get_entity_by_id(id).await?
    } else {
        let t = args
            .r#type
            .as_deref()
            .ok_or_else(|| anyhow!("--type is required when not using --id"))?;
        let n = args
            .name
            .as_deref()
            .ok_or_else(|| anyhow!("--name is required when not using --id"))?;
        store.get_entity("cc.fleet", t, n).await?
    }
    .ok_or_else(|| anyhow!("entity not found"))?;

    output::entity(&entity);

    if args.facts {
        let facts = store
            .effective_facts(entity.id, Default::default())
            .await?;
        println!("\n--- facts ---");
        for f in &facts {
            if f.key == "_attrs" {
                continue;
            }
            println!(
                "  {:<20} {} (conf={}, src={})",
                f.key, f.value, f.source.confidence, f.source.identity
            );
        }
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[arg(long)]
    pub r#type: Option<String>,
    #[arg(long)]
    pub name_prefix: Option<String>,
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: u32,
}

async fn list(namespace: &str, store: &PgStore, args: ListArgs, json_mode: bool) -> Result<()> {
    let mut filter = QueryFilter::new()
        .in_namespace(namespace)
        .with_limit(args.limit);
    if let Some(t) = &args.r#type {
        filter = filter.of_type(t);
    }
    if let Some(p) = &args.name_prefix {
        filter.name_prefix = Some(p.clone());
    }
    filter.tags = args.tags;
    let entities = store.query_entities(filter).await?;
    if json_mode {
        println!("{}", serde_json::to_string_pretty(&json!({"entities": entities, "count": entities.len()}))?);
    } else {
        output::entities(&entities);
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct RelateArgs {
    pub from: String,
    pub relation_type: String,
    pub to: String,
    #[arg(long, value_name = "JSON")]
    pub props: Option<String>,
}

fn parse_ref(s: &str) -> Result<EntityRef> {
    if let Ok(id) = EntityId::from_str(s) {
        return Ok(EntityRef::by_id(id));
    }
    let (t, n) = s
        .split_once(':')
        .ok_or_else(|| anyhow!("'{}' is neither an id nor a type:name ref", s))?;
    Ok(EntityRef::by_name("cc.fleet", t, n))
}

async fn relate(namespace: &str, store: &PgStore, args: RelateArgs) -> Result<()> {
    let from = parse_ref(&args.from)?;
    let to = parse_ref(&args.to)?;
    let props = match args.props {
        Some(s) => serde_json::from_str(&s)?,
        None => Value::Object(Default::default()),
    };
    let input = RelationInput {
        namespace: namespace.to_string(),
        from,
        to,
        relation_type: args.relation_type,
        props,
    };
    let relation = store.put_relation(input).await?;
    println!("{}", serde_json::to_string_pretty(&relation)?);
    Ok(())
}

#[derive(Args, Debug)]
pub struct QueryArgs {
    #[arg(long)]
    pub traverse: bool,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long, default_value_t = 3)]
    pub depth: u32,
    #[arg(long)]
    pub relation_type: Option<String>,
    #[arg(long, default_value = "outgoing")]
    pub direction: String,
    /// Run a Cypher query against the AGE graph (e.g. "MATCH (n) RETURN n LIMIT 10")
    #[arg(long)]
    pub cypher: Option<String>,
}

async fn query(store: &PgStore, args: QueryArgs, json_mode: bool) -> Result<()> {
    if let Some(cypher) = &args.cypher {
        let rows = store.cypher(cypher).await?;
        if json_mode {
            println!("{}", serde_json::to_string_pretty(&json!({"rows": rows, "count": rows.len()}))?);
            return Ok(());
        }
        if rows.is_empty() {
            println!("(no rows)");
            return Ok(());
        }
        // Compute display widths with ellipsis at 60 chars.
        let cols = rows[0].len();
        let mut widths = vec![0usize; cols];
        for r in &rows {
            for (i, c) in r.iter().enumerate() {
                let display_len = c.chars().count().min(60);
                widths[i] = widths[i].max(display_len);
            }
        }
        for r in &rows {
            let cells: Vec<String> = r
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let truncated: String = c.chars().take(60).collect();
                    let suffix = if c.chars().count() > 60 { "…" } else { "" };
                    format!("{:<width$}", format!("{truncated}{suffix}"), width = widths[i])
                })
                .collect();
            println!("{}", cells.join("  |  "));
        }
        println!("\n{} row{}", rows.len(), if rows.len() == 1 { "" } else { "s" });
        return Ok(());
    }
    let from = args.from.as_deref().ok_or_else(|| anyhow!("--from is required (or use --cypher)"))?;
    let from_id = match parse_ref(from)? {
        EntityRef::Id { id } => id,
        EntityRef::Name {
            namespace,
            entity_type,
            name,
        } => store
            .get_entity(&namespace, &entity_type, &name)
            .await?
            .ok_or_else(|| anyhow!("entity not found: {namespace}/{entity_type}/{name}"))?
            .id,
    };
    let direction = match args.direction.as_str() {
        "outgoing" | "out" => Direction::Outgoing,
        "incoming" | "in" => Direction::Incoming,
        "both" => Direction::Both,
        other => return Err(anyhow!("invalid --direction '{}'", other)),
    };
    let step = TraverseStep {
        relation_type: args.relation_type,
        direction,
        max_depth: args.depth,
    };
    let hits = store.traverse(from_id, step).await?;
    output::traverse_hits(&hits);
    Ok(())
}

// ---------------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct ServeArgs {
    #[command(subcommand)]
    pub command: ServeCommand,
}

#[derive(Subcommand, Debug)]
pub enum ServeCommand {
    /// MCP server (stdio for Claude Code; http for remote fleet agents)
    Mcp(McpArgs),
    /// Subscribe to cc.fleet.> and serve cmdb.query.> RPC over the ana bus
    Bus(BusArgs),
    /// REST + GraphQL HTTP server
    Http(HttpArgs),
    /// PG LISTEN/NOTIFY → NATS change-event bridge
    Changebus(ChangebusArgs),
    /// Run HTTP + bus + changebus in one process (MCP-stdio excluded;
    /// use `serve mcp` separately since it owns stdin).
    All(AllArgs),
}

#[derive(Args, Debug)]
pub struct McpArgs {
    #[arg(long, default_value = "stdio")]
    pub transport: String,
    #[arg(long, default_value = "0.0.0.0:8765")]
    pub addr: String,
}

#[derive(Args, Debug)]
pub struct BusArgs {
    #[arg(long, env = "CMDB_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    pub nats_url: String,
    #[arg(long, default_value = "cmdb")]
    pub identity: String,
    #[arg(long, env = "CMDB_ANA_PREFIX", default_value = "cc.fleet")]
    pub prefix: String,
}

#[derive(Args, Debug)]
pub struct HttpArgs {
    /// Bind address. Default 0.0.0.0 so the UI is reachable from other
    /// machines on the network; use 127.0.0.1 to restrict to localhost.
    #[arg(long, default_value = "0.0.0.0:8766")]
    pub addr: String,
    /// Require Bearer token on /api/v1/* and /graphql routes.
    #[arg(long, default_value_t = false)]
    pub require_auth: bool,
    /// Disable the embedded Web UI (REST/GraphQL only).
    #[arg(long, default_value_t = false)]
    pub no_ui: bool,
}

#[derive(Args, Debug)]
pub struct ChangebusArgs {
    #[arg(long, env = "CMDB_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    pub nats_url: String,
    #[arg(long, env = "CMDB_ANA_PREFIX", default_value = "cc.fleet")]
    pub prefix: String,
}

#[derive(Args, Debug)]
pub struct AllArgs {
    /// HTTP bind addr.
    #[arg(long, default_value = "0.0.0.0:8766")]
    pub http_addr: String,
    /// NATS URL for ana bridge + changebus.
    #[arg(long, env = "CMDB_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    pub nats_url: String,
    /// ana bus identity (cc.fleet.<identity>).
    #[arg(long, default_value = "cmdb")]
    pub identity: String,
    /// ana prefix.
    #[arg(long, env = "CMDB_ANA_PREFIX", default_value = "cc.fleet")]
    pub prefix: String,
    /// Require Bearer token on /api/v1/* and /graphql routes.
    #[arg(long, default_value_t = false)]
    pub require_auth: bool,
}

async fn serve(namespace: &str, actor: &str, store: &PgStore, args: ServeArgs) -> Result<()> {
    // Capture pool + db URL before coercing to trait object.
    let pool = store.pool().clone();
    let cli_db_url = std::env::var("CMDB_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| anyhow!("CMDB_DATABASE_URL required for serve"))?;
    let store: std::sync::Arc<dyn Store> = std::sync::Arc::new(store.clone());
    match args.command {
        ServeCommand::Mcp(m) => {
            let mcp_actor = format!("{actor}/mcp:{namespace}");
            match m.transport.as_str() {
                "stdio" => cmdb_mcp::serve_stdio(store, mcp_actor).await?,
                "http" => {
                    let addr: std::net::SocketAddr = m.addr.parse()?;
                    println!("MCP HTTP server on http://{addr}/mcp");
                    cmdb_mcp::serve_http(store, mcp_actor, addr).await?;
                }
                other => return Err(anyhow!("unknown transport: {other}")),
            }
        }
        ServeCommand::Bus(b) => {
            cmdb_ana_bridge::serve_bus(store, &b.nats_url, &b.identity, &b.prefix).await?;
        }
        ServeCommand::Http(h) => {
            let addr: std::net::SocketAddr = h.addr.parse()?;
            let opts = cmdb_http::HttpOptions {
                require_auth: h.require_auth,
                serve_ui: !h.no_ui,
            };
            println!("helios-cmdb HTTP server starting on http://{addr}");
            println!("  UI:        http://{addr}/");
            println!("  REST:      http://{addr}/api/v1/");
            println!("  GraphQL:   http://{addr}/graphql  (playground at /graphql/playground)");
            println!("  Metrics:   http://{addr}/metrics");
            println!("  Health:    http://{addr}/healthz");
            cmdb_http::run_with_options_and_pool(
                store,
                Some(pool),
                format!("{actor}/http:{namespace}"),
                addr,
                opts,
            ).await?;
        }
        ServeCommand::Changebus(c) => {
            // Convert PG pool URL for sqlx PgListener (same format).
            cmdb_ana_bridge::run_changebus(store, &cli_db_url, &c.nats_url, &c.prefix).await?;
        }
        ServeCommand::All(a) => {
            let http_addr: std::net::SocketAddr = a.http_addr.parse()?;
            let opts = cmdb_http::HttpOptions {
                require_auth: a.require_auth,
                serve_ui: true,
            };
            println!("helios-cmdb `serve all` starting");
            println!("  HTTP / UI:  http://{http_addr}/");
            println!("  ana bus:   identity={} prefix={}", a.identity, a.prefix);
            println!("  changebus: {} → NATS {}", cli_db_url, a.nats_url);

            // Spawn bus + changebus as background tasks; HTTP runs in foreground.
            let store_bus = store.clone();
            let store_cb = store.clone();
            let bus_id = a.identity.clone();
            let bus_prefix = a.prefix.clone();
            let nats_url_bus = a.nats_url.clone();
            let nats_url_cb = a.nats_url.clone();
            let db_url = cli_db_url.clone();
            let cb_prefix = a.prefix.clone();
            tokio::spawn(async move {
                if let Err(e) = cmdb_ana_bridge::serve_bus(store_bus, &nats_url_bus, &bus_id, &bus_prefix).await {
                    tracing::error!(error = %e, "ana bus task exited");
                }
            });
            tokio::spawn(async move {
                if let Err(e) = cmdb_ana_bridge::run_changebus(store_cb, &db_url, &nats_url_cb, &cb_prefix).await {
                    tracing::error!(error = %e, "changebus task exited");
                }
            });

            cmdb_http::run_with_options_and_pool(
                store,
                Some(pool),
                format!("{actor}/http:{namespace}"),
                http_addr,
                opts,
            ).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// collectors
// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct CollectorArgs {
    #[command(subcommand)]
    pub command: CollectorCommand,
}

#[derive(Subcommand, Debug)]
pub enum CollectorCommand {
    /// List registered collectors
    List,
    /// Run a collector by name
    Run {
        /// Collector name (e.g. ssh-facts)
        name: String,
        /// Comma-separated targets (collector-specific; e.g. hosts for ssh-facts)
        #[arg(long, value_delimiter = ',')]
        targets: Vec<String>,
        /// Run interval in seconds (0 = one-shot)
        #[arg(long, default_value_t = 0)]
        interval: u64,
        /// SSH user (ssh-facts only)
        #[arg(long, env = "CMDB_SSH_USER")]
        ssh_user: Option<String>,
        /// SSH port (ssh-facts only)
        #[arg(long, default_value_t = 22)]
        ssh_port: u16,
    },
}
async fn run_collector(
    namespace: &str,
    actor: &str,
    store: &PgStore,
    args: CollectorArgs,
) -> Result<()> {
    let store: std::sync::Arc<dyn Store> = std::sync::Arc::new(store.clone());
    match args.command {
        CollectorCommand::List => {
            println!("{:<18} {}", "name", "description");
            println!("{}", "-".repeat(70));
            for c in cmdb_collectors::list() {
                println!("{:<18} {}", c.name, c.description);
            }
        }
        CollectorCommand::Run {
            name,
            targets,
            interval,
            ssh_user,
            ssh_port,
        } => {
            let cfg = cmdb_collectors::CollectorConfig {
                namespace: namespace.to_string(),
                actor: actor.to_string(),
                targets,
                interval_seconds: interval,
                ssh_user,
                ssh_port,
            };
            cmdb_collectors::run(&name, store, cfg).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// stress
// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct StressArgs {
    /// How many entities to insert.
    #[arg(long, default_value_t = 1000)]
    pub count: u32,
    /// Entity type label (defaults to a stress.test type).
    #[arg(long, default_value = "stress.test")]
    pub entity_type: String,
}

async fn stress(store: &PgStore, args: StressArgs) -> Result<()> {
    let started = std::time::Instant::now();
    println!("inserting {} entities via generate_series...", args.count);
    // Use gen_random_uuid() as a per-row suffix to guarantee uniqueness
    // across re-runs (so we can benchmark repeatedly without manual cleanup).
    let rows = sqlx::query(
        r#"INSERT INTO entities (id, namespace, type, name, attrs, tags, created_at, updated_at, version)
           SELECT gen_random_uuid(),
                  'cc.fleet',
                  $1,
                  'stress-' || g || '-' || substring(gen_random_uuid()::text, 1, 8),
                  jsonb_build_object('idx', g, 'kind', 'stress'),
                  ARRAY[]::text[],
                  NOW(),
                  NOW(),
                  1
             FROM generate_series(1, $2) AS g"#,
    )
    .bind(&args.entity_type)
    .bind(args.count as i64)
    .execute(store.pool())
    .await?;
    println!(
        "inserted {} rows in {:.2}s ({:.0}/s)",
        rows.rows_affected(),
        started.elapsed().as_secs_f64(),
        rows.rows_affected() as f64 / started.elapsed().as_secs_f64().max(0.001),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// token
// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct TokenArgs {
    #[command(subcommand)]
    pub command: TokenCommand,
}

#[derive(Subcommand, Debug)]
pub enum TokenCommand {
    /// Create a new scoped token. Prints the raw secret ONCE.
    Create {
        #[arg(long)]
        identity: String,
        /// Namespace scope (empty = all). Comma-separated.
        #[arg(long, value_delimiter = ',')]
        ns_scope: Vec<String>,
        /// Operation scope: read, write, admin (empty = all). Comma-separated.
        #[arg(long, value_delimiter = ',')]
        ops: Vec<String>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        expires_days: Option<i64>,
    },
    /// List all tokens (revoked and active).
    List,
    /// Revoke a token by id.
    Revoke { id: String },
}

async fn token_cmd(store: &PgStore, args: TokenArgs) -> Result<()> {
    let mgr = cmdb_auth::TokenManager::new(store.pool().clone());
    match args.command {
        TokenCommand::Create {
            identity,
            ns_scope,
            ops,
            description,
            expires_days,
        } => {
            let expires_at = expires_days.map(|d| chrono::Utc::now() + chrono::Duration::days(d));
            let created = mgr
                .create(cmdb_auth::CreateToken {
                    identity,
                    namespace_scope: ns_scope,
                    op_scope: ops,
                    description,
                    expires_at,
                })
                .await?;
            println!("token id:     {}", created.token.id);
            println!("identity:     {}", created.token.identity);
            println!("namespaces:   {:?}", created.token.namespace_scope);
            println!("ops:          {:?}", created.token.op_scope);
            println!("\nRAW SECRET (store securely; will not be shown again):");
            println!("  {}", created.raw);
        }
        TokenCommand::List => {
            let tokens = mgr.list().await?;
            if tokens.is_empty() {
                println!("(no tokens)");
                return Ok(());
            }
            println!(
                "{:<28} {:<20} {:<14} {:<14} {:<10}",
                "id", "identity", "namespaces", "ops", "status"
            );
            println!("{}", "-".repeat(90));
            for t in tokens {
                let status = if t.revoked_at.is_some() {
                    "revoked"
                } else if t.expires_at.map(|e| e < chrono::Utc::now()).unwrap_or(false) {
                    "expired"
                } else {
                    "active"
                };
                println!(
                    "{:<28} {:<20} {:<14} {:<14} {:<10}",
                    t.id,
                    t.identity,
                    t.namespace_scope.join(","),
                    t.op_scope.join(","),
                    status
                );
            }
        }
        TokenCommand::Revoke { id } => {
            mgr.revoke(&id).await?;
            println!("revoked: {}", id);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// type governance
// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct TypeArgs {
    #[command(subcommand)]
    pub command: TypeCommand,
}

#[derive(Subcommand, Debug)]
pub enum TypeCommand {
    /// List registered entity types from the metamodel.
    List,
    /// Propose a new entity type (creates a metamodel.proposal entity).
    Propose {
        #[arg(long)]
        name: String,
        #[arg(long, value_name = "JSON")]
        schema: String,
        #[arg(long, default_value = "user:cli")]
        by: String,
        #[arg(long)]
        description: Option<String>,
    },
    /// List pending proposals.
    Proposals,
    /// Approve a proposal and register the type.
    Approve {
        id: String,
        #[arg(long, default_value = "user:admin")]
        by: String,
    },
    /// Reject a proposal.
    Reject {
        id: String,
        #[arg(long, default_value = "user:admin")]
        by: String,
    },
}

async fn type_cmd(namespace: &str, actor: &str, store: &PgStore, args: TypeArgs) -> Result<()> {
    match args.command {
        TypeCommand::List => {
            let rows: Vec<(String, Option<String>)> = sqlx::query_as(
                "SELECT name, description FROM entity_types WHERE namespace = $1 ORDER BY name",
            )
            .bind(namespace)
            .fetch_all(store.pool())
            .await?;
            for (name, desc) in rows {
                println!("{:<22} {}", name, desc.unwrap_or_default());
            }
        }
        TypeCommand::Propose {
            name,
            schema,
            by,
            description,
        } => {
            let schema_json: serde_json::Value = serde_json::from_str(&schema)?;
            let proposal_attrs = serde_json::json!({
                "proposed_type": name,
                "proposed_schema": schema_json,
                "proposed_by": by,
                "description": description,
                "status": "pending",
            });
            let proposal_name = format!("proposal:{}", name);
            let input = EntityInput::new(namespace, "metamodel.proposal", &proposal_name)
                .with_attrs(proposal_attrs);
            let entity = store.put_entity(input, Source::new_cli(actor)).await?;
            println!("proposal created:");
            println!("  id:   {}", entity.id);
            println!("  type: {}", name);
            println!("\napprove with: cmdb type approve {}", entity.id);
        }
        TypeCommand::Proposals => {
            let filter = QueryFilter::new().in_namespace(namespace).of_type("metamodel.proposal");
            let proposals = store.query_entities(filter).await?;
            if proposals.is_empty() {
                println!("(no proposals)");
                return Ok(());
            }
            for p in proposals {
                let status = p.attrs.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                let proposed = p.attrs.get("proposed_type").and_then(|v| v.as_str()).unwrap_or("?");
                println!(
                    "{:<28} {:<14} {:<24} by={}",
                    p.id.to_string(),
                    status,
                    proposed,
                    p.attrs.get("proposed_by").and_then(|v| v.as_str()).unwrap_or("?")
                );
            }
        }
        TypeCommand::Approve { id, by } => {
            let proposal_id: cmdb_core::id::EntityId = id.parse()?;
            let proposal = store
                .get_entity_by_id(proposal_id)
                .await?
                .ok_or_else(|| anyhow!("proposal not found: {}", proposal_id))?;

            if proposal.entity_type != "metamodel.proposal" {
                return Err(anyhow!("not a proposal: {}", proposal.entity_type));
            }
            let status = proposal.attrs.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if status == "approved" {
                return Err(anyhow!("already approved"));
            }

            let proposed_type = proposal
                .attrs
                .get("proposed_type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("proposal missing proposed_type"))?
                .to_string();
            let proposed_schema = proposal
                .attrs
                .get("proposed_schema")
                .cloned()
                .ok_or_else(|| anyhow!("proposal missing proposed_schema"))?;
            let description: Option<String> = proposal
                .attrs
                .get("description")
                .and_then(|v| v.as_str())
                .map(String::from);
            let namespace = proposal.namespace.clone();

            // Register the type.
            sqlx::query(
                r#"INSERT INTO entity_types (namespace, name, description, attrs_schema)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (namespace, name) DO UPDATE
                     SET attrs_schema = EXCLUDED.attrs_schema,
                         description = EXCLUDED.description,
                         updated_at = NOW()"#,
            )
            .bind(&namespace)
            .bind(&proposed_type)
            .bind(&description)
            .bind(&proposed_schema)
            .execute(store.pool())
            .await?;

            // Mark proposal as approved.
            let mut new_attrs = proposal.attrs.clone();
            new_attrs["status"] = serde_json::json!("approved");
            new_attrs["decided_by"] = serde_json::json!(by);
            new_attrs["decided_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
            let input = EntityInput::new(&namespace, "metamodel.proposal", &proposal.name)
                .with_attrs(new_attrs);
            store
                .put_entity(input, Source::new_cli(&by))
                .await?;

            println!("approved: {} -> registered type {}/{}", proposal_id, namespace, proposed_type);
        }
        TypeCommand::Reject { id, by } => {
            let proposal_id: cmdb_core::id::EntityId = id.parse()?;
            let proposal = store
                .get_entity_by_id(proposal_id)
                .await?
                .ok_or_else(|| anyhow!("proposal not found: {}", proposal_id))?;

            let mut new_attrs = proposal.attrs.clone();
            new_attrs["status"] = serde_json::json!("rejected");
            new_attrs["decided_by"] = serde_json::json!(by);
            new_attrs["decided_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
            let input = EntityInput::new(&proposal.namespace, "metamodel.proposal", &proposal.name)
                .with_attrs(new_attrs);
            store.put_entity(input, Source::new_cli(&by)).await?;

            println!("rejected: {}", proposal_id);
        }
    }
    Ok(())
}
