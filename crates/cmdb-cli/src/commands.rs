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
use serde_json::Value;
use std::str::FromStr;

pub async fn dispatch(cli: Cli) -> Result<()> {
    let namespace = cli.namespace.clone();
    let actor = cli.actor.clone();
    let store = open_store(&cli).await?;

    match cli.command {
        Command::Migrate => migrate(&store).await?,
        Command::PutEntity(args) => put_entity(&namespace, &actor, &store, args).await?,
        Command::Get(args) => get(&store, args).await?,
        Command::List(args) => list(&namespace, &store, args).await?,
        Command::Relate(args) => relate(&namespace, &store, args).await?,
        Command::Query(args) => query(&store, args).await?,
        Command::Serve(args) => serve(&namespace, &actor, &store, args).await?,
        Command::Collector(args) => run_collector(&namespace, &actor, &store, args).await?,
        Command::Tui => {
            let store_arc: std::sync::Arc<dyn Store> = std::sync::Arc::new(store.clone());
            cmdb_tui::run(store_arc, namespace.clone()).await?;
        }
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

async fn list(namespace: &str, store: &PgStore, args: ListArgs) -> Result<()> {
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
    output::entities(&entities);
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

async fn query(store: &PgStore, args: QueryArgs) -> Result<()> {
    if let Some(cypher) = &args.cypher {
        let rows = store.cypher(cypher).await?;
        if rows.is_empty() {
            println!("(no rows)");
            return Ok(());
        }
        let cols = rows[0].len();
        let mut widths = vec![0usize; cols];
        for r in &rows {
            for (i, c) in r.iter().enumerate() {
                widths[i] = widths[i].max(c.len().min(60));
            }
        }
        for r in &rows {
            let cells: Vec<String> = r
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{:<width$}", c.chars().take(60).collect::<String>(), width = widths[i]))
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
}

#[derive(Args, Debug)]
pub struct McpArgs {
    #[arg(long, default_value = "stdio")]
    pub transport: String,
    #[arg(long, default_value = "127.0.0.1:8765")]
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
    #[arg(long, default_value = "127.0.0.1:8766")]
    pub addr: String,
}

async fn serve(namespace: &str, actor: &str, store: &PgStore, args: ServeArgs) -> Result<()> {
    let store: std::sync::Arc<dyn Store> = std::sync::Arc::new(store.clone());
    match args.command {
        ServeCommand::Mcp(m) => {
            let mcp_actor = format!("{actor}/mcp:{namespace}");
            match m.transport.as_str() {
                "stdio" => cmdb_mcp::serve_stdio(store, mcp_actor).await?,
                "http" => {
                    let addr: std::net::SocketAddr = m.addr.parse()?;
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
            cmdb_http::run(store, format!("{actor}/http:{namespace}"), addr).await?;
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
        }        CollectorCommand::Run {
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
