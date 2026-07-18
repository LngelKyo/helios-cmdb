//! helios-cmdb CLI.
//!
//! P0 commands:
//!   cmdb migrate                          apply migrations
//!   cmdb put-entity --type T --name N     insert/update entity
//!   cmdb get --type T --name N            fetch entity by name
//!   cmdb get --id <ulid>                  fetch entity by id
//!   cmdb relate <from> <type> <to>        create relation
//!   cmdb query --traverse --from <id>     graph traversal
//!   cmdb list --type T                    list entities
//!
//! P1 commands:
//!   cmdb serve mcp --transport stdio      MCP server (for Claude Code etc.)
//!   cmdb serve mcp --transport http --addr 0.0.0.0:8765
//!   cmdb serve bus                        subscribe to cc.fleet.> and serve RPC
//!   cmdb collector run ssh-facts --target host1,host2

mod commands;
mod output;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "cmdb", version, about = "helios-cmdb CLI", long_about = None)]
pub struct Cli {
    #[arg(long, env = "CMDB_DATABASE_URL", global = true)]
    pub database_url: Option<String>,

    #[arg(long, env = "CMDB_NAMESPACE", global = true, default_value = "cc.fleet")]
    pub namespace: String,

    #[arg(long, env = "CMDB_ACTOR", global = true, default_value = "user:cli")]
    pub actor: String,

    /// Disable semantic embedding (skip embedder init; vector search returns empty).
    #[arg(long, env = "CMDB_NO_EMBED", global = true, default_value_t = false)]
    pub no_embed: bool,

    /// Output everything as JSON (where supported). Currently affects
    /// `query --cypher` and `list`.
    #[arg(long, global = true, default_value_t = false)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Apply pending migrations to the database
    Migrate,
    /// Insert or update an entity
    PutEntity(commands::PutEntityArgs),
    /// Get an entity by name or id
    Get(commands::GetArgs),
    /// List entities matching a filter
    List(commands::ListArgs),
    /// Create a relation between two entities
    Relate(commands::RelateArgs),
    /// Run a graph traversal from an entity
    Query(commands::QueryArgs),
    /// Run a server (MCP, ana bus, ...)
    Serve(commands::ServeArgs),
    /// Run collectors (ssh-facts, k8s_observe, ...)
    Collector(commands::CollectorArgs),
    /// Interactive TUI browser
    Tui,
    /// Manage scoped API tokens (create / list / revoke)
    Token(commands::TokenArgs),
    /// Entity-type governance (propose / approve / reject)
    Type(commands::TypeArgs),
    /// Bulk-insert test entities for load testing (uses generate_series).
    Stress(commands::StressArgs),
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(commands::dispatch(cli))
}
