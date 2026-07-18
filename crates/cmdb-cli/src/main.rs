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

mod commands;
mod output;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "cmdb", version, about = "helios-cmdb CLI", long_about = None)]
pub struct Cli {
    #[arg(long, env = "CMDB_DATABASE_URL", global = true)]
    database_url: Option<String>,

    #[arg(long, env = "CMDB_NAMESPACE", global = true, default_value = "cc.fleet")]
    namespace: String,

    #[arg(long, env = "CMDB_ACTOR", global = true, default_value = "user:cli")]
    actor: String,

    #[command(subcommand)]
    command: Command,
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
