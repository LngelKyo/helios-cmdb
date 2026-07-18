//! Pluggable collectors. Each collector runs as a `cmdb collector run <name>`
//! subcommand.
//!
//! P1 ships `ssh-facts` (gather uname/disk/docker ps from hosts via SSH).

pub mod ssh_facts;

use async_trait::async_trait;
use cmdb_core::Store;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CollectorConfig {
    pub namespace: String,
    pub actor: String,
    pub targets: Vec<String>,
    pub interval_seconds: u64,
    pub ssh_user: Option<String>,
    pub ssh_port: u16,
}

pub struct CollectorInfo {
    pub name: &'static str,
    pub description: &'static str,
}

pub fn list() -> Vec<CollectorInfo> {
    vec![
        CollectorInfo {
            name: "ssh-facts",
            description: "SSH into hosts and gather uname/disk/load/docker ps facts.",
        },
    ]
}

pub async fn run(
    name: &str,
    store: Arc<dyn Store>,
    cfg: CollectorConfig,
) -> anyhow::Result<()> {
    match name {
        "ssh-facts" => ssh_facts::run(store, cfg).await,
        other => anyhow::bail!("unknown collector: {other}"),
    }
}

#[async_trait]
pub trait Collector: Send + Sync {
    async fn tick(&self, store: &dyn Store, cfg: &CollectorConfig) -> anyhow::Result<()>;
}
