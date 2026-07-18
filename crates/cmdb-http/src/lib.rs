//! helios-cmdb HTTP server: REST + GraphQL on the same axum router.

pub mod gql;
pub mod rest;

use anyhow::Result;
use cmdb_core::Store;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn run(store: Arc<dyn Store>, actor: String, addr: SocketAddr) -> Result<()> {
    rest::run(store, actor, addr).await
}

pub async fn run_gql(store: Arc<dyn Store>, addr: SocketAddr) -> Result<()> {
    gql::run(store, addr).await
}
