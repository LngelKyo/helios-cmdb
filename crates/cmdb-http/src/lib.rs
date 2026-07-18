//! helios-cmdb HTTP server: REST + GraphQL + Web UI + metrics on one axum router.

pub mod gql;
pub mod metrics;
pub mod rest;
pub mod ui;

use anyhow::Result;
use cmdb_core::Store;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct HttpOptions {
    pub require_auth: bool,
    pub serve_ui: bool,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            require_auth: false,
            serve_ui: true,
        }
    }
}

pub async fn run(store: Arc<dyn Store>, actor: String, addr: SocketAddr) -> Result<()> {
    run_with(store, actor, addr, HttpOptions::default()).await
}

pub async fn run_with(
    store: Arc<dyn Store>,
    actor: String,
    addr: SocketAddr,
    opts: HttpOptions,
) -> Result<()> {
    rest::run_with_options(store, actor, addr, opts).await
}

pub async fn run_with_options_and_pool(
    store: Arc<dyn Store>,
    pool: Option<sqlx::PgPool>,
    actor: String,
    addr: SocketAddr,
    opts: HttpOptions,
) -> Result<()> {
    rest::run_with_options_and_pool(store, pool, actor, addr, opts).await
}
