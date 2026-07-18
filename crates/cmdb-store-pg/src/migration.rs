//! Migration runner. Wraps `sqlx::migrate!` so the CLI can run
//! `cmdb migrate` without needing `sqlx-cli` installed.

use anyhow::Result;
use sqlx::postgres::PgPool;

pub async fn run(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}
