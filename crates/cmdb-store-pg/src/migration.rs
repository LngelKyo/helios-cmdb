//! Migration runner.

use anyhow::Result;

pub async fn run(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}

