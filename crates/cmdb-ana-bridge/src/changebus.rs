//! Change-event bridge: PG LISTEN/NOTIFY → NATS publish.
//!
//! Subscribes to the `cmdb_changes` PostgreSQL NOTIFY channel (populated by
//! the triggers in migration 0005) and republishes each event as a JSON
//! message on the NATS subject `<prefix>.cmdb.alert.change`.
//!
//! Fleet agents can then subscribe to `<prefix>.cmdb.alert.>` to receive
//! real-time CMDB change notifications.

use anyhow::Result;
use async_nats::{Client, Subject};
use cmdb_core::Store;
use sqlx::postgres::PgListener;
use std::sync::Arc;

pub async fn run(
    _store: Arc<dyn Store>,
    pg_url: &str,
    nats_url: &str,
    prefix: &str,
) -> Result<()> {
    let nats = async_nats::connect(nats_url).await?;
    let mut listener = PgListener::connect(pg_url).await?;
    listener.listen("cmdb_changes").await?;

    tracing::info!(%nats_url, %pg_url, prefix, "changebus up");

    let subject = Subject::from(format!("{prefix}.cmdb.alert.change"));
    loop {
        match listener.recv().await {
            Ok(notification) => {
                let payload = notification.payload();
                if let Err(e) = nats.publish(subject.clone(), payload.to_string().into()).await {
                    tracing::warn!(error = %e, "NATS publish failed");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "PG listener recv error; reconnecting");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

