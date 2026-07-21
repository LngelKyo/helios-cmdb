//! Change-event bridge: PG LISTEN/NOTIFY → NATS publish.
//!
//! Subscribes to the `cmdb_changes` PostgreSQL NOTIFY channel (populated by
//! the triggers in migration 0005) and republishes each event as an ana
//! **Alert** envelope on the NATS subject `<prefix>.cmdb.alert.change`.
//!
//! Fleet agents with ana-listener receive it as a typed Alert (not Unknown).

use anyhow::Result;
use async_nats::{Subject};
use cmdb_core::Store;
use serde_json::json;
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
                let pg_payload: serde_json::Value =
                    serde_json::from_str(notification.payload()).unwrap_or_else(|_| {
                        json!({"raw": notification.payload()})
                    });
                // Wrap in ana Alert envelope so ana-listener parse_envelope
                // accepts it as a typed Alert, not Unknown.
                let alert = json!({
                    "type": "alert",
                    "from": "cmdb",
                    "ts": crate::envelopes::now_iso(),
                    "event": "change",
                    "level": "info",
                    "data": pg_payload,
                    "origin_subject": null,
                });
                let payload_bytes = serde_json::to_vec(&alert).unwrap_or_default();
                if let Err(e) = nats
                    .publish(subject.clone(), payload_bytes.into())
                    .await
                {
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
