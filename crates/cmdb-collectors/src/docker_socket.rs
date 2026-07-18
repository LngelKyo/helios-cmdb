//! docker-socket collector: query local docker daemon via unix socket.
//!
//! Uses `docker ps --format json` (line-delimited JSON, one container per
//! line). Each container becomes an `infra.container` entity. The local
//! hostname becomes a `fleet.host` and the container `runs_on` it.
//!
//! P3 may switch to a real HTTP client against /var/run/docker.sock.

use crate::CollectorConfig;
use anyhow::{anyhow, Result};
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::source::{Source, SourceKind, Transport};
use cmdb_core::Store;
use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

pub async fn run(store: Arc<dyn Store>, cfg: CollectorConfig) -> Result<()> {
    let host = hostname_or_default();
    loop {
        if let Err(e) = observe_once(&store, &cfg, &host).await {
            tracing::warn!(error = %e, "docker-socket: tick failed");
        }
        if cfg.interval_seconds == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_seconds)).await;
    }
    Ok(())
}

async fn observe_once(store: &Arc<dyn Store>, cfg: &CollectorConfig, host: &str) -> Result<()> {
    let source = make_source(cfg, host);

    let host_entity = store
        .put_entity(
            EntityInput::new(&cfg.namespace, "fleet.host", host)
                .with_attrs(serde_json::json!({"kind": "docker_host"})),
            source.clone(),
        )
        .await?;

    let output = Command::new("docker")
        .args(["ps", "--format", "json"])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("docker ps failed: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: DContainer = match serde_json::from_str(line) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, line = %line, "docker ps: parse failed");
                continue;
            }
        };
        let attrs = serde_json::json!({
            "image": parsed.image,
            "command": parsed.command,
            "status": parsed.status,
            "ports": parsed.ports,
            "labels": parsed.labels.unwrap_or_default(),
        });
        let container_entity = store
            .put_entity(
                EntityInput::new(&cfg.namespace, "infra.container", &parsed.names)
                    .with_attrs(attrs),
                source.clone(),
            )
            .await?;
        let _ = store
            .put_relation(cmdb_core::relation::RelationInput {
                namespace: cfg.namespace.clone(),
                from: EntityRef::by_id(container_entity.id),
                to: EntityRef::by_id(host_entity.id),
                relation_type: "runs_on".into(),
                props: serde_json::json!({"source": "docker-socket"}),
            })
            .await;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DContainer {
    #[serde(default, rename = "ID")]
    id: String,
    #[serde(default, rename = "Names")]
    names: String,
    #[serde(default, rename = "Image")]
    image: String,
    #[serde(default, rename = "Command")]
    command: String,
    #[serde(default, rename = "Status")]
    status: String,
    #[serde(default, rename = "Ports")]
    ports: String,
    #[serde(default, rename = "Labels")]
    labels: Option<Value>,
}

fn hostname_or_default() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("CMDB_HOST"))
        .unwrap_or_else(|_| {
            Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| "unknown".into())
        })
}

fn make_source(_cfg: &CollectorConfig, host: &str) -> Source {
    Source {
        kind: SourceKind::Collector,
        identity: format!("collector.docker.{}", host),
        transport: Transport::Cli,
        nats_subject: None,
        observed_at: Utc::now(),
        confidence: 0.9,
        ttl_seconds: Some(120),
        sig: None,
        evidence_ref: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_docker_ps_line() {
        let line = r#"{"Command":"postgres","CreatedAt":"2026-07-18T00:00:00Z","ID":"abc123","Image":"postgres:17","Labels":"com.docker.compose.service=pg","LocalVolumes":"1","Mounts":"/var/lib/postgresql/data","Names":"cmdb-pg","Networks":"bridge","Ports":"5432/tcp","RunningFor":"2 days","Size":"0B","Status":"Up 2 days"}"#;
        let c: DContainer = serde_json::from_str(line).unwrap();
        assert_eq!(c.names, "cmdb-pg");
        assert_eq!(c.image, "postgres:17");
    }
}
