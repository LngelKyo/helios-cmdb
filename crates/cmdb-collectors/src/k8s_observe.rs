//! k8s-observe collector: shell out to `kubectl` to list nodes / pods /
//! services and ingest as entities.
//!
//! P2 simplicity: invoke kubectl, parse JSON output. P3 may switch to the
//! `kube` crate for proper client behavior (watches, informers).

use crate::CollectorConfig;
use anyhow::{anyhow, Result};
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::source::{Source, SourceKind, Transport};
use cmdb_core::Store;
use chrono::Utc;
use serde_json::Value;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

pub async fn run(store: Arc<dyn Store>, cfg: CollectorConfig) -> Result<()> {
    let kubeconfig = std::env::var("KUBECONFIG").or_else(|_| std::env::var("HOME").map(|h| format!("{h}/.kube/config"))).ok();
    let namespaces = if cfg.targets.is_empty() {
        vec!["default".to_string()]
    } else {
        cfg.targets.clone()
    };

    loop {
        if let Err(e) = observe_once(&store, &cfg, &namespaces, kubeconfig.as_deref()).await {
            tracing::warn!(error = %e, "k8s-observe: tick failed");
        }
        if cfg.interval_seconds == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_seconds)).await;
    }
    Ok(())
}

async fn observe_once(
    store: &Arc<dyn Store>,
    cfg: &CollectorConfig,
    namespaces: &[String],
    kubeconfig: Option<&str>,
) -> Result<()> {
    let source = make_source(cfg);

    // Nodes (cluster-scoped; observe once)
    if let Ok(nodes) = kubectl_json::<Value>(kubeconfig, &["get", "nodes", "-o", "json"]) {
        if let Some(items) = nodes.get("items").and_then(|i| i.as_array()) {
            for item in items {
                if let Some(node) = ingest_node(store, cfg, item, &source).await? {
                    // For each node, also relate pods that run on it (we do this in pod loop below).
                    let _ = node;
                }
            }
        }
    }

    for ns in namespaces {
        if let Ok(pods) = kubectl_json::<Value>(kubeconfig, &["get", "pods", "-n", ns, "-o", "json"]) {
            if let Some(items) = pods.get("items").and_then(|i| i.as_array()) {
                for item in items {
                    ingest_pod(store, cfg, item, &source).await?;
                }
            }
        }
        if let Ok(services) = kubectl_json::<Value>(kubeconfig, &["get", "services", "-n", ns, "-o", "json"]) {
            if let Some(items) = services.get("items").and_then(|i| i.as_array()) {
                for item in items {
                    ingest_service(store, cfg, item, &source).await?;
                }
            }
        }
    }
    Ok(())
}

async fn ingest_node(
    store: &Arc<dyn Store>,
    cfg: &CollectorConfig,
    item: &Value,
    source: &Source,
) -> Result<Option<cmdb_core::id::EntityId>> {
    let name = item.pointer("/metadata/name").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("node missing name"))?;
    let labels = item.pointer("/metadata/labels").cloned().unwrap_or(Value::Null);
    let internal_ip = item
        .pointer("/status/addresses")
        .and_then(|a| a.as_array())
        .and_then(|arr| {
            arr.iter().find_map(|a| {
                if a.get("type").and_then(|t| t.as_str()) == Some("InternalIP") {
                    a.get("address").and_then(|v| v.as_str()).map(String::from)
                } else {
                    None
                }
            })
        });
    let cpus = item
        .pointer("/status/capacity/cpu")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    let mem = item
        .pointer("/status/capacity/memory")
        .and_then(|v| v.as_str());

    let attrs = serde_json::json!({
        "kind": "k8s_node",
        "internal_ip": internal_ip,
        "cpus": cpus,
        "memory": mem,
        "labels": labels,
    });

    let input = EntityInput::new(&cfg.namespace, "fleet.host", name).with_attrs(attrs);
    let entity = store.put_entity(input, source.clone()).await?;
    Ok(Some(entity.id))
}

async fn ingest_pod(
    store: &Arc<dyn Store>,
    cfg: &CollectorConfig,
    item: &Value,
    source: &Source,
) -> Result<()> {
    let name = item.pointer("/metadata/name").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("pod missing name"))?;
    let ns = item.pointer("/metadata/namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let node = item.pointer("/spec/nodeName").and_then(|v| v.as_str());
    let phase = item.pointer("/status/phase").and_then(|v| v.as_str()).unwrap_or("Unknown");
    let ip = item.pointer("/status/podIP").and_then(|v| v.as_str());
    let containers: Vec<String> = item
        .pointer("/spec/containers")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let attrs = serde_json::json!({
        "namespace": ns,
        "node": node,
        "phase": phase,
        "pod_ip": ip,
        "containers": containers,
    });

    let pod_input = EntityInput::new(&cfg.namespace, "infra.pod", name).with_attrs(attrs);
    let pod_entity = store.put_entity(pod_input, source.clone()).await?;

    if let Some(node_name) = node {
        let host_input = EntityInput::new(&cfg.namespace, "fleet.host", node_name);
        let host_entity = store.put_entity(host_input, source.clone()).await?;
        let _ = store
            .put_relation(cmdb_core::relation::RelationInput {
                namespace: cfg.namespace.clone(),
                from: EntityRef::by_id(pod_entity.id),
                to: EntityRef::by_id(host_entity.id),
                relation_type: "runs_on".into(),
                props: serde_json::json!({"source": "k8s-observe"}),
            })
            .await;
    }
    Ok(())
}

async fn ingest_service(
    store: &Arc<dyn Store>,
    cfg: &CollectorConfig,
    item: &Value,
    source: &Source,
) -> Result<()> {
    let name = item.pointer("/metadata/name").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("service missing name"))?;
    let ns = item.pointer("/metadata/namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let cluster_ip = item.pointer("/spec/clusterIP").and_then(|v| v.as_str());
    let ports: Vec<Value> = item
        .pointer("/spec/ports")
        .and_then(|p| p.as_array())
        .map(|a| a.iter().cloned().collect())
        .unwrap_or_default();

    let attrs = serde_json::json!({
        "namespace": ns,
        "cluster_ip": cluster_ip,
        "ports": ports,
        "kind": "k8s_service",
    });

    let input = EntityInput::new(&cfg.namespace, "infra.service", name).with_attrs(attrs);
    store.put_entity(input, source.clone()).await?;
    Ok(())
}

fn kubectl_json<T: serde::de::DeserializeOwned>(
    kubeconfig: Option<&str>,
    args: &[&str],
) -> Result<T> {
    let mut cmd = Command::new("kubectl");
    if let Some(kc) = kubeconfig {
        cmd.env("KUBECONFIG", kc);
    }
    let output = cmd.args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("kubectl {} failed: {}", args.join(" "), stderr.trim()));
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn make_source(cfg: &CollectorConfig) -> Source {
    Source {
        kind: SourceKind::Collector,
        identity: format!("collector.k8s-observe.{}", cfg.actor),
        transport: Transport::Cli,
        nats_subject: None,
        observed_at: Utc::now(),
        confidence: 0.9,
        ttl_seconds: Some(300),
        sig: None,
        evidence_ref: None,
    }
}
