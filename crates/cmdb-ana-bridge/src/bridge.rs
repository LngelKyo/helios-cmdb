//! ana bus bridge — main service loop.

use crate::envelopes::{parse_envelope, now_iso, EnvelopeBase, ParsedEnvelope, Reply};
use crate::subjects::SubjectScheme;
use anyhow::Result;
use async_nats::Subject;
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::fact::FactInput;
use cmdb_core::id::EntityId;
use cmdb_core::source::{Source, SourceKind, Transport};
use cmdb_core::store::{Direction, QueryFilter, TraverseStep};
use cmdb_core::Store;
use chrono::Utc;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;

pub async fn serve_bus(
    store: Arc<dyn Store>,
    nats_url: &str,
    identity: &str,
    prefix: &str,
    nats_token: Option<&str>,
    nats_creds: Option<&str>,
) -> Result<()> {
    let mut opts = async_nats::ConnectOptions::new()
        .name(format!("{identity}-cmdb-bridge"));
    if let Some(token) = nats_token {
        opts = opts.token(token.to_string());
    }
    if let Some(creds) = nats_creds {
        opts = opts.credentials_file(creds).await?;
    }
    let client = opts.connect(nats_url).await?;
    let scheme = Arc::new(SubjectScheme::new(prefix));
    let identity = Arc::new(identity.to_string());

    tracing::info!(%nats_url, prefix = %scheme.prefix, identity = %identity, "ana bridge up");

    // ---- subscriptions ----------------------------------------------------
    let mut sub_discovery = subscribe(&client, &format!("{}.*.discovery", scheme.prefix)).await?;
    let mut sub_pulse = subscribe(&client, &format!("{}.*.pulse", scheme.prefix)).await?;
    let mut sub_query = subscribe(
        &client,
        &format!("{}.{}.query.>", scheme.prefix, identity),
    )
    .await?;

    let store_d = store.clone();
    let scheme_d = scheme.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_discovery.next().await {
            if let Err(e) = handle_discovery(&store_d, &scheme_d, &msg).await {
                tracing::warn!(error = %e, "discovery handler");
            }
        }
    });

    let store_p = store.clone();
    let scheme_p = scheme.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_pulse.next().await {
            if let Err(e) = handle_pulse(&store_p, &scheme_p, &msg).await {
                tracing::warn!(error = %e, "pulse handler");
            }
        }
    });

    let store_q = store.clone();
    let scheme_q = scheme.clone();
    let identity_q = identity.clone();
    let client_q = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_query.next().await {
            if let Err(e) = handle_query(&store_q, &scheme_q, &identity_q, &client_q, msg).await {
                tracing::warn!(error = %e, "query handler");
            }
        }
    });

    // publish our own discovery so other agents learn we're here
    let _ = publish_discovery(&client, &scheme, &identity).await;

    // run forever
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    Ok(())
}

async fn subscribe(
    client: &async_nats::Client,
    pattern: &str,
) -> Result<async_nats::Subscriber> {
    Ok(client.subscribe(Subject::from(pattern.to_string())).await?)
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

async fn handle_discovery(
    store: &Arc<dyn Store>,
    scheme: &SubjectScheme,
    msg: &async_nats::Message,
) -> anyhow::Result<()> {
    let ParsedEnvelope::Discovery(env) = parse_envelope(&msg.payload)? else {
        return Ok(());
    };

    let parsed = scheme.parse(&msg.subject.to_string());
    let identity = parsed.agent.as_deref().unwrap_or(&env.base.from);

    let attrs = json!({
        "role": env.role,
        "cluster": env.cluster,
        "subjects_owned": env.subjects_owned,
        "capabilities": env.capabilities,
        "host": env.base.host,
    });

    let source = Source {
        kind: SourceKind::Agent,
        identity: identity.to_string(),
        transport: Transport::Nats,
        nats_subject: Some(msg.subject.to_string()),
        observed_at: Utc::now(),
        confidence: 0.9,
        ttl_seconds: Some(600),
        sig: None,
        evidence_ref: None,
    };

    let entity = store
        .put_entity(
            EntityInput::new("cc.fleet", "fleet.agent", identity).with_attrs(attrs),
            source,
        )
        .await?;

    // If cluster present, upsert cluster entity + in_cluster relation.
    if let Some(cluster) = &env.cluster {
        let cluster_e = store
            .put_entity(
                EntityInput::new("cc.fleet", "fleet.cluster", cluster),
                Source::new_agent(identity),
            )
            .await?;
        let _ = store
            .put_relation(cmdb_core::relation::RelationInput {
                namespace: "cc.fleet".into(),
                from: EntityRef::by_id(entity.id),
                to: EntityRef::by_id(cluster_e.id),
                relation_type: "in_cluster".into(),
                props: json!({}),
            })
            .await;
    }

    // Derive runs_on host from envelope's `host` field.
    if let Some(host) = &env.base.host {
        let host_e = store
            .put_entity(
                EntityInput::new("cc.fleet", "fleet.host", host),
                Source::new_agent(identity),
            )
            .await?;
        let _ = store
            .put_relation(cmdb_core::relation::RelationInput {
                namespace: "cc.fleet".into(),
                from: EntityRef::by_id(entity.id),
                to: EntityRef::by_id(host_e.id),
                relation_type: "runs_on".into(),
                props: json!({"source": "discovery.host"}),
            })
            .await;
    }

    tracing::debug!(agent = %identity, "discovery ingested");
    Ok(())
}

async fn handle_pulse(
    store: &Arc<dyn Store>,
    scheme: &SubjectScheme,
    msg: &async_nats::Message,
) -> anyhow::Result<()> {
    let ParsedEnvelope::Pulse(env) = parse_envelope(&msg.payload)? else {
        return Ok(());
    };

    let parsed = scheme.parse(&msg.subject.to_string());
    let identity = parsed.agent.as_deref().unwrap_or(&env.base.from);

    let agent = store
        .get_entity("cc.fleet", "fleet.agent", identity)
        .await?;
    let Some(agent) = agent else {
        // We haven't seen discovery yet; skip until then.
        return Ok(());
    };

    let source = Source {
        kind: SourceKind::Agent,
        identity: identity.to_string(),
        transport: Transport::Nats,
        nats_subject: Some(msg.subject.to_string()),
        observed_at: Utc::now(),
        confidence: 0.9,
        ttl_seconds: Some(300),
        sig: None,
        evidence_ref: None,
    };

    if let Some(activity) = env.activity {
        let _ = store
            .add_fact(FactInput {
                namespace: "cc.fleet".into(),
                entity: EntityRef::by_id(agent.id),
                key: "activity".into(),
                value: json!(activity),
                source: source.clone(),
            })
            .await;
    }
    if let Some(state) = env.state {
        let _ = store
            .add_fact(FactInput {
                namespace: "cc.fleet".into(),
                entity: EntityRef::by_id(agent.id),
                key: "state".into(),
                value: json!(state),
                source: source.clone(),
            })
            .await;
    }

    // Refresh runs_on host if the pulse advertised a different host.
    if let Some(host) = &env.base.host {
        let host_e = store
            .put_entity(
                EntityInput::new("cc.fleet", "fleet.host", host),
                Source::new_agent(identity),
            )
            .await?;
        let _ = store
            .put_relation(cmdb_core::relation::RelationInput {
                namespace: "cc.fleet".into(),
                from: EntityRef::by_id(agent.id),
                to: EntityRef::by_id(host_e.id),
                relation_type: "runs_on".into(),
                props: json!({"source": "pulse.host"}),
            })
            .await;
    }

    Ok(())
}

async fn handle_query(
    store: &Arc<dyn Store>,
    scheme: &SubjectScheme,
    self_identity: &str,
    client: &async_nats::Client,
    msg: async_nats::Message,
) -> anyhow::Result<()> {
    let ParsedEnvelope::Query(env) = parse_envelope(&msg.payload)? else {
        return Ok(());
    };

    // v0.7: send Ack immediately so the caller's
    // query_and_wait(accept_ack=True) doesn't timeout while CMDB computes.
    let topic = parsed_topic(&msg.subject.to_string(), scheme);
    let ack_subject = scheme.verb(self_identity, "ack", Some(&topic));
    let ack = serde_json::json!({
        "type": "ack",
        "from": self_identity,
        "ts": now_iso(),
        "ack_for": msg.subject.to_string(),
        "note": "cmdb received, processing",
        "alive": true,
    });
    let _ = client
        .publish(
            Subject::from(ack_subject),
            serde_json::to_vec(&ack)?.into(),
        )
        .await;

    let reply_subject = env.reply_to.clone().unwrap_or_else(|| {
        scheme.reply(self_identity, &topic)
    });

    let data = dispatch_query(store, &env.query).await;

    let reply = Reply {
        base: EnvelopeBase {
            kind: "reply".into(),
            from: self_identity.to_string(),
            ts: now_iso(),
            clock: None,
            host: None,
        },
        reply_for: Some(msg.subject.to_string()),
        in_reply_to: env.request_id.clone(),
        request_id: env.request_id,
        txn_id: env.txn_id.clone(),
        data,
        note: Some("helios-cmdb".into()),
        to: Some(env.base.from.clone()),
    };

    let payload = serde_json::to_vec(&reply)?;
    client
        .publish(Subject::from(reply_subject), payload.into())
        .await?;
    Ok(())
}

fn parsed_topic(subject: &str, scheme: &SubjectScheme) -> String {
    scheme
        .parse(subject)
        .topic
        .unwrap_or_else(|| "default".into())
}

async fn dispatch_query(store: &Arc<dyn Store>, query: &str) -> Value {
    let q = query.trim();

    // Try JSON first.
    if let Ok(v) = serde_json::from_str::<Value>(q) {
        return run_json_query(store, &v).await;
    }

    // Fall back to keyword heuristics for natural-language queries from
    // agents that don't speak the JSON shape.
    let lower = q.to_lowercase();
    if lower.contains("list") && lower.contains("agent") {
        let agents = store
            .query_entities(QueryFilter::new().in_namespace("cc.fleet").of_type("fleet.agent"))
            .await
            .map_err(|e| e.to_string())
            .unwrap_or_default();
        return json!({
            "agents": agents.iter().map(|a| &a.name).collect::<Vec<_>>(),
            "count": agents.len(),
        });
    }
    if lower.contains("list") && lower.contains("host") {
        let hosts = store
            .query_entities(QueryFilter::new().in_namespace("cc.fleet").of_type("fleet.host"))
            .await
            .map_err(|e| e.to_string())
            .unwrap_or_default();
        return json!({
            "hosts": hosts.iter().map(|h| &h.name).collect::<Vec<_>>(),
            "count": hosts.len(),
        });
    }

    // Fuzzy: try to find by name substring.
    let filter = QueryFilter::new().in_namespace("cc.fleet").with_limit(20);
    let entities = store
        .query_entities(filter)
        .await
        .map_err(|e| e.to_string())
        .unwrap_or_default();
    let q_lower = q.to_lowercase();
    let filtered: Vec<_> = entities
        .into_iter()
        .filter(|e| e.name.to_lowercase().contains(&q_lower))
        .collect();
    json!({
        "matched": filtered.iter().map(|e| json!({
            "type": e.entity_type,
            "name": e.name,
            "id": e.id.to_string(),
        })).collect::<Vec<_>>(),
        "hint": "for structured queries, send JSON like {\"op\":\"get_entity\",\"type\":\"fleet.agent\",\"name\":\"e15\"}",
    })
}

async fn run_json_query(store: &Arc<dyn Store>, v: &Value) -> Value {
    let op = v.get("op").and_then(|s| s.as_str()).unwrap_or("get_entity");
    let ns = v.get("namespace").and_then(|s| s.as_str()).unwrap_or("cc.fleet");
    match op {
        "get_entity" => {
            if let Some(id) = v.get("id").and_then(|s| s.as_str()) {
                if let Ok(id) = EntityId::from_str(id) {
                    return serde_json::to_value(
                        store.get_entity_by_id(id).await.ok().flatten(),
                    )
                    .unwrap_or(Value::Null);
                }
            }
            let t = v.get("type").and_then(|s| s.as_str()).unwrap_or("");
            let n = v.get("name").and_then(|s| s.as_str()).unwrap_or("");
            serde_json::to_value(store.get_entity(ns, t, n).await.ok().flatten())
                .unwrap_or(Value::Null)
        }
        "query" => {
            let mut filter = QueryFilter::new().in_namespace(ns).with_limit(
                v.get("limit").and_then(|x| x.as_u64()).unwrap_or(50) as u32,
            );
            if let Some(t) = v.get("type").and_then(|s| s.as_str()) {
                filter = filter.of_type(t);
            }
            serde_json::to_value(store.query_entities(filter).await.ok().unwrap_or_default())
                .unwrap_or(Value::Null)
        }
        "traverse" => {
            let from = if let Some(id) = v.get("from").and_then(|s| s.as_str()) {
                EntityId::from_str(id).ok()
            } else if let (Some(t), Some(n)) = (
                v.get("type").and_then(|s| s.as_str()),
                v.get("name").and_then(|s| s.as_str()),
            ) {
                store.get_entity(ns, t, n).await.ok().flatten().map(|e| e.id)
            } else {
                None
            };
            let from = match from {
                Some(id) => id,
                None => return json!({"error": "valid 'from' id or ('type'+'name') required"}),
            };
            let depth = v.get("depth").and_then(|x| x.as_u64()).unwrap_or(3) as u32;
            let direction = match v
                .get("direction")
                .and_then(|s| s.as_str())
                .unwrap_or("outgoing")
            {
                "incoming" | "in" => Direction::Incoming,
                "both" => Direction::Both,
                _ => Direction::Outgoing,
            };
            let step = TraverseStep {
                relation_type: v.get("relation_type").and_then(|s| s.as_str()).map(String::from),
                direction,
                max_depth: depth,
            };
            let hits = store.traverse(from, step).await.ok().unwrap_or_default();
            json!(hits.iter().map(|h| json!({
                "depth": h.depth,
                "entity": h.entity.name,
                "type": h.entity.entity_type,
                "via": h.via_relation_type,
            })).collect::<Vec<_>>())
        }
        "search" => {
            let q = v.get("q").and_then(|s| s.as_str()).unwrap_or("");
            let limit = v.get("limit").and_then(|x| x.as_u64()).unwrap_or(10) as u32;
            let hits = store.vector_search(q, ns, limit).await.ok().unwrap_or_default();
            json!(hits.iter().map(|h| json!({
                "score": h.score,
                "name": h.entity.name,
                "type": h.entity.entity_type,
            })).collect::<Vec<_>>())
        }
        "cypher" => {
            let q = v.get("query").and_then(|s| s.as_str()).unwrap_or("");
            match store.cypher(q).await {
                Ok(rows) => json!({"rows": rows, "count": rows.len()}),
                Err(e) => json!({"error": e.to_string()}),
            }
        }
        "history" => {
            let entity_id = v.get("entity_id")
                .and_then(|s| s.as_str())
                .and_then(|s| EntityId::from_str(s).ok());
            let limit = v.get("limit").and_then(|x| x.as_u64()).unwrap_or(20) as u32;
            let changes = store.history(Some(ns), entity_id, limit).await.ok().unwrap_or_default();
            json!(changes.iter().map(|c| json!({
                "ts": c.ts,
                "actor": c.actor,
                "op": c.op.as_str(),
                "target_type": c.target_type,
            })).collect::<Vec<_>>())
        }
        "facts" => {
            let entity_id = v.get("id")
                .and_then(|s| s.as_str())
                .and_then(|s| EntityId::from_str(s).ok());
            match entity_id {
                Some(eid) => {
                    let facts = store.effective_facts(eid, Default::default()).await.ok().unwrap_or_default();
                    json!(facts.iter().map(|f| json!({
                        "key": f.key,
                        "value": f.value,
                        "confidence": f.source.confidence,
                        "source": f.source.identity,
                    })).collect::<Vec<_>>())
                }
                None => {
                    // Try name-based lookup
                    let t = v.get("type").and_then(|s| s.as_str()).unwrap_or("");
                    let n = v.get("name").and_then(|s| s.as_str()).unwrap_or("");
                    match store.get_entity(ns, t, n).await.ok().flatten() {
                        Some(e) => {
                            let facts = store.effective_facts(e.id, Default::default()).await.ok().unwrap_or_default();
                            json!(facts.iter().map(|f| json!({
                                "key": f.key,
                                "value": f.value,
                                "confidence": f.source.confidence,
                                "source": f.source.identity,
                            })).collect::<Vec<_>>())
                        }
                        None => json!({"error": "entity not found"}),
                    }
                }
            }
        }
        "relations" => {
            let t = v.get("type").and_then(|s| s.as_str()).unwrap_or("");
            let n = v.get("name").and_then(|s| s.as_str()).unwrap_or("");
            match store.get_entity(ns, t, n).await.ok().flatten() {
                Some(e) => {
                    let step = TraverseStep {
                        relation_type: None,
                        direction: Direction::Both,
                        max_depth: 1,
                    };
                    let hits = store.traverse(e.id, step).await.ok().unwrap_or_default();
                    json!(hits.iter().map(|h| json!({
                        "entity": h.entity.name,
                        "type": h.entity.entity_type,
                        "via": h.via_relation_type,
                    })).collect::<Vec<_>>())
                }
                None => json!({"error": "entity not found"}),
            }
        }
        "upsert_entity" => {
            let entity_type = v.get("type").and_then(|s| s.as_str()).unwrap_or("");
            let entity_name = v.get("name").and_then(|s| s.as_str()).unwrap_or("");
            let attrs = v.get("attrs").cloned().unwrap_or(json!({}));
            let input = cmdb_core::entity::EntityInput::new(ns, entity_type, entity_name)
                .with_attrs(attrs);
            let source = cmdb_core::source::Source::new_agent("cmdb-bus");
            match store.put_entity(input, source).await {
                Ok(e) => serde_json::to_value(&e).unwrap_or(Value::Null),
                Err(e) => json!({"error": e.to_string()}),
            }
        }
        "relate" => {
            let from_type = v.get("from_type").and_then(|s| s.as_str()).unwrap_or("");
            let from_name = v.get("from_name").and_then(|s| s.as_str()).unwrap_or("");
            let to_type = v.get("to_type").and_then(|s| s.as_str()).unwrap_or("");
            let to_name = v.get("to_name").and_then(|s| s.as_str()).unwrap_or("");
            let rel_type = v.get("relation_type").and_then(|s| s.as_str()).unwrap_or("");
            let input = cmdb_core::relation::RelationInput::new(
                ns,
                cmdb_core::entity::EntityRef::by_name(ns, from_type, from_name),
                cmdb_core::entity::EntityRef::by_name(ns, to_type, to_name),
                rel_type,
            );
            match store.put_relation(input).await {
                Ok(r) => serde_json::to_value(&r).unwrap_or(Value::Null),
                Err(e) => json!({"error": e.to_string()}),
            }
        }
        "list_types" => {
            json!({
                "types": [
                    {"name": "fleet.agent", "description": "An agent on the ana bus."},
                    {"name": "fleet.host", "description": "A physical or virtual host."},
                    {"name": "fleet.cluster", "description": "A cluster of agents."},
                    {"name": "infra.vm", "description": "A virtual machine."},
                    {"name": "infra.container", "description": "A running container."},
                    {"name": "infra.pod", "description": "A Kubernetes pod."},
                    {"name": "infra.service", "description": "A network service."},
                    {"name": "app.service", "description": "An application service in the catalog."},
                    {"name": "secret.ref", "description": "Reference to a secret."},
                    {"name": "kb.runbook", "description": "A runbook URL."},
                ]
            })
        }
        other => json!({"error": format!("unknown op: {other}")}),
    }
}

async fn publish_discovery(
    client: &async_nats::Client,
    scheme: &SubjectScheme,
    identity: &str,
) -> Result<()> {
    let env = json!({
        "type": "discovery",
        "from": identity,
        "ts": now_iso(),
        "host": hostname(),
        "role": "cmdb",
        "subjects_owned": [
            scheme.query(identity, ">"),
            scheme.reply(identity, ">"),
            scheme.alert(identity, ">"),
        ],
        "capabilities": {
            "helios_cmdb_version": env!("CARGO_PKG_VERSION"),
            "ana_compat": "0.7",
            "supports_ack": true,
            "supports_txn_id": true,
            "tools": ["get_entity","query","traverse","search","cypher","history","facts","relations","upsert_entity","relate","list_types"],
        },
    });
    let subject = scheme.discovery(identity);
    client
        .publish(Subject::from(subject), serde_json::to_vec(&env)?.into())
        .await?;
    Ok(())
}

fn hostname() -> String {
    // Try $HOSTNAME first (most shells export it). Fall back to hostname(1).
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.is_empty() {
            return h;
        }
    }
    if let Ok(h) = std::env::var("CMDB_HOST") {
        if !h.is_empty() {
            return h;
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            gethostname::gethostname()
                .into_string()
                .unwrap_or_else(|_| "unknown".into())
        })
}
