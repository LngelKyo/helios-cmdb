//! Tool registry and dispatch — the actual MCP tools.

use crate::protocol::{CallToolResult, Id, Message, PROTOCOL_VERSION, Response};
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::fact::FactInput;
use cmdb_core::id::EntityId;
use cmdb_core::relation::RelationInput;
use cmdb_core::source::Source;
use cmdb_core::store::{Direction, QueryFilter, TraverseHit, TraverseStep};
use cmdb_core::Store;
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;

pub struct McpServer {
    store: Arc<dyn Store>,
    actor: String,
}

impl McpServer {
    pub fn new(store: Arc<dyn Store>, actor: String) -> Self {
        Self { store, actor }
    }

    pub fn tool_list(&self) -> Vec<ToolDef> {
        TOOL_DEFS.iter().copied().collect()
    }

    /// Handle a single incoming JSON-RPC message. Returns the response (if any).
    pub async fn handle(&self, raw: &str) -> Option<String> {
        let parsed: Result<Message, _> = serde_json::from_str(raw);
        let msg = match parsed {
            Ok(m) => m,
            Err(e) => {
                let resp = Response::err(Id::Num(0), crate::protocol::PARSE_ERROR, e.to_string());
                return Some(serde_json::to_string(&resp).unwrap_or_default());
            }
        };
        match msg {
            Message::Request(req) => {
                let id = req.id.clone();
                let result = self.dispatch(&req.method, req.params).await;
                let resp = match result {
                    Ok(v) => Response::ok(id, v),
                    Err(msg) => Response::err(id, crate::protocol::INTERNAL_ERROR, msg),
                };
                Some(serde_json::to_string(&resp).unwrap_or_default())
            }
            Message::Notification(n) => {
                tracing::debug!(method = %n.method, "notification");
                None
            }
        }
    }

    async fn dispatch(&self, method: &str, params: Option<Value>) -> Result<Value, String> {
        match method {
            "initialize" => Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": { "listChanged": false }
                },
                "serverInfo": {
                    "name": "helios-cmdb",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            })),
            "initialized" | "notifications/initialized" => Ok(Value::Null),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({
                "tools": self.tool_list().iter().map(|t| {
                    let schema: Value = serde_json::from_str(t.input_schema)
                        .unwrap_or_else(|_| json!({"type":"object"}));
                    json!({
                        "name": t.name,
                        "description": t.description,
                        "inputSchema": schema,
                    })
                }).collect::<Vec<_>>()
            })),
            "tools/call" => {
                let p = params.ok_or("missing params")?;
                let name = p.get("name")
                    .and_then(|v| v.as_str())
                    .ok_or("missing 'name'")?;
                let args = p.get("arguments").cloned().unwrap_or(Value::Null);
                let result = self.call_tool(name, args).await?;
                Ok(serde_json::to_value(&result).map_err(|e| e.to_string())?)
            }
            _ => Err(format!("unknown method: {method}")),
        }
    }

    async fn call_tool(&self, name: &str, args: Value) -> Result<CallToolResult, String> {
        let namespace = args
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("cc.fleet");

        let actor = self.actor.clone();
        let result = match name {
            "list_types" => self.tool_list_types(namespace).await,
            "describe_type" => {
                let t = args.get("type").and_then(|v| v.as_str())
                    .ok_or("'type' required")?;
                self.tool_describe_type(namespace, t).await
            }
            "get_entity" => self.tool_get_entity(namespace, &args).await,
            "query" => self.tool_query(namespace, &args).await,
            "search" => {
                let q = args.get("q").and_then(|v| v.as_str()).ok_or("'q' required")?;
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as u32;
                self.tool_search(namespace, q, limit).await
            }
            "traverse" => self.tool_traverse(namespace, &args).await,
            "upsert_entity" => {
                let entity_type = args.get("type").and_then(|v| v.as_str())
                    .ok_or("'type' required")?;
                let entity_name = args.get("name").and_then(|v| v.as_str())
                    .ok_or("'name' required")?;
                let attrs = args.get("attrs").cloned().unwrap_or(json!({}));
                let tags: Vec<String> = args
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                let caller = args.get("caller").and_then(|v| v.as_str()).unwrap_or(&actor);
                let source = Source::new_agent(caller);
                let input = EntityInput::new(namespace, entity_type, entity_name)
                    .with_attrs(attrs)
                    .with_tags(tags);
                self.store.put_entity(input, source).await
                    .map(|e| serde_json::to_value(&e).unwrap_or(Value::Null))
                    .map_err(|e| e.to_string())
            }
            "upsert_fact" => {
                let entity = parse_entity_ref(args.get("entity").ok_or("'entity' required")?, namespace)?;
                let key = args.get("key").and_then(|v| v.as_str()).ok_or("'key' required")?;
                let value = args.get("value").cloned().unwrap_or(Value::Null);
                let confidence = args.get("confidence").and_then(|v| v.as_f64()).unwrap_or(0.7) as f32;
                let ttl_seconds = args.get("ttl_seconds").and_then(|v| v.as_i64());
                let caller = args.get("caller").and_then(|v| v.as_str()).unwrap_or(&actor);
                let mut source = Source::new_agent(caller);
                source.confidence = confidence;
                source.ttl_seconds = ttl_seconds;
                let input = FactInput {
                    namespace: namespace.to_string(),
                    entity,
                    key: key.to_string(),
                    value,
                    source,
                };
                self.store.add_fact(input).await
                    .map(|f| serde_json::to_value(&f).unwrap_or(Value::Null))
                    .map_err(|e| e.to_string())
            }
            "relate" => {
                let from = parse_entity_ref(args.get("from").ok_or("'from' required")?, namespace)?;
                let to = parse_entity_ref(args.get("to").ok_or("'to' required")?, namespace)?;
                let rel_type = args.get("type").and_then(|v| v.as_str()).ok_or("'type' required")?;
                let props = args.get("props").cloned().unwrap_or(json!({}));
                let input = RelationInput {
                    namespace: namespace.to_string(),
                    from, to,
                    relation_type: rel_type.to_string(),
                    props,
                };
                self.store.put_relation(input).await
                    .map(|r| serde_json::to_value(&r).unwrap_or(Value::Null))
                    .map_err(|e| e.to_string())
            }
            "unrelate" => {
                let id = args.get("id").and_then(|v| v.as_str()).ok_or("'id' required")?;
                let id: cmdb_core::id::RelationId = id.parse().map_err(|e: cmdb_core::id::IdParseError| e.to_string())?;
                self.store.delete_relation(id).await.map_err(|e| e.to_string())?;
                Ok(json!({"deleted": id.to_string()}))
            }
            "history" => self.tool_history(namespace, &args).await,
            "cypher" => {
                let q = args.get("query").and_then(|v| v.as_str()).ok_or("'query' required")?;
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
                let rows = self.store.cypher(q).await.map_err(|e| e.to_string())?;
                let truncated: Vec<&Vec<String>> = rows.iter().take(limit).collect();
                Ok(json!({
                    "rows": truncated,
                    "count": truncated.len(),
                    "truncated": rows.len() > limit,
                    "note": "agtype values are JSON-encoded; strings come back quoted",
                }))
            }
            other => Err(format!("unknown tool: {other}")),
        };

        match result {
            Ok(v) => Ok(CallToolResult::json(&v)),
            Err(e) => Ok(CallToolResult::error(e)),
        }
    }

    async fn tool_list_types(&self, namespace: &str) -> Result<Value, String> {
        // P1 stub: types are seeded into entity_types table at migration time.
        // A proper Store::list_entity_types() will land in P1.1 along with
        // schema introspection. For now, return the canonical seed list.
        let types: &[(&str, &str)] = if namespace == "cc.fleet" {
            &[
                ("fleet.agent", "An agent on the ana bus. Source: discovery envelope."),
                ("fleet.host", "A physical or virtual host. Source: pulse.host + ssh-facts collector."),
                ("fleet.cluster", "A cluster of agents. Source: discovery.cluster."),
                ("infra.vm", "A virtual machine."),
                ("infra.container", "A running container."),
                ("infra.pod", "A Kubernetes pod."),
                ("infra.service", "A network service."),
                ("app.service", "An application service in the catalog."),
                ("secret.ref", "Reference to a secret (path + rotation metadata, never the value)."),
                ("kb.runbook", "A runbook URL associated with one or more entities."),
            ]
        } else {
            &[]
        };
        Ok(json!({
            "namespace": namespace,
            "types": types.iter().map(|(n, d)| json!({"name": n, "description": d})).collect::<Vec<_>>(),
            "count": types.len(),
        }))
    }

    async fn tool_describe_type(&self, namespace: &str, t: &str) -> Result<Value, String> {
        // P1 stub: hardcoded canonical schemas. P1.1 will introspect live metamodel.
        let schema = match (namespace, t) {
            ("cc.fleet", "fleet.agent") => Some(json!({
                "type": "object",
                "properties": {
                    "role": {"type": "string"},
                    "cluster": {"type": "string"},
                    "subjects_owned": {"type": "array", "items": {"type": "string"}},
                    "capabilities": {"type": "object"},
                },
            })),
            ("cc.fleet", "fleet.host") => Some(json!({
                "type": "object",
                "properties": {
                    "os": {"type": "string"},
                    "kernel": {"type": "string"},
                    "cpus": {"type": "integer"},
                    "mem_gb": {"type": "integer"},
                },
            })),
            _ => None,
        };
        match schema {
            Some(s) => Ok(json!({"namespace": namespace, "type": t, "attrs_schema": s})),
            None => Err(format!("type {t} not known in namespace {namespace}")),
        }
    }

    async fn tool_get_entity(&self, namespace: &str, args: &Value) -> Result<Value, String> {
        if let Some(id) = args.get("id").and_then(|v| v.as_str()) {
            let id: cmdb_core::id::EntityId = id.parse().map_err(|e: cmdb_core::id::IdParseError| e.to_string())?;
            let entity = self.store.get_entity_by_id(id).await.map_err(|e| e.to_string())?
                .ok_or_else(|| format!("entity {id} not found"))?;
            return Ok(serde_json::to_value(&entity).unwrap_or(Value::Null));
        }
        let t = args.get("type").and_then(|v| v.as_str()).ok_or("'type' or 'id' required")?;
        let n = args.get("name").and_then(|v| v.as_str()).ok_or("'name' or 'id' required")?;
        let entity = self.store.get_entity(namespace, t, n).await.map_err(|e| e.to_string())?
            .ok_or_else(|| format!("entity {namespace}/{t}/{n} not found"))?;
        Ok(serde_json::to_value(&entity).unwrap_or(Value::Null))
    }

    async fn tool_query(&self, namespace: &str, args: &Value) -> Result<Value, String> {
        let mut filter = QueryFilter::new().in_namespace(namespace);
        if let Some(t) = args.get("type").and_then(|v| v.as_str()) {
            filter = filter.of_type(t);
        }
        if let Some(p) = args.get("name_prefix").and_then(|v| v.as_str()) {
            filter.name_prefix = Some(p.to_string());
        }
        if let Some(limit) = args.get("limit").and_then(|v| v.as_u64()) {
            filter = filter.with_limit(limit as u32);
        }
        if let Some(tags) = args.get("tags").and_then(|v| v.as_array()) {
            filter.tags = tags.iter().filter_map(|x| x.as_str().map(String::from)).collect();
        }
        let entities = self.store.query_entities(filter).await.map_err(|e| e.to_string())?;
        Ok(json!({ "entities": entities, "count": entities.len() }))
    }

    async fn tool_search(&self, namespace: &str, q: &str, limit: u32) -> Result<Value, String> {
        // Tier 1: pgvector semantic search.
        let hits = self
            .store
            .vector_search(q, namespace, limit)
            .await
            .map_err(|e| e.to_string())
            .unwrap_or_default();
        if !hits.is_empty() {
            return Ok(json!({
                "results": hits.iter().map(|h| json!({
                    "entity": h.entity,
                    "score": h.score,
                })).collect::<Vec<_>>(),
                "count": hits.len(),
                "mode": "semantic",
            }));
        }

        // Tier 2: pg_trgm fuzzy similarity.
        let fuzzy = self
            .store
            .text_search(q, namespace, limit)
            .await
            .map_err(|e| e.to_string())
            .unwrap_or_default();
        if !fuzzy.is_empty() {
            return Ok(json!({
                "results": fuzzy,
                "count": fuzzy.len(),
                "mode": "fuzzy",
            }));
        }

        // Tier 3: tokenized substring (split query into words, match any in name/type/attrs).
        let filter = QueryFilter::new().in_namespace(namespace).with_limit(200);
        let all = self.store.query_entities(filter).await.map_err(|e| e.to_string())?;
        let tokens: Vec<String> = q
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        let filtered: Vec<_> = if tokens.is_empty() {
            all.into_iter().filter(|e| {
                let q_lower = q.to_lowercase();
                e.name.to_lowercase().contains(&q_lower)
                    || e.entity_type.to_lowercase().contains(&q_lower)
            }).collect()
        } else {
            all.into_iter().filter(|e| {
                let name_lower = e.name.to_lowercase();
                let type_lower = e.entity_type.to_lowercase();
                let attrs_lower = e.attrs.to_string().to_lowercase();
                let name_words: Vec<&str> = name_lower
                    .split(|c: char| !c.is_alphanumeric())
                    .collect();
                tokens.iter().any(|t| {
                    name_lower.contains(t)
                        || type_lower.contains(t)
                        || attrs_lower.contains(t)
                        || e.tags.iter().any(|tag| tag.to_lowercase().contains(t))
                        || name_words.iter().any(|w| w == t)
                })
            }).collect()
        };
        let filtered: Vec<_> = filtered.into_iter().take(limit as usize).collect();
        Ok(json!({
            "results": filtered,
            "count": filtered.len(),
            "mode": "tokenized",
        }))
    }

    async fn tool_traverse(&self, namespace: &str, args: &Value) -> Result<Value, String> {
        let from_id = if let Some(id) = args.get("from").and_then(|v| v.as_str()) {
            cmdb_core::id::EntityId::from_str(id).map_err(|e: cmdb_core::id::IdParseError| e.to_string())?
        } else {
            let t = args.get("type").and_then(|v| v.as_str()).ok_or("'from' (id) or 'type' required")?;
            let n = args.get("name").and_then(|v| v.as_str()).ok_or("'name' required when 'from' absent")?;
            let e = self.store.get_entity(namespace, t, n).await.map_err(|e| e.to_string())?
                .ok_or_else(|| format!("entity {namespace}/{t}/{n} not found"))?;
            e.id
        };
        let max_depth = args.get("depth").and_then(|v| v.as_u64()).unwrap_or(3) as u32;
        let rel_type = args.get("relation_type").and_then(|v| v.as_str()).map(String::from);
        let direction = match args.get("direction").and_then(|v| v.as_str()).unwrap_or("outgoing") {
            "outgoing" | "out" => Direction::Outgoing,
            "incoming" | "in" => Direction::Incoming,
            "both" => Direction::Both,
            other => return Err(format!("invalid direction: {other}")),
        };
        let step = TraverseStep { relation_type: rel_type, direction, max_depth };
        let hits: Vec<TraverseHit> = self.store.traverse(from_id, step).await.map_err(|e| e.to_string())?;
        Ok(json!({
            "hits": hits.iter().map(|h| {
                json!({
                    "depth": h.depth,
                    "via": h.via_relation_type,
                    "entity": h.entity,
                    "path": h.path.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
            "count": hits.len(),
        }))
    }

    async fn tool_history(&self, namespace: &str, args: &Value) -> Result<Value, String> {
        let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as u32;
        let entity_id = if let Some(id) = args.get("entity_id").and_then(|v| v.as_str()) {
            Some(cmdb_core::id::EntityId::from_str(id).map_err(|e: cmdb_core::id::IdParseError| e.to_string())?)
        } else {
            None
        };
        let changes = self.store.history(Some(namespace), entity_id, limit).await.map_err(|e| e.to_string())?;
        Ok(json!({ "changes": changes, "count": changes.len() }))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: &'static str,
}

pub static TOOL_DEFS: &[ToolDef] = &[
    ToolDef {
        name: "list_types",
        description: "List all entity types defined in the metamodel for a namespace.",
        input_schema: r#"{"type":"object","properties":{"namespace":{"type":"string","default":"cc.fleet"}}}"#,
    },
    ToolDef {
        name: "describe_type",
        description: "Describe one entity type: its attr schema and allowed relations.",
        input_schema: r#"{"type":"object","required":["type"],"properties":{"namespace":{"type":"string"},"type":{"type":"string"}}}"#,
    },
    ToolDef {
        name: "get_entity",
        description: "Fetch a single entity by id or by (namespace, type, name).",
        input_schema: r#"{"type":"object","properties":{"namespace":{"type":"string"},"id":{"type":"string"},"type":{"type":"string"},"name":{"type":"string"}}}"#,
    },
    ToolDef {
        name: "query",
        description: "Filter entities by type, name prefix, tags.",
        input_schema: r#"{"type":"object","properties":{"namespace":{"type":"string"},"type":{"type":"string"},"name_prefix":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}},"limit":{"type":"integer","default":50}}}"#,
    },
    ToolDef {
        name: "search",
        description: "Free-text search over entity name/type/tags. (P2 adds semantic search.)",
        input_schema: r#"{"type":"object","required":["q"],"properties":{"namespace":{"type":"string"},"q":{"type":"string"},"limit":{"type":"integer","default":20}}}"#,
    },
    ToolDef {
        name: "traverse",
        description: "Graph traversal from a starting entity. Returns reachable entities with depths and path.",
        input_schema: r#"{"type":"object","properties":{"namespace":{"type":"string"},"from":{"type":"string","description":"entity id"},"type":{"type":"string"},"name":{"type":"string"},"depth":{"type":"integer","default":3},"relation_type":{"type":"string"},"direction":{"type":"string","enum":["outgoing","incoming","both"],"default":"outgoing"}}}"#,
    },
    ToolDef {
        name: "upsert_entity",
        description: "Insert or update an entity. Source defaults to the MCP actor. Pass 'caller' to attribute the write to a specific agent/session.",
        input_schema: r#"{"type":"object","required":["type","name"],"properties":{"namespace":{"type":"string"},"type":{"type":"string"},"name":{"type":"string"},"attrs":{"type":"object"},"tags":{"type":"array","items":{"type":"string"}},"caller":{"type":"string","description":"Optional identity for provenance, e.g. 'agent:hermes:session-abc'"}}}"#,
    },
    ToolDef {
        name: "upsert_fact",
        description: "Add a versioned observation about an entity. Pass 'caller' for provenance.",
        input_schema: r#"{"type":"object","required":["entity","key","value"],"properties":{"namespace":{"type":"string"},"entity":{"type":"object","properties":{"id":{"type":"string"},"type":{"type":"string"},"name":{"type":"string"}}},"key":{"type":"string"},"value":{},"confidence":{"type":"number","default":0.7},"ttl_seconds":{"type":"integer"},"caller":{"type":"string","description":"Optional identity for provenance"}}"#,
    },
    ToolDef {
        name: "relate",
        description: "Create a directed relation between two entities.",
        input_schema: r#"{"type":"object","required":["from","to","type"],"properties":{"namespace":{"type":"string"},"from":{"type":"object","properties":{"id":{"type":"string"},"type":{"type":"string"},"name":{"type":"string"}}},"to":{"type":"object","properties":{"id":{"type":"string"},"type":{"type":"string"},"name":{"type":"string"}}},"type":{"type":"string"},"props":{"type":"object"},"caller":{"type":"string","description":"Optional identity for provenance"}}"#,
    },
    ToolDef {
        name: "unrelate",
        description: "Delete a relation by id.",
        input_schema: r#"{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}"#,
    },
    ToolDef {
        name: "history",
        description: "Append-only change log for the namespace, optionally filtered by entity.",
        input_schema: r#"{"type":"object","properties":{"namespace":{"type":"string"},"entity_id":{"type":"string"},"limit":{"type":"integer","default":20}}}"#,
    },
    ToolDef {
        name: "cypher",
        description: "Run a Cypher query against the Apache AGE graph (`helios`). Vertices are :Entity {entity_id, namespace, type, name}; edges are :Relation {relation_id, namespace, type}. Returns agtype-encoded values.",
        input_schema: r#"{"type":"object","required":["query"],"properties":{"query":{"type":"string","description":"e.g. MATCH (a:Entity {type:'fleet.agent'})-[:Relation {type:'runs_on'}]->(h:Entity) RETURN a.name, h.name"},"limit":{"type":"integer","default":50}}}"#,
    },
];

fn parse_entity_ref(v: &Value, default_ns: &str) -> Result<EntityRef, String> {
    if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
        let id: EntityId = id.parse().map_err(|e: cmdb_core::id::IdParseError| e.to_string())?;
        return Ok(EntityRef::by_id(id));
    }
    let t = v.get("type").and_then(|x| x.as_str()).ok_or("entity needs 'id' or ('type'+'name')")?;
    let n = v.get("name").and_then(|x| x.as_str()).ok_or("entity needs 'name'")?;
    let ns = v.get("namespace").and_then(|x| x.as_str()).unwrap_or(default_ns);
    Ok(EntityRef::by_name(ns, t, n))
}

pub async fn serve_stdio(store: Arc<dyn Store>, actor: String) -> anyhow::Result<()> {
    crate::stdio::run(McpServer::new(store, actor)).await
}

pub async fn serve_http(
    store: Arc<dyn Store>,
    actor: String,
    addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    crate::http::run(McpServer::new(store, actor), addr).await
}
