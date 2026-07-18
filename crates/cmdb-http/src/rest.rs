//! REST API. All routes under `/api/v1/`.

use crate::gql::{schema_for, Schema};
use crate::ui;
use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{delete, get, post},
    Router,
};
use cmdb_auth::TokenManager;
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::fact::FactInput;
use cmdb_core::id::EntityId;
use cmdb_core::relation::RelationInput;
use cmdb_core::source::Source;
use cmdb_core::store::{Direction, QueryFilter, TraverseStep};
use cmdb_core::Store;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    store: Arc<dyn Store>,
    actor: String,
    token_mgr: Option<TokenManager>,
    require_auth: bool,
}

pub async fn run(store: Arc<dyn Store>, actor: String, addr: SocketAddr) -> Result<()> {
    run_with_options(store, actor, addr, crate::HttpOptions::default()).await
}

pub async fn run_with_options(
    store: Arc<dyn Store>,
    actor: String,
    addr: SocketAddr,
    opts: crate::HttpOptions,
) -> Result<()> {
    let schema = schema_for(store.clone());
    let state = AppState {
        store: store.clone(),
        actor,
        token_mgr: None,
        require_auth: opts.require_auth,
    };

    let api_routes: Router<(AppState, Schema)> = Router::new()
        .route("/api/v1/entities/{id}", get(get_entity).delete(delete_entity))
        .route("/api/v1/entities", get(list_entities).post(upsert_entity))
        .route("/api/v1/entities/{id}/facts", get(list_facts))
        .route("/api/v1/entities/{id}/traverse", get(traverse))
        .route("/api/v1/facts", post(add_fact))
        .route("/api/v1/relations", post(add_relation))
        .route("/api/v1/relations/{id}", delete(delete_relation))
        .route("/api/v1/types", get(list_types))
        .route("/api/v1/search", get(search))
        .route("/api/v1/history", get(history))
        .route("/graphql", post(graphql_handler));

    let mut app: Router<(AppState, Schema)> = Router::new()
        .route("/healthz", get(healthz))
        .route("/graphql/playground", get(playground));

    if opts.serve_ui {
        app = app
            .route("/ui", get(ui::index))
            .route("/ui/{*path}", get(ui::asset))
            .route("/", get(ui::index));
    }

    app = app.merge(api_routes);

    let app = app.with_state((state, schema));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        %addr,
        require_auth = opts.require_auth,
        serve_ui = opts.serve_ui,
        "HTTP server listening (REST + GraphQL + UI)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    Json(json!({"ok": true, "service": "helios-cmdb", "version": env!("CARGO_PKG_VERSION")}))
}

#[derive(Deserialize)]
struct EntityQuery {
    #[serde(rename = "type")]
    entity_type: Option<String>,
    name: Option<String>,
    name_prefix: Option<String>,
    tags: Option<String>,
    namespace: Option<String>,
    limit: Option<u32>,
}

async fn get_entity(
    State((s, _)): State<(AppState, Schema)>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let id = parse_id(&id)?;
    let entity = s.store.get_entity_by_id(id).await.map_err(AppError::store)?;
    match entity {
        Some(e) => Ok(Json(serde_json::to_value(&e).unwrap_or_default())),
        None => Err(AppError::not_found(format!("entity {id}"))),
    }
}

async fn list_entities(
    State((s, _)): State<(AppState, Schema)>,
    Query(q): Query<EntityQuery>,
) -> Result<Json<Value>, AppError> {
    let namespace = q.namespace.as_deref().unwrap_or("cc.fleet");
    let mut filter = QueryFilter::new().in_namespace(namespace);
    if let Some(t) = &q.entity_type {
        filter = filter.of_type(t);
    }
    if let Some(p) = &q.name_prefix {
        filter.name_prefix = Some(p.clone());
    }
    if let Some(tags) = &q.tags {
        filter.tags = tags.split(',').map(String::from).collect();
    }
    if let Some(limit) = q.limit {
        filter = filter.with_limit(limit);
    }
    let entities = s.store.query_entities(filter).await.map_err(AppError::store)?;
    Ok(Json(json!({"entities": entities, "count": entities.len()})))
}

#[derive(Deserialize)]
struct UpsertEntityBody {
    namespace: Option<String>,
    #[serde(rename = "type")]
    entity_type: String,
    name: String,
    #[serde(default)]
    attrs: Value,
    #[serde(default)]
    tags: Vec<String>,
}

async fn upsert_entity(
    State((s, _)): State<(AppState, Schema)>,
    Json(body): Json<UpsertEntityBody>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let namespace = body.namespace.unwrap_or_else(|| "cc.fleet".into());
    let input = EntityInput::new(&namespace, &body.entity_type, &body.name)
        .with_attrs(body.attrs)
        .with_tags(body.tags.into_iter());
    let source = Source::new_cli(&s.actor);
    let entity = s.store.put_entity(input, source).await.map_err(AppError::store)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(&entity).unwrap_or_default()),
    ))
}

async fn delete_entity(
    State((s, _)): State<(AppState, Schema)>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let id = parse_id(&id)?;
    s.store.delete_entity(id).await.map_err(AppError::store)?;
    Ok(Json(json!({"deleted": id.to_string()})))
}

#[derive(Deserialize)]
struct ListFactsQuery {
    min_confidence: Option<f32>,
    namespace: Option<String>,
}

async fn list_facts(
    State((s, _)): State<(AppState, Schema)>,
    Path(id): Path<String>,
    Query(q): Query<ListFactsQuery>,
) -> Result<Json<Value>, AppError> {
    let id = parse_id(&id)?;
    let fq = cmdb_core::fact::FactQuery {
        min_confidence: q.min_confidence,
        ..Default::default()
    };
    let facts = s.store.effective_facts(id, fq).await.map_err(AppError::store)?;
    Ok(Json(json!({"facts": facts, "count": facts.len()})))
}

#[derive(Deserialize)]
struct TraverseQuery {
    namespace: Option<String>,
    depth: Option<u32>,
    direction: Option<String>,
    relation_type: Option<String>,
}

async fn traverse(
    State((s, _)): State<(AppState, Schema)>,
    Path(id): Path<String>,
    Query(q): Query<TraverseQuery>,
) -> Result<Json<Value>, AppError> {
    let id = parse_id(&id)?;
    let direction = match q.direction.as_deref().unwrap_or("outgoing") {
        "incoming" | "in" => Direction::Incoming,
        "both" => Direction::Both,
        _ => Direction::Outgoing,
    };
    let step = TraverseStep {
        relation_type: q.relation_type,
        direction,
        max_depth: q.depth.unwrap_or(3),
    };
    let hits = s.store.traverse(id, step).await.map_err(AppError::store)?;
    Ok(Json(json!({
        "hits": hits.iter().map(|h| json!({
            "depth": h.depth,
            "via": h.via_relation_type,
            "entity": h.entity,
            "path": h.path.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "count": hits.len(),
    })))
}

#[derive(Deserialize)]
struct AddFactBody {
    namespace: Option<String>,
    entity: EntityRef,
    key: String,
    value: Value,
    confidence: Option<f32>,
    ttl_seconds: Option<i64>,
}

async fn add_fact(
    State((s, _)): State<(AppState, Schema)>,
    Json(body): Json<AddFactBody>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let namespace = body.namespace.unwrap_or_else(|| "cc.fleet".into());
    let mut source = Source::new_agent(&s.actor);
    if let Some(c) = body.confidence {
        source = source.with_confidence(c);
    }
    if let Some(ttl) = body.ttl_seconds {
        source = source.with_ttl_seconds(ttl);
    }
    let input = FactInput {
        namespace,
        entity: body.entity,
        key: body.key,
        value: body.value,
        source,
    };
    let fact = s.store.add_fact(input).await.map_err(AppError::store)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(&fact).unwrap_or_default()),
    ))
}

#[derive(Deserialize)]
struct AddRelationBody {
    namespace: Option<String>,
    from: EntityRef,
    to: EntityRef,
    #[serde(rename = "type")]
    relation_type: String,
    #[serde(default)]
    props: Value,
}

async fn add_relation(
    State((s, _)): State<(AppState, Schema)>,
    Json(body): Json<AddRelationBody>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let namespace = body.namespace.unwrap_or_else(|| "cc.fleet".into());
    let input = RelationInput {
        namespace,
        from: body.from,
        to: body.to,
        relation_type: body.relation_type,
        props: body.props,
    };
    let rel = s.store.put_relation(input).await.map_err(AppError::store)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(&rel).unwrap_or_default()),
    ))
}

async fn delete_relation(
    State((s, _)): State<(AppState, Schema)>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let id: cmdb_core::id::RelationId = id.parse().map_err(|e: cmdb_core::id::IdParseError| {
        AppError::bad_request(e.to_string())
    })?;
    s.store.delete_relation(id).await.map_err(AppError::store)?;
    Ok(Json(json!({"deleted": id.to_string()})))
}

#[derive(Serialize)]
struct TypeEntry {
    name: &'static str,
    description: &'static str,
}

async fn list_types(
    State(_): State<(AppState, Schema)>,
    Query(_): Query<EntityQuery>,
) -> Json<Value> {
    let types: Vec<TypeEntry> = vec![
        TypeEntry { name: "fleet.agent", description: "An agent on the ana bus." },
        TypeEntry { name: "fleet.host", description: "A physical or virtual host." },
        TypeEntry { name: "fleet.cluster", description: "A cluster of agents." },
        TypeEntry { name: "infra.vm", description: "A virtual machine." },
        TypeEntry { name: "infra.container", description: "A running container." },
        TypeEntry { name: "infra.pod", description: "A Kubernetes pod." },
        TypeEntry { name: "infra.service", description: "A network service." },
        TypeEntry { name: "app.service", description: "An application service in the catalog." },
        TypeEntry { name: "secret.ref", description: "Reference to a secret (path + rotation metadata)." },
        TypeEntry { name: "kb.runbook", description: "A runbook URL." },
    ];
    Json(json!({"types": types, "count": types.len()}))
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    namespace: Option<String>,
    limit: Option<u32>,
}

async fn search(
    State((s, _)): State<(AppState, Schema)>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<Value>, AppError> {
    let namespace = q.namespace.as_deref().unwrap_or("cc.fleet");
    let limit = q.limit.unwrap_or(20);
    let hits = s.store.vector_search(&q.q, namespace, limit).await.map_err(AppError::store)?;
    if !hits.is_empty() {
        return Ok(Json(json!({
            "results": hits.iter().map(|h| json!({"entity": h.entity, "score": h.score})).collect::<Vec<_>>(),
            "count": hits.len(),
            "mode": "semantic",
        })));
    }
    let filter = QueryFilter::new().in_namespace(namespace).with_limit(limit);
    let entities = s.store.query_entities(filter).await.map_err(AppError::store)?;
    let q_lower = q.q.to_lowercase();
    let filtered: Vec<_> = entities
        .into_iter()
        .filter(|e| e.name.to_lowercase().contains(&q_lower))
        .collect();
    Ok(Json(json!({
        "results": filtered,
        "count": filtered.len(),
        "mode": "substring",
    })))
}

#[derive(Deserialize)]
struct HistoryQuery {
    namespace: Option<String>,
    entity_id: Option<String>,
    limit: Option<u32>,
}

async fn history(
    State((s, _)): State<(AppState, Schema)>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<Value>, AppError> {
    let entity_id = match q.entity_id.as_deref() {
        Some(id) => Some(parse_id(id)?),
        None => None,
    };
    let changes = s
        .store
        .history(q.namespace.as_deref(), entity_id, q.limit.unwrap_or(50))
        .await
        .map_err(AppError::store)?;
    Ok(Json(json!({"changes": changes, "count": changes.len()})))
}

// ---------------------------------------------------------------------------
// GraphQL handler
// ---------------------------------------------------------------------------

async fn graphql_handler(
    State((_, schema)): State<(AppState, Schema)>,
    req: async_graphql_axum::GraphQLRequest,
) -> Result<Json<Value>, AppError> {
    let resp = schema.execute(req.into_inner()).await;
    let v = serde_json::to_value(&resp).map_err(|e| AppError::internal(e.to_string()))?;
    Ok(Json(v))
}

async fn playground() -> impl IntoResponse {
    let html = async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    );
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
}

// ---------------------------------------------------------------------------
// helpers / errors
// ---------------------------------------------------------------------------

fn parse_id(s: &str) -> Result<EntityId, AppError> {
    EntityId::from_str(s).map_err(|e: cmdb_core::id::IdParseError| AppError::bad_request(e.to_string()))
}

#[derive(Debug)]
struct AppError {
    code: StatusCode,
    msg: String,
}

impl AppError {
    fn store(e: cmdb_core::error::StoreError) -> Self {
        use cmdb_core::error::StoreError;
        let code = match e {
            StoreError::NotFound(_) => StatusCode::NOT_FOUND,
            StoreError::Invalid(_) => StatusCode::BAD_REQUEST,
            StoreError::Conflict(_) => StatusCode::CONFLICT,
            StoreError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            code,
            msg: e.to_string(),
        }
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self { code: StatusCode::NOT_FOUND, msg: msg.into() }
    }
    fn bad_request(msg: impl Into<String>) -> Self {
        Self { code: StatusCode::BAD_REQUEST, msg: msg.into() }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self { code: StatusCode::INTERNAL_SERVER_ERROR, msg: msg.into() }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(json!({"error": self.msg, "code": self.code.as_u16()}));
        (self.code, body).into_response()
    }
}

use std::str::FromStr;
use async_trait::async_trait;
#[allow(dead_code)]
fn _suppress_unused() {
    let _ = BTreeSet::<String>::new();
}
