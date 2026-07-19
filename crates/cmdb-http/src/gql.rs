//! GraphQL schema. Mirrors the REST surface in a single graph.
//!
//! Query:
//!   entity(id, type, name), entities(filter), search(q, limit),
//!   traverse(from, depth, direction, relationType), types,
//!   history(entityId, limit)
//!
//! Mutation:
//!   upsertEntity(input), deleteEntity(id), addFact(input),
//!   addRelation(input), deleteRelation(id)

use async_graphql::{Context, Object, Result as GqlResult, SimpleObject};
use cmdb_core::entity::{Entity, EntityInput, EntityRef};
use cmdb_core::fact::{Fact, FactInput};
use cmdb_core::id::{EntityId, RelationId};
use cmdb_core::relation::{Relation, RelationInput};
use cmdb_core::source::Source;
use cmdb_core::store::{Direction, QueryFilter, TraverseStep};
use cmdb_core::Store;
use std::str::FromStr;
use std::sync::Arc;

/// Extract Principal from the GraphQL context and verify it has the
/// requested op scope. Returns GqlResult<()>; errors out with "forbidden"
/// if scope is insufficient. Under no-auth mode (no principal) this is a
/// no-op.
fn require_op(ctx: &Context<'_>, op: &str) -> GqlResult<()> {
    let principal = ctx.data::<Option<cmdb_auth::Principal>>()?;
    if let Some(p) = principal {
        if !p.op_scope.is_empty()
            && !p.op_scope.contains(op)
            && !(op == "read" && (p.op_scope.contains("write") || p.op_scope.contains("admin")))
            && !(op == "write" && p.op_scope.contains("admin"))
        {
            return Err(format!("forbidden: token lacks '{op}' scope").into());
        }
    }
    Ok(())
}

pub type Schema = async_graphql::Schema<QueryRoot, MutationRoot, async_graphql::EmptySubscription>;

pub fn schema_for(store: Arc<dyn Store>) -> Schema {
    Schema::build(QueryRoot, MutationRoot, async_graphql::EmptySubscription)
        .data(store)
        .data::<Option<cmdb_auth::Principal>>(None) // default when no principal
        .finish()
}

pub async fn run(store: Arc<dyn Store>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let schema = schema_for(store);
    let app = axum::Router::new()
        .route("/graphql", axum::routing::post(graphql_handler))
        .route("/graphql/playground", axum::routing::get(playground))
        .with_state(schema);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "GraphQL server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn graphql_handler(
    axum::extract::State(schema): axum::extract::State<Schema>,
    req: async_graphql_axum::GraphQLRequest,
) -> axum::Json<serde_json::Value> {
    let resp = schema.execute(req.into_inner()).await;
    axum::Json(serde_json::to_value(resp).unwrap_or_default())
}

async fn playground() -> impl axum::response::IntoResponse {
    let html = async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    );
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
}

// ---------------------------------------------------------------------------
// types
// ---------------------------------------------------------------------------

#[derive(SimpleObject)]
struct GqlEntity {
    id: String,
    namespace: String,
    #[graphql(name = "type")]
    entity_type: String,
    name: String,
    attrs: serde_json::Value,
    tags: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    version: i32,
}

impl From<Entity> for GqlEntity {
    fn from(e: Entity) -> Self {
        Self {
            id: e.id.to_string(),
            namespace: e.namespace,
            entity_type: e.entity_type,
            name: e.name,
            attrs: e.attrs,
            tags: e.tags.into_iter().collect(),
            created_at: e.created_at,
            updated_at: e.updated_at,
            version: e.version,
        }
    }
}

#[derive(SimpleObject)]
struct GqlFact {
    id: String,
    entity_id: String,
    key: String,
    value: serde_json::Value,
    confidence: f32,
    observed_at: chrono::DateTime<chrono::Utc>,
    source_identity: String,
    source_kind: String,
}

impl From<Fact> for GqlFact {
    fn from(f: Fact) -> Self {
        Self {
            id: f.id.to_string(),
            entity_id: f.entity_id.to_string(),
            key: f.key,
            value: f.value,
            confidence: f.source.confidence,
            observed_at: f.source.observed_at,
            source_identity: f.source.identity,
            source_kind: format!("{:?}", f.source.kind).to_lowercase(),
        }
    }
}

#[derive(SimpleObject)]
struct GqlRelation {
    id: String,
    namespace: String,
    from_id: String,
    to_id: String,
    #[graphql(name = "type")]
    relation_type: String,
    props: serde_json::Value,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<Relation> for GqlRelation {
    fn from(r: Relation) -> Self {
        Self {
            id: r.id.to_string(),
            namespace: r.namespace,
            from_id: r.from_id.to_string(),
            to_id: r.to_id.to_string(),
            relation_type: r.relation_type,
            props: r.props,
            created_at: r.created_at,
        }
    }
}

#[derive(SimpleObject)]
struct GqlSearchHit {
    entity: GqlEntity,
    score: f32,
}

#[derive(SimpleObject)]
struct GqlTraverseHit {
    entity: GqlEntity,
    depth: i32,
    via_relation_type: Option<String>,
    path: Vec<String>,
}

#[derive(SimpleObject)]
struct GqlChange {
    id: String,
    ts: chrono::DateTime<chrono::Utc>,
    namespace: String,
    actor: String,
    op: String,
    target_type: String,
    target_id: Option<String>,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    reason: Option<String>,
}

impl From<cmdb_core::change::Change> for GqlChange {
    fn from(c: cmdb_core::change::Change) -> Self {
        Self {
            id: c.id.to_string(),
            ts: c.ts,
            namespace: c.namespace,
            actor: c.actor,
            op: c.op.as_str().into(),
            target_type: c.target_type,
            target_id: c.target_id.map(|i| i.to_string()),
            before: c.before,
            after: c.after,
            reason: c.reason,
        }
    }
}

#[derive(async_graphql::InputObject)]
struct EntityRefInput {
    id: Option<String>,
    #[graphql(name = "type")]
    entity_type: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
}

impl TryFrom<EntityRefInput> for EntityRef {
    type Error = String;
    fn try_from(v: EntityRefInput) -> Result<Self, String> {
        if let Some(id) = v.id {
            let id = EntityId::from_str(&id).map_err(|e| e.to_string())?;
            return Ok(EntityRef::by_id(id));
        }
        let t = v.entity_type.ok_or("entity needs 'id' or ('type'+'name')")?;
        let n = v.name.ok_or("entity needs 'name'")?;
        let ns = v.namespace.unwrap_or_else(|| "cc.fleet".into());
        Ok(EntityRef::by_name(ns, t, n))
    }
}

#[derive(async_graphql::InputObject)]
struct UpsertEntityInput {
    namespace: Option<String>,
    #[graphql(name = "type")]
    entity_type: String,
    name: String,
    attrs: Option<serde_json::Value>,
    tags: Option<Vec<String>>,
}

#[derive(async_graphql::InputObject)]
struct AddFactInput {
    namespace: Option<String>,
    entity: EntityRefInput,
    key: String,
    value: serde_json::Value,
    confidence: Option<f32>,
    ttl_seconds: Option<i64>,
}

#[derive(async_graphql::InputObject)]
struct AddRelationInput {
    namespace: Option<String>,
    from: EntityRefInput,
    to: EntityRefInput,
    #[graphql(name = "type")]
    relation_type: String,
    props: Option<serde_json::Value>,
}

#[derive(async_graphql::InputObject)]
struct EntityFilterInput {
    namespace: Option<String>,
    #[graphql(name = "type")]
    entity_type: Option<String>,
    name_prefix: Option<String>,
    tags: Option<Vec<String>>,
    limit: Option<i32>,
}

// ---------------------------------------------------------------------------
// roots
// ---------------------------------------------------------------------------

pub struct QueryRoot;
pub struct MutationRoot;

#[Object]
impl QueryRoot {
    async fn entity(
        &self,
        ctx: &Context<'_>,
        id: Option<String>,
        entity_type: Option<String>,
        name: Option<String>,
        namespace: Option<String>,
    ) -> GqlResult<Option<GqlEntity>> {
        let store = ctx.data::<Arc<dyn Store>>()?;
        let entity = if let Some(id_str) = id {
            let eid = EntityId::from_str(&id_str)?;
            store.get_entity_by_id(eid).await?
        } else {
            let t = entity_type.ok_or("'id' or ('type'+'name') required")?;
            let n = name.ok_or("'name' required when 'id' absent")?;
            let ns = namespace.as_deref().unwrap_or("cc.fleet");
            store.get_entity(ns, &t, &n).await?
        };
        Ok(entity.map(Into::into))
    }

    async fn entities(
        &self,
        ctx: &Context<'_>,
        filter: EntityFilterInput,
    ) -> GqlResult<Vec<GqlEntity>> {
        let store = ctx.data::<Arc<dyn Store>>()?;
        let mut q = QueryFilter::new().in_namespace(filter.namespace.as_deref().unwrap_or("cc.fleet"));
        if let Some(t) = &filter.entity_type {
            q = q.of_type(t);
        }
        if let Some(p) = &filter.name_prefix {
            q.name_prefix = Some(p.clone());
        }
        if let Some(tags) = &filter.tags {
            q.tags = tags.clone();
        }
        if let Some(limit) = filter.limit {
            q = q.with_limit(limit as u32);
        }
        let entities = store.query_entities(q).await?;
        Ok(entities.into_iter().map(Into::into).collect())
    }

    async fn search(
        &self,
        ctx: &Context<'_>,
        q: String,
        namespace: Option<String>,
        limit: Option<i32>,
    ) -> GqlResult<Vec<GqlSearchHit>> {
        let store = ctx.data::<Arc<dyn Store>>()?;
        let ns = namespace.as_deref().unwrap_or("cc.fleet");
        let hits = store.vector_search(&q, ns, limit.unwrap_or(20) as u32).await?;
        Ok(hits
            .into_iter()
            .map(|h| GqlSearchHit {
                entity: h.entity.into(),
                score: h.score,
            })
            .collect())
    }

    async fn traverse(
        &self,
        ctx: &Context<'_>,
        from: String,
        depth: Option<u32>,
        direction: Option<String>,
        relation_type: Option<String>,
    ) -> GqlResult<Vec<GqlTraverseHit>> {
        let store = ctx.data::<Arc<dyn Store>>()?;
        let from_id = EntityId::from_str(&from)?;
        let dir = match direction.as_deref().unwrap_or("outgoing") {
            "incoming" | "in" => Direction::Incoming,
            "both" => Direction::Both,
            _ => Direction::Outgoing,
        };
        let step = TraverseStep {
            relation_type,
            direction: dir,
            max_depth: depth.unwrap_or(3),
        };
        let hits = store.traverse(from_id, step).await?;
        Ok(hits
            .into_iter()
            .map(|h| GqlTraverseHit {
                entity: h.entity.into(),
                depth: h.depth as i32,
                via_relation_type: h.via_relation_type,
                path: h.path.into_iter().map(|i| i.to_string()).collect(),
            })
            .collect())
    }

    async fn history(
        &self,
        ctx: &Context<'_>,
        entity_id: Option<String>,
        namespace: Option<String>,
        limit: Option<i32>,
    ) -> GqlResult<Vec<GqlChange>> {
        let store = ctx.data::<Arc<dyn Store>>()?;
        let eid = match entity_id {
            Some(s) => Some(EntityId::from_str(&s)?),
            None => None,
        };
        let changes = store.history(namespace.as_deref(), eid, limit.unwrap_or(50) as u32).await?;
        Ok(changes.into_iter().map(Into::into).collect())
    }

    async fn types(&self, _ctx: &Context<'_>) -> GqlResult<Vec<serde_json::Value>> {
        Ok(vec![
            serde_json::json!({"name": "fleet.agent"}),
            serde_json::json!({"name": "fleet.host"}),
            serde_json::json!({"name": "fleet.cluster"}),
            serde_json::json!({"name": "infra.vm"}),
            serde_json::json!({"name": "infra.container"}),
            serde_json::json!({"name": "infra.pod"}),
            serde_json::json!({"name": "infra.service"}),
            serde_json::json!({"name": "app.service"}),
            serde_json::json!({"name": "secret.ref"}),
            serde_json::json!({"name": "kb.runbook"}),
        ])
    }
}

#[Object]
impl MutationRoot {
    async fn upsert_entity(
        &self,
        ctx: &Context<'_>,
        input: UpsertEntityInput,
    ) -> GqlResult<GqlEntity> {
        require_op(ctx, "write")?;
        let store = ctx.data::<Arc<dyn Store>>()?;
        let namespace = input.namespace.unwrap_or_else(|| "cc.fleet".into());
        let tags = input.tags.unwrap_or_default();
        let entity_input = EntityInput::new(&namespace, &input.entity_type, &input.name)
            .with_attrs(input.attrs.unwrap_or(serde_json::json!({})))
            .with_tags(tags.into_iter());
        let source = Source::new_cli("user:graphql");
        let entity = store.put_entity(entity_input, source).await?;
        Ok(entity.into())
    }

    async fn delete_entity(&self, ctx: &Context<'_>, id: String) -> GqlResult<bool> {
        require_op(ctx, "write")?;
        let store = ctx.data::<Arc<dyn Store>>()?;
        let eid = EntityId::from_str(&id)?;
        store.delete_entity(eid).await?;
        Ok(true)
    }

    async fn add_fact(&self, ctx: &Context<'_>, input: AddFactInput) -> GqlResult<GqlFact> {
        require_op(ctx, "write")?;
        let store = ctx.data::<Arc<dyn Store>>()?;
        let namespace = input.namespace.unwrap_or_else(|| "cc.fleet".into());
        let entity_ref: EntityRef = input.entity.try_into()?;
        let mut source = Source::new_agent("user:graphql");
        if let Some(c) = input.confidence {
            source = source.with_confidence(c);
        }
        if let Some(ttl) = input.ttl_seconds {
            source = source.with_ttl_seconds(ttl);
        }
        let fact = store
            .add_fact(FactInput {
                namespace,
                entity: entity_ref,
                key: input.key,
                value: input.value,
                source,
            })
            .await?;
        Ok(fact.into())
    }

    async fn add_relation(
        &self,
        ctx: &Context<'_>,
        input: AddRelationInput,
    ) -> GqlResult<GqlRelation> {
        require_op(ctx, "write")?;
        let store = ctx.data::<Arc<dyn Store>>()?;
        let namespace = input.namespace.unwrap_or_else(|| "cc.fleet".into());
        let from: EntityRef = input.from.try_into()?;
        let to: EntityRef = input.to.try_into()?;
        let rel = store
            .put_relation(RelationInput {
                namespace,
                from,
                to,
                relation_type: input.relation_type,
                props: input.props.unwrap_or(serde_json::json!({})),
            })
            .await?;
        Ok(rel.into())
    }

    async fn delete_relation(&self, ctx: &Context<'_>, id: String) -> GqlResult<bool> {
        require_op(ctx, "write")?;
        let store = ctx.data::<Arc<dyn Store>>()?;
        let rid = RelationId::from_str(&id)?;
        store.delete_relation(rid).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cmdb_core::mock::InMemoryStore;

    fn build_schema() -> Schema {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        schema_for(store)
    }

    fn request_with_principal(
        query: &str,
        principal: Option<cmdb_auth::Principal>,
    ) -> async_graphql::Request {
        async_graphql::Request::new(query).data(principal)
    }

    fn principal(ops: &[&str]) -> Option<cmdb_auth::Principal> {
        Some(cmdb_auth::Principal {
            identity: "test".into(),
            namespace_scope: std::collections::BTreeSet::new(),
            op_scope: ops.iter().map(|s| s.to_string()).collect(),
            token_id: "t".into(),
        })
    }

    #[tokio::test]
    async fn query_allowed_for_read_token() {
        let schema = build_schema();
        let q = "{ entities(filter:{type:\"x\"}) { name } }";
        let resp = schema
            .execute(request_with_principal(q, principal(&["read"])))
            .await;
        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
    }

    #[tokio::test]
    async fn mutation_forbidden_for_read_token() {
        let schema = build_schema();
        let q = "mutation { upsertEntity(input:{type:\"x\",name:\"y\"}) { id } }";
        let resp = schema
            .execute(request_with_principal(q, principal(&["read"])))
            .await;
        assert!(resp.is_err(), "expected error");
        let msg = resp.errors[0].message.to_string();
        assert!(msg.contains("write"), "got: {msg}");
    }

    #[tokio::test]
    async fn mutation_allowed_for_write_token() {
        let schema = build_schema();
        let q = "mutation { upsertEntity(input:{type:\"x\",name:\"y\"}) { id } }";
        let resp = schema
            .execute(request_with_principal(q, principal(&["read", "write"])))
            .await;
        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
    }

    #[tokio::test]
    async fn mutation_allowed_for_admin_token() {
        let schema = build_schema();
        let q = "mutation { upsertEntity(input:{type:\"x\",name:\"y\"}) { id } }";
        let resp = schema
            .execute(request_with_principal(q, principal(&["admin"])))
            .await;
        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
    }

    #[tokio::test]
    async fn mutation_allowed_when_no_auth() {
        // No-auth mode (principal = None): mutations succeed (no scope enforcement).
        let schema = build_schema();
        let q = "mutation { upsertEntity(input:{type:\"x\",name:\"y\"}) { id } }";
        let resp = schema.execute(request_with_principal(q, None)).await;
        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
    }
}
