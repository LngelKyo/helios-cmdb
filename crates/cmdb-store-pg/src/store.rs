//! Postgres-backed `Store` implementation.

use crate::queries;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cmdb_core::change::{Change, ChangeOp};
use cmdb_core::entity::{Entity, EntityInput, EntityRef};
use cmdb_core::error::{StoreError, StoreResult};
use cmdb_core::fact::{Fact, FactInput, FactQuery};
use cmdb_core::id::{ChangeId, EntityId, FactId, RelationId};
use cmdb_core::relation::{Relation, RelationInput};
use cmdb_core::source::Source;
use cmdb_core::store::{Direction, QueryFilter, TraverseHit, TraverseStep, VectorSearchHit};
use cmdb_core::Store;
use cmdb_embedding::Embedder;
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use std::sync::Arc;
use tracing::debug;

#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
    embedder: Option<Arc<dyn Embedder>>,
}

impl PgStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(url)
            .await?;
        Ok(Self { pool, embedder: None })
    }

    pub fn with_embedder(mut self, embedder: Arc<dyn Embedder>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn run_migrations(&self) -> Result<()> {
        crate::migration::run(&self.pool).await
    }

    async fn record_change(&self, c: ChangeRecord<'_>) -> StoreResult<()> {
        sqlx::query(queries::INSERT_CHANGE)
            .bind(c.id.as_uuid())
            .bind(&c.namespace)
            .bind(&c.actor)
            .bind(c.op.as_str())
            .bind(&c.target_type)
            .bind(c.target_id.map(|i| i.as_uuid()))
            .bind(c.before)
            .bind(c.after)
            .bind(c.reason)
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn upsert_embedding(&self, entity_id: EntityId, text: &str) {
        let Some(embedder) = &self.embedder else {
            return;
        };
        match embedder.embed(text).await {
            Ok(vec) => {
                let pv = pgvector::Vector::from(vec);
                let sql = r#"
                    INSERT INTO entity_embeddings (entity_id, embedding, model)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (entity_id) DO UPDATE
                      SET embedding = EXCLUDED.embedding,
                          model = EXCLUDED.model,
                          embedded_at = NOW()
                "#;
                if let Err(e) = sqlx::query(sql)
                    .bind(entity_id.as_uuid())
                    .bind(pv)
                    .bind(embedder.name())
                    .execute(&self.pool)
                    .await
                {
                    tracing::warn!(error = %e, "embedding upsert failed");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "embedding failed; entity will not be searchable via vector");
            }
        }
    }
}

struct ChangeRecord<'a> {
    id: ChangeId,
    namespace: &'a str,
    actor: &'a str,
    op: ChangeOp,
    target_type: &'a str,
    target_id: Option<EntityId>,
    before: Option<Value>,
    after: Option<Value>,
    reason: Option<&'a str>,
}

fn pg_err(e: sqlx::Error) -> StoreError {
    use sqlx::error::DatabaseError;
    match &e {
        sqlx::Error::RowNotFound => StoreError::NotFound("row".into()),
        sqlx::Error::Database(db) if db.is_unique_violation() => {
            StoreError::Conflict(db.message().to_string())
        }
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => {
            StoreError::Invalid(format!("foreign key: {}", db.message()))
        }
        other => StoreError::Backend(other.to_string()),
    }
}

fn map_entity(row: &PgRow) -> Entity {
    Entity {
        id: EntityId::from_uuid(row.get("id")),
        namespace: row.get("namespace"),
        entity_type: row.get("entity_type"),
        name: row.get("name"),
        attrs: row.get("attrs"),
        tags: row.get::<Vec<String>, _>("tags").into_iter().collect(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        version: row.get("version"),
    }
}

fn map_relation(row: &PgRow) -> Relation {
    Relation {
        id: RelationId::from_uuid(row.get("id")),
        namespace: row.get("namespace"),
        from_id: EntityId::from_uuid(row.get("from_id")),
        to_id: EntityId::from_uuid(row.get("to_id")),
        relation_type: row.get("relation_type"),
        props: row.get("props"),
        created_at: row.get("created_at"),
    }
}

fn map_fact(row: &PgRow) -> Fact {
    Fact {
        id: FactId::from_uuid(row.get("id")),
        namespace: row.get("namespace"),
        entity_id: EntityId::from_uuid(row.get("entity_id")),
        key: row.get("key"),
        value: row.get("value"),
        source: serde_json::from_value(row.get::<Value, _>("source")).unwrap_or_else(|_| {
            Source {
                kind: cmdb_core::source::SourceKind::Inferred,
                identity: "?".into(),
                transport: cmdb_core::source::Transport::Internal,
                nats_subject: None,
                observed_at: Utc::now(),
                confidence: 0.0,
                ttl_seconds: None,
                sig: None,
                evidence_ref: None,
            }
        }),
        superseded_by: row.get::<Option<uuid::Uuid>, _>("superseded_by").map(FactId::from_uuid),
    }
}

#[async_trait]
impl Store for PgStore {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn put_entity(&self, input: EntityInput, source: Source) -> StoreResult<Entity> {
        let id = EntityId::new();
        let tags: Vec<String> = input.tags.iter().cloned().collect();
        let row = sqlx::query(queries::UPSERT_ENTITY)
            .bind(id.as_uuid())
            .bind(&input.namespace)
            .bind(&input.entity_type)
            .bind(&input.name)
            .bind(&input.attrs)
            .bind(&tags)
            .fetch_one(&self.pool)
            .await
            .map_err(pg_err)?;
        let entity = map_entity(&row);

        // Compute + persist embedding (no-op if embedder is unconfigured).
        let text = cmdb_embedding::text_for_entity(&entity);
        self.upsert_embedding(entity.id, &text).await;

        self.record_change(ChangeRecord {
            id: ChangeId::new(),
            namespace: &entity.namespace,
            actor: &source.identity,
            op: ChangeOp::EntityUpsert,
            target_type: &entity.entity_type,
            target_id: Some(entity.id),
            before: None,
            after: serde_json::to_value(&entity).ok(),
            reason: None,
        })
        .await?;

        debug!(entity_id = %entity.id, "upserted entity");
        Ok(entity)
    }

    async fn get_entity(
        &self,
        namespace: &str,
        entity_type: &str,
        name: &str,
    ) -> StoreResult<Option<Entity>> {
        let row = sqlx::query(queries::GET_ENTITY_BY_NAME)
            .bind(namespace)
            .bind(entity_type)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(pg_err)?;
        Ok(row.as_ref().map(map_entity))
    }

    async fn get_entity_by_id(&self, id: EntityId) -> StoreResult<Option<Entity>> {
        let row = sqlx::query(queries::GET_ENTITY_BY_ID)
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(pg_err)?;
        Ok(row.as_ref().map(map_entity))
    }

    async fn query_entities(&self, filter: QueryFilter) -> StoreResult<Vec<Entity>> {
        let mut sql = String::from(
            r#"SELECT id, namespace, type AS entity_type, name, attrs, tags,
                      created_at, updated_at, version
               FROM entities WHERE 1=1"#,
        );
        let mut idx = 1usize;
        if filter.namespace.is_some() {
            sql.push_str(&format!(" AND namespace = ${idx}"));
            idx += 1;
        }
        if filter.entity_type.is_some() {
            sql.push_str(&format!(" AND type = ${idx}"));
            idx += 1;
        }
        if filter.name_prefix.is_some() {
            sql.push_str(&format!(" AND name LIKE ${idx}"));
            idx += 1;
        }
        if !filter.tags.is_empty() {
            sql.push_str(&format!(" AND tags @> ${idx}"));
            idx += 1;
        }
        sql.push_str(" ORDER BY created_at");
        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        let mut q = sqlx::query(&sql);
        if let Some(ns) = &filter.namespace {
            q = q.bind(ns);
        }
        if let Some(t) = &filter.entity_type {
            q = q.bind(t);
        }
        if let Some(p) = &filter.name_prefix {
            q = q.bind(format!("{}%", p));
        }
        if !filter.tags.is_empty() {
            q = q.bind(filter.tags);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(pg_err)?;
        Ok(rows.iter().map(map_entity).collect())
    }

    async fn delete_entity(&self, id: EntityId) -> StoreResult<()> {
        sqlx::query(queries::DELETE_ENTITY_BY_ID)
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn put_relation(&self, input: RelationInput) -> StoreResult<Relation> {
        let from = self.resolve_ref(&input.namespace, &input.from).await?;
        let to = self.resolve_ref(&input.namespace, &input.to).await?;

        let row = sqlx::query(queries::INSERT_RELATION)
            .bind(RelationId::new().as_uuid())
            .bind(&input.namespace)
            .bind(from.id.as_uuid())
            .bind(to.id.as_uuid())
            .bind(&input.relation_type)
            .bind(&input.props)
            .fetch_one(&self.pool)
            .await
            .map_err(pg_err)?;
        let relation = map_relation(&row);

        self.record_change(ChangeRecord {
            id: ChangeId::new(),
            namespace: &relation.namespace,
            actor: "system",
            op: ChangeOp::RelationUpsert,
            target_type: &relation.relation_type,
            target_id: None,
            before: None,
            after: None,
            reason: None,
        })
        .await?;

        debug!(relation_id = %relation.id, "upserted relation");
        Ok(relation)
    }

    async fn delete_relation(&self, id: RelationId) -> StoreResult<()> {
        sqlx::query("DELETE FROM relations WHERE id = $1")
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn traverse(&self, from: EntityId, step: TraverseStep) -> StoreResult<Vec<TraverseHit>> {
        let entity = self
            .get_entity_by_id(from)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("entity {from}")))?;

        let sql = match step.direction {
            Direction::Outgoing => queries::TRAVERSE_OUTGOING,
            Direction::Incoming => queries::TRAVERSE_INCOMING,
            Direction::Both => queries::TRAVERSE_BOTH,
        };
        let rt: Option<&str> = step.relation_type.as_deref();
        let rows = sqlx::query(sql)
            .bind(from.as_uuid())
            .bind(&entity.namespace)
            .bind(rt)
            .bind(step.max_depth as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(pg_err)?;

        let mut hits = Vec::with_capacity(rows.len());
        for row in &rows {
            let path_ids: Vec<uuid::Uuid> = row.get("path");
            let path: Vec<EntityId> = path_ids.into_iter().map(EntityId::from_uuid).collect();
            let via: Option<String> = row.get("via");
            let entity = map_entity(row);
            let depth: i32 = row.get("depth");
            hits.push(TraverseHit {
                entity,
                depth: depth as u32,
                path,
                via_relation_type: via,
            });
        }
        Ok(hits)
    }

    async fn vector_search(
        &self,
        query_text: &str,
        namespace: &str,
        limit: u32,
    ) -> StoreResult<Vec<VectorSearchHit>> {
        let Some(embedder) = &self.embedder else {
            return Ok(Vec::new());
        };
        let vec = embedder
            .embed(query_text)
            .await
            .map_err(|e| StoreError::Backend(format!("embed: {e}")))?;
        let pv = pgvector::Vector::from(vec);

        let sql = r#"
            SELECT e.id, e.namespace, e.type AS entity_type, e.name, e.attrs, e.tags,
                   e.created_at, e.updated_at, e.version,
                   1 - (ee.embedding <=> $1) AS score
              FROM entity_embeddings ee
              JOIN entities e ON e.id = ee.entity_id
             WHERE e.namespace = $2
             ORDER BY ee.embedding <=> $1
             LIMIT $3
        "#;
        let rows = sqlx::query(sql)
            .bind(pv)
            .bind(namespace)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|row| VectorSearchHit {
                entity: map_entity(row),
                score: row.get::<f64, _>("score") as f32,
            })
            .collect())
    }

    async fn add_fact(&self, input: FactInput) -> StoreResult<Fact> {
        let entity = self.resolve_ref(&input.namespace, &input.entity).await?;
        let id = FactId::new();
        let source_json = serde_json::to_value(&input.source).map_err(|e| StoreError::Invalid(e.to_string()))?;

        let row = sqlx::query(queries::INSERT_FACT_SIMPLE)
            .bind(id.as_uuid())
            .bind(&input.namespace)
            .bind(entity.id.as_uuid())
            .bind(&input.key)
            .bind(&input.value)
            .bind(&source_json)
            .bind(input.source.confidence)
            .bind(input.source.observed_at)
            .bind(input.source.ttl_seconds)
            .fetch_one(&self.pool)
            .await
            .map_err(pg_err)?;
        let fact = map_fact(&row);

        sqlx::query(queries::SUPERSEDE_PRIOR_FACTS)
            .bind(id.as_uuid())
            .bind(entity.id.as_uuid())
            .bind(&input.key)
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;

        self.record_change(ChangeRecord {
            id: ChangeId::new(),
            namespace: &fact.namespace,
            actor: &input.source.identity,
            op: ChangeOp::FactAdd,
            target_type: &fact.key,
            target_id: Some(entity.id),
            before: None,
            after: Some(fact.value.clone()),
            reason: None,
        })
        .await?;

        Ok(fact)
    }

    async fn effective_facts(
        &self,
        entity_id: EntityId,
        query: FactQuery,
    ) -> StoreResult<Vec<Fact>> {
        let rows = sqlx::query(queries::EFFECTIVE_FACTS)
            .bind(entity_id.as_uuid())
            .bind(query.min_confidence)
            .fetch_all(&self.pool)
            .await
            .map_err(pg_err)?;
        Ok(rows.iter().map(map_fact).collect())
    }

    async fn history(
        &self,
        namespace: Option<&str>,
        entity_id: Option<EntityId>,
        limit: u32,
    ) -> StoreResult<Vec<Change>> {
        let rows = sqlx::query(queries::HISTORY_FOR_ENTITY)
            .bind(entity_id.map(|i| i.as_uuid()))
            .bind(namespace)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(pg_err)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let op_str: String = row.get("op");
            let op = match op_str.as_str() {
                "entity_upsert" => ChangeOp::EntityUpsert,
                "entity_delete" => ChangeOp::EntityDelete,
                "fact_add" => ChangeOp::FactAdd,
                "fact_supersede" => ChangeOp::FactSupersede,
                "relation_upsert" => ChangeOp::RelationUpsert,
                "relation_delete" => ChangeOp::RelationDelete,
                "metamodel_change" => ChangeOp::MetaModelChange,
                other => return Err(StoreError::Backend(format!("unknown change op: {other}"))),
            };
            let target_id: Option<uuid::Uuid> = row.get("target_id");
            let ts: DateTime<Utc> = row.get("ts");
            out.push(Change {
                id: ChangeId::from_uuid(row.get("id")),
                ts,
                namespace: row.get("namespace"),
                actor: row.get("actor"),
                op,
                target_type: row.get("target_type"),
                target_id: target_id.map(EntityId::from_uuid),
                before: row.get("before"),
                after: row.get("after"),
                reason: row.get("reason"),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cmdb_core::entity::EntityInput;
    use cmdb_core::source::Source;

    fn db_url() -> Option<String> {
        std::env::var("CMDB_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok()
    }

    async fn setup() -> Option<PgStore> {
        let url = db_url()?;
        let store = PgStore::connect(&url).await.ok()?;
        store.run_migrations().await.ok()?;
        Some(store)
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL pointing at a Postgres with helios_cmdb database"]
    async fn pg_put_get_entity() {
        let store = match setup().await {
            Some(s) => s,
            None => return,
        };
        let input = EntityInput::new("test_ns", "test_type", "test_name")
            .with_attrs(serde_json::json!({"k": "v"}))
            .with_tags(["t1".to_string()]);
        let e = store
            .put_entity(input, Source::new_cli("user:tester"))
            .await
            .unwrap();
        assert_eq!(e.version, 1);

        let got = store
            .get_entity("test_ns", "test_type", "test_name")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, e.id);
        store.delete_entity(e.id).await.unwrap();
    }
}
