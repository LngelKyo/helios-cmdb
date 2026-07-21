//! Postgres-backed `Store` implementation.

use crate::queries;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cmdb_core::change::{Change, ChangeOp};
use cmdb_core::entity::{Entity, EntityInput};
#[allow(unused_imports)]
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

/// Name of the Apache AGE graph used by helios-cmdb.
///
/// Historical note: we used to call this `helios`, but that collided with
/// the typical DB username `helios` (AGE creates a same-named schema for
/// each graph). Renamed to `cmdb_graph` to avoid the collision.
pub const GRAPH_NAME: &str = "cmdb_graph";

/// Ensure a Postgres connection URL carries `options=-c search_path=public,ag_catalog`.
///
/// This is CRITICAL for two reasons:
///   1. AGE's `agtype` operator class lives in `ag_catalog`; without it in
///      search_path, MERGE / WHERE on agtype columns fails with misleading
///      "operator does not exist" errors.
///   2. The AGE graph creates a same-named schema; if that schema wins
///      search_path precedence, `_sqlx_migrations` rows land in the wrong
///      schema and subsequent `migrate` runs lose track of state.
///
/// Behavior:
///   - URL without `?`: append `?options=-c search_path=public,ag_catalog`.
///   - URL with `?` but no `options=`: append `&options=...`.
///   - URL with `options=` that already mentions `search_path`: leave alone
///     (assume caller knows what they're doing).
///   - URL with `options=` but no `search_path`: rewrite the options value
///     to include `-c search_path=public,ag_catalog` appended after the
///     caller's existing options. (libpq parses the options string as
///     space-separated `-c key=value` flags.)
pub fn normalize_pg_url(url: &str) -> String {
    let (base, query) = match url.find('?') {
        Some(i) => (&url[..i], &url[i + 1..]),
        None => return format!("{url}?options=-c%20search_path%3Dpublic%2Cag_catalog"),
    };

    // Walk the query params, looking for an existing options=.
    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), percent_decode(v)),
            None => (p.to_string(), String::new()),
        })
        .collect();

    let existing = params.iter().find(|(k, _)| k == "options");
    match existing {
        Some((_, v)) if v.contains("search_path") => {
            // Caller already sets search_path. Assume they know what they're
            // doing — but warn if ag_catalog isn't in it, since that's the
            // specific schema we need for AGE.
            if !v.contains("ag_catalog") {
                tracing::warn!(
                    url = %url,
                    "existing options= sets search_path without ag_catalog; AGE operators may fail"
                );
            }
            url.to_string()
        }
        Some(_) => {
            // Existing options= without search_path — augment it.
            for (k, v) in params.iter_mut() {
                if k == "options" {
                    let mut new_opts = v.clone();
                    new_opts.push_str(" -c search_path=public,ag_catalog");
                    *v = new_opts;
                    break;
                }
            }
            let q = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, percent_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            format!("{base}?{q}")
        }
        None => {
            params.push((
                "options".to_string(),
                "-c search_path=public,ag_catalog".to_string(),
            ));
            let q = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, percent_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            format!("{base}?{q}")
        }
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                out.push(b as char);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(' ');
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            b'=' => out.push_str("%3D"),
            b',' => out.push_str("%2C"),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

#[cfg(test)]
mod url_tests {
    use super::*;

    #[test]
    fn appends_options_to_bare_url() {
        let out = normalize_pg_url("postgres://user@host/db");
        assert!(out.contains("options="));
        assert!(out.contains("search_path"));
        assert!(out.contains("ag_catalog"));
        assert!(out.starts_with("postgres://user@host/db?"));
    }

    #[test]
    fn appends_options_to_url_with_query() {
        let out = normalize_pg_url("postgres://user@host/db?sslmode=require");
        assert!(out.contains("options="));
        assert!(out.contains("sslmode=require"));
        assert!(out.contains("&options="));
    }

    #[test]
    fn preserves_existing_search_path() {
        let url = "postgres://user@host/db?options=-c%20search_path%3Dapplication%2Cag_catalog";
        let out = normalize_pg_url(url);
        assert_eq!(out, url, "should be unchanged when search_path present");
    }

    #[test]
    fn augments_options_without_search_path() {
        let url = "postgres://user@host/db?options=-c%20statement_timeout%3D5000";
        let out = normalize_pg_url(url);
        // Should still have the original statement_timeout AND add search_path.
        assert!(out.contains("statement_timeout"), "out = {out}");
        assert!(out.contains("search_path"), "out = {out}");
        assert!(out.contains("ag_catalog"), "out = {out}");
        // Should still be one options= param (not two).
        assert_eq!(out.matches("options=").count(), 1);
    }
}

impl PgStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let url = normalize_pg_url(url);
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .min_connections(2)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .after_connect(|conn, _| Box::pin(async move {
                sqlx::query("LOAD 'age'").execute(&mut *conn).await?;
                sqlx::query("SET search_path = public, ag_catalog")
                    .execute(&mut *conn)
                    .await
                    .map(|_| ())
            }))
            .connect(&url)
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

    /// Escape a string for safe interpolation into a Cypher single-quoted
    /// literal. Cypher uses backslash escapes like C.
    fn cypher_escape(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '\'' => out.push_str("\\'"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ => out.push(ch),
            }
        }
        out
    }

    /// Run a Cypher query against the `cmdb_graph` AGE graph. Returns each
    /// row as raw agtype-encoded strings (caller decodes). Cypher queries
    /// must return a single column; use `RETURN {a: x, b: y}` to bundle.
    async fn age_query_raw(&self, cypher: &str) -> StoreResult<Vec<Vec<String>>> {
        let sql = format!(
            "SELECT (result)::text AS cell FROM ag_catalog.cypher('{GRAPH_NAME}', $$ {} $$) AS __t(result ag_catalog.agtype);",
            cypher
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await.map_err(|e| {
            StoreError::Backend(format!("cypher: {e}"))
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let cell: Option<String> = r.try_get("cell").unwrap_or(None);
            out.push(vec![cell.unwrap_or_else(|| "null".into())]);
        }
        Ok(out)
    }

    /// Dual-write an entity to the AGE graph. Failures are logged but do not
    /// fail the caller — the entities table is the source of truth.
    ///
    /// Uses MERGE with property matching — works correctly when the
    /// connection's search_path includes ag_catalog (see `normalize_pg_url`).
    /// entity_id is the UUID-form string to match what migration 0003's
    /// backfill writes (`r.id::text` on a UUID column).
    ///
    /// NOTE: AGE 1.7.0-rc0 doesn't support `ON CREATE SET` / `ON MATCH SET`
    /// spec syntax — use plain `SET` which applies unconditionally.
    async fn age_sync_entity(&self, e: &Entity) {
        let id = Self::cypher_escape(&e.id.as_uuid().to_string());
        let ns = Self::cypher_escape(&e.namespace);
        let typ = Self::cypher_escape(&e.entity_type);
        let name = Self::cypher_escape(&e.name);
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let cypher = format!(
                "MERGE (n:Entity {{entity_id: '{id}'}}) \
                 SET n.namespace = '{ns}', n.type = '{typ}', n.name = '{name}'"
            );
            let sql = format!(
                "SELECT (result)::text AS cell FROM ag_catalog.cypher('{GRAPH_NAME}', $$ {cypher} $$) AS __t(result ag_catalog.agtype);"
            );
            if let Err(err) = sqlx::query(&sql).fetch_all(&pool).await {
                tracing::warn!(error = %err, "AGE sync entity failed");
            }
        });
    }

    async fn age_delete_entity(&self, id: EntityId) {
        let id_s = Self::cypher_escape(&id.as_uuid().to_string());
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let cypher = format!("MATCH (n:Entity {{entity_id: '{id_s}'}}) DETACH DELETE n");
            let sql = format!(
                "SELECT (result)::text AS cell FROM ag_catalog.cypher('{GRAPH_NAME}', $$ {cypher} $$) AS __t(result ag_catalog.agtype);"
            );
            if let Err(err) = sqlx::query(&sql).fetch_all(&pool).await {
                tracing::info!(error = %err, "AGE delete entity (graph may be empty)");
            }
        });
    }

    async fn age_sync_relation(&self, r: &Relation) {
        let from = Self::cypher_escape(&r.from_id.as_uuid().to_string());
        let to = Self::cypher_escape(&r.to_id.as_uuid().to_string());
        let rid = Self::cypher_escape(&r.id.as_uuid().to_string());
        let ns = Self::cypher_escape(&r.namespace);
        let rt = Self::cypher_escape(&r.relation_type);
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let cypher = format!(
                "MATCH (a:Entity {{entity_id: '{from}'}}), (b:Entity {{entity_id: '{to}'}}) \
                 MERGE (a)-[rel:Relation {{relation_id: '{rid}'}}]->(b) \
                 SET rel.namespace = '{ns}', rel.type = '{rt}'"
            );
            let sql = format!(
                "SELECT (result)::text AS cell FROM ag_catalog.cypher('{GRAPH_NAME}', $$ {cypher} $$) AS __t(result ag_catalog.agtype);"
            );
            if let Err(err) = sqlx::query(&sql).fetch_all(&pool).await {
                tracing::warn!(error = %err, "AGE sync relation failed");
            }
        });
    }

    async fn age_delete_relation(&self, id: RelationId) {
        let id_s = Self::cypher_escape(&id.as_uuid().to_string());
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let cypher = format!("MATCH ()-[rel:Relation {{relation_id: '{id_s}'}}]->() DELETE rel");
            let sql = format!(
                "SELECT (result)::text AS cell FROM ag_catalog.cypher('{GRAPH_NAME}', $$ {cypher} $$) AS __t(result ag_catalog.agtype);"
            );
            if let Err(err) = sqlx::query(&sql).fetch_all(&pool).await {
                tracing::info!(error = %err, "AGE delete relation (graph may be empty)");
            }
        });
    }

    async fn upsert_embedding(&self, entity_id: EntityId, text: String) {
        let Some(embedder) = &self.embedder else {
            return;
        };
        let embedder = embedder.clone();
        let pool = self.pool.clone();
        tokio::spawn(async move {
            match embedder.embed(&text).await {
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
                        .execute(&pool)
                        .await
                    {
                        tracing::warn!(error = %e, "embedding upsert failed");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "embedding failed; entity will not be searchable via vector");
                }
            }
        });
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

        // Compute + persist embedding (async, fire-and-forget).
        let text = cmdb_embedding::text_for_entity(&entity);
        self.upsert_embedding(entity.id, text).await;

        // Dual-write to Apache AGE graph (async, fire-and-forget).
        self.age_sync_entity(&entity).await;

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

#[allow(unused_assignments)]
    async fn query_entities(&self, filter: QueryFilter) -> StoreResult<Vec<Entity>> {
        use sqlx::QueryBuilder;
        let mut qb = QueryBuilder::new(
            r#"SELECT id, namespace, type AS entity_type, name, attrs, tags,
                      created_at, updated_at, version
               FROM entities WHERE 1=1"#
        );
        if let Some(ns) = &filter.namespace {
            qb.push(" AND namespace = ").push_bind(ns.clone());
        }
        if let Some(t) = &filter.entity_type {
            qb.push(" AND type = ").push_bind(t.clone());
        }
        if let Some(p) = &filter.name_prefix {
            qb.push(" AND name LIKE ").push_bind(format!("{}%", p));
        }
        if !filter.tags.is_empty() {
            qb.push(" AND tags @> ").push_bind(filter.tags.clone());
        }
        qb.push(" ORDER BY created_at");
        if let Some(limit) = filter.limit {
            qb.push(" LIMIT ").push_bind(limit as i64);
        }
        let rows = qb.build().fetch_all(&self.pool).await.map_err(pg_err)?;
        Ok(rows.iter().map(map_entity).collect())
    }

    async fn delete_entity(&self, id: EntityId) -> StoreResult<()> {
        sqlx::query(queries::DELETE_ENTITY_BY_ID)
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        self.age_delete_entity(id).await;
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
        self.age_sync_relation(&relation).await;
        Ok(relation)
    }

    async fn delete_relation(&self, id: RelationId) -> StoreResult<()> {
        sqlx::query("DELETE FROM relations WHERE id = $1")
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        self.age_delete_relation(id).await;
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

    async fn text_search(
        &self,
        q: &str,
        namespace: &str,
        limit: u32,
    ) -> StoreResult<Vec<Entity>> {
        let rows = sqlx::query(
            r#"SELECT id, namespace, type AS entity_type, name, attrs, tags,
                      created_at, updated_at, version
               FROM entities
               WHERE namespace = $1 AND name % $2
               ORDER BY similarity(name, $2) DESC
               LIMIT $3"#,
        )
        .bind(namespace)
        .bind(q)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(pg_err)?;
        Ok(rows.iter().map(map_entity).collect())
    }

    async fn cypher(&self, query: &str) -> StoreResult<Vec<Vec<String>>> {
        // Try AGE native first. With search_path=public,ag_catalog (set by
        // normalize_pg_url at connect time), AGE 1.7.0-rc0 handles vertex
        // queries, MERGE, WHERE on most patterns. When AGE stabilizes in
        // a future 1.7.x / 1.8.x release, the translator fallback below
        // will become dead code and can be deleted in a one-line PR.
        match self.age_query_raw(query).await {
            Ok(rows) => return Ok(rows),
            Err(age_err) => {
                tracing::debug!(error = %age_err, cypher = %query, "AGE native failed; trying translator");
                // Fall through to translator.
                let age_err = age_err;
                return match crate::cypher::translate(query) {
                    Ok((sql, _cols)) => {
                        let rows = sqlx::query(&sql)
                            .fetch_all(&self.pool)
                            .await
                            .map_err(|e| {
                                StoreError::Backend(format!(
                                    "age: {age_err} | translator SQL exec: {e}"
                                ))
                            })?;
                        let mut out = Vec::with_capacity(rows.len());
                        for r in &rows {
                            let cell: Option<String> = r.try_get("result").unwrap_or(None);
                            out.push(vec![cell.unwrap_or_else(|| "null".into())]);
                        }
                        Ok(out)
                    }
                    Err(trans_err) => Err(StoreError::Backend(format!(
                        "age: {age_err} | translator: {trans_err}"
                    ))),
                };
            }
        }
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
