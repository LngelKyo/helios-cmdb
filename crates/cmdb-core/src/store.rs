//! The `Store` trait that all backends implement.

use crate::change::Change;
use crate::entity::{Entity, EntityInput, EntityRef};
use crate::error::{StoreError, StoreResult};
use crate::fact::{Fact, FactInput, FactQuery};
use crate::id::{EntityId, RelationId};
use crate::relation::{Relation, RelationInput};
use crate::source::Source;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryFilter {
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub entity_type: Option<String>,
    #[serde(default)]
    pub name_prefix: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub attrs_path: Option<String>,
    #[serde(default)]
    pub attrs_value: Option<serde_json::Value>,
    #[serde(default)]
    pub limit: Option<u32>,
}

impl QueryFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    pub fn of_type(mut self, t: impl Into<String>) -> Self {
        self.entity_type = Some(t.into());
        self
    }

    pub fn with_limit(mut self, n: u32) -> Self {
        self.limit = Some(n);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraverseStep {
    pub relation_type: Option<String>,
    pub direction: Direction,
    pub max_depth: u32,
}

impl TraverseStep {
    pub fn outgoing(max_depth: u32) -> Self {
        Self {
            relation_type: None,
            direction: Direction::Outgoing,
            max_depth,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraverseHit {
    pub entity: Entity,
    pub depth: u32,
    pub path: Vec<EntityId>,
    pub via_relation_type: Option<String>,
}

#[async_trait]
pub trait Store: Send + Sync {
    fn name(&self) -> &'static str {
        "store"
    }

    async fn put_entity(&self, input: EntityInput, source: Source) -> StoreResult<Entity>;

    async fn get_entity(
        &self,
        namespace: &str,
        entity_type: &str,
        name: &str,
    ) -> StoreResult<Option<Entity>>;

    async fn get_entity_by_id(&self, id: EntityId) -> StoreResult<Option<Entity>>;

    async fn resolve_ref(&self, namespace: &str, r: &EntityRef) -> StoreResult<Entity> {
        match r {
            EntityRef::Id { id } => self
                .get_entity_by_id(*id)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("entity {id}"))),
            EntityRef::Name {
                namespace: ns,
                entity_type,
                name,
            } => self
                .get_entity(ns, entity_type, name)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("{ns}/{entity_type}/{name}"))),
        }
        .map(|e| {
            let _ = namespace;
            e
        })
    }

    async fn query_entities(&self, filter: QueryFilter) -> StoreResult<Vec<Entity>>;

    async fn delete_entity(&self, id: EntityId) -> StoreResult<()>;

    async fn put_relation(&self, input: RelationInput) -> StoreResult<Relation>;

    async fn delete_relation(&self, id: RelationId) -> StoreResult<()>;

    async fn traverse(&self, from: EntityId, step: TraverseStep) -> StoreResult<Vec<TraverseHit>>;

    async fn add_fact(&self, input: FactInput) -> StoreResult<Fact>;

    async fn effective_facts(
        &self,
        entity_id: EntityId,
        query: FactQuery,
    ) -> StoreResult<Vec<Fact>>;

    async fn history(
        &self,
        namespace: Option<&str>,
        entity_id: Option<EntityId>,
        limit: u32,
    ) -> StoreResult<Vec<Change>>;
}
