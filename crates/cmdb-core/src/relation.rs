//! Relation: directed edge between two entities.

use crate::entity::EntityRef;
use crate::id::{EntityId, RelationId};
use crate::namespace::{Namespace, RelationType};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub id: RelationId,
    pub namespace: Namespace,
    pub from_id: EntityId,
    pub to_id: EntityId,
    #[serde(rename = "type")]
    pub relation_type: RelationType,
    pub props: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationInput {
    pub namespace: Namespace,
    pub from: EntityRef,
    pub to: EntityRef,
    #[serde(rename = "type")]
    pub relation_type: RelationType,
    #[serde(default)]
    pub props: Value,
}

impl RelationInput {
    pub fn new(
        namespace: impl Into<Namespace>,
        from: EntityRef,
        to: EntityRef,
        relation_type: impl Into<RelationType>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            from,
            to,
            relation_type: relation_type.into(),
            props: Value::Object(Default::default()),
        }
    }
}
