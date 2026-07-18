//! Change: append-only event log entry. The source of truth for "what
//! happened"; NATS publishes are derived from this.

use crate::id::{ChangeId, EntityId};
use crate::namespace::Namespace;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    EntityUpsert,
    EntityDelete,
    FactAdd,
    FactSupersede,
    RelationUpsert,
    RelationDelete,
    MetaModelChange,
}

impl ChangeOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeOp::EntityUpsert => "entity_upsert",
            ChangeOp::EntityDelete => "entity_delete",
            ChangeOp::FactAdd => "fact_add",
            ChangeOp::FactSupersede => "fact_supersede",
            ChangeOp::RelationUpsert => "relation_upsert",
            ChangeOp::RelationDelete => "relation_delete",
            ChangeOp::MetaModelChange => "metamodel_change",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub id: ChangeId,
    pub ts: DateTime<Utc>,
    pub namespace: Namespace,
    pub actor: String,
    pub op: ChangeOp,
    pub target_type: String,
    pub target_id: Option<EntityId>,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub reason: Option<String>,
}
