//! Fact: a versioned attribute observation with provenance.
//!
//! Multiple facts for the same `(entity_id, key)` coexist. The effective one
//! is determined at query time: newest non-expired, weighted by confidence.

use crate::entity::EntityRef;
use crate::id::{EntityId, FactId};
use crate::namespace::Namespace;
use crate::source::Source;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fact {
    pub id: FactId,
    pub namespace: Namespace,
    pub entity_id: EntityId,
    pub key: String,
    pub value: Value,
    pub source: Source,
    pub superseded_by: Option<FactId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactInput {
    pub namespace: Namespace,
    pub entity: EntityRef,
    pub key: String,
    pub value: Value,
    pub source: Source,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FactQuery {
    pub min_confidence: Option<f32>,
    pub max_age_seconds: Option<i64>,
    pub include_expired: bool,
    pub source_kinds: Vec<crate::source::SourceKind>,
}
