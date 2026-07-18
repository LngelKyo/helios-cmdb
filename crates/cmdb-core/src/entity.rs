//! Entity: one configuration item.

use crate::id::EntityId;
use crate::namespace::{EntityType, Namespace};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub namespace: Namespace,
    #[serde(rename = "type")]
    pub entity_type: EntityType,
    pub name: String,
    pub attrs: Value,
    pub tags: BTreeSet<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityInput {
    pub namespace: Namespace,
    pub entity_type: EntityType,
    pub name: String,
    #[serde(default)]
    pub attrs: Value,
    #[serde(default)]
    pub tags: BTreeSet<String>,
}

impl EntityInput {
    pub fn new(
        namespace: impl Into<Namespace>,
        entity_type: impl Into<EntityType>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            entity_type: entity_type.into(),
            name: name.into(),
            attrs: Value::Object(Default::default()),
            tags: Default::default(),
        }
    }

    pub fn with_attrs(mut self, attrs: Value) -> Self {
        self.attrs = attrs;
        self
    }

    pub fn with_tags<I: IntoIterator<Item = String>>(mut self, tags: I) -> Self {
        self.tags = tags.into_iter().collect();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EntityRef {
    Id { id: EntityId },
    Name {
        namespace: Namespace,
        #[serde(rename = "type")]
        entity_type: EntityType,
        name: String,
    },
}

impl EntityRef {
    pub fn by_id(id: EntityId) -> Self {
        EntityRef::Id { id }
    }

    pub fn by_name(
        namespace: impl Into<Namespace>,
        entity_type: impl Into<EntityType>,
        name: impl Into<String>,
    ) -> Self {
        EntityRef::Name {
            namespace: namespace.into(),
            entity_type: entity_type.into(),
            name: name.into(),
        }
    }
}
