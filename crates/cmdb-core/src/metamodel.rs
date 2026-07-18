//! MetaModel: schema-as-data. Entity and relation types are themselves stored
//! as rows so the data model can evolve at runtime without code changes or
//! migrations. Agents can introspect and (P4) propose new types.

use crate::namespace::Namespace;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityTypeDef {
    pub namespace: Namespace,
    pub name: String,
    pub description: Option<String>,
    pub attrs_schema: Value,
    pub allowed_relations: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationTypeDef {
    pub namespace: Namespace,
    pub name: String,
    pub from_types: Vec<String>,
    pub to_types: Vec<String>,
    pub props_schema: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl EntityTypeDef {
    pub fn new(
        namespace: impl Into<Namespace>,
        name: impl Into<String>,
        attrs_schema: Value,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            description: None,
            attrs_schema,
            allowed_relations: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

impl RelationTypeDef {
    pub fn new(namespace: impl Into<Namespace>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            from_types: Vec::new(),
            to_types: Vec::new(),
            props_schema: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}
