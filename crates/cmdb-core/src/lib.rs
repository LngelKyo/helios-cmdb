//! helios-cmdb core domain model.
//!
//! Zero IO. Defines the 4-tuple (Entity / Relation / Fact / Change) plus the
//! MetaModel and the `Store` trait that backends implement.

pub mod change;
pub mod entity;
pub mod error;
pub mod fact;
pub mod id;
pub mod metamodel;
pub mod namespace;
pub mod relation;
pub mod source;
pub mod store;

pub mod mock;

pub use change::*;
pub use entity::*;
pub use error::*;
pub use fact::*;
pub use id::*;
pub use metamodel::*;
pub use namespace::*;
pub use relation::*;
pub use source::*;
pub use store::*;

pub const DEFAULT_NAMESPACE: &str = "cc.fleet";
