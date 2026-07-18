//! Fleet namespace and type aliases.
//!
//! `Namespace` mirrors the ana prefix on the bus (default `cc.fleet`). All
//! tables carry it as a first-class column so multiple fleets can share one
//! CMDB instance, or be physically isolated by running separate instances.

pub type Namespace = String;
pub type EntityType = String;
pub type RelationType = String;

pub const DEFAULT_NAMESPACE: &str = "cc.fleet";
