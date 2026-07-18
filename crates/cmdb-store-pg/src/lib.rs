//! Postgres store implementation.
//!
//! P0: see `migration`, `entity`, `relation`, `fact`, `traverse` modules.

pub mod migration;
pub mod store;
mod queries;

pub use store::PgStore;
