//! Pluggable collectors. P1+ deliverable.
//!
//! Each collector implements `Collector` and is invoked via
//! `cmdb collector run <name>`.

use async_trait::async_trait;

pub trait Collector: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
}
