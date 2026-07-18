//! Confidence decay, ttl expiry, effective-fact computation utilities.
//!
//! P0 stub. Real impl lands in P1 alongside collectors.

pub use cmdb_core::source::Source;

pub fn confidence_after(_observed_confidence: f32, _age_seconds: i64, _ttl_seconds: Option<i64>) -> f32 {
    0.0
}
