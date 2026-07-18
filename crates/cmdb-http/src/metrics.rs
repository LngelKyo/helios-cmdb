//! Minimal Prometheus exposition-format metrics.
//!
//! No external metrics crate — we track counters in atomics and render the
//! text format directly. Adequate for a single-instance CMDB; for HA use
//! the `metrics` crate with a Prometheus exporter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Counters {
    pub http_requests: AtomicU64,
    pub http_requests_4xx: AtomicU64,
    pub http_requests_5xx: AtomicU64,
    pub entities_upserted: AtomicU64,
    pub facts_added: AtomicU64,
    pub relations_upserted: AtomicU64,
    pub cypher_queries: AtomicU64,
    pub vector_searches: AtomicU64,
}

pub type SharedCounters = Arc<Counters>;

pub fn shared() -> SharedCounters {
    Arc::new(Counters::default())
}

pub fn render(c: &Counters, entities_count: i64, relations_count: i64) -> String {
    let mut out = String::new();
    out.push_str("# HELP cmdb_http_requests_total Total HTTP requests received.\n");
    out.push_str("# TYPE cmdb_http_requests_total counter\n");
    out.push_str(&format!(
        "cmdb_http_requests_total {}\n",
        c.http_requests.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cmdb_http_requests_total{{code=\"4xx\"}} {}\n",
        c.http_requests_4xx.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cmdb_http_requests_total{{code=\"5xx\"}} {}\n",
        c.http_requests_5xx.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP cmdb_entities_total Current row count in entities table.\n");
    out.push_str("# TYPE cmdb_entities_total gauge\n");
    out.push_str(&format!("cmdb_entities_total {entities_count}\n"));
    out.push_str("# HELP cmdb_relations_total Current row count in relations table.\n");
    out.push_str("# TYPE cmdb_relations_total gauge\n");
    out.push_str(&format!("cmdb_relations_total {relations_count}\n"));
    out.push_str("# HELP cmdb_writes_total Total write operations.\n");
    out.push_str("# TYPE cmdb_writes_total counter\n");
    out.push_str(&format!(
        "cmdb_writes_total{{op=\"entity_upsert\"}} {}\n",
        c.entities_upserted.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cmdb_writes_total{{op=\"fact_add\"}} {}\n",
        c.facts_added.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cmdb_writes_total{{op=\"relation_upsert\"}} {}\n",
        c.relations_upserted.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cmdb_queries_total{{op=\"cypher\"}} {}\n",
        c.cypher_queries.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cmdb_queries_total{{op=\"vector_search\"}} {}\n",
        c.vector_searches.load(Ordering::Relaxed)
    ));
    out
}
