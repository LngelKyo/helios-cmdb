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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_all_metrics() {
        let c = Counters::default();
        c.http_requests.store(42, Ordering::Relaxed);
        c.http_requests_4xx.store(3, Ordering::Relaxed);
        c.http_requests_5xx.store(1, Ordering::Relaxed);
        c.entities_upserted.store(100, Ordering::Relaxed);
        c.facts_added.store(50, Ordering::Relaxed);
        c.relations_upserted.store(10, Ordering::Relaxed);
        c.cypher_queries.store(5, Ordering::Relaxed);
        c.vector_searches.store(8, Ordering::Relaxed);

        let out = render(&c, 9999, 42);
        // Verify Prometheus text format essentials.
        assert!(out.contains("# HELP cmdb_http_requests_total"));
        assert!(out.contains("# TYPE cmdb_http_requests_total counter"));
        assert!(out.contains("cmdb_http_requests_total 42\n"));
        assert!(out.contains("cmdb_http_requests_total{code=\"4xx\"} 3"));
        assert!(out.contains("cmdb_http_requests_total{code=\"5xx\"} 1"));
        assert!(out.contains("cmdb_entities_total 9999"));
        assert!(out.contains("cmdb_relations_total 42"));
        assert!(out.contains("cmdb_writes_total{op=\"entity_upsert\"} 100"));
        assert!(out.contains("cmdb_writes_total{op=\"fact_add\"} 50"));
        assert!(out.contains("cmdb_writes_total{op=\"relation_upsert\"} 10"));
        assert!(out.contains("cmdb_queries_total{op=\"cypher\"} 5"));
        assert!(out.contains("cmdb_queries_total{op=\"vector_search\"} 8"));
    }

    #[test]
    fn shared_returns_independent_counters() {
        let a = shared();
        let b = shared();
        a.http_requests.store(1, Ordering::Relaxed);
        // Different Arcs, independent state.
        assert_eq!(b.http_requests.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn counters_start_at_zero() {
        let c = Counters::default();
        assert_eq!(c.http_requests.load(Ordering::Relaxed), 0);
        assert_eq!(c.http_requests_4xx.load(Ordering::Relaxed), 0);
        assert_eq!(c.http_requests_5xx.load(Ordering::Relaxed), 0);
        assert_eq!(c.entities_upserted.load(Ordering::Relaxed), 0);
    }
}
