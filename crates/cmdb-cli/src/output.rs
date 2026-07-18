//! Pretty-printing helpers for CLI output.

use cmdb_core::change::Change;
use cmdb_core::entity::Entity;
use cmdb_core::store::TraverseHit;
use serde_json::Value;

pub fn entity(e: &Entity) {
    println!("{}", serde_json::to_string_pretty(e).unwrap_or_default());
}

pub fn entities(es: &[Entity]) {
    if es.is_empty() {
        println!("(none)");
        return;
    }
    println!(
        "{:<28} {:<18} {:<20} {:<8}",
        "id", "type", "name", "version"
    );
    println!("{}", "-".repeat(80));
    for e in es {
        println!(
            "{:<28} {:<18} {:<20} {:<8}",
            e.id, e.entity_type, e.name, e.version
        );
    }
    println!("\n{} entr{}", es.len(), if es.len() == 1 { "y" } else { "ies" });
}

pub fn traverse_hits(hits: &[TraverseHit]) {
    if hits.is_empty() {
        println!("(no neighbors)");
        return;
    }
    for h in hits {
        println!(
            "[d{}] {:<26} via {:<14} {}",
            h.depth,
            h.entity.id.to_string(),
            h.via_relation_type.as_deref().unwrap_or("-"),
            h.entity.name
        );
        let path_labels: Vec<String> = h
            .path
            .iter()
            .map(|id| id.to_string().chars().take(8).collect::<String>())
            .collect();
        println!("       path: {}", path_labels.join(" -> "));
    }
    println!("\n{} hit{}", hits.len(), if hits.len() == 1 { "" } else { "s" });
}

pub fn change_row(c: &Change) {
    println!(
        "{} {:<10} {:<14} by {}",
        c.ts.format("%Y-%m-%dT%H:%M:%SZ"),
        c.op.as_str(),
        c.target_type,
        c.actor
    );
    if let Some(after) = &c.after {
        let trimmed = trim_json(after, 200);
        println!("       after: {}", trimmed);
    }
}

fn trim_json(v: &Value, max: usize) -> String {
    let s = serde_json::to_string(v).unwrap_or_default();
    if s.len() > max {
        format!("{}…", &s[..max])
    } else {
        s
    }
}

pub fn kv(label: &str, value: impl std::fmt::Display) {
    println!("{:<14} {}", format!("{label}:"), value);
}
