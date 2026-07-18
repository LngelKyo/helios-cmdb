//! Minimal Cypher → SQL translator for the common patterns used in CMDB
//! queries.
//!
//! Apache AGE 1.7.0-rc0 has known bugs in edge traversal and boolean
//! expressions, so we translate the most useful subset to SQL directly
//! against the entities/relations tables. The AGE migration is kept for
//! when AGE stabilizes; raw AGE queries can still be made via
//! `cypher_raw()`.
//!
//! Supported patterns:
//!   MATCH (n) RETURN count(n)
//!   MATCH (n:Label) RETURN n
//!   MATCH (n) WHERE n.name = 'x' RETURN n
//!   MATCH (n) RETURN n LIMIT k
//!   MATCH (a:LabelA)-[:REL_TYPE]->(b:LabelB) RETURN a, b
//!   MATCH (a)-[:REL_TYPE]->(b) WHERE a.name = 'x' RETURN a.name, b.name

use cmdb_core::error::{StoreError, StoreResult};

pub fn translate(cypher: &str) -> StoreResult<(String, Vec<String>)> {
    let q = cypher.trim().trim_end_matches(';').trim();
    let lower = q.to_lowercase();

    if !lower.starts_with("match") {
        return Err(StoreError::Invalid("query must start with MATCH".into()));
    }

    let limit = extract_limit(&lower);
    let where_clause = extract_where(q);

    // Edge pattern first (more specific).
    if let Some(edge) = parse_edge_pattern(q) {
        return translate_edge(edge, &where_clause, limit, q);
    }

    // Vertex-only pattern.
    if let Some((label, var)) = parse_vertex_pattern(q) {
        return translate_vertex(&label, &var, &where_clause, limit, q);
    }

    Err(StoreError::Invalid(format!(
        "cypher pattern not supported by translator; try a simpler form \
         (MATCH (n:Label) RETURN n, MATCH (a)-[:TYPE]->(b) RETURN a, b). \
         Raw query was: {q}"
    )))
}

fn translate_vertex(
    label: &Option<String>,
    var: &str,
    where_clause: &Option<WhereClause>,
    limit: Option<u32>,
    q: &str,
) -> StoreResult<(String, Vec<String>)> {
    // RETURN count(n) -> SELECT count(*)
    if parse_return_count(q).is_some() {
        let mut sql = String::from("SELECT count(*)::text AS result FROM entities");
        let mut conds = Vec::new();
        if let Some(l) = label {
            conds.push(format!("type = '{}'", esc(l)));
        }
        if let Some(w) = where_clause {
            conds.push(w.sql.clone());
        }
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        return Ok((sql, vec!["count".into()]));
    }

    let returns = parse_return_props(q);
    if returns.is_empty() {
        return Err(StoreError::Invalid(
            "vertex query needs a RETURN clause".into(),
        ));
    }

    let cols: Vec<String> = returns
        .iter()
        .map(|ret| {
            if ret == var {
                row_as_json("e")
            } else if let Some(rest) = ret.strip_prefix(&format!("{var}.")) {
                col_for("e", rest)
            } else {
                format!("'{}'::text", esc(ret))
            }
        })
        .collect();

    let col_list = bundle_cols(&returns, &cols);
    let mut sql = format!("SELECT {col_list} AS result FROM entities e");
    let mut conds = Vec::new();
    if let Some(l) = label {
        conds.push(format!("e.type = '{}'", esc(l)));
    }
    if let Some(w) = where_clause {
        conds.push(w.sql.clone());
    }
    if !conds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }
    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {l}"));
    }
    Ok((sql, vec!["entity".into()]))
}

fn translate_edge(
    edge: EdgePattern,
    where_clause: &Option<WhereClause>,
    limit: Option<u32>,
    q: &str,
) -> StoreResult<(String, Vec<String>)> {
    let from_alias = "e1";
    let to_alias = "e2";
    let rel_alias = "r";
    let mut conds = vec![format!(
        "{rel_alias}.from_id = {from_alias}.id AND {rel_alias}.to_id = {to_alias}.id"
    )];
    if let Some(l) = edge.from_label {
        conds.push(format!("{from_alias}.type = '{}'", esc(&l)));
    }
    if let Some(l) = edge.to_label {
        conds.push(format!("{to_alias}.type = '{}'", esc(&l)));
    }
    if let Some(rt) = edge.rel_type {
        conds.push(format!("{rel_alias}.type = '{}'", esc(&rt)));
    }
    if let Some(w) = where_clause {
        conds.push(w.sql.clone());
    }

    let returns = parse_return_props(q);
    let cols: Vec<String> = returns
        .iter()
        .map(|ret| {
            if ret == &edge.from_var {
                row_as_json(from_alias)
            } else if ret == &edge.to_var {
                row_as_json(to_alias)
            } else if let Some(rest) = ret.strip_prefix(&format!("{}.", edge.from_var)) {
                col_for(from_alias, rest)
            } else if let Some(rest) = ret.strip_prefix(&format!("{}.", edge.to_var)) {
                col_for(to_alias, rest)
            } else {
                format!("'{}'::text", esc(ret))
            }
        })
        .collect();

    let col_list = if cols.is_empty() {
        format!("(e1.name || ' -> ' || e2.name)::text")
    } else {
        bundle_cols(&returns, &cols)
    };

    let mut sql = format!(
        "SELECT {col_list} AS result FROM entities {from_alias}, relations {rel_alias}, entities {to_alias} WHERE {}",
        conds.join(" AND ")
    );
    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {l}"));
    }
    Ok((sql, vec!["edge".into()]))
}

fn row_as_json(alias: &str) -> String {
    format!(
        "(SELECT row_to_json(t)::text FROM (SELECT id::text, namespace, type, name, attrs, tags FROM entities e2 WHERE e2.id = {alias}.id) t)"
    )
}

fn col_for(alias: &str, prop: &str) -> String {
    match prop {
        "name" => format!("{}.name::text", alias),
        "type" => format!("{}.type::text", alias),
        "namespace" => format!("{}.namespace::text", alias),
        "id" => format!("{}.id::text", alias),
        "attrs" => format!("{}.attrs::text", alias),
        other => format!("{}.attrs->>'{}'", alias, esc(other)),
    }
}

fn bundle_cols(returns: &[String], cols: &[String]) -> String {
    if cols.len() == 1 {
        return cols[0].clone();
    }
    let pairs: Vec<String> = returns
        .iter()
        .zip(cols.iter())
        .map(|(name, col)| format!("'{}', {}", esc(name), col))
        .collect();
    format!("json_build_object({})::text", pairs.join(", "))
}

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

fn extract_limit(lower: &str) -> Option<u32> {
    let idx = lower.find("limit ")?;
    let after = &lower[idx + 6..];
    let num: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    num.parse().ok()
}

struct WhereClause {
    sql: String,
}

fn extract_where(q: &str) -> Option<WhereClause> {
    let lower = q.to_lowercase();
    let idx = lower.find(" where ")?;
    let after_where = &q[idx + 7..];
    let lower_after = after_where.to_lowercase();
    let end = lower_after
        .find(" return ")
        .or_else(|| lower_after.find(" limit "))
        .unwrap_or(after_where.len());
    let raw = after_where[..end].trim();
    let translated = raw
        .replace("n.name", "e.name")
        .replace("n.type", "e.type")
        .replace("n.namespace", "e.namespace");
    Some(WhereClause { sql: translated })
}

fn parse_vertex_pattern(q: &str) -> Option<(Option<String>, String)> {
    let start = q.find('(')?;
    let end = q[start..].find(')')?;
    let inner = &q[start + 1..start + end];
    // Reject edge patterns.
    let after = &q[start + end + 1..];
    if after.contains("->") {
        return None;
    }
    if inner.is_empty() {
        return Some((None, "n".into()));
    }
    let mut parts = inner.splitn(2, ':');
    let var = parts.next()?.trim();
    if var.is_empty() {
        return None;
    }
    let label = parts
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    Some((label, var.to_string()))
}

struct EdgePattern {
    from_var: String,
    from_label: Option<String>,
    to_var: String,
    to_label: Option<String>,
    rel_type: Option<String>,
}

fn parse_edge_pattern(q: &str) -> Option<EdgePattern> {
    let open1 = q.find('(')?;
    let close1 = q[open1..].find(')')?;
    let seg1 = &q[open1 + 1..open1 + close1];
    let after1 = &q[open1 + close1 + 1..];
    if !after1.contains("->") {
        return None;
    }
    let rel_start = after1.find("-[")?;
    let rel_end = after1[rel_start..].find("]")?;
    let rel_inner = after1[rel_start + 2..rel_start + rel_end].trim();
    let rel_type = rel_inner
        .strip_prefix(':')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let after_rel = &after1[rel_start + rel_end + 1..];
    let arrow = after_rel.find("->")?;
    let after_arrow = &after_rel[arrow + 2..];
    let open2 = after_arrow.find('(')?;
    let close2 = after_arrow[open2..].find(')')?;
    let seg2 = &after_arrow[open2 + 1..open2 + close2];

    let (from_var, from_label) = parse_var_label(seg1)?;
    let (to_var, to_label) = parse_var_label(seg2)?;
    Some(EdgePattern {
        from_var,
        from_label,
        to_var,
        to_label,
        rel_type,
    })
}

fn parse_var_label(seg: &str) -> Option<(String, Option<String>)> {
    let seg = seg.trim();
    if seg.is_empty() {
        return Some(("n".into(), None));
    }
    let mut parts = seg.splitn(2, ':');
    let var = parts.next()?.trim().to_string();
    let label = parts
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    Some((var, label))
}

fn parse_return_count(q: &str) -> Option<String> {
    let lower = q.to_lowercase();
    let idx = lower.find("return count(")?;
    let after = &q[idx + 14..];
    let paren = after.find(')')?;
    let var = after[..paren].trim().to_string();
    Some(var)
}

fn parse_return_props(q: &str) -> Vec<String> {
    let lower = q.to_lowercase();
    let idx = match lower.find("return ") {
        Some(i) => i + 7,
        None => return Vec::new(),
    };
    let rest = &q[idx..];
    let end = rest.to_lowercase().find(" limit ").unwrap_or(rest.len());
    let raw = rest[..end].trim();
    if raw.is_empty() || parse_return_count(q).is_some() {
        return Vec::new();
    }
    raw.split(',').map(|s| s.trim().to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_all() {
        let (sql, _) = translate("MATCH (n) RETURN count(n)").unwrap();
        assert!(sql.contains("SELECT count(*)"));
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn vertex_by_label() {
        let (sql, _) = translate("MATCH (n:fleet.host) RETURN n LIMIT 10").unwrap();
        assert!(sql.contains("type = 'fleet.host'"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn edge_pattern() {
        let (sql, _) = translate(
            "MATCH (a:fleet.agent)-[:runs_on]->(b:fleet.host) RETURN a.name, b.name",
        )
        .unwrap();
        eprintln!("edge SQL: {sql}");
        assert!(sql.contains("FROM entities"));
        assert!(sql.contains("relations"));
        assert!(sql.contains("type = 'runs_on'"));
        assert!(sql.contains("type = 'fleet.agent'"));
        assert!(sql.contains("type = 'fleet.host'"));
    }

    #[test]
    fn edge_pattern_h_var() {
        let (sql, _) = translate(
            "MATCH (a:fleet.agent)-[:runs_on]->(h:fleet.host) RETURN a.name, h.name",
        )
        .unwrap();
        eprintln!("edge SQL with h: {sql}");
        // Both a.name and h.name should be column refs, not literals.
        assert!(sql.contains("e1.name"));
        assert!(sql.contains("e2.name"));
        assert!(!sql.contains("'h.name'::text"));
    }

    #[test]
    fn where_clause() {
        let (sql, _) = translate("MATCH (n) WHERE n.name = 'e15' RETURN n").unwrap();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("e.name = 'e15'"));
    }
}
