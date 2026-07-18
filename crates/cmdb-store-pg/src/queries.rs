//! Raw SQL string constants. P0 uses runtime `query()`/`query_as()` so we
//! don't need DATABASE_URL at compile time.

pub const UPSERT_ENTITY: &str = r#"
INSERT INTO entities (id, namespace, type, name, attrs, tags, created_at, updated_at, version)
VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), 1)
ON CONFLICT (namespace, type, name) DO UPDATE
  SET attrs = EXCLUDED.attrs,
      tags  = EXCLUDED.tags,
      updated_at = NOW(),
      version = entities.version + 1
RETURNING
  id,
  namespace,
  type   AS entity_type,
  name,
  attrs,
  tags,
  created_at,
  updated_at,
  version
"#;

pub const GET_ENTITY_BY_NAME: &str = r#"
SELECT id, namespace, type AS entity_type, name, attrs, tags, created_at, updated_at, version
FROM entities
WHERE namespace = $1 AND type = $2 AND name = $3
"#;

pub const GET_ENTITY_BY_ID: &str = r#"
SELECT id, namespace, type AS entity_type, name, attrs, tags, created_at, updated_at, version
FROM entities
WHERE id = $1
"#;

pub const DELETE_ENTITY_BY_ID: &str = r#"
DELETE FROM entities WHERE id = $1
"#;

pub const INSERT_RELATION: &str = r#"
INSERT INTO relations (id, namespace, from_id, to_id, type, props, created_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (namespace, from_id, to_id, type) DO UPDATE
  SET props = EXCLUDED.props
RETURNING id, namespace, from_id, to_id, type AS relation_type, props, created_at
"#;

pub const INSERT_FACT: &str = r#"
WITH new_fact AS (
  INSERT INTO facts (id, namespace, entity_id, key, value, source, confidence, observed_at, ttl_seconds)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
  RETURNING id, namespace, entity_id, key, value, source, superseded_by
)
UPDATE facts
   SET superseded_by = (SELECT id FROM new_fact)
 WHERE facts.entity_id = $3
   AND facts.key = $4
   AND facts.superseded_by IS NULL
   AND facts.id <> (SELECT id FROM new_fact)
RETURNING (SELECT id FROM new_fact) AS new_id;
"#;

pub const INSERT_FACT_SIMPLE: &str = r#"
INSERT INTO facts (id, namespace, entity_id, key, value, source, confidence, observed_at, ttl_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id, namespace, entity_id, key, value, source, superseded_by
"#;

pub const SUPERSEDE_PRIOR_FACTS: &str = r#"
UPDATE facts
   SET superseded_by = $1
 WHERE facts.entity_id = $2
   AND facts.key = $3
   AND facts.superseded_by IS NULL
   AND facts.id <> $1
"#;

pub const EFFECTIVE_FACTS: &str = r#"
SELECT id, namespace, entity_id, key, value, source, superseded_by
FROM facts
WHERE entity_id = $1
  AND superseded_by IS NULL
  AND ($2::real IS NULL OR (source->>'confidence')::real >= $2)
ORDER BY key
"#;

pub const TRAVERSE_OUTGOING: &str = r#"
WITH RECURSIVE walk AS (
  SELECT to_id AS cur, 1::int AS depth, ARRAY[from_id, to_id] AS path, type AS via
    FROM relations
   WHERE from_id = $1
     AND namespace = $2
     AND ($3::text IS NULL OR type = $3)
  UNION
  SELECT r.to_id, w.depth + 1, w.path || r.to_id, r.type
    FROM walk w
    JOIN relations r ON r.from_id = w.cur
   WHERE r.namespace = $2
     AND w.depth < $4
     AND ($3::text IS NULL OR r.type = $3)
     AND r.to_id <> ALL (w.path)
)
SELECT e.id, e.namespace, e.type AS entity_type, e.name, e.attrs, e.tags,
       e.created_at, e.updated_at, e.version, w.depth, w.path, w.via
  FROM walk w
  JOIN entities e ON e.id = w.cur
 ORDER BY w.depth, e.name;
"#;

pub const TRAVERSE_INCOMING: &str = r#"
WITH RECURSIVE walk AS (
  SELECT from_id AS cur, 1::int AS depth, ARRAY[to_id, from_id] AS path, type AS via
    FROM relations
   WHERE to_id = $1
     AND namespace = $2
     AND ($3::text IS NULL OR type = $3)
  UNION
  SELECT r.from_id, w.depth + 1, w.path || r.from_id, r.type
    FROM walk w
    JOIN relations r ON r.to_id = w.cur
   WHERE r.namespace = $2
     AND w.depth < $4
     AND ($3::text IS NULL OR r.type = $3)
     AND r.from_id <> ALL (w.path)
)
SELECT e.id, e.namespace, e.type AS entity_type, e.name, e.attrs, e.tags,
       e.created_at, e.updated_at, e.version, w.depth, w.path, w.via
  FROM walk w
  JOIN entities e ON e.id = w.cur
 ORDER BY w.depth, e.name;
"#;

pub const TRAVERSE_BOTH: &str = r#"
WITH RECURSIVE walk AS (
  SELECT CASE WHEN from_id = $1 THEN to_id ELSE from_id END AS cur,
         1::int AS depth,
         ARRAY[$1, CASE WHEN from_id = $1 THEN to_id ELSE from_id END] AS path,
         type AS via
    FROM relations
   WHERE namespace = $2
     AND (from_id = $1 OR to_id = $1)
     AND ($3::text IS NULL OR type = $3)
  UNION
  SELECT CASE WHEN r.from_id = w.cur THEN r.to_id ELSE r.from_id END,
         w.depth + 1,
         w.path || (CASE WHEN r.from_id = w.cur THEN r.to_id ELSE r.from_id END),
         r.type
    FROM walk w
    JOIN relations r ON (r.from_id = w.cur OR r.to_id = w.cur)
   WHERE r.namespace = $2
     AND w.depth < $4
     AND ($3::text IS NULL OR r.type = $3)
     AND (CASE WHEN r.from_id = w.cur THEN r.to_id ELSE r.from_id END) <> ALL (w.path)
)
SELECT e.id, e.namespace, e.type AS entity_type, e.name, e.attrs, e.tags,
       e.created_at, e.updated_at, e.version, w.depth, w.path, w.via
  FROM walk w
  JOIN entities e ON e.id = w.cur
 ORDER BY w.depth, e.name;
"#;

pub const INSERT_CHANGE: &str = r#"
INSERT INTO changes (id, namespace, actor, op, target_type, target_id, before, after, reason)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"#;

pub const HISTORY_FOR_ENTITY: &str = r#"
SELECT id, ts, namespace, actor, op, target_type, target_id, before, after, reason
  FROM changes
 WHERE ($1::uuid IS NULL OR target_id = $1)
   AND ($2::text IS NULL OR namespace = $2)
 ORDER BY ts DESC
 LIMIT $3
"#;
