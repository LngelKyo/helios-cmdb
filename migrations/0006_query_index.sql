-- Composite index for query_entities: WHERE namespace=$1 AND type=$2 ORDER BY created_at
-- Eliminates the sort step when filtering by type within a namespace.

CREATE INDEX IF NOT EXISTS entities_by_type_created
    ON entities (namespace, type, created_at DESC);