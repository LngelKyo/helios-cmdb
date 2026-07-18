-- Apache AGE graph layer.
-- Mirrors entities (as :Entity vertices) and relations (as :Relation edges)
-- into a Cypher-queryable graph named `helios`. Application code does the
-- dual-write at the Store layer; this migration just sets up the graph,
-- labels, and backfills existing rows.
--
-- Cypher queries are then possible:
--   SELECT * FROM ag_catalog.cypher('helios', $$
--     MATCH (a:Entity {type:'fleet.agent'})-[:Relation {type:'runs_on'}]->(h:Entity)
--     RETURN a.name, h.name
--   $$) AS (a agtype, h agtype);

CREATE EXTENSION IF NOT EXISTS age;
SET search_path = ag_catalog, "$user", public;

SELECT create_graph('helios');
SELECT create_vlabel('helios', 'Entity');
SELECT create_elabel('helios', 'Relation');

-- Backfill: sync every existing entity to an :Entity vertex.
-- Note: outer DO uses $age_do$ ... $age_do$ so it doesn't conflict with the
-- inner cypher() dollar-quoting (which uses plain $$).
DO $age_do$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT id::text AS id, namespace, type AS entity_type, name FROM entities LOOP
        PERFORM * FROM ag_catalog.cypher('helios',
            $$ CREATE (:Entity {entity_id: '` || r.id || `', namespace: '` || r.namespace || `', type: '` || r.entity_type || `', name: '` || r.name || `'}) $$
        ) AS (v agtype);
    END LOOP;
END $age_do$;

-- Backfill: sync every existing relation to an :Relation edge.
DO $age_do$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT rel.id::text AS id, rel.namespace, rel.type AS rel_type,
               rel.from_id::text AS from_id, rel.to_id::text AS to_id
          FROM relations rel
    LOOP
        PERFORM * FROM ag_catalog.cypher('helios',
            $$ MATCH (a:Entity {entity_id: '` || r.from_id || `'}), (b:Entity {entity_id: '` || r.to_id || `'})
               CREATE (a)-[:Relation {relation_id: '` || r.id || `', namespace: '` || r.namespace || `', type: '` || r.rel_type || `'}]->(b) $$
        ) AS (e agtype);
    END LOOP;
END $age_do$;
