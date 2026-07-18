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

-- Backfill entities. NOTE: `|| r.id ||` must be evaluated by PL/pgSQL,
-- NOT inside the dollar-quoted cypher string. We build the SQL via string
-- concatenation and EXECUTE it. Using `format('%s', ...)` would also work
-- but `||` is more explicit here.
DO $age_do$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT id::text AS id, namespace, type AS entity_type, name FROM entities LOOP
        EXECUTE 'SELECT * FROM ag_catalog.cypher(''helios'', $$ CREATE (:Entity {entity_id: '''
                || r.id
                || ''', namespace: '''
                || r.namespace
                || ''', type: '''
                || r.entity_type
                || ''', name: '''
                || r.name
                || '''}) $$) AS __t(v ag_catalog.agtype)';
    END LOOP;
END $age_do$;

-- Backfill relations. Same pattern: build SQL via ||, EXECUTE it.
DO $age_do$
DECLARE r RECORD;
BEGIN
    FOR r IN
        SELECT rel.id::text AS id, rel.namespace, rel.type AS rel_type,
               rel.from_id::text AS from_id, rel.to_id::text AS to_id
          FROM relations rel
    LOOP
        EXECUTE 'SELECT * FROM ag_catalog.cypher(''helios'', $$ MATCH (a:Entity) WHERE a.entity_id = '''
                || r.from_id
                || ''' MATCH (b:Entity) WHERE b.entity_id = '''
                || r.to_id
                || ''' CREATE (a)-[:Relation {relation_id: '''
                || r.id
                || ''', namespace: '''
                || r.namespace
                || ''', type: '''
                || r.rel_type
                || '''}]->(b) $$) AS __t(v ag_catalog.agtype)';
    END LOOP;
END $age_do$;
