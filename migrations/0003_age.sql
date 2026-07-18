-- Apache AGE graph layer.
-- Mirrors entities (as :Entity vertices) and relations (as :Relation edges)
-- into a Cypher-queryable graph named `cmdb_graph`. Application code does
-- the dual-write at the Store layer; this migration just sets up the graph,
-- labels, and backfills existing rows.
--
-- Graph name choice: `cmdb_graph` (NOT `helios`) — AGE creates a same-named
-- schema for each graph, and `helios` is also the typical DB username,
-- which caused a search_path collision in earlier versions.
--
-- Idempotent: safe to re-run. All create_* calls are guarded by existence
-- checks. Backfills only insert (MERGE prevents duplicates).
--
-- Cypher queries are then possible (note: connection must have
-- search_path=public,ag_catalog for the agtype operators to resolve):
--   SELECT * FROM cypher('cmdb_graph', $$
--     MATCH (a:Entity {type:'fleet.agent'})-[:Relation {type:'runs_on'}]->(h:Entity)
--     RETURN a.name, h.name
--   $$) AS (a agtype, h agtype);

CREATE EXTENSION IF NOT EXISTS age;
SET search_path = ag_catalog, "$user", public;

-- Idempotent graph creation.
DO $graph_do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = 'cmdb_graph') THEN
        PERFORM ag_catalog.create_graph('cmdb_graph');
    END IF;
END $graph_do$;

-- Idempotent label creation.
DO $label_do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_label WHERE name = 'Entity') THEN
        PERFORM ag_catalog.create_vlabel('cmdb_graph', 'Entity');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_label WHERE name = 'Relation') THEN
        PERFORM ag_catalog.create_elabel('cmdb_graph', 'Relation');
    END IF;
END $label_do$;

-- Backfill entities via MERGE (so re-runs don't create duplicate vertices).
-- `|| r.id ||` is PL/pgSQL string concat, evaluated OUTSIDE the cypher
-- dollar-quoted string (the cypher text is built first, then EXECUTEd).
DO $age_do$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT id::text AS id, namespace, type AS entity_type, name FROM entities LOOP
        EXECUTE 'SELECT * FROM ag_catalog.cypher(''cmdb_graph'', $$ MERGE (n:Entity {entity_id: '''
                || r.id
                || '''}) ON CREATE SET n.namespace = '''
                || r.namespace
                || ''', n.type = '''
                || r.entity_type
                || ''', n.name = '''
                || r.name
                || ''' $$) AS __t(v ag_catalog.agtype)';
    END LOOP;
END $age_do$;

-- Backfill relations via MERGE.
DO $age_do$
DECLARE r RECORD;
BEGIN
    FOR r IN
        SELECT rel.id::text AS id, rel.namespace, rel.type AS rel_type,
               rel.from_id::text AS from_id, rel.to_id::text AS to_id
          FROM relations rel
    LOOP
        EXECUTE 'SELECT * FROM ag_catalog.cypher(''cmdb_graph'', $$ MATCH (a:Entity {entity_id: '''
                || r.from_id
                || '''}), (b:Entity {entity_id: '''
                || r.to_id
                || '''}) MERGE (a)-[:Relation {relation_id: '''
                || r.id
                || '''}]->(b) $$) AS __t(v ag_catalog.agtype)';
    END LOOP;
END $age_do$;
