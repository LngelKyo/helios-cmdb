-- helios-cmdb initial schema
-- Tables: entities, relations, relation_closure, facts, changes
--         + metamodel (entity_types, relation_types)
-- All tables carry `namespace` as a first-class column (ana prefix).

CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- for gen_random_uuid() fallback
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- fuzzy text matching (P2+)

-- ---------------------------------------------------------------------------
-- entities: one configuration item
-- ---------------------------------------------------------------------------
CREATE TABLE entities (
    id           UUID         PRIMARY KEY,
    namespace    TEXT         NOT NULL,
    type         TEXT         NOT NULL,
    name         TEXT         NOT NULL,
    attrs        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    tags         TEXT[]       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    version      INTEGER      NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX entities_unique_name
    ON entities (namespace, type, name);
CREATE INDEX entities_by_type
    ON entities (namespace, type);
CREATE INDEX entities_attrs_gin
    ON entities USING GIN (attrs jsonb_path_ops);
CREATE INDEX entities_tags_gin
    ON entities USING GIN (tags);
CREATE INDEX entities_name_trgm
    ON entities USING GIN (name gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- relations: directed edges
-- ---------------------------------------------------------------------------
CREATE TABLE relations (
    id           UUID         PRIMARY KEY,
    namespace    TEXT         NOT NULL,
    from_id      UUID         NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    to_id        UUID         NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    type         TEXT         NOT NULL,
    props        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX relations_unique_edge
    ON relations (namespace, from_id, to_id, type);
CREATE INDEX relations_from
    ON relations (namespace, from_id, type);
CREATE INDEX relations_to
    ON relations (namespace, to_id, type);

-- ---------------------------------------------------------------------------
-- relation_closure: precomputed paths for shallow topology queries.
-- Depth capped at 4 (most queries are shallow). P3 introduces Apache AGE
-- for arbitrary-depth Cypher; this stays as a fast fallback.
-- ---------------------------------------------------------------------------
CREATE TABLE relation_closure (
    namespace    TEXT    NOT NULL,
    type         TEXT    NOT NULL,
    from_id      UUID    NOT NULL,
    to_id        UUID    NOT NULL,
    depth        INTEGER NOT NULL CHECK (depth BETWEEN 1 AND 4),
    PRIMARY KEY (namespace, type, from_id, to_id, depth)
);
CREATE INDEX relation_closure_lookup
    ON relation_closure (namespace, type, from_id, depth);

-- ---------------------------------------------------------------------------
-- facts: versioned attribute observations with provenance.
-- Multiple facts for the same (entity_id, key) coexist; the effective one is
-- the newest non-expired with highest confidence.
-- ---------------------------------------------------------------------------
CREATE TABLE facts (
    id             UUID         PRIMARY KEY,
    namespace      TEXT         NOT NULL,
    entity_id      UUID         NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    key            TEXT         NOT NULL,
    value          JSONB        NOT NULL,
    source         JSONB        NOT NULL,
    confidence     REAL         NOT NULL,
    observed_at    TIMESTAMPTZ  NOT NULL,
    ttl_seconds    INTEGER,
    superseded_by  UUID         REFERENCES facts(id) ON DELETE SET NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX facts_effective
    ON facts (namespace, entity_id, key, observed_at DESC)
    WHERE superseded_by IS NULL;
CREATE INDEX facts_stale_check
    ON facts (namespace, observed_at)
    WHERE superseded_by IS NULL;
CREATE INDEX facts_by_source_identity
    ON facts ((source->>'identity'));

-- ---------------------------------------------------------------------------
-- changes: append-only event log. The source of truth for "what happened".
-- Partitioned by month at P5; for P0 a flat table is fine.
-- ---------------------------------------------------------------------------
CREATE TABLE changes (
    id           UUID         PRIMARY KEY,
    ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    namespace    TEXT         NOT NULL,
    actor        TEXT         NOT NULL,
    op           TEXT         NOT NULL,
    target_type  TEXT         NOT NULL,
    target_id    UUID,
    before       JSONB,
    after        JSONB,
    reason       TEXT
);
CREATE INDEX changes_ts
    ON changes (ts DESC);
CREATE INDEX changes_namespace
    ON changes (namespace, ts DESC);
CREATE INDEX changes_target
    ON changes (target_type, target_id, ts DESC);

-- ---------------------------------------------------------------------------
-- metamodel: schema-as-data
-- ---------------------------------------------------------------------------
CREATE TABLE entity_types (
    namespace         TEXT    NOT NULL,
    name              TEXT    NOT NULL,
    description       TEXT,
    attrs_schema      JSONB   NOT NULL DEFAULT '{}'::jsonb,
    allowed_relations TEXT[]  NOT NULL DEFAULT '{}',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, name)
);

CREATE TABLE relation_types (
    namespace      TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    from_types     TEXT[]  NOT NULL DEFAULT '{}',
    to_types       TEXT[]  NOT NULL DEFAULT '{}',
    props_schema   JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, name)
);

-- ---------------------------------------------------------------------------
-- Seed: bootstrap entity types drawn from ana envelopes (P1 collector will
-- populate entities of these types).
-- ---------------------------------------------------------------------------
INSERT INTO entity_types (namespace, name, description, attrs_schema) VALUES
    ('cc.fleet', 'fleet.agent',
     'An agent on the ana bus. Source: discovery envelope.',
     '{"type":"object","properties":{"role":{"type":"string"},"cluster":{"type":"string"},"subjects_owned":{"type":"array","items":{"type":"string"}},"capabilities":{"type":"object"}}}'::jsonb),
    ('cc.fleet', 'fleet.host',
     'A physical or virtual host. Source: pulse.host + ssh-facts collector.',
     '{"type":"object","properties":{"os":{"type":"string"},"kernel":{"type":"string"},"cpus":{"type":"integer"},"mem_gb":{"type":"integer"}}}'::jsonb),
    ('cc.fleet', 'fleet.cluster',
     'A cluster of agents. Source: discovery.cluster.',
     '{"type":"object","properties":{"name":{"type":"string"},"nats_prefix":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'infra.vm',
     'A virtual machine.',
     '{"type":"object","properties":{"provider":{"type":"string"},"instance_type":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'infra.container',
     'A running container.',
     '{"type":"object","properties":{"image":{"type":"string"},"runtime":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'infra.pod',
     'A Kubernetes pod.',
     '{"type":"object","properties":{"node":{"type":"string"},"phase":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'infra.service',
     'A network service.',
     '{"type":"object","properties":{"port":{"type":"integer"},"protocol":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'app.service',
     'An application service in the catalog.',
     '{"type":"object","properties":{"owner":{"type":"string"},"slo":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'secret.ref',
     'Reference to a secret (path + rotation metadata, never the value).',
     '{"type":"object","properties":{"backend":{"type":"string"},"path":{"type":"string"},"rotation_days":{"type":"integer"}}}'::jsonb),
    ('cc.fleet', 'kb.runbook',
     'A runbook URL associated with one or more entities.',
     '{"type":"object","properties":{"url":{"type":"string"},"title":{"type":"string"}}}'::jsonb)
ON CONFLICT (namespace, name) DO NOTHING;

INSERT INTO relation_types (namespace, name, from_types, to_types) VALUES
    ('cc.fleet', 'runs_on',
     ARRAY['fleet.agent','infra.vm','infra.container','infra.pod'],
     ARRAY['fleet.host','infra.vm','infra.pod']),
    ('cc.fleet', 'in_cluster',
     ARRAY['fleet.agent','infra.pod','infra.service'],
     ARRAY['fleet.cluster']),
    ('cc.fleet', 'depends_on',
     ARRAY['app.service','infra.service'],
     ARRAY['app.service','infra.service']),
    ('cc.fleet', 'owns_subject',
     ARRAY['fleet.agent'],
     ARRAY['fleet.agent']),
    ('cc.fleet', 'referenced_by',
     ARRAY['secret.ref','kb.runbook'],
     ARRAY['app.service','infra.service','fleet.agent'])
ON CONFLICT (namespace, name) DO NOTHING;
