-- API tokens for scoped agent access.
--
-- Each token has the form `cmdb_<id>.<secret>` (similar to GitHub PATs).
-- The id portion is a ULID stored as UUID; the secret is hashed via
-- SHA-256 and only the hash is persisted. Tokens carry:
--   - identity: becomes source.identity on writes
--   - namespace_scope: which namespaces the token can touch (empty = all)
--   - op_scope: 'read' | 'write' | 'admin' (empty = all)
--
-- IMPORTANT: reset search_path to public — migration 0003 (age) leaves it
-- on ag_catalog which would otherwise land these tables in the wrong schema.

SET search_path = public, pg_catalog;

CREATE TABLE api_tokens (
    id              UUID         PRIMARY KEY,
    secret_hash     TEXT         NOT NULL,
    identity        TEXT         NOT NULL,
    namespace_scope TEXT[]       NOT NULL DEFAULT '{}',
    op_scope        TEXT[]       NOT NULL DEFAULT '{}',
    description     TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ,
    last_used_at    TIMESTAMPTZ
);

CREATE INDEX api_tokens_identity ON api_tokens (identity) WHERE revoked_at IS NULL;
CREATE INDEX api_tokens_lookup ON api_tokens (id) WHERE revoked_at IS NULL;
