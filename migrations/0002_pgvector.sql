-- pgvector semantic search.
-- Default vector dim is 768 (matches nomic-embed-text). For other embedders
-- (bge-m3=1024, OpenAI text-embedding-3-small=1536, etc.) drop and recreate
-- this table with the right dim, or just truncate + re-embed.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS entity_embeddings (
    entity_id   UUID         NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    embedding   VECTOR(768)  NOT NULL,
    model       TEXT         NOT NULL,
    embedded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id)
);

-- ivfflat for cosine similarity search. For >1M rows bump `lists` to ~sqrt(N).
CREATE INDEX IF NOT EXISTS entity_embeddings_embedding_idx
    ON entity_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS entity_embeddings_model
    ON entity_embeddings (model);
