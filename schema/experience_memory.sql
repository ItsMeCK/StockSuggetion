-- Experience Memory Schema for Failed Trade Autopsies
-- Requires pgvector extension for high‑dimensional embeddings

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS experience_memory (
    experience_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker VARCHAR(20) NOT NULL,
    pattern VARCHAR(50) NOT NULL,
    macro_regime VARCHAR(50) NOT NULL,
    failure_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    embedding VECTOR(1536), -- Claude Vision embedding size placeholder
    notes TEXT,
    metadata JSONB
);

-- Index for fast similarity search
CREATE INDEX IF NOT EXISTS idx_experience_embedding ON experience_memory USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
