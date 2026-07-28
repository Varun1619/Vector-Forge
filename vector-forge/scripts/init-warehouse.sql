-- Turn on pgvector so we can store embeddings as a native column type.
CREATE EXTENSION IF NOT EXISTS vector;

-- Medallion-style schemas: cleaned staging, then serving-ready curated.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;