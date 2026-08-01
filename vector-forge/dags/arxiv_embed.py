from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task
from sqlalchemy import create_engine, text

from vectorforge.chunking import chunk_text

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBED_DIM = 384


def _engine():
    u = os.environ["VF_WAREHOUSE_USER"]; p = os.environ["VF_WAREHOUSE_PASSWORD"]
    h = os.environ["VF_WAREHOUSE_HOST"]; port = os.environ["VF_WAREHOUSE_PORT"]
    d = os.environ["VF_WAREHOUSE_DB"]
    return create_engine(f"postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}")


@dag(
    dag_id="arxiv_embed",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vectorforge", "phase-3"],
)
def arxiv_embed():

    @task
    def ensure_table() -> None:
        with _engine().begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS curated.chunks (
                    id          BIGSERIAL PRIMARY KEY,
                    arxiv_id    TEXT NOT NULL,
                    chunk_index INT  NOT NULL,
                    content     TEXT NOT NULL,
                    embedding   vector({EMBED_DIM}) NOT NULL,
                    created_at  TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (arxiv_id, chunk_index)
                );
            """))

    @task
    def embed_and_load() -> str:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(MODEL_NAME)
        engine = _engine()

        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT s.arxiv_id, s.abstract
                FROM staging.arxiv_papers s
                LEFT JOIN curated.chunks c ON c.arxiv_id = s.arxiv_id
                WHERE c.arxiv_id IS NULL
            """)).fetchall()

        if not rows:
            return "nothing new to embed"

        payload = []
        for arxiv_id, abstract in rows:
            for i, chunk in enumerate(chunk_text(abstract)):
                vec = model.encode(chunk, normalize_embeddings=True).tolist()
                payload.append({"arxiv_id": arxiv_id, "idx": i,
                                "content": chunk, "embedding": str(vec)})

        with engine.begin() as conn:
            for r in payload:
                conn.execute(text("""
                    INSERT INTO curated.chunks
                        (arxiv_id, chunk_index, content, embedding)
                    VALUES (:arxiv_id, :idx, :content, :embedding)
                    ON CONFLICT (arxiv_id, chunk_index) DO NOTHING
                """), r)

        return f"embedded and loaded {len(payload)} chunks from {len(rows)} papers"

    @task
    def build_index() -> None:
        with _engine().begin() as conn:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS chunks_embedding_idx
                ON curated.chunks
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """))

    ensure_table() >> embed_and_load() >> build_index()


arxiv_embed()