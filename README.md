# VectorForge

Production-grade data pipeline that ingests arXiv papers, enforces data-quality
gates, and embeds them into pgvector for retrieval-augmented generation (RAG).
Built with production rigor: quality gates, freshness checks, and observability.

> **Status: Phase 4 complete** — the pipeline is functionally complete:
> ingest → validate → embed → serve → monitor, with a freshness SLA and a
> queryable run-health history.

## Stack

| Component | Role |
|-----------|------|
| **Apache Airflow** (LocalExecutor) | Orchestration — schedules, retries, dependency ordering |
| **Postgres + pgvector** | Analytical warehouse and vector store |
| **MinIO** (S3-compatible) | Immutable raw landing zone |
| **pandera** | Schema-based data-quality validation |
| **sentence-transformers** | Local embeddings (all-MiniLM-L6-v2, 384-dim) |
| **Docker Compose** | One-command reproducible local platform |

## Architecture

```
arXiv API ──▶ Airflow ──▶ MinIO (raw, immutable)
                 │           arxiv/cs.AI/<date>.json
                 ▼
         pandera quality gates ──▶ staging.arxiv_papers
                 │ (invalid → run fails)  (validated, deduped)
                 ▼
         chunk + embed (local) ──▶ curated.chunks
                                    vector(384) + IVFFlat cosine index
                                            │
                                            ▼
                                  semantic search (pgvector <=>)
                                            │
                                            ▼
                     freshness SLA + curated.pipeline_runs
                     (stale → run fails; metrics logged every run)
```

Airflow's metadata database is kept separate from the analytical warehouse.
Data is organized into `staging` (cleaned, validated) and `curated`
(serving-ready: chunks, embeddings, run metrics) schemas.

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build
```

- **Airflow UI** — http://localhost:8080 (admin / admin)
- **MinIO console** — http://localhost:9001

Run the DAGs in order: `arxiv_ingest` → `arxiv_stage` → `arxiv_embed`
→ `arxiv_freshness`.

## Pipeline

1. **Ingest** (`arxiv_ingest`) — pulls the newest cs.AI papers from the arXiv API
   and lands them as immutable, date-partitioned JSON in MinIO. Idempotent.
2. **Validate + stage** (`arxiv_stage`) — validates every record against a
   pandera schema and upserts clean rows into `staging.arxiv_papers`. Validation
   failures **fail the run**; load is an idempotent `ON CONFLICT` upsert inside a
   transaction.
3. **Chunk + embed** (`arxiv_embed`) — chunks abstracts, embeds them locally with
   all-MiniLM-L6-v2 (384-dim), and loads vectors into `curated.chunks` with an
   IVFFlat cosine index. **Incremental**: only unembedded papers are processed.
4. **Freshness + observability** (`arxiv_freshness`) — enforces a freshness SLA
   (**fails the run** if the newest paper is older than 3 days) and records
   counts, staleness, and status to `curated.pipeline_runs` on every run — pass
   or fail.

## Retrieval

Semantic search runs as a plain SQL query using pgvector's `<=>` cosine-distance
operator — the serving layer for the companion RAG project:

```sql
SELECT arxiv_id, content
FROM curated.chunks
ORDER BY embedding <=> :query_vector
LIMIT 5;
```

## Data quality & trust

The pipeline is built to **fail loudly** rather than corrupt or mislead:

- **Bad data** — records failing the pandera schema fail the run and never enter
  the warehouse.
- **Stale data** — a run can succeed at moving data yet still serve stale content
  if the source stops updating. The freshness check treats "data exists" and
  "data is fresh" as separate questions and fails when the newest record breaches
  the SLA.
- **Health history** — `curated.pipeline_runs` records every run's row counts,
  data age, and status, so pipeline health is queryable rather than buried in
  logs.

## Design principles

- **Fail fast** on bad or stale data rather than corrupt/mislead downstream.
- **Immutable raw** — the warehouse can always be rebuilt from source.
- **Idempotent** at every stage — safe to re-run.
- **Incremental** — never redoes work already done.
- **Observable** — run health is recorded and queryable.
- **Reproducible** — the whole platform stands up from one command.

## Project layout

```
vector-forge/
├── docker-compose.yml
├── .env.example
├── requirements.txt          # deps baked into the custom image
├── docker/airflow.Dockerfile
├── scripts/init-warehouse.sql
├── dags/
│   ├── platform_smoke_test.py
│   ├── arxiv_ingest.py
│   ├── arxiv_stage.py
│   ├── arxiv_embed.py
│   └── arxiv_freshness.py
└── include/vectorforge/
    ├── __init__.py
    ├── storage.py            # shared S3/MinIO client
    ├── schemas.py            # pandera validation schemas
    └── chunking.py           # word-boundary chunker with overlap
```

## Troubleshooting notes

Real issues encountered while building this platform, and the fixes.

### Airflow can't write logs — `PermissionError: /opt/airflow/logs`

Host-mounted log folder locks out Airflow's container user (UID 50000).
**Fix:** `AIRFLOW_UID=50000` in `.env` and `user: "${AIRFLOW_UID:-50000}:0"`.

### MinIO bucket fails to create — `Access Denied`

`$${VAR}` expanded to empty in the init container, and image `_FILE` defaults
overrode the plain credentials. **Fix:** single-`$` substitution and set
`MINIO_ROOT_USER_FILE`/`MINIO_ROOT_PASSWORD_FILE` to empty.

### DAG doesn't appear — `ModuleNotFoundError: No module named 'vectorforge'`

`include/` was mounted but not on the import path. **Fix:** set
`PYTHONPATH: /opt/airflow/include`; diagnose with `airflow dags list-import-errors`.

### Scheduler crashes after adding pandera — SQLAlchemy `Mapped[]` ArgumentError

`pandera` transitively upgraded SQLAlchemy to 2.0.x, breaking Airflow 2.10's ORM.
**Fix:** pin `sqlalchemy==1.4.54` so pandera can't upgrade it. Don't override
versions Airflow manages; verify edits saved before `docker compose build --no-cache`.

### DAG runs an old version of itself — stale `__pycache__`

After a DAG file was deleted and re-added, Airflow executed a cached `.pyc` from
`dags/__pycache__/`, reproducing an old failure from a since-fixed version.
**Fix:** clear the cache (`rm -rf /opt/airflow/dags/__pycache__`) and
`airflow dags reserialize` when a DAG behaves like an outdated version of itself.

## Status

- [x] **Phase 0** — Platform boots; smoke test passes.
- [x] **Phase 1** — Ingestion DAG lands raw arXiv data in MinIO.
- [x] **Phase 2** — Quality gates + validated staging load.
- [x] **Phase 3** — Chunking, local embeddings, and vector search.
- [x] **Phase 4** — Freshness SLA + run-level observability.
- [ ] Phase 5 — Tests, CI, and documentation.

## License

MIT