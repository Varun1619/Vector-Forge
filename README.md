# VectorForge

Production-grade data pipeline that ingests arXiv papers, enforces data-quality
gates, and embeds them into pgvector for retrieval-augmented generation (RAG).
Built with production rigor: quality gates, freshness checks, and observability.

> **Status: Phase 3 complete** — the pipeline is AI-ready. Validated papers are
> chunked, embedded locally, and loaded into pgvector; semantic similarity search
> runs as a plain SQL query.

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
                                  freshness + observability
```

Airflow's metadata database is kept separate from the analytical warehouse.
Data is organized into `staging` (cleaned, validated) and `curated`
(serving-ready: chunks + embeddings) schemas.

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build
```

- **Airflow UI** — http://localhost:8080 (admin / admin)
- **MinIO console** — http://localhost:9001

Run the DAGs in order: `platform_smoke_test` → `arxiv_ingest` → `arxiv_stage`
→ `arxiv_embed`.

## Pipeline

1. **Ingest** (`arxiv_ingest`) — pulls the newest cs.AI papers from the arXiv API
   and lands them as immutable, date-partitioned JSON in MinIO. Idempotent.
2. **Validate + stage** (`arxiv_stage`) — validates every record against a
   pandera schema and upserts clean rows into `staging.arxiv_papers`. Validation
   failures **fail the run**; load is an idempotent `ON CONFLICT` upsert inside a
   transaction.
3. **Chunk + embed** (`arxiv_embed`) — chunks abstracts (word-boundary windows
   with overlap), embeds them locally with all-MiniLM-L6-v2 (384-dim), and loads
   vectors into `curated.chunks` with an IVFFlat cosine index. **Incremental**:
   an anti-join processes only papers not yet embedded, so daily runs do minimal
   work.

## Retrieval

Semantic search runs as a plain SQL query using pgvector's `<=>` cosine-distance
operator — the serving layer for the companion RAG project:

```sql
SELECT arxiv_id, content
FROM curated.chunks
ORDER BY embedding <=> :query_vector
LIMIT 5;
```

Given a seed chunk, the store returns topically-related papers (compositional
generalization, long-horizon planning, pretraining) — meaning-based matching,
not keyword matching.

## Data quality

Every record passes an explicit schema before entering the warehouse. The
pipeline **fails loudly on bad data** rather than corrupting downstream tables.
Breaking a record (e.g. blanking an abstract) turns the run red with a precise
pandera report naming the offending column and rule.

## Design principles

- **Fail fast** on bad or missing data rather than corrupt downstream tables.
- **Immutable raw** — the warehouse can always be rebuilt from source.
- **Idempotent** at every stage — safe to re-run.
- **Incremental** — never redoes work already done.
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
│   └── arxiv_embed.py
└── include/vectorforge/
    ├── __init__.py
    ├── storage.py            # shared S3/MinIO client
    ├── schemas.py            # pandera validation schemas
    └── chunking.py           # word-boundary chunker with overlap
```

## Troubleshooting notes

Real issues encountered while building this platform, and the fixes — kept here
because the fixes are non-obvious and worth documenting.

### Airflow can't write logs — `PermissionError: /opt/airflow/logs`

Host-mounted log folder is owned by the host user; Airflow's container user (UID
50000) is locked out. **Fix:** add `AIRFLOW_UID=50000` to `.env` and
`user: "${AIRFLOW_UID:-50000}:0"` to the shared Airflow config.

### MinIO bucket fails to create — `Access Denied`

`$${VAR}` in the init command expanded to empty inside the container, and the
image's `MINIO_ROOT_USER_FILE` / `MINIO_ROOT_PASSWORD_FILE` defaults overrode the
plain credentials. **Fix:** single-`$` substitution in the init command and set
the `_FILE` variables to empty in the `minio` service.

### DAG doesn't appear — `ModuleNotFoundError: No module named 'vectorforge'`

The `include/` folder was mounted but not on Python's import path. **Fix:** set
`PYTHONPATH: /opt/airflow/include`. Diagnose with `airflow dags list-import-errors`.

### Scheduler crashes after adding pandera — SQLAlchemy `Mapped[]` ArgumentError

`pandera` transitively upgraded SQLAlchemy to 2.0.x, incompatible with Airflow
2.10's ORM — the scheduler crashed on startup. **Fix:** pin `sqlalchemy==1.4.54`
so pandera can't upgrade it. General rule: don't override versions Airflow
manages, and verify edits actually saved before rebuilding with `--no-cache`.

## Status

- [x] **Phase 0** — Platform boots; smoke test passes.
- [x] **Phase 1** — Ingestion DAG lands raw arXiv data in MinIO.
- [x] **Phase 2** — Quality gates + validated staging load.
- [x] **Phase 3** — Chunking, local embeddings, and vector search.
- [ ] Phase 4 — Freshness SLA + run-level observability.
- [ ] Phase 5 — Tests, CI, and documentation.

## License

MIT