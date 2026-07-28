# VectorForge

Production-grade data pipeline that ingests arXiv papers, enforces data-quality
gates, and embeds them into pgvector for retrieval-augmented generation (RAG).
Built with production rigor: quality gates, freshness checks, and observability.

> **Status: Phase 2 complete** — raw arXiv data is validated against a schema and
> loaded into the warehouse. Records that fail validation fail the run rather than
> silently corrupting downstream tables.

## Stack

| Component | Role |
|-----------|------|
| **Apache Airflow** (LocalExecutor) | Orchestration — schedules, retries, dependency ordering |
| **Postgres + pgvector** | Analytical warehouse and vector store |
| **MinIO** (S3-compatible) | Immutable raw landing zone |
| **pandera** | Schema-based data-quality validation |
| **Docker Compose** | One-command reproducible local platform |

## Architecture

```
arXiv API ──▶ Airflow ──▶ MinIO (raw, immutable)
                 │           arxiv/cs.AI/<date>.json
                 ▼
         pandera quality gates ──▶ staging.arxiv_papers
                 │                  (validated, deduped, upserted)
                 │ (invalid → run fails)
                 ▼
         chunk + embed ─▶ pgvector (curated)
                              │
                              ▼
                    freshness + observability
```

Airflow's metadata database is kept separate from the analytical warehouse.
Data is organized into `staging` (cleaned, validated) and `curated`
(serving-ready) schemas.

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build
```

- **Airflow UI** — http://localhost:8080 (admin / admin)
- **MinIO console** — http://localhost:9001

Run the DAGs in order: `platform_smoke_test` → `arxiv_ingest` → `arxiv_stage`.

## Pipeline

1. **Ingest** (`arxiv_ingest`) — pulls the newest cs.AI papers from the arXiv API
   and lands them as immutable, date-partitioned JSON in MinIO. Idempotent:
   re-running a given day overwrites rather than duplicates. Refuses to write an
   empty file if the API returns nothing.
2. **Validate + stage** (`arxiv_stage`) — reads the latest raw file, validates
   every record against a pandera schema (unique IDs, non-empty abstracts,
   well-formed links), and upserts clean rows into `staging.arxiv_papers`.
   Validation failures **fail the run**; the load is an idempotent
   `ON CONFLICT` upsert inside a transaction.

## Data quality

Every record passes an explicit schema before entering the warehouse. The
pipeline is designed to **fail loudly on bad data** rather than corrupt
downstream tables silently. Deliberately breaking a record (e.g. blanking an
abstract) turns the run red with a precise pandera report naming the offending
column and rule.

## Design principles

- **Fail fast** on bad or missing data rather than corrupt downstream tables.
- **Immutable raw** — the warehouse can always be rebuilt from source.
- **Idempotent** at every stage — safe to re-run.
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
│   └── arxiv_stage.py
└── include/vectorforge/
    ├── __init__.py
    ├── storage.py            # shared S3/MinIO client
    └── schemas.py            # pandera validation schemas
```

## Troubleshooting notes

Real issues encountered while building this platform, and the fixes — kept here
because the fixes are non-obvious and worth documenting.

### Airflow can't write logs — `PermissionError: /opt/airflow/logs`

Airflow services failed their healthchecks creating dated log subfolders. When a
host folder is mounted in, host ownership rules apply and Airflow's container
user (UID 50000) is locked out.

**Fix:** add `AIRFLOW_UID=50000` to `.env` and `user: "${AIRFLOW_UID:-50000}:0"`
to the shared Airflow config; recreate the `logs/` folder.

### MinIO bucket fails to create — `Access Denied`

The `minio-init` container added the alias but was denied bucket creation. Two
causes: credentials passed as `$${VAR}` expanded to empty inside the container,
and the image's `MINIO_ROOT_USER_FILE` / `MINIO_ROOT_PASSWORD_FILE` defaults
overrode the plain credentials.

**Fix:** use single-`$` substitution in the init command and set the `_FILE`
variables to empty in the `minio` service.

### DAG doesn't appear — `ModuleNotFoundError: No module named 'vectorforge'`

The first DAG importing the shared `vectorforge` package failed to load; the
`include/` folder was mounted but not on Python's import path.

**Fix:** set `PYTHONPATH: /opt/airflow/include`. Diagnose loading failures with
`airflow dags list-import-errors`.

### Scheduler crashes after adding pandera — SQLAlchemy `Mapped[]` ArgumentError

Adding `pandera` transitively upgraded SQLAlchemy to 2.0.x, which is
incompatible with Airflow 2.10's ORM models — the scheduler crashed on startup
with `Type annotation for "TaskInstance.dag_model" can't be correctly
interpreted`. The dependency was pulled in silently, not pinned directly.

**Fix:** pin `sqlalchemy==1.4.54` in `requirements.txt` to hold Airflow's
required version so pandera can't upgrade it. General rule: don't let libraries
override versions Airflow manages. When a rebuild seems to "not take", verify the
edited file actually saved and rebuild with `docker compose build --no-cache`.

## Status

- [x] **Phase 0** — Platform boots; smoke test passes.
- [x] **Phase 1** — Ingestion DAG lands raw arXiv data in MinIO.
- [x] **Phase 2** — Quality gates + validated staging load.
- [ ] Phase 3 — Chunking, local embeddings, and vector search.
- [ ] Phase 4 — Freshness SLA + run-level observability.
- [ ] Phase 5 — Tests, CI, and documentation.

## License

MIT