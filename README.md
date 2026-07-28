# VectorForge

Production-grade data pipeline that ingests arXiv papers, enforces data-quality
gates, and embeds them into pgvector for retrieval-augmented generation (RAG).
Built with production rigor: quality gates, freshness checks, and observability.

> **Status: Phase 1 complete** — the pipeline ingests live arXiv data and lands
> it as immutable, date-partitioned JSON in object storage.

## Stack

| Component | Role |
|-----------|------|
| **Apache Airflow** (LocalExecutor) | Orchestration — schedules, retries, dependency ordering |
| **Postgres + pgvector** | Analytical warehouse and vector store |
| **MinIO** (S3-compatible) | Immutable raw landing zone |
| **Docker Compose** | One-command reproducible local platform |

## Architecture

```
arXiv API ──▶ Airflow ──▶ MinIO (raw, immutable)
                 │           arxiv/cs.AI/<date>.json
                 ▼
         validate ─▶ staging ─▶ chunk + embed ─▶ pgvector (curated)
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

Run `platform_smoke_test` to verify connectivity, then `arxiv_ingest` to pull the
latest papers into the raw bucket.

## Pipeline

1. **Ingest** (`arxiv_ingest`) — pulls the newest cs.AI papers from the arXiv API
   and lands them as immutable, date-partitioned JSON in MinIO. Idempotent:
   re-running a given day overwrites rather than duplicates. Refuses to write an
   empty file if the API returns nothing.

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
├── requirements.txt          # ingestion deps (baked into custom image)
├── docker/airflow.Dockerfile # custom Airflow image
├── scripts/init-warehouse.sql
├── dags/
│   ├── platform_smoke_test.py
│   └── arxiv_ingest.py
└── include/vectorforge/
    ├── __init__.py
    └── storage.py            # shared S3/MinIO client
```

## Troubleshooting notes

Real issues encountered while building this platform, and the fixes — kept here
because the fixes are non-obvious and worth documenting.

### Airflow can't write logs — `PermissionError: /opt/airflow/logs`

Every Airflow service failed its healthcheck on startup with a permission error
creating dated log subfolders. When a host folder is mounted into the container,
host ownership rules apply and Airflow's container user (UID 50000) is locked
out.

**Fix:** pin the container user by adding `AIRFLOW_UID=50000` to `.env` and
`user: "${AIRFLOW_UID:-50000}:0"` to the shared Airflow service config, then
recreate the `logs/` folder.

### MinIO bucket fails to create — `Access Denied`

The `minio-init` container reported the alias was added but bucket creation was
denied. Two overlapping causes:
1. The init command passed credentials as `$${VAR}` (escaped), so Compose passed
   a literal `${VAR}` into a container that had no such variable — expanding to
   empty. Switching to `${VAR}` makes Compose substitute the real value.
2. The MinIO image carried `MINIO_ROOT_USER_FILE` / `MINIO_ROOT_PASSWORD_FILE`
   defaults that override the plain credentials.

**Fix:** use single-`$` substitution in the init command and neutralize the
`_FILE` variables in the `minio` service (`MINIO_ROOT_USER_FILE: ""`).

### DAG doesn't appear — `ModuleNotFoundError: No module named 'vectorforge'`

The first DAG that imported the shared `vectorforge` package failed to load
silently (the smoke test worked because it only used the standard library). The
`include/` folder was mounted but not on Python's import path.

**Fix:** set `PYTHONPATH: /opt/airflow/include` in the Airflow service config so
DAGs can import the shared package. Diagnose loading failures with
`airflow dags list-import-errors`.

## Status

- [x] **Phase 0** — Platform boots; smoke test passes.
- [x] **Phase 1** — Ingestion DAG lands raw arXiv data in MinIO.
- [ ] Phase 2 — Quality gates + validated staging load.
- [ ] Phase 3 — Chunking, local embeddings, and vector search.
- [ ] Phase 4 — Freshness SLA + run-level observability.
- [ ] Phase 5 — Tests, CI, and documentation.

## License

MIT