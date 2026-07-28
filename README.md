# VectorForge

Production-grade data pipeline that ingests arXiv papers, enforces data-quality
gates, and embeds them into pgvector for retrieval-augmented generation (RAG).
Built with production rigor: quality gates, freshness checks, and observability.

> **Status: Phase 0 complete** — the platform boots, all services are wired, and
> a smoke-test DAG verifies connectivity end to end.

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
                 │
                 ▼
         validate ─▶ staging ─▶ chunk + embed ─▶ pgvector (curated)
                                                      │
                                                      ▼
                                            freshness + observability
```

Airflow's own metadata database is kept separate from the analytical warehouse.
Data is organized into `staging` (cleaned, validated) and `curated`
(serving-ready: chunks + embeddings) schemas.

## Quickstart

```bash
cp .env.example .env
docker compose up -d
```

- **Airflow UI** — http://localhost:8080 (admin / admin)
- **MinIO console** — http://localhost:9001

Trigger the `platform_smoke_test` DAG to confirm the warehouse (with pgvector)
and MinIO are reachable.

To stop: `docker compose down`. To wipe all data: `docker compose down -v`.

## Project layout

```
vector-forge/
├── docker-compose.yml        # the local platform
├── .env.example              # config template (copy to .env)
├── scripts/
│   └── init-warehouse.sql    # enables pgvector; creates schemas
├── dags/
│   └── platform_smoke_test.py
├── include/vectorforge/      # shared pipeline helpers
└── docs/                     # build guide
```

## Design principles

- **Fail fast** on bad or stale data rather than corrupt downstream tables.
- **Immutable raw** — the warehouse can always be rebuilt from source.
- **Idempotent** at every stage — safe to re-run.
- **Reproducible** — the whole platform stands up from one command.

## Pipeline

*(Grows with each phase.)*

- Platform + connectivity smoke test.

## Status

- [x] **Phase 0** — Platform boots; smoke test passes.
- [ ] Phase 1 — Ingestion DAG lands raw arXiv data in MinIO.
- [ ] Phase 2 — Quality gates + validated staging load.
- [ ] Phase 3 — Chunking, local embeddings, and vector search.
- [ ] Phase 4 — Freshness SLA + run-level observability.
- [ ] Phase 5 — Tests, CI, and documentation.

## License

MIT