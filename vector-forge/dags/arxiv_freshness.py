from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from sqlalchemy import create_engine, text

MAX_STALE_DAYS = 3


def _engine():
    u = os.environ["VF_WAREHOUSE_USER"]; p = os.environ["VF_WAREHOUSE_PASSWORD"]
    h = os.environ["VF_WAREHOUSE_HOST"]; port = os.environ["VF_WAREHOUSE_PORT"]
    d = os.environ["VF_WAREHOUSE_DB"]
    return create_engine(f"postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}")


@dag(
    dag_id="arxiv_freshness",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vectorforge", "phase-4"],
)
def arxiv_freshness():

    @task
    def ensure_metrics_table() -> None:
        with _engine().begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS curated.pipeline_runs (
                    run_at        TIMESTAMPTZ DEFAULT now(),
                    staged_rows   INT,
                    chunk_rows    INT,
                    newest_paper  TEXT,
                    stale_days    INT,
                    status        TEXT
                );
            """))

    @task
    def check_and_record() -> str:
        with _engine().begin() as conn:
            staged = conn.execute(
                text("SELECT count(*) FROM staging.arxiv_papers")).scalar()
            chunks = conn.execute(
                text("SELECT count(*) FROM curated.chunks")).scalar()
            newest = conn.execute(
                text("SELECT max(published) FROM staging.arxiv_papers")).scalar()

            stale_days = None
            status = "ok"
            if newest:
                newest_dt = datetime.fromisoformat(newest.replace("Z", "+00:00"))
                stale_days = (datetime.now(newest_dt.tzinfo) - newest_dt).days
                if stale_days > MAX_STALE_DAYS:
                    status = "stale"

            conn.execute(text("""
                INSERT INTO curated.pipeline_runs
                    (staged_rows, chunk_rows, newest_paper, stale_days, status)
                VALUES (:s, :c, :n, :d, :st)
            """), {"s": staged, "c": chunks, "n": newest,
                   "d": stale_days, "st": status})

        if status == "stale":
            raise AirflowFailException(
                f"Data is stale: newest paper is {stale_days} days old "
                f"(threshold {MAX_STALE_DAYS})."
            )
        return f"fresh — {staged} staged, {chunks} chunks, newest {stale_days}d old"

    ensure_metrics_table() >> check_and_record()


arxiv_freshness()