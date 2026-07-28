from __future__ import annotations

import os
import urllib.request
from datetime import datetime

import psycopg2
from airflow.decorators import dag, task


@dag(
    dag_id="platform_smoke_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vectorforge", "phase-0"],
)
def platform_smoke_test():

    @task
    def check_warehouse() -> str:
        conn = psycopg2.connect(
            host=os.environ["VF_WAREHOUSE_HOST"],
            port=os.environ["VF_WAREHOUSE_PORT"],
            dbname=os.environ["VF_WAREHOUSE_DB"],
            user=os.environ["VF_WAREHOUSE_USER"],
            password=os.environ["VF_WAREHOUSE_PASSWORD"],
        )
        cur = conn.cursor()
        cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector';")
        found = cur.fetchone()
        cur.close()
        conn.close()
        assert found is not None, "pgvector extension is missing from the warehouse"
        return "warehouse OK — pgvector present"

    @task
    def check_minio() -> str:
        url = f"http://{os.environ['VF_MINIO_ENDPOINT']}/minio/health/live"
        with urllib.request.urlopen(url, timeout=5) as resp:
            assert resp.status == 200, f"MinIO health check returned {resp.status}"
        return "minio OK — reachable and live"

    check_warehouse()
    check_minio()


platform_smoke_test()