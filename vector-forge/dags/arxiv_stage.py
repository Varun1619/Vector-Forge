from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine, text

from vectorforge.storage import s3_client
from vectorforge.schemas import arxiv_schema

CATEGORY = "cs.AI"


def _warehouse_engine():
    user = os.environ["VF_WAREHOUSE_USER"]
    pw = os.environ["VF_WAREHOUSE_PASSWORD"]
    host = os.environ["VF_WAREHOUSE_HOST"]
    port = os.environ["VF_WAREHOUSE_PORT"]
    db = os.environ["VF_WAREHOUSE_DB"]
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")


@dag(
    dag_id="arxiv_stage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vectorforge", "phase-2"],
)
def arxiv_stage():

    @task
    def read_latest_raw() -> list[dict]:
        s3 = s3_client()
        bucket = os.environ["VF_MINIO_BUCKET"]
        prefix = f"arxiv/{CATEGORY}/"
        listing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = sorted(obj["Key"] for obj in listing.get("Contents", []))
        if not keys:
            raise FileNotFoundError(f"No raw files under s3://{bucket}/{prefix}")
        latest = keys[-1]
        obj = s3.get_object(Bucket=bucket, Key=latest)
        return json.loads(obj["Body"].read())

    @task
    def validate_and_stage(papers: list[dict]) -> str:
        df = pd.DataFrame(papers)
        before = len(df)
        df = df.drop_duplicates(subset="arxiv_id").reset_index(drop=True)

        # QUALITY GATE — raises SchemaError (fails the task) on any violation.
        validated = arxiv_schema.validate(df, lazy=True)

        keep = ["arxiv_id", "title", "abstract", "published", "link"]
        out = validated[keep]

        engine = _warehouse_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS staging.arxiv_papers (
                    arxiv_id  TEXT PRIMARY KEY,
                    title     TEXT NOT NULL,
                    abstract  TEXT NOT NULL,
                    published TEXT NOT NULL,
                    link      TEXT NOT NULL,
                    loaded_at TIMESTAMPTZ DEFAULT now()
                );
            """))
            for row in out.to_dict(orient="records"):
                conn.execute(text("""
                    INSERT INTO staging.arxiv_papers
                        (arxiv_id, title, abstract, published, link)
                    VALUES (:arxiv_id, :title, :abstract, :published, :link)
                    ON CONFLICT (arxiv_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        abstract = EXCLUDED.abstract,
                        published = EXCLUDED.published,
                        link = EXCLUDED.link,
                        loaded_at = now();
                """), row)

        return f"validated {len(out)}/{before} rows and upserted to staging"

    validate_and_stage(read_latest_raw())


arxiv_stage()