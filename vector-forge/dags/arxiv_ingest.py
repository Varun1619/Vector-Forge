from __future__ import annotations

import json
import os
from datetime import datetime

import feedparser
import requests
from airflow.decorators import dag, task

from vectorforge.storage import s3_client

ARXIV_API = "http://export.arxiv.org/api/query"
CATEGORY = "cs.AI"
MAX_RESULTS = 50


@dag(
    dag_id="arxiv_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["vectorforge", "phase-1"],
)
def arxiv_ingest():

    @task
    def fetch_papers() -> list[dict]:
        params = {
            "search_query": f"cat:{CATEGORY}",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": MAX_RESULTS,
        }
        resp = requests.get(ARXIV_API, params=params, timeout=30)
        resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        papers = []
        for e in feed.entries:
            papers.append({
                "arxiv_id": e.id.split("/abs/")[-1],
                "title": " ".join(e.title.split()),
                "abstract": " ".join(e.summary.split()),
                "authors": [a.name for a in e.authors],
                "published": e.published,
                "categories": [t.term for t in e.tags],
                "link": e.link,
            })
        if not papers:
            raise ValueError("arXiv returned zero papers — refusing to write empty raw file")
        return papers

    @task
    def land_raw(papers: list[dict]) -> str:
        s3 = s3_client()
        bucket = os.environ["VF_MINIO_BUCKET"]
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"arxiv/{CATEGORY}/{run_date}.json"
        body = json.dumps(papers, ensure_ascii=False, indent=2).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=body,
                      ContentType="application/json")
        return f"wrote {len(papers)} papers to s3://{bucket}/{key}"

    land_raw(fetch_papers())


arxiv_ingest()