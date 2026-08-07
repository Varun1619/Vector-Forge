#!/usr/bin/env python3
"""
Builds the static search index behind the GitHub Pages demo
(docs/index.html + docs/assets/app.js).

This is intentionally standalone -- no Airflow, no Postgres, no MinIO.
It fetches the newest cs.AI papers from arXiv, embeds each abstract with
sentence-transformers/all-MiniLM-L6-v2 (the same weights the browser
loads client-side via transformers.js as Xenova/all-MiniLM-L6-v2), and
merges the result into docs/data/papers.json -- keeping only the most
recent MAX_PAPERS records so the index (and the page load) stay small.

Runs on a schedule via .github/workflows/build-search-index.yml, which
commits the refreshed docs/data/papers.json straight back to the repo.
GitHub Pages then republishes automatically.

Local run:
    pip install requests feedparser sentence-transformers
    python scripts/build_search_index.py
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests

ARXIV_API = "http://export.arxiv.org/api/query"
CATEGORY = "cs.AI"
FETCH_RESULTS = 100
MAX_PAPERS = 300
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BROWSER_MODEL_ID = "Xenova/all-MiniLM-L6-v2"

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "docs" / "data" / "papers.json"


def fetch_feed_xml() -> str:
    params = {
        "search_query": f"cat:{CATEGORY}",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": FETCH_RESULTS,
    }
    resp = requests.get(ARXIV_API, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_feed(xml_text: str) -> list[dict]:
    """Pure parsing step, kept separate from the network call so it's testable offline."""
    feed = feedparser.parse(xml_text)
    papers = []
    for e in feed.entries:
        abstract = " ".join(e.summary.split())
        title = " ".join(e.title.split())
        if len(abstract) < 20 or not title:
            continue
        papers.append({
            "id": e.id.split("/abs/")[-1],
            "title": title,
            "abstract": abstract,
            "authors": [a.name for a in e.authors],
            "published": e.published,
            "link": e.link,
        })
    return papers


def fetch_papers() -> list[dict]:
    return parse_feed(fetch_feed_xml())


def embed_papers(papers: list[dict], model_name: str = MODEL_NAME) -> list[dict]:
    """Attaches a normalized 384-dim embedding to each paper. Imports torch lazily
    so callers that only need parse_feed()/merge() never pay that cost."""
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    texts = [p["abstract"] for p in papers]
    vectors = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    for p, vec in zip(papers, vectors):
        p["embedding"] = [round(float(x), 5) for x in vec]
    return papers


def merge(existing: list[dict], new: list[dict], max_papers: int = MAX_PAPERS) -> list[dict]:
    """Upserts new papers by id (new data wins on conflict), then keeps the
    most recently published max_papers records so the index stays bounded."""
    by_id = {p["id"]: p for p in existing}
    for p in new:
        by_id[p["id"]] = p

    merged = sorted(by_id.values(), key=lambda p: p["published"], reverse=True)
    return merged[:max_papers]


def load_existing(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text()).get("papers", [])
    except (json.JSONDecodeError, OSError):
        return []


def save_index(path: Path, papers: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": BROWSER_MODEL_ID,
        "category": CATEGORY,
        "count": len(papers),
        "papers": papers,
    }, ensure_ascii=False, separators=(",", ":")))


def main() -> None:
    fetched = fetch_papers()
    if not fetched:
        raise SystemExit("arXiv returned zero papers -- refusing to touch the index")

    fetched = embed_papers(fetched)
    existing = load_existing(OUT_PATH)
    merged = merge(existing, fetched)
    save_index(OUT_PATH, merged)

    print(f"wrote {len(merged)} papers ({len(fetched)} fetched this run, "
          f"{len(existing)} carried over) to {OUT_PATH}")


if __name__ == "__main__":
    main()
