import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from build_search_index import load_existing, merge, parse_feed, save_index  # noqa: E402

SAMPLE_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2508.00001v1</id>
    <title>  A Study of   Things </title>
    <summary>  This paper studies   things in great detail with rigor. </summary>
    <published>2025-08-01T00:00:00Z</published>
    <link href="http://arxiv.org/abs/2508.00001v1" rel="alternate" type="text/html"/>
    <author><name>Ada Lovelace</name></author>
    <author><name>Alan Turing</name></author>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2508.00002v1</id>
    <title>Too Short</title>
    <summary>short</summary>
    <published>2025-08-02T00:00:00Z</published>
    <link href="http://arxiv.org/abs/2508.00002v1" rel="alternate" type="text/html"/>
    <author><name>Grace Hopper</name></author>
  </entry>
</feed>
"""


def test_parse_feed_normalizes_whitespace_and_extracts_fields():
    papers = parse_feed(SAMPLE_FEED)
    assert len(papers) == 1  # the second entry's abstract is under the 20-char floor
    p = papers[0]
    assert p["id"] == "2508.00001v1"
    assert p["title"] == "A Study of Things"
    assert p["abstract"] == "This paper studies things in great detail with rigor."
    assert p["authors"] == ["Ada Lovelace", "Alan Turing"]
    assert p["link"] == "http://arxiv.org/abs/2508.00001v1"


def test_merge_upserts_by_id_and_prefers_new_data():
    existing = [{"id": "a", "title": "old title", "published": "2025-08-01T00:00:00Z"}]
    new = [{"id": "a", "title": "new title", "published": "2025-08-01T00:00:00Z"}]
    merged = merge(existing, new)
    assert len(merged) == 1
    assert merged[0]["title"] == "new title"


def test_merge_sorts_newest_first_and_caps_length():
    existing = [{"id": str(i), "title": "x", "published": f"2025-08-{i:02d}T00:00:00Z"}
                for i in range(1, 6)]
    merged = merge(existing, [], max_papers=3)
    assert [p["id"] for p in merged] == ["5", "4", "3"]


def test_save_index_round_trips_through_load_existing(tmp_path):
    path = tmp_path / "papers.json"
    papers = [{"id": "a", "title": "t", "published": "2025-08-01T00:00:00Z", "embedding": [0.1, 0.2]}]

    save_index(path, papers)
    on_disk = json.loads(path.read_text())
    assert on_disk["count"] == 1
    assert on_disk["model"] == "Xenova/all-MiniLM-L6-v2"

    assert load_existing(path) == papers


def test_load_existing_missing_file_returns_empty_list(tmp_path):
    assert load_existing(tmp_path / "nope.json") == []
