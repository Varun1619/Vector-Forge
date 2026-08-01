from vectorforge.chunking import chunk_text


def test_empty_returns_no_chunks():
    assert chunk_text("") == []


def test_short_text_is_single_chunk():
    assert chunk_text("hello world") == ["hello world"]


def test_long_text_splits_with_overlap():
    text = " ".join(f"word{i}" for i in range(400))
    chunks = chunk_text(text, max_chars=200, overlap=40)
    assert len(chunks) > 1
    # overlap: the end of one chunk reappears at the start of the next
    assert chunks[0].split()[-1] in chunks[1]


def test_no_word_is_split():
    chunks = chunk_text("supercalifragilistic " * 50, max_chars=100)
    for c in chunks:
        assert "supercalifragilisticsupercalifragilistic" not in c