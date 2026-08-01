from __future__ import annotations


def chunk_text(text: str, max_chars: int = 800, overlap: int = 100) -> list[str]:
    """Split text into overlapping windows on word boundaries."""
    words = text.split()
    if not words:
        return []

    chunks, current, length = [], [], 0
    for word in words:
        if length + len(word) + 1 > max_chars and current:
            chunks.append(" ".join(current))
            # keep the tail as overlap so context isn't cut mid-idea
            keep = " ".join(current)[-overlap:].split()
            current, length = keep, len(" ".join(keep))
        current.append(word)
        length += len(word) + 1

    if current:
        chunks.append(" ".join(current))
    return chunks