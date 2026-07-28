from __future__ import annotations

import pandera as pa
from pandera import Column, Check


arxiv_schema = pa.DataFrameSchema(
    {
        "arxiv_id": Column(str, nullable=False, unique=True),
        "title": Column(str, Check.str_length(min_value=1), nullable=False),
        "abstract": Column(str, Check.str_length(min_value=20), nullable=False),
        "published": Column(str, nullable=False),
        "link": Column(str, Check.str_startswith("http"), nullable=False),
    },
    strict=False,
    coerce=True,
)