from __future__ import annotations

from typing import Any

import pandas as pd
from pydantic import BaseModel


class ExcelFileInfo(BaseModel):
    """Metadata extracted from an uploaded Excel file."""
    filename: str
    total_rows: int
    total_columns: int
    sheet_names: list[str]
    dtypes: dict[str, str]           # column_name → dtype string
    sample_rows: list[dict[str, Any]]  # first 5 rows for rule classification


class DataFrameChunk(BaseModel):
    """A slice of a DataFrame for parallel processing."""
    chunk_id: int
    start_row: int
    end_row: int
    # The actual DataFrame is NOT in this pydantic model — passed separately

    model_config = {"arbitrary_types_allowed": True}
