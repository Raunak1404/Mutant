from __future__ import annotations

import asyncio
import io

import pandas as pd
import pyarrow.parquet as pq

from storage.backend import StorageBackend
from utils.logging import get_logger

logger = get_logger(__name__)


async def write_parquet_to_excel(
    storage: StorageBackend,
    parquet_key: str,
    output_key: str,
) -> None:
    """Download final parquet from storage, convert to Excel, upload back."""
    parquet_bytes = await storage.download(parquet_key)

    def _convert() -> bytes:
        buf_in = io.BytesIO(parquet_bytes)
        df = pq.read_table(buf_in).to_pandas()
        buf_out = io.BytesIO()
        with pd.ExcelWriter(buf_out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Output")
        return buf_out.getvalue()

    excel_bytes = await asyncio.to_thread(_convert)
    await storage.upload(output_key, excel_bytes)
    logger.info("excel_written", key=output_key)
