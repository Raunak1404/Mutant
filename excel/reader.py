from __future__ import annotations

import asyncio
import io
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from excel.models import ExcelFileInfo
from storage.backend import StorageBackend
from utils.logging import get_logger

logger = get_logger(__name__)


async def read_excel_to_parquet(
    storage: StorageBackend,
    input_key: str,
    output_key: str,
) -> ExcelFileInfo:
    """Download Excel from storage, convert to parquet, upload back, return metadata."""
    excel_bytes = await storage.download(input_key)

    def _parse() -> tuple[pd.DataFrame, list[str]]:
        xls = pd.ExcelFile(io.BytesIO(excel_bytes))
        sheet_names = xls.sheet_names
        df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=0, dtype=str)
        df = df.fillna("")
        return df, sheet_names

    df, sheet_names = await asyncio.to_thread(_parse)

    parquet_bytes = await asyncio.to_thread(_df_to_parquet_bytes, df)
    await storage.upload(output_key, parquet_bytes)

    sample_rows = df.head(5).to_dict(orient="records")

    info = ExcelFileInfo(
        filename=input_key.split("/")[-1],
        total_rows=len(df),
        total_columns=len(df.columns),
        sheet_names=sheet_names,
        dtypes={col: str(df[col].dtype) for col in df.columns},
        sample_rows=sample_rows,
    )
    logger.info(
        "excel_parsed",
        key=input_key,
        rows=info.total_rows,
        cols=info.total_columns,
    )
    return info


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    return buf.getvalue()


async def load_parquet_from_storage(
    storage: StorageBackend, key: str
) -> pd.DataFrame:
    parquet_bytes = await storage.download(key)
    return await asyncio.to_thread(_parquet_bytes_to_df, parquet_bytes)


def _parquet_bytes_to_df(data: bytes) -> pd.DataFrame:
    buf = io.BytesIO(data)
    return pq.read_table(buf).to_pandas()


async def save_df_to_storage(
    storage: StorageBackend, df: pd.DataFrame, key: str
) -> None:
    parquet_bytes = await asyncio.to_thread(_df_to_parquet_bytes, df)
    await storage.upload(key, parquet_bytes)
