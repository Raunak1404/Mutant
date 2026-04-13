from __future__ import annotations

import pandas as pd

from excel.models import DataFrameChunk
from models.results import RuleMetadata
from models.enums import DependencyType


def create_chunks(
    df: pd.DataFrame,
    metadata: RuleMetadata,
    chunk_size: int = 100,
) -> list[tuple[DataFrameChunk, pd.DataFrame]]:
    """
    Context-aware chunking. Returns list of (chunk_meta, chunk_df) tuples.
    For sequential chunks, overlap rows are included at the start but their
    row indices are flagged — results from overlap zone are discarded.
    """
    match metadata.dependency_type:
        case DependencyType.NONE:
            return _split_by_offset(df, chunk_size)
        case DependencyType.GROUP:
            return _split_by_group(df, metadata.group_key or "", chunk_size)
        case DependencyType.SEQUENTIAL:
            overlap = _parse_scope(metadata.dependency_scope)
            return _split_with_overlap(df, chunk_size, overlap)
        case DependencyType.GLOBAL:
            chunk = DataFrameChunk(chunk_id=0, start_row=0, end_row=len(df))
            return [(chunk, df)]
        case _:
            return _split_by_offset(df, chunk_size)


def _split_by_offset(df: pd.DataFrame, chunk_size: int) -> list[tuple[DataFrameChunk, pd.DataFrame]]:
    chunks = []
    for i, start in enumerate(range(0, len(df), chunk_size)):
        end = min(start + chunk_size, len(df))
        chunk_meta = DataFrameChunk(chunk_id=i, start_row=start, end_row=end)
        chunks.append((chunk_meta, df.iloc[start:end].copy()))
    return chunks


def _split_by_group(
    df: pd.DataFrame, group_key: str, max_chunk_size: int
) -> list[tuple[DataFrameChunk, pd.DataFrame]]:
    if not group_key or group_key not in df.columns:
        return _split_by_offset(df, max_chunk_size)

    chunks = []
    chunk_id = 0
    current_rows: list[int] = []

    for group_val, group_df in df.groupby(group_key, sort=False):
        # If adding this group would exceed max_chunk_size, flush first
        if current_rows and len(current_rows) + len(group_df) > max_chunk_size:
            subset = df.loc[current_rows]
            chunk_meta = DataFrameChunk(
                chunk_id=chunk_id,
                start_row=current_rows[0],
                end_row=current_rows[-1] + 1,
            )
            chunks.append((chunk_meta, subset.copy()))
            chunk_id += 1
            current_rows = []
        current_rows.extend(group_df.index.tolist())

    if current_rows:
        subset = df.loc[current_rows]
        chunk_meta = DataFrameChunk(
            chunk_id=chunk_id,
            start_row=current_rows[0] if isinstance(current_rows[0], int) else 0,
            end_row=len(df),
        )
        chunks.append((chunk_meta, subset.copy()))

    return chunks if chunks else _split_by_offset(df, max_chunk_size)


def _split_with_overlap(
    df: pd.DataFrame, chunk_size: int, overlap_rows: int
) -> list[tuple[DataFrameChunk, pd.DataFrame]]:
    """Each chunk includes `overlap_rows` rows from the previous chunk as READ-ONLY context."""
    chunks = []
    chunk_id = 0
    start = 0

    while start < len(df):
        end = min(start + chunk_size, len(df))
        overlap_start = max(0, start - overlap_rows)
        chunk_df = df.iloc[overlap_start:end].copy()
        n_overlap = start - overlap_start
        chunk_df["_overlap_context"] = [True] * n_overlap + [False] * (len(chunk_df) - n_overlap)

        chunk_meta = DataFrameChunk(chunk_id=chunk_id, start_row=start, end_row=end)
        chunks.append((chunk_meta, chunk_df))
        chunk_id += 1
        start = end

    return chunks


def _parse_scope(scope: str | None) -> int:
    """Parse 'backward_3' → 3."""
    if not scope:
        return 3
    parts = scope.split("_")
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 3
