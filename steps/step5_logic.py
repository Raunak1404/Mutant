"""
step5_logic.py — Step 5: Process ESJC Data

Load and process the eSJC (Electronic Supplementary Job Card) Detailed Report.
Independent track from SAP processing (Steps 1-4).

Standard interface:
    main(input_path, output_path, libraries_dir) -> dict
"""

import json
import re
import sys
from pathlib import Path

import pandas as pd


# Columns to select from ESJC raw data
ESJC_SELECT_COLUMNS = [
    "ESJC no.",
    "A/C Reg",
    "DATE(COMPLETE)",
    "DATE(RAISED)",
    "ARR FLT",
    "CHECK",
    "LAE SIGN OFF LIC/AUTH No.",
    "JOB No.",
    "ORIGINATION CARD CROSS REFERENCE",
    "SNO",
    "DEFECT/ACTION REQUIRED",
    "ACTION TAKEN",
    "MHR DECIMAL",
    "MM OR RELEVENT APPROVED INSTRUCTION REFERENCE",
]

# First 9 columns used for forward-fill logic
METADATA_COLUMNS = ESJC_SELECT_COLUMNS[:9]

# Columns to merge for Defect/Action required
MERGE_DEFECT_COLS = [
    "JOB No.",
    "ORIGINATION CARD CROSS REFERENCE",
    "DEFECT/ACTION REQUIRED",
    "MM OR RELEVENT APPROVED INSTRUCTION REFERENCE",
]

# Columns to merge for Corrective Action
MERGE_ACTION_COLS = [
    "MM OR RELEVENT APPROVED INSTRUCTION REFERENCE",
    "ACTION TAKEN",
]

# Final column order
FINAL_COLUMN_ORDER = [
    "ESJC no.",
    "A/C Reg",
    "DATE(COMPLETE)",
    "DATE(RAISED)",
    "ARR FLT",
    "CHECK",
    "LAE SIGN OFF LIC/AUTH No.",
    "JOB No.",
    "ORIGINATION CARD CROSS REFERENCE",
    "SNO",
    "DEFECT/ACTION REQUIRED",
    "Defect/Action required",
    "MHR DECIMAL",
    "MM OR RELEVENT APPROVED INSTRUCTION REFERENCE",
    "From",
    "To",
    "ACTION TAKEN",
    "Corrective Action",
]


def _normalized_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected_by_normalized = {
        _normalized_header(name): name for name in ESJC_SELECT_COLUMNS
    }
    rename_map: dict[str, str] = {}

    for column in df.columns:
        normalized = _normalized_header(column)
        canonical = expected_by_normalized.get(normalized)
        if canonical and column != canonical:
            rename_map[column] = canonical

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def main(input_path: str, output_path: str, libraries_dir: str) -> dict:
    """
    Process ESJC Data.

    Args:
        input_path: Path to eSJC Detailed Report Excel file
        output_path: Path to write processed output Excel file
        libraries_dir: Path to reference libraries directory (unused)

    Returns:
        dict with keys: success (bool), changelog (list[dict]), stats (dict)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(str(input_path))
    df = _canonicalize_columns(df)
    total_rows_raw = len(df)

    # Verify required columns exist
    missing_cols = [c for c in ESJC_SELECT_COLUMNS if c not in df.columns]
    if missing_cols:
        return {
            "success": False,
            "changelog": [],
            "stats": {"error": f"Missing columns: {missing_cols}"},
        }

    # 1. Select columns
    df = df[ESJC_SELECT_COLUMNS].copy()

    # 2. Add From/To columns
    df["From"] = ""
    df["To"] = "SIN"

    # 3. Merge columns into Defect/Action required
    df["Defect/Action required"] = (
        df[MERGE_DEFECT_COLS].fillna("").astype(str).agg(" ".join, axis=1)
    )

    # 4. Merge columns into Corrective Action
    df["Corrective Action"] = (
        df[MERGE_ACTION_COLS].fillna("").astype(str).agg(" ".join, axis=1)
    )

    # 5. Drop rows where ALL ORIGINAL columns are NaN (check before added
    #    columns like From/To, which are never null, would make dropna a no-op)
    before_drop = len(df)
    all_nan_mask = df[ESJC_SELECT_COLUMNS].isnull().all(axis=1)
    df = df[~all_nan_mask].reset_index(drop=True)
    rows_dropped_nan = before_drop - len(df)

    # 6. Forward-fill metadata for sub-rows
    # Identify rows where ALL metadata columns are NaN (these are sub-rows)
    mask = df[METADATA_COLUMNS].isnull().all(axis=1)
    sub_rows_count = mask.sum()

    # Forward-fill only those metadata columns, only for sub-rows
    if sub_rows_count > 0:
        df.loc[mask, METADATA_COLUMNS] = df[METADATA_COLUMNS].ffill().loc[mask]

    # 7. Rearrange to final column order
    df = df[FINAL_COLUMN_ORDER].copy()

    rows_output = len(df)

    # Write output
    df.to_excel(str(output_path), index=False)

    changelog = [
        {
            "type": "COLUMNS_SELECTED",
            "column": "all",
            "old_value": f"{total_rows_raw} rows, raw columns",
            "new_value": f"{len(ESJC_SELECT_COLUMNS)} columns selected",
            "reason": "Selected required ESJC columns",
        },
        {
            "type": "COLUMNS_ADDED",
            "column": "From, To",
            "old_value": "N/A",
            "new_value": "From='', To='SIN'",
            "reason": "Added routing columns",
        },
        {
            "type": "COLUMNS_MERGED",
            "column": "Defect/Action required, Corrective Action",
            "old_value": "separate columns",
            "new_value": "2 merged columns created",
            "reason": "Merged job/defect/action columns for readability",
        },
    ]

    if rows_dropped_nan > 0:
        changelog.append({
            "type": "ROWS_DROPPED_NAN",
            "column": "all",
            "old_value": f"{before_drop} rows",
            "new_value": f"{before_drop - rows_dropped_nan} rows",
            "reason": f"Removed {rows_dropped_nan} all-NaN rows",
        })

    if sub_rows_count > 0:
        changelog.append({
            "type": "ROWS_FORWARD_FILLED",
            "column": "metadata columns",
            "old_value": f"{sub_rows_count} sub-rows with empty metadata",
            "new_value": "metadata filled from parent row",
            "reason": "Forward-filled metadata for sub-rows of parent ESJC entries",
        })

    stats = {
        "total_rows_raw": total_rows_raw,
        "rows_dropped_nan": rows_dropped_nan,
        "sub_rows_filled": int(sub_rows_count),
        "rows_output": rows_output,
        "columns_output": len(FINAL_COLUMN_ORDER),
        "total_changelog_entries": len(changelog),
    }

    return {
        "success": True,
        "changelog": changelog,
        "stats": stats,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python step5_logic.py <input_path> <output_path> [libraries_dir]")
        sys.exit(1)

    inp = sys.argv[1]
    out = sys.argv[2]
    libs = sys.argv[3] if len(sys.argv) > 3 else "./libraries"

    result = main(inp, out, libs)
    print(json.dumps(result.get("stats", {}), indent=2))
    if not result["success"]:
        sys.exit(1)
