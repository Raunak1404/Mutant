"""
step1_logic.py — Step 1: Load & Clean SAP Data

Load the SAP Raw Data Excel file, select required columns, drop rows with
missing defect/action data, and filter out non-code descriptions.

Standard interface:
    main(input_path, output_path, libraries_dir) -> dict
"""

import json
import re
import sys
from pathlib import Path

import pandas as pd


# Column alias mapping — canonical name → accepted input variants.
# When a new column name variant appears in input data, add it to the set.
COLUMN_ALIASES = {
    "Tail": {"Tail"},
}

# Columns to select from the SAP raw data (canonical names, resolved via COLUMN_ALIASES)
SAP_COLUMNS = [
    "Tail",
    "Flight Number",
    "From Station",
    "To Station",
    "Date",
    "Description",
    "Defect Text1",
    "ACTION Text1",
]

# Regex: standalone word of 4+ letters (catches "WING", "FOUND", not "Z200")
WORD_PATTERN = r"\b[A-Za-z]{4,}\b"

# Rename mapping
RENAME_MAP = {
    "Defect Text1": "Defect",
    "ACTION Text1": "Corrective Action",
}


def _resolve_column(df: pd.DataFrame, canonical: str):
    """Find the actual column name in df for a canonical name using COLUMN_ALIASES."""
    aliases = COLUMN_ALIASES.get(canonical, {canonical})
    for alias in aliases:
        if alias in df.columns:
            return alias
    # Fallback: check if canonical itself exists (for columns not in COLUMN_ALIASES)
    if canonical in df.columns:
        return canonical
    return None


def main(input_path: str, output_path: str, libraries_dir: str) -> dict:
    """
    Load & Clean SAP Data.

    Args:
        input_path: Path to SAP Raw Data Excel file
        output_path: Path to write cleaned output Excel file
        libraries_dir: Path to reference libraries directory (unused in this step)

    Returns:
        dict with keys: success (bool), changelog (list[dict]), stats (dict)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(str(input_path))
    total_rows_raw = len(df)

    # Resolve columns using COLUMN_ALIASES (handles variant column names)
    select_columns = []
    for col in SAP_COLUMNS:
        resolved = _resolve_column(df, col)
        if resolved is None:
            aliases = COLUMN_ALIASES.get(col, {col})
            return {
                "success": False,
                "changelog": [],
                "stats": {"error": f"Missing column '{col}'. Tried aliases: {sorted(aliases)}, available: {list(df.columns)}"},
            }
        select_columns.append(resolved)

    # 1. Select columns
    df = df[select_columns].copy()

    # 2. Drop rows where Defect Text1 or ACTION Text1 is null
    before_drop = len(df)
    df = df.dropna(subset=["Defect Text1", "ACTION Text1"])
    rows_dropped_null = before_drop - len(df)

    # 3. Filter Description column
    df["Description"] = df["Description"].astype(str).str.strip()

    # Remove rows containing standalone words of 4+ letters
    before_filter = len(df)
    df = df[~df["Description"].str.contains(WORD_PATTERN, case=False, na=False)].copy()
    rows_removed_words = before_filter - len(df)

    # Keep only rows where Description contains at least one digit
    before_digit = len(df)
    df = df[df["Description"].str.contains(r"\d", na=False)].copy()
    rows_removed_no_digit = before_digit - len(df)

    # 4. Rename columns
    df = df.rename(columns=RENAME_MAP)

    rows_kept = len(df)

    # Write output
    df.to_excel(str(output_path), index=False)

    changelog = []
    if rows_dropped_null > 0:
        changelog.append({
            "type": "ROWS_DROPPED_NULL",
            "column": "Defect Text1 / ACTION Text1",
            "old_value": f"{before_drop} rows",
            "new_value": f"{before_drop - rows_dropped_null} rows",
            "reason": f"Removed {rows_dropped_null} rows with null Defect or Action",
        })
    if rows_removed_words > 0:
        changelog.append({
            "type": "ROWS_FILTERED_DESCRIPTION",
            "column": "Description",
            "old_value": f"{before_filter} rows",
            "new_value": f"{before_filter - rows_removed_words} rows",
            "reason": f"Removed {rows_removed_words} rows with standalone word descriptions",
        })
    if rows_removed_no_digit > 0:
        changelog.append({
            "type": "ROWS_FILTERED_NO_DIGIT",
            "column": "Description",
            "old_value": f"{before_digit} rows",
            "new_value": f"{before_digit - rows_removed_no_digit} rows",
            "reason": f"Removed {rows_removed_no_digit} rows with no digits in Description",
        })
    changelog.append({
        "type": "COLUMNS_RENAMED",
        "column": "Defect Text1, ACTION Text1",
        "old_value": "Defect Text1, ACTION Text1",
        "new_value": "Defect, Corrective Action",
        "reason": "Renamed columns for consistency",
    })

    stats = {
        "total_rows_raw": total_rows_raw,
        "rows_dropped_null": rows_dropped_null,
        "rows_removed_words": rows_removed_words,
        "rows_removed_no_digit": rows_removed_no_digit,
        "rows_kept": rows_kept,
        "total_changelog_entries": len(changelog),
    }

    return {
        "success": True,
        "changelog": changelog,
        "stats": stats,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python step1_logic.py <input_path> <output_path> [libraries_dir]")
        sys.exit(1)

    inp = sys.argv[1]
    out = sys.argv[2]
    libs = sys.argv[3] if len(sys.argv) > 3 else "./libraries"

    result = main(inp, out, libs)
    print(json.dumps(result.get("stats", {}), indent=2))
    if not result["success"]:
        sys.exit(1)
