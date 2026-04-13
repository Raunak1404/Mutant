"""
step4_logic.py — Step 4: Format SAP Output

Rearrange columns into final reporting order and add empty columns for
manual data entry by the vetting team.

Standard interface:
    main(input_path, output_path, libraries_dir) -> dict
"""

import json
import sys
from pathlib import Path

import pandas as pd


# Column alias mapping — canonical name → accepted input variants.
# When a new column name variant appears in input data, add it to the set.
COLUMN_ALIASES = {
    "Tail": {"Tail"},
}

# Final column order for data columns (canonical names, resolved via COLUMN_ALIASES)
DATA_COLUMN_ORDER = [
    "Tail",
    "Flight Number",
    "From Station",
    "To Station",
    "Date",
    "Resolved date",
    "Description",
    "Defect",
    "Corrective Action",
    "LIC NO",
    "Defect_Category",
    "Needs_Highlight",
    "Action_Count",
]

# Empty columns to add for manual data entry (in order)
MANUAL_ENTRY_COLUMNS = [
    "MHR",
    "CHARGEABLE (YES/UNSURE)",
    "REMARK",
    "GPU",
    "NOCO",
    "BM",
    "NITRO CART",
    "CRANE",
    "CH,PICKER",
    "ENTERED BY",
    "DATE UPDATED",
    "PPW VET BY",
    "REMARK.1",
    "SHAIK'S REMARK",
    "VETTER'S REMARK",
    "RAY'S NOTE",
    "INITIAL VET BY",
    "DATE VETTED",
    "CHECKED BY",
    "DATE",
]


def _resolve_column(df: pd.DataFrame, canonical: str):
    """Find the actual column name in df for a canonical name using COLUMN_ALIASES."""
    aliases = COLUMN_ALIASES.get(canonical, {canonical})
    for alias in aliases:
        if alias in df.columns:
            return alias
    if canonical in df.columns:
        return canonical
    return None


def main(input_path: str, output_path: str, libraries_dir: str) -> dict:
    """
    Format SAP Output.

    Args:
        input_path: Path to Step 3 output Excel file
        output_path: Path to write formatted output Excel file
        libraries_dir: Path to reference libraries directory (unused)

    Returns:
        dict with keys: success (bool), changelog (list[dict]), stats (dict)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(str(input_path))
    total_rows = len(df)

    # Resolve columns using COLUMN_ALIASES (handles variant column names)
    resolved_order = []
    for col in DATA_COLUMN_ORDER:
        resolved = _resolve_column(df, col)
        if resolved is None:
            aliases = COLUMN_ALIASES.get(col, {col})
            return {
                "success": False,
                "changelog": [],
                "stats": {"error": f"Missing column '{col}'. Tried aliases: {sorted(aliases)}, available: {list(df.columns)}"},
            }
        resolved_order.append(resolved)

    # 1. Rearrange data columns
    df = df[resolved_order].copy()

    # 2. Add empty manual-entry columns
    for col in MANUAL_ENTRY_COLUMNS:
        df[col] = ""

    total_columns = len(df.columns)

    # Write output
    df.to_excel(str(output_path), index=False)

    changelog = [
        {
            "type": "COLUMNS_REARRANGED",
            "column": "all",
            "old_value": "original order",
            "new_value": f"{len(DATA_COLUMN_ORDER)} data columns reordered",
            "reason": "Rearranged to final reporting order",
        },
        {
            "type": "COLUMNS_ADDED",
            "column": "manual entry columns",
            "old_value": "N/A",
            "new_value": f"{len(MANUAL_ENTRY_COLUMNS)} empty columns added",
            "reason": "Added empty columns for manual vetting data entry",
        },
    ]

    stats = {
        "total_rows": total_rows,
        "total_columns": total_columns,
        "data_columns": len(DATA_COLUMN_ORDER),
        "manual_entry_columns": len(MANUAL_ENTRY_COLUMNS),
        "total_changelog_entries": len(changelog),
    }

    return {
        "success": True,
        "changelog": changelog,
        "stats": stats,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python step4_logic.py <input_path> <output_path> [libraries_dir]")
        sys.exit(1)

    inp = sys.argv[1]
    out = sys.argv[2]
    libs = sys.argv[3] if len(sys.argv) > 3 else "./libraries"

    result = main(inp, out, libs)
    print(json.dumps(result.get("stats", {}), indent=2))
    if not result["success"]:
        sys.exit(1)
