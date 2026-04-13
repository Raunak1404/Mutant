"""
step2_logic.py — Step 2: Split & Extract SAP Actions

Split multi-action corrective action entries into separate rows, then extract
resolved date, license number, and destination station via regex.

Standard interface:
    main(input_path, output_path, libraries_dir) -> dict
"""

import json
import re
import sys
from pathlib import Path

import pandas as pd


def main(input_path: str, output_path: str, libraries_dir: str) -> dict:
    """
    Split & Extract SAP Actions.

    Args:
        input_path: Path to Step 1 output Excel file
        output_path: Path to write expanded output Excel file
        libraries_dir: Path to reference libraries directory (unused)

    Returns:
        dict with keys: success (bool), changelog (list[dict]), stats (dict)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(str(input_path))
    rows_before = len(df)

    required_cols = ["Corrective Action"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return {
            "success": False,
            "changelog": [],
            "stats": {"error": f"Missing columns: {missing}"},
        }

    # 1. Count DEFECT ACTION occurrences per row
    df["Action_Count"] = df["Corrective Action"].astype(str).str.count("DEFECT ACTION")

    # 2. Split Corrective Action on "DEFECT ACTION" using lookahead
    df["Corrective Action"] = df["Corrective Action"].apply(
        lambda x: re.split(r"(?=DEFECT ACTION)", str(x))
    )

    # 3. Explode into individual rows
    df = df.explode("Corrective Action").reset_index(drop=True)

    # 4. Remove empty/whitespace rows after split
    df = df[df["Corrective Action"].str.strip() != ""].reset_index(drop=True)

    rows_after_split = len(df)

    # 5. Extract Resolved date from action text
    df["Resolved date"] = df["Corrective Action"].str.extract(
        r"ACTION ENTRY DATE & TIME:\s*(\d{2}\.\d{2}\.\d{4})"
    )
    df["Resolved date"] = pd.to_datetime(
        df["Resolved date"], format="%d.%m.%Y", errors="coerce"
    )
    dates_extracted = df["Resolved date"].notna().sum()
    df["Resolved date"] = df["Resolved date"].dt.strftime("%d-%m-%Y").where(
        df["Resolved date"].notna()
    )

    # 6. Mark rows for highlighting (split actions with count > 1)
    df["Needs_Highlight"] = (
        df["Corrective Action"].str.contains("DEFECT ACTION", na=False)
        & (df["Action_Count"] > 1)
    )

    highlight_count = df["Needs_Highlight"].sum()

    # 7. Extract License Number
    df["LIC NO"] = df["Corrective Action"].str.extract(
        r"LICENSE/CAAS NO:\s*([^(\n\r]+)"
    )
    df["LIC NO"] = df["LIC NO"].str.strip()

    licenses_extracted = df["LIC NO"].notna().sum()

    # 8. Extract To Station (3-char code from last parentheses)
    extracted_to = df["Corrective Action"].str.extract(
        r"\((\w{3})\)\s*(?:\r?\n|$)"
    )
    # Only overwrite To Station where extraction succeeded
    if "To Station" in df.columns:
        mask = extracted_to[0].notna()
        df.loc[mask, "To Station"] = extracted_to.loc[mask, 0]
    else:
        df["To Station"] = extracted_to[0]

    stations_extracted = extracted_to[0].notna().sum()

    # Write output
    df.to_excel(str(output_path), index=False)

    changelog = [
        {
            "type": "ROWS_SPLIT",
            "column": "Corrective Action",
            "old_value": f"{rows_before} rows",
            "new_value": f"{rows_after_split} rows",
            "reason": f"Split multi-action entries, added {rows_after_split - rows_before} new rows",
        },
        {
            "type": "COLUMN_ADDED",
            "column": "Resolved date",
            "old_value": "N/A",
            "new_value": f"{dates_extracted} dates extracted",
            "reason": "Extracted from ACTION ENTRY DATE & TIME pattern",
        },
        {
            "type": "COLUMN_ADDED",
            "column": "LIC NO",
            "old_value": "N/A",
            "new_value": f"{licenses_extracted} licenses extracted",
            "reason": "Extracted from LICENSE/CAAS NO pattern",
        },
        {
            "type": "COLUMN_UPDATED",
            "column": "To Station",
            "old_value": "original values",
            "new_value": f"{stations_extracted} stations extracted",
            "reason": "Extracted 3-char station code from last parentheses",
        },
    ]

    stats = {
        "total_rows_input": rows_before,
        "total_rows_output": rows_after_split,
        "rows_added_by_split": rows_after_split - rows_before,
        "dates_extracted": int(dates_extracted),
        "licenses_extracted": int(licenses_extracted),
        "stations_extracted": int(stations_extracted),
        "highlight_count": int(highlight_count),
        "total_changelog_entries": len(changelog),
    }

    return {
        "success": True,
        "changelog": changelog,
        "stats": stats,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python step2_logic.py <input_path> <output_path> [libraries_dir]")
        sys.exit(1)

    inp = sys.argv[1]
    out = sys.argv[2]
    libs = sys.argv[3] if len(sys.argv) > 3 else "./libraries"

    result = main(inp, out, libs)
    print(json.dumps(result.get("stats", {}), indent=2))
    if not result["success"]:
        sys.exit(1)
