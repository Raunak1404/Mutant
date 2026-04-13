"""
step3_logic.py — Step 3: Classify SAP Defects

Categorize each defect row into one of five categories based on keyword
matching: Oxygen Issue, Toilet Choke, NIL DEFECT, Open Defect, Unclassified.

Standard interface:
    main(input_path, output_path, libraries_dir) -> dict
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# Keyword patterns for each defect category
OXY_DEFECT_KEYWORDS = r"OXY|o2|Bottle|BTL|SOK"
OXY_ACTION_KEYWORDS = (
    r"replace|rpl|service|uplift|raised|insp|inspection|OXY|o2|Bottle|BTL|SOK"
)

TOILET_DEFECT_KEYWORDS = r"toilet|choke"
TOILET_ACTION_KEYWORDS = (
    r"FOD AND WASTE WATER CLEARED|CHOKED CLEARED|TOILET BOWLS|"
    r"FLUSHING|TOILET BOWL FOUND CLOGGED"
)

NIL_DEFECT_PATTERN = r"^NIL DEFECT"
NIL_ACTION_KEYWORDS = (
    r"CLOSURE NOTED AS NIL DEFECT|NOTED THANK|NIL NOTED|"
    r"NIL FAULTED NOTED|CLOSURE NIL NOTED THANK|CLOSURE WELL NOTED"
)

OPEN_DEFECT_PATTERN = r"defect action - closure"


def main(input_path: str, output_path: str, libraries_dir: str) -> dict:
    """
    Classify SAP Defects.

    Args:
        input_path: Path to Step 2 output Excel file
        output_path: Path to write classified output Excel file
        libraries_dir: Path to reference libraries directory (unused)

    Returns:
        dict with keys: success (bool), changelog (list[dict]), stats (dict)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(str(input_path))
    total_rows = len(df)

    required_cols = ["Defect", "Corrective Action"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return {
            "success": False,
            "changelog": [],
            "stats": {"error": f"Missing columns: {missing}"},
        }

    defect_str = df["Defect"].astype(str)
    action_str = df["Corrective Action"].astype(str)

    # 1. Oxygen Issue mask
    oxy_mask = (
        defect_str.str.contains(OXY_DEFECT_KEYWORDS, case=False, na=False)
        & action_str.str.contains(OXY_ACTION_KEYWORDS, case=False, na=False)
    )

    # 2. Toilet Choke mask
    toilet_mask = (
        defect_str.str.contains(TOILET_DEFECT_KEYWORDS, case=False, na=False)
        & action_str.str.contains(TOILET_ACTION_KEYWORDS, case=False, na=False)
    )

    # 3. NIL DEFECT mask
    nil_mask = (
        defect_str.str.contains(NIL_DEFECT_PATTERN, case=False, na=False)
        & action_str.str.contains(NIL_ACTION_KEYWORDS, case=False, na=False)
    )

    # 4. Open Defect mask (neither Defect nor Action contains closure pattern)
    open_mask = (
        ~defect_str.str.contains(OPEN_DEFECT_PATTERN, case=False, na=False)
        & ~action_str.str.contains(OPEN_DEFECT_PATTERN, case=False, na=False)
    )

    # 5. Apply classification in priority order
    conditions = [oxy_mask, toilet_mask, nil_mask, open_mask]
    choices = ["Oxygen Issue", "Toilet Choke", "NIL DEFECT", "Open Defect"]
    df["Defect_Category"] = np.select(conditions, choices, default="Unclassified")

    # Count categories
    category_counts = df["Defect_Category"].value_counts().to_dict()

    # Write output
    df.to_excel(str(output_path), index=False)

    changelog = [
        {
            "type": "COLUMN_ADDED",
            "column": "Defect_Category",
            "old_value": "N/A",
            "new_value": f"{len(category_counts)} categories assigned",
            "reason": "Classified defects by keyword matching on Defect and Corrective Action",
        },
    ]

    stats = {
        "total_rows": total_rows,
        "category_counts": category_counts,
        "total_changelog_entries": len(changelog),
    }

    return {
        "success": True,
        "changelog": changelog,
        "stats": stats,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python step3_logic.py <input_path> <output_path> [libraries_dir]")
        sys.exit(1)

    inp = sys.argv[1]
    out = sys.argv[2]
    libs = sys.argv[3] if len(sys.argv) > 3 else "./libraries"

    result = main(inp, out, libs)
    print(json.dumps(result.get("stats", {}), indent=2))
    if not result["success"]:
        sys.exit(1)
