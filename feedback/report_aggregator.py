from __future__ import annotations

from collections import defaultdict
from typing import Any

from models.results import ReviewReport


def aggregate_reports(reports: list[ReviewReport]) -> dict[str, Any]:
    """
    Merge all step ReviewReports into a single summary.
    Groups failures by pattern across steps for question generation.
    """
    total_rows = 0
    total_failed = 0
    pattern_groups: dict[str, list[dict]] = defaultdict(list)
    step_summaries: list[dict] = []

    for report in reports:
        total_rows += report.total_rows
        total_failed += report.failed_rows

        for pattern in report.failure_patterns:
            pattern_groups[pattern].append({
                "step": report.step_number,
                "failed_rows": report.failed_rows,
                "narrative": report.narrative_summary,
            })

        step_summaries.append({
            "step": report.step_number,
            "total": report.total_rows,
            "passed": report.passed_rows,
            "failed": report.failed_rows,
            "patterns": report.failure_patterns,
            "narrative": report.narrative_summary,
            "confidence": report.confidence_score,
        })

    # Sort patterns by total failures
    sorted_patterns = sorted(
        pattern_groups.items(),
        key=lambda x: sum(s["failed_rows"] for s in x[1]),
        reverse=True,
    )

    return {
        "total_rows": total_rows,
        "total_failed": total_failed,
        "overall_pass_rate": round((total_rows - total_failed) / max(total_rows, 1), 4),
        "step_summaries": step_summaries,
        "top_patterns": [
            {
                "pattern": pattern,
                "occurrences": len(steps),
                "total_failures": sum(s["failed_rows"] for s in steps),
                "affected_steps": [s["step"] for s in steps],
            }
            for pattern, steps in sorted_patterns[:10]
        ],
    }
