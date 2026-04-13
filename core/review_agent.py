from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd

from core.agent_base import AgentBase
from core.agent_pool import AgentPool
from core.chunker_strategy import create_chunks
from models.messages import Message
from models.results import (
    PartialReviewReport,
    ReviewReport,
    RuleMetadata,
)
from models.enums import DependencyType
from utils.logging import get_logger

logger = get_logger(__name__)

REVIEW_SYSTEM_PROMPT = """You are a data quality reviewer.
Check each row against the INTENT of the given rules — not the literal column names.

CRITICAL GUIDELINES:
- The rules describe intent (e.g., "filter rows where income > 4000"). The actual column names in the data may differ from what the rules mention. This is EXPECTED and NOT a failure.
- Focus on whether the data VALUES are correct, not whether column names match the rules exactly.
- If a rule says "How to Review This Step", follow those instructions precisely.
- A row PASSES if its values satisfy the rule's intent. A row FAILS only if its values clearly violate the rule's intent.
- When in doubt, mark the row as PASSED. Only flag clear, unambiguous violations.
- IMPORTANT: If a pre-filter has already verified certain conditions deterministically (noted in the prompt), do NOT re-check those conditions. Trust the pre-filter results.

Return a JSON object with:
  - "passed_rows": integer
  - "failed_rows": list of {"row_index": int, "reason": str, "pattern": str}
  - "failure_patterns": list of strings (brief pattern descriptions)

Return ONLY the JSON object."""

SYNTHESIZE_SYSTEM_PROMPT = """You are a data quality analyst.
Given aggregated review statistics and representative failure examples,
write a concise narrative summary (3-5 sentences) and identify the top failure patterns.
Return JSON: {"narrative_summary": str, "confidence_score": float (0-1), "top_patterns": list[str]}"""


class ReviewAgent(AgentBase):
    """4-phase map-reduce review: pre-filter → map → reduce → synthesize."""

    def __init__(self, llm, job_id: str, pool: AgentPool | None = None) -> None:
        super().__init__(llm, job_id)
        self._pool = pool or AgentPool(max_concurrency=10)

    async def run(
        self,
        df: pd.DataFrame,
        rule_content: str,
        step_number: int,
        rule_metadata: RuleMetadata,
        chunk_size: int = 300,
    ) -> ReviewReport:
        # Phase 1: Pre-filter (deterministic, code-based)
        det_pass, det_fail, needs_llm_df, det_notes = _prefilter(df, rule_content)
        logger.info(
            "review_prefilter",
            step=step_number,
            det_pass=len(det_pass),
            det_fail=len(det_fail),
            needs_llm=len(needs_llm_df),
            det_notes=det_notes,
        )

        # Phase 2: MAP (parallel LLM review per chunk)
        partial_reports: list[PartialReviewReport] = []
        if not needs_llm_df.empty:
            chunks = create_chunks(needs_llm_df, rule_metadata, chunk_size)
            tasks = [
                self._review_chunk(chunk_df, rule_content, step_number, chunk_meta.chunk_id, det_notes)
                for chunk_meta, chunk_df in chunks
            ]
            partial_reports = await self._pool.run_all(tasks)

        # Phase 3: REDUCE (code-based merge)
        reduced = _reduce(partial_reports, det_fail)

        # Phase 4: SYNTHESIZE (1 small LLM call)
        narrative, confidence, top_patterns = await self._synthesize(reduced, step_number)

        return ReviewReport(
            step_number=step_number,
            total_rows=len(df),
            passed_rows=len(det_pass) + reduced["llm_passed"],
            failed_rows=len(det_fail) + reduced["llm_failed"],
            deterministic_failures=len(det_fail),
            llm_failures=reduced["llm_failed"],
            failure_patterns=top_patterns,
            failed_row_indices=reduced["failed_indices"],
            narrative_summary=narrative,
            confidence_score=confidence,
        )

    async def _review_chunk(
        self,
        chunk_df: pd.DataFrame,
        rule_content: str,
        step_number: int,
        chunk_id: int,
        det_notes: str = "",
    ) -> PartialReviewReport:
        rows_json = json.dumps(
            chunk_df.to_dict(orient="records"), default=str, indent=2
        )
        prefilter_note = ""
        if det_notes:
            prefilter_note = f"""
NOTE: The following conditions have ALREADY been verified deterministically by code.
Do NOT re-check these — they are guaranteed correct:
{det_notes}

Focus your review ONLY on aspects NOT covered by the deterministic checks above.
If all conditions for this step were verified deterministically, mark all rows as PASSED.
"""
        user_msg = f"""Step {step_number} rules:
{rule_content}
{prefilter_note}
Rows to review:
{rows_json}

Check each row and return a JSON review report."""

        try:
            response = await self.llm.complete(
                system_prompt=REVIEW_SYSTEM_PROMPT,
                messages=[Message(role="user", content=user_msg)],
                temperature=0.0,
                max_tokens=4096,
            )
            return _parse_partial_report(response.content, chunk_id, len(chunk_df))
        except Exception as exc:
            logger.error("review_chunk_error", chunk_id=chunk_id, error=str(exc))
            return PartialReviewReport(
                chunk_id=chunk_id,
                total_rows=len(chunk_df),
                passed_rows=0,
            )

    async def _synthesize(
        self,
        reduced: dict,
        step_number: int,
    ) -> tuple[str, float, list[str]]:
        top_examples = reduced["failed_examples"][:10]
        stats = {
            "total": reduced["total_reviewed"],
            "failed": reduced["llm_failed"],
            "patterns": reduced["pattern_counts"],
        }
        user_msg = f"""Step {step_number} review stats:
{json.dumps(stats, indent=2)}

Top failure examples:
{json.dumps(top_examples, default=str, indent=2)}

Provide a narrative summary."""

        try:
            response = await self.llm.complete(
                system_prompt=SYNTHESIZE_SYSTEM_PROMPT,
                messages=[Message(role="user", content=user_msg)],
                temperature=0.0,
                max_tokens=1024,
            )
            data = json.loads(_strip_json(response.content))
            return (
                data.get("narrative_summary", ""),
                float(data.get("confidence_score", 1.0)),
                data.get("top_patterns", []),
            )
        except Exception as exc:
            logger.warning("review_synthesize_error", error=str(exc))
            top_patterns = list(reduced.get("pattern_counts", {}).keys())[:5]
            return "Review synthesis unavailable.", 0.5, top_patterns


def _prefilter(df: pd.DataFrame, rule_content: str = "") -> tuple[list[int], list[int], pd.DataFrame, str]:
    """Deterministic pre-filter: nulls, empty strings, and rule-based numeric checks.

    Returns (pass_indices, fail_indices, needs_llm_df, deterministic_notes).
    deterministic_notes describes what was already verified by code.
    """
    det_notes_parts: list[str] = []

    # Basic null/empty check — only flag rows where ALL values are null/empty,
    # not rows with a single empty optional column (which is valid data).
    all_na = df.isna().all(axis=1)
    all_empty_str = df.apply(
        lambda col: col.astype(str).str.strip() == "" if col.dtype == object else pd.Series(False, index=df.index)
    ).all(axis=1)
    issue_mask = all_na | all_empty_str

    fail_indices = issue_mask[issue_mask].index.tolist()
    pass_indices = issue_mask[~issue_mask].index.tolist()

    # Parse rule content for deterministic numeric thresholds
    rule_checks = _extract_rule_checks(rule_content, df)

    if rule_checks:
        clean_df = df.loc[pass_indices]
        for check in rule_checks:
            col, op, val, desc = check["col"], check["op"], check["val"], check["desc"]
            if col in clean_df.columns:
                numeric_col = pd.to_numeric(clean_df[col], errors="coerce")
                if op == ">":
                    violators = clean_df[numeric_col <= val].index.tolist()
                    det_notes_parts.append(f"All rows have {desc} > {val} (verified by code)")
                elif op == ">=":
                    violators = clean_df[numeric_col < val].index.tolist()
                    det_notes_parts.append(f"All rows have {desc} >= {val} (verified by code)")
                elif op == "<":
                    violators = clean_df[numeric_col >= val].index.tolist()
                    det_notes_parts.append(f"All rows have {desc} < {val} (verified by code)")
                elif op == "<=":
                    violators = clean_df[numeric_col > val].index.tolist()
                    det_notes_parts.append(f"All rows have {desc} <= {val} (verified by code)")
                else:
                    continue

                # Move violators from pass to fail
                violator_set = set(violators)
                pass_indices = [i for i in pass_indices if i not in violator_set]
                fail_indices.extend(violators)

    # Check for expected columns (e.g., "Eligible to vote" should exist)
    expected_cols = list(dict.fromkeys(_extract_expected_columns(rule_content)))  # deduplicate
    for col_name in expected_cols:
        if col_name in df.columns:
            det_notes_parts.append(f"Column '{col_name}' exists in the output")
            # Verify values if it's a Yes/No column
            unique_vals = df[col_name].dropna().unique()
            if set(str(v) for v in unique_vals) <= {"Yes", "No"}:
                det_notes_parts.append(f"Column '{col_name}' contains only 'Yes'/'No' values")

    needs_llm = df.loc[pass_indices].copy()
    det_notes = "\n".join(f"- {n}" for n in det_notes_parts) if det_notes_parts else ""
    return pass_indices, fail_indices, needs_llm, det_notes


def _extract_rule_checks(rule_content: str, df: pd.DataFrame) -> list[dict]:
    """Extract deterministic numeric threshold checks from rule content."""
    checks = []

    # Look for threshold patterns like "Value: `4000`" with "Comparison: strictly greater than (`>`)"
    threshold_match = re.search(r'\*\*Value\*\*:\s*`(\d+(?:\.\d+)?)`', rule_content)
    comparison_match = re.search(r'\*\*Comparison\*\*:\s*.*?`([><=!]+)`', rule_content)

    if threshold_match and comparison_match:
        val = float(threshold_match.group(1))
        op = comparison_match.group(1)

        # Find the numeric column that matches the intent
        # Look for keywords in the rule to identify intent
        intent_col = _find_intent_column(rule_content, df)
        if intent_col:
            checks.append({
                "col": intent_col,
                "op": op,
                "val": val,
                "desc": f"income/numeric column '{intent_col}'",
            })

    # Look for age threshold patterns like "age >= 18"
    age_match = re.search(r'age\s*([><=!]+)\s*(\d+)', rule_content, re.IGNORECASE)
    if age_match:
        op = age_match.group(1)
        val = float(age_match.group(2))
        age_col = _find_column_by_intent(df, ["age"])
        if age_col:
            checks.append({
                "col": age_col,
                "op": op,
                "val": val,
                "desc": f"age column '{age_col}'",
            })

    return checks


def _find_intent_column(rule_content: str, df: pd.DataFrame) -> str | None:
    """Find the DataFrame column that matches the rule's intent."""
    rule_lower = rule_content.lower()

    if any(kw in rule_lower for kw in ["income", "salary", "wage", "earning", "monetary"]):
        return _find_column_by_intent(df, ["income", "salary", "wage", "earning", "pay"])

    return None


def _find_column_by_intent(df: pd.DataFrame, keywords: list[str]) -> str | None:
    """Find a column by keyword match, or fall back to the best numeric column."""
    # First: exact/partial name match
    for col in df.columns:
        col_lower = col.lower()
        for kw in keywords:
            if kw in col_lower:
                return col

    # Fallback: find numeric columns with values in a plausible range
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if len(numeric_cols) == 1:
        return numeric_cols[0]

    # For income: look for columns with values typically > 1000
    if "income" in keywords or "salary" in keywords:
        for col in numeric_cols:
            median_val = df[col].median()
            if median_val > 1000:
                return col

    # For age: look for columns with values typically 0-120
    if "age" in keywords:
        for col in numeric_cols:
            median_val = df[col].median()
            if 0 < median_val < 120:
                return col

    return None


def _extract_expected_columns(rule_content: str) -> list[str]:
    """Extract column names that should exist in the output (new columns only)."""
    expected = []
    # Look for patterns like "column should always be named `X`" or "New `X` column"
    col_matches = re.findall(r'(?:named|column)\s+`([^`]+)`', rule_content)
    # Common input column names/patterns to skip
    skip_patterns = {"monthly income", "age", "monthly_income", "income", "name", "address"}
    for col_name in col_matches:
        if col_name.lower() not in skip_patterns and len(col_name) > 1:
            expected.append(col_name)
    return expected


def _reduce(
    partial_reports: list[PartialReviewReport], det_fail_indices: list[int]
) -> dict:
    all_failed = list(det_fail_indices)
    failed_examples = []
    pattern_counts: dict[str, int] = {}
    llm_passed = 0
    llm_failed = 0
    total_reviewed = 0

    for report in partial_reports:
        total_reviewed += report.total_rows
        llm_passed += report.passed_rows
        llm_failed += len(report.failed_rows)
        for failure in report.failed_rows:
            all_failed.append(failure.get("row_index", -1))
            failed_examples.append(failure)
            pattern = failure.get("pattern", "unknown")
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

    # Deduplicate
    seen = set()
    unique_failed = [i for i in all_failed if not (i in seen or seen.add(i))]

    return {
        "total_reviewed": total_reviewed,
        "llm_passed": llm_passed,
        "llm_failed": llm_failed,
        "failed_indices": unique_failed,
        "failed_examples": failed_examples[:50],
        "pattern_counts": pattern_counts,
    }


def _parse_partial_report(content: str, chunk_id: int, total_rows: int) -> PartialReviewReport:
    try:
        data = json.loads(_strip_json(content))
        return PartialReviewReport(
            chunk_id=chunk_id,
            total_rows=total_rows,
            passed_rows=data.get("passed_rows", 0),
            failed_rows=data.get("failed_rows", []),
            failure_patterns=data.get("failure_patterns", []),
        )
    except (json.JSONDecodeError, ValueError):
        return PartialReviewReport(chunk_id=chunk_id, total_rows=total_rows, passed_rows=0)


def _strip_json(text: str) -> str:
    if "```json" in text:
        text = text.split("```json", 1)[1].split("```", 1)[0]
    elif "```" in text:
        text = text.split("```", 1)[1].split("```", 1)[0]
    return text.strip()
