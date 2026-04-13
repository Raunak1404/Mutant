from __future__ import annotations

import json
import uuid
from typing import Any

from llm.provider import LLMProvider
from models.messages import Message
from models.results import FeedbackQuestion, FeedbackSuggestion, ReviewReport
from utils.logging import get_logger

logger = get_logger(__name__)

QUESTION_SYSTEM_PROMPT = """You are a diagnostic assistant for a data transformation pipeline.
When processing fails, you analyze the failure in depth and generate questions with
actionable suggestions the user can click to resolve the issue.

Your job is to:
1. Analyze the error/failure deeply — identify root cause, not just symptoms.
2. For each issue, write a clear analysis summary explaining what went wrong and why.
3. Generate 2-3 specific, clickable suggestions the user can pick from.
4. Each suggestion must be a complete, self-contained instruction that can be used
   to update the step's rules or code.

Common failure types you should handle:
- **Column mismatch**: A column was renamed, removed, or added in the input data.
  Suggest specific column mappings based on available columns.
- **Data type error**: A column has unexpected data types (e.g., strings where numbers expected).
  Suggest type conversion or filtering approaches.
- **Code logic error**: The processing code has a bug or edge case.
  Suggest specific code fixes.
- **Rule mismatch**: The transformation rules don't match the actual data structure.
  Suggest rule updates.

Return a JSON array of objects:
[
  {
    "step_number": int,
    "question_text": "Clear description of the issue found",
    "failure_pattern": "The specific error pattern",
    "analysis_summary": "Detailed analysis: what went wrong, what was expected vs actual, and why this happened",
    "suggestions": [
      {
        "label": "Short action label (5-12 words max)",
        "description": "Complete instruction for fixing this issue. Be specific — mention exact column names, exact code changes, or exact rule modifications needed. This text will be used by an LLM to update the rules/code, so make it actionable."
      }
    ]
  }
]

IMPORTANT:
- Each question MUST have 2-3 suggestions.
- Suggestion labels should be concise action phrases (like button text).
- Suggestion descriptions should be detailed enough to drive an automated fix.
- The analysis_summary should help the user understand what happened without being overly technical.
- If input columns are mentioned in the error, use them to suggest specific column mappings.
- Focus on the most impactful failures first (max 5 questions)."""


async def generate_questions(
    llm: LLMProvider,
    reports: list[ReviewReport],
    aggregated: dict[str, Any],
) -> list[FeedbackQuestion]:
    """Generate user questions with clickable suggestions from review failures."""
    summary = json.dumps(aggregated, default=str, indent=2)

    step_details = []
    for report in reports:
        if report.failed_rows > 0:
            detail = (
                f"Step {report.step_number}: {report.failed_rows}/{report.total_rows} rows failed.\n"
                f"  Patterns: {report.failure_patterns}\n"
                f"  Summary: {report.narrative_summary}"
            )

            # Include structured error context when available
            if report.error_context:
                ctx = report.error_context
                error_type = ctx.get("error_type", "unknown")
                detail += f"\n  Error type: {error_type}"

                if error_type == "native_code_failure":
                    input_cols = ctx.get("input_columns", [])
                    if input_cols:
                        detail += f"\n  Available input columns: {input_cols}"
                    error_msg = ctx.get("error_message", "")
                    if error_msg:
                        detail += f"\n  Error message: {error_msg}"
                    code_snippet = ctx.get("code_snippet", "")
                    if code_snippet:
                        detail += f"\n  Code snippet (first 3000 chars):\n{code_snippet[:3000]}"

                elif error_type in ("step_exception", "resume_exception"):
                    exc_class = ctx.get("exception_class", "")
                    exc_msg = ctx.get("exception_message", "")
                    if exc_class:
                        detail += f"\n  Exception: {exc_class}: {exc_msg}"

            step_details.append(detail)

    if not step_details:
        logger.info("no_failures_no_questions")
        return []

    user_msg = f"""Review summary:
{summary}

Step failure details:
{chr(10).join(step_details)}

Analyze the failures and generate questions with actionable suggestions."""

    try:
        response = await llm.complete(
            system_prompt=QUESTION_SYSTEM_PROMPT,
            messages=[Message(role="user", content=user_msg)],
            temperature=0.0,
            max_tokens=4096,
        )
        questions = _parse_questions(response.content)
        if questions:
            return questions
        logger.warning("question_gen_empty_fallback")
        return _build_fallback_questions(reports)
    except Exception as exc:
        logger.error("question_gen_error", error=str(exc))
        return _build_fallback_questions(reports)


def _parse_questions(content: str) -> list[FeedbackQuestion]:
    if "```json" in content:
        content = content.split("```json", 1)[1].split("```", 1)[0]
    elif "```" in content:
        content = content.split("```", 1)[1].split("```", 1)[0]

    try:
        data = json.loads(content.strip())
        questions = []
        for item in data:
            # Parse suggestions
            raw_suggestions = item.get("suggestions", [])
            suggestions = []
            for s in raw_suggestions:
                label = (s.get("label") or "Apply fix").strip()
                desc = (s.get("description") or "").strip()
                if not desc:
                    continue  # skip suggestions with no actionable description
                suggestions.append(FeedbackSuggestion(
                    suggestion_id=str(uuid.uuid4()),
                    label=label,
                    description=desc,
                ))

            questions.append(FeedbackQuestion(
                question_id=str(uuid.uuid4()),
                step_number=item.get("step_number", 0),
                question_text=item.get("question_text", ""),
                failure_pattern=item.get("failure_pattern", ""),
                example_rows=item.get("example_rows", []),
                suggestions=suggestions,
                analysis_summary=item.get("analysis_summary", ""),
            ))
        return questions
    except (json.JSONDecodeError, KeyError, ValueError, AttributeError, TypeError) as exc:
        logger.warning("question_parse_error", error=str(exc))
        return []


def _build_fallback_questions(reports: list[ReviewReport]) -> list[FeedbackQuestion]:
    """Generate deterministic fallback questions with basic suggestions."""
    questions: list[FeedbackQuestion] = []
    for report in reports:
        if report.failed_rows <= 0:
            continue

        pattern = report.failure_patterns[0] if report.failure_patterns else "Unexpected output mismatch"

        # Build context-aware suggestions from error_context
        suggestions = _build_fallback_suggestions(report)

        analysis = report.narrative_summary or (
            f"Step {report.step_number} encountered {report.failed_rows} failures "
            f"with pattern: {pattern}"
        )

        questions.append(
            FeedbackQuestion(
                question_id=str(uuid.uuid4()),
                step_number=report.step_number,
                question_text=(
                    f"Step {report.step_number} has {report.failed_rows} failed rows. "
                    f"How should the pipeline handle this: {pattern}?"
                ),
                failure_pattern=pattern,
                example_rows=[],
                suggestions=suggestions,
                analysis_summary=analysis,
            )
        )

    return questions[:5]


def _build_fallback_suggestions(report: ReviewReport) -> list[FeedbackSuggestion]:
    """Build basic suggestions from ReviewReport error context."""
    suggestions = []
    ctx = report.error_context

    if ctx.get("error_type") == "native_code_failure":
        error_msg = ctx.get("error_message", "")
        input_cols = ctx.get("input_columns", [])

        # Try to detect column-related errors
        if "KeyError" in error_msg or "not in index" in error_msg.lower():
            suggestions.append(FeedbackSuggestion(
                suggestion_id=str(uuid.uuid4()),
                label="Update code to match new columns",
                description=(
                    f"Update the step code to match the current input columns: {input_cols}. "
                    f"The error was: {error_msg}"
                ),
            ))
            suggestions.append(FeedbackSuggestion(
                suggestion_id=str(uuid.uuid4()),
                label="Add flexible column matching",
                description=(
                    "Add flexible column matching logic that tries alternative column names "
                    "and handles missing columns gracefully instead of failing."
                ),
            ))
        else:
            suggestions.append(FeedbackSuggestion(
                suggestion_id=str(uuid.uuid4()),
                label="Fix the code error",
                description=(
                    f"Fix the step code to handle this error: {error_msg}. "
                    f"Input columns available: {input_cols}"
                ),
            ))

    if not suggestions:
        # Generic fallback suggestions
        pattern = report.failure_patterns[0] if report.failure_patterns else "the issue"
        suggestions.append(FeedbackSuggestion(
            suggestion_id=str(uuid.uuid4()),
            label="Update rules to handle this pattern",
            description=f"Update the step rules to correctly handle: {pattern}",
        ))
        suggestions.append(FeedbackSuggestion(
            suggestion_id=str(uuid.uuid4()),
            label="Skip failing rows and continue",
            description=(
                "Modify the rules to skip rows that don't match the expected pattern "
                "and continue processing the remaining data."
            ),
        ))

    return suggestions
