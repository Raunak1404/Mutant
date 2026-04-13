from __future__ import annotations

import json

from sqlalchemy.ext.asyncio import AsyncSession

from db.rules import create_rule_version, get_rule_by_version_id
from llm.provider import LLMProvider
from models.messages import Message
from models.results import FeedbackQuestion, UserFeedback
from utils.logging import get_logger

logger = get_logger(__name__)

RULE_UPDATE_SYSTEM_PROMPT = """You are a rules editor for a data transformation system.
Given existing transformation rules and user feedback addressing specific failures,
produce updated rules that incorporate the user's guidance.

Preserve the existing rule structure and format (markdown).
Add or modify rules only as needed to address the specific failures mentioned.
Return ONLY the updated rules markdown, no explanation."""


async def update_rules_from_feedback(
    session: AsyncSession,
    llm: LLMProvider,
    job_id: str,
    step_rule_snapshot_ids: dict[int, int],
    questions: list[FeedbackQuestion],
    answers: list[UserFeedback],
) -> dict[int, int]:
    """
    Update rule versions based on user feedback.
    Returns {step_number: new_rule_version_id}.
    """
    answer_map = {fb.question_id: fb.answer for fb in answers}

    # Group questions by step
    step_questions: dict[int, list[tuple[FeedbackQuestion, str]]] = {}
    for q in questions:
        answer = answer_map.get(q.question_id, "")
        if not answer or not answer.strip():
            continue
        step_questions.setdefault(q.step_number, []).append((q, answer))

    new_version_ids: dict[int, int] = {}

    for step_number, qa_pairs in step_questions.items():
        version_id = step_rule_snapshot_ids.get(step_number)
        if not version_id:
            logger.warning("no_snapshot_for_step", step=step_number)
            continue

        rule_version = await get_rule_by_version_id(session, version_id)
        if not rule_version:
            logger.warning("rule_version_not_found", version_id=version_id)
            continue

        qa_text = "\n".join(
            f"Q: {q.question_text}\nA: {answer}\nPattern: {q.failure_pattern}"
            for q, answer in qa_pairs
        )

        user_msg = f"""Current rules for step {step_number}:
{rule_version.content}

User feedback addressing failures:
{qa_text}

Produce updated rules incorporating this feedback."""

        try:
            response = await llm.complete(
                system_prompt=RULE_UPDATE_SYSTEM_PROMPT,
                messages=[Message(role="user", content=user_msg)],
                temperature=0.0,
                max_tokens=4096,
            )
            new_content = response.content.strip()

            # Skip insert if the LLM produced identical content — avoids
            # creating a redundant version that would overwrite a user's
            # explicit chat-confirmed rule change.
            if new_content == rule_version.content:
                logger.info(
                    "rule_update_skipped_identical",
                    job_id=job_id,
                    step=step_number,
                    version=rule_version.version,
                )
                new_version_ids[step_number] = rule_version.id
                continue

            new_rule = await create_rule_version(
                session,
                step_number=step_number,
                content=new_content,
                created_by=job_id,
            )
            new_version_ids[step_number] = new_rule.id
            logger.info(
                "rule_updated",
                job_id=job_id,
                step=step_number,
                old_version=rule_version.version,
                new_version=new_rule.version,
            )
        except Exception as exc:
            logger.error("rule_update_error", step=step_number, error=str(exc))

    return new_version_ids
