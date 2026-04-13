from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import JobRuleSnapshot, RuleVersion
from utils.logging import get_logger

logger = get_logger(__name__)


async def _get_latest_rule_version(
    session: AsyncSession,
    step_number: int,
) -> RuleVersion | None:
    result = await session.execute(
        select(RuleVersion)
        .where(RuleVersion.step_number == step_number)
        .order_by(RuleVersion.version.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_latest_custom_rule_version(
    session: AsyncSession,
    step_number: int,
) -> RuleVersion | None:
    result = await session.execute(
        select(RuleVersion)
        .where(RuleVersion.step_number == step_number)
        .where(RuleVersion.created_by != "system")
        .order_by(RuleVersion.version.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def seed_rules_from_files(session: AsyncSession, steps_dir: str = "./steps") -> None:
    """Seed or refresh rule versions from steps/*.md."""
    steps_path = Path(steps_dir)
    if not steps_path.exists():
        logger.warning("steps_dir_missing", path=str(steps_path))
        return

    for md_file in sorted(steps_path.glob("*.md")):
        try:
            step_number = int(md_file.stem)
        except ValueError:
            continue

        # Check if already seeded
        existing = await _get_latest_rule_version(session, step_number)

        content = md_file.read_text(encoding="utf-8")
        if existing:
            if existing.content == content:
                logger.debug("rule_already_seeded", step=step_number, version=existing.version)
                continue

            if existing.created_by != "system":
                # A user/chat/job-authored rule exists. Log a warning so
                # operators know the shipped file differs from the active rule.
                logger.warning(
                    "rule_seed_skipped_custom_override",
                    step=step_number,
                    active_version=existing.version,
                    active_created_by=existing.created_by,
                    file=str(md_file),
                    hint="Delete the custom rule version in the DB to allow reseeding",
                )
                continue

            rule = RuleVersion(
                step_number=step_number,
                version=existing.version + 1,
                content=content,
                created_by="system",
            )
            session.add(rule)
            logger.info(
                "rule_seeded_updated",
                step=step_number,
                version=rule.version,
                file=str(md_file),
            )
            continue

        rule = RuleVersion(
            step_number=step_number,
            version=1,
            content=content,
            created_by="system",
        )
        session.add(rule)
        logger.info("rule_seeded", step=step_number, file=str(md_file))

    await session.commit()


async def create_rule_version(
    session: AsyncSession,
    step_number: int,
    content: str,
    created_by: str = "user",
) -> RuleVersion:
    """Insert a new rule version (append-only)."""
    latest = await _get_latest_rule_version(session, step_number)
    next_version = (latest.version + 1) if latest else 1

    rule = RuleVersion(
        step_number=step_number,
        version=next_version,
        content=content,
        created_by=created_by,
    )
    session.add(rule)
    await session.commit()
    await session.refresh(rule)
    logger.info(
        "rule_version_created",
        step=step_number,
        version=next_version,
        created_by=created_by,
        content=content,
    )
    return rule


async def get_latest_rule(session: AsyncSession, step_number: int) -> RuleVersion | None:
    # Custom/chat/job-authored updates are treated as persistent overrides.
    # If one exists, it should stay active across app restarts instead of
    # being masked by a later system reseed from packaged step files.
    custom = await _get_latest_custom_rule_version(session, step_number)
    if custom is not None:
        return custom
    return await _get_latest_rule_version(session, step_number)


async def snapshot_rules_for_job(
    session: AsyncSession, job_id: str, step_numbers: list[int]
) -> dict[int, int]:
    """Freeze current rule versions for a job. Returns {step_number: rule_version_id}."""
    snapshot_ids: dict[int, int] = {}

    for step_number in step_numbers:
        rule = await get_latest_rule(session, step_number)
        if rule is None:
            raise ValueError(f"No rule found for step {step_number}")

        snapshot = JobRuleSnapshot(
            job_id=job_id,
            step_number=step_number,
            rule_version_id=rule.id,
        )
        session.add(snapshot)
        snapshot_ids[step_number] = rule.id

    await session.commit()
    logger.info("rules_snapshotted", job_id=job_id, steps=step_numbers)
    return snapshot_ids


async def get_rule_by_version_id(
    session: AsyncSession, version_id: int
) -> RuleVersion | None:
    result = await session.execute(
        select(RuleVersion).where(RuleVersion.id == version_id)
    )
    return result.scalar_one_or_none()
