from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import CodeVersion, JobCodeSnapshot
from utils.logging import get_logger

logger = get_logger(__name__)


async def _get_latest_code_version(
    session: AsyncSession,
    step_number: int,
) -> CodeVersion | None:
    result = await session.execute(
        select(CodeVersion)
        .where(CodeVersion.step_number == step_number)
        .order_by(CodeVersion.version.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_latest_custom_code_version(
    session: AsyncSession,
    step_number: int,
) -> CodeVersion | None:
    result = await session.execute(
        select(CodeVersion)
        .where(CodeVersion.step_number == step_number)
        .where(CodeVersion.created_by != "system")
        .order_by(CodeVersion.version.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def seed_code_from_files(session: AsyncSession, steps_dir: str = "./steps") -> None:
    """Seed or refresh code versions from steps/step*_logic.py."""
    steps_path = Path(steps_dir)
    if not steps_path.exists():
        logger.warning("steps_dir_missing", path=str(steps_path))
        return

    for py_file in sorted(steps_path.glob("step*_logic.py")):
        # Extract step number from filename like "step1_logic.py" -> 1
        try:
            stem = py_file.stem  # "step1_logic"
            step_number = int(stem.replace("step", "").replace("_logic", ""))
        except ValueError:
            continue

        # Check if already seeded
        existing = await _get_latest_code_version(session, step_number)

        content = py_file.read_text(encoding="utf-8")
        if existing:
            if existing.content == content:
                logger.debug("code_already_seeded", step=step_number, version=existing.version)
                continue

            if existing.created_by != "system":
                logger.warning(
                    "code_seed_skipped_custom_override",
                    step=step_number,
                    active_version=existing.version,
                    active_created_by=existing.created_by,
                    file=str(py_file),
                    hint="Delete the custom code version in the DB to allow reseeding",
                )
                continue

            code = CodeVersion(
                step_number=step_number,
                version=existing.version + 1,
                content=content,
                parent_version_id=existing.id,
                changed_function=None,
                created_by="system",
            )
            session.add(code)
            logger.info(
                "code_seeded_updated",
                step=step_number,
                version=code.version,
                parent_version_id=existing.id,
                file=str(py_file),
            )
            continue

        code = CodeVersion(
            step_number=step_number,
            version=1,
            content=content,
            parent_version_id=None,
            changed_function=None,
            created_by="system",
        )
        session.add(code)
        logger.info("code_seeded", step=step_number, file=str(py_file))

    await session.commit()


async def create_code_version(
    session: AsyncSession,
    step_number: int,
    content: str,
    parent_version_id: int | None = None,
    changed_function: str | None = None,
    created_by: str = "user",
) -> CodeVersion:
    """Insert a new code version (append-only)."""
    latest = await _get_latest_code_version(session, step_number)
    next_version = (latest.version + 1) if latest else 1

    code = CodeVersion(
        step_number=step_number,
        version=next_version,
        content=content,
        parent_version_id=parent_version_id,
        changed_function=changed_function,
        created_by=created_by,
    )
    session.add(code)
    await session.commit()
    await session.refresh(code)
    logger.info("code_version_created", step=step_number, version=next_version)
    return code


async def get_latest_code(session: AsyncSession, step_number: int) -> CodeVersion | None:
    # Custom/chat/job-authored updates are treated as persistent overrides.
    custom = await _get_latest_custom_code_version(session, step_number)
    if custom is not None:
        return custom
    return await _get_latest_code_version(session, step_number)


async def snapshot_code_for_job(
    session: AsyncSession, job_id: str, step_numbers: list[int]
) -> dict[int, int]:
    """Freeze current code versions for a job. Returns {step_number: code_version_id}.
    Only snapshots steps that have native code files."""
    snapshot_ids: dict[int, int] = {}

    for step_number in step_numbers:
        code = await get_latest_code(session, step_number)
        if code is None:
            # No native code for this step — skip (it will use LLM strategies)
            continue

        snapshot = JobCodeSnapshot(
            job_id=job_id,
            step_number=step_number,
            code_version_id=code.id,
        )
        session.add(snapshot)
        snapshot_ids[step_number] = code.id

    if snapshot_ids:
        await session.commit()
        logger.info("code_snapshotted", job_id=job_id, steps=list(snapshot_ids.keys()))

    return snapshot_ids


async def get_code_by_version_id(
    session: AsyncSession, version_id: int
) -> CodeVersion | None:
    result = await session.execute(
        select(CodeVersion).where(CodeVersion.id == version_id)
    )
    return result.scalar_one_or_none()
