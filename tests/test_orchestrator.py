from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from core.orchestrator import Orchestrator
from db.models import Base, JobCheckpoint, JobStatus


class _DummyProgress:
    def __init__(self) -> None:
        self.completed: list[tuple[str, str]] = []

    async def publish_completed(self, job_id: str, output_key: str) -> None:
        self.completed.append((job_id, output_key))

    async def publish_failed(self, job_id: str, error: str) -> None:  # pragma: no cover - defensive stub
        raise AssertionError(f"Unexpected failure publish for {job_id}: {error}")


async def _get_checkpoint(session: AsyncSession, job_id: str) -> JobCheckpoint | None:
    result = await session.execute(
        select(JobCheckpoint).where(JobCheckpoint.job_id == job_id)
    )
    return result.scalar_one_or_none()


async def _get_status(session: AsyncSession, job_id: str) -> JobStatus | None:
    result = await session.execute(
        select(JobStatus).where(JobStatus.job_id == job_id)
    )
    return result.scalar_one_or_none()


def _build_orchestrator(session: AsyncSession, progress: _DummyProgress) -> Orchestrator:
    return Orchestrator(
        llm=None,
        storage=None,
        session=session,
        progress=progress,
        execution_service_url="embedded://local",
    )


@pytest.mark.asyncio
async def test_finalize_marks_feedback_checkpoint_completed() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with Session() as session:
        job_id = "job-123"
        session.add(
            JobStatus(
                job_id=job_id,
                state="awaiting_feedback",
            )
        )
        session.add(
            JobCheckpoint(
                job_id=job_id,
                step_number=1,
                status="AWAITING_FEEDBACK",
                questions_json="[]",
                completed_steps_json="[1]",
                failed_row_indices_json="{}",
                rule_snapshot_ids_json="{}",
                storage_keys_json="{}",
            )
        )
        await session.commit()

        progress = _DummyProgress()
        orchestrator = _build_orchestrator(session, progress)
        output_key = f"jobs/{job_id}/step_6/output.xlsx"

        await orchestrator._finalize(job_id, output_key)

        checkpoint = await _get_checkpoint(session, job_id)
        status = await _get_status(session, job_id)

        assert checkpoint is not None
        assert checkpoint.status == "COMPLETED"
        assert status is not None
        assert status.state == "completed"
        assert status.output_storage_key == output_key
        assert progress.completed == [(job_id, output_key)]

    await engine.dispose()
