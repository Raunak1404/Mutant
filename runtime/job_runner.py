from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import select

from config.settings import Settings
from core.orchestrator import Orchestrator
from core.progress import ProgressPublisher
from db.models import JobStatus
from runtime.bootstrap import close_runtime_services, create_runtime_services
from utils.logging import get_logger

logger = get_logger(__name__)

_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()


def _track_background_task(task: asyncio.Task[Any]) -> None:
    _BACKGROUND_TASKS.add(task)

    def _done(completed: asyncio.Task[Any]) -> None:
        _BACKGROUND_TASKS.discard(completed)
        try:
            completed.result()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("background_task_failed", error=str(exc))

    task.add_done_callback(_done)


async def _mark_job_failed(session_factory, job_id: str, error: str) -> None:
    async with session_factory() as session:
        result = await session.execute(
            select(JobStatus).where(JobStatus.job_id == job_id)
        )
        status = result.scalar_one_or_none()
        if not status:
            return
        status.state = "failed"
        status.error_message = error
        await session.commit()


async def run_process_job(
    job_id: str,
    input_storage_keys: dict[str, str],
    *,
    settings: Settings | None = None,
) -> None:
    settings = settings or Settings()
    services = await create_runtime_services(settings)
    progress = ProgressPublisher(services.redis)

    try:
        async with services.session_factory() as session:
            orchestrator = Orchestrator(
                llm=services.llm,
                storage=services.storage,
                session=session,
                progress=progress,
                execution_service_url=settings.EXECUTION_SERVICE_URL,
                chunk_size=settings.CHUNK_SIZE_ROWS,
                review_chunk_size=settings.REVIEW_CHUNK_SIZE_ROWS,
                max_concurrency=settings.MAX_WORKER_CONCURRENCY,
                cache=services.cache,
                libraries_dir=settings.LIBRARIES_DIR,
            )
            await orchestrator.process_job(
                job_id=job_id,
                input_storage_keys=input_storage_keys,
                step_numbers=[1, 2, 3, 4, 5, 6],
            )
    except Exception as exc:
        logger.error("process_job_runner_error", job_id=job_id, error=str(exc))
        await _mark_job_failed(services.session_factory, job_id, str(exc))
        await progress.publish_failed(job_id, str(exc))
        raise
    finally:
        await close_runtime_services(services)


async def run_resume_job(
    job_id: str,
    feedback_answers: list[dict[str, Any]],
    step_numbers: list[int],
    *,
    settings: Settings | None = None,
) -> None:
    from models.results import UserFeedback

    settings = settings or Settings()
    answers = [UserFeedback(**fb) for fb in feedback_answers]
    services = await create_runtime_services(settings)
    progress = ProgressPublisher(services.redis)

    try:
        async with services.session_factory() as session:
            orchestrator = Orchestrator(
                llm=services.llm,
                storage=services.storage,
                session=session,
                progress=progress,
                execution_service_url=settings.EXECUTION_SERVICE_URL,
                chunk_size=settings.CHUNK_SIZE_ROWS,
                review_chunk_size=settings.REVIEW_CHUNK_SIZE_ROWS,
                max_concurrency=settings.MAX_WORKER_CONCURRENCY,
                cache=services.cache,
                libraries_dir=settings.LIBRARIES_DIR,
            )
            await orchestrator.resume_job(job_id, answers, step_numbers)
    except Exception as exc:
        logger.error("resume_job_runner_error", job_id=job_id, error=str(exc))
        await _mark_job_failed(services.session_factory, job_id, str(exc))
        await progress.publish_failed(job_id, str(exc))
        raise
    finally:
        await close_runtime_services(services)


async def schedule_process_job(job_id: str, input_storage_keys: dict[str, str]) -> None:
    settings = Settings()
    if settings.JOB_RUNNER == "taskiq":
        from tasks.process_job import process_job_task

        await process_job_task.kiq(job_id=job_id, input_storage_keys=input_storage_keys)
        return

    task = asyncio.create_task(run_process_job(job_id, input_storage_keys, settings=settings))
    _track_background_task(task)


async def schedule_resume_job(
    job_id: str,
    feedback_answers: list[dict[str, Any]],
    step_numbers: list[int],
) -> None:
    settings = Settings()
    if settings.JOB_RUNNER == "taskiq":
        from tasks.resume_job import resume_job_task

        await resume_job_task.kiq(
            job_id=job_id,
            feedback_answers=feedback_answers,
            step_numbers=step_numbers,
        )
        return

    task = asyncio.create_task(
        run_resume_job(
            job_id=job_id,
            feedback_answers=feedback_answers,
            step_numbers=step_numbers,
            settings=settings,
        )
    )
    _track_background_task(task)
