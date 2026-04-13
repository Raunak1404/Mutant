from __future__ import annotations

from typing import Any

from runtime.job_runner import run_resume_job
from tasks.broker import broker


@broker.task
async def resume_job_task(
    job_id: str,
    feedback_answers: list[dict[str, Any]],
    step_numbers: list[int],
) -> None:
    """Resume job after user feedback. Runs in Taskiq worker process."""
    await run_resume_job(
        job_id=job_id,
        feedback_answers=feedback_answers,
        step_numbers=step_numbers,
    )
