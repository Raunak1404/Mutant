from __future__ import annotations

from runtime.job_runner import run_process_job
from tasks.broker import broker


@broker.task
async def process_job_task(job_id: str, input_storage_keys: dict[str, str]) -> None:
    """Initial job processing task. Runs in Taskiq worker process.

    Args:
        job_id: Unique job identifier.
        input_storage_keys: {"sap": "jobs/.../sap_input.xlsx", "esjc": "jobs/.../esjc_input.xlsx"}
    """
    await run_process_job(job_id=job_id, input_storage_keys=input_storage_keys)
