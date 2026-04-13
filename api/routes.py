from __future__ import annotations

import hashlib
import io
import json
import uuid

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_cache, get_llm, get_session, get_settings, get_storage
from api.schemas import (
    FeedbackSubmitRequest,
    FeedbackSubmitResponse,
    JobQuestionsResponse,
    JobResultResponse,
    JobStatusResponse,
    JobUploadResponse,
)
from config.settings import Settings
from db.models import JobCheckpoint, JobStatus
from models.results import FeedbackQuestion
from runtime.job_runner import schedule_process_job, schedule_resume_job
from storage.backend import StorageBackend

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/upload", response_model=JobUploadResponse)
async def upload_job(
    sap_file: UploadFile = File(...),
    esjc_file: UploadFile = File(...),
    settings: Settings = Depends(get_settings),
    storage: StorageBackend = Depends(get_storage),
    session: AsyncSession = Depends(get_session),
):
    """Upload SAP and ESJC Excel files and start processing job."""
    for f, name in [(sap_file, "SAP"), (esjc_file, "ESJC")]:
        if not f.filename or not f.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail=f"{name} file must be .xlsx or .xls")

    job_id = str(uuid.uuid4())
    sap_key = f"jobs/{job_id}/sap_input.xlsx"
    esjc_key = f"jobs/{job_id}/esjc_input.xlsx"

    sap_content = await sap_file.read()
    esjc_content = await esjc_file.read()

    # Guard against excessively large uploads (50 MB per file)
    max_upload_bytes = 50 * 1024 * 1024
    if len(sap_content) > max_upload_bytes:
        raise HTTPException(status_code=413, detail="SAP file too large (max 50 MB)")
    if len(esjc_content) > max_upload_bytes:
        raise HTTPException(status_code=413, detail="ESJC file too large (max 50 MB)")

    await storage.upload(sap_key, sap_content)
    await storage.upload(esjc_key, esjc_content)

    # Compute column fingerprints for drift detection
    column_fingerprints = {}
    for label, data in [("sap", sap_content), ("esjc", esjc_content)]:
        try:
            cols = sorted(pd.read_excel(io.BytesIO(data), nrows=0).columns.tolist())
            column_fingerprints[label] = hashlib.sha256(
                json.dumps(cols, sort_keys=True).encode()
            ).hexdigest()[:16]
        except Exception:
            column_fingerprints[label] = ""

    # Store both keys as JSON in input_storage_key
    input_keys = {"sap": sap_key, "esjc": esjc_key}
    job_status = JobStatus(
        job_id=job_id,
        state="queued",
        input_storage_key=json.dumps({
            **input_keys,
            "_column_fingerprints": column_fingerprints,
        }),
    )
    session.add(job_status)
    await session.commit()

    # Enqueue the processing task
    await schedule_process_job(job_id=job_id, input_storage_keys=input_keys)

    return JobUploadResponse(job_id=job_id, sap_storage_key=sap_key, esjc_storage_key=esjc_key)


@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_status(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(JobStatus).where(JobStatus.job_id == job_id)
    )
    status = result.scalar_one_or_none()
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(
        job_id=status.job_id,
        state=status.state,
        created_at=status.created_at,
        updated_at=status.updated_at,
        error_message=status.error_message,
        output_storage_key=status.output_storage_key,
    )


@router.get("/{job_id}/questions", response_model=JobQuestionsResponse)
async def get_questions(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(JobCheckpoint).where(JobCheckpoint.job_id == job_id)
    )
    checkpoint = result.scalar_one_or_none()
    if not checkpoint:
        raise HTTPException(status_code=404, detail="No checkpoint found for job")
    if checkpoint.status != "AWAITING_FEEDBACK":
        raise HTTPException(status_code=409, detail=f"Job is not awaiting feedback (status: {checkpoint.status})")

    questions_data = json.loads(checkpoint.questions_json)
    questions = [FeedbackQuestion(**q) for q in questions_data]

    return JobQuestionsResponse(job_id=job_id, questions=questions)


@router.post("/{job_id}/feedback", response_model=FeedbackSubmitResponse)
async def submit_feedback(
    job_id: str,
    request: FeedbackSubmitRequest,
    session: AsyncSession = Depends(get_session),
    storage: StorageBackend = Depends(get_storage),
):
    result = await session.execute(
        select(JobCheckpoint).where(JobCheckpoint.job_id == job_id)
    )
    checkpoint = result.scalar_one_or_none()
    if not checkpoint or checkpoint.status != "AWAITING_FEEDBACK":
        raise HTTPException(status_code=409, detail="Job is not awaiting feedback")

    # Use the full step list so resume re-runs failed steps AND downstream
    storage_keys = json.loads(checkpoint.storage_keys_json)
    all_step_numbers = storage_keys.get("_all_step_numbers")
    if not all_step_numbers:
        all_step_numbers = json.loads(checkpoint.completed_steps_json)

    # Optimistically update state so polling clients see "resuming"
    job_result = await session.execute(
        select(JobStatus).where(JobStatus.job_id == job_id)
    )
    job_status = job_result.scalar_one_or_none()
    if job_status:
        job_status.state = "resuming"
        await session.commit()

    # Enqueue resume task
    await schedule_resume_job(
        job_id=job_id,
        feedback_answers=[fb.model_dump() for fb in request.answers],
        step_numbers=all_step_numbers,
    )

    return FeedbackSubmitResponse(job_id=job_id)


@router.get("/{job_id}/result", response_model=JobResultResponse)
async def get_result(
    job_id: str,
    session: AsyncSession = Depends(get_session),
    storage: StorageBackend = Depends(get_storage),
):
    result = await session.execute(
        select(JobStatus).where(JobStatus.job_id == job_id)
    )
    status = result.scalar_one_or_none()
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    if status.state != "completed":
        raise HTTPException(status_code=409, detail=f"Job not completed (state: {status.state})")
    if not status.output_storage_key:
        raise HTTPException(status_code=500, detail="Output file not found")

    download_url = await storage.get_presigned_url(status.output_storage_key)

    return JobResultResponse(job_id=job_id, download_url=download_url)


@router.get("/{job_id}/download")
async def download_result(
    job_id: str,
    session: AsyncSession = Depends(get_session),
    storage: StorageBackend = Depends(get_storage),
):
    """Download the output ZIP file containing both SAP and ESJC results."""
    result = await session.execute(
        select(JobStatus).where(JobStatus.job_id == job_id)
    )
    status = result.scalar_one_or_none()
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    if status.state != "completed":
        raise HTTPException(status_code=409, detail=f"Job not completed (state: {status.state})")
    if not status.output_storage_key:
        raise HTTPException(status_code=500, detail="Output file not found")

    data = await storage.download(status.output_storage_key)

    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=Mutant_Output_{job_id[:8]}.zip"},
    )
