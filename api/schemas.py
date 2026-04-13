from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from models.results import FeedbackQuestion, UserFeedback


class JobUploadResponse(BaseModel):
    job_id: str
    message: str = "Job queued"
    sap_storage_key: str
    esjc_storage_key: str


class JobStatusResponse(BaseModel):
    job_id: str
    state: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    error_message: str | None = None
    output_storage_key: str | None = None


class JobQuestionsResponse(BaseModel):
    job_id: str
    questions: list[FeedbackQuestion]


class FeedbackSubmitRequest(BaseModel):
    answers: list[UserFeedback]


class FeedbackSubmitResponse(BaseModel):
    job_id: str
    message: str = "Feedback received, job resumed"


class JobResultResponse(BaseModel):
    job_id: str
    download_url: str
