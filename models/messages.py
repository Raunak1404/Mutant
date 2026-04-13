from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class Message(BaseModel):
    role: Literal["user", "assistant", "system"]
    content: str


class LLMResponse(BaseModel):
    content: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    cached: bool = False


class ProgressEvent(BaseModel):
    job_id: str
    event_type: str   # "step_started" | "step_completed" | "chunk_done" | "review_done" | "awaiting_feedback" | "completed" | "failed"
    step_number: int | None = None
    message: str = ""
    data: dict = {}
