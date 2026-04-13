from __future__ import annotations

from pydantic import BaseModel, Field


class ProposedChange(BaseModel):
    step_number: int
    change_type: str  # "rule" | "code"
    description: str


class ChatRequest(BaseModel):
    session_id: str
    message: str


class ChatResponse(BaseModel):
    session_id: str
    message: str
    proposed_changes: list[ProposedChange] = Field(default_factory=list)
    needs_confirmation: bool = False
    questions: list[str] = Field(default_factory=list)
    applied_proposals: list[ProposedChange] = Field(default_factory=list)


class ChatConfirmRequest(BaseModel):
    session_id: str
    job_id: str | None = None
    message: str = "confirmed"


class ChatHistoryItem(BaseModel):
    role: str
    content: str
    metadata_json: str = "{}"
    created_at: str


class StepRuleSummary(BaseModel):
    step_number: int
    rule_content: str
    code_functions: list[str] = Field(default_factory=list)


class ChatDecision(BaseModel):
    proposed_changes: list[ProposedChange] = Field(default_factory=list)
    needs_confirmation: bool = False
    questions: list[str] = Field(default_factory=list)
