from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .enums import RowStatus, StepStatus, ProcessingStrategy, DependencyType


class RowResult(BaseModel):
    row_index: int
    status: RowStatus
    original_data: dict[str, Any]
    transformed_data: dict[str, Any] | None = None
    failure_reason: str | None = None


class PartialReviewReport(BaseModel):
    """Output of a single map-phase LLM review chunk."""
    chunk_id: int
    total_rows: int
    failed_rows: list[dict[str, Any]] = Field(default_factory=list)
    passed_rows: int = 0
    failure_patterns: list[str] = Field(default_factory=list)


class ReviewReport(BaseModel):
    """Fully reduced + synthesized review result for one step."""
    step_number: int
    total_rows: int
    passed_rows: int
    failed_rows: int
    deterministic_failures: int = 0
    llm_failures: int = 0
    failure_patterns: list[str] = Field(default_factory=list)
    failed_row_indices: list[int] = Field(default_factory=list)
    narrative_summary: str = ""
    confidence_score: float = 1.0
    error_context: dict[str, Any] = Field(default_factory=dict)


class StepResult(BaseModel):
    step_number: int
    status: StepStatus
    strategy: ProcessingStrategy | None = None
    total_rows: int = 0
    successful_rows: int = 0
    failed_rows: int = 0
    output_storage_key: str | None = None
    review_report: ReviewReport | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class RuleMetadata(BaseModel):
    """LLM classification output for a step's rules."""
    dependency_type: DependencyType = DependencyType.NONE
    dependency_scope: str | None = None   # e.g., "backward_3"
    group_key: str | None = None          # e.g., "invoice_id"
    mechanical: bool = False              # True = pandas can handle without LLM


class FeedbackSuggestion(BaseModel):
    """A clickable suggestion offered to the user for resolving a failure."""
    suggestion_id: str
    label: str
    description: str


class FeedbackQuestion(BaseModel):
    question_id: str
    step_number: int
    question_text: str
    failure_pattern: str
    example_rows: list[dict[str, Any]] = Field(default_factory=list)
    suggestions: list[FeedbackSuggestion] = Field(default_factory=list)
    analysis_summary: str = ""


class UserFeedback(BaseModel):
    question_id: str
    answer: str
