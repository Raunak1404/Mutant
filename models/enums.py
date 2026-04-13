from __future__ import annotations

from enum import Enum


class RowStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    AWAITING_FEEDBACK = "awaiting_feedback"


class ProcessingStrategy(str, Enum):
    CODE_GEN = "code_gen"           # Strategy A: LLM → pandas code → execution service
    CHUNK_PARALLEL = "chunk_parallel"  # Strategy B: row-by-row via LLM chunks
    SEQUENTIAL = "sequential"       # Strategy C: full DataFrame, single agent
    NATIVE = "native"               # Strategy D: persistent Python code → execution service


class JobState(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    AWAITING_FEEDBACK = "awaiting_feedback"
    RESUMING = "resuming"
    COMPLETED = "completed"
    FAILED = "failed"


class DependencyType(str, Enum):
    NONE = "none"           # rows are independent — plain chunking
    GROUP = "group"         # rows share a key (e.g., invoice_id)
    SEQUENTIAL = "sequential"  # rows depend on N preceding rows
    GLOBAL = "global"       # unbounded cross-row dependencies
