from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class RuleVersion(Base):
    """Append-only rule version table. Never update rows — only insert."""

    __tablename__ = "rule_versions"
    __table_args__ = (
        UniqueConstraint("step_number", "version", name="uq_rule_step_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)  # markdown rule text
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=True)  # "system" | job_id | "user"

    snapshots: Mapped[list["JobRuleSnapshot"]] = relationship(
        "JobRuleSnapshot", back_populates="rule_version"
    )


class JobRuleSnapshot(Base):
    """Freeze rule versions at job start to prevent race conditions."""

    __tablename__ = "job_rule_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    rule_version_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("rule_versions.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    rule_version: Mapped["RuleVersion"] = relationship(
        "RuleVersion", back_populates="snapshots"
    )


class JobCheckpoint(Base):
    """Lightweight checkpoint: metadata + storage references only (no DataFrame data)."""

    __tablename__ = "job_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True, index=True)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    questions_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    completed_steps_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    failed_row_indices_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    rule_snapshot_ids_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    storage_keys_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class CodeVersion(Base):
    """Append-only code version table for native step logic files."""

    __tablename__ = "code_versions"
    __table_args__ = (
        UniqueConstraint("step_number", "version", name="uq_code_step_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)  # full Python source
    parent_version_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("code_versions.id"), nullable=True
    )
    changed_function: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )  # which function was updated, if any
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=True)  # "system" | job_id | "user"

    snapshots: Mapped[list["JobCodeSnapshot"]] = relationship(
        "JobCodeSnapshot", back_populates="code_version"
    )


class JobCodeSnapshot(Base):
    """Freeze code versions at job start to prevent race conditions."""

    __tablename__ = "job_code_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    code_version_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("code_versions.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    code_version: Mapped["CodeVersion"] = relationship(
        "CodeVersion", back_populates="snapshots"
    )


class ChatMessage(Base):
    """Persistent chat messages for the sidebar AI assistant."""

    __tablename__ = "chat_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # "user" | "assistant"
    content: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class JobStatus(Base):
    """Current state of each job."""

    __tablename__ = "job_statuses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True, index=True)
    state: Mapped[str] = mapped_column(String(50), nullable=False, default="queued")
    input_storage_key: Mapped[str] = mapped_column(String(512), nullable=True)
    output_storage_key: Mapped[str] = mapped_column(String(512), nullable=True)
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
