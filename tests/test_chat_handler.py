from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from chat.chat_handler import (
    _attach_changes_to_active_checkpoint,
    _build_direct_change_decision,
    _compress_question_response,
    apply_confirmed_changes,
)
from chat.chat_models import ChatDecision, StepRuleSummary
from db.models import Base, ChatMessage, CodeVersion, JobCheckpoint, JobStatus, RuleVersion


def _make_summaries() -> list[StepRuleSummary]:
    return [
        StepRuleSummary(step_number=1, rule_content="# Step 1: Load & Clean SAP Data", code_functions=["main"]),
        StepRuleSummary(step_number=2, rule_content="# Step 2: Split & Extract SAP Actions", code_functions=["main"]),
        StepRuleSummary(step_number=3, rule_content="# Step 3: Classify SAP Defects", code_functions=["main"]),
        StepRuleSummary(step_number=4, rule_content="# Step 4: Format SAP Output", code_functions=["main"]),
        StepRuleSummary(step_number=5, rule_content="# Step 5: Process ESJC Data", code_functions=["main"]),
        StepRuleSummary(step_number=6, rule_content="# Step 6: Package & Export", code_functions=["main"]),
    ]


def test_direct_change_request_generates_step_rule_and_code_updates() -> None:
    decision = _build_direct_change_decision(
        "Please update step 3 to add a hydraulic defect category.",
        _make_summaries(),
    )

    assert decision is not None
    assert decision.needs_confirmation is True
    assert decision.questions == []

    proposed = {(change.step_number, change.change_type) for change in decision.proposed_changes}
    assert (3, "rule") in proposed
    assert (3, "code") in proposed


def test_keyword_only_change_request_still_infers_the_target_step() -> None:
    decision = _build_direct_change_decision(
        "Add a hydraulic defect category and keep open defect as the fallback.",
        _make_summaries(),
    )

    assert decision is not None
    step_numbers = {change.step_number for change in decision.proposed_changes}
    assert step_numbers == {3}


def test_question_response_is_reduced_to_one_targeted_question() -> None:
    text, decision = _compress_question_response(
        "Which step do you mean? What exact category do you want?",
        ChatDecision(questions=["Which step do you mean?", "What exact category do you want?"]),
    )

    assert text == "I need one detail before I apply this: Which step do you mean?"
    assert decision.questions == ["Which step do you mean?"]
    assert decision.proposed_changes == []


@pytest.mark.asyncio
async def test_active_checkpoint_is_updated_with_chat_applied_versions() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with Session() as session:
        session.add(JobStatus(job_id="job-123", state="awaiting_feedback"))
        checkpoint = JobCheckpoint(
            job_id="job-123",
            step_number=3,
            status="AWAITING_FEEDBACK",
            questions_json="[]",
            completed_steps_json="[1,2,3]",
            failed_row_indices_json=json.dumps({"3": [7, 8]}),
            rule_snapshot_ids_json=json.dumps({"3": 1}),
            storage_keys_json=json.dumps({"_code_snapshot_ids": {"3": 2}}),
        )
        session.add(checkpoint)
        await session.commit()

        updated = await _attach_changes_to_active_checkpoint(
            session,
            job_id="job-123",
            rule_version_ids={3: 11},
            code_version_ids={3: 22},
            changed_steps={3},
        )

        assert updated is True

        refreshed = await session.get(JobCheckpoint, checkpoint.id)
        assert refreshed is not None
        assert json.loads(refreshed.rule_snapshot_ids_json) == {"3": 11}
        assert json.loads(refreshed.storage_keys_json)["_code_snapshot_ids"] == {"3": 22}
        assert json.loads(refreshed.failed_row_indices_json) == {"3": [-1]}

    await engine.dispose()


@pytest.mark.asyncio
async def test_implicit_checkpoint_lookup_skips_completed_jobs() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime.now(timezone.utc)

    async with Session() as session:
        active_status = JobStatus(job_id="job-active", state="awaiting_feedback")
        active_checkpoint = JobCheckpoint(
            job_id="job-active",
            step_number=1,
            status="AWAITING_FEEDBACK",
            questions_json="[]",
            completed_steps_json="[1]",
            failed_row_indices_json=json.dumps({"1": [3]}),
            rule_snapshot_ids_json=json.dumps({"1": 1}),
            storage_keys_json=json.dumps({"_code_snapshot_ids": {"1": 2}}),
            created_at=now,
            updated_at=now,
        )
        stale_status = JobStatus(job_id="job-stale", state="completed")
        stale_checkpoint = JobCheckpoint(
            job_id="job-stale",
            step_number=1,
            status="AWAITING_FEEDBACK",
            questions_json="[]",
            completed_steps_json="[1]",
            failed_row_indices_json=json.dumps({"1": [4]}),
            rule_snapshot_ids_json=json.dumps({"1": 10}),
            storage_keys_json=json.dumps({"_code_snapshot_ids": {"1": 20}}),
            created_at=now + timedelta(minutes=1),
            updated_at=now + timedelta(minutes=1),
        )
        session.add_all([active_status, active_checkpoint, stale_status, stale_checkpoint])
        await session.commit()

        updated = await _attach_changes_to_active_checkpoint(
            session,
            job_id=None,
            rule_version_ids={1: 11},
            code_version_ids={1: 22},
            changed_steps={1},
        )

        assert updated is True

        refreshed_active = await session.get(JobCheckpoint, active_checkpoint.id)
        refreshed_stale = await session.get(JobCheckpoint, stale_checkpoint.id)
        assert refreshed_active is not None
        assert refreshed_stale is not None
        assert json.loads(refreshed_active.rule_snapshot_ids_json) == {"1": 11}
        assert json.loads(refreshed_active.storage_keys_json)["_code_snapshot_ids"] == {"1": 22}
        assert json.loads(refreshed_stale.rule_snapshot_ids_json) == {"1": 10}
        assert json.loads(refreshed_stale.storage_keys_json)["_code_snapshot_ids"] == {"1": 20}

    await engine.dispose()


class _NoLLM:
    async def complete(self, *args, **kwargs):  # pragma: no cover - should never be called
        raise AssertionError("LLM should not be used for restore requests")


@pytest.mark.asyncio
async def test_restore_request_uses_previous_saved_versions() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with Session() as session:
        session.add(
            RuleVersion(step_number=1, version=1, content="original rule", created_by="system")
        )
        session.add(
            RuleVersion(step_number=1, version=2, content="drift-compatible rule", created_by="chat")
        )
        session.add(
            CodeVersion(
                step_number=1,
                version=1,
                content="original code",
                parent_version_id=None,
                changed_function=None,
                created_by="system",
            )
        )
        session.add(
            CodeVersion(
                step_number=1,
                version=2,
                content="drift-compatible code",
                parent_version_id=1,
                changed_function=None,
                created_by="chat",
            )
        )
        session.add(
            ChatMessage(
                session_id="restore-session",
                role="assistant",
                content="pending restore",
                metadata_json=json.dumps(
                    {
                        "proposal_status": "pending",
                        "proposed_changes": [
                            {
                                "step_number": 1,
                                "change_type": "rule",
                                "description": "Restore step 1 to the original rules and behavior from before the Aircraft Tail change.",
                            },
                            {
                                "step_number": 1,
                                "change_type": "code",
                                "description": "Restore step 1 to the original rules and behavior from before the Aircraft Tail change.",
                            },
                        ],
                    }
                ),
            )
        )
        await session.commit()

        response = await apply_confirmed_changes(session, _NoLLM(), "restore-session")

        assert "restored to previous version" in response.message
        assert "active paused job" not in response.message.lower()

        latest_rule = (
            await session.execute(
                select(RuleVersion)
                .where(RuleVersion.step_number == 1)
                .order_by(RuleVersion.version.desc())
                .limit(1)
            )
        ).scalar_one()
        latest_code = (
            await session.execute(
                select(CodeVersion)
                .where(CodeVersion.step_number == 1)
                .order_by(CodeVersion.version.desc())
                .limit(1)
            )
        ).scalar_one()

        assert latest_rule.version == 3
        assert latest_rule.content == "original rule"
        assert latest_code.version == 3
        assert latest_code.content == "original code"

    await engine.dispose()
