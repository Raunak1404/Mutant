from __future__ import annotations

import json
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from cache.cache_manager import CacheManager
from core.pipeline import execute_step
from core.progress import ProgressPublisher
from db.models import JobCheckpoint, JobStatus
from db.code_versions import get_code_by_version_id, snapshot_code_for_job
from db.rules import get_rule_by_version_id, snapshot_rules_for_job
from excel.reader import read_excel_to_parquet, save_df_to_storage
from excel.writer import write_parquet_to_excel
from feedback.question_generator import generate_questions
from feedback.report_aggregator import aggregate_reports
from feedback.code_updater import update_code_from_feedback
from feedback.rule_updater import update_rules_from_feedback
from llm.provider import LLMProvider
from models.enums import JobState, StepStatus
from models.results import (
    FeedbackQuestion,
    ReviewReport,
    StepResult,
    UserFeedback,
)
from storage.backend import StorageBackend
from utils.logging import get_logger

logger = get_logger(__name__)

# --- Pipeline track configuration ---
PIPELINE_TRACKS = {
    "sap": {"input_key_name": "sap", "steps": [1, 2, 3, 4]},
    "esjc": {"input_key_name": "esjc", "steps": [5]},
}
FINALIZE_STEP = 6


def _get_track_for_step(step_number: int) -> str | None:
    """Return the track name a step belongs to, or None for the finalize step."""
    for track_name, config in PIPELINE_TRACKS.items():
        if step_number in config["steps"]:
            return track_name
    return None


class Orchestrator:
    """
    Super Brain: coordinates the full Excel processing pipeline.
    Handles dual-track execution (SAP + ESJC), progress publishing,
    checkpointing, and the feedback loop.
    """

    def __init__(
        self,
        llm: LLMProvider,
        storage: StorageBackend,
        session: AsyncSession,
        progress: ProgressPublisher,
        execution_service_url: str,
        chunk_size: int = 100,
        review_chunk_size: int = 300,
        max_concurrency: int = 10,
        cache: CacheManager | None = None,
        libraries_dir: str = "./libraries",
    ) -> None:
        self.llm = llm
        self.storage = storage
        self.session = session
        self.progress = progress
        self.execution_service_url = execution_service_url
        self.chunk_size = chunk_size
        self.review_chunk_size = review_chunk_size
        self.max_concurrency = max_concurrency
        self.cache = cache
        self.libraries_dir = libraries_dir

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    async def process_job(
        self,
        job_id: str,
        input_storage_keys: dict[str, str],
        step_numbers: list[int],
    ) -> None:
        """Main entry point for a new dual-track job."""
        logger.info("job_started", job_id=job_id, steps=step_numbers)
        await self._update_job_state(job_id, JobState.PROCESSING)

        # Copy both uploaded files into job-scoped storage
        storage_keys: dict = {"_input_storage_keys": input_storage_keys}

        for track_name, track_config in PIPELINE_TRACKS.items():
            key_name = track_config["input_key_name"]
            input_key = input_storage_keys[key_name]
            raw_excel_key = f"jobs/{job_id}/step_0/{track_name}_input.xlsx"

            try:
                raw_data = await self.storage.download(input_key)
                await self.storage.upload(raw_excel_key, raw_data)
            except Exception as exc:
                logger.error("input_copy_error", job_id=job_id, track=track_name, error=str(exc))
                await self._update_job_state(job_id, JobState.FAILED, error=str(exc))
                await self.progress.publish_failed(job_id, str(exc))
                return

            storage_keys[f"step_0_{track_name}_excel"] = raw_excel_key

        # Snapshot rule versions and code versions
        rule_snapshot_ids = await snapshot_rules_for_job(self.session, job_id, step_numbers)
        code_snapshot_ids = await snapshot_code_for_job(self.session, job_id, step_numbers)

        step_results: list[StepResult] = []
        review_reports: list[ReviewReport] = []

        # --- Execute each track ---
        for track_name, track_config in PIPELINE_TRACKS.items():
            current_input_key = storage_keys[f"step_0_{track_name}_excel"]
            current_parquet_key = ""

            for step_number in track_config["steps"]:
                if step_number not in step_numbers:
                    continue

                result = await self._run_step(
                    job_id=job_id,
                    step_number=step_number,
                    current_input_key=current_input_key,
                    current_parquet_key=current_parquet_key,
                    rule_snapshot_ids=rule_snapshot_ids,
                    code_snapshot_ids=code_snapshot_ids,
                    storage_keys=storage_keys,
                    step_results=step_results,
                    review_reports=review_reports,
                )

                if result is None:
                    # Exception during step — already paused for feedback
                    await self._pause_for_feedback(
                        job_id, step_numbers, review_reports,
                        rule_snapshot_ids, code_snapshot_ids, storage_keys,
                        completed_steps=[r.step_number for r in step_results],
                        current_step=step_number,
                    )
                    return

                step_results.append(result)

                if result.output_storage_key is None:
                    # Native step produced no output — enter feedback
                    break

                storage_keys[f"step_{step_number}_output"] = result.output_storage_key
                current_input_key = result.output_storage_key
                if result.output_storage_key.endswith(".parquet"):
                    current_parquet_key = result.output_storage_key

        # Check for failures across all tracks before finalize
        total_failures = sum(r.failed_rows for r in review_reports if r)
        if total_failures > 0:
            await self._pause_for_feedback(
                job_id, step_numbers, review_reports,
                rule_snapshot_ids, code_snapshot_ids, storage_keys,
                completed_steps=[r.step_number for r in step_results],
                current_step=step_results[-1].step_number if step_results else step_numbers[0],
            )
            return

        # --- Execute finalize step (step 6) ---
        if FINALIZE_STEP in step_numbers:
            finalize_result = await self._run_finalize_step(
                job_id=job_id,
                step_numbers=step_numbers,
                rule_snapshot_ids=rule_snapshot_ids,
                code_snapshot_ids=code_snapshot_ids,
                storage_keys=storage_keys,
                step_results=step_results,
                review_reports=review_reports,
            )
            if finalize_result is None:
                return  # feedback or failure already handled

        # All steps passed — complete the job
        final_output_key = storage_keys.get(f"step_{FINALIZE_STEP}_output")
        if not final_output_key:
            # Fallback: if no finalize step, use last track output
            final_output_key = storage_keys.get("step_4_output", "")

        await self._finalize(job_id, final_output_key)

    async def resume_job(
        self,
        job_id: str,
        feedback_answers: list[UserFeedback],
        step_numbers: list[int],
    ) -> None:
        """Resume a job after user feedback — dual-track aware."""
        logger.info("job_resuming", job_id=job_id)
        await self._update_job_state(job_id, JobState.RESUMING)

        # Load checkpoint
        from sqlalchemy import select
        result = await self.session.execute(
            select(JobCheckpoint).where(JobCheckpoint.job_id == job_id)
        )
        checkpoint = result.scalar_one_or_none()
        if not checkpoint:
            logger.error("checkpoint_not_found", job_id=job_id)
            await self._update_job_state(job_id, JobState.FAILED, error="Checkpoint not found")
            return

        storage_keys = json.loads(checkpoint.storage_keys_json)
        rule_snapshot_ids = {
            int(k): v for k, v in json.loads(checkpoint.rule_snapshot_ids_json).items()
        }
        questions_data = json.loads(checkpoint.questions_json)
        questions = [FeedbackQuestion(**q) for q in questions_data]
        failed_row_indices = json.loads(checkpoint.failed_row_indices_json)

        # Update rules based on feedback
        new_version_ids = await update_rules_from_feedback(
            self.session, self.llm, job_id, rule_snapshot_ids, questions, feedback_answers
        )
        rule_snapshot_ids.update(new_version_ids)

        # Refresh rule snapshots for steps that had no feedback answers —
        # the user may have changed rules via chat since the checkpoint was
        # created, and those steps still hold the old frozen snapshot IDs.
        from db.rules import get_latest_rule
        for sn in step_numbers:
            if sn not in new_version_ids:
                latest = await get_latest_rule(self.session, sn)
                if latest and latest.id != rule_snapshot_ids.get(sn):
                    rule_snapshot_ids[sn] = latest.id
                    logger.info("rule_snapshot_refreshed", job_id=job_id, step=sn, new_version_id=latest.id)

        # Update code based on feedback (for native steps)
        code_snapshot_ids_raw = storage_keys.pop("_code_snapshot_ids", {})
        code_snapshot_ids = {int(k): v for k, v in code_snapshot_ids_raw.items()} if isinstance(code_snapshot_ids_raw, dict) else {}

        if code_snapshot_ids:
            new_code_ids = await update_code_from_feedback(
                self.session, self.llm, job_id, code_snapshot_ids, questions, feedback_answers,
                rule_snapshot_ids=rule_snapshot_ids,
            )
            code_snapshot_ids.update(new_code_ids)

        # Re-process steps — track-aware input routing
        step_results: list[StepResult] = []
        review_reports: list[ReviewReport] = []
        upstream_reprocessed_tracks: set[str] = set()

        for track_name, track_config in PIPELINE_TRACKS.items():
            current_input_key = storage_keys.get(f"step_0_{track_name}_excel", "")
            current_parquet_key = ""
            track_reprocessed = False

            for step_number in track_config["steps"]:
                if step_number not in step_numbers:
                    continue

                raw_indices = failed_row_indices.get(str(step_number), [])
                failed_indices = [int(i) for i in raw_indices]
                full_rerun = failed_indices == [-1]

                # Skip if no failures and no upstream re-processing in this track
                if not failed_indices and not track_reprocessed:
                    # Use previous output as input for next step
                    prev_output = storage_keys.get(f"step_{step_number}_output")
                    if prev_output:
                        current_input_key = prev_output
                        if prev_output.endswith(".parquet"):
                            current_parquet_key = prev_output
                    continue

                rule_version_id = rule_snapshot_ids.get(step_number)
                rule_version = await get_rule_by_version_id(self.session, rule_version_id) if rule_version_id else None
                if not rule_version:
                    continue

                code_content = None
                code_version_id = code_snapshot_ids.get(step_number)
                if code_version_id:
                    code_version = await get_code_by_version_id(self.session, code_version_id)
                    if code_version:
                        code_content = code_version.content

                if code_content:
                    output_key = f"jobs/{job_id}/step_{step_number}/resume_output.xlsx"
                else:
                    output_key = f"jobs/{job_id}/step_{step_number}/resume_output.parquet"

                await self.progress.publish_step_started(job_id, step_number, "resume")

                step_failed_indices = None if (full_rerun or track_reprocessed) else (failed_indices or None)

                try:
                    result = await execute_step(
                        step_number=step_number,
                        job_id=job_id,
                        rule_content=rule_version.content,
                        input_storage_key=current_input_key,
                        output_storage_key=output_key,
                        storage=self.storage,
                        llm=self.llm,
                        execution_service_url=self.execution_service_url,
                        chunk_size=self.chunk_size,
                        review_chunk_size=self.review_chunk_size,
                        max_concurrency=self.max_concurrency,
                        failed_row_indices=step_failed_indices,
                        cache=self.cache,
                        code_content=code_content,
                        libraries_dir=self.libraries_dir,
                        parquet_input_key=current_parquet_key,
                    )
                except Exception as exc:
                    logger.error("resume_step_error", job_id=job_id, step=step_number, error=str(exc))
                    error_report = ReviewReport(
                        step_number=step_number,
                        total_rows=0, passed_rows=0, failed_rows=1,
                        deterministic_failures=1, llm_failures=0,
                        failure_patterns=[f"Resume step error: {exc}"],
                        failed_row_indices=[],
                        narrative_summary=f"Step {step_number} failed during resume: {exc}",
                        confidence_score=0.0,
                        error_context={
                            "error_type": "resume_exception",
                            "exception_class": type(exc).__name__,
                            "exception_message": str(exc)[:1000],
                            "step_number": step_number,
                        },
                    )
                    review_reports.append(error_report)
                    await self._pause_for_feedback(
                        job_id, step_numbers, review_reports,
                        rule_snapshot_ids, code_snapshot_ids, storage_keys,
                        completed_steps=[r.step_number for r in step_results],
                        current_step=step_number,
                    )
                    return

                step_results.append(result)
                if result.review_report:
                    review_reports.append(result.review_report)

                await self.progress.publish_step_completed(
                    job_id,
                    step_number,
                    {"passed": result.successful_rows, "failed": result.failed_rows},
                )

                storage_keys[f"step_{step_number}_output"] = output_key
                track_reprocessed = True

                if output_key.endswith(".xlsx"):
                    current_input_key = output_key
                else:
                    current_parquet_key = output_key
                    current_input_key = output_key

            if track_reprocessed:
                upstream_reprocessed_tracks.add(track_name)

        # Check for failures before finalize
        total_failures = sum(r.failed_rows for r in review_reports if r)
        if total_failures > 0:
            await self._pause_for_feedback(
                job_id, step_numbers, review_reports,
                rule_snapshot_ids, code_snapshot_ids, storage_keys,
                completed_steps=[r.step_number for r in step_results],
                current_step=step_results[-1].step_number if step_results else step_numbers[0],
            )
            return

        # Re-run finalize step (always re-run if any track was reprocessed)
        if FINALIZE_STEP in step_numbers:
            finalize_result = await self._run_finalize_step(
                job_id=job_id,
                step_numbers=step_numbers,
                rule_snapshot_ids=rule_snapshot_ids,
                code_snapshot_ids=code_snapshot_ids,
                storage_keys=storage_keys,
                step_results=step_results,
                review_reports=review_reports,
            )
            if finalize_result is None:
                return

        final_output_key = storage_keys.get(f"step_{FINALIZE_STEP}_output")
        if not final_output_key:
            final_output_key = storage_keys.get("step_4_output", "")

        await self._finalize(job_id, final_output_key)

    # ------------------------------------------------------------------
    # Step execution helpers
    # ------------------------------------------------------------------

    async def _run_step(
        self,
        job_id: str,
        step_number: int,
        current_input_key: str,
        current_parquet_key: str,
        rule_snapshot_ids: dict[int, int],
        code_snapshot_ids: dict[int, int],
        storage_keys: dict[str, str],
        step_results: list[StepResult],
        review_reports: list[ReviewReport],
        extra_libraries: dict[str, bytes] | None = None,
    ) -> StepResult | None:
        """Execute a single step. Returns StepResult or None if exception occurred."""
        rule_version_id = rule_snapshot_ids.get(step_number)
        if not rule_version_id:
            logger.error("rule_snapshot_missing", step=step_number)
            return None
        rule_version = await get_rule_by_version_id(self.session, rule_version_id)
        if not rule_version:
            logger.error("rule_version_missing", step=step_number, version_id=rule_version_id)
            return None

        code_content = None
        code_version_id = code_snapshot_ids.get(step_number)
        if code_version_id:
            code_version = await get_code_by_version_id(self.session, code_version_id)
            if code_version:
                code_content = code_version.content
                logger.info("using_native_code", step=step_number, code_version=code_version.version)

        if code_content:
            output_key = f"jobs/{job_id}/step_{step_number}/output.xlsx"
        else:
            output_key = f"jobs/{job_id}/step_{step_number}/output.parquet"

        await self.progress.publish_step_started(job_id, step_number, "native" if code_content else "auto")

        try:
            result = await execute_step(
                step_number=step_number,
                job_id=job_id,
                rule_content=rule_version.content,
                input_storage_key=current_input_key,
                output_storage_key=output_key,
                storage=self.storage,
                llm=self.llm,
                execution_service_url=self.execution_service_url,
                chunk_size=self.chunk_size,
                review_chunk_size=self.review_chunk_size,
                max_concurrency=self.max_concurrency,
                cache=self.cache,
                code_content=code_content,
                libraries_dir=self.libraries_dir,
                parquet_input_key=current_parquet_key,
                extra_libraries=extra_libraries,
            )
        except Exception as exc:
            logger.error("step_error", job_id=job_id, step=step_number, error=str(exc))

            error_report = ReviewReport(
                step_number=step_number,
                total_rows=0,
                passed_rows=0,
                failed_rows=1,
                deterministic_failures=1,
                llm_failures=0,
                failure_patterns=[f"Step execution error: {exc}"],
                failed_row_indices=[],
                narrative_summary=(
                    f"Step {step_number} raised an error during execution: {exc}. "
                    f"This likely indicates a mismatch between the step's code/rules "
                    f"and the input file format. The user should review and update "
                    f"the rules or code to handle this input."
                ),
                confidence_score=0.0,
                error_context={
                    "error_type": "step_exception",
                    "exception_class": type(exc).__name__,
                    "exception_message": str(exc)[:1000],
                    "step_number": step_number,
                },
            )
            review_reports.append(error_report)
            return None

        if result.review_report:
            review_reports.append(result.review_report)

        await self.progress.publish_step_completed(
            job_id, step_number,
            {"passed": result.successful_rows, "failed": result.failed_rows},
        )

        return result

    async def _run_finalize_step(
        self,
        job_id: str,
        step_numbers: list[int],
        rule_snapshot_ids: dict[int, int],
        code_snapshot_ids: dict[int, int],
        storage_keys: dict,
        step_results: list[StepResult],
        review_reports: list[ReviewReport],
    ) -> StepResult | None:
        """Execute the finalize step (step 6) which combines both track outputs."""
        step_4_output = storage_keys.get("step_4_output")
        step_5_output = storage_keys.get("step_5_output")

        if not step_4_output or not step_5_output:
            error = "Missing track outputs for finalize step (need step 4 + step 5)"
            logger.error("finalize_missing_inputs", job_id=job_id,
                         has_step4=bool(step_4_output), has_step5=bool(step_5_output))
            await self._update_job_state(job_id, JobState.FAILED, error=error)
            await self.progress.publish_failed(job_id, error)
            return None

        # Download ESJC output to inject as extra library for step 6
        esjc_output_data = await self.storage.download(step_5_output)

        result = await self._run_step(
            job_id=job_id,
            step_number=FINALIZE_STEP,
            current_input_key=step_4_output,
            current_parquet_key="",
            rule_snapshot_ids=rule_snapshot_ids,
            code_snapshot_ids=code_snapshot_ids,
            storage_keys=storage_keys,
            step_results=step_results,
            review_reports=review_reports,
            extra_libraries={"esjc_output.xlsx": esjc_output_data},
        )

        if result is None:
            # Exception — pause for feedback
            await self._pause_for_feedback(
                job_id, step_numbers, review_reports,
                rule_snapshot_ids, code_snapshot_ids, storage_keys,
                completed_steps=[r.step_number for r in step_results],
                current_step=FINALIZE_STEP,
            )
            return None

        step_results.append(result)

        if result.output_storage_key is None:
            # Finalize produced no output — feedback or fail
            total_failures = sum(r.failed_rows for r in review_reports if r)
            if total_failures > 0:
                await self._pause_for_feedback(
                    job_id, step_numbers, review_reports,
                    rule_snapshot_ids, code_snapshot_ids, storage_keys,
                    completed_steps=[r.step_number for r in step_results],
                    current_step=FINALIZE_STEP,
                )
            return None

        storage_keys[f"step_{FINALIZE_STEP}_output"] = result.output_storage_key
        return result

    # ------------------------------------------------------------------
    # Feedback / checkpoint
    # ------------------------------------------------------------------

    async def _pause_for_feedback(
        self,
        job_id: str,
        step_numbers: list[int],
        review_reports: list[ReviewReport],
        rule_snapshot_ids: dict[int, int],
        code_snapshot_ids: dict[int, int],
        storage_keys: dict[str, str],
        completed_steps: list[int],
        current_step: int,
    ) -> None:
        aggregated = aggregate_reports(review_reports)
        questions = await generate_questions(self.llm, review_reports, aggregated)

        # Build new failed_row_indices from current review cycle
        new_failed_row_indices = {}
        for r in review_reports:
            if r.failed_rows > 0 and not r.failed_row_indices:
                new_failed_row_indices[str(r.step_number)] = [-1]
            else:
                new_failed_row_indices[str(r.step_number)] = r.failed_row_indices

        storage_keys_with_meta = {
            **storage_keys,
            "_code_snapshot_ids": code_snapshot_ids,
            "_all_step_numbers": step_numbers,
        }

        from sqlalchemy import select
        existing = await self.session.execute(
            select(JobCheckpoint).where(JobCheckpoint.job_id == job_id)
        )
        checkpoint = existing.scalar_one_or_none()

        # Merge with existing checkpoint's failed_row_indices so that
        # chat-injected [-1] markers for other steps are preserved.
        if checkpoint and checkpoint.failed_row_indices_json:
            old_indices = json.loads(checkpoint.failed_row_indices_json)
            old_indices.update(new_failed_row_indices)
            failed_row_indices = old_indices
        else:
            failed_row_indices = new_failed_row_indices

        questions_data = [q.model_dump() for q in questions]

        if checkpoint:
            checkpoint.step_number = current_step
            checkpoint.status = "AWAITING_FEEDBACK"
            checkpoint.questions_json = json.dumps(questions_data, default=str)
            checkpoint.completed_steps_json = json.dumps(completed_steps)
            checkpoint.failed_row_indices_json = json.dumps(failed_row_indices)
            checkpoint.rule_snapshot_ids_json = json.dumps(rule_snapshot_ids)
            checkpoint.storage_keys_json = json.dumps(storage_keys_with_meta)
        else:
            checkpoint = JobCheckpoint(
                job_id=job_id,
                step_number=current_step,
                status="AWAITING_FEEDBACK",
                questions_json=json.dumps(questions_data, default=str),
                completed_steps_json=json.dumps(completed_steps),
                failed_row_indices_json=json.dumps(failed_row_indices),
                rule_snapshot_ids_json=json.dumps(rule_snapshot_ids),
                storage_keys_json=json.dumps(storage_keys_with_meta),
            )
            self.session.add(checkpoint)

        await self.session.commit()

        await self._update_job_state(job_id, JobState.AWAITING_FEEDBACK)
        await self.progress.publish_awaiting_feedback(
            job_id, [q.model_dump() for q in questions]
        )

    # ------------------------------------------------------------------
    # Finalize & state management
    # ------------------------------------------------------------------

    async def _finalize(self, job_id: str, output_key: str) -> None:
        """Mark job as completed. output_key is the ZIP from step 6."""
        if not output_key:
            await self._update_job_state(job_id, JobState.FAILED, error="No output file produced")
            await self.progress.publish_failed(job_id, "No output file produced")
            return

        await self._close_feedback_checkpoint(job_id, "COMPLETED")
        await self._update_job_state(job_id, JobState.COMPLETED, output_key=output_key)
        await self.progress.publish_completed(job_id, output_key)
        logger.info("job_completed", job_id=job_id, output_key=output_key)

    async def _close_feedback_checkpoint(self, job_id: str, status: str) -> None:
        from sqlalchemy import select

        result = await self.session.execute(
            select(JobCheckpoint).where(JobCheckpoint.job_id == job_id)
        )
        checkpoint = result.scalar_one_or_none()
        if checkpoint is None or checkpoint.status != "AWAITING_FEEDBACK":
            return

        checkpoint.status = status
        await self.session.commit()

    async def _update_job_state(
        self,
        job_id: str,
        state: JobState,
        error: str | None = None,
        output_key: str | None = None,
    ) -> None:
        from sqlalchemy import select
        result = await self.session.execute(
            select(JobStatus).where(JobStatus.job_id == job_id)
        )
        status = result.scalar_one_or_none()
        if status:
            status.state = state.value
            if error:
                status.error_message = error
            if output_key:
                status.output_storage_key = output_key
            await self.session.commit()
