from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd

from core.agent_pool import AgentPool
from core.chunker_strategy import create_chunks
from core.codegen_agent import CodeGenAgent
from core.review_agent import ReviewAgent
from core.worker_agent import WorkerAgent
from excel.reader import load_parquet_from_storage, save_df_to_storage
from execution_service.sandbox import execute_native_step as execute_native_step_embedded
from llm.provider import LLMProvider
from models.enums import DependencyType, ProcessingStrategy, StepStatus
from models.messages import Message
from models.results import ReviewReport, RuleMetadata, StepResult
from storage.backend import StorageBackend
from utils.logging import get_logger
from utils.retry import CircuitBreaker

logger = get_logger(__name__)

CLASSIFY_SYSTEM_PROMPT = """You are an expert at analyzing data transformation rules.
Classify the given rules by their row dependency type.

Respond with a JSON object:
{
  "dependency_type": "none" | "group" | "sequential" | "global",
  "dependency_scope": "backward_N" | null,
  "group_key": "column_name" | null,
  "mechanical": true | false
}

- "none": each row is independent
- "group": rows share a key (e.g., invoice_id); set group_key
- "sequential": rows depend on N preceding rows; set dependency_scope e.g. "backward_3"
- "global": unbounded cross-row dependencies (e.g., running totals)
- "mechanical": true if pure pandas (ffill, cumsum, shift) can handle this without LLM row-by-row"""


async def classify_rules(llm: LLMProvider, rule_content: str) -> RuleMetadata:
    """Use LLM to classify rule dependency type for context-aware chunking."""
    try:
        response = await llm.complete(
            system_prompt=CLASSIFY_SYSTEM_PROMPT,
            messages=[Message(role="user", content=f"Rules to classify:\n{rule_content}")],
            temperature=0.0,
            max_tokens=512,
        )
        content = response.content
        if "```json" in content:
            content = content.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in content:
            content = content.split("```", 1)[1].split("```", 1)[0]
        data = json.loads(content.strip())
        return RuleMetadata(**data)
    except Exception as exc:
        logger.warning("rule_classify_error", error=str(exc))
        return RuleMetadata()


def select_strategy(metadata: RuleMetadata) -> ProcessingStrategy:
    """Auto-select processing strategy based on rule metadata."""
    if metadata.mechanical or metadata.dependency_type == DependencyType.NONE:
        return ProcessingStrategy.CODE_GEN
    if metadata.dependency_type == DependencyType.GLOBAL:
        return ProcessingStrategy.SEQUENTIAL
    return ProcessingStrategy.CHUNK_PARALLEL


async def execute_step(
    step_number: int,
    job_id: str,
    rule_content: str,
    input_storage_key: str,
    output_storage_key: str,
    storage: StorageBackend,
    llm: LLMProvider,
    execution_service_url: str,
    chunk_size: int = 100,
    review_chunk_size: int = 300,
    max_concurrency: int = 10,
    failed_row_indices: list[int] | None = None,
    cache=None,
    # Native execution params
    code_content: str | None = None,
    libraries_dir: str | None = None,
    # Parquet key for LLM fallback when input is Excel
    parquet_input_key: str | None = None,
    # Extra library files to inject (e.g., ESJC output for step 6)
    extra_libraries: dict[str, bytes] | None = None,
) -> StepResult:
    """Execute a single pipeline step and return StepResult."""
    started_at = datetime.utcnow()

    # Determine strategy: if code_content is provided, use NATIVE
    df: pd.DataFrame | None = None

    if code_content is not None:
        strategy = ProcessingStrategy.NATIVE
        metadata = RuleMetadata(dependency_type=DependencyType.NONE, mechanical=True)
    else:
        df = await load_parquet_from_storage(storage, input_storage_key)
        if failed_row_indices:
            df = df.iloc[failed_row_indices].copy()
        metadata = await classify_rules(llm, rule_content)
        strategy = select_strategy(metadata)

    logger.info(
        "step_execute",
        job_id=job_id,
        step=step_number,
        strategy=strategy.value,
    )

    output_df: pd.DataFrame | None = None

    if strategy == ProcessingStrategy.NATIVE:
        # Native execution: run persistent Python code through execution service
        native_result = await _execute_native_step(
            code_content=code_content,
            input_storage_key=input_storage_key,
            output_storage_key=output_storage_key,
            storage=storage,
            execution_service_url=execution_service_url,
            libraries_dir=libraries_dir,
            extra_libraries=extra_libraries,
        )

        if native_result.success:
            result_data = json.loads(native_result.result_json)
            stats = result_data.get("stats", {})
            total_rows = stats.get("total_rows", 0)

            # For native steps, we skip the LLM review — the step itself
            # produces deterministic results with its own changelog
            review = ReviewReport(
                step_number=step_number,
                total_rows=total_rows,
                passed_rows=total_rows - stats.get("quantity_mismatches", 0),
                failed_rows=stats.get("quantity_mismatches", 0),
                deterministic_failures=0,
                llm_failures=0,
                failure_patterns=[],
                failed_row_indices=[],
                narrative_summary=f"Native step completed. {stats.get('total_changelog_entries', 0)} changes logged.",
                confidence_score=1.0,
            )

            return StepResult(
                step_number=step_number,
                status=StepStatus.COMPLETED,
                strategy=strategy,
                total_rows=total_rows,
                successful_rows=total_rows,
                failed_rows=0,
                output_storage_key=output_storage_key,
                review_report=review,
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )
        else:
            # Native execution failed — surface the diagnostic to the user
            # via the feedback loop instead of silently falling back to codegen
            logger.warning(
                "native_step_diagnosed",
                step=step_number,
                diagnostic=native_result.error,
            )

            columns_info = ""
            if native_result.input_columns:
                columns_info = f" The input file has columns: {native_result.input_columns}."

            review = ReviewReport(
                step_number=step_number,
                total_rows=0,
                passed_rows=0,
                failed_rows=1,
                deterministic_failures=1,
                llm_failures=0,
                failure_patterns=[f"Native code error: {native_result.error}"],
                failed_row_indices=[],
                narrative_summary=(
                    f"Step {step_number} native code failed to execute.{columns_info} "
                    f"Diagnostic: {native_result.error} "
                    f"The step's Python code needs to be updated to match the input data. "
                    f"No fallback was used — the user should review and correct the native code."
                ),
                confidence_score=0.0,
                error_context={
                    "error_type": "native_code_failure",
                    "input_columns": native_result.input_columns or [],
                    "error_message": native_result.error or "",
                    "code_snippet": (native_result.code_content or "")[:4000],
                    "stderr": native_result.stderr[:3000] if native_result.stderr else "",
                },
            )

            return StepResult(
                step_number=step_number,
                status=StepStatus.FAILED,
                strategy=strategy,
                total_rows=0,
                successful_rows=0,
                failed_rows=1,
                output_storage_key=None,
                review_report=review,
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

    # Lazy-load DataFrame for non-native strategies
    async def _ensure_df() -> pd.DataFrame:
        nonlocal df
        if df is None:
            # Prefer parquet key; fall back to input_storage_key only if it's parquet
            parquet_key = parquet_input_key or input_storage_key
            if parquet_key.endswith(".xlsx"):
                # Input is Excel, not Parquet — convert on the fly
                from excel.reader import read_excel_to_parquet
                temp_parquet_key = input_storage_key.replace(".xlsx", ".parquet")
                await read_excel_to_parquet(storage, parquet_key, temp_parquet_key)
                parquet_key = temp_parquet_key
            df = await load_parquet_from_storage(storage, parquet_key)
            if failed_row_indices:
                df = df.iloc[failed_row_indices].copy()
        return df

    if strategy == ProcessingStrategy.CODE_GEN:
        df = await _ensure_df()

        from excel.models import ExcelFileInfo
        file_info_sample = df.head(5).to_dict(orient="records")
        file_info = ExcelFileInfo(
            filename="",
            total_rows=len(df),
            total_columns=len(df.columns),
            sheet_names=["Sheet1"],
            dtypes={col: str(df[col].dtype) for col in df.columns},
            sample_rows=file_info_sample,
        )
        cb = CircuitBreaker(failure_threshold=5, window_seconds=60)
        agent = CodeGenAgent(llm, job_id, execution_service_url, cb)
        output_df = await agent.run(df, rule_content, file_info, step_number)

        if output_df is None:
            logger.warning("codegen_fallback_to_chunk_parallel", step=step_number)
            strategy = ProcessingStrategy.CHUNK_PARALLEL

    if strategy == ProcessingStrategy.CHUNK_PARALLEL:
        df = await _ensure_df()
        rule_hash = hashlib.sha256(rule_content.encode()).hexdigest()[:16]
        pool = AgentPool(max_concurrency=max_concurrency)
        chunks = create_chunks(df, metadata, chunk_size)
        tasks = [
            WorkerAgent(llm, job_id, cache).run(
                chunk_df, rule_content, step_number, chunk_meta.chunk_id, rule_hash
            )
            for chunk_meta, chunk_df in chunks
        ]
        chunk_results = await pool.run_all(tasks)
        output_df = _merge_chunk_results(df, chunk_results)

    if strategy == ProcessingStrategy.SEQUENTIAL:
        df = await _ensure_df()
        rule_hash = hashlib.sha256(rule_content.encode()).hexdigest()[:16]
        agent = WorkerAgent(llm, job_id, cache)
        results = await agent.run(df, rule_content, step_number, 0, rule_hash)
        output_df = _merge_chunk_results(df, [results])

    if output_df is None:
        output_df = df

    await save_df_to_storage(storage, output_df, output_storage_key)

    # Run review
    review_agent = ReviewAgent(llm, job_id)
    review = await review_agent.run(
        output_df, rule_content, step_number, metadata, review_chunk_size
    )

    return StepResult(
        step_number=step_number,
        status=StepStatus.COMPLETED,
        strategy=strategy,
        total_rows=len(df),
        successful_rows=review.passed_rows,
        failed_rows=review.failed_rows,
        output_storage_key=output_storage_key,
        review_report=review,
        started_at=started_at,
        completed_at=datetime.utcnow(),
    )


class NativeStepResult:
    """Result of a native step execution — success or diagnosed failure."""

    def __init__(
        self,
        success: bool,
        result_json: str = "{}",
        output_storage_key: str | None = None,
        error: str | None = None,
        stderr: str = "",
        input_columns: list[str] | None = None,
        code_content: str | None = None,
    ):
        self.success = success
        self.result_json = result_json
        self.output_storage_key = output_storage_key
        self.error = error
        self.stderr = stderr
        self.input_columns = input_columns
        self.code_content = code_content


async def _execute_native_step(
    code_content: str,
    input_storage_key: str,
    output_storage_key: str,
    storage: StorageBackend,
    execution_service_url: str,
    libraries_dir: str | None = None,
    extra_libraries: dict[str, bytes] | None = None,
) -> NativeStepResult:
    """Execute a native step via the execution service. Returns NativeStepResult (always)."""
    # Download the input file from storage
    input_data = await storage.download(input_storage_key)

    # If the input is parquet (e.g. from a previous CODE_GEN fallback),
    # convert it to Excel bytes so native steps can read it
    if input_storage_key.endswith(".parquet"):
        import io
        df_tmp = pd.read_parquet(io.BytesIO(input_data))
        buf = io.BytesIO()
        df_tmp.to_excel(buf, index=False)
        input_data = buf.getvalue()

    # Read input column names for diagnostics on failure
    import io
    try:
        input_columns = list(pd.read_excel(io.BytesIO(input_data), nrows=0).columns)
    except Exception:
        input_columns = None

    # Load library files
    libraries_b64: dict[str, str] = {}
    if libraries_dir:
        lib_path = Path(libraries_dir)
        if lib_path.exists():
            for lib_file in lib_path.glob("*.xlsx"):
                lib_data = lib_file.read_bytes()
                libraries_b64[lib_file.name] = base64.b64encode(lib_data).decode()

    # Merge in extra libraries (e.g., ESJC output for step 6)
    if extra_libraries:
        for name, data in extra_libraries.items():
            libraries_b64[name] = base64.b64encode(data).decode()

    # Call the execution service
    try:
        if execution_service_url.startswith("embedded://") or execution_service_url == "embedded":
            embedded_result = await execute_native_step_embedded(
                code_content=code_content,
                input_data=input_data,
                libraries={name: base64.b64decode(data) for name, data in libraries_b64.items()},
                timeout_seconds=120,
            )

            if not embedded_result.success:
                stderr = embedded_result.stderr[:3000]
                logger.error(
                    "native_step_failed",
                    stderr=stderr,
                    returncode=embedded_result.returncode,
                    result_json=embedded_result.result_json[:500],
                )
                return NativeStepResult(
                    success=False,
                    result_json=embedded_result.result_json,
                    stderr=stderr,
                    input_columns=input_columns,
                    code_content=code_content,
                    error=_diagnose_native_failure(
                        embedded_result.result_json,
                        stderr,
                        input_columns,
                        code_content,
                    ),
                )

            if embedded_result.output_data is not None:
                await storage.upload(output_storage_key, embedded_result.output_data)

            return NativeStepResult(
                success=True,
                result_json=embedded_result.result_json,
                output_storage_key=output_storage_key,
            )

        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(
                f"{execution_service_url}/execute-native",
                json={
                    "code_content": code_content,
                    "input_data_b64": base64.b64encode(input_data).decode(),
                    "libraries": libraries_b64,
                    "timeout_seconds": 120,
                },
            )
            response.raise_for_status()
            result = response.json()

        if not result.get("success"):
            result_json = result.get("result_json", "{}")
            stderr = result.get("stderr", "")[:500]
            logger.error(
                "native_step_failed",
                stderr=stderr,
                returncode=result.get("returncode"),
                result_json=result_json[:500],
            )
            return NativeStepResult(
                success=False,
                result_json=result_json,
                stderr=stderr,
                input_columns=input_columns,
                code_content=code_content,
                error=_diagnose_native_failure(result_json, stderr, input_columns, code_content),
            )

        # Upload the output Excel file to storage
        output_data = base64.b64decode(result["output_data_b64"])
        await storage.upload(output_storage_key, output_data)

        return NativeStepResult(
            success=True,
            result_json=result.get("result_json", "{}"),
            output_storage_key=output_storage_key,
        )

    except Exception as exc:
        logger.error("native_step_error", error=str(exc))
        return NativeStepResult(
            success=False,
            error=f"Execution service error: {exc}",
            input_columns=input_columns,
            code_content=code_content,
        )


def _diagnose_native_failure(
    result_json: str,
    stderr: str,
    input_columns: list[str] | None,
    code_content: str | None,
) -> str:
    """Build a human-readable diagnostic from native step failure info."""
    parts: list[str] = []

    # Extract error from the step's own result
    try:
        result_data = json.loads(result_json)
        stats = result_data.get("stats", {})
        step_error = stats.get("error", "")
        if step_error:
            parts.append(f"Step code error: {step_error}")
    except (json.JSONDecodeError, AttributeError):
        pass

    if stderr:
        parts.append(f"stderr: {stderr[:2000]}")

    if input_columns is not None:
        parts.append(f"Input file columns: {input_columns}")

    if not parts:
        parts.append("Native step failed but produced no output file.")

    return " | ".join(parts)


def _merge_chunk_results(df: pd.DataFrame, chunk_results: list) -> pd.DataFrame:
    """Merge worker results back into a DataFrame."""
    from models.enums import RowStatus
    output = df.copy()

    for results in chunk_results:
        if not results:
            continue
        for row_result in results:
            if row_result.status == RowStatus.SUCCESS and row_result.transformed_data:
                idx = row_result.row_index
                if idx < len(output):
                    # Use direct loc assignment instead of DataFrame.update()
                    # so that intentional NaN/None values are preserved.
                    for col, val in row_result.transformed_data.items():
                        if col in output.columns:
                            output.at[idx, col] = val

    return output
