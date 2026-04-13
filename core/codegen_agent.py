from __future__ import annotations

import base64
import io
import json

import httpx
import pandas as pd
import pyarrow.parquet as pq

from core.agent_base import AgentBase
from excel.models import ExcelFileInfo
from execution_service.sandbox import execute_code_in_subprocess
from models.messages import Message
from models.results import RuleMetadata
from utils.logging import get_logger
from utils.retry import CircuitBreaker, async_retry

logger = get_logger(__name__)


CODEGEN_SYSTEM_PROMPT = """You are an expert pandas data engineer.
Given transformation rules and sample data, write a Python function that transforms a pandas DataFrame.

Requirements:
- Read from `input_df` (already loaded)
- Produce `output_df` with the same columns unless rules specify otherwise
- Use only: pandas, numpy (already imported as pd, np)
- Do NOT import os, sys, subprocess, or any network library
- Handle missing/empty values gracefully
- The code must be production-safe and deterministic
"""


class CodeGenAgent(AgentBase):
    """
    Strategy A: LLM generates pandas transformation code,
    executed safely via the execution microservice.
    """

    def __init__(
        self,
        llm,
        job_id: str,
        execution_service_url: str,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        super().__init__(llm, job_id)
        self._exec_url = execution_service_url
        self._cb = circuit_breaker or CircuitBreaker(failure_threshold=5, window_seconds=60)

    async def run(
        self,
        df: pd.DataFrame,
        rule_content: str,
        file_info: ExcelFileInfo,
        step_number: int,
        max_retries: int = 2,
    ) -> pd.DataFrame | None:
        """Generate and execute pandas code. Returns transformed DataFrame or None on failure."""
        code = await self._generate_code(rule_content, file_info, step_number)
        if not code:
            return None

        last_error: str = "execution failed"
        for attempt in range(max_retries + 1):
            result = await self._execute(df, code)
            if isinstance(result, pd.DataFrame):
                self._cb.record_success()
                return result

            # Capture the actual error detail for self-healing
            if isinstance(result, str):
                last_error = result

            if attempt < max_retries:
                logger.info(
                    "codegen_retry",
                    job_id=self.job_id,
                    step=step_number,
                    attempt=attempt + 1,
                )
                code = await self._generate_code(
                    rule_content, file_info, step_number,
                    previous_error=last_error,
                )

        self._cb.record_failure()
        return None

    async def _generate_code(
        self,
        rule_content: str,
        file_info: ExcelFileInfo,
        step_number: int,
        previous_error: str | None = None,
    ) -> str | None:
        sample_json = json.dumps(file_info.sample_rows[:3], default=str, indent=2)
        error_section = f"\n\nPrevious attempt failed with:\n{previous_error}" if previous_error else ""

        user_msg = f"""Step {step_number} transformation rules:
{rule_content}

DataFrame columns: {list(file_info.dtypes.keys())}
Sample rows (first 3):
{sample_json}
{error_section}

Write Python code that transforms `input_df` into `output_df`.
Output ONLY the Python code block, no explanation."""

        try:
            response = await self.llm.complete(
                system_prompt=CODEGEN_SYSTEM_PROMPT,
                messages=[Message(role="user", content=user_msg)],
                temperature=0.0,
            )
            return _extract_code(response.content)
        except Exception as exc:
            logger.error("codegen_llm_error", job_id=self.job_id, error=str(exc))
            return None

    async def _execute(self, df: pd.DataFrame, code: str) -> pd.DataFrame | None:
        if self._cb.is_open:
            logger.warning("circuit_breaker_open_skip_codegen", job_id=self.job_id)
            return None

        import pyarrow as pa

        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf)
        input_data = buf.getvalue()

        try:
            if self._exec_url.startswith("embedded://") or self._exec_url == "embedded":
                result = await execute_code_in_subprocess(
                    code=code,
                    input_data=input_data,
                    timeout_seconds=60,
                )
                if result.success and result.output_data:
                    return pq.read_table(io.BytesIO(result.output_data)).to_pandas()
                logger.warning("codegen_exec_failed", stderr=result.stderr[:500])
                return result.stderr[:1000] if result.stderr else None

            input_b64 = base64.b64encode(input_data).decode()
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    f"{self._exec_url}/execute",
                    json={"code": code, "input_data_b64": input_b64, "timeout_seconds": 60},
                )
                resp.raise_for_status()
                data = resp.json()

            if data["success"] and data.get("output_data_b64"):
                output_bytes = base64.b64decode(data["output_data_b64"])
                buf_out = io.BytesIO(output_bytes)
                return pq.read_table(buf_out).to_pandas()

            stderr = data.get("stderr", "")[:1000]
            logger.warning("codegen_exec_failed", stderr=stderr[:500])
            return stderr if stderr else None
        except Exception as exc:
            self._cb.record_failure()
            logger.error("codegen_exec_error", error=str(exc))
            return None


def _extract_code(text: str) -> str:
    """Strip markdown code fences if present."""
    if "```python" in text:
        text = text.split("```python", 1)[1]
        text = text.split("```", 1)[0]
    elif "```" in text:
        text = text.split("```", 1)[1]
        text = text.split("```", 1)[0]
    return text.strip()
