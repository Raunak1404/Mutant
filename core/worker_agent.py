from __future__ import annotations

import json
from typing import Any

import pandas as pd

from cache.cache_manager import CacheManager
from core.agent_base import AgentBase
from models.messages import Message
from models.results import RowResult
from models.enums import RowStatus
from utils.logging import get_logger

logger = get_logger(__name__)


WORKER_SYSTEM_PROMPT = """You are a data transformation expert.
Apply the given rules to each row of the input data.
Return a JSON array where each element has:
  - "row_index": integer (matching input)
  - "transformed": object (transformed row data)
  - "status": "success" | "failed"
  - "failure_reason": string or null

Return ONLY the JSON array, no explanation."""


class WorkerAgent(AgentBase):
    """
    Strategy B: Chunk-parallel row processing via LLM.
    Each instance handles one DataFrame chunk.
    """

    def __init__(
        self,
        llm,
        job_id: str,
        cache: CacheManager | None = None,
    ) -> None:
        super().__init__(llm, job_id)
        self._cache = cache

    async def run(
        self,
        chunk_df: pd.DataFrame,
        rule_content: str,
        step_number: int,
        chunk_id: int,
        rule_hash: str,
    ) -> list[RowResult]:
        """Process a DataFrame chunk. Returns RowResult for each primary row."""
        rows = chunk_df[chunk_df.get("_overlap_context", pd.Series([False] * len(chunk_df))) == False].to_dict(orient="records")

        # Check cache for each row
        results: list[RowResult] = []
        uncached_rows: list[tuple[int, dict]] = []

        for pos, row in enumerate(rows):
            row_idx = int(row.get("_original_index", pos))
            if self._cache:
                cached = await self._cache.get_row_result(step_number, rule_hash, row)
                if cached:
                    results.append(RowResult(
                        row_index=row_idx,
                        status=RowStatus.SUCCESS,
                        original_data=row,
                        transformed_data=cached,
                    ))
                    continue
            uncached_rows.append((row_idx, row))

        if not uncached_rows:
            return results

        # Batch LLM call for uncached rows
        batch_results = await self._process_batch(uncached_rows, rule_content, step_number)

        # Cache successful results
        for result in batch_results:
            if result.status == RowStatus.SUCCESS and self._cache and result.transformed_data:
                original = next((r for _, r in uncached_rows if _ == result.row_index), None)
                if original:
                    await self._cache.set_row_result(
                        step_number, rule_hash, original, result.transformed_data
                    )

        results.extend(batch_results)
        return results

    async def _process_batch(
        self,
        rows: list[tuple[int, dict]],
        rule_content: str,
        step_number: int,
    ) -> list[RowResult]:
        rows_json = json.dumps(
            [{"row_index": idx, "data": row} for idx, row in rows],
            default=str,
            indent=2,
        )
        user_msg = f"""Step {step_number} rules:
{rule_content}

Rows to transform (JSON):
{rows_json}

Apply the rules to each row. Return a JSON array of results."""

        try:
            response = await self.llm.complete(
                system_prompt=WORKER_SYSTEM_PROMPT,
                messages=[Message(role="user", content=user_msg)],
                temperature=0.0,
                max_tokens=4096,
            )
            return _parse_worker_response(response.content, rows)
        except Exception as exc:
            logger.error("worker_llm_error", job_id=self.job_id, chunk=step_number, error=str(exc))
            return [
                RowResult(
                    row_index=idx,
                    status=RowStatus.FAILED,
                    original_data=row,
                    failure_reason=str(exc),
                )
                for idx, row in rows
            ]


def _parse_worker_response(
    content: str, original_rows: list[tuple[int, dict]]
) -> list[RowResult]:
    """Parse LLM JSON response into RowResult list."""
    try:
        if "```json" in content:
            content = content.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in content:
            content = content.split("```", 1)[1].split("```", 1)[0]

        data = json.loads(content.strip())
        results = []
        for item in data:
            results.append(RowResult(
                row_index=item["row_index"],
                status=RowStatus(item.get("status", "success")),
                original_data=next(
                    (r for idx, r in original_rows if idx == item["row_index"]), {}
                ),
                transformed_data=item.get("transformed"),
                failure_reason=item.get("failure_reason"),
            ))
        return results
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.warning("worker_parse_error", error=str(exc))
        return [
            RowResult(
                row_index=idx,
                status=RowStatus.FAILED,
                original_data=row,
                failure_reason=f"Parse error: {exc}",
            )
            for idx, row in original_rows
        ]
