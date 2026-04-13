from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine


class AgentPool:
    """
    Bounded concurrency pool using asyncio.TaskGroup + semaphore.
    Runs up to max_concurrency tasks simultaneously.
    """

    def __init__(self, max_concurrency: int = 10) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def run_all(
        self,
        tasks: list[Coroutine],
    ) -> list[Any]:
        """Run all coroutines concurrently, return results in input order.

        Partial failures are logged but do not discard successful results.
        Only raises if ALL tasks failed.
        """
        results: list[Any] = [None] * len(tasks)
        errors: list[Exception | None] = [None] * len(tasks)

        async def _run(index: int, coro: Coroutine) -> None:
            async with self._semaphore:
                try:
                    results[index] = await coro
                except Exception as exc:
                    errors[index] = exc

        async with asyncio.TaskGroup() as tg:
            for i, coro in enumerate(tasks):
                tg.create_task(_run(i, coro))

        # Log individual chunk failures but return partial results.
        # Only raise if every single task failed.
        error_count = sum(1 for e in errors if e is not None)
        if error_count > 0:
            from utils.logging import get_logger
            _logger = get_logger(__name__)
            for i, err in enumerate(errors):
                if err is not None:
                    _logger.warning("agent_pool_task_error", index=i, error=str(err))
            if error_count == len(tasks):
                # All tasks failed — raise the first error
                for err in errors:
                    if err is not None:
                        raise err

        return results
