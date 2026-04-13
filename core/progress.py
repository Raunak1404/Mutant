from __future__ import annotations

import inspect
import json

from models.messages import ProgressEvent
from utils.logging import get_logger

logger = get_logger(__name__)


class ProgressPublisher:
    """Publish job progress events to Redis pub/sub + list for WebSocket streaming."""

    CHANNEL_PREFIX = "job:progress:"
    HISTORY_PREFIX = "job:history:"
    HISTORY_TTL = 3600  # 1 hour

    def __init__(self, redis) -> None:
        self._redis = redis

    def _channel(self, job_id: str) -> str:
        return f"{self.CHANNEL_PREFIX}{job_id}"

    def _history_key(self, job_id: str) -> str:
        return f"{self.HISTORY_PREFIX}{job_id}"

    async def publish(self, event: ProgressEvent) -> None:
        channel = self._channel(event.job_id)
        history_key = self._history_key(event.job_id)
        message = event.model_dump_json()
        # Store in list for late-joining WebSocket clients, then publish
        await self._call("rpush", history_key, message)
        await self._call("expire", history_key, self.HISTORY_TTL)
        await self._call("publish", channel, message)
        logger.debug("progress_published", job_id=event.job_id, event_type=event.event_type)

    async def get_history(self, job_id: str) -> list[str]:
        """Return all stored events for a job (for late-joining clients)."""
        events = await self._call("lrange", self._history_key(job_id), 0, -1) or []
        return [
            e.decode() if isinstance(e, bytes) else e
            for e in events
        ]

    async def _call(self, method_name: str, *args):
        method = getattr(self._redis, method_name, None)
        if method is None:
            return None
        result = method(*args)
        if inspect.isawaitable(result):
            return await result
        return result

    async def publish_step_started(self, job_id: str, step: int, strategy: str) -> None:
        await self.publish(ProgressEvent(
            job_id=job_id,
            event_type="step_started",
            step_number=step,
            message=f"Step {step} started using {strategy} strategy",
            data={"strategy": strategy},
        ))

    async def publish_step_completed(self, job_id: str, step: int, result_summary: dict) -> None:
        await self.publish(ProgressEvent(
            job_id=job_id,
            event_type="step_completed",
            step_number=step,
            message=f"Step {step} completed",
            data=result_summary,
        ))

    async def publish_awaiting_feedback(self, job_id: str, questions: list[dict]) -> None:
        await self.publish(ProgressEvent(
            job_id=job_id,
            event_type="awaiting_feedback",
            message="Processing paused — awaiting user feedback",
            data={"question_count": len(questions)},
        ))

    async def publish_completed(self, job_id: str, output_key: str) -> None:
        await self.publish(ProgressEvent(
            job_id=job_id,
            event_type="completed",
            message="Job completed successfully",
            data={"output_key": output_key},
        ))

    async def publish_failed(self, job_id: str, error: str) -> None:
        await self.publish(ProgressEvent(
            job_id=job_id,
            event_type="failed",
            message=f"Job failed: {error}",
            data={"error": error},
        ))
