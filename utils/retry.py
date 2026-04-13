from __future__ import annotations

import asyncio
import functools
import random
import time
from collections import deque
from typing import Callable, TypeVar

from utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


def async_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """Exponential backoff decorator for async functions."""

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await fn(*args, **kwargs)
                except retryable_exceptions as exc:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.warning(
                            "max_retries_exceeded",
                            fn=fn.__name__,
                            attempts=attempt,
                            error=str(exc),
                        )
                        raise

                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if jitter:
                        delay *= 0.5 + random.random() * 0.5

                    # Respect Retry-After header if available
                    retry_after = getattr(exc, "retry_after", None)
                    if retry_after:
                        delay = max(delay, float(retry_after))

                    logger.info(
                        "retrying",
                        fn=fn.__name__,
                        attempt=attempt,
                        delay_s=round(delay, 2),
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator


class CircuitBreaker:
    """Simple circuit breaker: open after N failures in a time window."""

    def __init__(self, failure_threshold: int = 5, window_seconds: float = 60.0) -> None:
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self._failures: deque[float] = deque()
        self._open_until: float = 0.0

    @property
    def is_open(self) -> bool:
        now = time.monotonic()
        if now < self._open_until:
            return True
        # Prune old failures
        cutoff = now - self.window_seconds
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()
        return False

    def record_failure(self) -> None:
        now = time.monotonic()
        # Prune expired failures before checking threshold so stale entries
        # from outside the window don't falsely trigger the circuit breaker.
        cutoff = now - self.window_seconds
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()
        self._failures.append(now)
        if len(self._failures) >= self.failure_threshold:
            self._open_until = now + self.window_seconds
            logger.warning(
                "circuit_breaker_opened",
                failures=len(self._failures),
                window_s=self.window_seconds,
            )

    def record_success(self) -> None:
        self._failures.clear()
        self._open_until = 0.0
