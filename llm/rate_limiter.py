from __future__ import annotations

import asyncio
import time

from redis.asyncio import Redis

from utils.logging import get_logger

logger = get_logger(__name__)

# Lua token-bucket script — atomic, cross-process safe
BUCKET_SCRIPT = """
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or max_tokens
local last = tonumber(bucket[2]) or now

local elapsed = now - last
local refilled = math.min(max_tokens, tokens + elapsed * refill_rate)

if refilled >= requested then
    redis.call('HMSET', key, 'tokens', refilled - requested, 'last_refill', now)
    redis.call('EXPIRE', key, 120)
    return 1
end
return 0
"""


class GlobalRateLimiter:
    """Redis Lua token-bucket rate limiter. Safe across multiple processes/pods."""

    def __init__(self, redis: Redis, rpm: int = 50, tpm: int = 100_000) -> None:
        self.redis = redis
        self.rpm = rpm
        self.tpm = tpm

    async def acquire(self, estimated_tokens: int = 500) -> None:
        """Block until both RPM and TPM buckets have capacity."""
        while True:
            now = time.time()
            # Check TPM first WITHOUT deducting RPM — avoids burning RPM
            # tokens when TPM is insufficient.
            tpm_ok = await self.redis.eval(
                BUCKET_SCRIPT, 1, "rate:tpm",
                self.tpm, self.tpm / 60.0, estimated_tokens, now,
            )
            if not tpm_ok:
                await asyncio.sleep(0.1)
                continue
            rpm_ok = await self.redis.eval(
                BUCKET_SCRIPT, 1, "rate:rpm",
                self.rpm, self.rpm / 60.0, 1, now,
            )
            if rpm_ok:
                return
            await asyncio.sleep(0.1)

    async def adapt_from_headers(self, headers: dict) -> None:
        """Proactively slow down when remaining budget is low."""
        try:
            remaining = int(headers.get("anthropic-ratelimit-requests-remaining", self.rpm))
        except (ValueError, TypeError):
            remaining = self.rpm
        if remaining < self.rpm * 0.2:
            logger.info("rate_limit_proactive_slowdown", remaining=remaining)
            await asyncio.sleep(1.0)
