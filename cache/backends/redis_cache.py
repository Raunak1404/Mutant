from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis


class RedisCache:
    """L1: Redis exact-match cache. Shared across all processes/pods."""

    def __init__(self, redis: Redis, default_ttl: int = 3600) -> None:
        self._redis = redis
        self._ttl = default_ttl

    async def get(self, key: str) -> Any | None:
        value = await self._redis.get(key)
        if value is None:
            return None
        return json.loads(value)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        serialized = json.dumps(value, default=str)
        await self._redis.setex(key, ttl or self._ttl, serialized)

    async def delete(self, key: str) -> None:
        await self._redis.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self._redis.exists(key))
