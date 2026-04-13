from __future__ import annotations

import hashlib
import json
from typing import Any

from cache.backends.disk_cache import DiskCache
from cache.backends.memory_cache import MemoryCache
from cache.backends.redis_cache import RedisCache
from cache.bucketed import bucketed_key, exact_key
from utils.logging import get_logger

logger = get_logger(__name__)


class CacheManager:
    """
    Multi-tier cache facade.
      L0: In-process LRU (MemoryCache)
      L1: Redis exact-match (RedisCache)
      L2: diskcache SQLite (DiskCache)
      L2.5: Bucketed similarity (optional, same backends with bucketed key)
    """

    def __init__(
        self,
        memory: MemoryCache,
        redis: RedisCache,
        disk: DiskCache,
        enabled: bool = True,
        use_bucketed: bool = True,
    ) -> None:
        self._memory = memory
        self._redis = redis
        self._disk = disk
        self._enabled = enabled
        self._use_bucketed = use_bucketed

    async def get_row_result(
        self, step: int, rule_hash: str, row: dict[str, Any]
    ) -> Any | None:
        if not self._enabled:
            return None

        k = exact_key(step, rule_hash, row)

        # L0
        hit = self._memory.get(k)
        if hit is not None:
            logger.debug("cache_hit", tier="L0", key=k[:16])
            return hit

        # L1
        hit = await self._redis.get(k)
        if hit is not None:
            self._memory.set(k, hit)
            logger.debug("cache_hit", tier="L1", key=k[:16])
            return hit

        # L2
        hit = await self._disk.aget(k)
        if hit is not None:
            self._memory.set(k, hit)
            await self._redis.set(k, hit)
            logger.debug("cache_hit", tier="L2", key=k[:16])
            return hit

        # L2.5 bucketed
        if self._use_bucketed:
            bk = bucketed_key(step, rule_hash, row)
            hit = await self._disk.aget(bk)
            if hit is not None:
                logger.debug("cache_hit", tier="L2.5_bucketed", key=bk[:16])
                return hit

        return None

    async def set_row_result(
        self, step: int, rule_hash: str, row: dict[str, Any], value: Any
    ) -> None:
        if not self._enabled:
            return

        k = exact_key(step, rule_hash, row)
        self._memory.set(k, value)
        await self._redis.set(k, value)
        await self._disk.aset(k, value)

        if self._use_bucketed:
            bk = bucketed_key(step, rule_hash, row)
            await self._disk.aset(bk, value)

    def rule_hash(self, rule_content: str) -> str:
        return hashlib.sha256(rule_content.encode()).hexdigest()[:16]
