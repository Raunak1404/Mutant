from __future__ import annotations

import asyncio
from typing import Any

import diskcache


class DiskCache:
    """L2: diskcache SQLite-backed persistent cache. Per-node, not shared."""

    def __init__(self, directory: str = "./data/cache", default_ttl: int = 3600) -> None:
        self._cache = diskcache.Cache(directory)
        self._ttl = default_ttl

    def get(self, key: str) -> Any | None:
        return self._cache.get(key)

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        self._cache.set(key, value, expire=ttl or self._ttl)

    def delete(self, key: str) -> None:
        self._cache.delete(key)

    async def aget(self, key: str) -> Any | None:
        return await asyncio.to_thread(self.get, key)

    async def aset(self, key: str, value: Any, ttl: int | None = None) -> None:
        await asyncio.to_thread(self.set, key, value, ttl)

    def close(self) -> None:
        self._cache.close()
