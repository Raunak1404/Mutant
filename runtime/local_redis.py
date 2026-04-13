from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass
class _ValueEntry:
    value: Any
    expires_at: float | None = None


class LocalPubSub:
    def __init__(self, redis: "LocalRedis") -> None:
        self._redis = redis
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._channels: set[str] = set()
        self._closed = False

    async def __aenter__(self) -> "LocalPubSub":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def subscribe(self, channel: str) -> None:
        if channel in self._channels:
            return
        self._channels.add(channel)
        await self._redis._register_subscriber(channel, self._queue)

    async def unsubscribe(self, channel: str) -> None:
        if channel not in self._channels:
            return
        self._channels.remove(channel)
        await self._redis._unregister_subscriber(channel, self._queue)
        await self._queue.put({"type": "unsubscribe", "channel": channel, "data": None})

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for channel in list(self._channels):
            await self.unsubscribe(channel)
        await self._queue.put(None)

    async def listen(self):
        while True:
            item = await self._queue.get()
            if item is None:
                break
            if item.get("type") == "unsubscribe" and not self._channels:
                break
            yield item


class LocalRedis:
    """Small async Redis substitute for local single-process execution."""

    def __init__(self) -> None:
        self._kv: dict[str, _ValueEntry] = {}
        self._lists: dict[str, list[Any]] = defaultdict(list)
        self._list_expiry: dict[str, float] = {}
        self._subscribers: dict[str, set[asyncio.Queue[Any]]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def aclose(self) -> None:
        async with self._lock:
            self._kv.clear()
            self._lists.clear()
            self._list_expiry.clear()
            subscribers = list(self._subscribers.values())
            self._subscribers.clear()
        for queues in subscribers:
            for queue in queues:
                await queue.put(None)

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            self._purge_expired_locked(key)
            entry = self._kv.get(key)
            return entry.value if entry else None

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        expires_at = time.time() + ttl if ttl else None
        async with self._lock:
            self._kv[key] = _ValueEntry(value=value, expires_at=expires_at)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._kv.pop(key, None)
            self._lists.pop(key, None)
            self._list_expiry.pop(key, None)

    async def exists(self, key: str) -> int:
        async with self._lock:
            self._purge_expired_locked(key)
            return int(key in self._kv or key in self._lists)

    async def rpush(self, key: str, value: Any) -> int:
        async with self._lock:
            self._purge_list_locked(key)
            self._lists[key].append(value)
            return len(self._lists[key])

    async def expire(self, key: str, ttl: int) -> bool:
        expires_at = time.time() + ttl if ttl else None
        async with self._lock:
            if key in self._kv:
                self._kv[key].expires_at = expires_at
                return True
            if key in self._lists:
                self._list_expiry[key] = expires_at or 0
                return True
            return False

    async def lrange(self, key: str, start: int, end: int) -> list[Any]:
        async with self._lock:
            self._purge_list_locked(key)
            values = list(self._lists.get(key, []))
        if not values:
            return []
        if end == -1:
            end = len(values) - 1
        return values[start:end + 1]

    async def publish(self, channel: str, message: Any) -> int:
        async with self._lock:
            queues = list(self._subscribers.get(channel, set()))
        for queue in queues:
            await queue.put({"type": "message", "channel": channel, "data": message})
        return len(queues)

    def pubsub(self) -> LocalPubSub:
        return LocalPubSub(self)

    async def eval(
        self,
        _script: str,
        _numkeys: int,
        key: str,
        max_tokens: int,
        refill_rate: float,
        requested: int,
        now: float,
    ) -> int:
        async with self._lock:
            self._purge_expired_locked(key)
            entry = self._kv.get(key)
            bucket = entry.value if entry else {"tokens": float(max_tokens), "last_refill": now}

            tokens = float(bucket.get("tokens", max_tokens))
            last_refill = float(bucket.get("last_refill", now))
            elapsed = max(0.0, now - last_refill)
            refilled = min(float(max_tokens), tokens + elapsed * float(refill_rate))

            if refilled >= requested:
                self._kv[key] = _ValueEntry(
                    value={"tokens": refilled - requested, "last_refill": now},
                    expires_at=now + 120,
                )
                return 1
            self._kv[key] = _ValueEntry(
                value={"tokens": refilled, "last_refill": now},
                expires_at=now + 120,
            )
            return 0

    async def _register_subscriber(self, channel: str, queue: asyncio.Queue[Any]) -> None:
        async with self._lock:
            self._subscribers[channel].add(queue)

    async def _unregister_subscriber(self, channel: str, queue: asyncio.Queue[Any]) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(channel)
            if not subscribers:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(channel, None)

    def _purge_expired_locked(self, key: str) -> None:
        entry = self._kv.get(key)
        if entry and entry.expires_at is not None and entry.expires_at <= time.time():
            self._kv.pop(key, None)
        self._purge_list_locked(key)

    def _purge_list_locked(self, key: str) -> None:
        expires_at = self._list_expiry.get(key)
        if expires_at and expires_at <= time.time():
            self._list_expiry.pop(key, None)
            self._lists.pop(key, None)


_SHARED_LOCAL_REDIS: LocalRedis | None = None


def get_shared_local_redis() -> LocalRedis:
    global _SHARED_LOCAL_REDIS
    if _SHARED_LOCAL_REDIS is None:
        _SHARED_LOCAL_REDIS = LocalRedis()
    return _SHARED_LOCAL_REDIS
