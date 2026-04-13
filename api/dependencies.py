from __future__ import annotations

from functools import lru_cache
from typing import AsyncGenerator

from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from cache.backends.disk_cache import DiskCache
from cache.backends.memory_cache import MemoryCache
from cache.backends.redis_cache import RedisCache
from cache.cache_manager import CacheManager
from config.settings import Settings
from db.engine import create_session_factory
from llm.factory import create_llm_provider
from llm.rate_limiter import GlobalRateLimiter
from storage.factory import create_storage_backend


@lru_cache
def get_settings() -> Settings:
    return Settings()


# These are module-level singletons set during app lifespan startup
_redis: Redis | None = None
_session_factory = None
_storage = None
_llm = None
_cache: CacheManager | None = None


def set_singletons(
    redis: Redis,
    session_factory,
    storage,
    llm,
    cache: CacheManager,
) -> None:
    global _redis, _session_factory, _storage, _llm, _cache
    _redis = redis
    _session_factory = session_factory
    _storage = storage
    _llm = llm
    _cache = cache


def get_redis() -> Redis:
    assert _redis is not None, "Redis not initialized"
    return _redis


def get_storage():
    assert _storage is not None, "Storage not initialized"
    return _storage


def get_llm():
    assert _llm is not None, "LLM not initialized"
    return _llm


def get_cache() -> CacheManager:
    assert _cache is not None, "Cache not initialized"
    return _cache


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    assert _session_factory is not None, "DB not initialized"
    async with _session_factory() as session:
        yield session
