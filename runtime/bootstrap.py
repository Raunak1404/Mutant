from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

from cache.backends.disk_cache import DiskCache
from cache.backends.memory_cache import MemoryCache
from cache.backends.redis_cache import RedisCache
from cache.cache_manager import CacheManager
from config.settings import Settings
from db.engine import create_engine, create_session_factory
from db.models import Base
from llm.factory import create_llm_provider
from llm.rate_limiter import GlobalRateLimiter
from runtime.local_redis import get_shared_local_redis
from storage.factory import create_storage_backend


@dataclass
class RuntimeServices:
    redis: Any
    engine: Any
    session_factory: Any
    storage: Any
    llm: Any
    cache: CacheManager
    disk_cache: DiskCache


def create_redis_client(settings: Settings):
    if settings.USE_REDIS:
        return Redis.from_url(settings.REDIS_URL, decode_responses=False)
    return get_shared_local_redis()


async def create_runtime_services(
    settings: Settings,
    *,
    ensure_schema: bool = True,
) -> RuntimeServices:
    redis = create_redis_client(settings)
    engine = await create_engine(settings)
    session_factory = create_session_factory(engine)

    if ensure_schema:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    rate_limiter = GlobalRateLimiter(redis, rpm=settings.RATE_LIMIT_RPM, tpm=settings.RATE_LIMIT_TPM)
    llm = create_llm_provider(settings, rate_limiter)
    storage = create_storage_backend(settings)

    memory_cache = MemoryCache(max_size=settings.CACHE_MEMORY_MAX_SIZE)
    redis_cache = RedisCache(redis, default_ttl=settings.CACHE_TTL_SECONDS)
    disk_cache = DiskCache(directory=settings.CACHE_DISK_DIR, default_ttl=settings.CACHE_TTL_SECONDS)
    cache = CacheManager(memory_cache, redis_cache, disk_cache, enabled=settings.CACHE_ENABLED)

    return RuntimeServices(
        redis=redis,
        engine=engine,
        session_factory=session_factory,
        storage=storage,
        llm=llm,
        cache=cache,
        disk_cache=disk_cache,
    )


async def close_runtime_services(services: RuntimeServices) -> None:
    if hasattr(services.storage, "close"):
        await services.storage.close()
    if hasattr(services.llm, "close"):
        await services.llm.close()
    services.disk_cache.close()
    # Only close Redis if it is NOT the shared local singleton — closing the
    # singleton destroys all progress history, pub/sub subscriptions, and rate
    # limiter state for the entire process.
    shared = get_shared_local_redis()
    if services.redis is not shared:
        await services.redis.aclose()
    await services.engine.dispose()
