from __future__ import annotations

from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from config.settings import Settings


async def create_engine(settings: Settings) -> AsyncEngine:
    if settings.DATABASE_URL.startswith(("sqlite+aiosqlite:///", "sqlite:///")):
        db_path = Path(settings.DATABASE_URL.split(":///", 1)[1]).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_async_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        echo=settings.DEBUG,
    )
    if "sqlite" in settings.DATABASE_URL:
        async with engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA busy_timeout=30000"))
            await conn.execute(text("PRAGMA synchronous=NORMAL"))
    return engine


def create_session_factory(engine: AsyncEngine) -> sessionmaker:
    return sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
