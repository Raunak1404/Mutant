from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI

import main


class _DummySettings:
    DEBUG = False
    LLM_PROVIDER = "claude"
    STORAGE_BACKEND = "local"
    JOB_RUNNER = "local"
    USE_REDIS = False
    EXECUTION_SERVICE_URL = "embedded://local"
    STEPS_CODE_DIR = "/resolved/steps"


class _DummyServices:
    def __init__(self) -> None:
        self.redis = None
        self.storage = None
        self.llm = None
        self.cache = None

        @asynccontextmanager
        async def _session_factory():
            yield "session"

        self.session_factory = _session_factory


@pytest.mark.asyncio
async def test_lifespan_seeds_rules_from_resolved_steps_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    services = _DummyServices()
    rule_calls: list[tuple[object, str]] = []
    code_calls: list[tuple[object, str]] = []

    monkeypatch.setattr(main, "Settings", lambda: _DummySettings())
    monkeypatch.setattr(main, "configure_logging", lambda debug: None)
    monkeypatch.setattr(main, "set_singletons", lambda *args, **kwargs: None)

    async def _create_runtime_services(settings: _DummySettings) -> _DummyServices:
        return services

    async def _close_runtime_services(passed_services: _DummyServices) -> None:
        assert passed_services is services

    async def _seed_rules_from_files(session: object, steps_dir: str) -> None:
        rule_calls.append((session, steps_dir))

    async def _seed_code_from_files(session: object, steps_dir: str) -> None:
        code_calls.append((session, steps_dir))

    monkeypatch.setattr(main, "create_runtime_services", _create_runtime_services)
    monkeypatch.setattr(main, "close_runtime_services", _close_runtime_services)
    monkeypatch.setattr(main, "seed_rules_from_files", _seed_rules_from_files)
    monkeypatch.setattr(main, "seed_code_from_files", _seed_code_from_files)

    async with main.lifespan(FastAPI()):
        pass

    assert rule_calls == [("session", "/resolved/steps")]
    assert code_calls == [("session", "/resolved/steps")]
