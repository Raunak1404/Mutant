from __future__ import annotations

import sys
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from runtime.paths import app_root, default_app_data_dir, resolve_data_path, resolve_resource_path


ENV_FILENAMES = (".env", "weisiong.env", "WeiSiong.env")


def _iter_env_search_roots() -> list[Path]:
    executable = Path(sys.executable).resolve()
    roots = [
        Path.cwd(),
        default_app_data_dir(),
        app_root(),
        executable.parent.parent / "Resources",
        *executable.parents[:5],
    ]

    unique_roots: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        resolved = root.expanduser().resolve()
        if resolved in seen:
            continue
        unique_roots.append(resolved)
        seen.add(resolved)
    return unique_roots


def _env_files() -> tuple[str, ...]:
    existing: list[str] = []
    seen: set[Path] = set()
    for root in _iter_env_search_roots():
        for filename in ENV_FILENAMES:
            candidate = root / filename
            resolved = candidate.resolve()
            if resolved in seen or not resolved.exists():
                continue
            existing.append(str(resolved))
            seen.add(resolved)
    return tuple(existing)


def _resolve_sqlite_url(url: str, app_data_dir: Path) -> str:
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if not url.startswith(prefix):
            continue
        raw_path = url[len(prefix):]
        if not raw_path:
            return f"sqlite+aiosqlite:///{(app_data_dir / 'weisiong.db').resolve().as_posix()}"

        candidate = Path(raw_path).expanduser()
        if not candidate.is_absolute():
            candidate = app_data_dir / candidate
        return f"{prefix}{candidate.resolve().as_posix()}"
    return url


def _sqlite_path_from_url(url: str) -> Path | None:
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if url.startswith(prefix):
            return Path(url[len(prefix):]).expanduser()
    return None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_env_files(),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Runtime / Packaging ───────────────────────────────────────────────
    APP_DATA_DIR: str = ""
    JOB_RUNNER: str = "local"  # "local" | "taskiq"
    USE_REDIS: bool = False

    # ── LLM ──────────────────────────────────────────────────────────────
    LLM_PROVIDER: str = "claude"  # "claude" | "azure_openai"

    # Claude (Anthropic)
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_MODEL: str = "claude-sonnet-4-6"

    # Azure OpenAI
    AZURE_OPENAI_ENDPOINT: str = ""
    AZURE_OPENAI_API_KEY: str = ""
    AZURE_OPENAI_API_VERSION: str = "2024-08-01-preview"
    AZURE_OPENAI_DEPLOYMENT: str = "gpt-4o"

    # ── Database ──────────────────────────────────────────────────────────
    DATABASE_URL: str = ""
    # Production: "postgresql+asyncpg://user:pass@host:5432/weisiong"

    # ── Storage ───────────────────────────────────────────────────────────
    STORAGE_BACKEND: str = "local"  # "local" | "s3" | "azure_blob"
    STORAGE_LOCAL_DIR: str = ""

    # S3 / MinIO
    S3_BUCKET: str = "weisiong"
    S3_ENDPOINT_URL: str = ""  # empty = real AWS; set to MinIO URL for local
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"

    # Azure Blob Storage
    AZURE_STORAGE_CONNECTION_STRING: str = ""
    AZURE_STORAGE_CONTAINER: str = "weisiong"

    # ── Execution Service ─────────────────────────────────────────────────
    EXECUTION_SERVICE_URL: str = "embedded://local"
    EXECUTION_TIMEOUT_SECONDS: int = 60

    # ── Redis ─────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379"

    # ── Rate Limiter ──────────────────────────────────────────────────────
    RATE_LIMIT_RPM: int = 50
    RATE_LIMIT_TPM: int = 100_000

    # ── Cache ─────────────────────────────────────────────────────────────
    CACHE_ENABLED: bool = True
    CACHE_MEMORY_MAX_SIZE: int = 512        # L0 in-process LRU entries
    CACHE_DISK_DIR: str = ""                # L2 diskcache directory
    CACHE_TTL_SECONDS: int = 3600           # default TTL

    # ── Processing ────────────────────────────────────────────────────────
    CHUNK_SIZE_ROWS: int = 100              # strategy B chunk size
    REVIEW_CHUNK_SIZE_ROWS: int = 300       # review map phase chunk size
    MAX_WORKER_CONCURRENCY: int = 10        # max parallel worker agents

    # ── Native Step Execution ─────────────────────────────────────────────
    STEPS_CODE_DIR: str = "steps"           # directory containing step*_logic.py files
    LIBRARIES_DIR: str = "libraries"        # directory containing reference Excel libraries

    # ── API ───────────────────────────────────────────────────────────────
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = False

    @model_validator(mode="after")
    def resolve_paths(self) -> "Settings":
        if not self.APP_DATA_DIR:
            self.APP_DATA_DIR = str(default_app_data_dir())

        app_data_dir = Path(self.APP_DATA_DIR).expanduser().resolve()
        app_data_dir.mkdir(parents=True, exist_ok=True)
        self.APP_DATA_DIR = str(app_data_dir)

        if not self.DATABASE_URL:
            self.DATABASE_URL = f"sqlite+aiosqlite:///{(app_data_dir / 'weisiong.db').resolve().as_posix()}"
        else:
            self.DATABASE_URL = _resolve_sqlite_url(self.DATABASE_URL, app_data_dir)
        sqlite_path = _sqlite_path_from_url(self.DATABASE_URL)
        if sqlite_path is not None:
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.STORAGE_LOCAL_DIR:
            self.STORAGE_LOCAL_DIR = str((app_data_dir / "storage").resolve())
        else:
            self.STORAGE_LOCAL_DIR = str(resolve_data_path(self.STORAGE_LOCAL_DIR, app_data_dir))

        if not self.CACHE_DISK_DIR:
            self.CACHE_DISK_DIR = str((app_data_dir / "cache").resolve())
        else:
            self.CACHE_DISK_DIR = str(resolve_data_path(self.CACHE_DISK_DIR, app_data_dir))

        self.STEPS_CODE_DIR = str(resolve_resource_path(self.STEPS_CODE_DIR))
        self.LIBRARIES_DIR = str(resolve_resource_path(self.LIBRARIES_DIR))

        if self.JOB_RUNNER == "local" and self.EXECUTION_SERVICE_URL in {"", "http://localhost:8001"}:
            self.EXECUTION_SERVICE_URL = "embedded://local"
        return self
