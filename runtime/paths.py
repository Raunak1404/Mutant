from __future__ import annotations

import sys
from pathlib import Path


def app_root() -> Path:
    """Return the directory that contains packaged resources."""
    if getattr(sys, "frozen", False):
        bundle_root = getattr(sys, "_MEIPASS", None)
        if bundle_root:
            return Path(bundle_root).resolve()
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def default_app_data_dir() -> Path:
    """Return a writable per-user directory for local app state."""
    return (Path.home() / ".weisiong").resolve()


def resolve_resource_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (app_root() / path).resolve()


def resolve_data_path(path_value: str | Path, data_dir: str | Path | None = None) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    base_dir = Path(data_dir).expanduser() if data_dir else default_app_data_dir()
    return (base_dir / path).resolve()
