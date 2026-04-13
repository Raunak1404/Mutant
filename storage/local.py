from __future__ import annotations

import asyncio
from pathlib import Path


class LocalStorageBackend:
    """Filesystem-based storage for local development."""

    def __init__(self, base_dir: str = "./data") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.base_dir / key

    async def upload(self, key: str, data: bytes) -> None:
        path = self._path(key)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_bytes, data)

    async def download(self, key: str) -> bytes:
        path = self._path(key)
        return await asyncio.to_thread(path.read_bytes)

    async def get_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        # For local dev, just return a file:// URL
        return f"file://{self._path(key).resolve()}"

    async def delete(self, key: str) -> None:
        path = self._path(key)
        await asyncio.to_thread(lambda: path.unlink(missing_ok=True))

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(self._path(key).exists)
