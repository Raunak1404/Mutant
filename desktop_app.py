from __future__ import annotations

import os
import runpy
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path
from urllib.request import urlopen

import uvicorn

from runtime.paths import default_app_data_dir

ENV_FILENAMES = (".env", "weisiong.env", "WeiSiong.env")


def _maybe_run_embedded_script() -> bool:
    """Allow the frozen desktop executable to act like `python script.py`.

    Native step sandboxing launches subprocesses via `sys.executable`. In a
    PyInstaller app bundle that points to the app executable, not a standalone
    Python binary. Without this shim, every native step relaunches the desktop
    UI instead of executing the generated wrapper script.
    """
    if len(sys.argv) < 2:
        return False

    candidate = Path(sys.argv[1]).expanduser()
    if candidate.suffix != ".py" or not candidate.exists():
        return False

    sys.argv = [str(candidate), *sys.argv[2:]]
    runpy.run_path(str(candidate), run_name="__main__")
    return True


def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server(url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1.0) as response:
                if response.status < 500:
                    return
        except Exception:
            time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for local server at {url}")


def _unique_download_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    counter = 1
    while True:
        candidate = path.with_name(f"{stem} ({counter}){suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _iter_env_source_candidates() -> list[Path]:
    executable = Path(sys.executable).resolve()
    roots = [Path.cwd(), executable.parent.parent / "Resources", *executable.parents[:5]]

    candidates: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        resolved_root = root.expanduser().resolve()
        if resolved_root in seen:
            continue
        seen.add(resolved_root)
        for filename in ENV_FILENAMES:
            candidate = (resolved_root / filename).resolve()
            if candidate.exists():
                candidates.append(candidate)
    return candidates


def _bootstrap_user_env() -> None:
    if not getattr(sys, "frozen", False):
        return

    app_data_dir = default_app_data_dir()
    app_data_dir.mkdir(parents=True, exist_ok=True)
    target_env = app_data_dir / ".env"
    if target_env.exists():
        return

    for candidate in _iter_env_source_candidates():
        if candidate == target_env:
            continue
        try:
            target_env.write_bytes(candidate.read_bytes())
            return
        except OSError:
            continue


class DesktopBridge:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    def save_job_result(self, job_id: str) -> dict[str, str | bool]:
        if not job_id:
            return {"ok": False, "error": "Missing job id"}

        filename = f"Mutant_Output_{job_id[:8]}.zip"
        download_url = f"{self._base_url}/jobs/{job_id}/download"

        try:
            with urlopen(download_url, timeout=120) as response:
                data = response.read()
        except Exception as exc:
            return {"ok": False, "error": f"Failed to fetch result: {exc}"}

        downloads_dir = Path.home() / "Downloads"
        try:
            downloads_dir.mkdir(parents=True, exist_ok=True)
            target_path = _unique_download_path(downloads_dir / filename)
            target_path.write_bytes(data)
        except Exception as exc:
            return {"ok": False, "error": f"Failed to save ZIP: {exc}"}

        return {"ok": True, "path": str(target_path), "filename": target_path.name}


class EmbeddedServer(threading.Thread):
    def __init__(self, port: int) -> None:
        super().__init__(name="weisiong-server", daemon=True)
        self._port = port
        self.server: uvicorn.Server | None = None

    def run(self) -> None:
        from main import create_app

        config = uvicorn.Config(
            create_app(),
            host="127.0.0.1",
            port=self._port,
            log_level="info",
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self.server.run()

    def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True


def _configure_environment(port: int) -> None:
    os.environ.setdefault("JOB_RUNNER", "local")
    os.environ.setdefault("USE_REDIS", "false")
    os.environ.setdefault("EXECUTION_SERVICE_URL", "embedded://local")
    os.environ.setdefault("API_HOST", "127.0.0.1")
    os.environ.setdefault("API_PORT", str(port))
    os.environ.setdefault("APP_DATA_DIR", str(default_app_data_dir()))
    os.environ.setdefault("DEBUG", "false")


def main() -> None:
    port = _pick_port()
    _bootstrap_user_env()
    _configure_environment(port)
    server = EmbeddedServer(port)
    server.start()

    app_url = f"http://127.0.0.1:{port}/"
    _wait_for_server(app_url)

    try:
        import webview

        bridge = DesktopBridge(app_url)
        window = webview.create_window("Mutant", app_url, width=1440, height=960, js_api=bridge)
        window.events.closed += lambda: server.stop()
        webview.start()
    except ImportError:
        webbrowser.open(app_url)
        try:
            while server.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            server.stop()


if __name__ == "__main__":
    if not _maybe_run_embedded_script():
        main()
