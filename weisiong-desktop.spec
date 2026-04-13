# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for Mutant Agentic Excel Processor.

Build with:
    pyinstaller weisiong-desktop.spec

Output goes to: dist/Mutant/
"""
import os
import sys
from pathlib import Path

block_cipher = None

ROOT = os.path.abspath(os.path.dirname(SPECPATH))

# ── Collect all Python source packages ────────────────────────────────────
source_packages = [
    "api",
    "cache",
    "chat",
    "config",
    "core",
    "db",
    "excel",
    "execution_service",
    "feedback",
    "llm",
    "models",
    "runtime",
    "storage",
    "tasks",
    "utils",
]

hidden_imports = [
    "uvicorn.logging",
    "uvicorn.loops",
    "uvicorn.loops.auto",
    "uvicorn.protocols",
    "uvicorn.protocols.http",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.websockets",
    "uvicorn.protocols.websockets.auto",
    "uvicorn.lifespan",
    "uvicorn.lifespan.on",
    "aiosqlite",
    "sqlalchemy.dialects.sqlite",
    "pydantic",
    "pydantic_settings",
    "structlog",
    "diskcache",
    "httpx",
    "anthropic",
    "openai",
    "pandas",
    "openpyxl",
    "pyarrow",
]

# ── Data files to bundle ─────────────────────────────────────────────────
datas = [
    # Pre-built React frontend
    (os.path.join(ROOT, "static"), "static"),
    # Step logic + rule definitions
    (os.path.join(ROOT, "steps"), "steps"),
]

a = Analysis(
    [os.path.join(ROOT, "desktop_app.py")],
    pathex=[ROOT],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "tkinter",
        "matplotlib",
        "scipy",
        "IPython",
        "jupyter",
        "notebook",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Mutant",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # No console window on Windows
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="Mutant",
)
