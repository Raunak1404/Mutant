from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

_WRAPPER_HEADER = """
import sys
import io
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Load input
input_df = pq.read_table("__INPUT_PATH__").to_pandas()

# --- USER CODE START ---
"""

_WRAPPER_FOOTER = """
# --- USER CODE END ---

# Save output (user code must produce `output_df`)
if 'output_df' not in dir():
    # If user code didn't set output_df, assume it modified input_df in place
    output_df = input_df

table = pa.Table.from_pandas(output_df)
pq.write_table(table, "__OUTPUT_PATH__")
"""

NATIVE_WRAPPER_TEMPLATE = """
import sys
import json
from pathlib import Path

# Add the step code directory to path so imports work
sys.path.insert(0, "{code_dir}")

# Write the step code to a file that can be imported
step_code_path = Path("{code_dir}") / "step_module.py"
step_code_path.write_text(open("{code_file_path}").read(), encoding="utf-8")

# Import and run the step module
import importlib.util
spec = importlib.util.spec_from_file_location("step_module", str(step_code_path))
step_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(step_module)

result = step_module.main(
    input_path="{input_path}",
    output_path="{output_path}",
    libraries_dir="{libraries_dir}",
)

# Write result as JSON to a known location
result_path = Path("{result_path}")
result_path.write_text(json.dumps(result, default=str), encoding="utf-8")
"""

# Minimal environment — no secrets, no home dir
RESTRICTED_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": "",
    "HOME": "/tmp",
}


class ExecutionResult(NamedTuple):
    success: bool
    output_data: bytes | None
    stderr: str
    returncode: int


async def execute_code_in_subprocess(
    code: str,
    input_data: bytes,
    timeout_seconds: int = 60,
) -> ExecutionResult:
    """Run pandas transformation code in an isolated subprocess."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_path = tmpdir_path / "input.parquet"
        output_path = tmpdir_path / "output.parquet"
        code_path = tmpdir_path / "transform.py"

        input_path.write_bytes(input_data)
        # Build wrapper by concatenation instead of str.format() to avoid
        # collisions when LLM-generated code contains curly braces.
        wrapper = (
            _WRAPPER_HEADER.replace("__INPUT_PATH__", str(input_path))
            + code + "\n"
            + _WRAPPER_FOOTER.replace("__OUTPUT_PATH__", str(output_path))
        )
        code_path.write_text(wrapper, encoding="utf-8")

        def _run() -> subprocess.CompletedProcess:
            return subprocess.run(
                [sys.executable, str(code_path)],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env={**RESTRICTED_ENV},
                cwd=tmpdir,
            )

        try:
            result = await asyncio.to_thread(_run)
        except subprocess.TimeoutExpired:
            return ExecutionResult(
                success=False,
                output_data=None,
                stderr=f"Execution timed out after {timeout_seconds}s",
                returncode=-1,
            )

        if result.returncode == 0 and output_path.exists():
            output_data = output_path.read_bytes()
            return ExecutionResult(
                success=True,
                output_data=output_data,
                stderr=result.stderr,
                returncode=0,
            )

        return ExecutionResult(
            success=False,
            output_data=None,
            stderr=result.stderr[:5000],
            returncode=result.returncode,
        )


class NativeExecutionResult(NamedTuple):
    success: bool
    output_data: bytes | None
    result_json: str  # JSON string with changelog, stats
    stderr: str
    returncode: int


async def execute_native_step(
    code_content: str,
    input_data: bytes,
    libraries: dict[str, bytes],
    timeout_seconds: int = 120,
) -> NativeExecutionResult:
    """Run a native step logic file in an isolated subprocess.

    Args:
        code_content: Full Python source of the step logic file
        input_data: Raw Excel file bytes (not parquet)
        libraries: Dict of {filename: file_bytes} for reference Excel libraries
        timeout_seconds: Max execution time
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_path = tmpdir_path / "input.xlsx"
        output_path = tmpdir_path / "output.xlsx"
        result_path = tmpdir_path / "result.json"
        code_dir = tmpdir_path / "code"
        code_dir.mkdir()
        code_file_path = code_dir / "step_logic.py"
        libraries_dir = tmpdir_path / "libraries"
        libraries_dir.mkdir()

        # Write input Excel
        input_path.write_bytes(input_data)

        # Write step logic code
        code_file_path.write_text(code_content, encoding="utf-8")

        # Write library files
        for lib_name, lib_data in libraries.items():
            lib_path = libraries_dir / lib_name
            lib_path.write_bytes(lib_data)

        # Write the wrapper script
        wrapper_path = tmpdir_path / "run_step.py"
        wrapper_path.write_text(
            NATIVE_WRAPPER_TEMPLATE.format(
                code_dir=str(code_dir),
                code_file_path=str(code_file_path),
                input_path=str(input_path),
                output_path=str(output_path),
                libraries_dir=str(libraries_dir),
                result_path=str(result_path),
            ),
            encoding="utf-8",
        )

        def _run() -> subprocess.CompletedProcess:
            # Native steps need full environment for openpyxl, pandas, etc.
            env = os.environ.copy()
            env["PYTHONPATH"] = ""
            return subprocess.run(
                [sys.executable, str(wrapper_path)],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env=env,
                cwd=tmpdir,
            )

        try:
            proc_result = await asyncio.to_thread(_run)
        except subprocess.TimeoutExpired:
            return NativeExecutionResult(
                success=False,
                output_data=None,
                result_json="{}",
                stderr=f"Execution timed out after {timeout_seconds}s",
                returncode=-1,
            )

        # Read the result JSON
        result_json = "{}"
        if result_path.exists():
            result_json = result_path.read_text(encoding="utf-8")

        if proc_result.returncode == 0 and output_path.exists():
            output_data = output_path.read_bytes()
            return NativeExecutionResult(
                success=True,
                output_data=output_data,
                result_json=result_json,
                stderr=proc_result.stderr,
                returncode=0,
            )

        return NativeExecutionResult(
            success=False,
            output_data=None,
            result_json=result_json,
            stderr=proc_result.stderr[:5000],
            returncode=proc_result.returncode,
        )
