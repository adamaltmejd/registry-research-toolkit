"""Shared support for the `reg-meta` CLI test modules (`test_cli_*.py`)."""

from __future__ import annotations

import io
import json
import sys
from typing import TYPE_CHECKING

from reader_artifacts import build_reader_artifact
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path


def build_cli_source(directory: Path, name: str) -> str:
    """Build `conformance/cases/reader/<name>` as a catalog; return its `--db` dir."""
    return str(build_reader_artifact(directory, f"reader/{name}", "catalog").parent)


def run_json(argv: list[str], *, verbose: bool = True) -> tuple[dict, int]:
    """Run a CLI command and parse the JSON output.

    Forces --format json. Default verbose=True so tests get the full envelope.
    """
    if "--format" not in argv:
        argv = ["--format", "json", *argv]
    if verbose and "--verbose" not in argv and "-v" not in argv:
        argv = ["--verbose", *argv]

    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        exit_code = run(argv)
    finally:
        sys.stdout = old_stdout
    output = buf.getvalue()
    if output.strip():
        return json.loads(output), exit_code
    return {}, exit_code
