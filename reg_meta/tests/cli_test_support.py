"""Shared support for the `reg-meta` CLI test modules (`test_cli_*.py`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from reader_artifacts import build_reader_artifact

if TYPE_CHECKING:
    from pathlib import Path


def build_cli_source(directory: Path, name: str) -> str:
    """Build `conformance/cases/reader/<name>` as a catalog; return its `--db` dir."""
    return str(build_reader_artifact(directory, f"reader/{name}", "catalog").parent)
