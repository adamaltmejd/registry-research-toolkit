"""Build a single-file .py bundle for upload to MONA.

Concatenates the wizard runtime modules (classify, sql_emit, sources,
summarize, extract) into one file at
``mock_data_wizard/dist/mock_data_wizard_extract.py``. The user uploads
that file, edits the ``SOURCES = [...]`` block near the bottom, and
runs:

    python mock_data_wizard_extract.py

The bundle is self-contained -- only stdlib + duckdb + pyodbc + numpy
(all pre-installed on the WinPython distribution shipped with MONA's
batch client; see ``DESIGN.md`` for the runtime probe results).

Per-module docstrings and ``#`` comments are dropped during
amalgamation. The repo source remains the documentation; the bundle is
the artifact.

Usage:
    uv run python mock_data_wizard/scripts/build_mona_bundle.py
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PKG_DIR = REPO_ROOT / "mock_data_wizard" / "src" / "mock_data_wizard"
DIST_DIR = REPO_ROOT / "mock_data_wizard" / "dist"
OUTPUT_DEFAULT = DIST_DIR / "mock_data_wizard_extract.py"

# Dependency-ordered: each module imports only earlier ones.
MODULE_ORDER = ("classify", "sql_emit", "sources", "summarize", "extract")

BUNDLE_HEADER = '''\
"""mock-data-wizard MONA extract bundle.

Self-contained single-file Python script. Edit the SOURCES = [...] block
near the bottom, then run on MONA:

    python mock_data_wizard_extract.py

Output: stats.json next to this script.

Discovery mode: leaving any source without filters triggers a scan that
writes a mdw_sources_<TS>.py sidecar listing everything discoverable
and exits without writing stats.json. Edit that sidecar to narrow the
list, then re-run -- the sidecar overrides the in-script SOURCES.

PII discipline: only aggregate values cross the JSON boundary. Cell
suppression (k-anonymity, threshold = 5) and uniform noise injection
(+/- 0.5%) are applied after server-side aggregation. No row-level
data passes through Python.

This file is built from the mock_data_wizard package by
scripts/build_mona_bundle.py. DO NOT edit code mid-bundle by hand --
edit the source modules and re-bundle.
"""
from __future__ import annotations
'''

BUNDLE_RUNNER = """\
# ===========================================================================
# Runner -- edit the SOURCES list below before running on MONA.
# ===========================================================================

import logging
import os
import socket
import sys

# MBS-batch stdout footgun: in batch mode stdout is buffered to memory and
# can hang the script when the buffer fills. Per MONA Python docs, redirect
# on MBS-prefixed hosts.
if socket.gethostname().upper().startswith("MBS"):
    sys.stdout = open(os.devnull, "w")  # noqa: SIM115

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stderr,
)

# ---------------------------------------------------------------------------
# USER CONFIGURATION -- edit me before running on MONA.
# ---------------------------------------------------------------------------
SOURCES = [
    # Examples:
    #   file_source(path=r"\\\\micro.intra\\projekt\\P1105$\\P1105_Data"),
    #   sql_source(dsn="P1105"),
    #
    # A source with no include/exclude/pattern/all triggers discovery mode.
]


if __name__ == "__main__":
    main(SOURCES, output_dir=Path(__file__).resolve().parent)
"""


def _slice_module(name: str) -> str:
    """Read a module, drop docstring + __future__ + intra-pkg imports.

    The remaining body (functions, classes, constants, dataclasses,
    stdlib imports) is rendered via ``ast.unparse`` -- ``#`` comments
    are not preserved (they live in the source modules in the repo).
    """
    src = (PKG_DIR / f"{name}.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    body = list(tree.body)
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body = body[1:]

    kept: list[ast.stmt] = []
    for node in body:
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("__future__"):
                continue
            if node.level > 0:
                continue
            if node.module and node.module.startswith("mock_data_wizard"):
                continue
        kept.append(node)

    return ast.unparse(ast.Module(body=kept, type_ignores=[]))


def build_bundle(output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    parts: list[str] = [BUNDLE_HEADER, ""]
    for name in MODULE_ORDER:
        parts.append(f"# {'=' * 75}")
        parts.append(f"# {name}.py")
        parts.append(f"# {'=' * 75}")
        parts.append("")
        parts.append(_slice_module(name))
        parts.append("")
    parts.append(BUNDLE_RUNNER)
    output.write_text("\n".join(parts), encoding="utf-8")
    return output


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=OUTPUT_DEFAULT,
        help=f"Output path (default: {OUTPUT_DEFAULT})",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    out = build_bundle(args.output)
    size = out.stat().st_size
    print(f"Built {out} ({size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
