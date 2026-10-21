"""Unit tests for scripts/schema_pending_bump.py — the build-image pending-bump guard.

Pins the pure version-comparison verdict (the branch CI can't exercise on a normal
commit, since main's schema usually equals the latest release) plus one subprocess
smoke test of the stdout contract the workflow captures.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from _scripts_support import load_scripts_module

_MODULE = Path(__file__).resolve().parents[1] / "schema_pending_bump.py"
spb = load_scripts_module("schema_pending_bump")


def _verdict(code_db: str, code_doc: str, asset_db: str, asset_doc: str) -> str:
    verdict, _ = spb.classify_overall(code_db, code_doc, asset_db, asset_doc)
    return verdict


def test_pending_on_db_minor_behind() -> None:
    assert _verdict("5.4.0", "1.1.0", "5.3.0", "1.1.0") == "pending"


def test_compatible_when_asset_equals_code() -> None:
    assert _verdict("5.4.0", "1.1.0", "5.4.0", "1.1.0") == "compatible"


def test_compatible_when_asset_ahead_same_major() -> None:
    # Asset minor > code minor: forward-compatible, never pending.
    assert _verdict("5.4.0", "1.1.0", "5.5.0", "1.2.0") == "compatible"


def test_break_on_major_mismatch() -> None:
    # Major mismatch is a genuine incompatibility — the guard must fail red.
    assert _verdict("6.0.0", "1.1.0", "5.4.0", "1.1.0") == "break"


def test_break_suppresses_pending_on_other_axis() -> None:
    # Doc would be pending (1.1 > 1.0) but the DB major break dominates → break.
    assert _verdict("6.4.0", "1.1.0", "5.4.0", "1.0.0") == "break"


def test_malformed_version_nonzero_exit() -> None:
    rc = spb.main(
        [
            "--code-db",
            "5",
            "--code-doc",
            "1.1",
            "--asset-db",
            "5.4.0",
            "--asset-doc",
            "1.1.0",
        ]
    )
    assert rc != 0


def test_subprocess_stdout_contract() -> None:
    # Smoke the CLI: stdout is exactly the three-way verdict token (the workflow
    # case-matches it). The doc-minor-behind args yield `pending`.
    proc = subprocess.run(
        [
            sys.executable,
            str(_MODULE),
            "--code-db",
            "5.4",
            "--code-doc",
            "1.1",
            "--asset-db",
            "5.4.0",
            "--asset-doc",
            "1.0.0",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.strip() == "pending"
    assert proc.stderr.strip()  # human explanation on stderr
