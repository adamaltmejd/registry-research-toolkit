"""Unit tests for scripts/schema_pending_bump.py — the build-image pending-bump guard.

Pins the pure version-comparison verdict (the branch CI can't exercise on a normal
commit, since main's schema usually equals the latest release) plus one subprocess
smoke test of the stdout contract the workflow captures.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_MODULE = _SCRIPTS / "schema_pending_bump.py"
_SPEC = importlib.util.spec_from_file_location("schema_pending_bump", _MODULE)
assert _SPEC and _SPEC.loader
spb = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = spb
_SPEC.loader.exec_module(spb)


def _verdict(code_db: str, code_doc: str, asset_db: str, asset_doc: str) -> bool:
    pending, _ = spb.is_pending_bump(code_db, code_doc, asset_db, asset_doc)
    return pending


def test_pending_on_doc_minor_behind() -> None:
    # The #437 case: code doc 1.1.0, released asset still on 1.0.0.
    assert _verdict("5.4.0", "1.1.0", "5.4.0", "1.0.0") is True


def test_pending_on_db_minor_behind() -> None:
    assert _verdict("5.4.0", "1.1.0", "5.3.0", "1.1.0") is True


def test_compatible_when_asset_equals_code() -> None:
    assert _verdict("5.4.0", "1.1.0", "5.4.0", "1.1.0") is False


def test_compatible_when_asset_ahead_same_major() -> None:
    # Asset minor > code minor: forward-compatible, never pending.
    assert _verdict("5.4.0", "1.1.0", "5.5.0", "1.2.0") is False


def test_break_on_major_mismatch_is_not_pending() -> None:
    # Major mismatch is a genuine incompatibility, must fail red (NOT pending).
    assert _verdict("6.0.0", "1.1.0", "5.4.0", "1.1.0") is False


def test_pending_on_one_axis_while_other_compatible() -> None:
    # DB compatible (equal), doc minor behind → pending overall.
    assert _verdict("5.4.0", "1.1.0", "5.4.0", "1.0.0") is True


def test_break_suppresses_pending_on_other_axis() -> None:
    # Doc would be pending (1.1 > 1.0) but the DB major break dominates → not pending.
    assert _verdict("6.4.0", "1.1.0", "5.4.0", "1.0.0") is False


def test_classify_axis_buckets() -> None:
    assert spb.classify_axis("5.4.0", "5.4.0") == "compatible"
    assert spb.classify_axis("5.4.0", "5.5.0") == "compatible"
    assert spb.classify_axis("5.4.0", "5.3.0") == "pending"
    assert spb.classify_axis("5.4.0", "6.4.0") == "break"


def test_malformed_version_raises() -> None:
    with pytest.raises(ValueError):
        spb.classify_axis("5", "5.4.0")


def test_malformed_version_nonzero_exit() -> None:
    rc = spb.main(
        [
            "--code-db",
            "5",
            "--code-doc",
            "1.1.0",
            "--asset-db",
            "5.4.0",
            "--asset-doc",
            "1.1.0",
        ]
    )
    assert rc != 0


def test_subprocess_stdout_contract() -> None:
    # Smoke the CLI: stdout is exactly `true`/`false` (the workflow captures it).
    proc = subprocess.run(
        [
            sys.executable,
            str(_MODULE),
            "--code-db",
            "5.4.0",
            "--code-doc",
            "1.1.0",
            "--asset-db",
            "5.4.0",
            "--asset-doc",
            "1.0.0",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.strip() == "true"
    assert proc.stderr.strip()  # human explanation on stderr
