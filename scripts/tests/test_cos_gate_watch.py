"""Unit tests for scripts/cos_gate_watch.py.

Pins the fast-tier merge-gate wake contract: a gate that becomes ready-to-merge emits
exactly one line, keyed on (pr, head, updated) — so steady state never re-emits, while
a re-verification (`updated` bump) or a new head does. `status: blocked` never emits, a
gate whose `pr` field disagrees with its directory name is absent per the gate
protocol, corrupt/torn files are skipped without dying, and the merged/ archive is
never scanned. The scan/diff functions are pure; cos_watch.py's loop composes them.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from conftest import load_scripts_module

if TYPE_CHECKING:
    from pathlib import Path

cgw = load_scripts_module("cos_gate_watch")

HEAD = "abcdef1234567890abcdef1234567890abcdef12"


def _write_gate(gate_root: Path, pr: int, gate: dict) -> Path:
    d = gate_root / f"pr-{pr}"
    d.mkdir(parents=True, exist_ok=True)
    path = d / "gate.json"
    path.write_text(json.dumps(gate), encoding="utf-8")
    return path


def _gate(
    pr=956,
    *,
    status="ready-to-merge",
    head=HEAD,
    updated="2026-07-01T00:00:00+00:00",
    **extra,
) -> dict:
    gate = {
        "pr": pr,
        "head": head,
        "status": status,
        "updated": updated,
        "gates": {"ci": "pass", "tests": "pass"},
        "blocker": None,
    }
    gate.update(extra)
    return gate


def _poll(gate_root: Path, seen: dict) -> tuple[list[str], dict]:
    # One watch-loop iteration: scan, diff against the previous scan, carry state.
    current = cgw.scan_ready(gate_root)
    return cgw.ready_events(seen, current), current


def test_ready_gate_emits_once_then_stays_silent(tmp_path: Path) -> None:
    _write_gate(tmp_path, 956, _gate(956))

    lines, seen = _poll(tmp_path, {})
    assert lines == [
        f"ready gate: pr=956 head={HEAD[:12]} updated=2026-07-01T00:00:00+00:00"
    ]

    lines, _ = _poll(tmp_path, seen)
    assert lines == []


def test_updated_bump_reemits(tmp_path: Path) -> None:
    _write_gate(tmp_path, 956, _gate(956))
    _, seen = _poll(tmp_path, {})

    _write_gate(tmp_path, 956, _gate(956, updated="2026-07-01T01:00:00+00:00"))

    lines, _ = _poll(tmp_path, seen)
    assert lines == [
        f"ready gate: pr=956 head={HEAD[:12]} updated=2026-07-01T01:00:00+00:00"
    ]


def test_head_change_reemits(tmp_path: Path) -> None:
    _write_gate(tmp_path, 956, _gate(956))
    _, seen = _poll(tmp_path, {})

    new_head = "1234567890abcdef1234567890abcdef12345678"
    _write_gate(tmp_path, 956, _gate(956, head=new_head))

    lines, _ = _poll(tmp_path, seen)
    assert lines == [
        f"ready gate: pr=956 head={new_head[:12]} updated=2026-07-01T00:00:00+00:00"
    ]


def test_blocked_never_emits(tmp_path: Path) -> None:
    _write_gate(tmp_path, 956, _gate(956, status="blocked", blocker="missing visual"))

    lines, seen = _poll(tmp_path, {})
    assert lines == []
    assert seen == {}


def test_ready_to_blocked_to_ready_reemits(tmp_path: Path) -> None:
    # A blocked gate drops out of the seen state, so flipping back to ready re-emits
    # even if (head, updated) were somehow unchanged by the round-trip.
    _write_gate(tmp_path, 956, _gate(956))
    _, seen = _poll(tmp_path, {})

    _write_gate(tmp_path, 956, _gate(956, status="blocked"))
    lines, seen = _poll(tmp_path, seen)
    assert lines == []

    _write_gate(tmp_path, 956, _gate(956))
    lines, _ = _poll(tmp_path, seen)
    assert len(lines) == 1


def test_pr_field_dir_name_mismatch_ignored(tmp_path: Path) -> None:
    # Per the gate protocol, an entry whose `pr` disagrees with its directory name is
    # treated as absent.
    _write_gate(tmp_path, 956, _gate(999))

    assert cgw.scan_ready(tmp_path) == {}


def test_non_numeric_dir_ignored(tmp_path: Path) -> None:
    d = tmp_path / "pr-draft"
    d.mkdir(parents=True)
    (d / "gate.json").write_text(json.dumps(_gate(956)), encoding="utf-8")

    assert cgw.scan_ready(tmp_path) == {}


def test_corrupt_gate_skipped_without_dying(tmp_path: Path) -> None:
    path = _write_gate(tmp_path, 956, _gate(956))
    path.write_text("{ torn wri", encoding="utf-8")
    _write_gate(tmp_path, 957, _gate(957))

    assert set(cgw.scan_ready(tmp_path)) == {957}


def test_merged_archive_not_scanned(tmp_path: Path) -> None:
    archive = tmp_path / "merged" / "pr-900"
    archive.mkdir(parents=True)
    (archive / "gate.json").write_text(json.dumps(_gate(900)), encoding="utf-8")

    assert cgw.scan_ready(tmp_path) == {}


def test_missing_gate_root_is_empty(tmp_path: Path) -> None:
    assert cgw.scan_ready(tmp_path / "does-not-exist") == {}


def test_archived_gate_disappears_then_reappears_reemits(tmp_path: Path) -> None:
    # Merge flow: chief-of-staff archives the gate dir; the key drops from the seen
    # state, so a later fresh handoff for the same PR number emits again.
    path = _write_gate(tmp_path, 956, _gate(956))
    _, seen = _poll(tmp_path, {})

    path.unlink()
    path.parent.rmdir()
    lines, seen = _poll(tmp_path, seen)
    assert lines == []

    _write_gate(tmp_path, 956, _gate(956))
    lines, _ = _poll(tmp_path, seen)
    assert len(lines) == 1
