"""Unit tests for scripts/cos_watch.py.

Pins the orchestrator's event contracts: the slot-ledger scan follows the gate-store
read protocol (filename-stem agreement, tolerant read, done/ archive ignored), freed
slots emit and gate the single dispatch line on remaining budget, staleness emits once
per onset keyed on mtime, and the slow-tier probe mapping turns exit 10 into a wake
line, exit 0 into silence, and everything else (including unparseable probe output)
into a visible preflight-error line. The gate fast tier is cos_gate_watch's, pinned by
its own suite.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("cos_watch", _SCRIPTS / "cos_watch.py")
assert _SPEC and _SPEC.loader
cw = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = cw
_SPEC.loader.exec_module(cw)


def _write_slot(slots_root: Path, slug: str, slot: dict | None = None) -> Path:
    slots_root.mkdir(parents=True, exist_ok=True)
    path = slots_root / f"{slug}.json"
    payload = {"slot": slug, "issues": [994], "prs": [1010], "surface": "claude"}
    payload.update(slot or {})
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# --- scan_slots: ledger read protocol ---


def test_scan_slots_reads_valid_slots(tmp_path: Path) -> None:
    _write_slot(tmp_path, "lane-a")
    _write_slot(tmp_path, "lane-b")

    assert cw.scan_slots(tmp_path) == {"lane-a", "lane-b"}


def test_scan_slots_slot_field_stem_mismatch_ignored(tmp_path: Path) -> None:
    _write_slot(tmp_path, "lane-a", {"slot": "other-name"})

    assert cw.scan_slots(tmp_path) == set()


def test_scan_slots_corrupt_file_skipped(tmp_path: Path) -> None:
    path = _write_slot(tmp_path, "lane-a")
    path.write_text("{ torn", encoding="utf-8")
    _write_slot(tmp_path, "lane-b")

    assert cw.scan_slots(tmp_path) == {"lane-b"}


def test_scan_slots_done_archive_not_scanned(tmp_path: Path) -> None:
    _write_slot(tmp_path / "done", "lane-a")

    assert cw.scan_slots(tmp_path) == set()


def test_scan_slots_missing_root_is_empty(tmp_path: Path) -> None:
    assert cw.scan_slots(tmp_path / "does-not-exist") == set()


# --- slot_events: freed transitions gate the dispatch line ---


def test_slot_freed_emits_and_dispatches_when_below_max() -> None:
    lines = cw.slot_events({"a", "b", "c"}, {"a", "b"}, max_slots=3)

    assert lines == [
        "slot freed: c; busy 2/3",
        "dispatch: 1 slot(s) free; recommend next pr-pipeline lanes",
    ]


def test_slot_freed_over_budget_does_not_dispatch() -> None:
    # 4 registered (deliberate human override), one freed → still at max: no dispatch.
    lines = cw.slot_events({"a", "b", "c", "d"}, {"a", "b", "c"}, max_slots=3)

    assert lines == ["slot freed: d; busy 3/3"]


def test_slot_claim_is_silent() -> None:
    assert cw.slot_events({"a"}, {"a", "b"}, max_slots=3) == []


def test_slot_steady_state_is_silent() -> None:
    assert cw.slot_events({"a", "b"}, {"a", "b"}, max_slots=3) == []


def test_multiple_freed_slots_one_dispatch_line() -> None:
    lines = cw.slot_events({"a", "b", "c"}, {"a"}, max_slots=3)

    assert lines == [
        "slot freed: b; busy 1/3",
        "slot freed: c; busy 1/3",
        "dispatch: 2 slot(s) free; recommend next pr-pipeline lanes",
    ]


# --- stale_slot_events: emit once per staleness onset, keyed on mtime ---


def test_stale_slot_emits_once_then_reemits_after_touch(tmp_path: Path) -> None:
    path = _write_slot(tmp_path, "lane-a")
    mtime = path.stat().st_mtime
    emitted: dict[str, float] = {}
    stale_after = 3600.0

    lines = cw.stale_slot_events(
        tmp_path, {"lane-a"}, emitted, stale_after, now=mtime + 7200
    )
    assert lines == ["stale slot: lane-a; no update for 2h — adjudicate or release"]

    # Same onset: silent on the next poll.
    again = cw.stale_slot_events(
        tmp_path, {"lane-a"}, emitted, stale_after, now=mtime + 7300
    )
    assert again == []

    # A touch clears the onset; going stale again re-emits.
    fresh = cw.stale_slot_events(
        tmp_path, {"lane-a"}, emitted, stale_after, now=mtime + 100
    )
    assert fresh == []
    assert emitted == {}
    reemitted = cw.stale_slot_events(
        tmp_path, {"lane-a"}, emitted, stale_after, now=mtime + 7200
    )
    assert len(reemitted) == 1


def test_stale_state_pruned_for_freed_slug(tmp_path: Path) -> None:
    emitted = {"gone-lane": 123.0}

    cw.stale_slot_events(tmp_path, set(), emitted, 3600.0, now=1e9)

    assert emitted == {}


def test_fresh_slot_never_emits(tmp_path: Path) -> None:
    path = _write_slot(tmp_path, "lane-a")

    lines = cw.stale_slot_events(
        tmp_path, {"lane-a"}, {}, 3600.0, now=path.stat().st_mtime + 10
    )

    assert lines == []


# --- probe_events: slow-tier mapping ---


def test_probe_wake_emits_reasons() -> None:
    stdout = json.dumps(
        {
            "wake": True,
            "reasons": ["ready merge-gate PR changed: #1009", "lanes need re-rank"],
        }
    )

    lines = cw.probe_events(cw._cos_preflight.WAKE_EXIT, stdout, "")

    assert lines == ["wake: ready merge-gate PR changed: #1009; lanes need re-rank"]


def test_probe_idle_is_silent() -> None:
    assert cw.probe_events(0, "", "") == []


def test_probe_tool_error_emits_stderr_tail() -> None:
    lines = cw.probe_events(
        2, "", "warning: something\nfatal: not canonical checkout\n"
    )

    assert lines == ["preflight error (exit 2): fatal: not canonical checkout"]


def test_probe_tool_error_without_stderr() -> None:
    assert cw.probe_events(2, "", "") == ["preflight error (exit 2): no stderr"]


def test_probe_wake_with_garbage_stdout_is_an_error_not_a_crash() -> None:
    lines = cw.probe_events(cw._cos_preflight.WAKE_EXIT, "not json", "")

    assert lines == [
        f"preflight error (exit {cw._cos_preflight.WAKE_EXIT}): unexpected probe output"
    ]


# --- main --once: combined fast tier end to end ---


def test_main_once_skip_probe_emits_gates_and_no_slot_noise(
    tmp_path: Path, capsys
) -> None:
    gate_dir = tmp_path / "merge-gates"
    (gate_dir / "pr-7").mkdir(parents=True)
    (gate_dir / "pr-7" / "gate.json").write_text(
        json.dumps(
            {
                "pr": 7,
                "head": "abcdef1234567890",
                "status": "ready-to-merge",
                "updated": "2026-07-02T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    slots_dir = tmp_path / "pipeline-slots"
    _write_slot(slots_dir, "lane-a")

    rc = cw.main(
        [
            "--gate-dir",
            str(gate_dir),
            "--slots-dir",
            str(slots_dir),
            "--once",
            "--skip-probe",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out.strip().splitlines()
    # The startup slot baseline never emits "freed"; the ready gate emits.
    assert out == [
        "ready gate: pr=7 head=abcdef123456 updated=2026-07-02T00:00:00+00:00"
    ]
