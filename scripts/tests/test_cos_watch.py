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

import json
import os
import time
from typing import TYPE_CHECKING

from conftest import load_scripts_module

if TYPE_CHECKING:
    from pathlib import Path

cw = load_scripts_module("cos_watch")


def _write_slot(slots_root: Path, slug: str, slot: dict | None = None) -> Path:
    slots_root.mkdir(parents=True, exist_ok=True)
    path = slots_root / f"{slug}.json"
    payload = {"slot": slug, "issues": [994], "prs": [1010], "surface": "claude"}
    payload.update(slot or {})
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# scan_slots and the ledger constants now live in cos_preflight (so the durable probe
# snapshots the SAME ledger the fast tier watches); cos_watch re-exports them. Their read
# protocol is pinned in test_cos_preflight.py. Assert the re-export identity here so the
# fast tier can never diverge from the probe's scan.


def test_scan_slots_is_the_hoisted_preflight_function() -> None:
    assert cw.scan_slots is cw._cos_preflight.scan_slots
    assert cw.DEFAULT_MAX_SLOTS == cw._cos_preflight.DEFAULT_MAX_SLOTS
    assert cw.DEFAULT_SLOT_STALE_HOURS == cw._cos_preflight.DEFAULT_SLOT_STALE_HOURS


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


def test_probe_cmd_is_observe_only_and_forwards_ledger_config(tmp_path: Path) -> None:
    # --observe: never touch the tick's candidate/baseline. --gate-dir / --slots-dir /
    # --max-slots / --slot-stale-hours: the durable probe must read the SAME stores and
    # thresholds the fast tier watches, so the two tiers can never disagree.
    cmd = cw.probe_cmd(tmp_path / "gates", tmp_path / "slots", 5, 12.0)

    assert "--observe" in cmd
    assert cmd[cmd.index("--gate-dir") + 1] == str(tmp_path / "gates")
    assert cmd[cmd.index("--slots-dir") + 1] == str(tmp_path / "slots")
    assert cmd[cmd.index("--max-slots") + 1] == "5"
    assert cmd[cmd.index("--slot-stale-hours") + 1] == "12.0"


def test_probe_timeout_maps_to_error_line() -> None:
    # run_probe reports a killed (hung) probe as exit 124 with a synthetic stderr; the
    # mapping must surface it, not swallow it.
    lines = cw.probe_events(124, "", "probe timed out after 120s")

    assert lines == ["preflight error (exit 124): probe timed out after 120s"]


def test_probe_wake_with_garbage_stdout_is_an_error_not_a_crash() -> None:
    lines = cw.probe_events(cw._cos_preflight.WAKE_EXIT, "not json", "")

    assert lines == [
        f"preflight error (exit {cw._cos_preflight.WAKE_EXIT}): unexpected probe output"
    ]


def test_main_derives_slots_dir_from_gate_dir(tmp_path: Path, capsys) -> None:
    # With only --gate-dir overridden, the slot ledger defaults to its sibling —
    # both stores move together. Observable via a stale slot in the derived dir.
    gate_dir = tmp_path / "merge-gates"
    gate_dir.mkdir(parents=True)
    slot_path = _write_slot(tmp_path / "pipeline-slots", "lane-a")
    aged = time.time() - 100_000  # > 24h
    os.utime(slot_path, (aged, aged))

    rc = cw.main(["--gate-dir", str(gate_dir), "--once", "--skip-probe"])

    assert rc == 0
    assert "stale slot: lane-a" in capsys.readouterr().out


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
