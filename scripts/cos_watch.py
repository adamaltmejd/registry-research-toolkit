#!/usr/bin/env python3
"""Unified deterministic wake watcher for the chief-of-staff loop.

All wake cadence lives here; the agent session arms this script once as a persistent
monitor and then never wakes idle. Each stdout line is a wake event:

  ready gate: pr=<N> head=<sha12> updated=<iso>     merge-gate handoff (fast tier)
  slot freed: <slug>; busy <k>/<max>                a pipeline slot was released
  dispatch: <n> slot(s) free; recommend next pr-pipeline lanes
  stale slot: <slug>; no update for <h>h — adjudicate or release
  wake: <reason; reason; ...>                       preflight probe fired (slow tier)
  preflight error (exit <rc>): <stderr tail>        probe tool failure — never silent

Two tiers in one loop:

- FAST (default 20s, local only): the merge-gate ready scan from cos_gate_watch.py,
  plus pipeline-slot ledger transitions. The `dispatch:` line fires only when a freed
  slot leaves the ledger below --max-slots — new-lane recommendations follow a merge
  that freed budget; while all slots are busy the watcher stays silent on dispatch
  (merge and maintenance wakes still flow).
- SLOW (default 600s, remote): run the cos_preflight.py probe as a subprocess. Exit 10
  emits the probe's reasons; exit 0 (idle) emits nothing; anything else emits a
  preflight-error line. The probe's baseline/candidate/--commit contract is untouched —
  the woken tick still starts with its OWN probe and commits its own fingerprint.
  Exit-10 emissions are deliberately NOT deduped: the baseline only advances when a
  tick commits, so a re-emitted wake means the events are genuinely still unhandled
  (at-least-once), and a duplicate costs the agent one cheap idle probe.

The slot ledger lives next to the merge-gate store:
$XDG_STATE_HOME/registry-research-toolkit/pipeline-slots/<slug>.json, one file per
running pipeline agent (claude or codex). pr-pipeline registers a slot at lane claim;
chief-of-staff releases it at merge (archives to done/) once every PR in the slot is
merged or closed. A slot file whose `slot` field disagrees with its filename stem is
absent, mirroring the gate.json protocol. Stale slots are surfaced once for
adjudication, never auto-released — releasing is a judgment call the agent owns.

The slow tier must run from the canonical checkout (the probe verifies this itself and
a wrong cwd surfaces as a preflight-error emission, not a silent skip). Local
filesystem poll + one subprocess; no inotify/fswatch dependency (portable to macOS).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path

# Reuse the fast-tier gate scan from cos_gate_watch (which itself spec-loads
# cos_preflight for the store-root and tolerant-read leaf helpers).
_CGW_SPEC = importlib.util.spec_from_file_location(
    "cos_gate_watch", Path(__file__).with_name("cos_gate_watch.py")
)
assert _CGW_SPEC and _CGW_SPEC.loader
_cos_gate_watch = importlib.util.module_from_spec(_CGW_SPEC)
sys.modules[_CGW_SPEC.name] = _cos_gate_watch
_CGW_SPEC.loader.exec_module(_cos_gate_watch)
_cos_preflight = _cos_gate_watch._cos_preflight

DEFAULT_MAX_SLOTS = 3
DEFAULT_FAST_INTERVAL = 20.0
DEFAULT_SLOW_INTERVAL = 600.0
DEFAULT_SLOT_STALE_HOURS = 24.0


def default_slots_root() -> Path:
    return _cos_preflight.default_gate_root().parent / "pipeline-slots"


def scan_slots(slots_root: Path) -> set[str]:
    """Slugs of every valid pipeline-slot file (top level only; done/ is the archive).

    Mirrors the gate protocol's read rules: a slot whose `slot` field disagrees with
    its filename stem is absent; unreadable/corrupt files are skipped (slot files are
    written atomically, so a torn read is transient).
    """
    slots: set[str] = set()
    for path in sorted(slots_root.glob("*.json")):
        loaded = _cos_preflight._read_json_tolerant(path, "pipeline-slot file")
        if loaded is None:
            continue
        slot = loaded[1]
        if not isinstance(slot, dict) or slot.get("slot") != path.stem:
            continue
        slots.add(path.stem)
    return slots


def slot_events(previous: set[str], current: set[str], max_slots: int) -> list[str]:
    """Freed-slot lines, plus one dispatch line when freeing left budget below max.

    Claims are deliberately silent: a new slot usually originates from a lane the
    chief itself recommended, and the probe fingerprint doesn't move on a claim, so a
    wake would just burn an idle probe. Dispatch fires only on a freed transition —
    the post-merge moment the user's budget rhythm keys on — and only when the
    resulting busy count is under max (an over-budget ledger frees down to max
    without recommending more work).
    """
    freed = sorted(previous - current)
    busy = len(current)
    lines = [f"slot freed: {slug}; busy {busy}/{max_slots}" for slug in freed]
    if freed and busy < max_slots:
        lines.append(
            f"dispatch: {max_slots - busy} slot(s) free; "
            "recommend next pr-pipeline lanes"
        )
    return lines


def stale_slot_events(
    slots_root: Path,
    current: set[str],
    emitted: dict[str, float],
    stale_seconds: float,
    now: float,
) -> list[str]:
    """One line per slot whose file hasn't been touched in stale_seconds.

    Keyed on the file mtime so each staleness onset emits once; a touch (pipeline
    still alive, bumping its slot) clears the key and a later re-staleness re-emits.
    `emitted` is the caller-owned dedupe state, pruned here for vanished slugs.
    """
    for slug in list(emitted):
        if slug not in current:
            del emitted[slug]
    lines: list[str] = []
    for slug in sorted(current):
        try:
            mtime = (slots_root / f"{slug}.json").stat().st_mtime
        except OSError:
            continue
        if now - mtime < stale_seconds:
            emitted.pop(slug, None)
            continue
        if emitted.get(slug) == mtime:
            continue
        emitted[slug] = mtime
        hours = int((now - mtime) // 3600)
        lines.append(
            f"stale slot: {slug}; no update for {hours}h — adjudicate or release"
        )
    return lines


def probe_events(returncode: int, stdout: str, stderr: str) -> list[str]:
    """Map one cos_preflight probe run to wake lines. Idle (exit 0) is silence."""
    if returncode == 0:
        return []
    if returncode == _cos_preflight.WAKE_EXIT:
        try:
            reasons = json.loads(stdout)["reasons"]
            return [f"wake: {'; '.join(reasons)}"]
        except ValueError, KeyError, TypeError:
            return [f"preflight error (exit {returncode}): unparseable probe output"]
    tail = stderr.strip().splitlines()[-1] if stderr.strip() else "no stderr"
    return [f"preflight error (exit {returncode}): {tail}"]


def run_probe() -> tuple[int, str, str]:
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("cos_preflight.py"))],
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def emit(lines: list[str]) -> None:
    for line in lines:
        print(line, flush=True)


def watch(args: argparse.Namespace) -> None:
    seen_gates: dict[int, tuple[str, str]] = {}
    seen_slots = scan_slots(args.slots_dir)  # baseline: startup never emits "freed"
    stale_emitted: dict[str, float] = {}
    next_slow = time.monotonic()  # first slow pass runs immediately
    while True:
        current_gates = _cos_gate_watch.scan_ready(args.gate_dir)
        emit(_cos_gate_watch.ready_events(seen_gates, current_gates))
        seen_gates = current_gates

        current_slots = scan_slots(args.slots_dir)
        emit(slot_events(seen_slots, current_slots, args.max_slots))
        emit(
            stale_slot_events(
                args.slots_dir,
                current_slots,
                stale_emitted,
                args.slot_stale_hours * 3600,
                time.time(),
            )
        )
        seen_slots = current_slots

        if not args.skip_probe and time.monotonic() >= next_slow:
            emit(probe_events(*run_probe()))
            next_slow = time.monotonic() + args.slow_interval

        if args.once:
            return
        time.sleep(args.fast_interval)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--gate-dir",
        type=Path,
        default=_cos_preflight.default_gate_root(),
        help="local merge-gate store root; overrides the XDG-derived default",
    )
    ap.add_argument(
        "--slots-dir",
        type=Path,
        default=default_slots_root(),
        help="pipeline-slot ledger root; overrides the XDG-derived default",
    )
    ap.add_argument("--max-slots", type=int, default=DEFAULT_MAX_SLOTS)
    ap.add_argument("--fast-interval", type=float, default=DEFAULT_FAST_INTERVAL)
    ap.add_argument("--slow-interval", type=float, default=DEFAULT_SLOW_INTERVAL)
    ap.add_argument("--slot-stale-hours", type=float, default=DEFAULT_SLOT_STALE_HOURS)
    ap.add_argument(
        "--once",
        action="store_true",
        help="single pass (fast tier + one probe unless --skip-probe) and exit",
    )
    ap.add_argument(
        "--skip-probe",
        action="store_true",
        help="fast tier only — no cos_preflight subprocess (tests, quick local checks)",
    )
    args = ap.parse_args(argv)
    try:
        watch(args)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
