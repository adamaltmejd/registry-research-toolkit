#!/usr/bin/env python3
"""Event-tier wake for the chief-of-staff loop: watch the local merge-gate store.

The chief-of-staff loop wakes on two tiers. Tier 2 is the slow scheduled heartbeat that
polls remote GitHub drift. Tier 1 is this script: the highest-value event — "a PR is
ready to merge" — is a LOCAL file write (pr-pipeline atomically writes gate.json with
`status: ready-to-merge` into the gate store), so it can wake the loop within seconds
instead of waiting out the heartbeat. The loop arms this script once per session as a
persistent monitor; each stdout line is a wake event.

Emit contract (deliberately narrow so the watcher cannot thrash the loop):

  ready gate: pr=<N> head=<sha12> updated=<iso>

one line per gate that is ready-to-merge now and was NOT ready with this exact
(head, updated) on the previous scan — i.e. a new handoff, a re-verification (`updated`
bump), or a new head. Steady-state ready gates, `status: blocked`, evidence-file writes,
and self-serve `build_db` running stamps never emit; those are heartbeat concerns. The
FIRST scan emits every already-ready gate, so a handoff that landed while no watcher was
running is surfaced rather than silently absorbed (the preflight probe dedupes if it was
already handled).

This watcher makes no wake DECISIONS: the woken tick still starts with the
cos_preflight.py probe, whose baseline/fingerprint/--commit contract is unchanged. It
only makes the wake immediate. Local filesystem poll only — no network calls, no
inotify/fswatch dependency (portable to macOS).
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import time
from pathlib import Path

# Reuse cos_preflight's gate-store leaf helpers (XDG root resolution, tolerant JSON
# read) instead of re-pasting them; sibling scripts are spec-loaded, not packaged.
_CPF_SPEC = importlib.util.spec_from_file_location(
    "cos_preflight", Path(__file__).with_name("cos_preflight.py")
)
assert _CPF_SPEC and _CPF_SPEC.loader
_cos_preflight = importlib.util.module_from_spec(_CPF_SPEC)
sys.modules[_CPF_SPEC.name] = _cos_preflight
_CPF_SPEC.loader.exec_module(_cos_preflight)


def scan_ready(gate_root: Path) -> dict[int, tuple[str, str]]:
    """Map pr -> (head, updated) for every valid ready-to-merge gate entry.

    Follows the gate-store read protocol: an entry whose `pr` field disagrees with its
    directory name is absent; unreadable or corrupt files are skipped without dying
    (gate.json is written atomically, so a torn read is transient and resolves next
    poll). Only top-level pr-*/ dirs are scanned — the merged/ archive is not.
    """
    ready: dict[int, tuple[str, str]] = {}
    for path in sorted(gate_root.glob("pr-*/gate.json")):
        try:
            pr = int(path.parent.name.removeprefix("pr-"))
        except ValueError:
            continue
        loaded = _cos_preflight._read_json_tolerant(path, "merge-gate file")
        if loaded is None:
            continue
        gate = loaded[1]
        if not isinstance(gate, dict) or gate.get("pr") != pr:
            continue
        if gate.get("status") != "ready-to-merge":
            continue
        # A ready gate missing head/updated is malformed, but still emits (as "None"):
        # waking the tick to judge the malformed entry beats silently absorbing a
        # handoff, and the dedupe key stays stable either way.
        ready[pr] = (str(gate.get("head")), str(gate.get("updated")))
    return ready


def ready_events(
    previous: dict[int, tuple[str, str]], current: dict[int, tuple[str, str]]
) -> list[str]:
    """One line per gate ready now that wasn't ready with this (head, updated) before."""
    return [
        f"ready gate: pr={pr} head={head[:12]} updated={updated}"
        for pr, (head, updated) in sorted(current.items())
        if previous.get(pr) != (head, updated)
    ]


def watch(gate_root: Path, interval: float) -> None:
    seen: dict[int, tuple[str, str]] = {}
    while True:
        current = scan_ready(gate_root)
        for line in ready_events(seen, current):
            print(line, flush=True)
        seen = current
        time.sleep(interval)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--gate-dir",
        type=Path,
        default=_cos_preflight.default_gate_root(),
        help="local merge-gate store root (pr-<N>/gate.json lives under it); overrides "
        "the XDG-derived default, for tests and non-default setups",
    )
    ap.add_argument(
        "--interval",
        type=float,
        default=20.0,
        help="seconds between gate-store scans",
    )
    ap.add_argument(
        "--once",
        action="store_true",
        help="single scan: print every currently-ready gate and exit",
    )
    args = ap.parse_args(argv)
    if args.once:
        for line in ready_events({}, scan_ready(args.gate_dir)):
            print(line, flush=True)
        return 0
    try:
        watch(args.gate_dir, args.interval)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
