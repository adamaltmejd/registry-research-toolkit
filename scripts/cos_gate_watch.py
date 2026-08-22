"""Fast-tier merge-gate scan for the chief-of-staff watcher (cos_watch.py).

The highest-value wake event — "a PR is ready to merge" — is a LOCAL file write
(pr-pipeline atomically writes gate.json with `status: ready-to-merge` into the gate
store), so cos_watch.py polls it every ~20s via this module and wakes the loop within
seconds instead of waiting out a heartbeat.

Emit contract (deliberately narrow so the watcher cannot thrash the loop):

  ready gate: pr=<N> head=<sha12> updated=<iso>

one line per gate that is ready-to-merge now and was NOT ready with this exact
(head, updated) on the previous scan — i.e. a new handoff, a re-verification (`updated`
bump), or a new head. Steady-state ready gates, `status: blocked`, evidence-file
writes, and self-serve `build_db` running stamps never emit; those are slow-tier
concerns. The FIRST scan emits every already-ready gate, so a handoff that landed
while no watcher was running is surfaced rather than silently absorbed (the preflight
probe dedupes if it was already handled).

This scan makes no wake DECISIONS: the woken tick still starts with the
cos_preflight.py probe, whose baseline/fingerprint/--commit contract is unchanged. It
only makes the wake immediate. Local filesystem poll only — no network calls, no
inotify/fswatch dependency (portable to macOS).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def _load_gh() -> ModuleType:
    # The one leaf that can't go through _gh.load_sibling: _gh can't load itself. Kept a
    # tiny sys.modules-guarded spec-load, identical in every sibling script, so the whole
    # process shares ONE _gh instance (a single patch target, not one copy per loader).
    if (mod := sys.modules.get("_gh")) is not None:
        return mod
    spec = importlib.util.spec_from_file_location(
        "_gh", Path(__file__).with_name("_gh.py")
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_gh"] = mod
    spec.loader.exec_module(mod)
    return mod


# Reuse cos_preflight's gate-store leaf helpers (XDG root resolution, tolerant JSON read)
# instead of re-pasting them, via _gh's shared sys.modules-guarded loader — so this is the
# same cos_preflight instance cos_watch loads (the two tiers can't diverge).
_cos_preflight = _load_gh().load_sibling("cos_preflight")


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
