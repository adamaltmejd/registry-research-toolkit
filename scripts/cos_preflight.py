#!/usr/bin/env python3
"""Cheap deterministic wake gate for the registry chief-of-staff loop.

This script is the first tool call of a scheduled chief-of-staff agent session: it
checks the small set of repo/GitHub signals that can make a tick useful and, when the
state is unchanged, lets the session stop immediately without spending tokens.

Baseline-advance invariant: the probe auto-advances the baseline (writes the state file
directly) whenever it observed NOTHING actionable and the observation moved — i.e. reasons
empty AND (no baseline yet, or the fingerprint drifted). This is safe by construction: zero
reasons means there are no events to burn. Observations WITH reasons advance the baseline
ONLY via the fingerprint-bound `--commit`, so a crash before the end-of-tick commit
re-fires. A steady-state probe whose fingerprint equals the baseline writes nothing.

Three modes:
  cos_preflight.py             probe (default): read the committed baseline from the state
                               file, take a fresh snapshot, compare, and always stage the
                               snapshot as a CANDIDATE next to the state file. Per the
                               invariant above, a reason-free probe also advances the
                               baseline directly (first-run bootstrap, or idle drift); a
                               WAKING probe writes only the candidate and lets the
                               end-of-tick commit establish the baseline.
  cos_preflight.py --observe   read-only probe: same snapshot/compare/exit codes, but
                               writes NOTHING — no candidate staging, no idle baseline
                               advance. For an external watcher (scripts/cos_watch.py)
                               polling on the session's behalf: a mid-tick watcher probe
                               must never replace the candidate the active tick will
                               `--commit`, or the commit fails a fingerprint mismatch and
                               strands handled events. The agent tick's own probe still
                               stages and commits as usual.
  cos_preflight.py --commit F  promote the observed candidate (identified by the
                               fingerprint F the probe printed) to the state file via one
                               atomic rename and exit. No snapshot collection or network
                               calls, but it DOES still verify the canonical checkout.

Probe contract:
  exit 0  idle; no agent work needed
  exit 10 wake; stdout JSON names the reasons to resume the COS thread
  exit 2  tool/setup error; stderr explains what failed

Commit contract:
  exit 0  candidate promoted (or already committed — idempotent); one-line confirmation
  exit 2  fingerprint mismatch, no staged candidate, canonical-checkout failure, or a
          tool/setup error; stderr explains what failed

The session loop is: probe (exit 0 → stop / exit 10 → do the tick) → `--commit <fp>`
using the fingerprint from THAT probe's output → probe again → if 10, handle and
`--commit` the new fingerprint, and so on. Every round ends with its own commit; the last
batch must be committed too or it re-wakes as duplicate work. The probe writes what it
observes to a candidate file; `--commit` only promotes the candidate whose fingerprint the
caller observed, so an event that lands in the probe→commit window is caught by the
post-tick re-probe (which refreshes the candidate) rather than absorbed silently into the
committed baseline. Committing only after a successful tick gives at-least-once semantics:
a failed tick leaves the baseline at the last committed candidate, so the next scheduled
session re-observes the pending event, instead of the old at-most-once behavior where
writing at detection burned the event even when the tick that followed failed.

The snapshot also carries the pipeline-slot ledger (`slots`: a sorted list of
`{"slug", "stale"}`, stale = the slot file's mtime is older than the stale threshold at
observation time). Only slot MEMBERSHIP and staleness are fingerprinted — slot-file
contents (prs/issues/ownership) are deliberately excluded, so a draft PR opening or an
ownership stamp inside a slot never wakes the chief. Three slot reasons ride the same
probe/--commit durability as the PR/gate events: `pipeline slot freed: <slugs>` (a slug
left the ledger; previous-gated, no freed reason on first run), `dispatch: <n> slot(s)
free` (fires only on a freed transition that leaves the ledger under --max-slots, never
on a merely under-budget steady state), and `stale pipeline slot: <slug>; adjudicate or
release` (a stale slot that is new or just flipped fresh→stale; wakes on first run too,
since it is standing adjudication state). A slot claim (a new non-stale slug) and a
stale→fresh flip are silent: the fingerprint moves with zero reasons, absorbed by the
idle auto-advance path. The watcher's fast tier (cos_watch.py) sees the same ledger
transitions from emission text alone for low latency; this probe re-observes them
durably (at-least-once) even if a session misses a fast-tier emission.

Fork-PR trust gate (this repo is public): a PR from a branch outside this repository
carries untrusted text and an untrusted `Closes #N`, so its per-PR summary is stripped to
`{number, fork: true, author, draft}` — NEITHER its title/body nor its closing claims
enter the snapshot the chief-of-staff model reads, and it never fetches the Codex signal
or reaches a ready/draft/stale bucket. The only load-bearing signal on a fork is
provenance: a fork PR can never write the local gate store (only local agents can), so a
gate entry for one is an error — it is flagged (`gate_present: true`) and surfaced as a
distinct wake reason so the chief refuses and investigates rather than merging. A plain
fork's appearance/disappearance is visible via its own named reason (how the chief learns
it exists) without its content ever being ingested. Own-branch PR summaries are unchanged.

It does not make coordination decisions, edit issues, merge PRs, or run the dev
preview. It only decides whether a real chief-of-staff tick is worth spending tokens on.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

WAKE_EXIT = 10
# Bump whenever the snapshot dict shape changes; load_state treats a mismatch as first-run
# so a schema change re-baselines cleanly instead of comparing incompatible shapes.
SNAPSHOT_VERSION = 5
DEFAULT_CANONICAL = Path("/Users/adam/Code/registry-research-toolkit")
PASSING_CONCLUSIONS = {"SUCCESS", "SKIPPED", "NEUTRAL"}
NO_STATUS_CHANGES = "projection delta:\nno status changes"
# Pipeline-slot ledger defaults. The ledger lives next to the merge-gate store (see
# default_slots_root); cos_watch.py's fast tier references these same constants so the
# two tiers can never disagree on the concurrency budget or the staleness threshold.
DEFAULT_MAX_SLOTS = 3
DEFAULT_SLOT_STALE_HOURS = 24.0


def _load_sibling(name: str) -> Any:
    # Sibling scripts load each other via importlib spec (not a plain import) so they
    # resolve under `uv run --no-project python scripts/<name>.py` and spec-loaded pytest
    # alike, regardless of sys.path — the same idiom gh_issue.py uses for _gh.py.
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(f"{name}.py")
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_plan_sequence = _load_sibling("plan_sequence")
_gh = _load_sibling("_gh")
# gh_issue.is_own_pr is the single fail-closed fork predicate (only isCrossRepository is
# False is own-branch); reuse it rather than re-implementing the check here.
gh_issue = _load_sibling("gh_issue")
DEFAULT_PR_FETCH_CAP = getattr(_plan_sequence, "FETCH_CAP", 5000)

# plan_sequence.py --tick prints a deterministic `lanes: <verdict>` line on stderr and
# encodes the same verdict in its exit code (1 re-rank / 2 re-stamp; 0 is fresh). An
# unhandled traceback also exits 1 (or a non-{0,1,2} code) but WITHOUT the sentinel, so
# we accept 1/2 as a signal only when its sentinel is present; otherwise it's a tool
# error. This stops a crash + its recovery from reading as two spurious wakes. Derive the
# sentinel strings from plan_sequence's own _FRESHNESS_MSG (matched by --tick's `lanes: `
# prefix) so a rewording there can't misclassify a real verdict as a crash.
PLAN_TICK_SENTINELS = {
    _plan_sequence._FRESHNESS_EXIT[v]: f"lanes: {_plan_sequence._FRESHNESS_MSG[v]}"
    for v in ("rerank", "restamp")
}


# Run-a-command-tolerating-non-zero: the shared _gh.run_tolerant primitive (a missing
# executable maps to a `missing executable` SystemExit; a non-zero exit is handed back for
# the caller to inspect). Bound at module level so tests can still monkeypatch cpf.run_cmd.
run_cmd = _gh.run_tolerant


def require_canonical(canonical: Path | None) -> None:
    if canonical is None:
        return
    cwd = Path.cwd().resolve()
    if cwd != canonical.resolve():
        raise SystemExit(
            f"cos-preflight must run from canonical checkout {canonical}; got {cwd}"
        )
    if not Path(".git").is_dir():
        raise SystemExit(
            "cos-preflight requires a real .git directory, not a worktree .git file"
        )
    branch = run_cmd(["git", "branch", "--show-current"])
    if branch.returncode != 0:
        raise SystemExit(branch.stderr.strip() or "failed to read current branch")
    if branch.stdout.strip() != "main":
        raise SystemExit(
            f"cos-preflight must run on main; got {branch.stdout.strip()!r}"
        )


def git_stdout(args: list[str]) -> str:
    proc = run_cmd(["git", *args])
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"git {' '.join(args)} failed")
    return proc.stdout.strip()


def remote_main_sha() -> str:
    out = git_stdout(["ls-remote", "origin", "refs/heads/main"])
    return out.split()[0] if out else ""


def local_head_sha() -> str:
    return git_stdout(["rev-parse", "HEAD"])


def gh_json(args: list[str]) -> Any:
    proc = run_cmd(["gh", *args])
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"gh {' '.join(args)} failed")
    return json.loads(proc.stdout)


def repo_name_with_owner() -> str:
    data = gh_json(["repo", "view", "--json", "nameWithOwner"])
    return data["nameWithOwner"]


def run_plan_tick(epic: int) -> dict[str, Any]:
    proc = run_cmd(
        [sys.executable, "scripts/plan_sequence.py", "--tick", "--epic", str(epic)]
    )
    sentinel = PLAN_TICK_SENTINELS.get(proc.returncode)
    if sentinel is not None and sentinel not in proc.stderr:
        # exit 1/2 without its sentinel is a crash exiting non-zero, not a re-rank/re-stamp
        # signal — treat as a tool error so the traceback doesn't pollute the fingerprint.
        raise SystemExit(proc.stderr.strip() or "plan_sequence.py --tick crashed")
    if proc.returncode not in {0, 1, 2}:
        raise SystemExit(proc.stderr.strip() or "plan_sequence.py --tick failed")
    return {
        "exit": proc.returncode,
        "basis": proc.stdout.strip(),
        "report": proc.stderr.strip(),
    }


def default_gate_root() -> Path:
    # Local gate store root: $XDG_STATE_HOME/registry-research-toolkit/merge-gates, or the
    # XDG default ~/.local/state/... when XDG_STATE_HOME is unset. All pipelines run on this
    # machine, so a local file is durable across worktree deletion / git clean / reboots.
    xdg = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "state"
    return base / "registry-research-toolkit" / "merge-gates"


def default_slots_root() -> Path:
    # The pipeline-slot ledger lives next to the merge-gate store (its sibling under the
    # same registry-research-toolkit state root), so a --gate-dir override moves both.
    return default_gate_root().parent / "pipeline-slots"


def scan_slots(slots_root: Path) -> set[str]:
    """Slugs of every valid pipeline-slot file (top level only; done/ is the archive).

    Mirrors the gate protocol's read rules: a slot whose `slot` field disagrees with
    its filename stem is absent; unreadable/corrupt files are skipped (slot files are
    written atomically, so a torn read is transient); a missing root scans as empty.
    """
    slots: set[str] = set()
    for path in sorted(slots_root.glob("*.json")):
        loaded = _read_json_tolerant(path, "pipeline-slot file")
        if loaded is None:
            continue
        slot = loaded[1]
        if not isinstance(slot, dict) or slot.get("slot") != path.stem:
            continue
        slots.add(path.stem)
    return slots


def _read_json_tolerant(path: Path, label: str) -> tuple[bytes, Any] | None:
    # Shared self-heal loader for the on-disk JSON files (state file, merge-gate file).
    # Returns (raw_bytes, parsed) or None on any read/decode failure — a missing file is
    # None with no warning; an unreadable file (OSError) or undecodable/corrupt content
    # warns then None. json.loads on bytes raises UnicodeDecodeError for non-UTF-8 input,
    # and read_text raises it too; both are ValueError subclasses alongside JSONDecodeError,
    # so catching ValueError covers corrupt-JSON AND bad-encoding without an uncaught
    # traceback breaking the caller's exit contract. Callers keep their own shape checks
    # (version sentinel; dict + pr-match) on the parsed value.
    try:
        raw_bytes = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        print(
            f"warning: ignoring unreadable {label} {path} ({exc}); treating as absent",
            file=sys.stderr,
        )
        return None
    try:
        return raw_bytes, json.loads(raw_bytes)
    except ValueError as exc:
        print(
            f"warning: ignoring corrupt {label} {path} ({exc}); treating as absent",
            file=sys.stderr,
        )
        return None


def read_merge_gate(
    gate_root: Path, pr_number: int, head_oid: str
) -> dict[str, str | bool | None]:
    # The merge-gate handoff lives in the local gate store (pr-<N>/gate.json), not the PR
    # body. Returns the same shape summarize_pr consumes; gate_hash fingerprints the raw
    # bytes so an evidence/status edit changes it and wakes the chief.
    absent: dict[str, str | bool | None] = {
        "state": "absent",
        "status": None,
        "head": None,
        "current": False,
        "gate_hash": None,
    }
    path = gate_root / f"pr-{pr_number}" / "gate.json"
    loaded = _read_json_tolerant(path, "merge-gate file")
    if loaded is None:
        return absent
    raw_bytes, gate = loaded
    if not isinstance(gate, dict) or gate.get("pr") != pr_number:
        print(
            f"warning: ignoring merge-gate file {path} whose shape/pr is unexpected "
            f"(pr={gate.get('pr') if isinstance(gate, dict) else '?'!r}, "
            f"expected {pr_number}); treating as absent",
            file=sys.stderr,
        )
        return absent
    gate_hash = hashlib.sha256(raw_bytes).hexdigest()
    status = gate.get("status")
    head = gate.get("head")
    current = bool(head and head == head_oid)
    if status == "ready-to-merge" and current:
        state = "current-ready"
    elif status == "ready-to-merge":
        state = "stale-ready"
    else:
        state = "present"
    return {
        "state": state,
        "status": status,
        "head": head,
        "current": current,
        "gate_hash": gate_hash,
    }


def normalize_checks(checks: list[dict[str, Any]]) -> list[dict[str, str | None]]:
    out: list[dict[str, str | None]] = []
    for check in checks:
        out.append(
            {
                "name": check.get("name") or check.get("context") or "",
                "workflow": check.get("workflowName"),
                "status": check.get("status") or check.get("state"),
                "conclusion": check.get("conclusion"),
            }
        )
    return sorted(out, key=lambda c: (c["workflow"] or "", c["name"] or ""))


# Legacy commit statuses (StatusContext) expose only `state` and no `conclusion`; a
# FAILURE/ERROR there must read as failing, not fall through the "still running" gate
# below (COMPLETED/SUCCESS) into "pending".
FAILING_STATES = {"FAILURE", "ERROR"}


def checks_verdict(checks: list[dict[str, Any]]) -> str:
    normalized = normalize_checks(checks)
    if not normalized:
        return "none"
    for check in normalized:
        if check["status"] in FAILING_STATES or (
            (conclusion := check["conclusion"])
            and conclusion not in PASSING_CONCLUSIONS
        ):
            return "failing"
    for check in normalized:
        status = check["status"]
        if status and status not in {"COMPLETED", "SUCCESS"}:
            return "pending"
    return "passing"


def codex_signal(pr: int) -> str:
    proc = run_cmd([sys.executable, "scripts/pr_review_status.py", str(pr), "--once"])
    if proc.returncode not in {0, 1}:
        raise SystemExit(
            proc.stderr.strip() or f"pr_review_status.py {pr} --once failed"
        )
    return json.loads(proc.stdout)["signal"]


def normalize_reviews(reviews: list[dict[str, Any]]) -> list[dict[str, str | None]]:
    # Only the author login + submittedAt of each latest review — enough to see a new
    # Codex/human verdict land on an in-flight PR without embedding the (churny) body.
    out = [
        {
            "author": (review.get("author") or {}).get("login"),
            "submitted_at": review.get("submittedAt"),
        }
        for review in reviews
    ]
    return sorted(out, key=lambda r: (r["author"] or "", r["submitted_at"] or ""))


def summarize_pr(
    raw: dict[str, Any], current_repo: str, gate_root: Path
) -> dict[str, Any]:
    if not gh_issue.is_own_pr(raw):
        # Fork PR (this repo is public): its title/body are untrusted text and its
        # `Closes #N` is an untrusted claim on this repo's issues, so NEITHER may enter the
        # snapshot the chief-of-staff model reads. Surface only the number, author login,
        # and a fork flag — enough for the chief to learn the fork PR exists (appearance /
        # disappearance) without ingesting attacker-controlled content. Skip the
        # closing-claim computation entirely (issue-holdout DoS) and never fetch the Codex
        # signal or reach a ready/draft/stale bucket. read_merge_gate reads the LOCAL gate
        # store (keyed by PR number + head, not untrusted text), so it is safe: a fork PR
        # can never write it, so an entry is an error condition — flag it so actionable_
        # reasons surfaces "refuse and investigate" rather than the chief merging a fork.
        author = raw.get("author") or {}
        fork_summary: dict[str, Any] = {
            "number": raw["number"],
            "fork": True,
            "author": author.get("login"),
            "draft": bool(raw.get("isDraft")),
        }
        gate_state = read_merge_gate(gate_root, raw["number"], raw["headRefOid"])[
            "state"
        ]
        if gate_state != "absent":
            fork_summary["gate_present"] = True
        return fork_summary

    # The closing refs still come from the PR body (only the GATE moved to the local
    # store); the gate entry is now read from gate_root/pr-<N>/gate.json.
    body = raw.get("body") or ""
    closing = {ref["number"] for ref in raw.get("closingIssuesReferences") or []}
    closing.update(_plan_sequence.closing_issue_numbers_from_body(body, current_repo))
    gate = read_merge_gate(gate_root, raw["number"], raw["headRefOid"])

    if not closing and gate["state"] == "absent":
        # An unclaimed PR (no closing refs, no gate entry): the chief still needs to know
        # it exists so it can fix the missing `Closes #N` claim drift. Include it
        # MINIMALLY — number + isDraft only. Deliberately NOT the head SHA, so routine
        # pushes don't wake; only appearance / disappearance / draft-flip does.
        return {
            "number": raw["number"],
            "claimed": False,
            "draft": bool(raw.get("isDraft")),
        }

    signal = None
    if gate["state"] == "current-ready":
        signal = codex_signal(raw["number"])

    summary: dict[str, Any] = {
        "number": raw["number"],
        "claimed": True,
        "title": raw.get("title") or "",
        "issues": sorted(closing),
        "base": raw.get("baseRefName"),
        "head": raw["headRefOid"],
        "draft": bool(raw.get("isDraft")),
        # Overall checks verdict only — the full per-check-run list would wake on every
        # individual check transition within one CI run.
        "checks": checks_verdict(raw.get("statusCheckRollup") or []),
        "gate": gate,
        "codex_signal": signal,
    }
    if gate["state"] != "absent":
        # PR carries a gate entry, so its mergeability IS load-bearing: a tick may have
        # deferred the merge because mergeability was still UNKNOWN, and it must wake when
        # GitHub resolves UNKNOWN→MERGEABLE. Store the verbatim tri-state. Flapping on gate
        # PRs is rare and co-occurs with the remote_main wakes that recompute triggers.
        summary["mergeable"] = raw.get("mergeable")
    else:
        # No gate entry: mergeability isn't load-bearing, so keep only the stable
        # CONFLICTING signal — GitHub's transient UNKNOWN↔MERGEABLE flapping would
        # otherwise wake the chief on every recompute.
        summary["conflicting"] = raw.get("mergeable") == "CONFLICTING"
    # A new review on an in-flight issue-closing PR is the chief's "send unblock
    # follow-up" trigger even before the gate is current-ready, so surface it here. This
    # is only a cheap change-detector for review ACTIVITY: a Codex clean verdict shaped as
    # a "Reviewed commit: <sha>" comment or a 👍 reaction is invisible here — reading that
    # verdict stays the merge-gate poller's (scripts/pr_review_status.py) job.
    if closing:
        summary["reviews"] = normalize_reviews(raw.get("latestReviews") or [])
    return summary


def fetch_pr_summaries(
    limit: int, current_repo: str, gate_root: Path
) -> list[dict[str, Any]]:
    prs = gh_json(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            str(limit),
            "--json",
            "number,title,body,author,isCrossRepository,closingIssuesReferences,baseRefName,isDraft,mergeable,headRefOid,statusCheckRollup,latestReviews",
        ]
    )
    if len(prs) >= limit:
        raise SystemExit(
            f"open PR fetch hit --pr-limit={limit}; increase the cap or paginate before "
            "using cos-preflight for idle gating"
        )
    summaries = [summarize_pr(pr, current_repo, gate_root) for pr in prs]
    return sorted(summaries, key=lambda s: s["number"])


def collect_slots(slots_root: Path, stale_seconds: float) -> list[dict[str, Any]]:
    """Snapshot the pipeline-slot ledger as a sorted list of {"slug", "stale"}.

    Deliberately records ONLY membership + staleness, never the slot file's contents
    (prs/issues/ownership): a draft PR opening or an ownership stamp inside a slot must
    not wake the chief — that is fast-tier / tick-body concern, not a wake signal. Stale
    means the slot file's mtime is older than the threshold at observation time; a slot
    whose file vanishes between scan_slots and the stat (racy release) is dropped.
    """
    now = datetime.now(UTC).timestamp()
    slots: list[dict[str, Any]] = []
    for slug in sorted(scan_slots(slots_root)):
        try:
            mtime = (slots_root / f"{slug}.json").stat().st_mtime
        except OSError:
            continue
        slots.append({"slug": slug, "stale": now - mtime >= stale_seconds})
    return slots


def snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    stable = {
        "local_head": snapshot["local_head"],
        "remote_main": snapshot["remote_main"],
        "plan_tick": snapshot["plan_tick"],
        "prs": snapshot["prs"],
        # Slot membership + staleness only (collect_slots excludes slot-file contents),
        # so slot transitions ride the same fingerprint/--commit durability as PRs.
        "slots": snapshot["slots"],
    }
    raw = json.dumps(stable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _changed_pr_numbers(
    snapshot: dict[str, Any], previous: dict[str, Any] | None
) -> set[int]:
    """PR numbers whose snapshot entry differs from the previous snapshot.

    First run (no previous) counts every PR as changed. Otherwise a PR is changed if it
    appeared, disappeared, or its entry moved — so a gate reason names only the PRs that
    actually moved, not every PR in the map.
    """
    current = {pr["number"]: pr for pr in snapshot["prs"]}
    if previous is None:
        return set(current)
    prior = {pr["number"]: pr for pr in previous.get("prs") or []}
    return {n for n in current.keys() | prior.keys() if current.get(n) != prior.get(n)}


def _slot_reasons(
    snapshot: dict[str, Any], previous: dict[str, Any] | None
) -> list[str]:
    """Freed / dispatch / stale-onset reasons from the slot-ledger delta.

    Mirrors slot_events in cos_watch.py so the durable (probe) and low-latency (fast
    tier) paths agree: freed keys on a slug leaving the ledger (previous-gated — no
    freed reason on first run), dispatch keys on a freed TRANSITION that leaves the
    ledger under max_slots (never on a merely under-budget steady state), and stale
    onset keys on a stale slot that is new or just flipped fresh→stale (waking on first
    run too, since it is standing adjudication state). A claim (new non-stale slug) and
    a stale→fresh flip emit nothing — the fingerprint moved, and the idle auto-advance
    path absorbs it.
    """
    max_slots = snapshot["max_slots"]
    current = {s["slug"]: s for s in snapshot["slots"]}
    prior = (
        {s["slug"]: s for s in previous.get("slots") or []}
        if previous is not None
        else {}
    )

    reasons: list[str] = []
    # Freed: a slug present before, gone now. Previous-gated — on first run `prior` is
    # empty so nothing reads as freed (same rationale as the origin/main first-run gate).
    freed = sorted(prior.keys() - current.keys()) if previous is not None else []
    if freed:
        reasons.append(f"pipeline slot freed: {', '.join(freed)}")
        # Dispatch keys on the freed TRANSITION, not on merely being under budget, so an
        # idle under-budget steady state never re-recommends. max_slots is a
        # comparison-time input, intentionally NOT part of the fingerprint.
        if len(current) < max_slots:
            reasons.append(f"dispatch: {max_slots - len(current)} slot(s) free")

    # Stale onset: a current slot that is stale AND whose entry is new (first appearance,
    # incl. first run) or just flipped fresh→stale. A stale→fresh flip changes the entry
    # but emits nothing (the fingerprint move is absorbed by idle auto-advance).
    onset = sorted(
        slug
        for slug, entry in current.items()
        if entry["stale"] and prior.get(slug) != entry
    )
    if onset:
        reasons.append(
            f"stale pipeline slot: {', '.join(onset)}; adjudicate or release"
        )
    return reasons


def actionable_reasons(
    snapshot: dict[str, Any], previous: dict[str, Any] | None
) -> list[str]:
    if previous and previous.get("fingerprint") == snapshot["fingerprint"]:
        return []

    reasons: list[str] = []
    plan = snapshot["plan_tick"]
    if plan["exit"] == 1:
        reasons.append("lanes need re-rank")
    elif plan["exit"] == 2:
        reasons.append("lanes need re-stamp")
    if NO_STATUS_CHANGES not in plan["report"]:
        reasons.append("issue projection changed")
    if previous is None and snapshot["local_head"] != snapshot["remote_main"]:
        reasons.append("origin/main changed")

    changed = _changed_pr_numbers(snapshot, previous)

    ready: list[str] = []
    draft_ready: list[str] = []
    stale: list[str] = []
    unclaimed: list[str] = []
    fork: list[str] = []
    fork_gated: list[str] = []
    named: set[int] = set()  # PRs already called out by a named bucket below
    for pr in snapshot["prs"]:
        if pr["number"] not in changed:
            continue
        if pr.get("fork"):
            # Fork PR (untrusted, from a branch outside this repo): a gate entry for one
            # can only be a provenance error — only local agents can write the gate store,
            # so a fork can never self-certify — surface it distinctly so the chief refuses
            # and investigates rather than merging. A plain fork gets its own named reason
            # too (not the previous-gated generic one) so its first-run appearance is
            # visible: it is how the chief learns a fork PR exists, and its text stayed out
            # of the snapshot entirely.
            bucket = fork_gated if pr.get("gate_present") else fork
        elif not pr.get("claimed", True):
            # Unclaimed PR (no Closes ref, no gate entry): its own named reason, so a
            # first-run probe (where `changed` covers every PR) wakes for pre-existing
            # claim drift instead of the generic reason silently absorbing it into the
            # bootstrap baseline. The generic reason is previous-gated, so it can't.
            bucket = unclaimed
        else:
            state = pr.get("gate", {}).get("state")
            if state == "current-ready" and not pr["draft"]:
                bucket = ready
            elif state == "current-ready" and pr["draft"]:
                bucket = draft_ready
            elif state == "stale-ready":
                bucket = stale
            else:
                continue
        bucket.append(f"#{pr['number']}")
        named.add(pr["number"])
    if ready:
        reasons.append(f"ready merge-gate PR changed: {', '.join(ready)}")
    if draft_ready:
        reasons.append(f"draft PR has ready merge-gate entry: {', '.join(draft_ready)}")
    if stale:
        reasons.append(f"stale merge-gate PR changed: {', '.join(stale)}")
    if unclaimed:
        reasons.append(
            f"unclaimed open PR (no Closes, no gate entry): {', '.join(unclaimed)}"
        )
    if fork:
        reasons.append(f"fork PR present (text ignored): {', '.join(fork)}")
    if fork_gated:
        reasons.append(
            f"fork PR with merge-gate entry: {', '.join(fork_gated)}; "
            "refuse and investigate"
        )

    if previous:
        if previous.get("remote_main") != snapshot["remote_main"]:
            reasons.append("origin/main changed")
        # Generic fallback only for changed PRs no named bucket already covered — otherwise
        # a named-state change would emit both its specific reason and this redundant one.
        if changed - named:
            reasons.append("open PR state changed")

    reasons.extend(_slot_reasons(snapshot, previous))

    return sorted(dict.fromkeys(reasons))


def default_state_file() -> Path:
    return Path(".git") / "cos-preflight-state.json"


def candidate_file(state_file: Path) -> Path:
    return state_file.with_name(state_file.name + ".candidate")


def load_state(path: Path) -> dict[str, Any] | None:
    # Self-heal via the shared loader: a missing, unreadable, or truncated/corrupt state
    # file is treated as first-run (warn, then probe as if no prior snapshot). The cost is
    # one extra wake; the alternative — hard-stopping every run until someone hand-deletes
    # the file — silently disables the whole gate. The first-run bootstrap re-baselines it.
    loaded = _read_json_tolerant(path, "cos-preflight state file")
    if loaded is None:
        return None
    state = loaded[1]
    if not isinstance(state, dict) or state.get("version") != SNAPSHOT_VERSION:
        # A schema bump (SNAPSHOT_VERSION) changes the snapshot shape, so an old baseline
        # can't be compared meaningfully. Treat it as first-run; the bootstrap re-baselines
        # to the new shape instead of emitting one tick of spurious migration-noise reasons.
        print(
            f"warning: ignoring cos-preflight state file {path} with incompatible "
            f"version {state.get('version') if isinstance(state, dict) else '?'!r} "
            f"(expected {SNAPSHOT_VERSION}); treating as first run",
            file=sys.stderr,
        )
        return None
    return state


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    # The shared torn-write guard: write a temp file in the SAME dir (so the rename is a
    # cheap intra-directory atomic swap) then rename over `path`, so a concurrent reader
    # (scan_slots / load_state) never sees a partial write. On a rename failure the temp
    # file is unlinked so no *.tmp is leaked. Callers own their divergent pre-steps (the
    # parent-dir policy) — this is only the write+rename tail. json.dumps(indent=2,
    # sort_keys=True) + a trailing newline is the on-disk format both callers commit to.
    with NamedTemporaryFile(
        "w",
        dir=path.parent,
        encoding="utf-8",
        prefix=f"{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as fh:
        tmp = Path(fh.name)
        fh.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    try:
        tmp.replace(path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def write_state(path: Path, snapshot: dict[str, Any]) -> None:
    parent = path.parent
    if not parent.is_dir():
        # Never mkdir(parents=True) here: with --no-canonical-check and a missing .git,
        # the default state path (.git/cos-preflight-state.json) would silently create a
        # stray .git/ directory. Also refuse when .git is a FILE (a linked worktree): the
        # parent isn't a real directory, so NamedTemporaryFile below would raise an
        # uncaught NotADirectoryError and break the exit-2 contract. Fail clearly instead.
        raise SystemExit(
            f"cos-preflight state directory {parent} is not a directory; "
            "run from the canonical checkout or pass an existing --state-file dir"
        )
    _atomic_write_json(path, snapshot)


def promote_candidate(state_file: Path, fingerprint: str) -> tuple[str, bool]:
    """Promote the observed candidate to the state file, bound to `fingerprint`.

    `--commit <fingerprint>` passes the `fingerprint` the probe printed in its result JSON,
    so the promotion is bound to what the caller actually observed. Two holes this closes:

    1. A candidate left by an ABANDONED tick (crash / loop-bound exit with events
       unhandled) persists on disk. A later mis-ordered `--commit` run before any probe
       would otherwise promote it and burn events no tick processed. Refusing to promote a
       candidate whose fingerprint the caller didn't observe blocks that.
    2. A `--commit` that succeeded then lost its result (tool timeout after the rename)
       leaves the baseline already at `fingerprint` with no candidate. Treating that as an
       idempotent success (exit 0, "already committed") makes retry-once always safe
       instead of a false "no candidate" failure.

    Returns (fingerprint, already_committed). Promotion is a single atomic rename
    (candidate.replace(state_file)) — no separate write+unlink, so there is no partial
    state to lose. Raises SystemExit (exit 2) on a mismatch or a genuine missing candidate.
    """
    candidate = candidate_file(state_file)
    staged = load_state(candidate)
    if staged is not None:
        if staged["fingerprint"] != fingerprint:
            raise SystemExit(
                f"staged candidate {staged['fingerprint'][:12]} does not match "
                f"--commit fingerprint {fingerprint[:12]}; re-run the probe"
            )
        candidate.replace(state_file)  # atomic: no partial write+unlink window
        return fingerprint, False
    # No candidate. If the baseline already IS this fingerprint, a prior --commit landed
    # and only its result was lost — idempotent success. Otherwise nothing observed this
    # was ever staged, so there is nothing to commit.
    baseline = load_state(state_file)
    if baseline is not None and baseline["fingerprint"] == fingerprint:
        return fingerprint, True
    raise SystemExit(
        f"no staged candidate at {candidate}; run the probe before --commit"
    )


def collect_snapshot(
    epic: int,
    pr_limit: int,
    gate_root: Path,
    slots_root: Path,
    max_slots: int,
    slot_stale_hours: float,
) -> dict[str, Any]:
    current_repo = repo_name_with_owner()
    snapshot = {
        "version": SNAPSHOT_VERSION,
        "observed_at": datetime.now(UTC).isoformat(),
        "local_head": local_head_sha(),
        "remote_main": remote_main_sha(),
        "plan_tick": run_plan_tick(epic),
        "prs": fetch_pr_summaries(pr_limit, current_repo, gate_root),
        "slots": collect_slots(slots_root, slot_stale_hours * 3600),
        # max_slots is a comparison-time input for _slot_reasons' dispatch gate, carried
        # on the snapshot but deliberately kept OUT of snapshot_fingerprint's stable set:
        # a budget change must not by itself move the fingerprint or burn events.
        "max_slots": max_slots,
    }
    snapshot["fingerprint"] = snapshot_fingerprint(snapshot)
    return snapshot


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--epic", type=int, default=328)
    ap.add_argument("--pr-limit", type=int, default=DEFAULT_PR_FETCH_CAP)
    ap.add_argument("--state-file", type=Path, default=default_state_file())
    ap.add_argument(
        "--gate-dir",
        type=Path,
        default=default_gate_root(),
        help="local merge-gate store root (pr-<N>/gate.json lives under it); overrides "
        "the XDG-derived default, for tests and non-default setups",
    )
    ap.add_argument(
        "--slots-dir",
        type=Path,
        default=None,
        help="pipeline-slot ledger root; defaults to the sibling of --gate-dir "
        "(pipeline-slots next to merge-gates), so overriding the gate root moves both "
        "stores together — the same sibling rule cos_watch.py uses",
    )
    ap.add_argument(
        "--max-slots",
        type=int,
        default=DEFAULT_MAX_SLOTS,
        help="pipeline concurrency budget; the dispatch reason fires only when a freed "
        "slot leaves the ledger below this. Comparison-time input, not fingerprinted",
    )
    ap.add_argument(
        "--slot-stale-hours",
        type=float,
        default=DEFAULT_SLOT_STALE_HOURS,
        help="a slot untouched for longer than this is stale (adjudicate or release)",
    )
    ap.add_argument(
        "--canonical",
        type=Path,
        default=DEFAULT_CANONICAL,
        help="required canonical checkout path",
    )
    ap.add_argument(
        "--no-canonical-check",
        action="store_true",
        help="skip the canonical checkout guard, for tests or local runs",
    )
    ap.add_argument(
        "--commit",
        metavar="FINGERPRINT",
        help="promote the observed candidate (identified by the FINGERPRINT the probe "
        "printed) to the state file and exit; no snapshot collection or network calls, "
        "but it does still verify the canonical checkout. Run after a successful tick so "
        "the observed events are marked handled",
    )
    ap.add_argument(
        "--observe",
        action="store_true",
        help="read-only probe: compute reasons against the committed baseline but write "
        "nothing (no candidate staging, no idle baseline advance). For external "
        "watchers polling on the session's behalf; the tick's own probe still stages",
    )
    args = ap.parse_args(argv)
    if args.commit is not None and args.observe:
        print("--observe and --commit are mutually exclusive", file=sys.stderr)
        return 2
    if args.slots_dir is None:
        # Sibling of the gate root, so a --gate-dir override moves both stores together
        # (the same rule cos_watch.py applies). --commit ignores this — no snapshot.
        args.slots_dir = args.gate_dir.parent / "pipeline-slots"

    canonical = None if args.no_canonical_check else args.canonical
    try:
        require_canonical(canonical)
        if args.commit is not None:
            # Promote only — no snapshot collection / network. Bound to the observed
            # fingerprint so a stale abandoned candidate can't be promoted and a lost-result
            # retry is idempotent.
            fingerprint, already = promote_candidate(args.state_file, args.commit)
            verb = "already committed" if already else "committed"
            print(
                f"{verb} cos-preflight snapshot {fingerprint[:12]} to {args.state_file}"
            )
            return 0
        # Probe: read the committed baseline, snapshot fresh, compare, stage the candidate.
        previous = load_state(args.state_file)
        snapshot = collect_snapshot(
            args.epic,
            args.pr_limit,
            args.gate_dir,
            args.slots_dir,
            args.max_slots,
            args.slot_stale_hours,
        )
        reasons = actionable_reasons(snapshot, previous)
        # --observe is a read-only probe: same snapshot/compare/exit codes, but it skips
        # BOTH writes below — a watcher probe racing an active tick must not replace the
        # candidate that tick will --commit (fingerprint mismatch would strand handled
        # events). Unhandled events keep re-firing (baseline unmoved) until the agent's
        # own staging probe + commit absorbs them.
        if not args.observe:
            # Always stage the candidate — including on the auto-advance paths below — so
            # a later `--commit <fp>` has the observed snapshot to promote; the
            # fingerprint binding is what protects against promoting it out of order.
            write_state(candidate_file(args.state_file), snapshot)
            # Invariant: the probe auto-advances the baseline whenever it observed NOTHING
            # actionable (reasons empty) and the observation moved (no baseline yet, or a
            # fingerprint drift). This is safe by construction — zero reasons means there
            # are no events to burn — and it closes two holes: (1) an idle first run
            # bootstraps the baseline so steady state can begin; (2) an idle follow-up
            # probe whose live state drifted to a reason-free fingerprint records that
            # drift, so a later return to a previously-committed fingerprint is not
            # suppressed forever by actionable_reasons' fingerprint-equality early return.
            # A WAKING probe never writes the state file here: it stages the candidate
            # only and advances the baseline exclusively via the fingerprint-bound
            # `--commit <fp>`, so a crash before that commit re-fires.
            if not reasons and (
                previous is None
                or previous.get("fingerprint") != snapshot["fingerprint"]
            ):
                write_state(args.state_file, snapshot)
                label = (
                    "bootstrap: wrote initial" if previous is None else "advanced idle"
                )
                print(
                    f"{label} cos-preflight baseline {snapshot['fingerprint'][:12]} "
                    f"to {args.state_file}",
                    file=sys.stderr,
                )
    except SystemExit as exc:
        message = exc.code if isinstance(exc.code, str) else ""
        if message:
            print(message, file=sys.stderr)
        return 2

    result = {
        "wake": bool(reasons),
        "reasons": reasons,
        "fingerprint": snapshot["fingerprint"],
        "state_file": str(args.state_file),
    }
    if reasons:
        print(json.dumps(result, indent=2))
        return WAKE_EXIT
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
