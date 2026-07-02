#!/usr/bin/env python3
"""Cheap deterministic wake gate for the registry chief-of-staff loop.

This script is the first tool call of a scheduled chief-of-staff agent session: it
checks the small set of repo/GitHub signals that can make a tick useful and, when the
state is unchanged, lets the session stop immediately without spending tokens.

Two modes:
  cos_preflight.py            probe (default): read the state file, take a fresh
                              snapshot, compare. NEVER writes the state file.
  cos_preflight.py --commit   write the current snapshot to the state file, print a
                              one-line confirmation. Does not compare or print reasons.

Probe contract:
  exit 0  idle; no agent work needed
  exit 10 wake; stdout JSON names the reasons to resume the COS thread
  exit 2  tool/setup error; stderr explains what failed

Commit contract:
  exit 0  snapshot written; stdout one-line confirmation
  exit 2  tool/setup error; stderr explains what failed

The session loop is: probe (exit 0 → stop / exit 10 → do the tick) → `--commit` → probe
again → if 10, keep working in the same tick. Committing only after a successful tick
gives at-least-once semantics: a failed tick leaves the event uncommitted, so the next
scheduled session re-observes it, instead of the old at-most-once behavior where writing
at detection burned the event even when the tick that followed failed.

It does not make coordination decisions, edit issues, merge PRs, or run the dev
preview. It only decides whether a real chief-of-staff tick is worth spending tokens on.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

WAKE_EXIT = 10
DEFAULT_CANONICAL = Path("/Users/adam/Code/registry-research-toolkit")
PASSING_CONCLUSIONS = {"SUCCESS", "SKIPPED", "NEUTRAL"}
MERGE_GATE_START_RE = re.compile(
    r"<!--\s*pr-pipeline-merge-gate\s*-->(.*?)(?:<!--\s*/pr-pipeline-merge-gate\s*-->|$)",
    re.IGNORECASE | re.DOTALL,
)
GATE_FIELD_RE = re.compile(r"^\s*[-*]?\s*(?P<key>status|head):\s*(?P<value>\S+)",
                           re.IGNORECASE | re.MULTILINE)  # fmt: skip
NO_STATUS_CHANGES = "projection delta:\nno status changes"
# plan_sequence.py --tick prints a deterministic `lanes: <verdict>` line on stderr and
# encodes the same verdict in its exit code (0 fresh / 1 re-rank / 2 re-stamp). An
# unhandled traceback also exits 1 (or a non-{0,1,2} code) but WITHOUT the sentinel, so
# we accept 1/2 as a signal only when its sentinel is present; otherwise it's a tool
# error. This stops a crash + its recovery from reading as two spurious wakes.
PLAN_TICK_SENTINELS = {1: "lanes: stale (re-rank)", 2: "lanes: stale (re-stamp"}

_PLAN_SPEC = importlib.util.spec_from_file_location(
    "plan_sequence", Path(__file__).with_name("plan_sequence.py")
)
assert _PLAN_SPEC and _PLAN_SPEC.loader
_plan_sequence = importlib.util.module_from_spec(_PLAN_SPEC)
sys.modules[_PLAN_SPEC.name] = _plan_sequence
_PLAN_SPEC.loader.exec_module(_plan_sequence)
DEFAULT_PR_FETCH_CAP = getattr(_plan_sequence, "FETCH_CAP", 5000)


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise SystemExit(
            f"missing executable for cos-preflight command {cmd[0]!r}: {exc}"
        ) from exc


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


def parse_merge_gate(body: str | None, head_oid: str) -> dict[str, str | bool | None]:
    match = MERGE_GATE_START_RE.search(body or "")
    if not match:
        return {
            "state": "absent",
            "status": None,
            "head": None,
            "current": False,
            "block_hash": None,
        }
    block_hash = hashlib.sha256(match.group(0).encode()).hexdigest()
    fields = {
        m.group("key").lower(): m.group("value")
        for m in GATE_FIELD_RE.finditer(match.group(1))
    }
    status = fields.get("status")
    head = fields.get("head")
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
        "block_hash": block_hash,
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


def summarize_pr(raw: dict[str, Any], current_repo: str) -> dict[str, Any] | None:
    body = raw.get("body") or ""
    closing = {ref["number"] for ref in raw.get("closingIssuesReferences") or []}
    closing.update(_plan_sequence.closing_issue_numbers_from_body(body, current_repo))
    gate = parse_merge_gate(body, raw["headRefOid"])

    if not closing and gate["state"] == "absent":
        # An unclaimed PR (no closing refs, no gate block): the chief still needs to know
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
        # Only the CONFLICTING verdict is stable; GitHub's transient UNKNOWN↔MERGEABLE
        # flapping would otherwise wake the chief on every recompute.
        "conflicting": raw.get("mergeable") == "CONFLICTING",
        # Overall checks verdict only — the full per-check-run list would wake on every
        # individual check transition within one CI run.
        "checks": checks_verdict(raw.get("statusCheckRollup") or []),
        "gate": gate,
        "codex_signal": signal,
    }
    # A new review on an in-flight issue-closing PR is the chief's "send unblock
    # follow-up" trigger even before the gate is current-ready, so surface it here.
    if closing:
        summary["reviews"] = normalize_reviews(raw.get("latestReviews") or [])
    return summary


def fetch_pr_summaries(limit: int, current_repo: str) -> list[dict[str, Any]]:
    prs = gh_json(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            str(limit),
            "--json",
            "number,title,body,closingIssuesReferences,baseRefName,isDraft,mergeable,headRefOid,statusCheckRollup,latestReviews",
        ]
    )
    if len(prs) >= limit:
        raise SystemExit(
            f"open PR fetch hit --pr-limit={limit}; increase the cap or paginate before "
            "using cos-preflight for idle gating"
        )
    summaries = [summarize_pr(pr, current_repo) for pr in prs]
    return sorted((s for s in summaries if s is not None), key=lambda s: s["number"])
    # summarize_pr never returns None now (unclaimed PRs get a minimal entry), but the
    # guard is cheap insurance if that changes.


def snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    stable = {
        "local_head": snapshot["local_head"],
        "remote_main": snapshot["remote_main"],
        "plan_tick": snapshot["plan_tick"],
        "prs": snapshot["prs"],
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

    def _named(pr: dict[str, Any], reason: str, bucket: list[str]) -> None:
        if pr["number"] in changed:
            bucket.append(reason)

    ready: list[str] = []
    draft_ready: list[str] = []
    stale: list[str] = []
    for pr in snapshot["prs"]:
        gate = pr.get("gate", {})
        num = f"#{pr['number']}"
        if gate.get("state") == "current-ready" and not pr["draft"]:
            _named(pr, num, ready)
        elif gate.get("state") == "current-ready" and pr["draft"]:
            _named(pr, num, draft_ready)
        elif gate.get("state") == "stale-ready":
            _named(pr, num, stale)
    if ready:
        reasons.append(f"ready merge-gate PR changed: {', '.join(ready)}")
    if draft_ready:
        reasons.append(f"draft PR has ready merge-gate block: {', '.join(draft_ready)}")
    if stale:
        reasons.append(f"stale merge-gate PR changed: {', '.join(stale)}")

    if previous:
        if previous.get("remote_main") != snapshot["remote_main"]:
            reasons.append("origin/main changed")
        if changed:
            reasons.append("open PR state changed")

    return sorted(dict.fromkeys(reasons))


def default_state_file() -> Path:
    return Path(".git") / "cos-preflight-state.json"


def load_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        # Self-heal: a truncated/corrupt state file is treated as first-run (warn, then
        # probe as if no prior snapshot). The cost is one extra wake; the alternative —
        # hard-stopping every run until someone hand-deletes the file — silently disables
        # the whole gate.
        print(
            f"warning: ignoring corrupt cos-preflight state file {path} ({exc}); "
            "treating as first run",
            file=sys.stderr,
        )
        return None


def write_state(path: Path, snapshot: dict[str, Any]) -> None:
    parent = path.parent
    if not parent.exists():
        # Never mkdir(parents=True) here: with --no-canonical-check and a missing .git,
        # the default state path (.git/cos-preflight-state.json) would silently create a
        # stray .git/ directory. Fail clearly instead.
        raise SystemExit(
            f"cos-preflight state directory {parent} does not exist; "
            "run from the canonical checkout or pass an existing --state-file dir"
        )
    with NamedTemporaryFile(
        "w",
        dir=path.parent,
        encoding="utf-8",
        prefix=f"{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as fh:
        tmp = Path(fh.name)
        fh.write(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    try:
        tmp.replace(path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def collect_snapshot(epic: int, pr_limit: int) -> dict[str, Any]:
    current_repo = repo_name_with_owner()
    snapshot = {
        "version": 1,
        "observed_at": datetime.now(UTC).isoformat(),
        "local_head": local_head_sha(),
        "remote_main": remote_main_sha(),
        "plan_tick": run_plan_tick(epic),
        "prs": fetch_pr_summaries(pr_limit, current_repo),
    }
    snapshot["fingerprint"] = snapshot_fingerprint(snapshot)
    return snapshot


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--epic", type=int, default=328)
    ap.add_argument("--pr-limit", type=int, default=DEFAULT_PR_FETCH_CAP)
    ap.add_argument("--state-file", type=Path, default=default_state_file())
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
        action="store_true",
        help="write the current snapshot to the state file and exit (no comparison); "
        "run after a successful tick so the observed events are marked handled",
    )
    args = ap.parse_args(argv)

    canonical = None if args.no_canonical_check else args.canonical
    try:
        require_canonical(canonical)
        if args.commit:
            snapshot = collect_snapshot(args.epic, args.pr_limit)
            write_state(args.state_file, snapshot)
            print(
                f"committed cos-preflight snapshot {snapshot['fingerprint'][:12]} "
                f"to {args.state_file}"
            )
            return 0
        # Probe: read prior state, snapshot fresh, compare. Never writes.
        previous = load_state(args.state_file)
        snapshot = collect_snapshot(args.epic, args.pr_limit)
        reasons = actionable_reasons(snapshot, previous)
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
