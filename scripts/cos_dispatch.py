#!/usr/bin/env python3
"""Deterministic pr-pipeline launcher for the chief-of-staff `auto` dispatch mode.

On a `dispatch:` wake (cos_watch.py emits it once a freed slot leaves the pipeline
ledger below --max-slots), the chief-of-staff in `auto` mode runs THIS script once per
lane it decides to dispatch, instead of only recommending commands. The script is the
side-effecting half of that decision: it claims a slot, materializes a worktree, and
launches a detached pr-pipeline agent (codex or claude). The default `--issues` mode
starts a fresh lane from `origin/main`; `--continue-pr` resumes an existing same-repo PR
branch in a fresh agent after rebasing it onto the PR base branch by default.

It is deliberately dumb about WHICH lane to run — the agent (or /plan-lanes) picks the
issues; this script just performs the launch atomically and records the claim. Order of
operations is chosen so a failure never leaks a slot: the slot file is written only after
the agent process is spawned, and it is written promptly so the unrecorded window is
tiny. A failure after the worktree is created leaks only the worktree, which the error
names for adjudication.

Steps (fail-fast between each). After arg/canonical validation (require_canonical,
target resolution, resolve_profile, slug validation — all exit 2):
  1. kill switch  — <state-root>/auto-dispatch.off present ⇒ refuse (exit 3).
  2. budget       — busy slots >= --max-slots ⇒ refuse (exit 4).
  3. collision    — fresh mode refuses slot OR worktree collision; continue mode also
                    refuses any live slot already claiming the PR.
  3b. author check — each --issues number, and the --continue-pr PR number, must be
                    maintainer-authored (gh_issue.is_maintainer_authored); refuse
                    (exit 2) before any side effect. Skipped under --dry-run (its
                    no-network contract).
  4. worktree     — fresh mode fetches origin/main and `git worktree add -b wt/<slug> …
                    origin/main`. Continue mode fetches origin/main plus the PR head and
                    base branches, creates or reuses a clean worktree on the PR branch,
                    and rebases onto the PR base unless --no-rebase is set.
  5. launch       — append a `cos.run.started` JSON sentinel to the per-slug dispatch
                    log, then spawn the agent DETACHED.
                    The codex argv runs `-s workspace-write` with TWO `--add-dir` grants —
                    <state-root> AND <canonical>/.git, the latter because the linked
                    worktree's writable git state (index/HEAD/refs/objects) lives under the
                    canonical checkout's git dir, outside the sandboxed cwd (#1050); see
                    build_launch_argv. The child env carries the
                    --state-root override as XDG_STATE_HOME so its ledger/gate writes land
                    under the same root). We do not wait; the process re-parents when this
                    session exits. Then a short HEALTH CHECK: wait a grace window and poll —
                    Popen succeeding only means exec() worked, so ANY completed exit inside
                    the window (a rejected flag, missing auth, bad config) is a launch
                    failure. A launch failure here (spawn OSError, dispatch-log setup OSError,
                    or an instant child exit) → exit 2 naming the leaked worktree; NO slot
                    file is written. The sentinel offset scopes the later codex-id poll
                    and cos_tail --from-run-start to THIS run's log bytes, including
                    plain claude-surface lanes.
  6. slot file    — write of <state-root>/pipeline-slots/<slug>.json IMMEDIATELY after a
                    healthy launch (session=null for codex, the pre-generated uuid for
                    claude). An OVERLAY write: if the detached child reached its
                    register-slot step first, we overlay only dispatcher-owned fields
                    and preserve the child's fresher issue/PR claim; continue mode owns
                    `prs:[<pr>]` because the PR already exists. The `slot` field MUST
                    equal the filename stem or scan_slots treats it as absent. A write
                    failure here (agent already running) becomes exit 2 naming the
                    orphan, never a traceback.
  7. session id   — codex only: poll the JSONL log (bytes past the step-5 offset) for the
                    first event carrying a thread/session id, bounded; on timeout,
                    session=null (the chief's fuzzy-thread-search fallback covers it) plus
                    a stderr warning. Then RE-READ the slot file and merge-update ONLY the
                    `session` field, preserving any issues/prs a fast child pipeline wrote
                    during the poll window (its claim is fresher); a vanished/invalid file
                    falls back to a full rewrite. A merge-update failure after the step-6
                    write is a stderr warning but still exit 0 — the slot exists, only the
                    session enrichment failed. claude's session is already final at step 6.

Exit codes: 0 launched (or --dry-run OK); 2 usage/collision/tool error; 3 kill switch;
4 no free slot budget.

Launch profiles (`--tier`, default `hard`): each tier is ONE blessed launch profile —
a surface plus the model/effort/advisor pins that are validated to work together, kept
together in LAUNCH_PROFILES so the set can't drift apart:

  hard (default): codex + `-m gpt-5.5 -c model_reasoning_effort=xhigh`. The default
                  pipeline for everything non-trivial.
  easy:           claude + `--model claude-sonnet-5 --effort high --advisor opus`. A
                  cheaper Sonnet-5 main that escalates hard decisions to an Opus advisor.

Rationale: small, straightforward lanes go to the cheaper Sonnet-5 pipeline (Opus advisor
for the hard calls); everything else defaults to the Codex gpt-5.5 xhigh pipeline. The
easy/hard CHOICE is the chief-of-staff's judgment at dispatch time — this script only
encodes the profiles. The model+advisor stay paired in the profile because `claude` exits
with an error on a rejected main/advisor pairing (Sonnet-5 main + Opus advisor is the
accepted combo; requires Claude Code >= 2.1.98).

`--surface` is an explicit override. When it CONTRADICTS the tier's implied surface, the
launch runs on that surface with its AMBIENT defaults — NO model/effort/advisor pins, so
we never invent an unblessed model combo. When `--surface` merely restates the tier's own
surface (or is omitted), the tier's profile applies in full.

Stores match the sibling cos_* scripts: --state-root defaults to the parent of
cos_preflight.default_gate_root() ($XDG_STATE_HOME/registry-research-toolkit), so the
pipeline-slots ledger, merge-gates, and dispatch logs all sit together and a --state-root
override moves them as one. Like cos_preflight, the script must run from the canonical
main checkout (reuse require_canonical, with the same --canonical/--no-canonical-check
escape for tests).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

DEFAULT_TIER = "hard"
RUN_STARTED_TYPE = "cos.run.started"
MAIN_FETCH_REFSPEC = "+refs/heads/main:refs/remotes/origin/main"
_CLOSING_KEYWORD_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)"
    r"\s+#(?P<number>\d+)\b",
    re.IGNORECASE | re.MULTILINE,
)

# The blessed launch profiles, kept in ONE place so surface + model/effort/advisor stay
# together as a validated set (never composed ad hoc at a call site). Each value is
# (surface, extra_flags): `surface` is the tier's implied dispatch surface; the flags are
# the EXTRA launch flags layered on top of that surface's pinned base flags (see
# build_launch_argv). The flags are validated to work together — notably the claude
# main/advisor pairing, which `claude` rejects with an error if unblessed. An explicit
# --surface that contradicts the tier surface drops the flags (see resolve_profile),
# because the pins are only valid for their own surface.
LAUNCH_PROFILES: dict[str, tuple[str, list[str]]] = {
    "hard": ("codex", ["-m", "gpt-5.5", "-c", "model_reasoning_effort=xhigh"]),
    "easy": (
        "claude",
        ["--model", "claude-sonnet-5", "--effort", "high", "--advisor", "opus"],
    ),
}

# Bounded poll for the codex thread id in its JSONL log. 30s ceiling; a null session is
# a tolerated outcome (the chief falls back to fuzzy thread search), so this never blocks
# the dispatch. Injectable via dispatch() args so the timeout test doesn't sleep 30s.
DEFAULT_CODEX_ID_TIMEOUT = 30.0
DEFAULT_CODEX_ID_POLL = 0.25

# Post-spawn health check. Popen returning a pid says nothing about whether the child
# survived: a CLI that rejects a pinned flag, a missing auth token, or a bad config makes
# the child exit within the first moments — yet we'd still write a slot and exit 0, and the
# dead slot would squat on budget until the 24h stale path reclaims it. So after spawn we
# wait a short grace window and poll: ANY completed exit inside it (zero OR nonzero — a real
# pipeline runs for minutes, so an instant exit is failure either way) is a launch failure,
# no slot written. Injectable via dispatch() args so tests shrink the grace to ~milliseconds
# and don't slow the suite; the healthy-path stubs sleep just past it to survive the check.
DEFAULT_LAUNCH_GRACE = 1.0
DEFAULT_LAUNCH_GRACE_POLL = 0.1


# Git-context env vars that override cwd-based repo discovery. A git hook (pre-push runs
# our test suite) exports GIT_DIR / GIT_WORK_TREE / GIT_INDEX_FILE etc. into the child
# environment; git then targets the HOOK's repo regardless of a subprocess's cwd or `-C`.
# So passing an explicit cwd is NOT enough — every git call (and the launched agent, which
# runs git internally) must run with these scrubbed. We drop all GIT_* keys wholesale:
# none of git's config-affecting env vars belong in a fresh dispatch, and a blanket rule
# can't miss a newly added repo-targeting var. (GIT_SSH/GIT_ASKPASS auth helpers live in
# the user's shell config, not this exported hook set, so dropping them here is harmless.)
def _scrubbed_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def resolve_profile(tier: str, surface_override: str | None) -> tuple[str, list[str]]:
    """Resolve (surface, extra_flags) from the tier and an optional --surface override.

    - No override, or an override that restates the tier's own surface → the tier's full
      profile (its surface + its validated model/effort/advisor flags).
    - An override that CONTRADICTS the tier's surface → that surface with AMBIENT defaults
      (empty flags): the tier's pins are only valid for the tier's own surface, and we do
      not synthesize an unblessed model combo for the other one.
    """
    tier_surface, flags = LAUNCH_PROFILES[tier]
    if surface_override is None or surface_override == tier_surface:
        return tier_surface, list(flags)
    return surface_override, []


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


# _gh's sys.modules-guarded loader gives single process-wide instances of the siblings:
# cos_preflight for its store-root/slot leaf helpers (default_gate_root, default_slots_root,
# scan_slots, DEFAULT_MAX_SLOTS, require_canonical), and gh_issue for the dispatch
# chokepoint's public author check (is_maintainer_authored). Both are shared, not private
# copies, so a monkeypatch of cd._gh_issue reaches this consumer.
_gh = _load_gh()
_cos_preflight = _gh.load_sibling("cos_preflight")
_gh_issue = _gh.load_sibling("gh_issue")


def require_maintainer_authored(issues: list[int]) -> None:
    """Refuse (SystemExit) unless every issue is maintainer-authored — defense in depth.

    The dispatch path launches a permission-bypassed pr-pipeline on `--issues`; those
    issues' text becomes the lane's "spec". This repo is public, so a stranger's issue is
    untrusted — gating authorship here at the chokepoint (in addition to the pipeline's own
    trust-gate reads) means a non-maintainer or missing issue never reaches a launch. Reuses
    gh_issue's public author check rather than re-deriving it. Read-only network I/O (`gh
    issue view` per issue), run before any side effect.

    A maintainer-authored PR number passes too: the trust boundary is authorship, not
    issue-ness (`gh issue view` resolves a PR number — see gh_issue.is_maintainer_authored).
    """
    for number in issues:
        if not _gh_issue.is_maintainer_authored(number):
            raise SystemExit(
                f"issue #{number} is not maintainer-authored "
                f"(author != {_gh_issue.maintainer_login()}); "
                "refusing to dispatch a pipeline on untrusted issue content"
            )


def default_state_root() -> Path:
    # Parent of the merge-gate root: $XDG_STATE_HOME/registry-research-toolkit. The slot
    # ledger, gate store, and dispatch logs all live under it, so a --state-root override
    # keeps them together (mirrors cos_watch's slots-dir = gate-dir.parent default).
    return _cos_preflight.default_gate_root().parent


def parse_issues(raw: str) -> list[int]:
    """Comma-separated issue numbers → sorted-input-order list of ints (fail-fast)."""
    issues: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            issues.append(int(token))
        except ValueError:
            raise SystemExit(f"invalid issue number {token!r} in --issues {raw!r}")
    if not issues:
        raise SystemExit("--issues must name at least one issue number")
    return issues


def default_slug(surface: str, issues: list[int]) -> str:
    return f"auto-{surface}-issue-{issues[0]}"


def default_continue_slug(surface: str, pr: int) -> str:
    return f"continue-{surface}-pr-{pr}"


def validate_slug(slug: str) -> None:
    # The slot filename is <slug>.json and scan_slots requires slot==stem, so the slug
    # must be a clean single-path-component stem — no separators, no traversal.
    if not slug or "/" in slug or slug in {".", ".."} or slug != Path(slug).name:
        raise SystemExit(f"invalid --slug {slug!r}: must be a valid filename stem")


def _closing_issue_numbers(body: str, refs: list[dict] | None) -> list[int]:
    """Closing issue refs from strict body keywords plus GitHub's complete refs."""
    seen: set[int] = set()
    issues: list[int] = []
    for match in _CLOSING_KEYWORD_RE.finditer(body):
        number = int(match.group("number"))
        if number not in seen:
            seen.add(number)
            issues.append(number)
    if refs:
        for ref in refs:
            number = ref.get("number") if isinstance(ref, dict) else None
            if isinstance(number, int) and number not in seen:
                seen.add(number)
                issues.append(number)
    return issues


def resolve_continue_pr(pr: int) -> dict:
    """Resolve the same-repo PR branch and closing issues for --continue-pr.

    PR body text is parsed only as data to discover closing keywords; it never directs
    tool use. Fork PRs are refused because the launched pipeline must push back to the
    repository-owned head branch.
    """
    proc = subprocess.run(
        [
            "gh",
            "pr",
            "view",
            str(pr),
            "--json",
            "number,title,body,baseRefName,headRefName,isCrossRepository,closingIssuesReferences",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"could not resolve PR #{pr}: {proc.stderr.strip() or proc.stdout.strip()}"
        )
    try:
        data = json.loads(proc.stdout)
    except ValueError as exc:
        raise SystemExit(f"could not parse PR #{pr} metadata from gh") from exc
    if data.get("isCrossRepository"):
        raise SystemExit(
            f"refusing to continue fork PR #{pr}; head branch is not local"
        )
    branch = data.get("headRefName")
    if not isinstance(branch, str) or not branch:
        raise SystemExit(f"PR #{pr} has no resolvable head branch")
    base_branch = data.get("baseRefName")
    if not isinstance(base_branch, str) or not base_branch:
        raise SystemExit(f"PR #{pr} has no resolvable base branch")
    issues = _closing_issue_numbers(
        str(data.get("body") or ""), data.get("closingIssuesReferences")
    )
    if not issues:
        raise SystemExit(
            f"PR #{pr} has no closing issue references; refusing to infer lane scope"
        )
    return {
        "pr": pr,
        "branch": branch,
        "base_branch": base_branch,
        "issues": issues,
        "title": str(data.get("title") or ""),
    }


def live_slot_for_pr(slots_root: Path, pr: int) -> str | None:
    """Return the live slot slug already claiming PR `pr`, if any."""
    for slug in sorted(_cos_preflight.scan_slots(slots_root)):
        loaded = _cos_preflight._read_json_tolerant(
            slots_root / f"{slug}.json", "pipeline-slot file"
        )
        if loaded is None:
            continue
        data = loaded[1]
        prs = data.get("prs")
        if isinstance(prs, list) and pr in prs:
            return slug
    return None


def read_brief(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(f"could not read --brief-file {path}: {exc}") from exc


def continuation_prompt(
    surface: str, pr: int, issues: list[int], branch: str, base_branch: str, brief: str
) -> str:
    prefix = "$pr-pipeline" if surface == "codex" else "/pr-pipeline"
    issue_text = ", ".join(f"#{issue}" for issue in issues) or "none"
    parts = [
        f"{prefix} continue PR #{pr}",
        (
            f"Continue PR #{pr} on branch `{branch}` for issue(s) {issue_text} in "
            "this worktree. Do NOT restart the work, do NOT open a new PR, and do "
            "NOT change the closing keywords except to keep them accurate."
        ),
        (
            "The branch has been checked out from the PR head and rebased onto "
            f"`origin/{base_branch}` unless this dispatch used `--no-rebase`. Inspect the "
            "current diff, fix the requested follow-up, run the relevant gates, "
            "push this same branch, and refresh the existing PR's merge-gate handoff."
        ),
    ]
    if brief:
        parts.append(f"Continuation brief:\n{brief}")
    return "\n\n".join(parts)


def build_launch_argv(
    surface: str,
    worktree: Path,
    issues: list[int],
    state_root: Path,
    session_id: str | None,
    profile_flags: list[str],
    canonical: Path,
    *,
    prompt: str | None = None,
) -> list[str]:
    """The exact detached launch argv for the chosen surface + tier profile flags.

    The pr-pipeline prompt is ONE string argument — `$pr-pipeline 1011 1012` (codex) /
    `/pr-pipeline 1011 1012` (claude) — with issues space-separated. `profile_flags` are
    the tier's validated model/effort/advisor pins (empty when a --surface override
    contradicts the tier), inserted before the prompt but after the surface's own pinned
    base flags.

    codex runs `-s workspace-write`, whose writable set is {the `-C` cwd (the linked
    worktree), each `--add-dir`}. Two grants are needed, not one:
      - <state_root> so the child's ledger/gate writes land under the dispatch state root.
      - <canonical>/.git — the worktree is a LINKED worktree, so its `.git` is a FILE
        pointing at `<canonical>/.git/worktrees/<slug>`, and every writable git object
        (index, HEAD, refs, logs, packed-refs) lives under the canonical checkout's git
        dir, OUTSIDE the worktree cwd. Without this grant every ref/index/object write the
        pipeline makes is denied by the sandbox (#1050). `--add-dir` is repeatable
        (codex 0.142.5). claude is unsandboxed and needs no equivalent.
    """
    issues_arg = " ".join(str(n) for n in issues)
    if surface == "codex":
        prompt = prompt or f"$pr-pipeline {issues_arg}"
        return [
            "codex",
            "exec",
            "-C",
            str(worktree),
            "-s",
            "workspace-write",
            "-c",
            "approval_policy=never",
            "--add-dir",
            str(state_root),
            "--add-dir",
            str(canonical / ".git"),  # linked worktree's writable git state lives here
            "--json",
            *profile_flags,
            prompt,
        ]
    # claude: the session id is pre-generated so we know it before launch.
    assert session_id is not None
    prompt = prompt or f"/pr-pipeline {issues_arg}"
    return [
        "claude",
        "--session-id",
        session_id,
        *profile_flags,
        "-p",
        prompt,
        "--dangerously-skip-permissions",
    ]


# Observed codex 0.142.5 JSONL (from `codex exec --json --ephemeral -s read-only`):
#   {"type":"thread.started","thread_id":"019f2334-4455-70a1-bc1b-2e86d5ecfccf"}
#   {"type":"turn.started"}
#   {"type":"item.completed","item":{...}}
#   {"type":"turn.completed","usage":{...}}
# The session/thread id is `thread_id` on the FIRST event. We take the id from the first
# event that carries one (thread_id preferred, session_id/id tolerated) and ignore
# unknown event types, so a future rename of the leading event can't strand the parser.
_ID_KEYS = ("thread_id", "session_id", "id")


def _extract_session_id(line: str) -> str | None:
    try:
        event = json.loads(line)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    for key in _ID_KEYS:
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def append_run_sentinel(
    log_path: Path,
    *,
    slug: str,
    issues: list[int],
    prs: list[int],
    surface: str,
    tier: str,
    mode: str,
) -> int:
    """Append the explicit per-run boundary marker and return its byte offset."""
    event = {
        "type": RUN_STARTED_TYPE,
        "slug": slug,
        "issues": issues,
        "prs": prs,
        "surface": surface,
        "tier": tier,
        "mode": mode,
        "dispatched": datetime.now(UTC).isoformat(),
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as fh:
            offset = fh.tell()
            fh.write(json.dumps(event, separators=(",", ":")) + "\n")
    except OSError as exc:
        raise SystemExit(f"failed to write run sentinel {log_path}: {exc}") from exc
    return offset


def poll_codex_session_id(
    log_path: Path, timeout: float, poll_interval: float, start_offset: int = 0
) -> str | None:
    """Tail the JSONL dispatch log for the codex thread id, bounded by timeout.

    Only bytes at or after `start_offset` (the log size captured just before launch) are
    parsed, so a reused per-slug log from a prior dispatch can't yield a stale thread id.

    Returns the id, or None if none appeared within `timeout` (the caller warns and
    records session=null; the chief's fuzzy thread search covers a null session).
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(start_offset)
                text = fh.read()
        except FileNotFoundError:
            text = ""
        for line in text.splitlines():
            session_id = _extract_session_id(line)
            if session_id is not None:
                return session_id
        if time.monotonic() >= deadline:
            return None
        time.sleep(poll_interval)


def run_git(cwd: Path, args: list[str]) -> None:
    """Run a git command in the canonical checkout, failing fast (exit 2) with stderr.

    cwd is passed explicitly (not relied on as the ambient process cwd) so the fetch and
    worktree-add always act on the canonical repo — never on whatever directory the
    caller happens to be in, which under --no-canonical-check would corrupt an unrelated
    checkout.
    """
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        env=_scrubbed_env(),  # cwd is not enough: scrub inherited GIT_DIR/GIT_WORK_TREE
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        tail = (
            proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed"
        )
        raise SystemExit(f"git {' '.join(args)} failed: {tail}")


def git_output(cwd: Path, args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        env=_scrubbed_env(),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        tail = (
            proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed"
        )
        raise SystemExit(f"git {' '.join(args)} failed: {tail}")
    return proc.stdout.strip()


def branch_exists(canonical: Path, branch: str) -> bool:
    proc = subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=str(canonical),
        env=_scrubbed_env(),
    )
    return proc.returncode == 0


def ensure_clean_worktree(worktree: Path) -> None:
    dirty = git_output(worktree, ["status", "--short"])
    if dirty:
        raise SystemExit(f"worktree {worktree} has local changes; refusing to continue")


def ensure_existing_continue_worktree(worktree: Path, branch: str) -> None:
    toplevel = git_output(worktree, ["rev-parse", "--show-toplevel"])
    if Path(toplevel).resolve() != worktree.resolve():
        raise SystemExit(
            f"existing worktree {worktree} resolves to {toplevel}; refusing to continue"
        )
    current = git_output(worktree, ["branch", "--show-current"])
    if current != branch:
        raise SystemExit(
            f"existing worktree {worktree} is on branch {current!r}, not PR branch "
            f"{branch!r}; refusing to continue"
        )
    ensure_clean_worktree(worktree)


def rebase_onto(worktree: Path, base_ref: str) -> None:
    proc = subprocess.run(
        ["git", "rebase", base_ref],
        cwd=str(worktree),
        env=_scrubbed_env(),
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return
    subprocess.run(
        ["git", "rebase", "--abort"],
        cwd=str(worktree),
        env=_scrubbed_env(),
        capture_output=True,
        text=True,
    )
    tail = proc.stderr.strip() or proc.stdout.strip() or f"git rebase {base_ref} failed"
    raise SystemExit(f"git rebase {base_ref} failed and was aborted: {tail}")


def prepare_continue_worktree(
    canonical: Path,
    worktree: Path,
    branch: str,
    base_branch: str,
    *,
    rebase: bool,
) -> None:
    run_git(canonical, ["fetch", "origin", MAIN_FETCH_REFSPEC])
    run_git(
        canonical,
        ["fetch", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}"],
    )
    if base_branch != "main":
        run_git(
            canonical,
            [
                "fetch",
                "origin",
                f"+refs/heads/{base_branch}:refs/remotes/origin/{base_branch}",
            ],
        )
    if worktree.exists():
        ensure_existing_continue_worktree(worktree, branch)
    elif branch_exists(canonical, branch):
        run_git(canonical, ["worktree", "add", "--force", str(worktree), branch])
    else:
        run_git(
            canonical,
            [
                "worktree",
                "add",
                "-b",
                branch,
                str(worktree),
                f"refs/remotes/origin/{branch}",
            ],
        )
    ensure_clean_worktree(worktree)
    run_git(worktree, ["merge", "--ff-only", f"refs/remotes/origin/{branch}"])
    remote_tip = git_output(worktree, ["rev-parse", f"refs/remotes/origin/{branch}"])
    head = git_output(worktree, ["rev-parse", "HEAD"])
    if head != remote_tip:
        raise SystemExit(
            f"worktree {worktree} is not at origin/{branch}; refusing to continue "
            "without manual reconciliation"
        )
    if rebase:
        rebase_onto(worktree, f"refs/remotes/origin/{base_branch}")
        ensure_clean_worktree(worktree)


def require_git_checkout(canonical: Path) -> None:
    """Refuse (exit 2) unless `canonical` is itself a git worktree root.

    Belt-and-braces before the mutating fetch/worktree-add: git's `-C <dir>` walks UP to
    the nearest enclosing repo, so a `canonical` that is NOT a checkout (an empty dir, a
    stray path) would silently target whatever repo sits above it — the exact class of
    accident that let a test's throwaway path resolve to the real checkout. Requiring the
    resolved `show-toplevel` to EQUAL `canonical` (true for a main checkout and for a
    linked worktree root, both valid dispatch origins) rejects that: an ambient parent
    repo has a different toplevel, and a non-repo dir makes rev-parse fail outright. This
    does not weaken the production contract — the real canonical checkout satisfies it.
    """
    proc = subprocess.run(
        ["git", "-C", str(canonical), "rev-parse", "--show-toplevel"],
        env=_scrubbed_env(),  # else an inherited GIT_DIR resolves the hook's repo, not canonical
        capture_output=True,
        text=True,
    )
    toplevel = proc.stdout.strip()
    if proc.returncode != 0 or not toplevel:
        raise SystemExit(
            f"canonical checkout {canonical} is not a git worktree "
            f"({proc.stderr.strip() or 'rev-parse failed'})"
        )
    if Path(toplevel).resolve() != canonical.resolve():
        raise SystemExit(
            f"canonical checkout {canonical} is not a worktree root; git resolved it to "
            f"the enclosing repo {toplevel} — refusing to operate on an unintended repo"
        )


# The slot payload is split across two owners: the DISPATCHER owns the ownership fields
# below; the launched child pipeline owns the rest (`issues`/`prs` and anything it adds).
# Both the initial ownership write and the later session enrichment must overlay only their
# own fields onto whatever the child may already have written, never clobbering the child's
# fresher claim — so they share one overlay leaf (_write_or_overlay_slot) and can't drift.
_OWNERSHIP_FIELDS = ("slot", "surface", "tier", "session", "pid", "dispatched")


def _full_slot_payload(
    slug: str,
    issues: list[int],
    surface: str,
    tier: str,
    session_id: str | None,
    pid: int,
    *,
    prs: list[int] | None = None,
    mode: str | None = None,
) -> dict:
    # `slot` MUST equal the filename stem or scan_slots drops the entry. `prs: []` is the
    # dispatcher's empty starting point; the child (pr-pipeline SKILL.md) fills it in — the
    # overlay path below never lets this empty list clobber a fresher child claim.
    payload = {
        "slot": slug,
        "issues": issues,
        "prs": [] if prs is None else prs,
        "surface": surface,
        "tier": tier,
        "session": session_id,
        "pid": pid,
        "dispatched": datetime.now(UTC).isoformat(),
    }
    if mode is not None:
        payload["mode"] = mode
    return payload


def _write_or_overlay_slot(
    slot_path: Path, payload: dict, overlay_fields: tuple[str, ...]
) -> None:
    """Write the slot, overlaying only `overlay_fields` when a valid slot already exists.

    Tolerantly reads the slot path; if a valid dict is already there (a fast child pipeline
    reached its register-slot step first, or an earlier dispatcher write landed), overlay
    ONLY `overlay_fields` from `payload` and preserve everything else — the child's
    `issues`/`prs` are fresher than ours. If the file is absent/unreadable/invalid (never
    written, torn, someone deleted it), write the full `payload` — the slot must exist for
    the launched agent.

    The torn-write guarantee (temp file + rename, so scan_slots never sees a partial read)
    is the shared cos_preflight._atomic_write_json leaf. DELIBERATE divergence from
    write_state's pre-step: we mkdir(parents=True) the ledger dir because the pipeline-slots
    root always lives under $XDG_STATE_HOME (never a .git-relative path), so there is no risk
    of conjuring a dir inside a missing checkout — unlike write_state, whose no-mkdir policy
    guards that exact footgun. Only the parent-dir policy diverges; the write+rename tail is
    shared.
    """
    slot_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = json.loads(slot_path.read_text(encoding="utf-8"))
        if not isinstance(existing, dict):
            raise ValueError("slot file is not a JSON object")
    except OSError, ValueError:
        # Absent / torn / invalid: write the full payload from our known-good values.
        _cos_preflight._atomic_write_json(slot_path, payload)
        return
    for field in overlay_fields:
        existing[field] = payload[field]
    _cos_preflight._atomic_write_json(slot_path, existing)


def write_slot_file(
    slot_path: Path,
    slug: str,
    issues: list[int],
    surface: str,
    tier: str,
    session_id: str | None,
    pid: int,
    *,
    prs: list[int] | None = None,
    mode: str | None = None,
) -> None:
    """The dispatcher's initial ownership write, immediately after a successful launch.

    Overlays only the ownership fields onto any pre-existing slot: if the detached child
    reached its register-slot step BEFORE this write, an unconditional full write would
    replace the child's fresher record with our empty `prs`. So we overlay our ownership
    fields and preserve the child's `issues`/`prs` (the same discipline as the session
    merge); if no valid slot exists yet we write the full payload. See _write_or_overlay_slot.
    """
    payload = _full_slot_payload(
        slug, issues, surface, tier, session_id, pid, prs=prs, mode=mode
    )
    overlay_fields = _OWNERSHIP_FIELDS
    if mode is not None:
        overlay_fields += ("mode",)
    if mode == "continue":
        overlay_fields += ("prs",)
    _write_or_overlay_slot(slot_path, payload, overlay_fields)


def merge_session_into_slot(
    slot_path: Path,
    slug: str,
    issues: list[int],
    surface: str,
    tier: str,
    session_id: str | None,
    pid: int,
    *,
    prs: list[int] | None = None,
    mode: str | None = None,
) -> None:
    """Enrich the codex slot with its polled session id WITHOUT clobbering the child claim.

    Between the initial ownership write (write_slot_file) and the codex session id
    resolving (up to the poll ceiling), a fast child pipeline may already have registered
    drafts/PRs into the SAME slot file (see pr-pipeline SKILL.md). Its claim is fresher, so
    we overlay ONLY the `session` field, preserving whatever `issues`/`prs`/other fields it
    now carries. If the file vanished or is unreadable/invalid (torn write, someone deleted
    it), we fall back to rewriting the full ownership payload from our own known-good values
    — the slot must exist for the launched agent.

    Written atomically (temp + rename) via the shared leaf, same as the initial write.
    """
    payload = _full_slot_payload(
        slug, issues, surface, tier, session_id, pid, prs=prs, mode=mode
    )
    _write_or_overlay_slot(slot_path, payload, ("session",))


def _child_env(state_root: Path) -> dict[str, str]:
    """The launched child's env: GIT_*-scrubbed, with XDG_STATE_HOME set to the override.

    The agent runs git in its own worktree, and an inherited GIT_DIR from a dispatching hook
    would point every git call it makes at the wrong repo — so we scrub all GIT_* (see
    _scrubbed_env). The child also derives its OWN pipeline-slots / merge-gate stores from
    XDG_STATE_HOME; if the operator passed a non-default --state-root, that override is
    invisible to the child unless we propagate it, so the child would split its ledger/gate
    writes into the ambient store. We set XDG_STATE_HOME to <state_root>.parent ONLY when
    state_root has the standard `.../registry-research-toolkit` shape (so the child re-derives
    exactly this override under $XDG_STATE_HOME/registry-research-toolkit); for a non-standard
    root we can't reconstruct a matching XDG_STATE_HOME, so we leave it ambient and warn.
    """
    env = _scrubbed_env()
    if state_root.name == "registry-research-toolkit":
        env["XDG_STATE_HOME"] = str(state_root.parent)
    else:
        print(
            f"warning: --state-root {state_root} is not a "
            "'.../registry-research-toolkit' path; the launched child will use the AMBIENT "
            "state stores (its ledger/gate writes may not land under this root)",
            file=sys.stderr,
        )
    return env


def launch_detached(
    argv: list[str],
    worktree: Path,
    log_path: Path,
    state_root: Path,
    *,
    grace: float = DEFAULT_LAUNCH_GRACE,
    grace_poll: float = DEFAULT_LAUNCH_GRACE_POLL,
) -> int:
    """Spawn the agent detached; health-check it survives the grace window; return its pid.

    start_new_session=True detaches it into its own process group so it survives this
    session ending (it re-parents to init on our exit). stdin is closed (codex exec
    otherwise blocks reading stdin); stdout+stderr append to the per-slug log. The env is
    GIT_*-scrubbed and carries the --state-root override as XDG_STATE_HOME (see _child_env).

    After spawn we wait a short grace window and poll: Popen succeeding only means exec()
    worked, not that the child is viable. A child that rejects a pinned flag / lacks auth /
    hits a bad config exits within moments — a real pipeline runs for minutes, so ANY
    completed exit inside the grace window (zero OR nonzero) is a launch failure. We raise a
    SystemExit naming the child's exit code (the dispatch log holds its stderr) so the caller
    treats it exactly like a spawn OSError: NO slot is written, and the leaked worktree is
    named for adjudication.
    """
    # Dispatch-log setup can fail after the worktree exists (e.g. dispatch-logs wedged as a
    # regular file). Convert that OSError to a SystemExit so the caller's wrapper names the
    # leaked worktree for adjudication — the documented exit-2 contract, never a bare
    # traceback (exit 1). No process has launched yet, so there is no orphan, only the
    # worktree.
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = log_path.open("a", encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"failed to open dispatch log {log_path}: {exc}") from exc
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(worktree),
            env=_child_env(state_root),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        raise SystemExit(f"failed to launch {argv[0]!r}: {exc}") from exc
    finally:
        # The child inherits its own dup of the fd; closing our handle is safe and avoids
        # leaking it into this process (also runs on the OSError path — no double close).
        log.close()

    # Health check: give the child a moment, then confirm it is still running. An instant
    # exit (a rejected flag, missing auth, bad config) is a launch failure, not a launch.
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        rc = proc.poll()
        if rc is not None:
            raise SystemExit(
                f"{argv[0]!r} exited immediately (rc={rc}) within the "
                f"{grace}s launch grace window — treating as a launch failure; "
                f"see {log_path} for its stderr"
            )
        time.sleep(grace_poll)
    return proc.pid


def dispatch(
    args: argparse.Namespace,
    *,
    codex_id_timeout: float = DEFAULT_CODEX_ID_TIMEOUT,
    codex_id_poll: float = DEFAULT_CODEX_ID_POLL,
    launch_grace: float = DEFAULT_LAUNCH_GRACE,
    launch_grace_poll: float = DEFAULT_LAUNCH_GRACE_POLL,
) -> int:
    canonical = None if args.no_canonical_check else args.canonical
    _cos_preflight.require_canonical(canonical)

    tier = args.tier
    # The tier picks the surface and its blessed pins; an explicit --surface can override
    # the surface (dropping the pins if it contradicts the tier). See resolve_profile.
    surface, profile_flags = resolve_profile(tier, args.surface)

    continue_pr = getattr(args, "continue_pr", None)
    brief_file = getattr(args, "brief_file", None)
    no_rebase = bool(getattr(args, "no_rebase", False))
    if continue_pr is None:
        mode = "fresh"
        issues = parse_issues(args.issues)
        prs: list[int] = []
        pr_branch = None
        pr_base_branch = None
        prompt = None
        slug = args.slug or default_slug(surface, issues)
    else:
        mode = "continue"
        issues = []
        prs = [continue_pr]
        pr_branch = None
        pr_base_branch = None
        prompt = None
        slug = args.slug or default_continue_slug(surface, continue_pr)
    validate_slug(slug)

    state_root: Path = args.state_root
    slots_root = state_root / "pipeline-slots"
    slot_path = slots_root / f"{slug}.json"
    worktree = args.canonical / ".claude" / "worktrees" / slug
    log_path = state_root / "dispatch-logs" / f"{slug}.log"

    # 1. Kill switch.
    kill_switch = state_root / "auto-dispatch.off"
    if kill_switch.exists():
        print(f"auto-dispatch disabled: kill switch present at {kill_switch}")
        return 3

    # 2. Slot budget.
    busy = len(_cos_preflight.scan_slots(slots_root)) if slots_root.is_dir() else 0
    if busy >= args.max_slots:
        print(f"no free slot budget: busy {busy}/{args.max_slots}")
        return 4

    # 3. Collision — a slot for the same PR is already live, or the target slot exists.
    if continue_pr is not None and (
        existing_slot := live_slot_for_pr(slots_root, continue_pr)
    ):
        raise SystemExit(
            f"PR #{continue_pr} is already claimed by live slot {existing_slot}"
        )
    if slot_path.exists():
        raise SystemExit(f"slot collision: {slot_path} already exists")
    if mode == "fresh" and worktree.exists():
        raise SystemExit(f"worktree collision: {worktree} already exists")

    if continue_pr is not None:
        if not args.dry_run:
            require_maintainer_authored([continue_pr])
        pr_info = resolve_continue_pr(continue_pr)
        issues = list(pr_info["issues"])
        pr_branch = str(pr_info["branch"])
        pr_base_branch = str(pr_info.get("base_branch") or "main")
        prompt = continuation_prompt(
            surface,
            continue_pr,
            issues,
            pr_branch,
            pr_base_branch,
            read_brief(brief_file),
        )

    # A pre-generated uuid is the claude session id (known before launch); codex's is
    # parsed from its JSONL log after launch.
    pre_session = str(uuid.uuid4()) if surface == "claude" else None
    argv = build_launch_argv(
        surface,
        worktree,
        issues,
        state_root,
        pre_session,
        profile_flags,
        args.canonical,
        prompt=prompt,
    )

    if args.dry_run:
        # Checks 1–3 only; no worktree, no launch, no slot file, no log. The argv already
        # reflects the resolved tier profile; surface/tier are echoed for the operator.
        print(
            json.dumps(
                {
                    "launch_argv": argv,
                    "slot_path": str(slot_path),
                    "surface": surface,
                    "tier": tier,
                    "mode": mode,
                    "issues": issues,
                    "prs": prs,
                },
                indent=2,
            )
        )
        return 0

    # 3b. Author check — defense in depth: refuse to launch a pipeline on any issue that is
    # not maintainer-authored. Read-only network I/O, run BEFORE the first side effect (the
    # worktree). Deliberately AFTER the --dry-run return: dry-run promises zero side effects
    # AND no author-gate network, and its check-only tests do not stub `gh` — so a live
    # author lookup there would break that contract. Dry-run still surfaces the resolved
    # argv/slot; the author gate is a launch-path guard, so gating it here loses nothing
    # for the preview. Continue mode checks the PR before resolving its body; here it checks
    # the closing issues before the first side effect.
    require_maintainer_authored(issues)

    # 4. Worktree — fresh mode creates a placeholder wt/<slug> branch off origin/main;
    # continue mode materializes or reuses the PR branch and rebases it onto the PR base.
    canonical_repo: Path = args.canonical
    # Guard the mutating git ops: confirm canonical is a worktree ROOT so `git -C` can't
    # walk up into an unintended enclosing repo (see require_git_checkout).
    require_git_checkout(canonical_repo)
    if mode == "fresh":
        run_git(canonical_repo, ["fetch", "origin", MAIN_FETCH_REFSPEC])
        run_git(
            canonical_repo,
            [
                "worktree",
                "add",
                "-b",
                f"wt/{slug}",
                str(worktree),
                "origin/main",
            ],
        )
    else:
        assert pr_branch is not None
        assert pr_base_branch is not None
        prepare_continue_worktree(
            canonical_repo,
            worktree,
            pr_branch,
            pr_base_branch,
            rebase=not no_rebase,
        )

    # 5. Launch detached, then health-check it survived the grace window. A launch failure
    # after the worktree exists (spawn OSError, log-setup OSError, or an instant child exit)
    # leaks only the worktree (named in the error for adjudication); NO slot file is written.
    # Append an explicit run sentinel FIRST and poll only from that byte offset, so both
    # codex and plain-surface logs get a stable current-run boundary.
    try:
        log_offset = append_run_sentinel(
            log_path,
            slug=slug,
            issues=issues,
            prs=prs,
            surface=surface,
            tier=tier,
            mode=mode,
        )
        pid = launch_detached(
            argv,
            worktree,
            log_path,
            state_root,
            grace=launch_grace,
            grace_poll=launch_grace_poll,
        )
    except SystemExit as exc:
        raise SystemExit(
            f"{exc.code} (worktree left at {worktree} for adjudication)"
        ) from exc

    # 6. Slot file — written IMMEDIATELY after a successful launch (not last), so a failed
    # launch never leaks a slot AND the unrecorded window between launch and registration is
    # as small as possible. The child pipeline is told to create/update this SAME slot file
    # at its lane claim; writing our ownership record now (session=null for codex, the
    # pre-generated uuid for claude) preserves the "slot only after launch" invariant while
    # letting a fast child register its drafts/prs into it during the codex id poll (step 7).
    #
    # The agent is ALREADY running here: if the write fails (e.g. an unwritable ledger dir)
    # we must NOT crash with a traceback (exit 1) — that would violate the exit-2 contract
    # and leave a running agent with no ledger entry. Convert to exit 2 with everything
    # needed to adjudicate the orphan by hand.
    session_id = (
        pre_session  # claude: final; codex: null placeholder, enriched in step 7
    )
    try:
        write_slot_file(
            slot_path,
            slug,
            issues,
            surface,
            tier,
            session_id,
            pid,
            prs=prs,
            mode="continue" if mode == "continue" else None,
        )
    except OSError as exc:
        raise SystemExit(
            f"slot write failed: {exc} — agent ALREADY LAUNCHED and now unrecorded "
            f"(pid={pid}, surface={surface}, session={session_id}, "
            f"log={log_path}, worktree={worktree}, slot={slot_path}); "
            "adjudicate the orphan and register or kill it manually"
        ) from exc

    # 7. Codex session id — poll the log (only bytes past log_offset) for the thread id,
    # then MERGE it into the slot's `session` field without clobbering whatever the child
    # pipeline may have written into issues/prs during the poll window (its claim is
    # fresher). A merge-update failure AFTER a successful step-6 write is non-fatal: the
    # slot exists (budget is accounted, the agent is recorded), only the session enrichment
    # failed — warn on stderr but still exit 0. claude's session is already final in step 6.
    if surface == "codex":
        session_id = poll_codex_session_id(
            log_path, codex_id_timeout, codex_id_poll, start_offset=log_offset
        )
        if session_id is None:
            print(
                f"warning: no codex session id in {log_path} after "
                f"{int(codex_id_timeout)}s; recording session=null "
                "(fuzzy thread search will recover it)",
                file=sys.stderr,
            )
        try:
            merge_session_into_slot(
                slot_path,
                slug,
                issues,
                surface,
                tier,
                session_id,
                pid,
                prs=prs,
                mode="continue" if mode == "continue" else None,
            )
        except OSError as exc:
            print(
                f"warning: could not enrich slot {slot_path} with codex session "
                f"{session_id!r}: {exc}; slot is registered but its session field may be "
                "stale (fuzzy thread search will recover the id)",
                file=sys.stderr,
            )

    print(
        json.dumps(
            {
                "slot": slug,
                "worktree": str(worktree),
                "surface": surface,
                "tier": tier,
                "mode": mode,
                "issues": issues,
                "prs": prs,
                "session": session_id,
                "pid": pid,
                "log": str(log_path),
            },
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--issues",
        help="comma-separated issue numbers for the pr-pipeline lane, e.g. 1011,1012",
    )
    target.add_argument(
        "--continue-pr",
        type=int,
        help="continue an existing same-repository PR branch instead of dispatching a "
        "fresh lane from origin/main",
    )
    ap.add_argument(
        "--brief-file",
        type=Path,
        default=None,
        help="additional continuation instructions for --continue-pr",
    )
    ap.add_argument(
        "--no-rebase",
        action="store_true",
        help="with --continue-pr, skip the default rebase onto the PR base branch",
    )
    ap.add_argument(
        "--tier",
        choices=tuple(LAUNCH_PROFILES),
        default=DEFAULT_TIER,
        help="launch profile (default: hard = codex gpt-5.5 xhigh; easy = claude "
        "sonnet-5 + opus advisor). See the module docstring for the full profiles",
    )
    ap.add_argument(
        "--surface",
        choices=("codex", "claude"),
        default=None,
        help="explicit surface override; defaults to the --tier's implied surface. When "
        "it CONTRADICTS the tier surface the launch runs on it with ambient defaults "
        "(no model/effort/advisor pins)",
    )
    ap.add_argument(
        "--slug",
        default=None,
        help="slot/worktree slug; defaults to auto-<surface>-issue-<first issue>",
    )
    ap.add_argument(
        "--state-root",
        type=Path,
        default=default_state_root(),
        help="state store root holding pipeline-slots/, dispatch-logs/, and the kill "
        "switch; defaults to $XDG_STATE_HOME/registry-research-toolkit",
    )
    ap.add_argument("--max-slots", type=int, default=_cos_preflight.DEFAULT_MAX_SLOTS)
    ap.add_argument(
        "--canonical",
        type=Path,
        default=_cos_preflight.DEFAULT_CANONICAL,
        help="required canonical checkout path (git commands run here)",
    )
    ap.add_argument(
        "--no-canonical-check",
        action="store_true",
        help="skip the canonical-checkout guard, for tests or local runs",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="run checks 1–3 only, print the launch argv + intended slot path as JSON, "
        "and exit 0 with zero side effects",
    )
    args = ap.parse_args(argv)
    try:
        return dispatch(args)
    except SystemExit as exc:
        message = exc.code if isinstance(exc.code, str) else ""
        if message:
            print(message, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
