#!/usr/bin/env python3
"""Codex-surface pr-pipeline lane runner: owns the codex_bot review↔fix loop.

A codex-surface lane structurally cannot run its own `codex review`: a `codex review`
launched *inside* the lane agent's own seatbelt is a nested sandbox, and every exec is
denied (`sandbox_apply: Operation not permitted`, kind `nested_sandbox` — see
codex_local_review.py and #1049). So a codex lane does everything a normal pr-pipeline
lane does EXCEPT the codex_bot gate: it opens the draft PR (`Closes #N`), registers the
slot `prs`, runs independent review / build_db / visual, and writes `gate.json` with the
`codex_bot` line deferred and `status: blocked` (blocker=codex_bot).

THIS runner is the sibling — launched OUTSIDE the agent's seatbelt (by cos_dispatch, or
directly) — that completes only that gate. It runs `codex_local_review.py` un-nested,
and drives the fix loop by resuming the SAME warm codex session with a findings brief,
until the review is clean (or a bound is hit). It owns the `codex_bot` gate line and —
only when codex_bot is the SOLE remaining unmet gate — flips `status` to
`ready-to-merge`. The chief-of-staff then sees a single finished handoff.

Execution model (the key insight): `codex exec <prompt>` runs ONE turn to completion and
EXITS. So the runner does not stream-parse; it foreground-runs each codex turn (stdout +
stderr appended to the per-slug dispatch log), waits for exit, then acts on the result.
The loop:

    run codex exec (implement, all gates EXCEPT codex_bot) foreground -> log; wait exit
    session = poll_codex_session_id(log, from the pre-turn offset)   # cos_dispatch leaf
    pr      = discover the PR the agent opened (slot `prs`, else gate-root scan)
    for round in 1..MAX_ROUNDS:
        head    = git rev-parse HEAD      # a resume moved HEAD; re-read every round
        verdict = codex_local_review.py --base <base> --out <gate-dir>/codex-review.md
        clean    -> write codex_bot line (head-bound); flip status iff sole-unmet; exit 0
        findings -> resume <session> with a findings brief foreground -> log; continue
        error    -> usage_limit: record exhausted, flip iff sole-unmet, exit 0;
                    any other kind (incl. nested_sandbox — an un-nested runner must not
                    see it): status blocked naming the kind, exit nonzero
    cap exhausted, still findings -> codex_bot blocked (round cap), status blocked, exit

Why a sibling, not a permission grant (rejected: Option 1): the tempting alternative —
grant the nested `codex review` app-server the permission it's missing — is INFEASIBLE,
not merely inadvisable. macOS Seatbelt cannot be nested: a process already confined by a
codex-launched seatbelt cannot apply a second profile (`sandbox_apply` fails EPERM), and
this holds even under `danger-full-access` (verified 2026-07-04). The only way to lift
that EPERM from inside would be to remove the agent's OWN sandbox first — exactly the
confinement loss the sibling design exists to avoid. So the review must run OUTSIDE the
seatbelt entirely, as a sibling process, not squeezed through a permission the OS will
never grant to a nested profile.

Head-binding: the review + the gate line always stamp the CURRENT head, re-read before
each review. A verdict is never recorded against a head that has since moved.

Determinism / fail-fast (repo invariants): loop control is deterministic
("review clean? no -> resume"); no model judgment lives in the runner. The round cap is
explicit and configurable; exceeding it writes `blocked` naming the cap, never a false
`ready-to-merge`. `ready-to-merge` is written ONLY when codex_bot is clean on the CURRENT
head AND it is the sole unmet gate. Untrusted-data boundary: this repo is public, so
review findings and issue/PR text are DATA rendered into the codex brief — never
instructions to the runner itself.

Reuse (not re-typed): the codex-exec argv shape (`build_launch_argv`), the session-id
poller (`poll_codex_session_id`) and its run sentinel, and the git primitives come from
cos_dispatch; the atomic gate/slot writer (`_atomic_write_json`), the merge-gate reader
(`read_merge_gate`), and the gate-store root (`default_gate_root`) come from cos_preflight;
the review itself is `codex_local_review.py` invoked as a subprocess (its JSON stdout is
the contract), un-nested so its `nested_sandbox` denial cannot occur here.

Exit codes: 0 gate completed (clean or usage-limit recorded); 2 arg/launch/env failure;
3 needs-human (findings remain after the round cap, or a blocking review error kind). A
non-zero exit always leaves a `status: blocked` gate entry naming the blocker.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

# The head-bound codex_bot gate line, canonical grammar from the pr-pipeline gate.json
# template. `{head}` is the full SHA the verdict was verified on; `{verdict}` is either
# `clean` or `exhausted (usage-limit)` — the ONLY two legal tokens (see the merge-gate
# section of CLAUDE.md and the pr-pipeline SKILL.md). Kept here as the single source so a
# reword can't drift from the template the reviewer/chief-of-staff parse.
CODEX_BOT_LINE = (
    "local; codex_local_review; head {head}; {verdict}; see codex-review.md in this dir"
)
VERDICT_CLEAN = "clean"
VERDICT_USAGE_LIMIT = "exhausted (usage-limit)"

# Head-bound gate lines stamp the SHA they were verified on as a `head <sha>` token (the
# build_db / visual / codex_bot canonical grammar). Used to detect a gate line that has gone
# stale after a fix round moved HEAD. A line with NO such token (`"not required"`,
# `"pass; gh pr checks"`, `"updated; ..."`) is not head-bound and can never be stale.
HEAD_TOKEN_RE = re.compile(r"head\s+([0-9a-f]{7,40})", re.IGNORECASE)

# Default round cap: one initial review + up to this many fix rounds. 3 mirrors the
# pr-pipeline review-loop budget; injectable via --max-rounds.
DEFAULT_MAX_ROUNDS = 3

# Bounded poll for the codex thread id in the implement turn's JSONL log (reused from
# cos_dispatch's contract). A null session is tolerated — if findings then need fixing, the
# loop records a head-bound blocked codex_bot line and hands off to a human (no resume is
# possible without a session). Injectable via run()'s kwargs so tests don't sleep the ceiling.
DEFAULT_CODEX_ID_TIMEOUT = 30.0
DEFAULT_CODEX_ID_POLL = 0.25

# The review error kind that is recordable (the merge gate's exhausted-analog), not a
# blocker; every other kind codex_local_review emits (timeout / format_drift /
# precondition / tool_failure / nested_sandbox) is a hard blocker.
RECORDABLE_ERROR_KIND = "usage_limit"

# Exit codes (fail-fast + stable, matching the cos_* sibling conventions).
EXIT_OK = 0
EXIT_TOOL = 2
EXIT_NEEDS_HUMAN = 3


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


# _gh's sys.modules-guarded loader gives single process-wide instances of the siblings, so
# a monkeypatch of any of these reaches every consumer (no split copies): cos_dispatch for
# the codex-exec argv builder + the session-id poller/sentinel + git primitives;
# cos_preflight for the atomic gate writer, merge-gate reader, and gate-store root.
_gh = _load_gh()
_cos_dispatch = _gh.load_sibling("cos_dispatch")
_cos_preflight = _gh.load_sibling("cos_preflight")

# The review subprocess is codex_local_review.py, invoked un-nested so its `nested_sandbox`
# denial cannot occur; its JSON stdout is the contract, so we resolve its path once here.
CODEX_LOCAL_REVIEW = Path(__file__).with_name("codex_local_review.py")


def _now() -> str:
    return datetime.now(UTC).isoformat()


# ---- the codex prompts (the runner's contract with the lane agent) -----------


def implement_prompt(issues: list[int]) -> str:
    """The implement-turn prompt: run the FULL pr-pipeline EXCEPT the codex_bot gate.

    This IS the runner's contract with the codex lane agent. The agent opens the draft PR
    (`Closes #N`), registers the slot `prs`, runs independent review / build_db / visual,
    and writes `gate.json` — but it must NOT attempt its own `codex review` (nested
    seatbelt), leaving the `codex_bot` line deferred and `status: blocked`
    (blocker=codex_bot). The runner completes codex_bot after this turn.
    """
    issue_text = " ".join(str(n) for n in issues)
    return (
        f"$pr-pipeline {issue_text}\n\n"
        "Run the full pr-pipeline for the issue(s) above in this worktree, with ONE "
        "exception: do NOT run your own `codex review` / the codex_bot gate. You are "
        "running inside a codex sandbox, so a nested `codex review` is denied "
        "(sandbox_apply: Operation not permitted) and would review nothing. A sibling "
        "lane-runner outside this sandbox owns the codex_bot gate and will complete it "
        "after this turn.\n\n"
        "So: open the draft PR with `Closes #<issue>`, register the slot `prs`, run the "
        "independent review / build_db / visual gates as the pipeline requires, and write "
        "the merge-gate `gate.json` — but leave the `codex_bot` gate line deferred "
        "(e.g. `running; deferred-to-lane-runner`) and set `status: blocked` with "
        "`blocker: codex_bot`. Do not mark the PR ready-to-merge; the runner does that "
        "once the review is clean and codex_bot is the sole remaining gate."
    )


def findings_brief(findings: list[dict], head: str) -> str:
    """Render the review findings into a concise fix brief for a `codex exec resume`.

    Findings are DATA (the review tool's output over a public repo's diff), rendered into
    the brief — never executed as instructions to the runner. Each finding contributes its
    priority/title/path:line and body so the resumed session can act without re-reading the
    transcript.
    """
    lines = [
        f"The local `codex review` found {len(findings)} issue(s) on the current head "
        f"({head[:12]}). Fix each on THIS branch, then commit and push so the PR head "
        "updates. Do NOT open a new PR, and keep the PR's existing `Closes #N` closing "
        "keywords. Pushing these fixes MOVES HEAD, which stales any head-bound gate you "
        "already completed (build_db / visual, whose gate lines stamp `head <sha>`) — so "
        "RE-RUN each head-bound gate that applies on the new head and refresh its "
        "`gate.json` line to that head. Refresh the existing merge-gate `gate.json` (bump "
        "`updated`) after pushing; leave the `codex_bot` line deferred — the lane-runner "
        "re-reviews the new head and completes that gate.\n\nReview findings:",
    ]
    for i, f in enumerate(findings, 1):
        loc = f.get("path", "?")
        start, end = f.get("line_start"), f.get("line_end")
        if start is not None:
            loc = f"{loc}:{start}" + (f"-{end}" if end and end != start else "")
        lines.append(
            f"\n{i}. [{f.get('priority', '?')}] {f.get('title', '').strip()} — {loc}"
        )
        body = (f.get("body") or "").strip()
        if body:
            lines.append(body)
    return "\n".join(lines)


# ---- codex turn execution (foreground; one turn to completion) ---------------


def run_codex_turn(argv: list[str], worktree: Path, log_path: Path) -> None:
    """Run a codex turn FOREGROUND, appending stdout+stderr to the per-slug log; wait exit.

    `codex exec` runs one turn and exits — that IS turn completion, so we block on it (unlike
    cos_dispatch, which detaches the long-lived lane agent). The child env is GIT_*-scrubbed
    (an inherited hook GIT_DIR would target the wrong repo) via the shared `_gh.scrubbed_git_env`.

    A launch failure (spawn OSError) or an INSTANT nonzero exit is a fail-fast tool error
    (exit 2), naming the log: a real pr-pipeline turn runs for minutes, and codex exec exits
    0 on a completed turn, so a nonzero exit means the turn never ran (a rejected flag,
    missing auth, bad config) — there is nothing to poll a session id from.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with log_path.open("a", encoding="utf-8") as log:
            # simplify: no timeout by design — a real pr-pipeline turn legitimately runs for
            # many minutes, so any fixed ceiling would kill healthy turns; a wedged turn is
            # caught by cos_watch's stale-slot timer, which surfaces the hung slot for
            # adjudication. Add a timeout only if a non-slot caller ever needs a bound.
            proc = subprocess.run(
                argv,
                cwd=str(worktree),
                env=_gh.scrubbed_git_env(),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
    except OSError as exc:
        raise SystemExit(
            f"{EXIT_TOOL}:failed to launch codex turn {argv[0]!r}: {exc}; see {log_path}"
        ) from exc
    if proc.returncode != 0:
        raise SystemExit(
            f"{EXIT_TOOL}:codex turn exited {proc.returncode} (the turn did not "
            f"complete — a completed `codex exec` exits 0); see {log_path}"
        )


# ---- PR discovery ------------------------------------------------------------


def discover_pr(slot_file: Path | None, gate_root: Path) -> int:
    """The PR the lane agent opened: slot `prs` first entry, else a gate-root pr-* scan.

    The slot file's `prs` is the authoritative claim the agent registered; a gate-root scan
    (`pr-<N>/` dirs) is the fallback when no slot file was passed or it carries no PR yet.
    A gate directory without a matching `gate.json` (or whose `gate.json` `pr` disagrees) is
    skipped, mirroring the gate protocol's read rule. Fail-fast (exit 2) if none is found —
    the agent turn was supposed to open a PR. A slot claiming MORE THAN ONE PR also fails
    fast: the runner completes codex_bot for a single PR, and silently taking prs[0] would
    strand the others' gates (real multi-PR support is a follow-up).
    """
    if slot_file is not None:
        loaded = _cos_preflight._read_json_tolerant(slot_file, "pipeline-slot file")
        if loaded is not None:
            prs = loaded[1].get("prs") if isinstance(loaded[1], dict) else None
            if isinstance(prs, list) and prs:
                if len(prs) > 1:
                    # The runner owns codex_bot for exactly ONE PR; silently completing prs[0]
                    # would leave the other PRs' codex_bot gates uncompleted. Fail fast until
                    # real multi-PR support lands (a follow-up).
                    raise SystemExit(
                        f"{EXIT_TOOL}:the lane-runner supports a single PR per lane, but this "
                        f"slot claims {prs}; split the lane or dispatch with --no-lane-runner"
                    )
                first = prs[0]
                if isinstance(first, int):
                    return first
    found: list[int] = []
    for path in sorted(gate_root.glob("pr-*")):
        if not path.is_dir():
            continue
        try:
            number = int(path.name[len("pr-") :])
        except ValueError:
            continue
        loaded = _cos_preflight._read_json_tolerant(
            path / "gate.json", "merge-gate file"
        )
        if loaded is None or not isinstance(loaded[1], dict):
            continue
        if loaded[1].get("pr") == number:
            found.append(number)
    if len(found) == 1:
        return found[0]
    raise SystemExit(
        f"{EXIT_TOOL}:could not discover the PR the lane agent opened "
        f"(slot_file={slot_file}, gate-root scan found {found or 'none'}); the implement "
        "turn was supposed to open exactly one draft PR"
    )


# ---- gate.json read / write --------------------------------------------------


def read_gate(gate_dir: Path, pr: int) -> dict:
    """Read the PR's gate.json as a raw dict (fail-fast if absent/mismatched).

    The runner completes a gate the lane agent already wrote, so an absent or pr-mismatched
    gate.json is a hard error — there is nothing to complete. (Distinct from
    cos_preflight.read_merge_gate, which returns a summarized/absent shape for the wake
    probe; here we need the raw `gates`/`status`/`blocker` fields to update in place.)
    """
    path = gate_dir / "gate.json"
    loaded = _cos_preflight._read_json_tolerant(path, "merge-gate file")
    if loaded is None or not isinstance(loaded[1], dict) or loaded[1].get("pr") != pr:
        raise SystemExit(
            f"{EXIT_TOOL}:gate.json for PR #{pr} is missing or does not match at {path}; "
            "the implement turn was supposed to write it"
        )
    return loaded[1]


def codex_bot_is_sole_unmet(gate: dict) -> bool:
    """True iff codex_bot is the ONLY unmet gate — the guard on flipping ready-to-merge.

    Trusts the lane agent's `blocker` contract rather than parsing free-text gate lines. The
    runner's own implement prompt instructs the agent to write `status: blocked` with
    `blocker: "codex_bot"` EXACTLY when codex_bot is the sole unmet gate (all others met);
    any other remaining blocker is named in `blocker` instead. A free-text scan of the gate
    lines can't work — the real template grammar has `tests: "<commands run>"`,
    `docs: "updated; ..."`, `stack: "before #N"` etc., none of which match a fixed
    met-token whitelist, so every real PR would read as not-sole-unmet and the runner would
    never flip. So key off the one field the agent set for exactly this purpose.

    This flip is advisory, never the sole merge authority: the chief-of-staff re-checks every
    gate + live CI + mergeability immediately before merging (CLAUDE.md "PR merge gate").
    """
    return gate.get("blocker") == "codex_bot"


def head_bound_gates_current(gate: dict, head: str) -> tuple[bool, str | None]:
    """Are all OTHER head-bound gate lines still verified on the current head?

    The runner only re-verifies codex_bot. But a `findings → resume` round pushes fix commits
    and MOVES HEAD, which makes the OTHER head-bound gates (build_db, visual — whose lines
    carry a `head <sha>` stamp) stale: they were verified on the PRE-fix head. Flipping to
    ready-to-merge in that state would hand off a PR whose build_db/visual gate no longer
    matches the head. So before flipping, require every non-codex_bot gate line that stamps a
    head to match the CURRENT head.

    codex_bot is EXCLUDED — it is being (re)written to the current head by this same call, so
    its own stamp is authoritative, not a staleness signal. A gate line with no `head ` token
    is not head-bound (`"not required"`, `"pass; gh pr checks"`, `"updated; ..."`) → never
    stale. Prefix-match either direction because lines may carry the full or a truncated
    (12-char) sha. Returns (all_current, first_stale_gate_name).
    """
    gates = gate.get("gates")
    if not isinstance(gates, dict):
        return True, None
    for name, line in gates.items():
        if name == "codex_bot" or not isinstance(line, str):
            continue
        match = HEAD_TOKEN_RE.search(line)
        if match is None:
            continue  # not head-bound → never stale
        sha = match.group(1).lower()
        current = head.lower()
        if not (current.startswith(sha) or sha.startswith(current[:12])):
            return False, name
    return True, None


def write_codex_bot_gate(
    gate_dir: Path,
    pr: int,
    head: str,
    *,
    verdict: str = VERDICT_CLEAN,
    blocking: bool = False,
    blocker: str,
) -> None:
    """Update the PR's gate.json codex_bot line, then set status, then atomic-write.

    `verdict` is consumed ONLY when NOT blocking — it is the completed codex_bot line's
    verdict token (`clean` or the usage-limit form). When blocking, `verdict` is ignored:
    `blocker` names the unmet item and the codex_bot line records the block reason. status
    flips to `ready-to-merge` ONLY when the verdict is non-blocking AND codex_bot is the
    sole unmet gate (all other gate lines already met) AND every OTHER head-bound gate line
    is still verified on the CURRENT head (a fix round that moved HEAD staled build_db /
    visual — see head_bound_gates_current). If a head-bound gate is stale, status stays
    `blocked` naming it; if the agent named another gate as the blocker, status stays
    `blocked` preserving that blocker. Written atomically (temp+rename) via the shared
    cos_preflight leaf so the preflight probe never sees a torn write; the caller is
    responsible for the codex-review.md evidence already existing.
    """
    gate = read_gate(gate_dir, pr)
    gates = gate.get("gates")
    if not isinstance(gates, dict):
        gates = {}
        gate["gates"] = gates

    if blocking:
        gates["codex_bot"] = (
            f"local; codex_local_review; head {head}; blocked; {blocker}"
        )
        gate["status"] = "blocked"
        gate["blocker"] = blocker
    else:
        gates["codex_bot"] = CODEX_BOT_LINE.format(head=head, verdict=verdict)
        all_current, stale_gate = head_bound_gates_current(gate, head)
        if codex_bot_is_sole_unmet(gate) and all_current:
            gate["status"] = "ready-to-merge"
            gate["blocker"] = None
        elif codex_bot_is_sole_unmet(gate) and not all_current:
            # codex_bot is clean on the current head, but a fix round moved HEAD and left
            # another head-bound gate (build_db / visual) verified on a stale head. Fail
            # closed: never a ready-to-merge with a stale gate. The resumed agent is asked to
            # re-verify head-bound gates it completed (see findings_brief); this freshness
            # check is the backstop when it doesn't.
            gate["status"] = "blocked"
            gate["blocker"] = (
                f"{stale_gate} verified on a stale head; re-verify on {head[:12]}"
            )
        else:
            # codex_bot is done, but the agent named another gate as the blocker — never a
            # false ready-to-merge. Leave status blocked and PRESERVE the agent's existing
            # `blocker` (the real remaining gate); do not synthesize a new blocker string.
            gate["status"] = "blocked"

    gate["head"] = head
    gate["updated"] = _now()
    _cos_preflight._atomic_write_json(gate_dir / "gate.json", gate)


# ---- slot heartbeat ----------------------------------------------------------


def touch_slot(slot_file: Path | None) -> None:
    """Bump the slot's `updated` heartbeat so cos_watch's stale timer doesn't fire mid-lane.

    A long review↔fix loop can outlast the stale-slot threshold; a small overlay write (the
    same atomic writer, preserving every other field) keeps the slot fresh. Best-effort: a
    missing/invalid slot file is a no-op (the loop's real work is the gate, not the touch).
    """
    if slot_file is None:
        return
    loaded = _cos_preflight._read_json_tolerant(slot_file, "pipeline-slot file")
    if loaded is None or not isinstance(loaded[1], dict):
        return
    data = loaded[1]
    data["updated"] = _now()
    # Heartbeat is best-effort; a wedged slot dir must not sink the gate work.
    with contextlib.suppress(OSError):
        _cos_preflight._atomic_write_json(slot_file, data)


def enrich_slot_session(slot_file: Path | None, session: str | None) -> None:
    """Overlay the resolved codex session id onto the slot without clobbering other fields.

    The runner launches codex, so it owns the session id (as cos_dispatch does on its path).
    Best-effort overlay: preserve the child's issues/prs, only stamp `session`.
    """
    if slot_file is None or session is None:
        return
    loaded = _cos_preflight._read_json_tolerant(slot_file, "pipeline-slot file")
    if loaded is None or not isinstance(loaded[1], dict):
        return
    data = loaded[1]
    data["session"] = session
    data["updated"] = _now()
    with contextlib.suppress(OSError):
        _cos_preflight._atomic_write_json(slot_file, data)


# ---- review subprocess -------------------------------------------------------


def run_review(base: str, gate_dir: Path, worktree: Path) -> dict:
    """Run codex_local_review.py as a subprocess in the worktree; return its parsed JSON.

    Invoked un-nested (this runner is outside the lane agent's seatbelt), so the review's
    `nested_sandbox` denial cannot legitimately occur; if it somehow does, the caller treats
    it as an environment blocker. `--out <gate-dir>/codex-review.md` lands the evidence
    straight in the gate directory. codex_local_review's stdout JSON is the contract (exit 0
    clean / 1 findings / 2 error); a torn/empty stdout is a tool failure.
    """
    proc = subprocess.run(
        [
            sys.executable,
            str(CODEX_LOCAL_REVIEW),
            "--base",
            base,
            "--out",
            str(gate_dir / "codex-review.md"),
        ],
        cwd=str(worktree),
        env=_gh.scrubbed_git_env(),
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(proc.stdout)
    except ValueError as exc:
        raise SystemExit(
            f"{EXIT_TOOL}:could not parse codex_local_review.py JSON (exit "
            f"{proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()!r}"
        ) from exc


# ---- the loop ----------------------------------------------------------------


def run_loop(
    *,
    worktree: Path,
    base: str,
    gate_dir: Path,
    pr: int,
    slot_file: Path | None,
    session: str | None,
    log_path: Path,
    max_rounds: int,
    state_root: Path,
    canonical: Path,
    profile_flags: list[str],
) -> int:
    """The review↔fix loop over the PR the agent opened. Returns the process exit code.

    Each round re-reads the live head (a resume moved it), runs the review against that
    head, and acts: clean records the gate; findings resume the warm session with a brief;
    an error records (usage_limit) or blocks (any other kind). Exhausting the cap with
    findings still present is a needs-human block (exit 3).
    """
    for _round in range(1, max_rounds + 1):
        touch_slot(slot_file)
        head = _cos_dispatch.git_output(worktree, ["rev-parse", "HEAD"])
        result = run_review(base, gate_dir, worktree)
        verdict = result.get("verdict")
        # Bind the gate stamp to the head codex actually reviewed when it reports one, so a
        # concurrent HEAD move between the rev-parse and the review can't mislabel the stamp.
        reviewed_head = result.get("head") or head

        if verdict == "clean":
            write_codex_bot_gate(
                gate_dir,
                pr,
                reviewed_head,
                verdict=VERDICT_CLEAN,
                blocking=False,
                blocker="codex_bot",
            )
            print(
                json.dumps(
                    {
                        "pr": pr,
                        "head": reviewed_head,
                        "codex_bot": "clean",
                        "round": _round,
                    },
                    indent=2,
                )
            )
            return EXIT_OK

        if verdict == "error":
            kind = (result.get("error") or {}).get("kind")
            if kind == RECORDABLE_ERROR_KIND:
                write_codex_bot_gate(
                    gate_dir,
                    pr,
                    reviewed_head,
                    verdict=VERDICT_USAGE_LIMIT,
                    blocking=False,
                    blocker="codex_bot",
                )
                print(
                    json.dumps(
                        {
                            "pr": pr,
                            "head": reviewed_head,
                            "codex_bot": "exhausted (usage-limit)",
                        },
                        indent=2,
                    )
                )
                return EXIT_OK
            # Any other kind (timeout / format_drift / precondition / tool_failure /
            # nested_sandbox) is a hard blocker — the ENVIRONMENT or tooling is wrong, not
            # a PR finding. An un-nested runner must never see nested_sandbox; if it does,
            # it is still recorded as a block, not treated as clean.
            blocker = f"codex review error: {kind or 'unknown'}"
            write_codex_bot_gate(gate_dir, pr, head, blocking=True, blocker=blocker)
            print(f"{blocker}; wrote status: blocked for PR #{pr}", file=sys.stderr)
            return EXIT_NEEDS_HUMAN

        # verdict == "findings" (or an unexpected verdict treated as unfinished): resume the
        # warm session to fix them, then loop to re-review the new head. On the last round we
        # do not resume — fall through to the cap block below.
        findings = result.get("findings") or []
        if _round == max_rounds:
            break
        if session is None:
            # No warm session to resume — we can't deterministically continue the same
            # thread, and a cold session would re-derive the whole lane. Record an accurate
            # head-bound BLOCKED codex_bot line (every other terminal path writes one) and
            # hand off to a human rather than raising with a stale gate.
            blocker = (
                "no codex session id resolved from the implement turn — cannot resume to "
                "fix review findings"
            )
            write_codex_bot_gate(
                gate_dir,
                pr,
                reviewed_head,
                blocking=True,
                blocker=blocker,
            )
            print(f"{blocker}; wrote status: blocked for PR #{pr}", file=sys.stderr)
            return EXIT_NEEDS_HUMAN
        brief = findings_brief(findings, reviewed_head)
        resume_argv = _resume_argv(
            session,
            brief,
            state_root=state_root,
            canonical=canonical,
            profile_flags=profile_flags,
        )
        run_codex_turn(resume_argv, worktree, log_path)

    # Cap exhausted with findings still present: a clear needs-human block naming the cap.
    head = _cos_dispatch.git_output(worktree, ["rev-parse", "HEAD"])
    blocker = f"codex review still had findings after {max_rounds} round(s)"
    write_codex_bot_gate(gate_dir, pr, head, blocking=True, blocker=blocker)
    print(f"{blocker}; wrote status: blocked for PR #{pr}", file=sys.stderr)
    return EXIT_NEEDS_HUMAN


def _resume_argv(
    session: str | None,
    brief: str,
    *,
    state_root: Path,
    canonical: Path,
    profile_flags: list[str],
) -> list[str]:
    """`codex exec resume` — resume the SAME warm session to fix findings, sandboxed + granted.

    A resolved session id is required to resume the warm context; without it the runner
    cannot deterministically continue the same thread, so it fails fast rather than opening a
    cold session that re-derives the whole lane.

    Unlike `codex exec`, `codex exec resume` (codex 0.142.5) does NOT accept `-C` / `-s` /
    `--add-dir` — only `-c <key=value>`, `-m`, `--json`. So the sandbox + writable grants the
    implement turn passes via those flags must instead go through config keys:
      - `approval_policy="never"` — the resume runs stdin=DEVNULL; a prompt would wedge it.
      - `sandbox_mode="workspace-write"` — same posture as the implement turn.
      - `sandbox_workspace_write.writable_roots` — the `--add-dir` equivalent, a TOML array;
        it must grant the SAME two dirs the implement turn does: the dispatch state root and
        `<canonical>/.git` (the linked worktree's writable git state lives OUTSIDE cwd, so
        without it the fix turn's commit/push is denied, #1050). Built via json.dumps so the
        dir strings are safely quoted (a JSON string array is valid TOML).
    The subprocess cwd is set to the worktree by run_codex_turn, so directory needs no `-C`.
    `profile_flags` (`-m`/`-c model_reasoning_effort=…`) are accepted by resume and pin the
    same model tier as the implement turn.
    """
    if session is None:
        raise SystemExit(
            f"{EXIT_TOOL}:no codex session id resolved from the implement turn's log; "
            "cannot resume to fix review findings"
        )
    writable_roots = json.dumps([str(state_root), str(canonical / ".git")])
    return [
        "codex",
        "exec",
        "resume",
        "-c",
        'approval_policy="never"',
        "-c",
        'sandbox_mode="workspace-write"',
        "-c",
        f"sandbox_workspace_write.writable_roots={writable_roots}",
        *profile_flags,
        "--json",
        session,
        brief,
    ]


def run(
    args: argparse.Namespace,
    *,
    codex_id_timeout: float = DEFAULT_CODEX_ID_TIMEOUT,
    codex_id_poll: float = DEFAULT_CODEX_ID_POLL,
) -> int:
    canonical = None if args.no_canonical_check else args.canonical
    if canonical is not None and args.worktree.resolve() == canonical.resolve():
        # The runner drives codex + git INSIDE the worktree, which is a linked worktree, not
        # the canonical main checkout — guard against being pointed at main by mistake.
        raise SystemExit(
            f"{EXIT_TOOL}:--worktree must be a lane worktree, not the canonical checkout "
            f"{canonical}"
        )

    # continue-pr carries no issues here — the lane already exists and the prompt names it.
    issues = _cos_dispatch.parse_issues(args.issues) if args.issues is not None else []

    gate_root: Path = args.gate_root
    log_path: Path = args.log
    slot_file: Path | None = args.slot_file
    state_root: Path = (
        gate_root.parent
    )  # --add-dir grant for the child's ledger/gate writes

    # Resolve the tier's blessed model/effort pins (surface fixed to codex — this runner only
    # ever drives a codex session), used for BOTH the implement turn and the resume turns so
    # they run on the same model tier, not codex's ambient default.
    _, profile_flags = _cos_dispatch.resolve_profile(args.tier, "codex")

    # 1. Build + run the implement turn (foreground; one codex turn to completion). Capture
    #    the log offset BEFORE the turn so the session-id poll reads only this turn's bytes.
    if args.continue_pr is not None:
        prompt = (
            f"$pr-pipeline continue PR #{args.continue_pr}\n\n"
            "Continue this PR in the worktree: address the outstanding work, run every "
            "pipeline gate EXCEPT codex_bot (you are inside a codex sandbox, so a nested "
            "`codex review` is denied), and refresh the merge-gate gate.json leaving "
            "codex_bot deferred with status: blocked (blocker: codex_bot). The sibling "
            "lane-runner completes codex_bot after this turn."
        )
        # The operator's continuation brief (cos_dispatch --brief-file) is DATA describing
        # the follow-up work — rendered into the prompt, mirroring cos_dispatch's
        # continuation_prompt shape. Reuse cos_dispatch.read_brief's tolerant read (a missing
        # file fails fast there). Only meaningful in continue mode; fresh mode has none.
        brief = _cos_dispatch.read_brief(args.brief_file)
        if brief:
            prompt += f"\n\nContinuation brief:\n{brief}"
    else:
        prompt = implement_prompt(issues)
    implement_argv = _cos_dispatch.build_launch_argv(
        "codex",
        args.worktree,
        issues,
        state_root,
        None,
        profile_flags,
        args.canonical,
        prompt=prompt,
    )

    if args.dry_run:
        print(
            json.dumps(
                {
                    "implement_argv": implement_argv,
                    "worktree": str(args.worktree),
                    "base": args.base,
                    "gate_root": str(gate_root),
                    "issues": issues,
                    "continue_pr": args.continue_pr,
                    "max_rounds": args.max_rounds,
                },
                indent=2,
            )
        )
        return EXIT_OK

    log_offset = _cos_dispatch.append_run_sentinel(
        log_path,
        slug=args.worktree.name,
        issues=issues,
        prs=[args.continue_pr] if args.continue_pr is not None else [],
        surface="codex",
        tier=args.tier,
        mode="continue" if args.continue_pr is not None else "fresh",
    )
    run_codex_turn(implement_argv, args.worktree, log_path)

    # 2. Resolve the session id (for the resume turns) and enrich the slot.
    session = _cos_dispatch.poll_codex_session_id(
        log_path, codex_id_timeout, codex_id_poll, start_offset=log_offset
    )
    if session is None:
        print(
            f"warning: no codex session id in {log_path} after the implement turn; "
            "resume turns to fix findings will fail fast if any are needed",
            file=sys.stderr,
        )
    enrich_slot_session(slot_file, session)

    # 3. Discover the PR the agent opened (continue mode names it explicitly — trust that over
    #    discovery so we can't complete the wrong PR), then run the review↔fix loop over it.
    pr = (
        args.continue_pr
        if args.continue_pr is not None
        else discover_pr(slot_file, gate_root)
    )
    gate_dir = gate_root / f"pr-{pr}"
    return run_loop(
        worktree=args.worktree,
        base=args.base,
        gate_dir=gate_dir,
        pr=pr,
        slot_file=slot_file,
        session=session,
        log_path=log_path,
        max_rounds=args.max_rounds,
        state_root=state_root,
        canonical=args.canonical,
        profile_flags=profile_flags,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--worktree",
        type=Path,
        required=True,
        help="the lane worktree where codex runs and the review runs (NOT the canonical "
        "checkout)",
    )
    ap.add_argument(
        "--base",
        required=True,
        help="the PR base ref for the review (origin/main, or the predecessor branch for a "
        "stacked PR) — passed straight to codex_local_review.py",
    )
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--issues",
        help="comma-separated issue numbers the implement turn's pr-pipeline lane covers",
    )
    target.add_argument(
        "--continue-pr",
        type=int,
        help="continue an existing PR instead of a fresh lane (the implement turn resumes "
        "the PR, still deferring codex_bot to the runner)",
    )
    ap.add_argument(
        "--gate-root",
        type=Path,
        default=_cos_preflight.default_gate_root(),
        help="local merge-gate store root (pr-<N>/gate.json lives under it); defaults to "
        "$XDG_STATE_HOME/registry-research-toolkit/merge-gates",
    )
    ap.add_argument(
        "--log",
        type=Path,
        required=True,
        help="per-slug dispatch log to append the codex turns' output to",
    )
    ap.add_argument(
        "--slot-file",
        type=Path,
        default=None,
        help="pipeline-slot file to read the agent's registered PR from and to enrich with "
        "the resolved session id / heartbeat",
    )
    ap.add_argument(
        "--brief-file",
        type=Path,
        default=None,
        help="operator continuation brief woven into the implement turn's --continue-pr "
        "prompt (only meaningful with --continue-pr)",
    )
    ap.add_argument(
        "--max-rounds",
        type=int,
        default=DEFAULT_MAX_ROUNDS,
        help=f"max review rounds before recording codex_bot blocked (default "
        f"{DEFAULT_MAX_ROUNDS})",
    )
    ap.add_argument(
        "--tier",
        choices=tuple(_cos_dispatch.LAUNCH_PROFILES),
        default="hard",
        help="launch tier whose model/effort pins drive both the implement and resume turns "
        "(surface fixed to codex); default hard (gpt-5.5 xhigh)",
    )
    ap.add_argument(
        "--canonical",
        type=Path,
        default=_cos_preflight.DEFAULT_CANONICAL,
        help="canonical checkout path (used only to grant the child its git-dir --add-dir "
        "and to guard --worktree is not main)",
    )
    ap.add_argument(
        "--no-canonical-check",
        action="store_true",
        help="skip the --worktree-is-not-canonical guard, for tests or local runs",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="print the implement-turn argv + plan and exit 0 with zero side effects",
    )
    args = ap.parse_args(argv)
    try:
        return run(args)
    except SystemExit as exc:
        # The fail-fast SystemExits above encode `"<exit-code>:<message>"`; split so the
        # process exits with the stable code and the message lands on stderr.
        code = exc.code
        if isinstance(code, str) and ":" in code:
            prefix, _, message = code.partition(":")
            try:
                rc = int(prefix)
            except ValueError:
                rc, message = EXIT_TOOL, code
            print(message, file=sys.stderr)
            return rc
        if isinstance(code, str):
            print(code, file=sys.stderr)
            return EXIT_TOOL
        return code if isinstance(code, int) else EXIT_TOOL


if __name__ == "__main__":
    raise SystemExit(main())
