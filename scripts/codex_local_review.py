#!/usr/bin/env python3
"""Local `codex review` launcher for the PR merge gate.

Replaces the old GitHub Codex bot-review-window poller (the deleted
`pr_review_status.py`): instead of marking a PR ready and polling GitHub for Codex's
web-integration verdict, the pipeline (and the chief-of-staff, when it self-serves)
runs `codex review` locally against the PR's committed HEAD and reads the verdict off
the transcript. Nothing is posted to GitHub; the verdict lands in the PR's local
merge-gate entry (`gate.json` `codex_bot` line), head-SHA-bound like `visual`/`build_db`.

Run from the PR worktree:

    uv run --no-project python scripts/codex_local_review.py [options]

It resolves `merge_base = git merge-base HEAD <base>` and invokes
`codex review -c sandbox_mode="read-only" --base <merge_base_sha>`, capturing the codex
transcript to `--out`. codex's `--base` accepts a full SHA and is mutually exclusive with a
positional prompt, so no prompt is ever passed.

`--base` is REQUIRED — the caller must pass the ref the PR actually targets (e.g.
`origin/main`, or the predecessor branch for a stacked PR). There is no default: a
defaulted `origin/main` would silently review the wrong diff for a stacked or non-main-based
PR, producing merge-gate evidence against the wrong base — the same silent-wrong-base class
the removed `origin/main → main` fallback was.

The **codex CLI itself exits 0 even when it finds issues** — the transcript text is the
only signal for the clean/findings split, so this script parses codex's stdout, not its
exit code. But a NONZERO codex exit, an empty transcript, a timeout, a format-drifted
transcript, or a no-op review that inspected nothing (a nested-sandbox denial, or no
successful exec — see below) is a hard failure surfaced here as **exit 2** with a
classified error — never a silent false `clean`.

Transcript shapes (verified live, codex-cli 0.142.5):
  findings — a findings header line — either `Full review comments:` (multi-finding form)
             or `Review comment:` (singular form codex emits for a single finding) — then
             entries of the form
             `- [P1] Title text — /abs/or/rel/path.py:13-13` (line range `NN` or `NN-MM`),
             each followed by indented body lines until the next `- [P` entry or the end.
             The parser accepts EITHER header exactly; the first line matching either is
             the header.
  clean    — a prose final message with NO findings header (an empty diff prints e.g.
             "The diff against the requested base is empty…"; a reviewed-clean diff is
             prose too).

stdout and stderr are captured separately: only stdout is parsed, and the evidence file is
stdout followed by a `--- stderr ---` delimited section (when stderr is non-empty), so a
stderr marker or trailing stderr can neither create a phantom finding nor blind the
no-header drift guard.

Format-drift guard (fail fast rather than report a false `clean`): a findings header
present but zero findings parse, a header whose parsed `- [P` line count disagrees with
the findings list (a separator variant silently absorbed), OR the header absent but a
`- [P` line appears after the last `codex` message marker — all exit 2 with kind
`format_drift`.

Output: one JSON object on stdout. On success —
  {"head", "base", "merge_base", "verdict": "clean"|"findings",
   "findings": [{"priority", "title", "path", "line_start", "line_end", "body"}],
   "output_path", "duration_s"}
On an exit-2 failure a machine-readable error object is still emitted to stdout —
  {"verdict": "error",
   "error": {"kind": "usage_limit"|"timeout"|"format_drift"|"precondition"|"tool_failure",
             "message": <one line>},
   "output_path": <str or null>, "head"?, "base"?, "merge_base"?}
(head/base/merge_base included only when already resolved). The one-line message is on
stderr too. Exit codes mirror the verdict: **0** clean · **1** findings · **2** error.
Only kind `usage_limit` is the merge-gate's exhausted-analog (recordable, not a blocker);
every other exit-2 kind is a blocker. A no-op review — a transcript with no successful exec
marker on STDERR — is a `tool_failure`, so it blocks rather than passing as a false `clean`;
a nested-sandbox denial (`sandbox_apply: Operation not permitted`, #1049) is one such case
and selects the more actionable message, but the denial string is not an independent blocker
(it is PR-controllable, so a legitimate review that quotes it must not fire the guard).

Stdlib only, no `gh` **API** access (unlike the sibling scripts) — it works purely off the
local git worktree and the codex CLI.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

# The codex CLI has no structured review mode (`codex exec` has --json; `codex review` does
# not, as of codex-cli 0.142.5), so the verdict is read from the prose transcript.
# simplify: prose-transcript parsing pinned to codex-cli 0.142.5 output (review has no
# structured mode; codex exec already has --json) — switch off the regex contract when
# codex review grows one.

# A finding line: `- [P1] Title — /path.py:13` or `- [P2] Title — path.py:13-20`. The
# title/path split is on the LAST em dash so a title containing " — " keeps its text and
# the trailing `path:line[-line]` is the path. Codex emits an em dash (— U+2014) here.
FINDING_RE = re.compile(r"^- \[(P\d)\] (.+) — (.+?):(\d+)(?:-(\d+))?$")
# The header(s) that precede the findings block in codex's final message. codex-cli 0.142.5
# emits `Full review comments:` for multiple findings and the singular `Review comment:` for
# exactly one — accept either (first line matching either is the header).
FINDINGS_HEADERS = ("Full review comments:", "Review comment:")
# codex prints a bare `codex` line as the marker before each of its own messages; the final
# message (verdict) is the tail after the LAST such marker.
CODEX_MARKER = "codex"
# Delimiter separating captured stdout from stderr in the evidence transcript, so a stderr
# marker or trailing text can neither create a phantom finding nor blind the drift guard.
STDERR_DELIMITER = "\n--- stderr ---\n"
# codex spawns sandboxed grandchildren that inherit the pipes, so a plain wait after a
# timeout can block forever on their still-open fds. TIMEOUT_S bounds the whole review;
# it outlasts the foreground Bash cap, so callers launch this script in the background.
TIMEOUT_S = 30 * 60
# codex CLI exits 0 even on findings, but a real failure (auth, usage limit) exits nonzero
# and prints this phrase on STDERR; classify it so the merge gate can record it as the
# exhausted-analog. The full phrase (not a loose "usage limit" substring) and the stderr-only
# scope are load-bearing: PR-controlled transcript content (codex quoting rate-limiting code
# out of the diff) lands on stdout, and a loose substring there could flip a hard-blocker
# tool_failure into a merge-passable usage_limit.
USAGE_LIMIT_MARKER = "reached your codex usage limits"
# codex emits a ` succeeded in <N>ms:` line on STDERR after each exec it runs successfully
# (timing unit is ms/s/m with an optional decimal on longer commands). A genuine review ALWAYS
# runs at least the initial `git diff --stat/--name-status` and that exec succeeds — even an
# empty-diff clean run runs and succeeds it — so the ABSENCE of any success marker on STDERR
# means codex ran no exec at all and reviewed nothing (a false clean). This absence is the
# SINGLE no-op-review gate in run_codex: when it holds, the review inspected nothing and we
# fail closed; when it does NOT hold, codex reviewed the diff and any `sandbox_apply`/prose
# text elsewhere is PR content, not a failure.
EXEC_SUCCESS_RE = re.compile(r"succeeded in \d+(?:\.\d+)?(?:ms|s|m)\b")
# When codex can't spawn its sandbox (the nested Codex/Claude sandbox failure class, #1049),
# every exec — including the initial `git diff` — fails with this denial on STDERR and codex
# still exits 0 with a prose "I could not inspect the patch" message. This distinctive
# substring (binary-path variants of `sandbox-exec:` still match) is the specific signal.
# NOT an independent blocker: the string is PR-controllable (this PR's source, tests, and
# docs all quote it) and codex echoes the reviewed diff on stderr, so scanning for it as a
# blocker would false-fire on a legitimate review that merely mentions it. It is consulted
# ONLY on STDERR, and ONLY once the exec-success absence has already decided to fail closed
# (see run_codex), to pick the more actionable nested-sandbox message over the generic one.
SANDBOX_DENIED_MARKER = "sandbox_apply: Operation not permitted"

# Error kinds carried on PreconditionError and echoed into the JSON error object.
KIND_PRECONDITION = "precondition"
KIND_TOOL_FAILURE = "tool_failure"
KIND_USAGE_LIMIT = "usage_limit"
KIND_TIMEOUT = "timeout"
KIND_FORMAT_DRIFT = "format_drift"


class PreconditionError(Exception):
    """A precondition/tool/parse failure — mapped to exit 2 with a classified one-line kind.

    `kind` names which failure class this is (precondition / tool_failure / usage_limit /
    timeout / format_drift); only `usage_limit` is the merge gate's exhausted-analog, so
    the kind rides into the JSON error object consumers read.
    """

    def __init__(self, message: str, *, kind: str = KIND_PRECONDITION) -> None:
        super().__init__(message)
        self.kind = kind
        # Filled in by review() once the refs / evidence path are known, so an error object
        # emitted after a codex/parse failure carries head/base/merge_base and the transcript
        # path (timeout / tool_failure / format_drift all wrote evidence); None until then.
        self.head: str | None = None
        self.base: str | None = None
        self.merge_base: str | None = None
        self.output_path: str | None = None


def _scrubbed_git_env() -> dict[str, str]:
    # Drop GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE: the pre-push hook exports GIT_DIR and
    # hijacks child git, pointing it at the real repo instead of the target worktree.
    return {
        k: v
        for k, v in os.environ.items()
        if k not in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")
    }


def _git(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a git command in `cwd` with the GIT_* hijack env scrubbed.

    A missing git binary maps to a kind=precondition PreconditionError rather than a
    traceback. (This can't route through `run_tolerant` — that primitive takes no env
    override, and the scrub is load-bearing here.)
    """
    try:
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            env=_scrubbed_git_env(),
        )
    except FileNotFoundError as exc:
        raise PreconditionError(
            f"git binary not found on PATH: {exc}", kind=KIND_PRECONDITION
        ) from exc


def check_preconditions(base: str, *, cwd: Path) -> tuple[str, str]:
    """Verify codex is on PATH and the worktree is clean; resolve (head, merge_base).

    Raises PreconditionError (→ exit 2) with an actionable message on any failure:
    codex missing, not a git work tree, dirty tracked worktree, or an unresolvable base.
    Untracked files are fine — they're invisible to the committed diff codex reviews.
    """
    if shutil.which("codex") is None:
        raise PreconditionError("codex CLI not found on PATH")
    inside = _git(["rev-parse", "--is-inside-work-tree"], cwd=cwd)
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        raise PreconditionError(f"not inside a git work tree: {cwd}")
    status = _git(["status", "--porcelain", "--untracked-files=no"], cwd=cwd)
    if status.returncode != 0:
        raise PreconditionError(status.stderr.strip() or "failed to read git status")
    if status.stdout.strip():
        raise PreconditionError(
            "tracked worktree is dirty; commit or stash before reviewing "
            "(untracked files are fine)"
        )
    head = _git(["rev-parse", "HEAD"], cwd=cwd)
    if head.returncode != 0:
        raise PreconditionError(head.stderr.strip() or "failed to resolve HEAD")
    # An unresolvable base is a hard error — no fallback: a typo (or an unfetched
    # origin/main) must not silently review against the wrong base (e.g. a stale local
    # `main`), the exact silent-wrong-base failure this guard exists to prevent.
    if _git(["rev-parse", "--verify", "--quiet", base], cwd=cwd).returncode != 0:
        raise PreconditionError(f"base ref {base!r} does not resolve in this worktree")
    merge_base = _git(["merge-base", "HEAD", base], cwd=cwd)
    if merge_base.returncode != 0 or not merge_base.stdout.strip():
        raise PreconditionError(f"failed to resolve merge-base of HEAD and {base!r}")
    return head.stdout.strip(), merge_base.stdout.strip()


def _as_text(value: str | bytes | None) -> str:
    """Coerce a `TimeoutExpired.stdout/.stderr` value to str (empty for None).

    The pipes run in text mode (`text=True`), so the buffered output CPython attaches to a
    `TimeoutExpired` is already `str`; the bytes branch (statically typed but never taken
    here) is decoded defensively rather than stringified into a `b'…'` literal.
    """
    if value is None:
        return ""
    return value if isinstance(value, str) else value.decode("utf-8", "replace")


def _write_evidence(out_path: Path, stdout: str, stderr: str) -> None:
    """Write the evidence transcript: stdout, then a delimited stderr section if non-empty.

    The streams are kept apart in the file (and only stdout is parsed) so a stderr `codex`
    marker or trailing stderr text can neither create a phantom finding nor blind the
    no-header drift guard — both were verified bugs of the old combined-stream capture.
    """
    parts = [stdout]
    if stderr.strip():
        parts.append(STDERR_DELIMITER)
        parts.append(stderr)
    out_path.write_text("".join(parts), encoding="utf-8")


def run_codex(merge_base: str, out_path: Path, *, cwd: Path, timeout_s: float) -> str:
    """Run `codex review` against `merge_base`, returning codex's STDOUT (the parsed stream).

    `--base` takes the full merge-base SHA and is mutually exclusive with a positional
    prompt (verified), so none is passed. The evidence transcript (stdout + a delimited
    stderr section) is written to out_path; only stdout is returned for parsing.

    Fail-closed on anything other than a clean codex run:
      - nonzero exit → PreconditionError, kind usage_limit ONLY when the exact codex
        usage-limit phrase appears on STDERR, else tool_failure (fail-closed: PR-controlled
        stdout can't downgrade a blocker; a plain exit-0-parses-clean would mask the error);
      - exit 0 with empty/whitespace-only stdout → tool_failure ("no transcript");
      - timeout → the process group is SIGKILLed (codex spawns sandboxed grandchildren that
        inherit the pipes; killing only the direct child leaves communicate() blocked
        forever — the repo learned this in cos_watch), the partial transcript is written as
        gate evidence for diagnosing the hang, and kind timeout is raised;
      - a no-op review (exit 0, non-empty prose) where codex reviewed NOTHING → tool_failure.
        The SINGLE gate is the ABSENCE of any `succeeded in …` exec-success marker on STDERR
        (the exec markers land on stderr, which parse_transcript never sees; a genuine review
        always runs the initial `git diff` successfully, so no marker means nothing was
        reviewed). Scoped to stderr only so PR-controlled/prose stdout can't downgrade it to a
        false clean. The SANDBOX_DENIED_MARKER (#1049) is NOT an independent blocker — the
        string is PR-controllable (this PR quotes it) and codex echoes the reviewed diff on
        stderr, so it is consulted ONLY inside this fail-closed branch, on STDERR, to pick the
        actionable nested-sandbox message over the generic backstop. Fail-closed so a review
        that inspected nothing can't land as `clean`.
    codex's exit code is NOT used for the clean/findings split — it exits 0 even with findings.
    A missing codex binary is caught by the shutil.which precondition, so it is not re-guarded
    here.
    """
    cmd = [
        "codex",
        "review",
        "-c",
        'sandbox_mode="read-only"',
        "--base",
        merge_base,
    ]
    # start_new_session puts codex in its own process group so a timeout can killpg the
    # whole tree (grandchildren inherit the pipes and would otherwise block communicate()).
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_scrubbed_git_env(),
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        # The child may have exited (and its group vanished) between the communicate timeout
        # and this kill; ProcessLookupError then just means there's nothing left to kill.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        try:
            stdout, stderr = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired as drain_exc:
            # A setsid-escaped grandchild can still hold the pipes past the drain. Don't
            # overwrite with empty strings — CPython accumulates the buffered output on the
            # TimeoutExpired, so salvage it and land the partial transcript as evidence.
            # (TimeoutExpired.stdout/.stderr are typed str|bytes|None; text=True means str,
            # so _as_text normalizes the None/bytes branches the type checker still sees.)
            stdout, stderr = _as_text(drain_exc.stdout), _as_text(drain_exc.stderr)
        _write_evidence(out_path, stdout or "", stderr or "")
        raise PreconditionError(
            f"codex review timed out after {timeout_s / 60:.0f} min",
            kind=KIND_TIMEOUT,
        ) from None

    stdout, stderr = stdout or "", stderr or ""
    _write_evidence(out_path, stdout, stderr)
    if proc.returncode != 0:
        # Fail-closed: classify usage_limit ONLY on the exact codex phrase in STDERR.
        # Scoping to stderr (never stdout) means a mis-routed usage-limit message on stdout
        # blocks (safe), while PR-controlled stdout content can never downgrade a
        # hard-blocker tool_failure into a merge-passable usage_limit (the reverse, unsafe).
        kind = (
            KIND_USAGE_LIMIT
            if USAGE_LIMIT_MARKER in stderr.lower()
            else KIND_TOOL_FAILURE
        )
        detail = "usage limit reached" if kind == KIND_USAGE_LIMIT else "tool failure"
        raise PreconditionError(
            f"codex review exited {proc.returncode} ({detail}); see {out_path}",
            kind=kind,
        )
    if not stdout.strip():
        raise PreconditionError(
            f"codex produced no transcript; see {out_path}", kind=KIND_TOOL_FAILURE
        )
    # No-op-review gate. A review that ran no successful exec inspected nothing → fail closed.
    # The `succeeded in <N>ms` exec-success markers live on codex's STDERR (all 58 in the real
    # PR #1078 transcript were there); their ABSENCE on stderr is the single gate. When codex
    # DID exec successfully it reviewed the diff, so any `sandbox_apply`/prose text in stdout or
    # the echoed diff is PR CONTENT (this PR itself documents/tests that exact string) — we must
    # NOT fire on it. That is why the denial marker is consulted ONLY inside this branch (on
    # stderr, to pick the message), never as an independent blocker over PR-controlled content.
    if not EXEC_SUCCESS_RE.search(stderr):
        # Nothing ran. Prefer the actionable nested-sandbox message (#1049) when codex
        # couldn't spawn its seatbelt (the denial lands on stderr); else the generic backstop.
        if SANDBOX_DENIED_MARKER in stderr:
            raise PreconditionError(
                "codex could not spawn its sandbox here (sandbox_apply: Operation not "
                "permitted) — every exec failed, so nothing was reviewed; re-run the launcher "
                f"outside the agent sandbox / with escalated permissions; see {out_path}",
                kind=KIND_TOOL_FAILURE,
            )
        raise PreconditionError(
            "codex ran no successful exec (no `git diff` succeeded), so the review "
            f"inspected nothing — treat as a failed review, not clean; see {out_path}",
            kind=KIND_TOOL_FAILURE,
        )
    return stdout


def _normalize_path(path: str, *, worktree_root: Path) -> str:
    """Repo-relative path: strip the worktree-root prefix when codex emitted an abs path.

    Both the candidate and the root are `resolve()`d before `relative_to` so a symlinked
    worktree (macOS /tmp → /private/tmp in throwaway worktrees) doesn't leak a machine-
    absolute path; the raw path is the fallback when it lies outside the root.
    """
    raw = path.strip()
    candidate = Path(raw)
    if candidate.is_absolute():
        try:
            return str(candidate.resolve().relative_to(worktree_root.resolve()))
        except ValueError:
            return raw
    return raw


def parse_transcript(transcript: str, *, worktree_root: Path) -> dict[str, Any]:
    """Parse codex's transcript into a verdict + findings list. Pure — the tested core.

    Returns {"verdict": "clean"|"findings", "findings": [...]}. Raises PreconditionError
    (kind format_drift → exit 2) when the transcript's header/finding shape is inconsistent:
    a findings header (`Full review comments:` or the singular `Review comment:`) with zero
    parsable findings, a header whose count of `- [P` lines disagrees with the parsed
    findings list (a separator variant — en dash, plain hyphen — silently absorbed into the
    previous finding's body and dropped), or a `- [P` line after the last `codex` marker with
    no header. Each would otherwise let a false `clean` (or a dropped finding) through.
    """
    lines = transcript.splitlines()
    # The first line matching EITHER accepted header is the findings header (if both ever
    # appear, first wins — treated equivalently).
    header_idx = next(
        (i for i, line in enumerate(lines) if line.strip() in FINDINGS_HEADERS), None
    )

    findings: list[dict[str, Any]] = []
    if header_idx is not None:
        region = lines[header_idx + 1 :]
        current: dict[str, Any] | None = None
        # Body lines accumulate in this list and are joined once when the finding closes;
        # each piece is already `line.strip()`ed, so edge whitespace is impossible and no
        # post-parse trim is needed.
        body_lines: list[str] = []

        def _close(finding: dict[str, Any] | None) -> None:
            if finding is not None:
                finding["body"] = "\n".join(body_lines)

        for line in region:
            match = FINDING_RE.match(line.rstrip())
            if match:
                _close(current)
                body_lines = []
                priority, title, path, start, end = match.groups()
                current = {
                    "priority": priority,
                    "title": title.strip(),
                    "path": _normalize_path(path, worktree_root=worktree_root),
                    "line_start": int(start),
                    "line_end": int(end) if end else int(start),
                    "body": "",
                }
                findings.append(current)
            elif current is not None and line.strip():
                # Indented continuation line: attach to the current finding's body.
                body_lines.append(line.strip())
        _close(current)
        if not findings:
            raise PreconditionError(
                "format drift: findings header present but no findings parsed — inspect "
                "the transcript",
                kind=KIND_FORMAT_DRIFT,
            )
        # Every `- [P…` line under the header must have parsed as a finding. A count
        # mismatch means a separator variant (en dash, plain hyphen) was silently absorbed
        # into the previous body and dropped — fail fast rather than under-report.
        # Count only UNINDENTED `- [P` lines (no lstrip), matching FINDING_RE's `^` anchor:
        # codex emits finding entries at column 0 and their bodies indented, so an indented
        # body sub-bullet (`  - [Possible fix]`) is a body line, not a dropped finding.
        bullet_count = sum(1 for line in region if line.startswith("- [P"))
        if bullet_count != len(findings):
            raise PreconditionError(
                f"format drift: {bullet_count} '- [P…]' finding line(s) under the header "
                f"but only {len(findings)} parsed — a separator variant was dropped; "
                "inspect the transcript",
                kind=KIND_FORMAT_DRIFT,
            )
        return {"verdict": "findings", "findings": findings}

    # No header → clean, UNLESS a stray finding line appears after the last codex marker
    # (format drift: a findings-shaped line with no header must not read as clean).
    last_marker = max(
        (i for i, line in enumerate(lines) if line.strip() == CODEX_MARKER),
        default=-1,
    )
    for line in lines[last_marker + 1 :]:
        if line.lstrip().startswith("- [P"):
            raise PreconditionError(
                "format drift: a '- [P…]' finding line appears with no findings header "
                "('Full review comments:' or 'Review comment:') — inspect the transcript",
                kind=KIND_FORMAT_DRIFT,
            )
    return {"verdict": "clean", "findings": []}


def review(
    *, base: str, out_path: Path | None, cwd: Path, timeout_s: float
) -> dict[str, Any]:
    """Run the full local review: preconditions → codex → parse. Returns the result dict.

    `out_path` is where the transcript is written; pass None to lazily create a temp file
    only AFTER preconditions pass (so a dirty-worktree / missing-codex failure doesn't
    orphan an empty temp file, and COS retries across ticks don't accumulate litter). When
    given, its parent dir is created. Any PreconditionError raised after the refs resolve
    carries head/base/merge_base for the error JSON.
    """
    head, merge_base = check_preconditions(base, cwd=cwd)
    resolved = out_path if out_path is not None else _default_out_path()
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    try:
        transcript = run_codex(merge_base, resolved, cwd=cwd, timeout_s=timeout_s)
        parsed = parse_transcript(transcript, worktree_root=cwd)
    except PreconditionError as exc:
        exc.head, exc.base, exc.merge_base = head, base, merge_base
        exc.output_path = str(resolved)
        raise
    return {
        "head": head,
        "base": base,
        "merge_base": merge_base,
        "verdict": parsed["verdict"],
        "findings": parsed["findings"],
        "output_path": str(resolved),
        "duration_s": round(time.monotonic() - started, 1),
    }


def _default_out_path() -> Path:
    # A named temp file in the system temp dir, kept after close so the transcript survives
    # for the gate dir; its path is reported in the JSON. Created lazily (only after
    # preconditions pass) so failing preconditions never orphan an empty temp file.
    with NamedTemporaryFile(
        "w", prefix="codex-review-", suffix=".md", delete=False, encoding="utf-8"
    ) as fh:
        return Path(fh.name)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Run `codex review` locally against the PR's committed HEAD and report "
        "the verdict as JSON (exit 0 clean / 1 findings / 2 error). Run from the PR "
        "worktree; no GitHub access."
    )
    ap.add_argument(
        "--base",
        required=True,
        help="base ref the PR targets (e.g. origin/main, or the predecessor branch for a "
        "stacked PR) — required so merge-gate evidence can never be produced against the "
        "wrong base; any unresolvable base is a hard error (no fallback)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="where to write the full codex transcript (default: a temp file created only "
        "after preconditions pass); the path is always reported in the JSON. Point it at "
        "the merge-gate dir (e.g. --out <gate-dir>/codex-review.md) so evidence lands there",
    )
    args = ap.parse_args(argv)

    cwd = Path.cwd()
    try:
        result = review(
            base=args.base,
            out_path=args.out,
            cwd=cwd,
            timeout_s=TIMEOUT_S,
        )
    except Exception as exc:  # noqa: BLE001
        # Every failure maps to the exit-2 error contract with a machine-readable JSON
        # object on stdout (and the one-line message on stderr). A PreconditionError
        # carries its classified `kind` and, once review() resolved the refs, the
        # head/base/merge_base + transcript path; any other uncaught error (e.g. an OSError
        # from an unwritable --out parent) is kind tool_failure with none of those. Mapping
        # the bare-Exception case here matters because Python's default exit 1 is what the
        # contract reserves for `findings` — a false clean-ish signal. KeyboardInterrupt /
        # SystemExit are BaseException, not Exception, so they still propagate.
        print(str(exc), file=sys.stderr)
        kind = exc.kind if isinstance(exc, PreconditionError) else KIND_TOOL_FAILURE
        error: dict[str, Any] = {
            "verdict": "error",
            "error": {"kind": kind, "message": str(exc)},
            "output_path": getattr(exc, "output_path", None),
        }
        head = getattr(exc, "head", None)
        if head is not None:
            error["head"] = head
            error["base"] = getattr(exc, "base", None)
            error["merge_base"] = getattr(exc, "merge_base", None)
        print(json.dumps(error, indent=2))
        return 2

    print(json.dumps(result, indent=2))
    return 0 if result["verdict"] == "clean" else 1


if __name__ == "__main__":
    raise SystemExit(main())
