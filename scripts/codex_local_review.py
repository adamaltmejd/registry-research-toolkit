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
transcript to `--out`. `--base` accepts a full SHA and is mutually exclusive with a
positional prompt, so no prompt is ever passed.

The **codex CLI itself exits 0 even when it finds issues** — the transcript text is the
only signal for the clean/findings split, so this script parses codex's stdout, not its
exit code. But a NONZERO codex exit, an empty transcript, a timeout, or a format-drifted
transcript is a hard failure surfaced here as **exit 2** with a classified error — never a
silent false `clean`.

Transcript shapes (verified live, codex-cli 0.142.5):
  findings — a `Full review comments:` line, then entries of the form
             `- [P1] Title text — /abs/or/rel/path.py:13-13` (line range `NN` or `NN-MM`),
             each followed by indented body lines until the next `- [P` entry or the end.
  clean    — a prose final message with NO `Full review comments:` header (an empty diff
             prints e.g. "The diff against the requested base is empty…"; a reviewed-clean
             diff is prose too).

stdout and stderr are captured separately: only stdout is parsed, and the evidence file is
stdout followed by a `--- stderr ---` delimited section (when stderr is non-empty), so a
stderr marker or trailing stderr can neither create a phantom finding nor blind the
no-header drift guard.

Format-drift guard (fail fast rather than report a false `clean`): a `Full review
comments:` header present but zero findings parse, a header whose parsed `- [P` line count
disagrees with the findings list (a separator variant silently absorbed), OR the header
absent but a `- [P` line appears after the last `codex` message marker — all exit 2 with
kind `format_drift`.

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
every other exit-2 kind is a blocker.

Stdlib only, no `gh` **API** access (unlike the sibling scripts) — it works purely off the
local git worktree and the codex CLI.
"""

from __future__ import annotations

import argparse
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
# The header that precedes the findings block in codex's final message.
FINDINGS_HEADER = "Full review comments:"
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
# and prints this phrase; classify it so the merge gate can record it as the exhausted-analog.
USAGE_LIMIT_MARKER = "usage limit"

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


def _resolve_base(base: str, *, cwd: Path) -> str:
    """The base ref to diff against; any unresolvable base is a hard error.

    No fallback: silently reviewing against a stale local `main` is the exact silent-wrong-
    base failure this guard exists to prevent, so an unresolvable base (typo, or origin/main
    not fetched) fails fast rather than picking a different ref.
    """
    if _git(["rev-parse", "--verify", "--quiet", base], cwd=cwd).returncode == 0:
        return base
    raise PreconditionError(f"base ref {base!r} does not resolve in this worktree")


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
    resolved_base = _resolve_base(base, cwd=cwd)
    merge_base = _git(["merge-base", "HEAD", resolved_base], cwd=cwd)
    if merge_base.returncode != 0 or not merge_base.stdout.strip():
        raise PreconditionError(
            f"failed to resolve merge-base of HEAD and {resolved_base!r}"
        )
    return head.stdout.strip(), merge_base.stdout.strip()


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
      - nonzero exit → PreconditionError, kind usage_limit if the transcript names a usage
        limit else tool_failure (a plain exit-0-parses-clean would mask an auth/limit error);
      - exit 0 with empty/whitespace-only stdout → tool_failure ("no transcript");
      - timeout → the process group is SIGKILLed (codex spawns sandboxed grandchildren that
        inherit the pipes; killing only the direct child leaves communicate() blocked
        forever — the repo learned this in cos_watch), the partial transcript is written as
        gate evidence for diagnosing the hang, and kind timeout is raised.
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
        os.killpg(proc.pid, signal.SIGKILL)
        try:
            stdout, stderr = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        _write_evidence(out_path, stdout or "", stderr or "")
        raise PreconditionError(
            f"codex review timed out after {timeout_s / 60:.0f} min",
            kind=KIND_TIMEOUT,
        ) from None

    stdout, stderr = stdout or "", stderr or ""
    _write_evidence(out_path, stdout, stderr)
    if proc.returncode != 0:
        combined = f"{stdout}\n{stderr}".lower()
        kind = KIND_USAGE_LIMIT if USAGE_LIMIT_MARKER in combined else KIND_TOOL_FAILURE
        detail = "usage limit reached" if kind == KIND_USAGE_LIMIT else "tool failure"
        raise PreconditionError(
            f"codex review exited {proc.returncode} ({detail}); see {out_path}",
            kind=kind,
        )
    if not stdout.strip():
        raise PreconditionError(
            f"codex produced no transcript; see {out_path}", kind=KIND_TOOL_FAILURE
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
    a `Full review comments:` header with zero parsable findings, a header whose count of
    `- [P` lines disagrees with the parsed findings list (a separator variant — en dash,
    plain hyphen — silently absorbed into the previous finding's body and dropped), or a
    `- [P` line after the last `codex` marker with no header. Each would otherwise let a
    false `clean` (or a dropped finding) through.
    """
    lines = transcript.splitlines()
    header_idx = next(
        (i for i, line in enumerate(lines) if line.strip() == FINDINGS_HEADER), None
    )

    findings: list[dict[str, Any]] = []
    if header_idx is not None:
        region = lines[header_idx + 1 :]
        current: dict[str, Any] | None = None
        for line in region:
            match = FINDING_RE.match(line.rstrip())
            if match:
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
                current["body"] = (
                    f"{current['body']}\n{line.strip()}"
                    if current["body"]
                    else line.strip()
                )
        for finding in findings:
            finding["body"] = finding["body"].strip()
        if not findings:
            raise PreconditionError(
                "format drift: 'Full review comments:' header present but no findings "
                "parsed — inspect the transcript",
                kind=KIND_FORMAT_DRIFT,
            )
        # Every `- [P…` line under the header must have parsed as a finding. A count
        # mismatch means a separator variant (en dash, plain hyphen) was silently absorbed
        # into the previous body and dropped — fail fast rather than under-report.
        bullet_count = sum(1 for line in region if line.lstrip().startswith("- [P"))
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
                "format drift: a '- [P…]' finding line appears with no "
                f"'{FINDINGS_HEADER}' header — inspect the transcript",
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
        default="origin/main",
        help="base ref to diff against (default origin/main); any unresolvable base is a "
        "hard error — no fallback (a stale local main would be a silent-wrong-base review)",
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
    except PreconditionError as exc:
        # Still emit a machine-readable error object to stdout so consumers get a signal;
        # the one-line message goes to stderr too. head/base/merge_base ride along only
        # when review() already resolved them (a post-precondition codex/parse failure).
        print(str(exc), file=sys.stderr)
        error: dict[str, Any] = {
            "verdict": "error",
            "error": {"kind": exc.kind, "message": str(exc)},
            "output_path": exc.output_path,
        }
        if exc.head is not None:
            error["head"] = exc.head
            error["base"] = exc.base
            error["merge_base"] = exc.merge_base
        print(json.dumps(error, indent=2))
        return 2

    print(json.dumps(result, indent=2))
    return 0 if result["verdict"] == "clean" else 1


if __name__ == "__main__":
    raise SystemExit(main())
