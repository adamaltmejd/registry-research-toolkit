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
`codex review -c sandbox_mode="read-only" --base <merge_base_sha>`, capturing the combined
stdout+stderr transcript to `--out`. `--base` accepts a full SHA and is mutually exclusive
with a positional prompt, so no prompt is ever passed.

The **codex CLI itself exits 0 even when it finds issues** — the transcript text is the
only signal, so this script parses the transcript, not codex's exit code. A codex
usage-limit / tool failure surfaces here as **exit 2**; per the merge gate that is an
end-of-wait condition to RECORD on the gate line (like the old `exhausted`), not something
to silently retry.

Transcript shapes (verified live, codex-cli 0.142.5):
  findings — a `Full review comments:` line, then entries of the form
             `- [P1] Title text — /abs/or/rel/path.py:13-13` (line range `NN` or `NN-MM`),
             each followed by indented body lines until the next `- [P` entry or the end.
  clean    — a prose final message with NO `Full review comments:` header (an empty diff
             prints e.g. "The diff against the requested base is empty…"; a reviewed-clean
             diff is prose too).

Format-drift guard (fail fast rather than report a false `clean`): a `Full review
comments:` header present but zero findings parse, OR the header absent but a `- [P` line
appears after the last `codex` message marker, exits 2.

Output: one JSON object on stdout —
  {"head", "base", "merge_base", "verdict": "clean"|"findings",
   "findings": [{"priority", "title", "path", "line_start", "line_end", "body"}],
   "output_path", "duration_s"}
Exit codes mirror the verdict: **0** clean · **1** findings · **2** tool/precondition/parse
error. The JSON is the full signal.

Stdlib only, no `gh` access at all (unlike the sibling scripts) — it works purely off the
local git worktree and the codex CLI.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

# A finding line: `- [P1] Title — /path.py:13` or `- [P2] Title — path.py:13-20`. The
# title/path split is on the LAST em dash so a title containing " — " keeps its text and
# the trailing `path:line[-line]` is the path. Codex emits an em dash (— U+2014) here.
FINDING_RE = re.compile(r"^- \[(P\d)\] (.+) — (.+?):(\d+)(?:-(\d+))?$")
# The header that precedes the findings block in codex's final message.
FINDINGS_HEADER = "Full review comments:"
# codex prints a bare `codex` line as the marker before each of its own messages; the final
# message (verdict) is the tail after the LAST such marker.
CODEX_MARKER = "codex"


class PreconditionError(Exception):
    """A precondition/tool/parse failure — mapped to exit 2 with a one-line message."""


def _git(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )


def _resolve_base(base: str, *, cwd: Path) -> str:
    """The base ref to diff against, falling back main when origin/main doesn't resolve.

    Only the default `origin/main` falls back to `main`; an explicitly passed base that
    doesn't resolve is a hard error (a typo shouldn't silently review against main).
    """
    if _git(["rev-parse", "--verify", "--quiet", base], cwd=cwd).returncode == 0:
        return base
    if base == "origin/main" and (
        _git(["rev-parse", "--verify", "--quiet", "main"], cwd=cwd).returncode == 0
    ):
        return "main"
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


def run_codex(merge_base: str, out_path: Path, *, cwd: Path, timeout_s: float) -> str:
    """Run `codex review` against `merge_base`, capturing the transcript to `out_path`.

    `--base` takes the full merge-base SHA and is mutually exclusive with a positional
    prompt (verified), so none is passed. Combined stdout+stderr is written to out_path
    and returned. A timeout kills codex and raises PreconditionError (→ exit 2); so does a
    missing codex binary. codex's own exit code is ignored — it exits 0 even with findings.
    """
    cmd = [
        "codex",
        "review",
        "-c",
        'sandbox_mode="read-only"',
        "--base",
        merge_base,
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise PreconditionError(
            f"codex review timed out after {timeout_s / 60:.0f} min"
        ) from None
    except FileNotFoundError:
        raise PreconditionError("codex CLI not found on PATH") from None
    transcript = (proc.stdout or "") + (proc.stderr or "")
    out_path.write_text(transcript, encoding="utf-8")
    return transcript


def _normalize_path(path: str, *, worktree_root: Path) -> str:
    """Repo-relative path: strip the worktree-root prefix when codex emitted an abs path."""
    raw = path.strip()
    candidate = Path(raw)
    if candidate.is_absolute():
        try:
            return str(candidate.relative_to(worktree_root))
        except ValueError:
            return raw
    return raw


def parse_transcript(transcript: str, *, worktree_root: Path) -> dict[str, Any]:
    """Parse codex's transcript into a verdict + findings list. Pure — the tested core.

    Returns {"verdict": "clean"|"findings", "findings": [...]}. Raises PreconditionError
    (→ exit 2, format drift) when the transcript's header/finding shape is inconsistent:
    a `Full review comments:` header with zero parsable findings, or a `- [P` line after
    the last `codex` marker with no header. Both would otherwise let a false `clean` through.
    """
    lines = transcript.splitlines()
    header_idx = next(
        (i for i, line in enumerate(lines) if line.strip() == FINDINGS_HEADER), None
    )

    findings: list[dict[str, Any]] = []
    if header_idx is not None:
        current: dict[str, Any] | None = None
        for line in lines[header_idx + 1 :]:
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
                "parsed — inspect the transcript"
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
                f"'{FINDINGS_HEADER}' header — inspect the transcript"
            )
    return {"verdict": "clean", "findings": []}


def review(*, base: str, out_path: Path, cwd: Path, timeout_s: float) -> dict[str, Any]:
    """Run the full local review: preconditions → codex → parse. Returns the result dict."""
    head, merge_base = check_preconditions(base, cwd=cwd)
    started = time.monotonic()
    transcript = run_codex(merge_base, out_path, cwd=cwd, timeout_s=timeout_s)
    parsed = parse_transcript(transcript, worktree_root=cwd)
    return {
        "head": head,
        "base": base,
        "merge_base": merge_base,
        "verdict": parsed["verdict"],
        "findings": parsed["findings"],
        "output_path": str(out_path),
        "duration_s": round(time.monotonic() - started, 1),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Run `codex review` locally against the PR's committed HEAD and report "
        "the verdict as JSON (exit 0 clean / 1 findings / 2 tool/precondition/parse error). "
        "Run from the PR worktree; no GitHub access."
    )
    ap.add_argument(
        "--base",
        default="origin/main",
        help="base ref to diff against (default origin/main; falls back to main when "
        "origin/main doesn't resolve)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="where to write the full codex transcript (default: a temp file); the path "
        "is always reported in the JSON",
    )
    ap.add_argument(
        "--timeout-min",
        type=float,
        default=30.0,
        help="kill codex after this many minutes and exit 2 (default 30)",
    )
    args = ap.parse_args(argv)

    cwd = Path.cwd()
    if args.out is not None:
        out_path = args.out
    else:
        # A named temp file in the system temp dir, kept after close so the transcript
        # survives for the gate dir; its path is reported in the JSON.
        with NamedTemporaryFile(
            "w",
            prefix="codex-review-",
            suffix=".md",
            delete=False,
            encoding="utf-8",
        ) as fh:
            out_path = Path(fh.name)

    try:
        result = review(
            base=args.base,
            out_path=out_path,
            cwd=cwd,
            timeout_s=args.timeout_min * 60,
        )
    except PreconditionError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2))
    return 0 if result["verdict"] == "clean" else 1


if __name__ == "__main__":
    raise SystemExit(main())
