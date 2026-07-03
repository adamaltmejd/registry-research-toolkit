"""Enforcement: no skill ingests raw untrusted issue text into a model.

This repo is public, so a stranger's issue/comment is untrusted. The automation reads
issue content only through the maintainer-author trust gate (`scripts/gh_issue.py`). This
test greps every `*.md` under the two mirrored skill trees (`.claude/skills/`,
`.agents/skills/`) and FAILS if any line re-introduces a raw model-read ingestion vector:

  - `gh issue view` — the body/comment ingestion vehicle (use `gh_issue.py view`);
  - `gh issue list --state open` — work-set enumeration (use `gh_issue.py`).

Allowlisted (NOT ingestion of untrusted body text, so they must not trip): the
dedupe-before-filing `gh issue list ... --search`, the write/mutation subcommands
(`gh issue edit/create/comment`), any `gh pr ...` (PRs are gated separately by the
fork-never-automerge rule), and the `gh_issue.py` replacement line itself.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SKILL_DIRS = [_ROOT / ".claude" / "skills", _ROOT / ".agents" / "skills"]

# The gated replacement — a line naming it is the fix, never a violation.
_GATE_REF = "gh_issue.py"

# Raw ingestion vectors. `gh issue list --state open` enumerates the work-set; a
# `--search` list is a bounded title lookup before filing (allowlisted below).
_VIEW_RE = re.compile(r"\bgh issue view\b")
_LIST_OPEN_RE = re.compile(r"\bgh issue list\b.*--state\s+open\b")


def _md_files() -> list[Path]:
    return sorted(p for d in _SKILL_DIRS if d.exists() for p in d.rglob("*.md"))


def _offending_lines() -> list[str]:
    hits: list[str] = []
    for path in _md_files():
        rel = path.relative_to(_ROOT)
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if _GATE_REF in line:
                continue  # the gated replacement itself
            if _VIEW_RE.search(line) or _LIST_OPEN_RE.search(line):
                hits.append(f"{rel}:{lineno}: {line.strip()}")
    return hits


def test_skill_dirs_exist() -> None:
    # Guard against a silently-passing test if the mirrors move — the grep would find
    # nothing and read as green.
    assert _md_files(), "no skill *.md files found; check _SKILL_DIRS"


def test_no_raw_issue_ingestion_in_skills() -> None:
    offending = _offending_lines()
    assert not offending, (
        "skill files ingest raw issue content into a model (route through "
        "`scripts/gh_issue.py`):\n" + "\n".join(offending)
    )


def test_allowlisted_reads_do_not_trip() -> None:
    # The bounded dedupe-before-filing search, write subcommands, and PR reads are not
    # untrusted-body ingestion — they must NOT be flagged.
    allowed = [
        'gh issue list --state all --search "<keywords>"',
        "gh issue edit <n> --parent <epic>",
        "gh issue create --title ...",
        "gh issue comment <n> --body ...",
        "gh pr view <pr> --json headRefOid",
        "uv run --no-project python scripts/gh_issue.py view <n> --comments",
    ]
    for line in allowed:
        if _GATE_REF in line:
            continue
        assert not _VIEW_RE.search(line), line
        assert not _LIST_OPEN_RE.search(line), line


@pytest.mark.parametrize(
    "line",
    [
        "gh issue view <n> --comments",
        "run `gh issue view 328` to read it",
        "gh issue list --state open --limit 5000",
    ],
)
def test_detector_catches_known_vectors(line: str) -> None:
    assert _VIEW_RE.search(line) or _LIST_OPEN_RE.search(line), line
