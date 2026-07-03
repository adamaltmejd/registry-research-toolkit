"""Enforcement: no skill ingests raw untrusted issue text into a model.

This repo is public, so a stranger's issue/comment is untrusted. The automation reads
issue content only through the maintainer-author trust gate (`scripts/gh_issue.py`). This
test greps every `*.md` under the two mirrored skill trees (`.claude/skills/`,
`.agents/skills/`) and FAILS if any line re-introduces a raw model-read ingestion vector:

  - `gh issue view` — the body/comment ingestion vehicle (use `gh_issue.py view`);
  - `gh issue list` WITHOUT `--search` — work-set enumeration (use `gh_issue.py`).
    `--state open` is *not* required to enumerate: `gh issue list` defaults to open
    state, so a bare `gh issue list` reads the same untrusted set. Only the bounded
    `--search` dedupe-before-filing form is allowlisted;
  - `gh api .../issues/...` and `gh api graphql` — REST/GraphQL issue+comment node reads;
  - `gh search issues` — a search-shaped work-set/body ingestion.

Allowlisted (NOT ingestion of untrusted body text, so they must not trip): the
dedupe-before-filing `gh issue list ... --search`, the write/mutation subcommands
(`gh issue edit/create/comment`), any `gh pr ...` and `gh api .../pulls/...` (PRs are
gated separately by the fork-never-automerge rule), and the `gh_issue.py` replacement
line itself.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SKILL_DIRS = [_ROOT / ".claude" / "skills", _ROOT / ".agents" / "skills"]

# The gated replacement — a line naming it is the fix, never a violation.
_GATE_REF = "gh_issue.py"

# Raw ingestion vectors, searched against each raw line. Skill markdown is
# maintainer-authored and uses the single-space command forms below, so the patterns
# match those forms directly (this is a regression guard, not an adversarial filter).
#   - `gh issue view` — the body/comment ingestion vehicle;
#   - `gh api .../issues/...` — REST issue/comment node reads (a `.../pulls/...` path is a
#     PR read, gated separately);
#   - `gh api graphql` — GraphQL issue/comment body reads;
#   - `gh search issues` — a search-shaped work-set/body ingestion.
_FORBIDDEN_RES = [
    re.compile(r"\bgh issue view\b"),
    re.compile(r"\bgh api\b[^\n]*/issues/"),
    re.compile(r"\bgh api graphql\b"),
    re.compile(r"\bgh search issues\b"),
]

# `gh issue list` enumerates the untrusted work-set and DEFAULTS to open state, so
# `--state open` is not what makes it an ingestion vector — a bare `gh issue list` reads
# the same set and must trip too. The ONLY allowlisted form is the bounded
# dedupe-before-filing `--search` lookup (documented in AGENTS.md's Issue tracker
# section). So: any `gh issue list` line is forbidden UNLESS it carries `--search`.
#
# The `--search` exemption is scoped to the invocation's OWN argument span — from the
# `gh issue list` match to the end of that shell command — so a trailing `# ... --search`
# comment or downstream prose can't suppress a real violation. The span terminates at
# whatever ends the command as skill markdown actually writes them: a `#` comment start,
# an unescaped command separator (`|`, `&&`, `;`), a closing backtick for inline code, or
# end of line. This is a deliberately conservative "arguments segment" cut, not a shell
# parser.
_ISSUE_LIST_RE = re.compile(r"\bgh issue list\b")
_SEARCH_RE = re.compile(r"--search\b")
# Terminators that end the `gh issue list` invocation's argument span.
_ARG_SPAN_END_RE = re.compile(r"[#`|;]|&&")


def _issue_list_arg_span(line: str, start: int) -> str:
    """Text of the `gh issue list` invocation's own arguments, from ``start``.

    Stops at the first command terminator (comment, closing backtick, pipe/`&&`/`;`) so
    `--search` mentioned after the command (comment or prose) doesn't exempt it.
    """
    rest = line[start:]
    end = _ARG_SPAN_END_RE.search(rest)
    return rest[: end.start()] if end else rest


def _is_forbidden(line: str) -> bool:
    m = _ISSUE_LIST_RE.search(line)
    if m and not _SEARCH_RE.search(_issue_list_arg_span(line, m.end())):
        return True
    return any(rx.search(line) for rx in _FORBIDDEN_RES)


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
            if _is_forbidden(line):
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
    # The bounded dedupe-before-filing search, write subcommands, PR reads (issue view and
    # the `.../pulls/...` REST path), and the gate replacement are not untrusted-body
    # ingestion — they must NOT be flagged.
    allowed = [
        'gh issue list --state all --search "<keywords>"',
        # `--search` inside the invocation's own arg span before a pipe still exempts it.
        'gh issue list --search "x" | head',
        "gh issue edit <n> --parent <epic>",
        "gh issue create --title ...",
        "gh issue comment <n> --body ...",
        "gh pr view <pr> --json headRefOid",
        "gh api repos/{owner}/{repo}/pulls/{n}/reviews",
        "uv run --no-project python scripts/gh_issue.py view <n> --comments",
    ]
    for line in allowed:
        if _GATE_REF in line:
            continue
        assert not _is_forbidden(line), line


@pytest.mark.parametrize(
    "line",
    [
        # literal forms
        "gh issue view <n> --comments",
        "run `gh issue view 328` to read it",
        "gh issue list --state open --limit 5000",
        # a bare `gh issue list` (no --search) enumerates the same set — default is open
        "gh issue list --limit 5000",
        "gh issue list",
        "gh issue list --state all --json number",  # non-search filters don't exempt it
        # a trailing comment mentioning --search must NOT exempt the bare command — the
        # exemption is scoped to the invocation's own arg span (closes the suffix bypass)
        "gh issue list --state open  # use --search instead",
        # `--search` in a SEPARATE backticked fragment (prose), not the command's own
        # args, doesn't exempt: the closing backtick terminates the arg span. Preserves
        # the "prose mention of the command is a hit" intent already asserted for
        # `gh issue view` in prose above.
        "prefer `gh issue list` over the `--search` form when enumerating",
        # newly-forbidden ingestion forms
        "gh api repos/{owner}/{repo}/issues/{n}",
        "gh api repos/{owner}/{repo}/issues/{n}/comments",
        "gh api graphql -f query='...'",
        "gh search issues --repo owner/repo 'foo'",
    ],
)
def test_detector_catches_known_vectors(line: str) -> None:
    assert _is_forbidden(line), line
