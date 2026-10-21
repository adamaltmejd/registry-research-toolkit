"""No skill reads raw issue text into a model.

The repo is public, so issue and comment bodies are untrusted; automation reads them only
through the maintainer-author gate `scripts/gh_issue.py`. Every skill markdown line is
checked for a raw read: `gh issue view`, `gh issue list` without `--search` in its own
arguments (it defaults to open issues, so `--state` does not matter), `gh api .../issues/`,
`gh api graphql` and `gh search issues`. Writes (`gh issue create/edit/comment`), PR reads
and a line naming `gh_issue.py` are allowed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SKILL_DIRS = [
    ROOT / ".claude/skills",
    ROOT / ".agents/skills",
    *ROOT.glob("plugins/*/skills"),
]
FORBIDDEN = re.compile(
    r"\bgh issue view\b|\bgh api\b[^\n]*/issues/|\bgh api graphql\b|\bgh search issues\b"
)
# A `gh issue list` invocation's own arguments end at a comment, a closing backtick or a
# command separator, so a `--search` after it does not exempt it.
ISSUE_LIST = re.compile(r"\bgh issue list\b([^#`|;&]*)")


def is_forbidden(line: str) -> bool:
    if "gh_issue.py" in line:
        return False
    raw_list = any("--search" not in m[1] for m in ISSUE_LIST.finditer(line))
    return raw_list or bool(FORBIDDEN.search(line))


def skill_markdown() -> list[Path]:
    files = {p.resolve(): p for d in SKILL_DIRS for p in d.rglob("*.md")}
    return sorted(files.values())


def test_no_raw_issue_reads_in_skills() -> None:
    files = skill_markdown()
    assert files, "no skill markdown found; check SKILL_DIRS"
    hits = [
        f"{path.relative_to(ROOT)}:{n}: {line.strip()}"
        for path in files
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if is_forbidden(line)
    ]
    assert not hits, "route issue reads through scripts/gh_issue.py:\n" + "\n".join(
        hits
    )


@pytest.mark.parametrize(
    ("line", "forbidden"),
    [
        ('gh issue list --state all --search "<keywords>"', False),
        ('gh issue list --search "a" && gh issue list --search "b"', False),
        ("gh issue create --title ...", False),
        ("gh issue comment <n> --body ...", False),
        ("gh api repos/{owner}/{repo}/pulls/{n}/reviews", False),
        ("uv run --no-project python scripts/gh_issue.py view <n> --comments", False),
        ("run `gh issue view 328` to read it", True),
        ("gh issue list", True),
        ("gh issue list --state all --json number", True),
        ('gh issue list --search "a"; gh issue list --state open', True),
        ("gh issue list --state open  # use --search instead", True),
        ("prefer `gh issue list` over the `--search` form", True),
        ("gh api repos/{owner}/{repo}/issues/{n}/comments", True),
        ("gh api graphql -f query='...'", True),
        ("gh search issues --repo owner/repo 'foo'", True),
    ],
)
def test_verdict(line: str, forbidden: bool) -> None:
    assert is_forbidden(line) is forbidden
