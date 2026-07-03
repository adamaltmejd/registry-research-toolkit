"""Unit tests for scripts/check_issue_hygiene.py — the issue-tracker hygiene validator.

The validator's parsing logic (the relationship + touches regexes and the build-debt
path classifier) is the bug-prone surface — two review passes found ~15 defects in it.
These tests pin that behaviour and the per-issue checks. The `gh`/`git`-calling functions
(fetch_*, check_done_but_open, check_unreleased_build_debt, main) are covered by live runs.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_ROOT = _SCRIPTS.parent
_SPEC = importlib.util.spec_from_file_location(
    "check_issue_hygiene", _SCRIPTS / "check_issue_hygiene.py"
)
assert _SPEC and _SPEC.loader
h = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(h)

_AGENTS = (_ROOT / "AGENTS.md").read_text(encoding="utf-8")


# --- parse_relationships -------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("this work is related to #99 in passing", []),  # prose mid-line → not a tie
        ("Part of #365", [("part of", 365)]),  # plain line
        ("- Part of #365", [("part of", 365)]),  # bullet
        ("> Blocked by #5", [("blocked by", 5)]),  # blockquote
        ("depends on #7", [("depends on", 7)]),  # case-insensitive
        ("Follow-up to #42", [("follow-up to", 42)]),
        ("Depends  on #6", [("depends on", 6)]),  # reflow double space
        (
            "Blocked by #1, #2, #3",
            [("blocked by", 1), ("blocked by", 2), ("blocked by", 3)],
        ),  # comma list
    ],
)
def test_parse_relationships(body: str, expected: list[tuple[str, int]]) -> None:
    assert h.parse_relationships(body) == expected


def test_parse_relationships_ignores_fenced_code() -> None:
    body = "Depends on #5\n```\nRelated to #999\n```\n"
    assert h.parse_relationships(body) == [("depends on", 5)]


def test_parse_relationships_ignores_indented_fenced_code() -> None:
    # A fence nested under a list item (indented) must still be stripped, else the
    # line-anchored REL_RE matches the keyword inside it → false dangling-target ERROR.
    body = "- example\n    ```\n    Related to #999\n    ```\n"
    assert h.parse_relationships(body) == []


# --- parse_touches -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("```touches\nreg_meta/x.py\n```", ["reg_meta/x.py"]),
        ("```touches\r\nreg_meta/x.py\r\n```\r\n", ["reg_meta/x.py"]),  # CRLF
        ("- item\n    ```touches\n    a/b.py\n    ```", ["a/b.py"]),  # indented
        ("```touches\npath.py  # note\n# full comment\n\n```", ["path.py"]),
        ("no touches block here", []),
    ],
)
def test_parse_touches(body: str, expected: list[str]) -> None:
    assert h.parse_touches(body) == expected


# --- build-debt path classifier ------------------------------------------------------


def _is_global_content(f: str) -> bool:
    return f not in h.BUILD_IGNORE and bool(h.BUILD_CONTENT_RE.match(f))


@pytest.mark.parametrize(
    ("path", "is_content"),
    [
        ("reg_meta_build/src/reg_meta_build/db.py", True),
        ("reg_meta_build/concept_groups.toml", True),
        ("reg_meta_build/fqid_slugs/sos.toml", True),
        ("reg_meta_build/fqid_slugs/swecov/.snapshot.json", False),  # steward flavor
        ("reg_meta_build/pyproject.toml", False),  # version bump
        ("reg_meta_build/src/reg_meta_build/__init__.py", False),  # __version__ only
        ("reg_meta_build/tests/test_x.py", False),
        ("reg_meta_build/DESIGN.md", False),
        ("reg_meta/src/reg_meta/db.py", False),  # different package
    ],
)
def test_build_debt_classifier(path: str, is_content: bool) -> None:
    assert _is_global_content(path) is is_content


# --- check_issue (per-issue checks) --------------------------------------------------


def _check(
    *,
    body: str = "",
    labels: tuple[str, ...] = (),
    known: set[int] = frozenset(),  # type: ignore[assignment]
    open_numbers: set[int] = frozenset(),  # type: ignore[assignment]
    parent_of: dict[int, int] | None = None,
    num: int = 1,
) -> list[tuple[str, int | None, str]]:
    out = h.Findings()
    issue = {
        "number": num,
        "labels": [{"name": label} for label in labels],
        "body": body,
    }
    h.check_issue(
        issue, set(known), set(open_numbers), dict(parent_of or {}), _ROOT, out
    )
    return out.items


def _has(items: list[tuple[str, int | None, str]], level: str, needle: str) -> bool:
    return any(lvl == level and needle in msg for lvl, _, msg in items)


def test_missing_labels_two_errors() -> None:
    items = _check(labels=())
    assert _has(items, "ERROR", "area label")
    assert _has(items, "ERROR", "type label")


def test_valid_labels_no_label_error() -> None:
    items = _check(labels=("reg_meta", "bug"))
    assert not any("label" in msg for _, _, msg in items)


def test_two_area_labels_error() -> None:
    items = _check(labels=("reg_meta", "reg_webapp", "bug"))
    assert _has(items, "ERROR", "area label")


def test_multiple_priority_labels_error() -> None:
    items = _check(labels=("reg_meta", "bug", "priority:high", "priority:low"))
    assert _has(items, "ERROR", "priority label")


def test_single_priority_label_ok() -> None:
    items = _check(labels=("reg_meta", "bug", "priority:high"))
    assert not _has(items, "ERROR", "priority label")


def test_no_priority_label_ok() -> None:
    items = _check(labels=("reg_meta", "bug"))
    assert not _has(items, "ERROR", "priority label")


def test_parked_label_ok_without_blocker() -> None:
    items = _check(labels=("reg_meta", "bug", "parked"))
    assert not _has(items, "WARN", "blocked")
    assert not _has(items, "ERROR", "label")


def test_open_blocker_with_parked_label_does_not_require_blocked_label() -> None:
    items = _check(
        labels=("reg_meta", "bug", "parked"),
        body="Blocked by #2",
        known={1, 2},
        open_numbers={2},
    )
    assert not _has(items, "WARN", "no 'blocked' label")


def test_blocked_and_parked_labels_warn() -> None:
    items = _check(labels=("reg_meta", "bug", "blocked", "parked"))
    assert _has(items, "WARN", "both 'blocked' and 'parked'")


def test_dangling_relationship_error() -> None:
    items = _check(labels=("reg_meta", "bug"), body="Depends on #999", known={1})
    assert _has(items, "ERROR", "#999")


def test_resolvable_relationship_no_error() -> None:
    items = _check(labels=("reg_meta", "bug"), body="Depends on #2", known={1, 2})
    assert not _has(items, "ERROR", "#2")


def test_blocked_label_without_open_blocker_warns() -> None:
    # #2 exists (known) but is not open → the blocked label is stale.
    items = _check(
        labels=("reg_meta", "bug", "blocked"),
        body="Blocked by #2",
        known={1, 2},
    )
    assert _has(items, "WARN", "blocked")


def test_open_blocker_without_blocked_label_warns() -> None:
    items = _check(
        labels=("reg_meta", "bug"),
        body="Blocked by #2",
        known={1, 2},
        open_numbers={2},
    )
    assert _has(items, "WARN", "blocker")


def test_blocked_by_open_pr_counts_as_blocker() -> None:
    # `Blocked by #<open PR>` is a real blocker — open_numbers carries open PRs too.
    items = _check(
        labels=("reg_meta", "bug"),
        body="Blocked by #50",
        known={1, 50},
        open_numbers={50},
    )
    assert _has(items, "WARN", "blocker")


def test_part_of_without_native_parent_warns() -> None:
    items = _check(labels=("reg_meta", "bug"), body="Part of #5", known={1, 5})
    assert _has(items, "WARN", "Part of #5")


def test_part_of_matches_native_parent_ok() -> None:
    items = _check(
        labels=("reg_meta", "bug"), body="Part of #5", known={1, 5}, parent_of={1: 5}
    )
    assert not any("Part of" in msg for _, _, msg in items)


def test_native_parent_without_part_of_warns() -> None:
    items = _check(labels=("reg_meta", "bug"), parent_of={1: 5})
    assert _has(items, "WARN", "native sub-issue")


@pytest.mark.parametrize("pattern", ["/etc/passwd", ".", "../sibling/x.py"])
def test_touches_non_relative_warns_not_crash(pattern: str) -> None:
    items = _check(labels=("reg_meta", "bug"), body=f"```touches\n{pattern}\n```")
    assert _has(items, "WARN", "not a repo-relative path")


def test_touches_matching_file_no_warn() -> None:
    items = _check(
        labels=("reg_meta", "bug"),
        body="```touches\nscripts/check_issue_hygiene.py\n```",
    )
    assert not any("touches" in msg for _, _, msg in items)


def test_touches_missing_file_warns() -> None:
    items = _check(labels=("reg_meta", "bug"), body="```touches\nno/such/file.xyz\n```")
    assert _has(items, "WARN", "matches no files")


# --- non-maintainer redaction (public-repo untrusted-text gate) ----------------------

_MAINT = "adamaltmejd"


def _gated(
    *,
    author_login: str | None,
    body: str = "",
    labels: tuple[str, ...] = (),
    known: set[int] = frozenset(),  # type: ignore[assignment]
    open_numbers: set[int] = frozenset(),  # type: ignore[assignment]
    parent_of: dict[int, int] | None = None,
    num: int = 1,
) -> list[tuple[str, int | None, str]]:
    out = h.Findings()
    issue = {
        "number": num,
        "labels": [{"name": label} for label in labels],
        "body": body,
        "author": {"login": author_login} if author_login is not None else None,
    }
    h.check_issue_gated(
        issue,
        set(known),
        set(open_numbers),
        dict(parent_of or {}),
        _ROOT,
        out,
        _MAINT,
    )
    return out.items


def test_non_maintainer_issue_messages_are_number_only() -> None:
    # A stranger's issue that fails several checks with body-derived detail (a bogus
    # touches pattern, a dangling relationship, a title-laden label problem) must NOT leak
    # any of that text — only the number and a code-authored check-name.
    items = _gated(
        author_login="stranger",
        labels=(),  # missing area + type labels
        body="Depends on #999\n```touches\n/etc/passwd\n```",
        known={1},
    )
    assert items, "a failing stranger issue should still be flagged"
    for _lvl, _num, msg in items:
        assert "(non-maintainer)" in msg
        assert "inspect manually" in msg
        # none of the body-derived strings leak
        assert "#999" not in msg
        assert "/etc/passwd" not in msg
        assert "area label (has:" not in msg  # the verbose derived form is gone


def test_non_maintainer_blocked_drift_is_non_actionable() -> None:
    # A stranger issue with an open blocker but no `blocked` label would, for a maintainer,
    # emit the verbose "no 'blocked' label" line /issue-pulse pattern-matches to auto-add
    # the label. Redacted, it must NOT carry that actionable phrasing.
    items = _gated(
        author_login="stranger",
        labels=("reg_meta", "bug"),
        body="Blocked by #2",
        known={1, 2},
        open_numbers={2},
    )
    assert _has(items, "WARN", "blocked-label-drift")
    assert not _has(items, "WARN", "no 'blocked' label")
    assert not _has(items, "WARN", "remove it if unblocked")


def test_non_maintainer_null_author_is_redacted() -> None:
    # A missing/null author is not the maintainer → fail-closed to the redacted path.
    items = _gated(author_login=None, labels=(), known={1})
    assert items
    assert all("(non-maintainer)" in msg for _, _, msg in items)


def test_maintainer_issue_messages_unchanged() -> None:
    # The maintainer's own issue keeps the full verbose messages — no redaction.
    items = _gated(
        author_login=_MAINT,
        labels=(),
        body="Depends on #999",
        known={1},
    )
    assert _has(items, "ERROR", "needs exactly one area label")
    assert _has(items, "ERROR", "#999")
    assert not any("(non-maintainer)" in msg for _, _, msg in items)


def test_maintainer_case_insensitive_match_stays_verbose() -> None:
    # Authorship match is case-insensitive (mirrors the gate) → still verbose.
    items = _gated(author_login=_MAINT.upper(), labels=())
    assert _has(items, "ERROR", "needs exactly one area label")
    assert not any("(non-maintainer)" in msg for _, _, msg in items)


def test_maintainer_clean_issue_no_findings() -> None:
    # A clean maintainer issue produces nothing (redaction must not manufacture findings).
    items = _gated(author_login=_MAINT, labels=("reg_meta", "bug"))
    assert items == []


# --- doc <-> code agreement ----------------------------------------------------------


@pytest.mark.parametrize(
    "keyword",
    ["Part of", "Depends on", "Blocked by", "Follow-up to", "Supersedes", "Related to"],
)
def test_doc_relationship_keywords_parse(keyword: str) -> None:
    assert keyword in _AGENTS  # documented
    assert h.parse_relationships(f"- {keyword} #1") == [(keyword.lower(), 1)]


def test_doc_touches_example_parses() -> None:
    assert "reg_meta_build/concept_groups.toml" in h.parse_touches(_AGENTS)


def test_area_and_type_labels_documented() -> None:
    for label in h.AREA_LABELS | h.TYPE_LABELS | h.PRIORITY_LABELS | h.STATUS_LABELS:
        assert label in _AGENTS, f"label '{label}' missing from AGENTS.md"


# --- steward catalog staleness -------------------------------------------------------


def _steward_repo(tmp_path: Path, version: str | None) -> Path:
    """A tmp repo root with one steward catalog carrying `version` (omit field if None)."""
    catalog = (
        tmp_path / "reg_webapp" / "stewards" / "swecov" / "steward.project_data.json"
    )
    catalog.parent.mkdir(parents=True)
    payload: dict[str, str] = {}
    if version is not None:
        payload["reg_meta_version"] = version
    catalog.write_text(json.dumps(payload), encoding="utf-8")
    return tmp_path


def _stub_releases(
    monkeypatch: pytest.MonkeyPatch, releases: list[dict[str, object]]
) -> None:
    """Stub the `gh release list --json tagName,isDraft` resolution.

    `check_steward_catalog_staleness` resolves the latest *published* release via
    `gh_json` (mirroring container-build.yml), so the staleness tests stub that call's
    JSON payload rather than the old git-tag `run`.
    """
    monkeypatch.setattr(h, "gh_json", lambda *_args, **_kw: releases)


def _staleness(
    repo_root: Path, tag: str, monkeypatch: pytest.MonkeyPatch
) -> h.Findings:
    _stub_releases(monkeypatch, [{"tagName": tag, "isDraft": False}])
    out = h.Findings()
    h.check_steward_catalog_staleness(repo_root, out)
    return out


def test_steward_catalog_behind_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _steward_repo(tmp_path, "reg_meta/v0.22.0")
    out = _staleness(repo, "reg_meta/v0.23.0", monkeypatch)
    assert _has(out.items, "WARN", "swecov")
    assert _has(out.items, "WARN", "reg_meta/v0.23.0")


def test_steward_catalog_equal_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _steward_repo(tmp_path, "reg_meta/v0.23.0")
    out = _staleness(repo, "reg_meta/v0.23.0", monkeypatch)
    assert out.items == []


def test_steward_catalog_ahead_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Defensive: a catalog ahead of the tag (e.g. mid-release) must not warn.
    repo = _steward_repo(tmp_path, "reg_meta/v0.24.0")
    out = _staleness(repo, "reg_meta/v0.23.0", monkeypatch)
    assert out.items == []


def test_steward_catalog_numeric_not_lexical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # v0.9.0 < v0.10.0 numerically, though "0.9" > "0.10" lexically.
    repo = _steward_repo(tmp_path, "reg_meta/v0.9.0")
    out = _staleness(repo, "reg_meta/v0.10.0", monkeypatch)
    assert _has(out.items, "WARN", "swecov")


def test_steward_catalog_missing_version_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _steward_repo(tmp_path, None)
    out = _staleness(repo, "reg_meta/v0.23.0", monkeypatch)
    assert _has(out.items, "WARN", "malformed")


def test_steward_staleness_no_release_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _steward_repo(tmp_path, "reg_meta/v0.1.0")
    _stub_releases(monkeypatch, [])  # no published reg_meta release yet
    out = h.Findings()
    h.check_steward_catalog_staleness(repo, out)
    assert out.items == []


def test_steward_staleness_draft_only_not_compared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The newest release is a DRAFT (cut but not published) — the container can't bake it,
    # so it must NOT drive the comparison. The catalog matches the published v0.22.0, so
    # the draft v0.23.0 must stay silent (no false warn during the draft window).
    repo = _steward_repo(tmp_path, "reg_meta/v0.22.0")
    _stub_releases(
        monkeypatch,
        [
            {"tagName": "reg_meta/v0.23.0", "isDraft": True},
            {"tagName": "reg_meta/v0.22.0", "isDraft": False},
        ],
    )
    out = h.Findings()
    h.check_steward_catalog_staleness(repo, out)
    assert out.items == []


def test_steward_staleness_picks_newest_published_not_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Resolution is max-by-version over the published set, not list order — a draft newest
    # plus an out-of-order published list still resolves the true newest published release.
    repo = _steward_repo(tmp_path, "reg_meta/v0.22.0")
    _stub_releases(
        monkeypatch,
        [
            {"tagName": "reg_meta/v0.24.0", "isDraft": True},
            {"tagName": "reg_meta/v0.21.0", "isDraft": False},
            {"tagName": "reg_meta/v0.23.0", "isDraft": False},
        ],
    )
    out = h.Findings()
    h.check_steward_catalog_staleness(repo, out)
    assert _has(out.items, "WARN", "reg_meta/v0.23.0")


# --- emit ordering -------------------------------------------------------------------


def test_emit_is_deterministic(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    out = h.Findings()
    out.warn(50, "some-check", "later issue")
    out.warn(None, "some-check", "corpus alert")
    out.error(10, "some-check", "earlier issue")
    h.emit(out, "test")
    printed = capsys.readouterr().out
    assert printed.index("#10") < printed.index("#50") < printed.index("corpus alert")
