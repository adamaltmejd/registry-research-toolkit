"""Enforcement: the Codex skill catalog serves the SAME yard-operator routine.

Claude Code discovers skills under `.claude/skills/`, Codex under `.agents/skills/`.
`yard init` scaffolds only the Claude path, so Codex discovery is bridged by
`.agents/skills/yard-operator` — a *relative* directory symlink to the canonical
`.claude/skills/yard-operator`. Relative matters: an absolute link would break in every
clone, worktree and lane container. A symlink (not a copy) is what makes drift between
the two operator routines impossible — the other skills in these two trees ARE copies,
and they have already drifted.

This test fails if the alias is deleted, turned into a copied directory, or repointed.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_CANONICAL = _ROOT / ".claude" / "skills" / "yard-operator"
_ALIAS = _ROOT / ".agents" / "skills" / "yard-operator"
_TARGET = "../../.claude/skills/yard-operator"


def test_alias_is_a_relative_symlink_to_the_canonical_skill() -> None:
    assert _ALIAS.is_symlink(), f"{_ALIAS} must be a symlink, not a copied directory"
    link = _ALIAS.readlink()
    assert link == Path(_TARGET), f"alias points at {link}, expected {_TARGET}"
    assert _ALIAS.resolve() == _CANONICAL.resolve()


def test_skill_md_reads_through_the_alias() -> None:
    # Discovery is worthless if the link resolves but the file behind it does not read.
    text = (_ALIAS / "SKILL.md").read_text(encoding="utf-8")
    assert text.startswith("---\nname: yard-operator\n"), text[:64]
    assert text == (_CANONICAL / "SKILL.md").read_text(encoding="utf-8")
