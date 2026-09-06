"""Enforcement: the Codex skill catalog serves the SAME skill bodies as Claude Code.

Claude Code discovers skills under `.claude/skills/`, Codex under `.agents/skills/`.
`yard init` scaffolds only the Claude path, so Codex discovery is bridged by a *relative*
directory symlink per shared skill — `.agents/skills/<name>` → `.claude/skills/<name>`.
Relative matters: an absolute link would break in every clone, worktree and lane
container. A symlink (not a copy) is what makes drift between the two catalogs
impossible — the skills in these trees that are still copies have already drifted.

Where an aliased skill has a Codex `agents/openai.yaml`, it lives inside the canonical
Claude directory (Claude Code ignores it), so one body serves both catalogs.

This test fails if an alias is deleted, turned into a copied directory, or repointed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_ALIASED = ["yard-operator", "reg-webapp-frontend-design", "reg-webapp-design-reviewer"]


def test_aliased_list_matches_the_symlinks_on_disk() -> None:
    # Guard against silently-shrinking coverage: a symlink added under `.agents/skills/`
    # without an `_ALIASED` entry leaves the parametrized tests green with fewer cases.
    links = {p.name for p in (_ROOT / ".agents" / "skills").iterdir() if p.is_symlink()}
    assert links == set(_ALIASED), f"symlinks {sorted(links)} != {sorted(_ALIASED)}"


@pytest.mark.parametrize("skill", _ALIASED)
def test_alias_is_a_relative_symlink_to_the_canonical_skill(skill: str) -> None:
    alias = _ROOT / ".agents" / "skills" / skill
    target = Path(f"../../.claude/skills/{skill}")
    assert alias.is_symlink(), f"{alias} must be a symlink, not a copied directory"
    link = alias.readlink()
    assert link == target, f"alias points at {link}, expected {target}"
    assert alias.resolve() == (_ROOT / ".claude" / "skills" / skill).resolve()


@pytest.mark.parametrize("skill", _ALIASED)
def test_skill_md_reads_through_the_alias(skill: str) -> None:
    # `Path.resolve()` above is non-strict and passes on a dangling link, so discovery is
    # only proven once the body actually reads through the alias.
    alias = _ROOT / ".agents" / "skills" / skill
    text = (alias / "SKILL.md").read_text(encoding="utf-8")
    assert text.startswith(f"---\nname: {skill}\n"), text[:64]
