"""Enforcement: the Codex skill catalog serves the SAME skill bodies as Claude Code.

Claude Code discovers skills under `.claude/skills/`, Codex under `.agents/skills/`. Each
shared skill lives as a real directory in ONE catalog and a *relative* directory symlink
in the other. Relative matters: an absolute link would break in every clone, worktree and
lane container. A symlink (not a copy) is what makes drift between the two catalogs
impossible — the skills in these trees that are still copies have already drifted.

Two link directions exist:

- `yard init` scaffolds the generated Yard routine (`yard-operator`, `yard-file`,
  `yard-drive`) under `.agents/skills/`, so `.claude/skills/<name>` is the alias —
  `.claude/skills/<name>` → `../../.agents/skills/<name>`. The hand-authored
  `upgrade-deps` maintenance skill uses that same direction.
- The specialized design skills are hand-authored under `.claude/skills/`, so
  `.agents/skills/<name>` is the alias — `.agents/skills/<name>` → `../../.claude/skills/<name>`.

Where an aliased skill has a Codex `agents/openai.yaml`, it lives inside the canonical
directory (the other catalog ignores it), so one body serves both catalogs.

This test fails if an alias is deleted, turned into a copied directory, or repointed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
# skill name -> (canonical catalog, alias catalog). The specialized design skills are
# hand-authored under `.claude/skills/`; the generated Yard routine is scaffolded under
# `.agents/skills/`. Either way, the alias catalog holds only the symlink.
_SKILLS = {
    "reg-webapp-frontend-design": (".claude", ".agents"),
    "reg-webapp-design-reviewer": (".claude", ".agents"),
    "yard-operator": (".agents", ".claude"),
    "yard-file": (".agents", ".claude"),
    "yard-drive": (".agents", ".claude"),
    "upgrade-deps": (".agents", ".claude"),
}


def test_aliased_list_matches_the_symlinks_on_disk() -> None:
    # Guard against silently-shrinking coverage: a symlink added under either catalog
    # without a matching `_SKILLS` entry leaves the parametrized tests green with fewer
    # cases.
    for alias_catalog in (".agents", ".claude"):
        expected = {
            name for name, (_, alias) in _SKILLS.items() if alias == alias_catalog
        }
        links = {
            p.name
            for p in (_ROOT / alias_catalog / "skills").iterdir()
            if p.is_symlink()
        }
        assert links == expected, (
            f"{alias_catalog}/skills symlinks {sorted(links)} != {sorted(expected)}"
        )


@pytest.mark.parametrize("skill", sorted(_SKILLS))
def test_alias_is_a_relative_symlink_to_the_canonical_skill(skill: str) -> None:
    canonical, alias_catalog = _SKILLS[skill]
    alias = _ROOT / alias_catalog / "skills" / skill
    target = Path(f"../../{canonical}/skills/{skill}")
    assert alias.is_symlink(), f"{alias} must be a symlink, not a copied directory"
    link = alias.readlink()
    assert link == target, f"alias points at {link}, expected {target}"
    assert alias.resolve() == (_ROOT / canonical / "skills" / skill).resolve()


@pytest.mark.parametrize("skill", sorted(_SKILLS))
def test_skill_md_reads_through_the_alias(skill: str) -> None:
    # `Path.resolve()` above is non-strict and passes on a dangling link, so discovery is
    # only proven once the body actually reads through the alias.
    _, alias_catalog = _SKILLS[skill]
    alias = _ROOT / alias_catalog / "skills" / skill
    text = (alias / "SKILL.md").read_text(encoding="utf-8")
    assert text.startswith(f"---\nname: {skill}\n"), text[:64]
