"""Claude Code (`.claude/skills/`) and Codex (`.agents/skills/`) serve the same skills.

Each skill is a real directory in one catalog and a relative symlink to it in the other,
so the two cannot drift and the link survives any clone or worktree. The canonical
directory also holds the Codex `agents/openai.yaml`. Fails on a skill present in only one
catalog, a copied directory, or an absolute or repointed link.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CATALOGS = (".claude", ".agents")
SKILLS = sorted(
    {p.name for c in CATALOGS for p in (ROOT / c / "skills").iterdir() if p.is_dir()}
)


def test_skills_found() -> None:
    assert SKILLS, "no skills found under .claude/skills or .agents/skills"


@pytest.mark.parametrize("skill", SKILLS)
def test_skill_is_real_in_one_catalog_and_aliased_in_the_other(skill: str) -> None:
    paths = {c: ROOT / c / "skills" / skill for c in CATALOGS}
    links = [c for c, p in paths.items() if p.is_symlink()]
    assert len(links) == 1, f"{skill}: exactly one catalog must hold a symlink"
    (alias_catalog,) = links
    (canonical,) = set(CATALOGS) - {alias_catalog}
    alias = paths[alias_catalog]
    assert alias.readlink() == Path(f"../../{canonical}/skills/{skill}")
    assert (
        (alias / "SKILL.md")
        .read_text(encoding="utf-8")
        .startswith(f"---\nname: {skill}\n")
    )
    assert (paths[canonical] / "agents/openai.yaml").is_file()
