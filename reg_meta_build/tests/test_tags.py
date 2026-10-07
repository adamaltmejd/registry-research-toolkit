"""Curated cross-register thematic tag layer (#311) — build-side machinery.

Covers the loader (`load_tags`: shape + exactly-one-grain + dedup validation). The
validator closure check is pinned by the `tag-*` cases under `cases/validate/`; common writer
and dependency tests cover resolved tag materialization.
Tests use both small local fixtures and the committed seed `tags.toml`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.tags import (
    load_tags,
)

if TYPE_CHECKING:
    from pathlib import Path


def _write_tags_toml(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "tags.toml"
    path.write_text(text, encoding="utf-8")
    return path


# ── loader ───────────────────────────────────────────────────────────────────


def test_load_tags_parses_both_grains(tmp_path: Path) -> None:
    path = _write_tags_toml(
        tmp_path,
        """
[[tag]]
slug = "income"
label = "Income & earnings"
description = "Income measures"
  [[tag.member]]
  variable = "scb/lisa/dispink04"
  starred = true
  rank = 0
  note = "primary income measure"
  [[tag.member]]
  register = "scb/lisa"
  rank = 1
""",
    )
    tags = load_tags(path)
    assert len(tags) == 1
    tag = tags[0]
    assert tag.slug == "income"
    assert tag.description == "Income measures"
    assert len(tag.members) == 2
    var_m, reg_m = tag.members
    assert var_m.variable == "dispink04" and var_m.starred and var_m.rank == 0
    assert var_m.note == "primary income measure"
    assert reg_m.variable is None and reg_m.register == "lisa" and reg_m.rank == 1
    assert not reg_m.starred


def test_load_tags_empty_when_no_file() -> None:
    assert load_tags(None) == ()


@pytest.mark.parametrize(
    "member_body",
    [
        # BOTH grains set.
        '  variable = "scb/lisa/kon"\n  register = "scb/lisa"',
        # NEITHER grain set (only a rank, no variable/register key).
        "  rank = 0",
    ],
    ids=["both", "neither"],
)
def test_load_tags_member_needs_exactly_one_grain(
    tmp_path: Path, member_body: str
) -> None:
    path = _write_tags_toml(
        tmp_path,
        f"""
[[tag]]
slug = "x"
label = "X"
  [[tag.member]]
{member_body}
""",
    )
    with pytest.raises(RegMetaError) as exc:
        load_tags(path)
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == "tags_invalid"


@pytest.mark.parametrize(
    "ref_line",
    [
        'variable = "scb/lisa"',  # too few segments for a variable
        'register = "scb/lisa/kon"',  # too many segments for a register
    ],
)
def test_load_tags_rejects_malformed_fqid(tmp_path: Path, ref_line: str) -> None:
    path = _write_tags_toml(
        tmp_path,
        f"""
[[tag]]
slug = "x"
label = "X"
  [[tag.member]]
  {ref_line}
""",
    )
    with pytest.raises(RegMetaError) as exc:
        load_tags(path)
    assert exc.value.code == "tags_invalid"


def test_load_tags_rejects_duplicate_slug(tmp_path: Path) -> None:
    path = _write_tags_toml(
        tmp_path,
        """
[[tag]]
slug = "income"
label = "A"
  [[tag.member]]
  register = "scb/lisa"
[[tag]]
slug = "income"
label = "B"
  [[tag.member]]
  register = "scb/rams"
""",
    )
    with pytest.raises(RegMetaError) as exc:
        load_tags(path)
    assert exc.value.code == "tags_invalid"


def test_load_tags_rejects_duplicate_member_within_tag(tmp_path: Path) -> None:
    path = _write_tags_toml(
        tmp_path,
        """
[[tag]]
slug = "income"
label = "A"
  [[tag.member]]
  register = "scb/lisa"
  [[tag.member]]
  register = "scb/lisa"
""",
    )
    with pytest.raises(RegMetaError) as exc:
        load_tags(path)
    assert exc.value.code == "tags_invalid"


def test_load_tags_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    # A typo like `[[tags]]` must be a loud error, not a silent no-op.
    path = _write_tags_toml(
        tmp_path,
        """
[[tags]]
slug = "income"
label = "A"
""",
    )
    with pytest.raises(RegMetaError) as exc:
        load_tags(path)
    assert exc.value.code == "tags_invalid"
