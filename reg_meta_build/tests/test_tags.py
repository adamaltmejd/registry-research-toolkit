"""Curated cross-register thematic tag layer (#311) — build-side machinery.

Covers the loader (`load_tags`: shape + exactly-one-grain + dedup validation),
per-grain uniqueness and exactly-one-grain enforced by the DDL, and the validator
closure check. Common writer and dependency tests cover resolved tag materialization.
Tests use both small local fixtures and the committed seed `tags.toml`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_variable, build_slugged_db, seed_tags
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build._curation import repo_curation_path
from reg_meta_build.tags import (
    CuratedTag,
    TagMember,
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


def test_repo_tags_toml_seeds_scb_subset() -> None:
    tags = load_tags(repo_curation_path("tags.toml"))
    assert [tag.slug for tag in tags] == [
        "income",
        "economic-assistance-amount",
        "economic-assistance-months",
        "earned-income",
        "employment",
        "education",
        "family-household",
        "geography",
    ]
    assert all(any(member.starred for member in tag.members) for tag in tags)

    refs = {
        f"{member.provider}/{member.register}"
        + (f"/{member.variable}" if member.variable is not None else "")
        for tag in tags
        for member in tag.members
    }
    assert {
        "scb/lisa",
        "scb/iot",
        "scb/ureg",
        "scb/hreg",
        "scb/rams",
        "scb/iot/disponibel-inkomst",
        "scb/ekonomiskt-bistand/utbetalt-belopp-till-hushallet",
        "sos/ekb/ekbbelopp",
        "scb/ekonomiskt-bistand/antmanad",
        "sos/ekb/ekbantmanad",
        "scb/lisa/sammanraknad-forvarvsinkomst",
        "scb/rams/arbink",
        "scb/lisa/arbetsmarknadsstatus",
        "scb/ureg/sun2020niva",
        "scb/iot/familjetyp",
        "scb/lisa/kommun-for-bostaden",
    }.issubset(refs)


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


# ── database constraints ─────────────────────────────────────────────────────


def _tag(members: tuple[TagMember, ...], **overrides) -> CuratedTag:
    kwargs: dict = {
        "slug": "income",
        "label": "Income",
        "description": None,
        "members": members,
    }
    kwargs.update(overrides)
    return CuratedTag(**kwargs)


def test_tag_member_per_grain_uniqueness() -> None:
    """A (tag, register) pair must be unique — the partial UNIQUE index is the DB
    backstop (load-time dedup is by literal FQID; a slug rename could collide two
    distinct refs onto one id). A duplicate INSERT raises IntegrityError."""
    import sqlite3

    conn = build_slugged_db(classification=None)
    seed_tags(
        conn,
        (_tag((TagMember("scb", "lisa", None, 0, False, None),)),),
    )
    tag_id = conn.execute("SELECT tag_id FROM tag").fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tag_member (tag_id, register_id, variable_id) "
            "VALUES (?, 1, NULL)",
            (tag_id,),
        )


def test_tag_member_variable_grain_uniqueness() -> None:
    """Symmetric to the register-grain test: a (tag, variable) pair must be unique
    via `idx_tag_member_variable`. Guards against both partial indexes accidentally
    keying on the SAME column (e.g. both on register_id), which would leave the
    variable grain unprotected."""
    import sqlite3

    conn = build_slugged_db(classification=None)  # variable `kon` exists
    seed_tags(
        conn,
        (_tag((TagMember("scb", "lisa", "kon", 0, False, None),)),),
    )
    tag_id, var_id = conn.execute(
        "SELECT tm.tag_id, tm.variable_id FROM tag_member tm"
    ).fetchone()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tag_member (tag_id, register_id, variable_id) "
            "VALUES (?, NULL, ?)",
            (tag_id, var_id),
        )


def test_tag_member_exactly_one_grain_check() -> None:
    """The DDL CHECK rejects a row with both grains set or neither."""
    import sqlite3

    conn = build_slugged_db(classification=None)
    seed_tags(
        conn,
        (_tag((TagMember("scb", "lisa", None, 0, False, None),)),),
    )
    tag_id = conn.execute("SELECT tag_id FROM tag").fetchone()[0]
    # Both grains set.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tag_member (tag_id, register_id, variable_id) VALUES (?, 1, 1)",
            (tag_id,),
        )
    # Neither grain set.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tag_member (tag_id, register_id, variable_id) "
            "VALUES (?, NULL, NULL)",
            (tag_id,),
        )


def test_validator_closure_passes_on_materialized_tags() -> None:
    from reg_meta_build.validate import ValidationResult, _check_tags

    conn = build_slugged_db(classification=None)
    add_variable(conn, register_id=1, var_id=90, name="Income", slug="dispink")
    seed_tags(
        conn,
        (
            _tag(
                (
                    TagMember("scb", "lisa", "dispink", 0, True, "primary"),
                    TagMember("scb", "lisa", None, 1, False, None),
                )
            ),
        ),
    )
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_tags(conn, result, tables)
    assert result.passed, result.failures


def test_synthetic_build_ships_empty_tags(fixture_db) -> None:
    """The explicit synthetic catalog has empty, queryable tag tables."""
    import sqlite3

    conn = sqlite3.connect(fixture_db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM tag").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM tag_member").fetchone()[0] == 0
    finally:
        conn.close()
