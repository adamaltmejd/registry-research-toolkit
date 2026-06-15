"""Tests for the thin curated-provider adapter (#422; `sources/curated.py`).

Covers the adapter mechanics on a synthetic TOML, an end-to-end `build_db` over
the REAL committed FOHM catalog (SmiNet + NVR), the malformed-input failure
modes, and the #422 generalization of the global minted-id band check to every
seeded non-SCB provider.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest
from reg_meta.errors import RegMetaError
from reg_meta_build.id import _MINT_BIT, mint
from reg_meta_build.ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
)
from reg_meta_build.sources.curated import CuratedAdapter
from reg_meta_build.validate import validate_built_db

_REPO = Path(__file__).resolve().parents[1]  # reg_meta_build/
_REAL_INPUT = _REPO / "input_data"
_REAL_SLUG = _REPO / "fqid_slugs" / "fohm.toml"


def _emit(provider: str, toml_text: str, tmp_path: Path) -> list:
    src = tmp_path / "src"
    src.mkdir()
    (src / f"{provider}.toml").write_text(toml_text, encoding="utf-8")
    return list(CuratedAdapter(provider).emit(src))


# ── adapter mechanics ────────────────────────────────────────────────────────

_TWO_VARIANT = """\
[[register]]
key = "reg1"
name = "Register One"
purpose = "prose"
valid_from = "2010-01-01"

  [[register.variant]]
  key = "fall"
  name = "Fall"

  [[register.variant]]
  key = "manad"
  name = "Månad"

  [[register.variable]]
  name = "Personnummer"
  column = "pnr"
  is_identifier = true
  is_sensitive = true

  [[register.variable]]
  name = "Belopp"
  column = "belopp"
  data_type = "decimal"
  variants = ["fall"]
  valid_from = "2015-06-01"
"""


def test_emit_two_variant_register(tmp_path: Path) -> None:
    objs = _emit("fk", _TWO_VARIANT, tmp_path)
    regs = [o for o in objs if isinstance(o, IRRegister)]
    variants = [o for o in objs if isinstance(o, IRVariant)]
    variables = [o for o in objs if isinstance(o, IRVariable)]
    states = [o for o in objs if isinstance(o, IRVariableState)]
    aliases = [o for o in objs if isinstance(o, IRVariableAlias)]

    assert len(regs) == 1
    assert regs[0].provider == "fk"
    assert regs[0].register_id == mint("fk", "reg1")
    assert regs[0].purpose == "prose"
    # register table has no `description` column — adapter leaves it None.
    assert regs[0].description is None

    assert {v.name for v in variants} == {"Fall", "Månad"}
    assert all(not v.synthesized for v in variants)

    # `pnr` is delivered in BOTH variants (no `variants` key) → two states.
    pnr = next(v for v in variables if v.provider_key == "pnr")
    assert pnr.is_identifier and pnr.is_sensitive
    pnr_states = [s for s in states if s.variable_id == pnr.variable_id]
    assert len(pnr_states) == 2
    assert {s.register_variant_id for s in pnr_states} == {
        mint("fk", "reg1", "fall"),
        mint("fk", "reg1", "manad"),
    }
    # Inherits the register coverage start.
    assert all(s.valid_from == "2010-01-01" for s in pnr_states)
    assert all(
        s.valid_to is None for s in pnr_states
    )  # open-ended → sentinel at insert

    # `belopp` is pinned to `fall` only → one state, with its own valid_from.
    belopp = next(v for v in variables if v.provider_key == "belopp")
    belopp_states = [s for s in states if s.variable_id == belopp.variable_id]
    assert len(belopp_states) == 1
    assert belopp_states[0].register_variant_id == mint("fk", "reg1", "fall")
    assert belopp_states[0].valid_from == "2015-06-01"
    assert belopp_states[0].data_type == "decimal"

    # One alias per state, carrying the delivery column.
    assert len(aliases) == len(states) == 3
    assert {a.delivery_column_name for a in aliases} == {"pnr", "belopp"}

    # All ids minted into the high band.
    assert all(
        o.register_id >= _MINT_BIT
        for o in objs
        if isinstance(o, IRRegister | IRVariant | IRVariable)
    )


def test_emit_synthesizes_default_variant(tmp_path: Path) -> None:
    toml = """\
[[register]]
key = "solo"
name = "Solo"
valid_from = "2000-01-01"

  [[register.variable]]
  name = "X"
  column = "x"
"""
    objs = _emit("fohm", toml, tmp_path)
    variants = [o for o in objs if isinstance(o, IRVariant)]
    assert len(variants) == 1
    assert variants[0].name == "_default"
    assert variants[0].synthesized is True
    assert variants[0].register_variant_id == mint("fohm", "solo", "_default")


@pytest.mark.parametrize(
    "toml, fragment",
    [
        ("[[register]]\nname='x'\nvalid_from='2000-01-01'\n", "key"),  # missing key
        ('[[register]]\nkey="r"\nname="R"\n', "valid_from"),  # missing valid_from
        (
            '[[register]]\nkey="r"\nname="R"\nvalid_from="2000"\n',
            "ISO date",
        ),  # bad date
        (
            '[[register]]\nkey="r"\nname="R"\nvalid_from="2000-01-01"\n',
            "variable",
        ),  # no variables
        (
            '[[register]]\nkey="r"\nname="R"\nvalid_from="2000-01-01"\n'
            '[[register.variable]]\nname="A"\ncolumn="c"\n'
            '[[register.variable]]\nname="B"\ncolumn="c"\n',
            "duplicate column",
        ),
    ],
)
def test_malformed_toml_raises(tmp_path: Path, toml: str, fragment: str) -> None:
    with pytest.raises(RegMetaError) as exc:
        _emit("fohm", toml, tmp_path)
    assert exc.value.code in {"curated_toml_invalid", "curated_toml_not_found"}
    assert fragment in exc.value.message


def test_unknown_variant_ref_raises(tmp_path: Path) -> None:
    toml = """\
[[register]]
key = "r"
name = "R"
valid_from = "2000-01-01"

  [[register.variant]]
  key = "a"
  name = "A"

  [[register.variable]]
  name = "X"
  column = "x"
  variants = ["nope"]
"""
    with pytest.raises(RegMetaError) as exc:
        _emit("fohm", toml, tmp_path)
    assert "unknown variant" in exc.value.message


def test_missing_file_raises(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    with pytest.raises(RegMetaError) as exc:
        list(CuratedAdapter("fohm").emit(tmp_path / "src"))
    assert exc.value.code == "curated_toml_not_found"


# ── end-to-end build over the REAL committed FOHM catalog ────────────────────


@pytest.fixture
def fohm_db(tmp_path: Path) -> Path:
    """Build a `fohm`-only DB from the committed catalog + slug TOML."""
    from reg_meta_build.db import build_db

    slug_dir = tmp_path / "slugs"
    slug_dir.mkdir()
    shutil.copy(_REAL_SLUG, slug_dir / "fohm.toml")
    build_db(
        input_dir=_REAL_INPUT,
        db_dir=tmp_path / "db",
        providers=("fohm",),
        skip_classifications=True,
        slug_dir=slug_dir,
    )
    return tmp_path / "db" / "reg_meta.db"


def test_real_fohm_catalog_builds(fohm_db: Path) -> None:
    conn = sqlite3.connect(fohm_db)
    try:
        regs = dict(conn.execute("SELECT slug, name FROM register").fetchall())
        assert regs == {
            "sminet": "SmiNet",
            "nvr": "Nationella vaccinationsregistret",
        }
        # Every register gets the synthesized single-table variant.
        variant_slugs = [
            r[0] for r in conn.execute("SELECT slug FROM register_variant").fetchall()
        ]
        assert variant_slugs == ["_default", "_default"]

        # Panel coordinates land from the slug TOML.
        sminet_panel = conn.execute(
            "SELECT panel_entity_key, panel_time_key, panel_time_grain "
            "FROM register_variant rv JOIN register r USING (register_id) "
            "WHERE r.slug = 'sminet'"
        ).fetchone()
        assert sminet_panel == ("personnummer", "diagnosdatum", "row")

        # Identifier + sensitivity flags survived.
        pnr = conn.execute(
            "SELECT is_identifier, is_sensitive FROM variable v "
            "JOIN register r USING (register_id) "
            "WHERE r.slug = 'sminet' AND v.slug = 'personnummer'"
        ).fetchone()
        assert pnr == (1, 1)
        diagnos = conn.execute(
            "SELECT is_identifier, is_sensitive FROM variable v "
            "JOIN register r USING (register_id) "
            "WHERE r.slug = 'sminet' AND v.slug = 'diagnos'"
        ).fetchone()
        assert diagnos == (0, 1)

        # Every variable has a state + an alias; no value sets (deferred).
        n_var, n_state, n_alias, n_vset = (
            conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM variable_alias").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0],
        )
        assert n_var == n_state == n_alias > 0
        assert n_vset == 0

        # Per-variable coverage override (NVR dosnummer is covid-only from 2021).
        dosnummer_from = conn.execute(
            "SELECT vs.valid_from FROM variable_state vs "
            "JOIN variable v USING (variable_id) JOIN register r USING (register_id) "
            "WHERE r.slug = 'nvr' AND v.slug = 'dosnummer'"
        ).fetchone()[0]
        assert dosnummer_from == "2021-01-01"
    finally:
        conn.close()


def test_real_fohm_catalog_validates(fohm_db: Path) -> None:
    result = validate_built_db(fohm_db, corpus=False)
    assert not result.failures, result.failures


def test_fohm_ids_are_high_band(fohm_db: Path) -> None:
    conn = sqlite3.connect(fohm_db)
    try:
        for table, col in (
            ("register", "register_id"),
            ("register_variant", "register_variant_id"),
            ("variable", "variable_id"),
            ("variable_state", "state_id"),
        ):
            lo = conn.execute(f"SELECT MIN({col}) FROM {table}").fetchone()[0]
            assert lo >= _MINT_BIT, f"{table}.{col} not minted high-band"
    finally:
        conn.close()


def test_global_band_check_catches_low_band_fohm(fohm_db: Path) -> None:
    """#422: the GLOBAL (non-flavored) band check now enforces every seeded
    non-SCB provider — a low-band FOHM id is caught without `flavored=True`."""
    conn = sqlite3.connect(fohm_db)
    rid = conn.execute("SELECT register_id FROM register LIMIT 1").fetchone()[0]
    conn.execute("UPDATE register SET register_id = 5 WHERE register_id = ?", (rid,))
    conn.execute(
        "UPDATE register_variant SET register_id = 5 WHERE register_id = ?", (rid,)
    )
    conn.execute("UPDATE variable SET register_id = 5 WHERE register_id = ?", (rid,))
    conn.commit()
    conn.close()
    result = validate_built_db(fohm_db, corpus=False)
    assert any("below the minted band" in f for f in result.failures), result.failures
