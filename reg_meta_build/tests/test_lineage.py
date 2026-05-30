"""A2.4 (§5.6) tests: `variable_state_lineage` interval-overlap join.

Two harnesses, mirroring the rest of the suite:

- Unit tests build a tiny DB by hand (DDL + `seed_providers` + `_slugged_db`
  helpers), seed `variable_state` rows with explicit validity ranges, write a
  minimal lineage TOML to `tmp_path`, call `link_variable_state_lineage`, and
  assert on the `variable_state_lineage` / `variable_state_lineage_warning`
  tables. This is the primary suite — it seeds `variable_state` directly, with
  no CSV/coalesce dependency, so each test controls the exact intervals.
- The end-to-end wiring (pipeline ordering, `slug_root` plumbing, and the
  via_source_id KEEP regression) lives in `test_build_db.py`'s build-backed
  suite, against the shared CSV fixtures.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_register, add_variable, add_variant
from reg_meta.errors import RegMetaError
from reg_meta_build.db import (
    DDL,
    _variable_set_via_same_as,
    link_variable_state_lineage,
    seed_providers,
)

from reg_meta_build.fqid_slugs import load_lineage_config

if TYPE_CHECKING:
    from pathlib import Path

# Source register RTB-style, consumer LISA-style. RTB owns Kön; LISA's Kön is
# sourced from it. IDs are arbitrary but stable across the helpers below.
_SOURCE_REGISTER_ID = 1
_CONSUMER_REGISTER_ID = 2


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    seed_providers(conn)
    return conn


def _add_state(
    conn: sqlite3.Connection,
    *,
    variable_id: int,
    register_variant_id: int,
    valid_from: str,
    valid_to: str,
) -> int:
    """Insert one variable_state with explicit validity and return its state_id."""
    cur = conn.execute(
        "INSERT INTO variable_state "
        "(variable_id, register_variant_id, valid_from, valid_to, data_type) "
        "VALUES (?, ?, ?, ?, 'int')",
        (variable_id, register_variant_id, valid_from, valid_to),
    )
    return int(cur.lastrowid)


def _variable_id(conn: sqlite3.Connection, register_id: int, var_id: int) -> int:
    return int(
        conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = ? AND provider_key = CAST(? AS TEXT)",
            (register_id, var_id),
        ).fetchone()[0]
    )


def _add_same_as(
    conn: sqlite3.Connection,
    a: tuple[str, str, str],
    b: tuple[str, str, str],
) -> None:
    """Insert a variable_same_as edge in BOTH directions (as the build does)."""
    for src, tgt in ((a, b), (b, a)):
        conn.execute(
            "INSERT INTO variable_same_as "
            "(a_provider, a_register, a_variable, b_provider, b_register, b_variable) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (*src, *tgt),
        )


def _write_lineage_toml(
    tmp_path: Path,
    *,
    defaults: dict[str, str] | None = None,
    overrides: dict[str, dict[str, str]] | None = None,
) -> Path:
    """Write a minimal scb.toml exercising only the lineage blocks.

    `defaults` → `[lineage_defaults]`; `overrides` keyed by the dotted
    `"<consumer>.<slug>"` string → a `{source_register, source_variant}` block.
    """
    lines: list[str] = []
    if defaults:
        lines.append("[lineage_defaults]")
        lines.extend(f'{k} = "{v}"' for k, v in defaults.items())
    for key, block in (overrides or {}).items():
        lines.append(f'[lineage."{key}"]')
        lines.extend(f'{bk} = "{bv}"' for bk, bv in block.items())
    (tmp_path / "scb.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return tmp_path


def _seed_kon_registers(
    conn: sqlite3.Connection,
    *,
    source_variant_id: int = 10,
    source_variant_slug: str = "folkbokforda-personer",
    consumer_variant_id: int = 20,
) -> None:
    """RTB (source) + LISA (consumer), each with one variant and a Kön variable.

    LISA's Kön carries source_register_id = RTB. Both stored slugs are `kon`.
    Callers add the variable_state rows (the intervals under test) afterward.
    """
    add_register(conn, register_id=_SOURCE_REGISTER_ID, slug="rtb", name="RTB")
    add_variant(
        conn,
        register_variant_id=source_variant_id,
        register_id=_SOURCE_REGISTER_ID,
        slug=source_variant_slug,
        name="Folkbokförda personer",
    )
    add_variable(
        conn, register_id=_SOURCE_REGISTER_ID, var_id=44, name="Kön", slug="kon"
    )
    add_register(conn, register_id=_CONSUMER_REGISTER_ID, slug="lisa", name="LISA")
    add_variant(
        conn,
        register_variant_id=consumer_variant_id,
        register_id=_CONSUMER_REGISTER_ID,
        slug="individer",
        name="Individer",
    )
    add_variable(
        conn,
        register_id=_CONSUMER_REGISTER_ID,
        var_id=44,
        name="Kön",
        source_register_id=_SOURCE_REGISTER_ID,
        slug="kon",
    )


def _edges(conn: sqlite3.Connection) -> list[tuple[int, int, str, str]]:
    return [
        (r[0], r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT consumer_state_id, source_state_id, valid_from, valid_to "
            "FROM variable_state_lineage ORDER BY consumer_state_id, source_state_id"
        ).fetchall()
    ]


def _warnings(conn: sqlite3.Connection) -> list[tuple[int, str, str]]:
    return [
        (r[0], r[1], r[2])
        for r in conn.execute(
            "SELECT consumer_state_id, warning_kind, message "
            "FROM variable_state_lineage_warning "
            "ORDER BY consumer_state_id, warning_kind"
        ).fetchall()
    ]


class TestVariableStateLineage:
    def test_lineage_year_aligned_single_edge(self, tmp_path: Path):
        """Year-aligned consumer + source Kön states produce exactly one edge
        spanning the shared year, with no warnings (the trivial §5.6 case)."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        s_src = _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 1, "warnings_ambiguous": 0, "warnings_no_source": 0}
        assert _edges(conn) == [(s_cons, s_src, "2021-01-01", "2021-12-31")]
        assert _warnings(conn) == []

    def test_lineage_open_ended_intersection(self, tmp_path: Path):
        """Two open-ended states intersect from the later start to the shared
        '9999-12-31' sentinel."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        s_src = _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2010-01-01",
            valid_to="9999-12-31",
        )
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2015-01-01",
            valid_to="9999-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        link_variable_state_lineage(conn, slug_dir)
        assert _edges(conn) == [(s_cons, s_src, "2015-01-01", "9999-12-31")]

    def test_lineage_partial_overlap_clipped(self, tmp_path: Path):
        """Partially overlapping states clip to the intersection on both ends."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        s_src = _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
        )
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2012-01-01",
            valid_to="2018-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        link_variable_state_lineage(conn, slug_dir)
        assert _edges(conn) == [(s_cons, s_src, "2015-01-01", "2018-12-31")]

    def test_lineage_disjoint_no_edge_no_warning(self, tmp_path: Path):
        """A source state that EXISTS but does not overlap the consumer state
        yields zero edges AND zero warnings — distinct from `no_source_state`
        (the variable was found; non-overlap is a legitimate empty result)."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2015-01-01",
            valid_to="2016-12-31",
        )
        _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2008-01-01",
            valid_to="2009-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 0, "warnings_ambiguous": 0, "warnings_no_source": 0}
        assert _edges(conn) == []
        assert _warnings(conn) == []

    def test_lineage_multistate_two_edges_across_rename(self, tmp_path: Path):
        """Two consumer states across a source-side rename (RTB kon → kon-v2)
        each link to the temporally matching source state. Proves same_as
        expansion + per-state validity narrowing together."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        # RTB's renamed sibling `kon-v2` in the same variant + a same_as edge.
        add_variable(
            conn, register_id=_SOURCE_REGISTER_ID, var_id=45, name="Kön", slug="kon-v2"
        )
        _add_same_as(conn, ("scb", "rtb", "kon"), ("scb", "rtb", "kon-v2"))
        src_kon = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        src_kon_v2 = _variable_id(conn, _SOURCE_REGISTER_ID, 45)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        s_src_old = _add_state(
            conn,
            variable_id=src_kon,
            register_variant_id=10,
            valid_from="2010-01-01",
            valid_to="2014-12-31",
        )
        s_src_new = _add_state(
            conn,
            variable_id=src_kon_v2,
            register_variant_id=10,
            valid_from="2015-01-01",
            valid_to="2099-12-31",
        )
        s_cons_old = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2010-01-01",
            valid_to="2014-12-31",
        )
        s_cons_new = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2015-01-01",
            valid_to="2099-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts["edges"] == 2
        assert _edges(conn) == [
            (s_cons_old, s_src_old, "2010-01-01", "2014-12-31"),
            (s_cons_new, s_src_new, "2015-01-01", "2099-12-31"),
        ]
        assert _warnings(conn) == []

    def test_lineage_same_as_renamed_source_variable(self, tmp_path: Path):
        """The §5.6 motivating case: the source retired `kon` and now only
        ships `kon-v2`. Naive slug equality (consumer `kon` → source `kon`)
        would miss it and warn `no_source_state`; the BFS finds `kon-v2`."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        add_variable(
            conn, register_id=_SOURCE_REGISTER_ID, var_id=45, name="Kön", slug="kon-v2"
        )
        _add_same_as(conn, ("scb", "rtb", "kon"), ("scb", "rtb", "kon-v2"))
        src_kon_v2 = _variable_id(conn, _SOURCE_REGISTER_ID, 45)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        # NOTE: no state on the retired `kon` variable — only `kon-v2`.
        s_src = _add_state(
            conn,
            variable_id=src_kon_v2,
            register_variant_id=10,
            valid_from="2010-01-01",
            valid_to="9999-12-31",
        )
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2018-01-01",
            valid_to="2018-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 1, "warnings_ambiguous": 0, "warnings_no_source": 0}
        assert _edges(conn) == [(s_cons, s_src, "2018-01-01", "2018-12-31")]

    def test_lineage_ambiguous_variant_fallback_warns(self, tmp_path: Path):
        """With no pin, the linker emits edges to the matching source states
        across ALL variants and warns `ambiguous_source_variant`, naming the
        candidate variant slugs (sorted)."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        # A second source variant also carrying a Kön state.
        add_variant(
            conn,
            register_variant_id=11,
            register_id=_SOURCE_REGISTER_ID,
            slug="grund-bosattning",
            name="Grundläggande bosättning",
        )
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        s_src_a = _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        s_src_b = _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=11,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        # No [lineage_defaults] for rtb, no override → fallback.
        slug_dir = _write_lineage_toml(tmp_path)

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 2, "warnings_ambiguous": 1, "warnings_no_source": 0}
        assert _edges(conn) == [
            (s_cons, s_src_a, "2021-01-01", "2021-12-31"),
            (s_cons, s_src_b, "2021-01-01", "2021-12-31"),
        ]
        warns = _warnings(conn)
        assert len(warns) == 1
        assert warns[0][0] == s_cons
        assert warns[0][1] == "ambiguous_source_variant"
        # Message names both candidate variants, sorted.
        assert "folkbokforda-personer" in warns[0][2]
        assert "grund-bosattning" in warns[0][2]

    def test_lineage_override_pins_variant(self, tmp_path: Path):
        """A per-(consumer, slug) override pins one variant: only that
        variant's source state links, and no ambiguous warning fires."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        add_variant(
            conn,
            register_variant_id=11,
            register_id=_SOURCE_REGISTER_ID,
            slug="grund-bosattning",
            name="Grundläggande bosättning",
        )
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        s_src_b = _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=11,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path,
            overrides={
                "lisa.kon": {
                    "source_register": "rtb",
                    "source_variant": "grund-bosattning",
                }
            },
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 1, "warnings_ambiguous": 0, "warnings_no_source": 0}
        # Only the grund-bosattning state links — folkbokforda-personer ignored.
        assert _edges(conn) == [(s_cons, s_src_b, "2021-01-01", "2021-12-31")]

    def test_lineage_no_source_state_warns(self, tmp_path: Path):
        """A consumer sourced from a register with NO matching variable state
        gets a `no_source_state` warning and no edge."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        # No state seeded on RTB's `kon`; only the consumer has a state.
        s_cons = _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 0, "warnings_ambiguous": 0, "warnings_no_source": 1}
        assert _edges(conn) == []
        warns = _warnings(conn)
        assert len(warns) == 1
        assert warns[0][0] == s_cons
        assert warns[0][1] == "no_source_state"
        assert "rtb" in warns[0][2]

    def test_lineage_canonical_variable_skipped(self, tmp_path: Path):
        """A canonical (own-register) variable with source_register_id NULL is
        never a consumer — no edges, no warnings generated for its states."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)  # RTB Kön, source=NULL
        _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 0, "warnings_ambiguous": 0, "warnings_no_source": 0}

    def test_lineage_self_sourced_variable_skipped(self, tmp_path: Path):
        """source_register_id == own register_id is also excluded (a variable
        can't be its own lineage source) — guards the `!= v.register_id`
        filter, not just the NULL check above."""
        conn = _new_conn()
        add_register(conn, register_id=_SOURCE_REGISTER_ID, slug="rtb", name="RTB")
        add_variant(
            conn,
            register_variant_id=10,
            register_id=_SOURCE_REGISTER_ID,
            slug="folkbokforda-personer",
            name="Folkbokförda personer",
        )
        # Pathological: variable claims its OWN register as source.
        add_variable(
            conn,
            register_id=_SOURCE_REGISTER_ID,
            var_id=44,
            name="Kön",
            source_register_id=_SOURCE_REGISTER_ID,
            slug="kon",
        )
        vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        _add_state(
            conn,
            variable_id=vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "folkbokforda-personer"}
        )

        counts = link_variable_state_lineage(conn, slug_dir)
        assert counts == {"edges": 0, "warnings_ambiguous": 0, "warnings_no_source": 0}

    def test_lineage_override_mismatched_register_fails(self, tmp_path: Path):
        """An override whose source_register contradicts the variable's resolved
        source register is curator error → fail-fast RegMetaError naming both."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        # A third register the override wrongly points at.
        add_register(conn, register_id=3, slug="rams", name="RAMS")
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        # Variable resolves source_register_id -> rtb, but the override says rams.
        slug_dir = _write_lineage_toml(
            tmp_path,
            overrides={
                "lisa.kon": {
                    "source_register": "rams",
                    "source_variant": "individregister",
                }
            },
        )

        with pytest.raises(RegMetaError) as exc:
            link_variable_state_lineage(conn, slug_dir)
        assert exc.value.code == "lineage_override_register_mismatch"
        assert "rams" in exc.value.message
        assert "rtb" in exc.value.message

    def test_lineage_default_unknown_variant_fails(self, tmp_path: Path):
        """A [lineage_defaults] pin naming a non-existent variant is fail-fast
        (curation typo), not a silent fallback."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        src_vid = _variable_id(conn, _SOURCE_REGISTER_ID, 44)
        cons_vid = _variable_id(conn, _CONSUMER_REGISTER_ID, 44)
        _add_state(
            conn,
            variable_id=src_vid,
            register_variant_id=10,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        _add_state(
            conn,
            variable_id=cons_vid,
            register_variant_id=20,
            valid_from="2021-01-01",
            valid_to="2021-12-31",
        )
        conn.commit()
        slug_dir = _write_lineage_toml(
            tmp_path, defaults={"rtb": "nonexistent-variant"}
        )

        with pytest.raises(RegMetaError) as exc:
            link_variable_state_lineage(conn, slug_dir)
        assert exc.value.code == "lineage_pin_unknown_variant"
        assert "nonexistent-variant" in exc.value.message


class TestLoadLineageConfig:
    def test_parses_defaults_and_overrides(self, tmp_path: Path):
        (tmp_path / "scb.toml").write_text(
            '[lineage_defaults]\nrtb = "folkbokforda-personer"\niot = "bostadshushall"\n'
            '[lineage."lisa.inkomst_pension"]\n'
            'source_register = "rams"\nsource_variant = "individregister"\n',
            encoding="utf-8",
        )
        cfg = load_lineage_config(tmp_path)
        assert cfg.defaults == {
            "rtb": "folkbokforda-personer",
            "iot": "bostadshushall",
        }
        assert cfg.overrides == {
            ("lisa", "inkomst_pension"): ("rams", "individregister")
        }

    def test_classifications_toml_skipped(self, tmp_path: Path):
        """`classifications.toml` carries no lineage blocks and must be skipped
        (it has a different top-level grammar)."""
        (tmp_path / "scb.toml").write_text(
            '[lineage_defaults]\nrtb = "folkbokforda-personer"\n', encoding="utf-8"
        )
        (tmp_path / "classifications.toml").write_text(
            '[classification."SUN2020"]\nslug = "sun"\n', encoding="utf-8"
        )
        cfg = load_lineage_config(tmp_path)
        assert cfg.defaults == {"rtb": "folkbokforda-personer"}

    def test_missing_source_variant_in_override_fails(self, tmp_path: Path):
        (tmp_path / "scb.toml").write_text(
            '[lineage."lisa.kon"]\nsource_register = "rtb"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            load_lineage_config(tmp_path)
        assert exc.value.code == "lineage_override_incomplete"

    def test_duplicate_default_across_files_fails(self, tmp_path: Path):
        (tmp_path / "scb.toml").write_text(
            '[lineage_defaults]\nrtb = "folkbokforda-personer"\n', encoding="utf-8"
        )
        (tmp_path / "sos.toml").write_text(
            '[lineage_defaults]\nrtb = "grund-bosattning"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            load_lineage_config(tmp_path)
        assert exc.value.code == "lineage_default_duplicate"

    def test_empty_dir_yields_empty_config(self, tmp_path: Path):
        cfg = load_lineage_config(tmp_path)
        assert cfg.defaults == {}
        assert cfg.overrides == {}


class TestVariableSetViaSameAs:
    # The linker starts the BFS at the SOURCE-side identity node
    # (source_provider, source_register, consumer_slug), so these mirror that
    # call shape: start == ("scb", "rtb", "kon"), target == "rtb".

    def test_identity_only_when_no_same_as_edge(self, tmp_path: Path):
        """No same_as edge (the common no-rename case) → just the identity slug
        in the source register. NOT empty: the source-side identity match is
        the 99% path; an empty result would be `no_source_state` for every
        un-renamed consumer."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        conn.commit()
        result = _variable_set_via_same_as(conn, "scb", "rtb", "kon", "rtb")
        assert result == {"kon"}

    def test_finds_renamed_source_slug(self, tmp_path: Path):
        """A rename edge inside the source register (RTB kon ↔ kon-v2) adds the
        renamed sibling to the identity match."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        add_variable(
            conn, register_id=_SOURCE_REGISTER_ID, var_id=45, name="Kön", slug="kon-v2"
        )
        _add_same_as(conn, ("scb", "rtb", "kon"), ("scb", "rtb", "kon-v2"))
        conn.commit()
        result = _variable_set_via_same_as(conn, "scb", "rtb", "kon", "rtb")
        assert result == {"kon", "kon-v2"}

    def test_excludes_other_register_nodes(self, tmp_path: Path):
        """Reachable nodes in a DIFFERENT register are not in the result — only
        target-register slugs count (a cross-provider edge to a non-source
        register must not leak in)."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        # A same_as edge from the source identity to a LISA-side variable.
        _add_same_as(conn, ("scb", "rtb", "kon"), ("scb", "lisa", "kon"))
        conn.commit()
        result = _variable_set_via_same_as(conn, "scb", "rtb", "kon", "rtb")
        # lisa.kon is reachable but not in the target register → excluded.
        assert result == {"kon"}

    def test_cycle_in_materialized_table_terminates(self, tmp_path: Path):
        """A↔B↔C↔A in the materialized (both-directions) table must terminate
        and return the reachable target-register slugs without looping."""
        conn = _new_conn()
        _seed_kon_registers(conn)
        add_variable(
            conn, register_id=_SOURCE_REGISTER_ID, var_id=45, name="Kön", slug="kon-b"
        )
        add_variable(
            conn, register_id=_SOURCE_REGISTER_ID, var_id=46, name="Kön", slug="kon-c"
        )
        # Dense triangle among RTB variables. Every _add_same_as already stores
        # both directions, so each pair is a 2-cycle; the triangle adds a 3-cycle.
        _add_same_as(conn, ("scb", "rtb", "kon"), ("scb", "rtb", "kon-b"))
        _add_same_as(conn, ("scb", "rtb", "kon-b"), ("scb", "rtb", "kon-c"))
        _add_same_as(conn, ("scb", "rtb", "kon-c"), ("scb", "rtb", "kon"))
        conn.commit()
        result = _variable_set_via_same_as(conn, "scb", "rtb", "kon", "rtb")
        assert result == {"kon", "kon-b", "kon-c"}
