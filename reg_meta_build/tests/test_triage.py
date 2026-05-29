"""Tests for §5.7 build-time triage (fold / split / collapse).

The pure decision helpers are unit-tested directly; the fold/split integration
is exercised through `build_db` with crafted Registerinformation rows that put
one source `var_id` under multiple delivery columns. These builds run with
`skip_slugs=True`, so they assert the *structural* triage outcome (sibling
`variable` rows, per-state `value_set_version_label` labels, the uniqueness
index). Slug derivation (fold stem, sibling slugs) and `variable_related_to`
edge materialization need the slug pipeline and are covered by the real
`build-db` validation (they no-op under `skip_slugs`).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from _csv_fixtures import (
    REGISTERINFORMATION_ROWS,
    _ri_row,
    write_scb_input,
)
from reg_meta.db import open_db
from reg_meta_build.db import (
    _common_prefix_len,
    _decide_fold_or_split,
    _fold_token_from_grain,
    build_db,
)


def _var_row(
    *,
    colname: str,
    cvid: int,
    var_id: int,
    varname: str = "GenericVar",
    year: str = "2020",
    regver_id: int = 110,
    data_type: str = "int",
    data_length: str = "1",
) -> str:
    """A Registerinformation row for register TESTREG (register_id 1, variant
    register_variant_id 10), varying only the fields triage keys on."""
    return _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        year,
        f"Version {year}",
        "",
        "Godkänd",
        f"{year}-01-01",
        f"{year}-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        f"{year}-12-31",
        "Person",
        "Fysisk person",
        varname,
        "A generic family label",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        colname,
        data_type,
        data_length,
        str(cvid),
        "1",
        "10",
        str(regver_id),
        str(var_id),
    )


def _build(tmp_path: Path, extra_rows: list[str]) -> sqlite3.Connection:
    ri_rows = list(REGISTERINFORMATION_ROWS) + extra_rows
    input_dir = tmp_path / "input"
    write_scb_input(input_dir, registerinformation_rows=ri_rows)
    db_dir = tmp_path / "db"
    build_db(
        input_dir=input_dir,
        db_dir=db_dir,
        skip_classifications=True,
        skip_slugs=True,
    )
    return open_db(db_dir / "reg_meta.db", check_schema=False)


# ── pure decision helpers ─────────────────────────────────────────────────


class TestFoldTokenFromGrain:
    def test_position_grain(self) -> None:
        assert _fold_token_from_grain("5 positioner") == "5pos"
        assert _fold_token_from_grain("3 position") == "3pos"

    def test_named_grains(self) -> None:
        assert _fold_token_from_grain("Grov gruppering") == "grov"
        assert _fold_token_from_grain("Detaljgrupp") == "detalj"
        assert _fold_token_from_grain("NivaOld") == "old"

    def test_no_match_returns_none(self) -> None:
        assert _fold_token_from_grain("") is None
        assert _fold_token_from_grain(None) is None
        assert _fold_token_from_grain("something else") is None


class TestCommonPrefixLen:
    def test_shared_stem(self) -> None:
        assert _common_prefix_len(["ssyk3", "ssyk5"]) == 4
        assert _common_prefix_len(["ftgsni69", "ftgsni92"]) == 6

    def test_disjoint(self) -> None:
        assert _common_prefix_len(["hemkommun", "skolkommun"]) == 0

    def test_edge_cases(self) -> None:
        assert _common_prefix_len([]) == 0
        assert _common_prefix_len(["x"]) == 1


class TestDecideFoldOrSplit:
    def test_shared_stem_folds(self) -> None:
        assert _decide_fold_or_split(["ssyk3", "ssyk5"], set()) == "fold"
        assert _decide_fold_or_split(["bciv", "bcivred"], set()) == "fold"

    def test_disjoint_stems_split(self) -> None:
        assert _decide_fold_or_split(["hemkommun", "skolkommun"], set()) == "split"
        assert _decide_fold_or_split(["lid", "lnamn"], set()) == "split"

    def test_one_classification_family_folds(self) -> None:
        # Even disjoint stems fold when they're one classification family.
        assert _decide_fold_or_split(["foo", "bar"], {7}) == "fold"

    def test_multiple_classification_families_split(self) -> None:
        assert _decide_fold_or_split(["foosni", "foosni2"], {7, 9}) == "split"


# ── integration: fold / split / collapse + uniqueness index ───────────────


class TestSplit:
    """Disjoint columns under one var_id → distinct sibling `variable` rows."""

    def test_split_mints_sibling_variables(self, tmp_path: Path) -> None:
        conn = _build(
            tmp_path,
            [
                _var_row(colname="Hemkommun", cvid=9300, var_id=920),
                _var_row(colname="Skolkommun", cvid=9301, var_id=920),
            ],
        )
        # Two variables share provider_key '920' (a split, §5.7 DECISION POINT 1).
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '920'"
        ).fetchall()
        assert len(sibs) == 2, "split should mint a second sibling variable"

    def test_split_states_route_to_distinct_siblings(self, tmp_path: Path) -> None:
        conn = _build(
            tmp_path,
            [
                _var_row(colname="Hemkommun", cvid=9300, var_id=920),
                _var_row(colname="Skolkommun", cvid=9301, var_id=920),
            ],
        )
        # Each sibling owns the state for its own column.
        rows = conn.execute(
            "SELECT vs.variable_id, vs.delivery_column_name FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '920' "
            "ORDER BY vs.delivery_column_name"
        ).fetchall()
        cols = [r["delivery_column_name"] for r in rows]
        vids = {r["variable_id"] for r in rows}
        assert cols == ["Hemkommun", "Skolkommun"]
        assert len(vids) == 2, "each disjoint column resolves to its own sibling"


class TestFold:
    """Stem-sharing columns under one var_id → one variable, labeled states."""

    def test_fold_keeps_one_variable(self, tmp_path: Path) -> None:
        conn = _build(
            tmp_path,
            [
                _var_row(colname="Ssyk3", cvid=9400, var_id=930),
                _var_row(colname="Ssyk5", cvid=9401, var_id=930),
            ],
        )
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '930'"
        ).fetchall()
        assert len(sibs) == 1, "a fold must NOT mint siblings"

    def test_fold_states_get_distinct_labels(self, tmp_path: Path) -> None:
        conn = _build(
            tmp_path,
            [
                _var_row(colname="Ssyk3", cvid=9400, var_id=930),
                _var_row(colname="Ssyk5", cvid=9401, var_id=930),
            ],
        )
        rows = conn.execute(
            "SELECT vs.value_set_version_label, vs.delivery_column_name "
            "FROM variable_state vs JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '930' "
            "ORDER BY vs.delivery_column_name"
        ).fetchall()
        assert len(rows) == 2, "fold keeps both representation states"
        labels = [r["value_set_version_label"] for r in rows]
        assert len(set(labels)) == 2, f"folded states need distinct labels: {labels}"
        assert all(lbl for lbl in labels), "each folded state carries a token"


class TestUniquenessIndex:
    """The §5.7 state-uniqueness index is created post-triage and is live."""

    def test_index_exists(self, tmp_path: Path) -> None:
        conn = _build(tmp_path, [])
        row = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND name = 'idx_variable_state_unique'"
        ).fetchone()
        assert row is not None

    def test_index_rejects_duplicate_state(self, tmp_path: Path) -> None:
        # Reopen writable to attempt a duplicate insert.
        conn = _build(tmp_path, [])
        path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        conn.close()
        rw = sqlite3.connect(path)
        existing = rw.execute(
            "SELECT variable_id, register_variant_id, valid_from, "
            "value_set_version_label FROM variable_state LIMIT 1"
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            rw.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, value_set_version_label) "
                "VALUES (?, ?, ?, '9999-12-31', ?)",
                existing,
            )
        rw.close()
