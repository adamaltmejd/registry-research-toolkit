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
from collections import Counter
from pathlib import Path

import pytest
from _csv_fixtures import (
    REGISTERINFORMATION_ROWS,
    _ri_row,
    write_scb_input,
)
from reg_meta.db import open_db
from reg_meta_build.db import (
    _apply_fold,
    _collapse_residual,
    _common_prefix_len,
    _decide_fold_or_split,
    _fold_token_from_grain,
    _StateGroup,
    _TriageResult,
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


class TestNoSplitOnColumnRename:
    """Codex P1 #139: columns that never co-occur in the same (variant, year) —
    e.g. SCB renaming a delivery column between editions — are ONE longitudinal
    variable, NOT a split (triage acts only on real same-year collisions)."""

    def test_renamed_column_stays_one_variable(self, tmp_path: Path) -> None:
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="OldKol", cvid=9500, var_id=940, year="2018", regver_id=118
                ),
                _var_row(
                    colname="NewKol", cvid=9501, var_id=940, year="2019", regver_id=119
                ),
            ],
        )
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '940'"
        ).fetchall()
        assert len(sibs) == 1, "a cross-year column rename must NOT split"
        # Both editions' states survive on the single variable.
        n_states = conn.execute(
            "SELECT COUNT(*) FROM variable_state vs JOIN variable v "
            "ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '940'"
        ).fetchone()[0]
        assert n_states == 2, "both renamed-column editions should remain as states"


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


class TestCollapseResidual:
    """Unit regression: the residual-collapse tiebreaker must tolerate a mixed
    None/int `value_set_id` across groups in one scope. A raw-gkey tiebreaker
    raised `'<' not supported between int and NoneType` on the real corpus
    (value_set_id is gkey position 5, int | None); fixtures never mixed them."""

    @staticmethod
    def _grp(value_set_id: int | None, regver_max: int) -> _StateGroup:
        return _StateGroup(
            register_id=1,
            register_variant_id=10,
            var_id=44,
            data_type="int",
            data_length="",
            value_set_id=value_set_id,
            value_set_version_label="",
            regver_min=2018,
            regver_max=regver_max,
            latest_alias="Kon",
        )

    def test_mixed_value_set_id_collapses_without_raising(self) -> None:
        # One column, one (variable_id, variant, year) scope, two groups whose
        # only difference is value_set_id (None vs 5) — pure code-list drift.
        gk_none = (1, 10, 44, "int", "", None, "", "", "kon")
        gk_int = (1, 10, 44, "int", "", 5, "", "", "kon")
        groups = {gk_none: self._grp(None, 2018), gk_int: self._grp(5, 2019)}
        res = _TriageResult(
            assignments={gk_none: 1, gk_int: 1},
            labels={},
            dropped=set(),
            fold_slug_hints={},
            related_edges=[],
            stats=Counter(),
        )
        _collapse_residual(groups, res)  # must not raise on None vs int
        # Drift collapses to one; the latest-era group (regver_max 2019) wins.
        assert len(res.dropped) == 1
        assert gk_int not in res.dropped


class TestFoldSlugHint:
    """A fold whose shared stem isn't a valid slug must NOT emit a hint — real
    build hit `slug_toml_invalid` for an all-digit stem (`2501`/`2502` → `250`),
    which violates the §5.2 grammar (slugs start with a letter)."""

    def test_digit_stem_emits_no_hint(self) -> None:
        gk1 = (1, 10, 99, "int", "", None, "", "", "2501")
        gk2 = (1, 10, 99, "int", "", None, "", "", "2502")

        def grp() -> _StateGroup:
            return _StateGroup(
                register_id=1,
                register_variant_id=10,
                var_id=99,
                data_type="int",
                data_length="",
                value_set_id=None,
                value_set_version_label="",
                regver_min=2020,
                regver_max=2020,
            )

        groups = {gk1: grp(), gk2: grp()}
        res = _TriageResult(
            assignments={gk1: 5, gk2: 5},
            labels={},
            dropped=set(),
            fold_slug_hints={},
            related_edges=[],
            stats=Counter(),
        )
        _apply_fold(
            groups,
            {"2501": [gk1], "2502": [gk2]},
            ["2501", "2502"],
            ["2501", "2502"],
            5,
            res,
        )
        # No valid slug derivable from the digit stem → no hint (falls back to
        # the name/provider_key chain in populate_variable_slugs).
        assert 5 not in res.fold_slug_hints
        # Folded states still get distinct labels (digit tokens are fine on the
        # label — only slugs carry the letter-start grammar).
        assert res.labels[gk1] != res.labels[gk2]

    def test_alpha_stem_emits_valid_hint(self) -> None:
        gk1 = (1, 10, 99, "int", "", None, "", "", "Ssyk3")
        gk2 = (1, 10, 99, "int", "", None, "", "", "Ssyk5")

        def grp() -> _StateGroup:
            return _StateGroup(
                register_id=1,
                register_variant_id=10,
                var_id=99,
                data_type="int",
                data_length="",
                value_set_id=None,
                value_set_version_label="",
                regver_min=2020,
                regver_max=2020,
            )

        groups = {gk1: grp(), gk2: grp()}
        res = _TriageResult(
            assignments={gk1: 5, gk2: 5},
            labels={},
            dropped=set(),
            fold_slug_hints={},
            related_edges=[],
            stats=Counter(),
        )
        _apply_fold(
            groups,
            {"Ssyk3": [gk1], "Ssyk5": [gk2]},
            ["Ssyk3", "Ssyk5"],
            ["ssyk3", "ssyk5"],
            5,
            res,
        )
        assert res.fold_slug_hints[5] == "ssyk"


class TestSplitSiblingSlugCache:
    """Split siblings share a `provider_key`, so the auto-slug cache must key on
    a discriminator — else a persisted `auto.toml` re-applies one slug to every
    sibling on the next build and trips `UNIQUE(register_id, slug)` (Codex P2)."""

    def test_sibling_slugs_stable_across_rebuild(self, tmp_path: Path) -> None:
        from reg_meta_build.fqid_slugs import populate_variable_slugs

        ri = list(REGISTERINFORMATION_ROWS) + [
            _var_row(colname="Hemkommun", cvid=9300, var_id=920),
            _var_row(colname="Skolkommun", cvid=9301, var_id=920),
        ]
        input_dir = tmp_path / "input"
        write_scb_input(input_dir, registerinformation_rows=ri)
        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        conn = sqlite3.connect(db_dir / "reg_meta.db")

        def sibling_slugs() -> dict[int, str]:
            return dict(
                conn.execute(
                    "SELECT variable_id, slug FROM variable "
                    "WHERE register_id = 1 AND provider_key = '920'"
                ).fetchall()
            )

        # First build derives + persists auto slugs.
        populate_variable_slugs(conn, slug_dir)
        conn.commit()
        first = sibling_slugs()
        assert len(set(first.values())) == 2, "siblings need distinct slugs"

        # Second build reads the just-written auto.toml and re-applies it — a
        # shared cache key would collapse both siblings onto one slug here and
        # raise IntegrityError on the UNIQUE index.
        populate_variable_slugs(conn, slug_dir)
        conn.commit()
        second = sibling_slugs()
        conn.close()
        assert first == second, "auto slugs must be immutable across rebuilds"
        assert len(set(second.values())) == 2


class TestSplitSiblingSameAsAnchor:
    """Codex P2 #139: a curated 3-part `[variable."<reg>.<var>.<disc>"]` key
    resolves to the SPECIFIC split sibling for same_as anchoring. The disc is
    the same `_split_sibling_disc` the auto-slug cache uses, so curator and cache
    agree on which sibling a key names."""

    def test_three_part_key_resolves_specific_sibling(self, tmp_path: Path) -> None:
        from reg_meta.errors import RegMetaError

        from reg_meta_build.fqid_slugs import (
            SlugEntry,
            _split_sibling_disc,
            _variable_source_slug,
        )

        ri = list(REGISTERINFORMATION_ROWS) + [
            _var_row(colname="Hemkommun", cvid=9300, var_id=920),
            _var_row(colname="Skolkommun", cvid=9301, var_id=920),
        ]
        input_dir = tmp_path / "input"
        write_scb_input(input_dir, registerinformation_rows=ri)
        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = sqlite3.connect(db_dir / "reg_meta.db")

        disc = _split_sibling_disc(conn, 1, 920)
        assert len(set(disc.values())) == 2, "siblings need distinct discriminators"
        # Give each sibling a known slug, then resolve it via its 3-part key.
        for vid, d in disc.items():
            conn.execute(
                "UPDATE variable SET slug = ? WHERE variable_id = ?", (f"v-{d}", vid)
            )
        conn.commit()
        for d in disc.values():
            entry = SlugEntry(
                kind="variable", source_id=f"1.920.{d}", slug=None, provider="scb"
            )
            assert _variable_source_slug(conn, 1, 920, entry) == f"v-{d}"
        # A bare 2-part key on a split var_id stays ambiguous → rejected.
        bare = SlugEntry(kind="variable", source_id="1.920", slug=None, provider="scb")
        with pytest.raises(RegMetaError):
            _variable_source_slug(conn, 1, 920, bare)
        conn.close()
