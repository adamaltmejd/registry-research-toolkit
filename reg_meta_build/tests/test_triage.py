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
    _var_row,
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

    def test_alias_column_ties_to_owning_sibling(self, tmp_path: Path) -> None:
        # A2.7 reparent precision (PR #149 P1): each split sibling's
        # `variable_alias` (the source `get_datacolumns` reads, filtered by
        # `variable_id`) must carry ONLY that sibling's delivery column — NOT
        # every column sharing the non-unique `(register_id, provider_key)`.
        # Pre-fix the bare provider_key join fanned each cvid's column onto all
        # siblings, so this asserted both columns under each sibling.
        conn = _build(
            tmp_path,
            [
                _var_row(colname="Hemkommun", cvid=9300, var_id=920),
                _var_row(colname="Skolkommun", cvid=9301, var_id=920),
            ],
        )
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '920' "
            "ORDER BY variable_id"
        ).fetchall()
        assert len(sibs) == 2
        # Each sibling's alias set == its single owning column (the §5.7 query
        # `get_datacolumns` runs: SELECT delivery_column_name … WHERE variable_id=?).
        alias_by_vid = {
            r["variable_id"]: sorted(
                a["delivery_column_name"]
                for a in conn.execute(
                    "SELECT delivery_column_name FROM variable_alias "
                    "WHERE variable_id = ?",
                    (r["variable_id"],),
                )
            )
            for r in sibs
        }
        owned_cols = sorted(cols for cols in alias_by_vid.values())
        assert owned_cols == [["Hemkommun"], ["Skolkommun"]], (
            f"each sibling must own exactly its column, got {alias_by_vid}"
        )
        # Union across siblings preserves full recall (no column lost).
        all_cols = {c for cols in alias_by_vid.values() for c in cols}
        assert all_cols == {"Hemkommun", "Skolkommun"}

    def test_non_contested_column_gets_own_variable(self, tmp_path: Path) -> None:
        # Hemkommun + Skolkommun collide in 2020 (split). Telefon is delivered
        # ALONE in 2019 (non-contested). Once the var_id is a split container,
        # Telefon must get its OWN variable, NOT default onto the lex-first
        # sibling (Hemkommun) — else its history is mis-attributed (Codex P2).
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="Hemkommun",
                    cvid=9600,
                    var_id=950,
                    year="2020",
                    regver_id=120,
                ),
                _var_row(
                    colname="Skolkommun",
                    cvid=9601,
                    var_id=950,
                    year="2020",
                    regver_id=120,
                ),
                _var_row(
                    colname="Telefon", cvid=9602, var_id=950, year="2019", regver_id=119
                ),
            ],
        )
        rows = conn.execute(
            "SELECT v.variable_id, vs.delivery_column_name FROM variable v "
            "JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '950'"
        ).fetchall()
        by_col = {r["delivery_column_name"]: r["variable_id"] for r in rows}
        assert set(by_col) == {"Hemkommun", "Skolkommun", "Telefon"}
        assert len(set(by_col.values())) == 3, (
            "non-contested column needs its own variable"
        )
        assert by_col["Telefon"] not in (by_col["Hemkommun"], by_col["Skolkommun"])

    def test_shared_edition_across_years_splits(self, tmp_path: Path) -> None:
        # Hemkommun is delivered in the 2018 AND 2019 editions; Skolkommun only
        # in 2019. They share the 2019 edition (regver 122) → contested → split,
        # even though Hemkommun's valid_from is 2018 — contested detection
        # buckets by edition, not lower-bound year (Codex #139).
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="Hemkommun",
                    cvid=9700,
                    var_id=960,
                    year="2018",
                    regver_id=121,
                ),
                _var_row(
                    colname="Hemkommun",
                    cvid=9701,
                    var_id=960,
                    year="2019",
                    regver_id=122,
                ),
                _var_row(
                    colname="Skolkommun",
                    cvid=9702,
                    var_id=960,
                    year="2019",
                    regver_id=122,
                ),
            ],
        )
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '960'"
        ).fetchall()
        assert len(sibs) == 2, "shared-edition co-delivery must split"

    def test_subannual_term_rename_does_not_split(self, tmp_path: Path) -> None:
        # Two distinct editions in the SAME calendar year (a sub-annual variant,
        # e.g. HT2018 then VT2018). A term-to-term column rename is sequential,
        # not co-delivered — bucketing by edition (not year) must NOT split it
        # into siblings (Codex #139).
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="OldKol", cvid=9800, var_id=970, year="2018", regver_id=130
                ),
                _var_row(
                    colname="NewKol", cvid=9801, var_id=970, year="2018", regver_id=131
                ),
            ],
        )
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '970'"
        ).fetchall()
        assert len(sibs) == 1, (
            "a term-to-term rename (distinct editions) must NOT split"
        )


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


class TestVariableRelatedToEdges:
    """Maintainer: the `variable_related_to` materializer (both-direction
    (N choose 2) edges, FQID endpoints, `note='auto:triage'`, skip-on-missing-slug)
    is no-op'd under skip_slugs, so cover it directly."""

    @staticmethod
    def _split_db(tmp_path: Path) -> sqlite3.Connection:
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
        conn.row_factory = sqlite3.Row
        conn.execute("UPDATE register SET slug = 'testreg' WHERE register_id = 1")
        return conn

    def test_edges_have_fqid_endpoints_both_directions(self, tmp_path: Path) -> None:
        from reg_meta_build.db import _materialize_variable_related_to

        conn = self._split_db(tmp_path)
        prov = conn.execute(
            "SELECT p.slug FROM provider p JOIN register r ON r.provider_id = "
            "p.provider_id WHERE r.register_id = 1"
        ).fetchone()[0]
        sibs = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 "
            "AND provider_key = '920' ORDER BY variable_id"
        ).fetchall()
        a, b = sibs[0]["variable_id"], sibs[1]["variable_id"]
        conn.execute(
            "UPDATE variable SET slug = 'kommun-hem' WHERE variable_id = ?", (a,)
        )
        conn.execute(
            "UPDATE variable SET slug = 'kommun-skol' WHERE variable_id = ?", (b,)
        )
        conn.commit()

        n = _materialize_variable_related_to(
            conn, [(a, b, "same_definition_different_column")]
        )
        assert n == 2  # (N choose 2) × both directions
        rows = conn.execute(
            "SELECT a_provider, a_register, a_variable, b_provider, b_register, "
            "b_variable, relation_kind, note FROM variable_related_to"
        ).fetchall()
        assert {(r["a_variable"], r["b_variable"]) for r in rows} == {
            ("kommun-hem", "kommun-skol"),
            ("kommun-skol", "kommun-hem"),
        }
        for r in rows:
            assert (r["a_provider"], r["a_register"]) == (prov, "testreg")
            assert (r["b_provider"], r["b_register"]) == (prov, "testreg")
            assert r["relation_kind"] == "same_definition_different_column"
            assert r["note"] == "auto:triage"
        conn.close()

    def test_edge_skipped_when_a_slug_is_missing(self, tmp_path: Path) -> None:
        from reg_meta_build.db import _materialize_variable_related_to

        conn = self._split_db(tmp_path)
        sibs = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 "
            "AND provider_key = '920' ORDER BY variable_id"
        ).fetchall()
        a, b = sibs[0]["variable_id"], sibs[1]["variable_id"]
        # Only one sibling gets a slug; the edge has a NULL endpoint → skipped,
        # not inserted with a NULL (which would corrupt the FQID).
        conn.execute(
            "UPDATE variable SET slug = 'kommun-hem' WHERE variable_id = ?", (a,)
        )
        conn.commit()
        n = _materialize_variable_related_to(
            conn, [(a, b, "same_definition_different_column")]
        )
        assert n == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM variable_related_to").fetchone()[0] == 0
        )
        conn.close()


def _seed_split_for_backfill(conn: sqlite3.Connection) -> tuple[int, int, int, int]:
    """Seed the PRE-ship state for the classification backfill: two split
    siblings sharing `provider_key` '920' (distinct columns + distinct
    classifications), each with its `variable_instance` + `variable_alias_build`
    rows and a NULL-classification `variable_state`.

    Mirrors what the build pipeline holds right before
    `_backfill_state_classifications` runs (coalescer done, classifications
    populated, `variable_instance` not yet dropped). Returns
    ``(variable_id_a, variable_id_b, cls_a, cls_b)``.

    The two states are CODE-LESS (`value_set_id` NULL on both states and both
    cvids) — the §5.7 "version-label-tagged family with no value codes" case the
    backfill docstring calls out. This is what defeats the old
    `(register_id, provider_key)` join: its only discriminator was
    `vi.value_set_id IS variable_state.value_set_id`, and `NULL IS NULL` matches,
    so every cvid's classification fanned onto every sibling's state. (With
    distinct non-NULL value sets the old query would coincidentally separate the
    siblings, hiding the bug; NULL is the faithful failing case.) Each cvid
    carries its OWNING `variable_id` (the ground truth the coalescer stamps onto
    `variable_instance.variable_id` post-triage; set directly here), and the
    backfill attributes by that — NOT by value_set_id — so the siblings stay
    isolated regardless.
    """
    from reg_meta_build.db import DDL, seed_providers

    conn.executescript(DDL)
    seed_providers(conn)
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, name) "
        "VALUES (1, 1, 'testreg', 'TESTREG')"
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, slug, name) "
        "VALUES (10, 1, 'individer', 'Individer')"
    )
    # Two classifications, one owned by each sibling. cls_a has the LOWER id, so
    # the old cross-attribution's min() tie-break pulls cls_a onto sibling B's
    # state too (B owns cls_b) — the observable leak.
    cls_a = conn.execute(
        "INSERT INTO classification (short_name, name) VALUES ('KOMMUN_HEM', 'Hemkommun')"
    ).lastrowid
    cls_b = conn.execute(
        "INSERT INTO classification (short_name, name) VALUES ('KOMMUN_SKOL', 'Skolkommun')"
    ).lastrowid
    # Sibling A (Hemkommun) and B (Skolkommun): same provider_key '920'.
    vid_a = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (1, '920', 'Kommun', 'kommun-hem')"
    ).lastrowid
    vid_b = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (1, '920', 'Kommun', 'kommun-skol')"
    ).lastrowid
    # Coalesced states — classification_id + value_set_id NULL (code-less).
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES (?, 10, '2018-01-01', '9999-12-31', 'int', 'Hemkommun')",
        (vid_a,),
    )
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES (?, 10, '2018-01-01', '9999-12-31', 'int', 'Skolkommun')",
        (vid_b,),
    )
    # Pre-ship cvids: each carries its OWN classification + OWNING variable_id
    # (the coalescer's post-triage stamp), value_set_id NULL. Both share
    # provider_key 920; the variable_id stamp is what isolates the siblings.
    conn.execute(
        "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
        "regver_id, var_id, classification_id, variable_id) "
        "VALUES (9300, 1, 10, 100, 920, ?, ?)",
        (cls_a, vid_a),
    )
    conn.execute(
        "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
        "regver_id, var_id, classification_id, variable_id) "
        "VALUES (9301, 1, 10, 100, 920, ?, ?)",
        (cls_b, vid_b),
    )
    conn.executemany(
        "INSERT INTO variable_alias_build (cvid, delivery_column_name) VALUES (?, ?)",
        [(9300, "Hemkommun"), (9301, "Skolkommun")],
    )
    conn.commit()
    return vid_a, vid_b, cls_a, cls_b


class TestBackfillStateClassifications:
    """A2.7 [P2]: `_backfill_state_classifications` must attribute each cvid's
    classification to its OWNING split sibling (by the coalescer-stamped
    `variable_instance.variable_id`), not fan it across every sibling sharing the
    non-unique `(register_id, provider_key)`."""

    def test_each_sibling_gets_own_classification(self) -> None:
        from reg_meta_build.db import _backfill_state_classifications

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        vid_a, vid_b, cls_a, cls_b = _seed_split_for_backfill(conn)

        _backfill_state_classifications(conn)

        got = {
            r["variable_id"]: r["classification_id"]
            for r in conn.execute(
                "SELECT variable_id, classification_id FROM variable_state"
            )
        }
        # Sibling A's state carries cls_a; B's carries cls_b. Pre-fix the bare
        # provider_key join fanned both cvids onto both siblings; the min()
        # tie-break then put cls_a (lower id) on B's state too.
        assert got[vid_a] == cls_a
        assert got[vid_b] == cls_b
        conn.close()

    def test_classifications_for_variable_is_sibling_isolated(self) -> None:
        from reg_meta.queries import classifications_for_variable
        from reg_meta_build.db import _backfill_state_classifications

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        vid_a, vid_b, cls_a, cls_b = _seed_split_for_backfill(conn)
        _backfill_state_classifications(conn)

        a_cls = classifications_for_variable(conn, vid_a)
        b_cls = classifications_for_variable(conn, vid_b)
        assert [c["id"] for c in a_cls] == [cls_a]  # only A's, not B's
        assert [c["id"] for c in b_cls] == [cls_b]
        conn.close()


class TestReparentVariableAlias:
    """Post-#149: `_reparent_variable_alias` attributes each cvid's delivery
    columns to its OWNING split sibling via the coalescer-stamped
    `variable_instance.variable_id` — no column-tie heuristic, no skip."""

    def test_split_siblings_get_own_columns_incl_historical(self) -> None:
        """A cvid whose ONLY column is a historical one that never became a
        coalesced `variable_state` is still attributed to its owning sibling. The
        old column-tie SKIPPED such a cvid (its columns resolved to no
        state-bearing sibling), dropping the column from `variable_alias`; the
        ground-truth `variable_id` stamp recovers it under the right sibling."""
        from reg_meta_build.db import DDL, _reparent_variable_alias, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (1, 1, 'testreg', 'TESTREG')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, name) "
            "VALUES (10, 1, 'individer', 'Individer')"
        )
        # Two split siblings sharing provider_key '920'.
        vid_a = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '920', 'Kommun', 'kommun-hem')"
        ).lastrowid
        vid_b = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '920', 'Kommun', 'kommun-skol')"
        ).lastrowid
        # Surviving latest-era states, one delivery column each.
        conn.executemany(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name) "
            "VALUES (?, 10, '2020-01-01', '9999-12-31', ?)",
            [(vid_a, "Hemkommun"), (vid_b, "Skolkommun")],
        )
        # cvids stamped with their owning variable_id (coalescer ground truth).
        # cvid 9302 carries ONLY 'Hemkn_old' — a historical column for sibling A
        # that never became a state (the previously-skipped case).
        conn.executemany(
            "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
            "regver_id, var_id, variable_id) VALUES (?, 1, 10, 100, 920, ?)",
            [(9300, vid_a), (9301, vid_b), (9302, vid_a)],
        )
        conn.executemany(
            "INSERT INTO variable_alias_build (cvid, delivery_column_name) VALUES (?, ?)",
            [(9300, "Hemkommun"), (9301, "Skolkommun"), (9302, "Hemkn_old")],
        )
        conn.commit()

        _reparent_variable_alias(conn)

        cols = {
            (r["variable_id"], r["delivery_column_name"])
            for r in conn.execute(
                "SELECT variable_id, delivery_column_name FROM variable_alias"
            )
        }
        # A keeps its current + historical column; B only its own. Neither
        # sibling leaks the other's column, and 'Hemkn_old' is recovered.
        assert cols == {
            (vid_a, "Hemkommun"),
            (vid_a, "Hemkn_old"),
            (vid_b, "Skolkommun"),
        }
        conn.close()
