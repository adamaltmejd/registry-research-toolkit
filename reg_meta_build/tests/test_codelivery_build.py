"""End-to-end coverage for the co-delivery cascade (see DESIGN.md → Build-time triage (SCB)) in
`_coalesce_variable_states` — the build-side glue the per-resolver unit tests in
`test_triage.py` don't reach: timeline routing (`_spans_overlap`), per-year
resolution, RLE run emission + the supersession carve, and the build-time FAIL
branch (`coalesce_unresolved_codelivery`) that backstops the
`(variable, variant, period, column) → one value set` invariant.

Fully synthetic (CLAUDE.md): each test augments the standard SCB CSV fixture with
one extra variable in TESTREG/individer and runs a real `build_db`, then asserts
the emitted `variable_state` windows (clean carve) or that the build fails
(genuine same-column conflict). The repo `codelivery.toml` is keyed on real SCB
register ids, so it never accidentally cures these synthetic register_id=1 cases.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    PIPE,
    REGISTERINFORMATION_ROWS,
    VARDEMANGDER_ROWS,
    _var_row,
    write_scb_input,
)
from _shared_fixtures import _write_fixture_slug_dir
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.db import build_db

if TYPE_CHECKING:
    from pathlib import Path


def _vm_rows(cvid: int, version: str, codes: list[tuple[str, str]]) -> list[str]:
    """Vardemangder rows for one cvid: [version, niva, kod, benämning, CVID, ItemId].
    `niva="1"` is a non-historical grain (matches the default fixture); ItemId is
    left empty (the importer accepts it, and no ValidDates row means always-valid).
    The value_set_id is derived from the (kod, benämning) set, so two cvids sharing
    identical codes fold into ONE value set; the `version` becomes the state's
    `value_set_version_label`."""
    return [PIPE.join([version, "1", kod, ben, str(cvid), ""]) for kod, ben in codes]


# Two clearly-distinct codings on one column: disjoint codes (symmetric diff 6 >
# _COSMETIC_MAX_SYM=2 → not cosmetic) and DIFFERENT version labels (→ no
# same-label-drift, and arbitrary labels rank equal under _label_resolution_rank →
# no freshness tiebreak). Plain "YYYY" register versions → equal authority/recency.
# So nothing in the cascade resolves them except SUPERSESSION (distinct intro year).
_CODING_A = [("11", "Alpha ett"), ("12", "Alpha två"), ("13", "Alpha tre")]
_CODING_B = [("21", "Beta ett"), ("22", "Beta två"), ("23", "Beta tre")]


def _build(
    tmp_path: Path, ri_extra: list[str], vm_extra: list[str]
) -> sqlite3.Connection:
    """Run a real SCB build with the standard fixture plus the extra rows; return a
    read-only connection to the built DB. Never touches the live DB (tmp only)."""
    input_dir = tmp_path / "input"
    db_dir = tmp_path / "db"
    slug_dir = tmp_path / "slugs"
    for d in (input_dir, db_dir, slug_dir):
        d.mkdir()
    write_scb_input(
        input_dir,
        registerinformation_rows=REGISTERINFORMATION_ROWS + ri_extra,
        vardemangder_rows=VARDEMANGDER_ROWS + vm_extra,
    )
    _write_fixture_slug_dir(slug_dir)
    build_db(
        input_dir=input_dir,
        db_dir=db_dir,
        skip_classifications=True,
        slug_dir=slug_dir,
    )
    return sqlite3.connect(db_dir / "reg_meta.db")


class TestCascadeCarvesCleanly:
    """Supersession carve: coding A is delivered 2018-2022 and the later-introduced
    coding B supersedes it for 2020 only, so A's won years RLE into TWO runs around
    the B year — the civilfar `vs2 [..] / vs2834 [2010] / vs2 [..]` shape."""

    def test_supersession_carves_two_runs(self, tmp_path: Path) -> None:
        # Coding A (var 700, column CarveCol): one cvid per year 2018-2022, all
        # sharing codes _CODING_A → one folded group spanning the five years.
        ri = [
            _var_row(
                colname="CarveCol",
                cvid=7000 + i,
                var_id=700,
                varname="CarveVar",
                year=str(year),
                regver_id=700 + i,
                data_length="3",
            )
            for i, year in enumerate((2018, 2019, 2020, 2021, 2022))
        ]
        vm = [r for i in range(5) for r in _vm_rows(7000 + i, "AlphaA", _CODING_A)]
        # Coding B: a single 2020 cvid with disjoint codes → introduced in 2020, so
        # SUPERSESSION (latest min-year) hands it the 2020 cell over coding A.
        ri.append(
            _var_row(
                colname="CarveCol",
                cvid=7100,
                var_id=700,
                varname="CarveVar",
                year="2020",
                regver_id=720,
                data_length="3",
            )
        )
        vm += _vm_rows(7100, "BetaB", _CODING_B)

        conn = _build(tmp_path, ri, vm)
        try:
            rows = conn.execute(
                "SELECT valid_from, valid_to, value_set_id FROM variable_state "
                "WHERE delivery_column_name = 'CarveCol' ORDER BY valid_from"
            ).fetchall()
        finally:
            conn.close()

        # Three contiguous runs: A[2018-2019], B[2020], A[2021-2022].
        assert [r[0][:4] for r in rows] == ["2018", "2020", "2021"]
        assert [r[1][:4] for r in rows] == ["2019", "2020", "2022"]
        # vs2 / vs2834 / vs2: the outer runs share one value set; the middle differs.
        assert rows[0][2] is not None and rows[1][2] is not None
        assert rows[0][2] == rows[2][2]
        assert rows[1][2] != rows[0][2]


class TestGenuineConflictFailsBuild:
    """A genuine same-column co-delivery (two distinct non-cosmetic codings on one
    column in one period, nothing in the cascade able to pick one) must FAIL the
    build before materializing — even on a default (non-`--validate`) run."""

    def test_unresolvable_conflict_raises(self, tmp_path: Path) -> None:
        # var 800, column ClashCol: two codings, both introduced in 2020 (equal
        # min-year → no supersession), distinct labels, disjoint codes. The cascade
        # has no lever left → GENUINE → the coalescer raises.
        ri = [
            _var_row(
                colname="ClashCol",
                cvid=8001,
                var_id=800,
                varname="ClashVar",
                year="2020",
                regver_id=800,
                data_length="3",
            ),
            _var_row(
                colname="ClashCol",
                cvid=8002,
                var_id=800,
                varname="ClashVar",
                year="2020",
                regver_id=801,
                data_length="3",
            ),
        ]
        vm = _vm_rows(8001, "Coding A", _CODING_A) + _vm_rows(
            8002, "Coding B", _CODING_B
        )

        with pytest.raises(RegMetaError) as exc:
            _build(tmp_path, ri, vm)
        assert exc.value.code == "coalesce_unresolved_codelivery"
        assert exc.value.exit_code == EXIT_CONFIG
        # Actionable: names the offending column so a maintainer can write the
        # pin. The coalescer's column identity is the case-folded rule-2 key
        # (#196), so the message carries the folded form; the codelivery loader
        # folds the pin's `column` the same way, so any TOML casing matches.
        assert "clashcol" in exc.value.message

    def test_all_yearless_conflict_raises(self, tmp_path: Path) -> None:
        # var 900, column YlessCol: two distinct codings whose registerversionnamn
        # carries NO parseable year, so both are YEARLESS open-span groups. With no
        # year-bearing coding to win and no curation pin, the per-year cascade can't
        # separate them → unresolvable open-span co-delivery → the build must fail
        # (not silently ship two overlapping open states on one column).
        ri = [
            _var_row(
                colname="YlessCol",
                cvid=9001,
                var_id=900,
                varname="YlessVar",
                year="Senaste version",
                regver_id=900,
                data_length="3",
            ),
            _var_row(
                colname="YlessCol",
                cvid=9002,
                var_id=900,
                varname="YlessVar",
                year="Tidigare version",
                regver_id=901,
                data_length="3",
            ),
        ]
        vm = _vm_rows(9001, "Coding A", _CODING_A) + _vm_rows(
            9002, "Coding B", _CODING_B
        )

        with pytest.raises(RegMetaError) as exc:
            _build(tmp_path, ri, vm)
        assert exc.value.code == "coalesce_unresolved_codelivery"
        assert "ylesscol" in exc.value.message  # folded rule-2 column key (#196)
        assert "yearless" in exc.value.message

    def test_stale_pin_with_yearbearing_rival_raises(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # A `keep` pin for a column whose conflict mixes a YEARLESS coding and a
        # YEAR-BEARING rival, but whose label matches NEITHER (stale/typo), must
        # FAIL the build — not silently drop the yearless codings and ship the
        # year-bearing default (with a single year-bearing rival the year loop
        # returns before consulting curation, so the bad pin would go unreported).
        ri = [
            _var_row(
                colname="StaleCol",
                cvid=9101,
                var_id=910,
                varname="StaleVar",
                year="Aktuell version",  # yearless
                regver_id=910,
                data_length="3",
            ),
            _var_row(
                colname="StaleCol",
                cvid=9102,
                var_id=910,
                varname="StaleVar",
                year="2020",  # year-bearing rival
                regver_id=911,
                data_length="3",
            ),
        ]
        vm = _vm_rows(9101, "Coding A", _CODING_A) + _vm_rows(
            9102, "Coding B", _CODING_B
        )
        pin = tmp_path / "codelivery.toml"
        pin.write_text(
            '[[resolve]]\nregister_id = 1\nvar_id = 910\ncolumn = "StaleCol"\n'
            'keep = "Nonexistent coding"\n',
            encoding="utf-8",
        )
        import reg_meta_build.codelivery as _cd

        monkeypatch.setattr(_cd, "repo_codelivery_path", lambda: pin)

        with pytest.raises(RegMetaError) as exc:
            _build(tmp_path, ri, vm)
        assert exc.value.code == "coalesce_unresolved_codelivery"
        assert "stalecol" in exc.value.message  # folded rule-2 column key (#196)


# Two codings whose CODE sets are identical ({30,31,32}) but one code is RELABELED
# — symmetric code-diff is 0 (well within _COSMETIC_MAX_SYM=2), so the pre-gate
# cosmetic rule would silently keep one. A relabel is a genuine re-coding, not
# cosmetic drift, so the label-aware gate must refuse to collapse it.
_RECODE_BASE = [("30", "Stockholm"), ("31", "Göteborg"), ("32", "Malmö")]
_RECODE_RELABELED = [("30", "Stockholm"), ("31", "Göteborg"), ("32", "Uppsala")]
# True cosmetic drift: the smaller is a clean subset (one code dropped), no shared
# code relabeled — symmetric diff 1 ≤ 2, so it still collapses to the larger.
_DRIFT_FULL = [("30", "Stockholm"), ("31", "Göteborg"), ("32", "Malmö")]
_DRIFT_MINUS_ONE = [("30", "Stockholm"), ("31", "Göteborg")]


class TestLabelAwareCosmetic:
    """The cosmetic collapse (≤ _COSMETIC_MAX_SYM symmetric codes → keep larger)
    is label-aware: a tiny code-diff that hides a relabeled shared code is a genuine
    re-coding and must NOT silently merge; clean drift (no relabel) still merges."""

    def test_relabeled_shared_code_under_threshold_raises(self, tmp_path: Path) -> None:
        # var 950, column ReCol: two codings, same year, same code set {30,31,32}
        # (symmetric diff 0), but code 32 is relabeled Malmö→Uppsala. Distinct
        # version labels (no same-label drift), equal intro year (no supersession).
        # Pre-gate this collapsed silently; now it must fail as a genuine conflict.
        ri = [
            _var_row(
                colname="ReCol",
                cvid=9501,
                var_id=950,
                varname="ReVar",
                year="2020",
                regver_id=950,
                data_length="3",
            ),
            _var_row(
                colname="ReCol",
                cvid=9502,
                var_id=950,
                varname="ReVar",
                year="2020",
                regver_id=951,
                data_length="3",
            ),
        ]
        vm = _vm_rows(9501, "Coding base", _RECODE_BASE) + _vm_rows(
            9502, "Coding relabeled", _RECODE_RELABELED
        )
        with pytest.raises(RegMetaError) as exc:
            _build(tmp_path, ri, vm)
        assert exc.value.code == "coalesce_unresolved_codelivery"
        assert "recol" in exc.value.message  # folded rule-2 column key (#196)

    def test_clean_drift_still_collapses(self, tmp_path: Path) -> None:
        # var 960, column DriftCol: same setup but the smaller coding is a clean
        # SUBSET (code 32 absent, none relabeled) — symmetric diff 1 ≤ 2, no shared
        # code differs → genuine cosmetic drift, still collapses to the larger.
        ri = [
            _var_row(
                colname="DriftCol",
                cvid=9601,
                var_id=960,
                varname="DriftVar",
                year="2020",
                regver_id=960,
                data_length="3",
            ),
            _var_row(
                colname="DriftCol",
                cvid=9602,
                var_id=960,
                varname="DriftVar",
                year="2020",
                regver_id=961,
                data_length="3",
            ),
        ]
        vm = _vm_rows(9601, "Coding full", _DRIFT_FULL) + _vm_rows(
            9602, "Coding minus", _DRIFT_MINUS_ONE
        )
        conn = _build(tmp_path, ri, vm)
        try:
            rows = conn.execute(
                "SELECT DISTINCT value_set_id FROM variable_state "
                "WHERE delivery_column_name = 'DriftCol' AND value_set_id IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()
        # Collapsed to exactly one value set (the larger, 3-code coding).
        assert len(rows) == 1

    def test_case_only_relabel_still_collapses(self, tmp_path: Path) -> None:
        # var 970, column CaseCol: identical code set, one code differs ONLY by
        # case ('Malmö' vs 'MALMÖ') — the FodelseLandNamn 'Makedonien'/'MAKEDONIEN'
        # pattern. Normalized labels match → not a meaningful re-coding → still
        # collapses (cosmetic), so a cosmetic case-normalization isn't a conflict.
        case_a = [("30", "Stockholm"), ("31", "Göteborg"), ("32", "Malmö")]
        case_b = [("30", "Stockholm"), ("31", "Göteborg"), ("32", "MALMÖ")]
        ri = [
            _var_row(
                colname="CaseCol",
                cvid=9701,
                var_id=970,
                varname="CaseVar",
                year="2020",
                regver_id=970,
                data_length="3",
            ),
            _var_row(
                colname="CaseCol",
                cvid=9702,
                var_id=970,
                varname="CaseVar",
                year="2020",
                regver_id=971,
                data_length="3",
            ),
        ]
        vm = _vm_rows(9701, "Coding cased", case_a) + _vm_rows(
            9702, "Coding upper", case_b
        )
        conn = _build(tmp_path, ri, vm)
        try:
            rows = conn.execute(
                "SELECT DISTINCT value_set_id FROM variable_state "
                "WHERE delivery_column_name = 'CaseCol' AND value_set_id IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) == 1


class TestExtendsLater:
    """Sequential re-coding whose INTRODUCTION year ties at supersession: the
    coding whose timeline reaches a later period (max regver_max) is the modern
    one and wins the contested transition year — no curation pin needed. Mirrors
    the political-bloc series (plain → '…2009' → '…2014') and Avlopp's FoB75 coding
    that carries forward."""

    def test_later_extending_coding_wins(self, tmp_path: Path) -> None:
        # column ExtCol: OLD coding observed in 2020 only; MODERN coding observed
        # 2020-2022 (distinct, non-cosmetic codes → won't merge). Both introduced
        # at 2020 (regver_min ties), but MODERN extends to 2022 → it wins 2020 and
        # carries 2021-2022, OLD is dropped. The build resolves (no conflict).
        ri = [
            _var_row(
                colname="ExtCol",
                cvid=9801,
                var_id=980,
                varname="ExtVar",
                year="2020",
                regver_id=980,
                data_length="3",
            ),
        ]
        ri += [
            _var_row(
                colname="ExtCol",
                cvid=9810 + i,
                var_id=980,
                varname="ExtVar",
                year=str(y),
                regver_id=981 + i,
                data_length="3",
            )
            for i, y in enumerate((2020, 2021, 2022))
        ]
        vm = _vm_rows(9801, "Coding old", _CODING_A)
        for i in range(3):
            vm += _vm_rows(9810 + i, "Coding modern", _CODING_B)

        conn = _build(tmp_path, ri, vm)
        try:
            vsids = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT value_set_id FROM variable_state "
                    "WHERE delivery_column_name = 'ExtCol' AND value_set_id IS NOT NULL"
                )
            ]
            codes = {
                r[0]
                for r in conn.execute(
                    "SELECT vc.code FROM value_set_member vsm "
                    "JOIN value_code vc ON vc.code_id = vsm.code_id "
                    "WHERE vsm.value_set_id = ?",
                    (vsids[0],),
                )
            }
        finally:
            conn.close()
        # Exactly the MODERN coding survives (its codes, not the old coding's).
        assert len(vsids) == 1
        assert codes == {c for c, _ in _CODING_B}


class TestResidualClampReconciliation:
    """Fast-path residual reconciliation (`_collapse_residual` pass 2), the
    complement to the timeline cascade above: two states on ONE column carry the
    SAME value set under the SAME label but drift in `data_length` across
    overlapping year spans (so the only gkey difference is `data_length`). Same
    value set → `_spans_overlap` is False → they never reach the timeline, so
    pass 2 owns the emitted overlap: it DROPS a fully-subsumed span and CLAMPS a
    crossing one — without losing the older span's non-overlap coverage."""

    @staticmethod
    def _drift_rows(
        *, colname: str, var_id: int, years: range, regver_base: int, data_length: str
    ) -> tuple[list[str], list[str]]:
        # One cvid per year on `colname`, all SAME codes (_CODING_A) + SAME version
        # label, so the only gkey difference from the rival span is `data_length`.
        ri: list[str] = []
        vm: list[str] = []
        for i, year in enumerate(years):
            cvid = regver_base * 10 + i
            ri.append(
                _var_row(
                    colname=colname,
                    cvid=cvid,
                    var_id=var_id,
                    varname=f"{colname}Var",
                    year=str(year),
                    regver_id=regver_base + i,
                    data_length=data_length,
                )
            )
            vm += _vm_rows(cvid, "Coding", _CODING_A)
        return ri, vm

    def test_crossing_clamp_preserves_older_non_overlap(self, tmp_path: Path) -> None:
        # Older span 2016-2020 (len 3); younger 2019-2022 (len 5) starts inside it
        # and extends past → CROSSING. The older is clamped to 2018 (younger.min-1),
        # so its non-overlap years 2016-2018 SURVIVE; the younger owns 2019-2022.
        ri_old, vm_old = self._drift_rows(
            colname="ClampCol",
            var_id=1750,
            years=range(2016, 2021),
            regver_base=1750,
            data_length="3",
        )
        ri_new, vm_new = self._drift_rows(
            colname="ClampCol",
            var_id=1750,
            years=range(2019, 2023),
            regver_base=1790,
            data_length="5",
        )
        conn = _build(tmp_path, ri_old + ri_new, vm_old + vm_new)
        try:
            rows = conn.execute(
                "SELECT valid_from, valid_to, data_length, value_set_id "
                "FROM variable_state WHERE delivery_column_name = 'ClampCol' "
                "ORDER BY valid_from"
            ).fetchall()
        finally:
            conn.close()
        # Two non-overlapping states; the older keeps 2016-2018 (clamped, not dropped).
        assert [(r[0][:4], r[1][:4]) for r in rows] == [
            ("2016", "2018"),
            ("2019", "2022"),
        ]
        assert [r[2] for r in rows] == ["3", "5"]  # both drifting lengths ship
        assert rows[0][3] == rows[1][3]  # same value set throughout
        assert rows[0][1] < rows[1][0]  # no overlap: older ends before younger starts

    def test_nested_subsumed_span_dropped(self, tmp_path: Path) -> None:
        # Younger span 2018-2020 (len 5) is fully inside older 2016-2022 (len 3) →
        # SUBSUMED. The younger is redundant drift (the older already covers those
        # years with the identical coding) → dropped; the older keeps its full span.
        ri_old, vm_old = self._drift_rows(
            colname="DropCol",
            var_id=1760,
            years=range(2016, 2023),
            regver_base=1760,
            data_length="3",
        )
        ri_new, vm_new = self._drift_rows(
            colname="DropCol",
            var_id=1760,
            years=range(2018, 2021),
            regver_base=1860,
            data_length="5",
        )
        conn = _build(tmp_path, ri_old + ri_new, vm_old + vm_new)
        try:
            rows = conn.execute(
                "SELECT valid_from, valid_to, data_length FROM variable_state "
                "WHERE delivery_column_name = 'DropCol' ORDER BY valid_from"
            ).fetchall()
        finally:
            conn.close()
        # Exactly one state survives: the wider older span, its data_length intact.
        assert len(rows) == 1
        assert (rows[0][0][:4], rows[0][1][:4]) == ("2016", "2022")
        assert rows[0][2] == "3"
