"""Tests for build-time triage (fold / split / collapse); see DESIGN.md → Build-time triage (SCB).

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
    PIPE,
    REGISTERINFORMATION_ROWS,
    UNIKA_ROWS,
    VARDEMANGDER_ROWS,
    _var_row,
    write_scb_input,
)
from reg_meta.db import open_db
from reg_meta.fqid import period_token_to_bounds
from reg_meta_build.db import build_db
from reg_meta_build.sources.scb import (
    _AUTH_FINAL,
    _AUTH_PLAIN,
    _AUTH_PRELIM,
    _AUTH_SUBANNUAL,
    _apply_clustered,
    _apply_fold,
    _apply_split,
    _cluster_contested,
    _collapse_residual,
    _common_prefix_len,
    _data_type_class,
    _decide_fold_or_split,
    _edition_authority,
    _edition_bounds,
    _fold_token_from_grain,
    _import_bug_suspect,
    _looks_like_code_label_pair,
    _pick_state_rep,
    _resolve_year_winners,
    _rle_runs,
    _spans_overlap,
    _split_off_non_contested,
    _split_relation_kind,
    _StateGroup,
    _triage_groups,
    _TriageResult,
)


def _state(
    value_set_id: int | None,
    *,
    col: str = "agrupp",
    grain: str = "",
    authority: int = _AUTH_PLAIN,
    approval: str = "",
    regver_max: int | None = None,
    regver_min: int | None = None,
    label: str | None = None,
    dlen: str = "1",
    year: int = 2020,
) -> tuple[tuple, _StateGroup]:
    """Return (gkey, group) for the per-year resolver tests. The gkey is a real
    9-tuple — [5]=value_set_id, [7]=grain, [8]=column — the fields the
    column-aware resolver reads. `dlen` varies the shape so two same-value-set
    groups on one column stay distinct gkeys. `label` defaults to a UNIQUE
    per-value-set string so distinct value sets are distinct-label (genuine) unless
    a test sets a shared label to exercise the same-label-drift rule."""
    vlabel = label if label is not None else f"vs{value_set_id}"
    gkey = (1, 10, 1, "int", dlen, value_set_id, vlabel, grain, col)
    g = _StateGroup(
        register_id=1,
        register_variant_id=10,
        var_id=1,
        data_type="int",
        data_length=dlen,
        value_set_id=value_set_id,
        value_set_version_label=vlabel,
    )
    g.regyears = {year}
    g.year_authority = {year: authority}
    g.year_approval = {year: approval}
    g.regver_max = regver_max
    g.regver_min = regver_min
    return gkey, g


def _shape(data_type: str | None, data_length: str | None = None) -> _StateGroup:
    """A minimal `_StateGroup` carrying only the shape fields the split
    relation_kind heuristics read (`data_type` / `data_length`)."""
    return _StateGroup(1, 10, 1, data_type, data_length, None, None)


class TestEditionAuthority:
    def test_finality_ranks(self) -> None:
        assert _edition_authority("2020, slutlig version") == _AUTH_FINAL
        assert _edition_authority("2020, preliminär version") == _AUTH_PRELIM
        assert _edition_authority("2020") == _AUTH_PLAIN
        assert _edition_authority("1992_old") == 0

    def test_subannual_markers(self) -> None:
        assert _edition_authority("Höstterminen 2018") == _AUTH_SUBANNUAL
        assert _edition_authority("2010 kvartal 2-4") == _AUTH_SUBANNUAL
        assert _edition_authority("Läsåret 2015/2016") == _AUTH_SUBANNUAL

    def test_finality_beats_subannual_order(self) -> None:
        # 'slutlig' must win even if a sub-annual word also appears.
        assert _edition_authority("Höstterminen 2018, slutlig version") == _AUTH_FINAL

    def test_compact_term_forms_are_subannual(self) -> None:
        # No-space `HT2018`/`VT2018` and a bare `HT`/`VT` token must rank
        # sub-annual (else they tie a full-year annual at _AUTH_PLAIN).
        assert _edition_authority("HT2018") == _AUTH_SUBANNUAL
        assert _edition_authority("VT2018") == _AUTH_SUBANNUAL
        assert _edition_authority("Skolår 2018 HT") == _AUTH_SUBANNUAL
        # A plain annual is unaffected (no false HT/VT match mid-word).
        assert _edition_authority("Avbrott 2018") == _AUTH_PLAIN

    def test_month_names_word_bounded(self) -> None:
        # A real month name → sub-annual, but the word boundary blocks a substring
        # false-positive: `juni` ⊄ `junior`, `maj` ⊄ `majversion`.
        assert _edition_authority("Mars 2018") == _AUTH_SUBANNUAL
        assert _edition_authority("2018 juni") == _AUTH_SUBANNUAL
        assert _edition_authority("Majversion 2018") == _AUTH_PLAIN
        assert _edition_authority("Junioravgång 2018") == _AUTH_PLAIN


class TestEditionBounds:
    """#219 sub-annual delivery-window parser. The term/quarter/half forms must
    AGREE with reg_meta's `period_token_to_bounds` (build & resolve share the same
    expansion); everything else stays full-year. The second arg is the row's edition
    year — only markers matching it are narrowed, so a window can never escape it."""

    def test_autumn_term_forms_agree_with_period_bounds(self) -> None:
        ht = period_token_to_bounds("HT2024")
        for name in (
            "Höstterminen 2024",
            "Hösttermin 2024",
            "2024 höstterminen",
            "HT 2024",
            "Ht 2024",
            "HT2024",
        ):
            assert _edition_bounds(name, 2024) == ht, name
        assert ht == ("2024-07-01", "2024-12-31")

    def test_spring_term_forms_agree_with_period_bounds(self) -> None:
        vt = period_token_to_bounds("VT2024")
        for name in (
            "Vårterminen 2024",
            "Vårtermin 2024",
            "2024 vårterminen",
            "VT 2024",
            "Vt 2024",
            "VT2024",
            "Vårterminen 2024 - betyg",
        ):
            assert _edition_bounds(name, 2024) == vt, name
        assert vt == ("2024-01-01", "2024-06-30")

    def test_quarter_forms_agree_with_period_bounds(self) -> None:
        assert _edition_bounds("2005 kvartal 1", 2005) == period_token_to_bounds(
            "2005-Q1"
        )
        assert _edition_bounds("2011 kv1", 2011) == period_token_to_bounds("2011-Q1")
        # A range spans its endpoints (union of the two quarter tokens).
        assert _edition_bounds("2005 kvartal 2-4", 2005) == ("2005-04-01", "2005-12-31")
        assert _edition_bounds("2007 kv 2-kv 4", 2007) == ("2007-04-01", "2007-12-31")
        assert _edition_bounds("Kvartal 1-3 fr.o.m 2010", 2010) == (
            "2010-01-01",
            "2010-09-30",
        )

    def test_half_year_forms_agree_with_period_bounds(self) -> None:
        assert _edition_bounds("Första halvåret 1995", 1995) == period_token_to_bounds(
            "1995-H1"
        )
        assert _edition_bounds("Andra halvåret 1995", 1995) == period_token_to_bounds(
            "1995-H2"
        )

    def test_school_year_range_keeps_only_edition_year_term(self) -> None:
        # The edition year (extract_year = the FIRST year) selects which term is
        # narrowed; the other-year term is dropped, so the window stays WITHIN the
        # edition year (start narrowed, year-granular end — no cross-year extension).
        assert _edition_bounds("Höstterminen 2020 - Vårterminen 2021", 2020) == (
            "2020-07-01",
            "2020-12-31",
        )
        assert _edition_bounds("Komvux HT 1988 - VT 2024", 1988) == (
            "1988-07-01",
            "1988-12-31",
        )

    def test_term_for_other_year_dropped_no_inversion(self) -> None:
        # A collection note whose term names a DIFFERENT year than the edition year
        # must NOT yield that term's bounds (which would invert valid_from > valid_to
        # once the materializer clamps valid_to to the edition year). The term is
        # dropped → full edition year.
        assert _edition_bounds("Insamling 2019 avseende höstterminen 2020", 2019) == (
            "2019-01-01",
            "2019-12-31",
        )

    def test_out_of_range_term_year_does_not_crash(self) -> None:
        # `period_token_to_bounds("HT1850")` would raise FqidError (year < 1900). The
        # 1850 term mismatches the edition year 2024 → dropped → full 2024, no crash.
        assert _edition_bounds("HT 1850, version 2024", 2024) == (
            "2024-01-01",
            "2024-12-31",
        )

    def test_full_year_forms_not_narrowed(self) -> None:
        # Bare year, dated annual, finality qualifiers, month names, seasons,
        # läsår, and the summer term all expand to the FULL year — their sub-year
        # span is ambiguous, so narrowing is deliberately withheld.
        full = ("2024-01-01", "2024-12-31")
        for name in (
            "2024",
            "15 oktober 2024",
            "2024, slutlig version",
            "2024, preliminär version",
            "2024_old",
            "Mars 2024",
            "Hösten 2024",
            "Våren 2024",
            "Sommarterminen 2024",
            "2024 Kvartal",  # bare 'Kvartal', no quarter number → all quarters
        ):
            assert _edition_bounds(name, 2024) == full, name
        assert _edition_bounds("Läsåret 2013/2014", 2013) == (
            "2013-01-01",
            "2013-12-31",
        )

    def test_yearless_returns_none(self) -> None:
        # year=None (yearless row) → None regardless of the name's content.
        assert _edition_bounds(None, None) is None
        assert _edition_bounds("", None) is None
        assert _edition_bounds("Senaste versionen", None) is None
        assert _edition_bounds("Ekonomiskt bistånd, kvartal", None) is None


class TestRleRuns:
    def test_contiguous(self) -> None:
        assert _rle_runs([2000, 2001, 2002]) == [(2000, 2002)]

    def test_gap_splits(self) -> None:
        assert _rle_runs([2000, 2002]) == [(2000, 2000), (2002, 2002)]
        assert _rle_runs([2000, 2001, 2003, 2004, 2005]) == [(2000, 2001), (2003, 2005)]

    def test_empty_and_single(self) -> None:
        assert _rle_runs([]) == []
        assert _rle_runs([1999]) == [(1999, 1999)]


class TestResolveYearWinners:
    def _codes(self, sizes: dict[int, int]):
        # value_set_id -> a NESTED code set of the requested size (shared prefix,
        # so two sets' symmetric diff == their size difference — the real-world
        # cosmetic-drift shape where a coding gains/loses a few codes).
        store = {vs: frozenset(f"c{i}" for i in range(n)) for vs, n in sizes.items()}
        return lambda vs: store.get(vs, frozenset())

    def test_single_candidate(self) -> None:
        a = _state(1)
        winners, genuine = _resolve_year_winners(
            [a[0]], dict([a]), 2020, self._codes({1: 3})
        )
        assert winners == [a[0]]
        assert genuine is False

    def test_authority_breaks_tie(self) -> None:
        # same column, distinct value sets -> authority resolves within the column.
        a = _state(1, col="x", authority=_AUTH_FINAL)
        b = _state(2, col="x", authority=_AUTH_PRELIM)
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 3, 2: 3})
        )
        assert winners == [a[0]]
        assert genuine is False

    def test_recency_breaks_tie(self) -> None:
        a = _state(1, col="x", approval="2022-01-01")
        b = _state(2, col="x", approval="2014-01-01")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 3, 2: 3})
        )
        assert winners == [a[0]]
        assert genuine is False

    def test_cosmetic_keeps_larger(self) -> None:
        # same column + authority + approval, codes differ by 1 -> cosmetic.
        a = _state(1, col="x")
        b = _state(2, col="x")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 11, 2: 10})
        )
        assert winners == [a[0]]  # larger
        assert genuine is False

    def test_authority_precedes_cosmetic(self) -> None:
        # PRECEDENCE: authority (step 1) runs before cosmetic (step 10). A is FINAL
        # but SMALLER; B is plain but larger (cosmetic would keep B). Authority wins
        # → A, proving the earlier step short-circuits.
        a = _state(1, col="x", authority=_AUTH_FINAL)
        b = _state(2, col="x", authority=_AUTH_PLAIN)
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 3, 2: 4})
        )
        assert winners == [a[0]]  # authority, not the larger cosmetic pick
        assert genuine is False

    def test_supersession_precedes_cosmetic(self) -> None:
        # PRECEDENCE: supersession (step 5) runs before cosmetic (step 10). The
        # codes differ by 1 (cosmetic-eligible) AND introduction years differ; the
        # later-introduced coding is the SMALLER one, so supersession (not cosmetic,
        # which would keep the larger) decides → the later coding wins.
        old_big = _state(1, col="x", regver_min=2006)
        new_small = _state(2, col="x", regver_min=2008)
        winners, genuine = _resolve_year_winners(
            [old_big[0], new_small[0]],
            dict([old_big, new_small]),
            2020,
            self._codes({1: 4, 2: 3}),
        )
        assert winners == [new_small[0]]  # supersession, not the larger cosmetic pick
        assert genuine is False

    def test_supersession_latest_introduced_wins(self) -> None:
        # one column, sequential vintages overlapping at the transition year: the
        # later-introduced coding (higher min year) supersedes; big code diff so
        # only supersession (not cosmetic) can resolve it.
        old = _state(1, col="NgS1", regver_min=2006)
        new = _state(2, col="NgS1", regver_min=2008)
        winners, genuine = _resolve_year_winners(
            [old[0], new[0]], dict([old, new]), 2008, self._codes({1: 778, 2: 821})
        )
        assert winners == [new[0]]  # latest-introduced supersedes the boundary
        assert genuine is False

    def test_same_label_drift_keeps_larger(self) -> None:
        # one column, SAME source label (a `-N` collapse near-dup), distinct value
        # sets, equal introduction -> keep the larger (most complete).
        small = _state(1, col="x", label="SEI_PSU")
        big = _state(2, col="x", label="SEI_PSU")
        winners, genuine = _resolve_year_winners(
            [small[0], big[0]], dict([small, big]), 2020, self._codes({1: 19, 2: 22})
        )
        assert winners == [big[0]]
        assert genuine is False

    def test_label_freshness_final_beats_preliminary(self) -> None:
        a = _state(1, col="x", label="RTB 2021 preliminär")
        b = _state(2, col="x", label="RTB 2021")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2021, self._codes({1: 234, 2: 251})
        )
        assert winners == [b[0]]  # final beats preliminary
        assert genuine is False

    def test_label_freshness_later_snapshot_wins(self) -> None:
        a = _state(1, col="x", label="Skolenhetskod 2015-05-15")
        b = _state(2, col="x", label="Skolenhetskod 2015-10-15")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2015, self._codes({1: 8854, 2: 9110})
        )
        assert winners == [b[0]]  # later dated snapshot
        assert genuine is False

    def test_label_freshness_calendar_beats_academic(self) -> None:
        a = _state(1, col="x", label="Komvux Kurskod 2001/2002")
        b = _state(2, col="x", label="Komvux Kurskod 2001")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2001, self._codes({1: 5955, 2: 6473})
        )
        assert winners == [b[0]]  # calendar-year canonical over academic
        assert genuine is False

    def test_label_freshness_autumn_term_no_space(self) -> None:
        # the no-space `HT1986` form must match (regression: `\bht\b` missed it).
        a = _state(1, col="x", label="Komvux Kurskod VT1986")
        b = _state(2, col="x", label="Komvux Kurskod HT1986")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 1986, self._codes({1: 837, 2: 780})
        )
        assert winners == [b[0]]  # autumn term wins despite fewer codes
        assert genuine is False

    def test_distinct_label_recoding_stays_genuine(self) -> None:
        # one column, SAME introduction, DIFFERENT source labels (Br92 vs Br07) →
        # genuine → curation.
        a = _state(1, col="AL2", regver_min=1998, label="Br92-kod")
        b = _state(2, col="AL2", regver_min=1998, label="Br07-kod")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 1998, self._codes({1: 53, 2: 45})
        )
        assert genuine is True
        assert set(winners) == {a[0], b[0]}

    def test_curation_pin_resolves_genuine(self) -> None:
        # a codelivery pin keeps the named label, resolving the genuine conflict.
        a = _state(1, col="AL2", regver_min=1998, label="Br92-kod")
        b = _state(2, col="AL2", regver_min=1998, label="Br07-kod")
        # gkey: [0]=register_id=1, [2]=var_id=1, [8]=column="AL2"
        codelivery = {(1, 1, "AL2"): ("Br07-kod", None)}
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 1998, self._codes({1: 53, 2: 45}), codelivery
        )
        assert winners == [b[0]]  # the pinned Br07 coding
        assert genuine is False

    def test_curation_pin_matches_emitted_label(self) -> None:
        # a fold relabels the raw value_set_version_label (origA/origB) to
        # emitted tokens (4pos/4pos-1). The pin matches the EMITTED label (what the
        # maintainer sees in variable_state), resolving to '4pos' over cosmetic's
        # would-be larger pick.
        a = _state(1, col="X", label="origA")
        b = _state(2, col="X", label="origB")
        emitted = {a[0]: "4pos", b[0]: "4pos-1"}
        codelivery = {(1, 1, "X"): ("4pos", None)}
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]],
            dict([a, b]),
            2020,
            self._codes({1: 377, 2: 378}),
            codelivery,
            emitted,
        )
        assert winners == [a[0]]  # the pinned '4pos' (377), not cosmetic's larger
        assert genuine is False

    def test_curation_keep_rule_latest_year(self) -> None:
        # keep_rule=latest_year keeps the coding whose label embeds the latest year
        # (recurring SFI-year vintages on one column).
        a = _state(1, col="Skolkod", label="Skolkod SFI 1999")
        b = _state(2, col="Skolkod", label="Skolkod SFI 2000")
        codelivery = {(1, 1, "Skolkod"): (None, "latest_year")}
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 1999, self._codes({1: 574, 2: 597}), codelivery
        )
        assert winners == [b[0]]  # SFI 2000 (later vintage)
        assert genuine is False

    def test_curation_pin_mismatch_falls_through(self) -> None:
        # a pin matching no surviving label at this year falls through to GENUINE
        # (no crash) — the year stays unresolved for --validate to flag.
        a = _state(1, col="AL2", regver_min=1998, label="Br92-kod")
        b = _state(2, col="AL2", regver_min=1998, label="Br07-kod")
        codelivery = {(1, 1, "AL2"): ("Something else", None)}
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 1998, self._codes({1: 53, 2: 45}), codelivery
        )
        assert genuine is True
        assert set(winners) == {a[0], b[0]}

    def test_genuine_same_column_returns_both(self) -> None:
        # SAME column, distinct value sets, big diff -> genuine same-column conflict.
        a = _state(1, col="x")
        b = _state(2, col="x")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 11, 2: 6})
        )
        assert genuine is True
        assert set(winners) == {a[0], b[0]}

    def test_distinct_columns_coexist(self) -> None:
        # DISTINCT columns = parallel representations of one concept -> co-exist,
        # NOT a conflict (the SSYK 3/5-digit, age 5/10-yr bracket case).
        a = _state(1, col="agrupp")
        b = _state(2, col="agrupp2")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 11, 2: 6})
        )
        assert genuine is False
        assert set(winners) == {a[0], b[0]}  # both kept

    def test_current_beats_historical(self) -> None:
        # one column, current vs historical grain -> current wins (a column holds
        # one coding/period; historical is a superseded vintage).
        cur = _state(1, col="Kommun", grain="Kommun")
        hist = _state(2, col="Kommun", grain="Kommun historisk")
        winners, genuine = _resolve_year_winners(
            [cur[0], hist[0]], dict([cur, hist]), 2020, self._codes({1: 300, 2: 350})
        )
        assert winners == [cur[0]]  # current wins despite historical being larger
        assert genuine is False

    def test_same_value_set_not_genuine(self) -> None:
        # one column, same value set, shape drift (data_length) -> one rep.
        a = _state(1, col="x", regver_max=2019, dlen="1")
        b = _state(1, col="x", regver_max=2020, dlen="2")
        winners, genuine = _resolve_year_winners(
            [a[0], b[0]], dict([a, b]), 2020, self._codes({1: 3})
        )
        assert genuine is False
        assert len(winners) == 1
        assert winners[0] == b[0]  # latest-era rep


class TestLoadCodelivery:
    def test_parses_keep_and_rule(self, tmp_path: Path) -> None:
        from reg_meta_build.codelivery import load_codelivery

        toml = tmp_path / "codelivery.toml"
        toml.write_text(
            '[[resolve]]\nregister_id=187\nvar_id=3310\ncolumn="AL2UndEjU"\n'
            'keep="Br07-kod"\n\n'
            '[[resolve]]\nregister_id=248\nvar_id=104\ncolumn="Skolkod"\n'
            'keep_rule="latest_year"\n',
            encoding="utf-8",
        )
        cmap = load_codelivery(toml)
        # Column keys are case-folded at load to the coalescer's rule-2
        # connectivity key (#196) — TOML casing is cosmetic.
        assert cmap[(187, 3310, "al2undeju")] == ("Br07-kod", None)
        assert cmap[(248, 104, "skolkod")] == (None, "latest_year")

    def test_rejects_both_or_neither(self, tmp_path: Path) -> None:
        from reg_meta.errors import EXIT_CONFIG, RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        both = tmp_path / "both.toml"
        both.write_text(
            '[[resolve]]\nregister_id=1\nvar_id=1\ncolumn="c"\nkeep="x"\n'
            'keep_rule="latest_year"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(both)
        assert "exactly one" in exc.value.message
        assert exc.value.exit_code == EXIT_CONFIG

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        from reg_meta.errors import EXIT_CONFIG, RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "bad.toml"
        bad.write_text("[[resolve]]\nregister_id = = 1\n", encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codelivery_toml_unreadable"

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        from reg_meta_build.codelivery import load_codelivery

        assert load_codelivery(tmp_path / "nope.toml") == {}
        assert load_codelivery(None) == {}

    def test_valid_file_with_no_entries_is_empty(self, tmp_path: Path) -> None:
        # A present but entry-less file is a no-op, not an error (the byte-identity
        # path the new shape / unknown-key guards must not regress).
        from reg_meta_build.codelivery import load_codelivery

        empty = tmp_path / "empty.toml"
        empty.write_text("# no resolve entries yet\n", encoding="utf-8")
        assert load_codelivery(empty) == {}

    def test_scalar_resolve_rejected(self, tmp_path: Path) -> None:
        # `resolve = 5` is non-iterable → without the list guard `for entry in 5`
        # raises a RAW uncaught TypeError (the loop is outside the per-entry
        # try/except), escaping the EXIT_CONFIG contract.
        from reg_meta.errors import EXIT_CONFIG, RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "scalar.toml"
        bad.write_text("resolve = 5\n", encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codelivery_invalid"

    def test_single_resolve_table_rejected(self, tmp_path: Path) -> None:
        # `[resolve]` makes `resolve` a single table, not the `[[resolve]]` array →
        # reject (don't let its keys iterate as bogus entries).
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "single.toml"
        bad.write_text(
            '[resolve]\nregister_id=1\nvar_id=1\nkeep="x"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.code == "codelivery_invalid"

    def test_non_table_resolve_entries_rejected(self, tmp_path: Path) -> None:
        # `resolve` is an array but its entries aren't tables.
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "ints.toml"
        bad.write_text("resolve = [1, 2]\n", encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.code == "codelivery_invalid"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[resolves]]` (typo) would silently disable ALL curation → loud error.
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "typo.toml"
        bad.write_text(
            '[[resolves]]\nregister_id=1\nvar_id=1\ncolumn="c"\nkeep="x"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.code == "codelivery_invalid"
        assert "resolves" in exc.value.message

    def test_unhashable_keep_rule_rejected(self, tmp_path: Path) -> None:
        # `keep_rule = [1]` / `{a = 1}` is non-None and UNHASHABLE → without an
        # isinstance guard the `keep_rule not in _KEEP_RULES` membership test raises
        # a RAW `TypeError: unhashable type` outside the entry try/except, escaping
        # the EXIT_CONFIG contract. Both must land on codelivery_invalid, no crash.
        from reg_meta.errors import EXIT_CONFIG, RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        for rule in ("[1]", "{a = 1}"):
            bad = tmp_path / "rule.toml"
            bad.write_text(
                f"[[resolve]]\nregister_id=1\nvar_id=2\nkeep_rule={rule}\n",
                encoding="utf-8",
            )
            with pytest.raises(RegMetaError) as exc:
                load_codelivery(bad)
            assert exc.value.exit_code == EXIT_CONFIG
            assert exc.value.code == "codelivery_invalid"

    def test_non_string_keep_label_rejected(self, tmp_path: Path) -> None:
        # A non-string `keep` (here a list) would str()-coerce into an inert pin
        # (`"[1]"`) that never matches a real value_set_version_label → reject it at
        # load with an actionable error instead of a confusing downstream failure.
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "label.toml"
        bad.write_text(
            '[[resolve]]\nregister_id=1\nvar_id=2\ncolumn="c"\nkeep=[1]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.code == "codelivery_invalid"

    @pytest.mark.parametrize(
        ("ids", "why"),
        [
            ('register_id=1\nvar_id="01"', "leading-zero string aliases the int"),
            ("register_id=1\nvar_id=1.5", "float truncates silently"),
            ("register_id=true\nvar_id=1", "bool is an int subclass (true == 1)"),
            ("register_id=-5\nvar_id=1", "negative id is not a real source id"),
        ],
    )
    def test_non_canonical_id_rejected(
        self, tmp_path: Path, ids: str, why: str
    ) -> None:
        # Parity with fold_overrides: ids must be canonical ints (shared
        # `canonical_int`). `int(...)` would coerce each of these to an inert
        # never-matching pin (`why`) instead of a load-time error.
        from reg_meta.errors import EXIT_CONFIG, RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "id.toml"
        bad.write_text(f'[[resolve]]\n{ids}\ncolumn="c"\nkeep="x"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codelivery_invalid", why

    def test_non_string_column_rejected(self, tmp_path: Path) -> None:
        # A non-string `column` (list/dict/number/bool) would str()-coerce into a
        # column name that can never match a real delivery column → reject at load.
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        for column in ("[1]", "{a = 1}", "3", "true"):
            bad = tmp_path / "col.toml"
            bad.write_text(
                f'[[resolve]]\nregister_id=1\nvar_id=2\ncolumn={column}\nkeep="x"\n',
                encoding="utf-8",
            )
            with pytest.raises(RegMetaError) as exc:
                load_codelivery(bad)
            assert exc.value.code == "codelivery_invalid"

    def test_missing_id_rejected(self, tmp_path: Path) -> None:
        # A missing register_id/var_id must still be rejected: the old
        # `int(entry["register_id"])` raised KeyError; the new `entry.get(...)` +
        # canonical_int path must keep the same EXIT_CONFIG rejection (mirrors
        # fold_overrides' test_missing_id_rejected), not fall through to a raw
        # crash if the `reg is None or var is None` guard ever regresses.
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "missing.toml"
        bad.write_text(
            '[[resolve]]\nvar_id=1\ncolumn="c"\nkeep="x"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.code == "codelivery_invalid"


class TestPickStateRep:
    def test_latest_era_wins(self) -> None:
        a = _state(1, regver_max=2010, dlen="1")
        b = _state(1, regver_max=2020, dlen="2")
        assert _pick_state_rep([a[0], b[0]], dict([a, b])) == b[0]


def _build(
    tmp_path: Path,
    extra_rows: list[str],
    *,
    vm_extra: list[str] | None = None,
    unika_extra: list[str] | None = None,
) -> sqlite3.Connection:
    """`vm_extra` adds Vardemangder rows (value codes → non-null value_set_id, e.g.
    to force the timeline path); `unika_extra` adds UnikaRegisterOchVariabler rows
    (e.g. a blank-VersionSista row for the still-active open sentinel). Both append
    to the standard fixture lists."""
    ri_rows = list(REGISTERINFORMATION_ROWS) + extra_rows
    input_dir = tmp_path / "input"
    write_scb_input(
        input_dir,
        registerinformation_rows=ri_rows,
        vardemangder_rows=VARDEMANGDER_ROWS + (vm_extra or []),
        unika_rows=UNIKA_ROWS + (unika_extra or []),
    )
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
        assert _decide_fold_or_split(["ssyk3", "ssyk5"]) == "fold"
        assert _decide_fold_or_split(["bciv", "bcivred"]) == "fold"

    def test_disjoint_stems_split(self) -> None:
        assert _decide_fold_or_split(["hemkommun", "skolkommun"]) == "split"
        assert _decide_fold_or_split(["lid", "lnamn"]) == "split"


class TestClusterContested:
    """Per-cluster partition (#223): shared-stem + rep-suffix columns cluster
    (fold); disjoint / non-rep-suffix columns are singletons (split). The
    classification family is NOT consulted — the column stem is the boundary."""

    @staticmethod
    def _clusters(cols: list[str], **kw: object) -> list[list[str]]:
        # Normalize to sorted-list-of-sorted-lists for order-independent asserts.
        return sorted(sorted(c) for c in _cluster_contested(cols, **kw))  # type: ignore[arg-type]

    def test_shared_stem_rep_suffix_one_cluster(self) -> None:
        assert self._clusters(["Ssyk3", "Ssyk5"]) == [["Ssyk3", "Ssyk5"]]
        assert self._clusters(["FtgSni02", "FtgSni07", "FtgSni69", "FtgSni92"]) == [
            ["FtgSni02", "FtgSni07", "FtgSni69", "FtgSni92"]
        ]
        assert self._clusters(["BCIV", "BCIVRED"]) == [["BCIV", "BCIVRED"]]

    def test_disjoint_stems_separate(self) -> None:
        assert self._clusters(["Hemkommun", "Skolkommun"]) == [
            ["Hemkommun"],
            ["Skolkommun"],
        ]

    def test_non_rep_suffix_separate(self) -> None:
        # Shared stem `kommun`, but `namn` is not a representation suffix → split.
        assert self._clusters(["Kommun", "Kommunnamn"]) == [["Kommun"], ["Kommunnamn"]]

    def test_mixed_container_partitions(self) -> None:
        assert self._clusters(["Ssyk3", "Ssyk5", "Hemkommun"]) == [
            ["Hemkommun"],
            ["Ssyk3", "Ssyk5"],
        ]
        assert self._clusters(["Ssyk3", "Ssyk5", "Lid", "LNamn"]) == [
            ["LNamn"],
            ["Lid"],
            ["Ssyk3", "Ssyk5"],
        ]

    def test_forced_same_folds_by_fiat(self) -> None:
        # The curated-override seam (#261): force-merge disjoint stems into one
        # cluster, bypassing the stem rule. Default (no override) keeps them split.
        forced = [frozenset({"Hemkommun", "Skolkommun"})]
        assert self._clusters(["Hemkommun", "Skolkommun"], forced_same=forced) == [
            ["Hemkommun", "Skolkommun"]
        ]
        assert self._clusters(["Hemkommun", "Skolkommun"]) == [
            ["Hemkommun"],
            ["Skolkommun"],
        ]


class TestLooksLikeCodeLabelPair:
    def test_two_namn_columns_are_not_a_pair(self) -> None:
        assert not _looks_like_code_label_pair("Fornamn", "Efternamn")

    def test_code_label_pairs(self) -> None:
        assert _looks_like_code_label_pair("Lid", "LNamn")  # code suffix vs namn
        assert _looks_like_code_label_pair("Sun2000Kod", "Sun2000Namn")  # kod vs namn
        assert _looks_like_code_label_pair("Kommun", "Kommunnamn")  # bare stem vs namn

    def test_order_independent(self) -> None:
        assert _looks_like_code_label_pair("Kommunnamn", "Kommun")


class TestDataTypeClass:
    def test_classes(self) -> None:
        assert _data_type_class("Heltal") == "numeric"
        assert _data_type_class("Sträng (text)") == "text"  # locks the ä→a fold
        assert _data_type_class("Datum") == "other"
        assert _data_type_class(None) == "other"


class TestImportBugSuspect:
    def test_numeric_vs_text_fires(self) -> None:
        assert _import_bug_suspect(_shape("int"), _shape("text"))

    def test_same_class_differing_width_does_not_fire(self) -> None:
        assert not _import_bug_suspect(_shape("int", "4"), _shape("int", "8"))

    def test_none_side_does_not_fire(self) -> None:
        assert not _import_bug_suspect(None, _shape("int"))
        assert not _import_bug_suspect(_shape("int"), None)

    def test_length_fallback_requires_length_on_both_sides(self) -> None:
        # Unclassifiable types (class "other") fall back to data_length, but only
        # when BOTH lengths are present — a blank side must short-circuit, never
        # compare None/"" against a real width.
        assert not _import_bug_suspect(_shape(None, ""), _shape("Datum", "10"))
        assert _import_bug_suspect(_shape("Datum", "4"), _shape("Datum", "8"))


class TestSplitRelationKind:
    """Per-CO-DELIVERED-pair precedence: code_vs_label_pair → import_bug_suspect
    → generic; a non-co-delivered pair short-circuits to generic."""

    @staticmethod
    def _kind(
        col_a: str, type_a: str, col_b: str, type_b: str, *, codelivered: bool = True
    ) -> str:
        gk_a = (1, 10, 1, type_a, "1", 1, "", "", col_a)
        gk_b = (1, 10, 1, type_b, "1", 2, "", "", col_b)
        groups = {gk_a: _shape(type_a), gk_b: _shape(type_b)}
        by_col = {col_a: [gk_a], col_b: [gk_b]}
        pairs = {frozenset((col_a, col_b))} if codelivered else set()
        return _split_relation_kind(col_a, col_b, groups, by_col, pairs)

    def test_name_match_wins_over_datatype_signal(self) -> None:
        # Lid int / LNamn text matches BOTH signals; the name-based, higher-
        # confidence kind takes precedence over import_bug_suspect.
        assert self._kind("Lid", "int", "LNamn", "text") == "code_vs_label_pair"

    def test_datatype_signal_when_names_do_not_pair(self) -> None:
        assert (
            self._kind("Hemkommun", "int", "Skolkommun", "text") == "import_bug_suspect"
        )

    def test_generic_when_neither_signal(self) -> None:
        assert (
            self._kind("Hemkommun", "int", "Skolkommun", "int")
            == "same_definition_different_column"
        )

    def test_non_codelivered_pair_stays_generic_despite_signals(self) -> None:
        # A pair that never shared an edition bucket is generic even though its
        # names AND types would otherwise trip code_vs_label_pair.
        assert (
            self._kind("Lid", "int", "LNamn", "text", codelivered=False)
            == "same_definition_different_column"
        )


class TestSplitEmitsSpecificRelationKind:
    """End-to-end: triage of a real same-edition Lid/LNamn collision routes the
    specific `code_vs_label_pair` kind onto the split's edge. Drives
    `_triage_groups` and inspects `res.related_edges`; the edge → DB-row
    materializer passthrough is covered by `TestVariableRelatedToEdges`."""

    def test_lid_lnamn_split_emits_code_vs_label_pair(self) -> None:
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)  # FKs off → no provider/register parent needed
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '920', 'Lan')"
        )
        orig = cur.lastrowid
        assert orig is not None
        # Two disjoint-stem columns under one var_id co-delivered in one edition
        # (shared regver) → a split; int code vs text label → code_vs_label_pair
        # wins over the numeric-vs-text import_bug_suspect signal.
        gk_lid = (1, 10, 920, "int", "10", 1, "", "", "Lid")
        gk_lnamn = (1, 10, 920, "text", "40", 2, "", "", "LNamn")
        g_lid = _StateGroup(1, 10, 920, "int", "10", 1, "")
        g_lid.regvers = {100}
        g_lnamn = _StateGroup(1, 10, 920, "text", "40", 2, "")
        g_lnamn.regvers = {100}
        res = _triage_groups(conn, {gk_lid: g_lid, gk_lnamn: g_lnamn}, {(1, 920): orig})
        assert res.stats["splits"] == 1
        assert {kind for _, _, kind in res.related_edges} == {"code_vs_label_pair"}
        conn.close()

    def test_temporal_pair_stays_generic_only_codelivered_is_specific(self) -> None:
        # One split container with BOTH a co-delivered Lid/LNamn collision in
        # edition 100 AND a non-contested `Lidnamn` delivered alone in edition
        # 200. `Lidnamn` would form a code/label pair with `Lid` by name, but it
        # never co-occurred — so only the genuinely co-delivered pair is specific.
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '920', 'Lan')"
        )
        orig = cur.lastrowid
        assert orig is not None
        gk_lid = (1, 10, 920, "int", "10", 1, "", "", "Lid")
        gk_lnamn = (1, 10, 920, "text", "40", 2, "", "", "LNamn")
        gk_temporal = (1, 10, 920, "text", "40", 3, "", "", "Lidnamn")
        g_lid = _StateGroup(1, 10, 920, "int", "10", 1, "")
        g_lid.regvers = {100}
        g_lnamn = _StateGroup(1, 10, 920, "text", "40", 2, "")
        g_lnamn.regvers = {100}
        g_temporal = _StateGroup(1, 10, 920, "text", "40", 3, "")
        g_temporal.regvers = {200}  # different edition → never co-delivered
        res = _triage_groups(
            conn,
            {gk_lid: g_lid, gk_lnamn: g_lnamn, gk_temporal: g_temporal},
            {(1, 920): orig},
        )
        assert res.stats["splits"] == 1
        kind_by_pair = {frozenset((a, b)): kind for a, b, kind in res.related_edges}
        vid_lid = res.assignments[gk_lid]
        vid_temporal = res.assignments[gk_temporal]
        # The co-delivered code/label pair is specific; the temporal pair (which
        # would otherwise trip the name heuristic) stays generic.
        assert kind_by_pair[frozenset((orig, vid_lid))] == "code_vs_label_pair"
        assert (
            kind_by_pair[frozenset((vid_lid, vid_temporal))]
            == "same_definition_different_column"
        )
        conn.close()


class TestPerClusterFoldSplit:
    """End-to-end (#223): a var_id mixing a foldable stem-family with a disjoint
    column folds the family and splits the rest — instead of over-splitting all
    of them. Drives `_triage_groups` and inspects the routing + stats."""

    def test_mixed_container_folds_family_splits_disjoint(self) -> None:
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '920', 'Var')"
        )
        orig = cur.lastrowid
        assert orig is not None
        # Ssyk3/Ssyk5 (shared stem) + Hemkommun (disjoint), all co-delivered in
        # one edition. Same data_type so the cross-cluster edge stays generic.
        gk_s3 = (1, 10, 920, "int", "1", 1, "", "", "Ssyk3")
        gk_s5 = (1, 10, 920, "int", "1", 2, "", "", "Ssyk5")
        gk_hem = (1, 10, 920, "int", "1", 3, "", "", "Hemkommun")
        g_s3 = _StateGroup(1, 10, 920, "int", "1", 1, "")
        g_s3.regvers = {100}
        g_s5 = _StateGroup(1, 10, 920, "int", "1", 2, "")
        g_s5.regvers = {100}
        g_hem = _StateGroup(1, 10, 920, "int", "1", 3, "")
        g_hem.regvers = {100}
        res = _triage_groups(
            conn,
            {gk_s3: g_s3, gk_s5: g_s5, gk_hem: g_hem},
            {(1, 920): orig},
        )
        # Two variables: Ssyk3/Ssyk5 fold into one, Hemkommun splits into its own
        # (NOT three siblings, NOT one folded variable).
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id = 1 AND provider_key = '920'"
        ).fetchone()[0]
        assert n_vars == 2
        assert res.assignments[gk_s3] == res.assignments[gk_s5]  # Ssyk* folded
        assert res.assignments[gk_hem] != res.assignments[gk_s3]  # Hemkommun split
        assert res.stats["clustered"] == 1
        assert res.stats["folds"] == 1  # one multi-column fold cluster
        assert res.stats["splits"] == 1  # one singleton cluster (Hemkommun)
        # One cross-cluster sibling edge, generic (not a code/label or type pair).
        assert {kind for _, _, kind in res.related_edges} == {
            "same_definition_different_column"
        }
        conn.close()

    def test_fold_cluster_emits_slug_hint_and_distinct_labels(self) -> None:
        # Guards the vid passed to `_apply_fold` inside the clustered path: the
        # fold cluster's states must get DISTINCT value_set_version_labels and the
        # cluster's (possibly minted) vid must carry the shared-stem slug hint.
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '920', 'Var')"
        )
        orig = cur.lastrowid
        assert orig is not None
        gk_s3 = (1, 10, 920, "int", "1", 1, "", "", "Ssyk3")
        gk_s5 = (1, 10, 920, "int", "1", 2, "", "", "Ssyk5")
        gk_hem = (1, 10, 920, "int", "1", 3, "", "", "Hemkommun")
        g_s3 = _StateGroup(1, 10, 920, "int", "1", 1, "")
        g_s3.regvers = {100}
        g_s5 = _StateGroup(1, 10, 920, "int", "1", 2, "")
        g_s5.regvers = {100}
        g_hem = _StateGroup(1, 10, 920, "int", "1", 3, "")
        g_hem.regvers = {100}
        res = _triage_groups(
            conn, {gk_s3: g_s3, gk_s5: g_s5, gk_hem: g_hem}, {(1, 920): orig}
        )
        ssyk_vid = res.assignments[gk_s3]
        assert res.assignments[gk_s5] == ssyk_vid  # the two Ssyk states folded
        assert res.labels[gk_s3] != res.labels[gk_s5]  # distinct fold labels
        assert ssyk_vid in res.fold_slug_hints  # shared-stem ("ssyk") slug hint
        conn.close()

    def test_cluster_rep_edge_code_vs_label_for_lid_lnamn(self) -> None:
        # Confirms `reps = [min(c) …]` + `_split_relation_kind` wiring through the
        # clustered path: the Lid/LNamn rep pair (name-based code/label) keeps its
        # specific kind, while the Ssyk cluster's cross edges stay generic. Ssyk
        # is typed neutrally (class "other") so neither Ssyk↔Lid (int) nor
        # Ssyk↔LNamn (text) trips the numeric-vs-text import_bug heuristic.
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '920', 'Var')"
        )
        orig = cur.lastrowid
        assert orig is not None
        gk_s3 = (1, 10, 920, "", "1", 1, "", "", "Ssyk3")
        gk_s5 = (1, 10, 920, "", "1", 2, "", "", "Ssyk5")
        gk_lid = (1, 10, 920, "int", "1", 3, "", "", "Lid")
        gk_lnamn = (1, 10, 920, "text", "1", 4, "", "", "LNamn")
        g_s3 = _StateGroup(1, 10, 920, "", "1", 1, "")
        g_s3.regvers = {100}
        g_s5 = _StateGroup(1, 10, 920, "", "1", 2, "")
        g_s5.regvers = {100}
        g_lid = _StateGroup(1, 10, 920, "int", "1", 3, "")
        g_lid.regvers = {100}
        g_lnamn = _StateGroup(1, 10, 920, "text", "1", 4, "")
        g_lnamn.regvers = {100}
        res = _triage_groups(
            conn,
            {gk_s3: g_s3, gk_s5: g_s5, gk_lid: g_lid, gk_lnamn: g_lnamn},
            {(1, 920): orig},
        )
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id = 1 AND provider_key = '920'"
        ).fetchone()[0]
        assert n_vars == 3  # Ssyk fold + Lid + LNamn
        kind_by_pair = {frozenset((a, b)): kind for a, b, kind in res.related_edges}
        vid_lnamn = res.assignments[gk_lnamn]  # lex-first cluster keeps the origin
        vid_lid = res.assignments[gk_lid]
        vid_ssyk = res.assignments[gk_s3]
        assert vid_lnamn == orig
        assert kind_by_pair[frozenset((vid_lid, vid_lnamn))] == "code_vs_label_pair"
        assert (
            kind_by_pair[frozenset((vid_ssyk, vid_lid))]
            == "same_definition_different_column"
        )
        assert (
            kind_by_pair[frozenset((vid_ssyk, vid_lnamn))]
            == "same_definition_different_column"
        )
        conn.close()


class TestSplitSiblingFlagInheritance:
    """A split sibling inherits is_identifier / is_sensitive from its
    pre-split origin. The A1.2 flags are lifted BEFORE triage, so a sibling
    minted without copying them defaults both to 0 and the flag survives only on
    the lex-first column — the corpus-wide false-negative-identifier regression."""

    @staticmethod
    def _flagged_origin() -> tuple[sqlite3.Connection, int]:
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)  # FKs default off → no register/provider parents needed
        cur = conn.execute(
            "INSERT INTO variable "
            "(register_id, provider_key, name, is_sensitive, is_identifier) "
            "VALUES (1, '57', 'Personnummer', 1, 1)"
        )
        assert cur.lastrowid is not None
        return conn, cur.lastrowid

    def test_apply_split_propagates_flags_to_siblings(self) -> None:
        conn, orig = self._flagged_origin()
        res = _TriageResult({}, {}, set(), {}, {}, [], Counter())
        # 'PNR' (lex-first) keeps the origin; 'PersonNr' mints a sibling that must
        # inherit the flags rather than defaulting to 0/0.
        by_col = {"PNR": [("gk-pnr",)], "PersonNr": [("gk-personnr",)]}
        # codelivered_pairs empty: this test only asserts flag propagation, and
        # PNR/PersonNr don't co-occur here, so every edge is generic.
        _apply_split(conn, {}, by_col, ["PNR", "PersonNr"], set(), 1, 57, orig, res)
        rows = conn.execute(
            "SELECT name, is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '57'"
        ).fetchall()
        assert len(rows) == 2  # origin + one minted sibling
        assert all(r["is_identifier"] == 1 for r in rows)
        assert all(r["is_sensitive"] == 1 for r in rows)
        assert all(r["name"] == "Personnummer" for r in rows)

    def test_split_off_non_contested_propagates_flags(self) -> None:
        conn, orig = self._flagged_origin()
        res = _TriageResult({}, {}, set(), {}, {}, [], Counter())
        _split_off_non_contested(
            conn, {}, {"PersonNrSamh": [("gk",)]}, ["PersonNrSamh"], 1, 57, orig, res
        )
        rows = conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '57'"
        ).fetchall()
        assert len(rows) == 2
        assert all(r["is_identifier"] == 1 and r["is_sensitive"] == 1 for r in rows)

    def test_apply_clustered_propagates_flags_to_siblings(self) -> None:
        # A minted cluster sibling must inherit the flags too (the #139 class).
        # One fold cluster (Ssyk3/Ssyk5) + one singleton (Hemkommun): one of them
        # keeps the origin, the other mints a sibling. Real groups are required —
        # the multi-column cluster routes through `_apply_fold` (reads groups[gk]).
        conn, orig = self._flagged_origin()
        res = _TriageResult({}, {}, set(), {}, {}, [], Counter())
        gk_s3 = (1, 10, 57, "int", "1", 1, "", "", "Ssyk3")
        gk_s5 = (1, 10, 57, "int", "1", 2, "", "", "Ssyk5")
        gk_hem = (1, 10, 57, "int", "1", 3, "", "", "Hemkommun")
        groups = {
            gk_s3: _StateGroup(1, 10, 57, "int", "1", 1, ""),
            gk_s5: _StateGroup(1, 10, 57, "int", "1", 2, ""),
            gk_hem: _StateGroup(1, 10, 57, "int", "1", 3, ""),
        }
        by_col = {"Ssyk3": [gk_s3], "Ssyk5": [gk_s5], "Hemkommun": [gk_hem]}
        _apply_clustered(
            conn,
            groups,
            by_col,
            [["Ssyk3", "Ssyk5"], ["Hemkommun"]],
            set(),
            1,
            57,
            orig,
            res,
        )
        rows = conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '57'"
        ).fetchall()
        assert len(rows) == 2  # origin + one minted sibling
        assert all(r["is_identifier"] == 1 and r["is_sensitive"] == 1 for r in rows)


# ── integration: fold / split / collapse + uniqueness index ───────────────


# Two disjoint 3-code value sets — distinct value_set_ids with symmetric diff 6
# (> the cosmetic threshold), so two codings on one column are genuinely different
# and route to the per-year TIMELINE (mirrors test_codelivery_build's coding pair).
_CLAMP_CODING_A = [("11", "Alpha ett"), ("12", "Alpha två"), ("13", "Alpha tre")]
_CLAMP_CODING_B = [("21", "Beta ett"), ("22", "Beta två"), ("23", "Beta tre")]


def _vm_rows(cvid: int, version: str, codes: list[tuple[str, str]]) -> list[str]:
    """Vardemangder rows for one cvid (version, niva=1, kod, benämning, CVID, ItemId).
    Identical codes across cvids fold to ONE value_set; `version` is the state's
    value_set_version_label."""
    return [PIPE.join([version, "1", kod, ben, str(cvid), ""]) for kod, ben in codes]


class TestSubAnnualBoundaryClamp:
    """#219: a state's lifetime START/END is clamped to the actual sub-annual
    delivery window instead of over-claiming the boundary year. Single-column,
    code-less (value_set_id NULL) variables with no unika row take the fast span
    path, so `valid_from`/`valid_to` come straight from the group's ISO envelope."""

    def _state_bounds(self, conn: sqlite3.Connection, provider_key: str) -> tuple:
        rows = conn.execute(
            "SELECT vs.valid_from, vs.valid_to FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = ?",
            (provider_key,),
        ).fetchall()
        assert len(rows) == 1, f"expected one state for {provider_key}, got {rows}"
        return rows[0]["valid_from"], rows[0]["valid_to"]

    def test_autumn_term_start_clamps_valid_from(self, tmp_path: Path) -> None:
        # Earliest edition is an autumn term (Jul–Dec); the latest is a full year.
        # valid_from narrows to Jul 1, valid_to stays the full boundary year.
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="HtCol",
                    cvid=9600,
                    var_id=940,
                    year="2018",
                    versionname="Höstterminen 2018",
                    regver_id=9610,
                ),
                _var_row(
                    colname="HtCol",
                    cvid=9601,
                    var_id=940,
                    year="2020",
                    versionname="2020",
                    regver_id=9611,
                ),
            ],
        )
        assert self._state_bounds(conn, "940") == ("2018-07-01", "2020-12-31")

    def test_spring_term_end_clamps_valid_to(self, tmp_path: Path) -> None:
        # Latest edition is a spring term (Jan–Jun); valid_to narrows to Jun 30,
        # valid_from stays the full earliest year.
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="VtCol",
                    cvid=9620,
                    var_id=941,
                    year="2018",
                    versionname="2018",
                    regver_id=9630,
                ),
                _var_row(
                    colname="VtCol",
                    cvid=9621,
                    var_id=941,
                    year="2020",
                    versionname="Vårterminen 2020",
                    regver_id=9631,
                ),
            ],
        )
        assert self._state_bounds(conn, "941") == ("2018-01-01", "2020-06-30")

    def test_full_year_edition_blocks_narrowing_at_same_boundary(
        self, tmp_path: Path
    ) -> None:
        # A full-year edition sharing the boundary year keeps -01-01/-12-31: the
        # envelope's min/max sees the full-year bound, so the autumn-term sibling
        # in the same year does NOT narrow it.
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="MixCol",
                    cvid=9640,
                    var_id=942,
                    year="2018",
                    versionname="2018",
                    regver_id=9650,
                ),
                _var_row(
                    colname="MixCol",
                    cvid=9641,
                    var_id=942,
                    year="2018",
                    versionname="Höstterminen 2018",
                    regver_id=9651,
                ),
            ],
        )
        assert self._state_bounds(conn, "942") == ("2018-01-01", "2018-12-31")

    def test_school_year_range_narrows_start_only(self, tmp_path: Path) -> None:
        # A lone school-year range edition narrows its START (Jul 1) but keeps a
        # year-granular END: extending the span into the next calendar year would
        # risk a same-column overlap with a distinct value set delivered then
        # (year-over-year recoding), so the spring tail is deliberately NOT
        # captured (#219 — fixing that UNDER-claim is out of scope here).
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="RangeCol",
                    cvid=9680,
                    var_id=944,
                    year="2020",
                    versionname="Höstterminen 2020 - Vårterminen 2021",
                    regver_id=9690,
                ),
            ],
        )
        assert self._state_bounds(conn, "944") == ("2020-07-01", "2020-12-31")

    def test_yearless_edition_keeps_sentinel_fallback(self, tmp_path: Path) -> None:
        # No parseable year and no unika row → from_iso/to_iso stay None and the
        # existing yearless/open-ended fallback is unchanged.
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="NoYrCol",
                    cvid=9660,
                    var_id=943,
                    versionname="Senaste versionen",
                    regver_id=9670,
                ),
            ],
        )
        assert self._state_bounds(conn, "943") == ("0001-01-01", "9999-12-31")

    def test_timeline_first_run_ht_start_interior_stays_year_granular(
        self, tmp_path: Path
    ) -> None:
        # Force the TIMELINE path: two distinct codings on ONE column (var 950,
        # TimeCol) with overlapping spans. Coding A (HT-start 2018 + 2020) loses
        # year 2019 to coding B (introduced 2019), so A's owned years carve into TWO
        # runs [2018] and [2020]. Exercises the `is_first and run_lo == regver_min`
        # guard in the timeline run loop — never reached by the fast-path tests.
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="TimeCol",
                    cvid=9700,
                    var_id=950,
                    varname="TimeVar",
                    year="2018",
                    versionname="Höstterminen 2018",
                    regver_id=9700,
                    data_length="3",
                ),
                _var_row(
                    colname="TimeCol",
                    cvid=9701,
                    var_id=950,
                    varname="TimeVar",
                    year="2020",
                    versionname="2020",
                    regver_id=9702,
                    data_length="3",
                ),
                _var_row(
                    colname="TimeCol",
                    cvid=9710,
                    var_id=950,
                    varname="TimeVar",
                    year="2019",
                    versionname="2019",
                    regver_id=9711,
                    data_length="3",
                ),
            ],
            vm_extra=(
                _vm_rows(9700, "AlphaA", _CLAMP_CODING_A)
                + _vm_rows(9701, "AlphaA", _CLAMP_CODING_A)
                + _vm_rows(9710, "BetaB", _CLAMP_CODING_B)
            ),
        )
        rows = conn.execute(
            "SELECT valid_from, valid_to, value_set_id FROM variable_state "
            "WHERE delivery_column_name = 'TimeCol' ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 3, rows
        # Coding A's FIRST run starts at its HT-start (Jul 1) — the timeline first-run
        # clamp. The carve middle run (coding B, 2019) is the year-granular handoff.
        assert (rows[0]["valid_from"], rows[0]["valid_to"]) == (
            "2018-07-01",
            "2018-12-31",
        )
        assert (rows[1]["valid_from"], rows[1]["valid_to"]) == (
            "2019-01-01",
            "2019-12-31",
        )
        # A's SECOND (interior) run is fully year-granular — from_iso (2018-07-01) is
        # NOT reapplied to a run that doesn't open at regver_min.
        assert (rows[2]["valid_from"], rows[2]["valid_to"]) == (
            "2020-01-01",
            "2020-12-31",
        )
        # Outer runs are coding A (one value set); the middle is coding B.
        assert (
            rows[0]["value_set_id"]
            == rows[2]["value_set_id"]
            != rows[1]["value_set_id"]
        )

    def test_residual_clamp_overrides_to_iso(self, tmp_path: Path) -> None:
        # `_collapse_residual` pass 2 caps a VT-ending group (to_iso 2020-06-30) whose
        # [regver_min, regver_max] span is crossed by a later same-column, same-value-
        # set group. The YEAR clamp must win over the sub-annual envelope (the
        # `clamp is None` guard in `_emit_span`). Two code-less groups distinguished by
        # data_length share value_set_id NULL + label "" so they stay on the fast path
        # and subgroup together in pass 2.
        conn = _build(
            tmp_path,
            [
                # Group P (data_length 1): 2018 .. VT2020 → to_iso 2020-06-30.
                _var_row(
                    colname="ClampCol",
                    cvid=9720,
                    var_id=951,
                    varname="ClampVar",
                    year="2018",
                    versionname="2018",
                    regver_id=9720,
                    data_length="1",
                ),
                _var_row(
                    colname="ClampCol",
                    cvid=9721,
                    var_id=951,
                    varname="ClampVar",
                    year="2020",
                    versionname="Vårterminen 2020",
                    regver_id=9722,
                    data_length="1",
                ),
                # Group Q (data_length 2): 2020 .. 2021 — crosses P, so pass 2 clamps
                # P to end 2019 (the year before Q's lower bound).
                _var_row(
                    colname="ClampCol",
                    cvid=9723,
                    var_id=951,
                    varname="ClampVar",
                    year="2020",
                    versionname="2020",
                    regver_id=9724,
                    data_length="2",
                ),
                _var_row(
                    colname="ClampCol",
                    cvid=9725,
                    var_id=951,
                    varname="ClampVar",
                    year="2021",
                    versionname="2021",
                    regver_id=9726,
                    data_length="2",
                ),
            ],
        )
        # P (data_length 1): valid_to is the clamp year-end 2019-12-31, NOT the
        # VT2020 envelope end 2020-06-30.
        row = conn.execute(
            "SELECT valid_from, valid_to FROM variable_state "
            "WHERE delivery_column_name = 'ClampCol' AND data_length = '1'"
        ).fetchone()
        assert (row["valid_from"], row["valid_to"]) == ("2018-01-01", "2019-12-31")

    def test_open_sentinel_overrides_to_iso(self, tmp_path: Path) -> None:
        # A still-active group (unika row with blank VersionSista) whose latest
        # edition is a spring term (to_iso 2020-06-30). The open-ended sentinel must
        # win over the sub-annual envelope (the `to_year is not None` guard in
        # `_emit_span`): a currently-live variable stays open, not clamped to VT-end.
        unika_open = PIPE.join(
            [
                "TESTREG",
                "Testregistret",
                "Individer",
                "Individer",
                "OpenVar",
                "OpenCol",
                "2018",
                "",
                "0",
                "0",
                "0",
            ]
        )
        conn = _build(
            tmp_path,
            [
                _var_row(
                    colname="OpenCol",
                    cvid=9730,
                    var_id=952,
                    varname="OpenVar",
                    year="2018",
                    versionname="2018",
                    regver_id=9730,
                ),
                _var_row(
                    colname="OpenCol",
                    cvid=9731,
                    var_id=952,
                    varname="OpenVar",
                    year="2020",
                    versionname="Vårterminen 2020",
                    regver_id=9731,
                ),
            ],
            unika_extra=[unika_open],
        )
        assert self._state_bounds(conn, "952") == ("2018-01-01", "9999-12-31")


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
        # Two variables share provider_key '920' (a split).
        sibs = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = '920'"
        ).fetchall()
        assert len(sibs) == 2, "split should mint a second sibling variable"

    def test_declared_split_var_flags_all_siblings(self, tmp_path: Path) -> None:
        """Change 1 × Change 2 end-to-end: a var_id declared in Identifierare.csv
        that ALSO splits. The declared flag lands on the pre-split variable
        (`_populate_sensitivity_flags`, before triage) and must propagate to
        every split sibling (`_inherited_flags`). var_id 303 (LopNr) is in the
        default IDENTIFIERARE_ROWS; delivering it under two disjoint columns
        (here in reg 1) splits it into siblings that must all stay flagged."""
        conn = _build(
            tmp_path,
            [
                _var_row(colname="Hemkommun", cvid=9500, var_id=303),
                _var_row(colname="Skolkommun", cvid=9501, var_id=303),
            ],
        )
        rows = conn.execute(
            "SELECT is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '303'"
        ).fetchall()
        assert len(rows) == 2, "var_id 303 should split into two siblings"
        assert all(r[0] == 1 for r in rows), (
            "a declared (Identifierare.csv) var_id must flag ALL its split siblings"
        )

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
        # Each sibling's alias set == its single owning column (the query
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
    """The state-uniqueness index is created post-triage and is live."""

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
            clamped_to={},
            fold_slug_hints={},
            related_edges=[],
            stats=Counter(),
        )
        _collapse_residual(groups, res)  # must not raise on None vs int
        # Drift collapses to one; the latest-era group (regver_max 2019) wins.
        assert len(res.dropped) == 1
        assert gk_int not in res.dropped


class TestCollapseResidualOverlap:
    """Pass 2 of `_collapse_residual`: same-column, same-value-set, same-label
    groups whose `[regver_min, regver_max]` spans overlap across DIFFERENT lower
    bounds are reconciled on the fast path — a contained group is dropped, a
    crossing container is range-clamped — while timeline (distinct value set) and
    different-column overlaps are left alone. Each group below starts in its own
    `valid_from` scope, so pass 1 is a no-op and the assertions isolate pass 2."""

    @staticmethod
    def _grp(
        value_set_id: int | None,
        regver_min: int,
        regver_max: int,
        alias: str = "Kon",
    ) -> _StateGroup:
        return _StateGroup(
            register_id=1,
            register_variant_id=10,
            var_id=44,
            data_type="int",
            data_length="",
            value_set_id=value_set_id,
            value_set_version_label="",
            regver_min=regver_min,
            regver_max=regver_max,
            latest_alias=alias,
        )

    @staticmethod
    def _gk(tag: str, value_set_id: int | None) -> tuple:
        # Distinct gk[8] component keeps gkeys distinct; pass 2 keys the column by
        # `latest_alias` (set on the group), not by this component.
        return (1, 10, 44, "int", "", value_set_id, "", "", tag)

    def _res(self, gkeys: list[tuple]) -> _TriageResult:
        return _TriageResult(
            assignments=dict.fromkeys(gkeys, 1),
            labels={},
            dropped=set(),
            clamped_to={},
            fold_slug_hints={},
            related_edges=[],
            stats=Counter(),
        )

    def test_contained_group_dropped(self) -> None:
        gk_wide = self._gk("a", 5)
        gk_inner = self._gk("b", 5)
        groups = {
            gk_wide: self._grp(5, 2010, 2020),
            gk_inner: self._grp(5, 2012, 2015),  # fully inside the wide span
        }
        res = self._res([gk_wide, gk_inner])
        _collapse_residual(groups, res)
        assert gk_inner in res.dropped
        assert gk_wide not in res.dropped
        assert res.clamped_to == {}

    def test_crossing_container_clamped(self) -> None:
        gk_old = self._gk("a", 5)
        gk_new = self._gk("b", 5)
        groups = {
            gk_old: self._grp(5, 2010, 2015),
            gk_new: self._grp(5, 2013, 2020),  # starts inside, extends past
        }
        res = self._res([gk_old, gk_new])
        _collapse_residual(groups, res)
        # Container clamped to end the year before the newer span begins.
        assert res.clamped_to == {gk_old: 2012}
        assert res.dropped == set()

    def test_gap_groups_left_alone(self) -> None:
        gk_a = self._gk("a", 5)
        gk_b = self._gk("b", 5)
        groups = {gk_a: self._grp(5, 2010, 2012), gk_b: self._grp(5, 2015, 2018)}
        res = self._res([gk_a, gk_b])
        _collapse_residual(groups, res)
        assert res.dropped == set()
        assert res.clamped_to == {}

    def test_distinct_value_set_subgrouped_apart(self) -> None:
        # DISTINCT value sets land in distinct pass-2 sub-group keys (value_set_id
        # is in the key), so pass 2 never compares them — independent of routing.
        # (regyears unset here → these are yearless on the fast path; the
        # _spans_overlap-True timeline SKIP is covered by the next test.)
        gk_a = self._gk("a", 5)
        gk_b = self._gk("b", 6)
        groups = {gk_a: self._grp(5, 2010, 2020), gk_b: self._grp(6, 2012, 2015)}
        res = self._res([gk_a, gk_b])
        _collapse_residual(groups, res)
        assert res.dropped == set()
        assert res.clamped_to == {}

    def test_timeline_partition_skipped(self) -> None:
        # A partition routed to the per-year TIMELINE — two YEAR-BEARING (populated
        # regyears) groups with DISTINCT value sets over overlapping windows →
        # _spans_overlap path (a) True — is skipped WHOLESALE by pass 2. This is
        # load-bearing: the SAME-column same-value-set crossing pair (SameCol/vs5)
        # that pass 2 WOULD clamp on the fast path is left untouched, because the
        # timeline owns the whole partition. (`_grp` doesn't init regyears, so set
        # it here — without it both vs5/vs6 groups read as yearless and the
        # distinct-value-set overlap never trips path (a).)
        gk_x = self._gk("x", 5)
        gk_y = self._gk("y", 5)  # crosses gk_x on the same column + value set
        gk_z = self._gk("z", 6)  # distinct value set → forces _spans_overlap True
        gx = self._grp(5, 2010, 2012, alias="SameCol")
        gy = self._grp(5, 2011, 2013, alias="SameCol")
        gz = self._grp(6, 2012, 2016, alias="OtherCol")
        gx.regyears = {2010, 2011, 2012}
        gy.regyears = {2011, 2012, 2013}
        gz.regyears = {2012, 2013, 2014, 2015, 2016}
        groups = {gk_x: gx, gk_y: gy, gk_z: gz}
        # Path (a) actually fires (unlike the regyears-empty test above).
        assert _spans_overlap(groups, [gk_x, gk_y, gk_z]) is True
        res = self._res([gk_x, gk_y, gk_z])
        _collapse_residual(groups, res)
        # Whole partition is timeline-owned → pass 2 makes NO change, not even to
        # the vs5 crossing pair it would otherwise clamp.
        assert res.dropped == set()
        assert res.clamped_to == {}

    def test_different_column_left_alone(self) -> None:
        # Same value set, overlapping spans, but DIFFERENT delivery column
        # (latest_alias) → a legitimate parallel co-delivery, not drift.
        gk_a = self._gk("a", 5)
        gk_b = self._gk("b", 5)
        groups = {
            gk_a: self._grp(5, 2010, 2020, alias="ColA"),
            gk_b: self._grp(5, 2012, 2015, alias="ColB"),
        }
        res = self._res([gk_a, gk_b])
        _collapse_residual(groups, res)
        assert res.dropped == set()
        assert res.clamped_to == {}

    def test_chain_of_three_crossing_clamps_each(self) -> None:
        # A staircase of three overlapping spans clamps each earlier container to
        # the year before the next begins; the latest stays open.
        gk_a = self._gk("a", 5)
        gk_b = self._gk("b", 5)
        gk_c = self._gk("c", 5)
        groups = {
            gk_a: self._grp(5, 2010, 2015),
            gk_b: self._grp(5, 2012, 2018),
            gk_c: self._grp(5, 2016, 2020),
        }
        res = self._res([gk_a, gk_b, gk_c])
        _collapse_residual(groups, res)
        assert res.clamped_to == {gk_a: 2011, gk_b: 2015}
        assert res.dropped == set()

    def test_pass1_disambiguation_feeds_pass2_subgrouping(self) -> None:
        # Two groups collide in ONE pass-1 scope (SAME valid_from year) under the
        # same source `value_set_version_label`, so pass 1 disambiguates the
        # earlier-ending one to 'ver-1'. Pass 2 keys on the EMITTED label
        # (`res.labels.get(gk, value_set_version_label)`), which reads res.labels
        # first, so the two land in DISTINCT sub-groups and are left alone. Guards
        # against keying value_set_version_label before res.labels, which would
        # re-merge them under 'ver' and wrongly clamp. (Same regver_min is REQUIRED
        # here — a different one would put them in separate pass-1 scopes and never
        # disambiguate.)
        gk_a = self._gk("a", 5)
        gk_b = self._gk("b", 5)
        ga = self._grp(5, 2010, 2016)
        gb = self._grp(5, 2010, 2012)  # same scope (2010) + same label → 'ver-1'
        ga.value_set_version_label = "ver"
        gb.value_set_version_label = "ver"
        groups = {gk_a: ga, gk_b: gb}
        res = self._res([gk_a, gk_b])
        _collapse_residual(groups, res)
        # Pass 1 disambiguated → distinct emitted labels.
        assert {res.labels[gk_a], res.labels[gk_b]} == {"ver", "ver-1"}
        # Pass 2 saw distinct labels → distinct sub-groups → no overlap action.
        assert res.dropped == set()
        assert res.clamped_to == {}

    def test_distinct_vintage_sharing_grain_not_merged(self) -> None:
        # Two same-column, same-value-set groups that SHARE a grain mapping to a
        # fold token (gk[7]='grov') but carry DIFFERENT value_set_version_labels
        # ('v2012' vs 'v2012rev'), overlapping years, distinct regver_min, BOTH
        # pass-1 singletons (different from_year → pass 1 leaves res.labels empty).
        # Pass 2 must key on the EMITTED label (res.labels-or-value_set_version_
        # label, what the materializer ships) → distinct sub-groups → left alone.
        # The OLD grain-token key (`_preferred_label`) would merge them under 'grov'
        # and clamp the older, collapsing a distinct vintage the materializer ships
        # intact — and `build-db --validate` would NOT catch it (no invariant
        # broken). gk[7]='grov' maps to fold token 'grov'; gk[6] mirrors the label.
        gk_a = (1, 10, 44, "int", "", 5, "v2012", "grov", "a")
        gk_b = (1, 10, 44, "int", "", 5, "v2012rev", "grov", "b")
        ga = self._grp(5, 2010, 2015, alias="Ssyk")
        gb = self._grp(5, 2012, 2018, alias="Ssyk")  # crosses ga, newer vintage
        ga.value_set_version_label = "v2012"
        gb.value_set_version_label = "v2012rev"
        groups = {gk_a: ga, gk_b: gb}
        res = self._res([gk_a, gk_b])
        _collapse_residual(groups, res)
        assert res.dropped == set()
        assert res.clamped_to == {}


class TestFoldSlugHint:
    """A fold whose shared stem isn't a valid slug must NOT emit a hint — real
    build hit `slug_toml_invalid` for an all-digit stem (`2501`/`2502` → `250`),
    which violates the FQID grammar (slugs start with a letter)."""

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
            clamped_to={},
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
            clamped_to={},
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
    """Seed the PRE-backfill state for the PROVIDER-BLIND classification reader:
    two split siblings sharing `provider_key` '920' (distinct columns + distinct
    classifications), each with a NULL-classification `variable_state` and a
    `classification_candidate` row.

    Mirrors what the build pipeline holds right before
    `_backfill_state_classifications` runs (coalescer done, classifications
    populated, candidate table fed by the SCB feed). A4.4e: the backfill reads
    ONLY `classification_candidate`, so the reader test seeds THAT directly — it
    is provider-blind and never touches `variable_instance`. Returns
    ``(variable_id_a, variable_id_b, cls_a, cls_b)``.

    The two states are CODE-LESS (`value_set_id` NULL on both states and both
    candidate rows) — the "code-less state still adopts a classification" case
    the backfill docstring calls out. This is what defeated the old
    `(register_id, provider_key)` join: its only discriminator was
    `value_set_id IS variable_state.value_set_id`, and `NULL IS NULL` matches, so
    every cvid's classification fanned onto every sibling's state. (With distinct
    non-NULL value sets the old query would coincidentally separate the siblings,
    hiding the bug; NULL is the faithful failing case.) Each candidate carries its
    OWNING `variable_id` (the ground truth the SCB feed projects from the
    coalescer-stamped `variable_instance.variable_id`; set directly here), and the
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
    # Provider-blind candidate rows: each carries its OWN classification + OWNING
    # variable_id, value_set_id NULL. Both siblings share provider_key 920; the
    # variable_id is what isolates them.
    conn.executemany(
        "INSERT INTO classification_candidate "
        "(variable_id, value_set_id, classification_id) VALUES (?, NULL, ?)",
        [(vid_a, cls_a), (vid_b, cls_b)],
    )
    conn.commit()
    return vid_a, vid_b, cls_a, cls_b


class TestBackfillStateClassifications:
    """A4.4e [GAP-1]: `_backfill_state_classifications` reads ONLY the
    provider-blind `classification_candidate` table and attributes each candidate
    to its OWNING split sibling (by the candidate's `variable_id`), not fanning it
    across every sibling sharing the non-unique `(register_id, provider_key)`. A
    code-less state (NULL value_set_id) is tagged via `value_set_id IS NULL`, and
    multiple candidates for one state key resolve by the lowest-id min()
    tie-break."""

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
        # Sibling A's (code-less, value_set_id NULL) state carries cls_a; B's
        # carries cls_b. Pre-fix the bare provider_key join fanned both candidates
        # onto both siblings; the min() tie-break then put cls_a (lower id) on B's
        # state too. The `value_set_id IS NULL` match is what tags the code-less
        # states here.
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

    def test_min_tiebreak_for_same_state_key(self) -> None:
        """Two candidate rows share one `(variable_id, value_set_id)` but carry
        DIFFERENT classifications; the lowest-id wins (deterministic min())."""
        from reg_meta_build.db import _backfill_state_classifications

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        vid_a, _vid_b, cls_a, cls_b = _seed_split_for_backfill(conn)
        # Add a SECOND candidate for sibling A's same (variable_id, NULL) key with
        # the HIGHER classification id (cls_b > cls_a). min() must pick cls_a.
        conn.execute(
            "INSERT INTO classification_candidate "
            "(variable_id, value_set_id, classification_id) VALUES (?, NULL, ?)",
            (vid_a, cls_b),
        )
        conn.commit()

        _backfill_state_classifications(conn)

        got = conn.execute(
            "SELECT classification_id FROM variable_state WHERE variable_id = ?",
            (vid_a,),
        ).fetchone()["classification_id"]
        assert got == min(cls_a, cls_b) == cls_a
        conn.close()


class TestClassificationCandidateFeed:
    """A4.4e: the STEP 2 SCB feed projects `variable_instance` into the
    provider-blind `classification_candidate` table, keeping ONLY the rows the
    backfill used to read directly (`classification_id IS NOT NULL AND
    variable_id IS NOT NULL`)."""

    def test_feed_keeps_only_classified_with_variable_id(self) -> None:
        from reg_meta_build.db import (
            _CLASSIFICATION_CANDIDATE_FEED_SQL,
            DDL,
            seed_providers,
        )

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (1, 1, 'testreg', 'TESTREG')"
        )
        conn.execute(
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name) "
            "VALUES (10, 1, 'individer', 'Individer')"
        )
        cls = conn.execute(
            "INSERT INTO classification (short_name, name) VALUES ('KON', 'Kon')"
        ).lastrowid
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '920', 'Kommun', 'kommun')"
        ).lastrowid
        # cvid 1: classified + stamped variable_id + a value_set_id → KEPT.
        # cvid 2: classified but NULL variable_id (collision residual) → DROPPED.
        # cvid 3: stamped variable_id but NULL classification → DROPPED.
        conn.execute(
            "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
            "regver_id, var_id, classification_id, value_set_id, variable_id) "
            "VALUES (1, 1, 10, 100, 920, ?, 77, ?)",
            (cls, vid),
        )
        conn.execute(
            "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
            "regver_id, var_id, classification_id, value_set_id, variable_id) "
            "VALUES (2, 1, 10, 100, 920, ?, NULL, NULL)",
            (cls,),
        )
        conn.execute(
            "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
            "regver_id, var_id, classification_id, value_set_id, variable_id) "
            "VALUES (3, 1, 10, 100, 920, NULL, NULL, ?)",
            (vid,),
        )
        conn.commit()

        conn.execute(_CLASSIFICATION_CANDIDATE_FEED_SQL)

        rows = conn.execute(
            "SELECT variable_id, value_set_id, classification_id "
            "FROM classification_candidate ORDER BY variable_id"
        ).fetchall()
        # Exactly the (classification_id IS NOT NULL AND variable_id IS NOT NULL)
        # subset — just cvid 1.
        assert [tuple(r) for r in rows] == [(vid, 77, cls)]
        conn.close()


class TestEmitVariableAliases:
    """A4.3a: `SCBAdapter._emit_variable_aliases` carries the FULL delivery-column
    history as `IRVariableAlias`, attributing each cvid's columns to its OWNING
    split sibling via the coalescer-stamped `variable_instance.variable_id` — no
    column-tie heuristic, no skip. (Replaces the deleted `_reparent_variable_alias`
    build pass; the materializer now writes `variable_alias` from this IR.)"""

    def test_split_siblings_get_own_columns_incl_historical(self) -> None:
        """A cvid whose ONLY column is a historical one that never became a
        coalesced `variable_state` is still attributed to its owning sibling. The
        old column-tie SKIPPED such a cvid (its columns resolved to no
        state-bearing sibling), dropping the column from `variable_alias`; the
        ground-truth `variable_id` stamp recovers it under the right sibling."""
        from reg_meta_build.db import DDL, seed_providers
        from reg_meta_build.ir import IRVariableAlias
        from reg_meta_build.sources.scb import SCBAdapter

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

        adapter = SCBAdapter(conn)
        aliases = list(adapter._emit_variable_aliases())
        assert all(isinstance(a, IRVariableAlias) for a in aliases)
        cols = {(a.variable_id, a.delivery_column_name) for a in aliases}
        # A keeps its current + historical column; B only its own. Neither
        # sibling leaks the other's column, and 'Hemkn_old' is recovered.
        assert cols == {
            (vid_a, "Hemkommun"),
            (vid_a, "Hemkn_old"),
            (vid_b, "Skolkommun"),
        }
        # Every alias rides the delivering variant (FK target for variable_alias).
        assert {a.register_variant_id for a in aliases} == {10}
        conn.close()
