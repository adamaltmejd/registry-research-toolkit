"""Edition conventions and legacy declaration validation used during conversion."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.fqid import period_token_to_bounds
from reg_meta_build._curation import _data_type_class, _looks_like_code_label_pair
from reg_meta_build.edition_bounds import (
    edition_bounds,
    edition_claims,
    vintage_claim,
)
from reg_meta_build.sources.scb import register_edition_claims

if TYPE_CHECKING:
    from pathlib import Path


class TestEditionBounds:
    """#219 sub-annual delivery-window parser. The term/quarter/half forms must
    AGREE with reg_meta's `period_token_to_bounds` (build & resolve share the same
    expansion); everything else stays full-year. The second arg is the row's edition
    year — only markers matching it are narrowed, so a window can never escape it.

    This is the WITHIN-ONE-YEAR half of the parse. A name whose own span crosses a
    year boundary is widened by `edition_claims` (`TestEditionClaims` below), which
    calls this for the single-year case."""

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
            assert edition_bounds(name, 2024) == ht, name
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
            assert edition_bounds(name, 2024) == vt, name
        assert vt == ("2024-01-01", "2024-06-30")

    def test_quarter_forms_agree_with_period_bounds(self) -> None:
        assert edition_bounds("2005 kvartal 1", 2005) == period_token_to_bounds(
            "2005-Q1"
        )
        assert edition_bounds("2011 kv1", 2011) == period_token_to_bounds("2011-Q1")
        # A range spans its endpoints (union of the two quarter tokens).
        assert edition_bounds("2005 kvartal 2-4", 2005) == ("2005-04-01", "2005-12-31")
        # …whichever dash SCB typed. `fold_column` NFKD-DROPS an en dash rather
        # than folding it, so without the shared dash normalization this read as
        # a lone Q2 and lost half the year (Y-113 r2).
        assert edition_bounds("2005 kvartal 2–4", 2005) == edition_bounds(
            "2005 kvartal 2-4", 2005
        )
        assert edition_bounds("2007 kv 2-kv 4", 2007) == ("2007-04-01", "2007-12-31")
        assert edition_bounds("Kvartal 1-3 fr.o.m 2010", 2010) == (
            "2010-01-01",
            "2010-09-30",
        )

    def test_half_year_forms_agree_with_period_bounds(self) -> None:
        assert edition_bounds("Första halvåret 1995", 1995) == period_token_to_bounds(
            "1995-H1"
        )
        assert edition_bounds("Andra halvåret 1995", 1995) == period_token_to_bounds(
            "1995-H2"
        )

    def test_term_range_narrows_to_the_edition_year_term(self) -> None:
        # The edition year (extract_year = the FIRST year) selects which term is
        # narrowed; the other-year term is dropped, so the window stays WITHIN the
        # edition year (start narrowed, year-granular end — no cross-year extension).
        # `edition_claims` is what carries the range's remaining years (Y-113).
        assert edition_bounds("Höstterminen 2020 - Vårterminen 2021", 2020) == (
            "2020-07-01",
            "2020-12-31",
        )
        assert edition_bounds("Komvux HT 1988 - VT 2024", 1988) == (
            "1988-07-01",
            "1988-12-31",
        )

    def test_term_for_other_year_dropped_no_inversion(self) -> None:
        # A collection note whose term names a DIFFERENT year than the edition year
        # must NOT yield that term's bounds (which would invert valid_from > valid_to
        # once the materializer clamps valid_to to the edition year). The term is
        # dropped → full edition year.
        assert edition_bounds("Insamling 2019 avseende höstterminen 2020", 2019) == (
            "2019-01-01",
            "2019-12-31",
        )

    def test_out_of_range_term_year_does_not_crash(self) -> None:
        # `period_token_to_bounds("HT1850")` would raise FqidError (year < 1900). The
        # 1850 term mismatches the edition year 2024 → dropped → full 2024, no crash.
        assert edition_bounds("HT 1850, version 2024", 2024) == (
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
            assert edition_bounds(name, 2024) == full, name
        # A school year carries no marker for its own edition year: the HT/VT split
        # only exists once the name's full span is read (`edition_claims`).
        assert edition_bounds("Läsåret 2013/2014", 2013) == (
            "2013-01-01",
            "2013-12-31",
        )

    def test_yearless_returns_none(self) -> None:
        # year=None (yearless row) → None regardless of the name's content.
        assert edition_bounds(None, None) is None
        assert edition_bounds("", None) is None
        assert edition_bounds("Senaste versionen", None) is None
        assert edition_bounds("Ekonomiskt bistånd, kvartal", None) is None


class TestEditionClaims:
    """Y-113: a version name that spans several years is claimed for all of them.

    One claim per calendar year spanned, each nested in its own year (the
    `Claim` contract), so the claim KEY SET still carries the run/gap structure
    the coalescer fuses on. A pure function of the name — the projection policy
    is the caller's declared register set, pinned in `TestVintageClaim`.
    """

    def test_single_year_names_claim_exactly_edition_bounds(self) -> None:
        # The whole pre-Y-113 surface: single-year, quarter, half-year and
        # single-term names each still yield ONE claim, `edition_bounds`' window.
        for name, year in (
            ("2024", 2024),
            ("2024, slutlig version", 2024),
            ("Höstterminen 2024", 2024),
            ("Vårterminen 2024", 2024),
            ("2005 kvartal 2-4", 2005),
            ("Första halvåret 1995", 1995),
            ("Kvartal 1-3 fr.o.m 2010", 2010),
            ("15 oktober 2024", 2024),
        ):
            assert edition_claims(name) == ((year, *edition_bounds(name, year)),), name

    def test_year_range_claims_every_year_it_spans(self) -> None:
        # innovation-foretag `2004 - 2006`; interior years are full calendar years
        # so a gap between versions stays a gap.
        assert edition_claims("2004 - 2006") == (
            (2004, "2004-01-01", "2004-12-31"),
            (2005, "2005-01-01", "2005-12-31"),
            (2006, "2006-01-01", "2006-12-31"),
        )
        # hreg doktorander `1971 - 2024`, and the un-spaced spelling.
        assert edition_claims("1971 - 2024")[-1] == (
            2024,
            "2024-01-01",
            "2024-12-31",
        )
        assert edition_claims("2004-2006") == edition_claims("2004 - 2006")

    def test_iso_date_range_claims_every_year_it_spans(self) -> None:
        # flergenreg `1961-01-01 –– 2025-12-31`: en dashes, which `fold_column`
        # would DELETE rather than fold, so they are mapped to ASCII first.
        claims = edition_claims("1961-01-01 –– 2025-12-31")
        assert len(claims) == 65
        assert claims[0] == (1961, "1961-01-01", "1961-12-31")
        assert claims[-1] == (2025, "2025-01-01", "2025-12-31")
        assert edition_claims("1961-01-01 -- 2025-12-31") == claims

    def test_school_year_claims_ht_through_vt(self) -> None:
        # grundskola-ak9 / gymnasieskola-betyg / lararreg `Läsåret 2012/2013`:
        # HT of the first year through VT of the last, on the period grammar's
        # own term bounds.
        assert edition_claims("Läsåret 2012/2013") == (
            (2012, *period_token_to_bounds("HT2012")),
            (2013, *period_token_to_bounds("VT2013")),
        )
        # A bare `A/B` reads the same way.
        assert edition_claims("2012/2013") == edition_claims("Läsåret 2012/2013")

    def test_school_year_range_claims_ht_first_through_vt_last(self) -> None:
        # hreg grundutbildning `Läsåren 1993/1994 - 2024/2025` → HT1993..VT2025.
        claims = edition_claims("Läsåren 1993/1994 - 2024/2025")
        assert claims[0] == (1993, *period_token_to_bounds("HT1993"))
        assert claims[-1] == (2025, *period_token_to_bounds("VT2025"))
        # Interior years are whole, so every year of the series is claimed once.
        assert [year for year, _, _ in claims] == list(range(1993, 2026))
        assert claims[1] == (1994, "1994-01-01", "1994-12-31")

    def test_non_consecutive_slash_pair_is_not_a_school_year(self) -> None:
        # A classification vintage pair is not a school year; it keeps the
        # single-year claim rather than inventing a 21-year span.
        assert edition_claims("SUN 2000/2020") == ((2000, "2000-01-01", "2000-12-31"),)

    def test_term_range_claims_first_term_through_last(self) -> None:
        # utbildningsanalyser `Höstterminen 2020 - Vårterminen 2021`.
        assert edition_claims("Höstterminen 2020 - Vårterminen 2021") == (
            (2020, *period_token_to_bounds("HT2020")),
            (2021, *period_token_to_bounds("VT2021")),
        )
        # ureg `Komvux HT 1988 - VT 2024`: HT1988, whole years, then VT2024.
        claims = edition_claims("Komvux HT 1988 - VT 2024")
        assert claims[0] == (1988, *period_token_to_bounds("HT1988"))
        assert claims[-1] == (2024, *period_token_to_bounds("VT2024"))
        assert claims[1] == (1989, "1989-01-01", "1989-12-31")

    def test_a_span_ending_in_the_future_is_still_a_span(self) -> None:
        # ESF `Programperiod 2021-2027` is a real programme period and claims all
        # of it, even though it ends after every edition the corpus names. The
        # name parser has no build year, corpus statistic or wall clock to
        # compare against — a forecast horizon is the CALLER's declared fact.
        claims = edition_claims("Programperiod 2021-2027")
        assert [year for year, _, _ in claims] == list(range(2021, 2028))
        # Read as a name alone, a projection horizon is no different.
        assert len(edition_claims("2011-2060")) == 50

    def test_span_must_start_in_the_edition_year(self) -> None:
        # A collection year in front of the period it describes: 2019 is the
        # edition's own claim, so the HT2020 note does not move or widen it
        # (it would otherwise drop 2019 out of the claim key set entirely).
        assert edition_claims("Insamling 2019 avseende höstterminen 2020") == (
            (2019, "2019-01-01", "2019-12-31"),
        )

    def test_out_of_range_term_year_does_not_crash(self) -> None:
        # `period_token_to_bounds("HT1850")` would raise; the marker is dropped,
        # leaving a single full-year 2024 claim.
        assert edition_claims("HT 1850, version 2024") == (
            (2024, "2024-01-01", "2024-12-31"),
        )

    def test_yearless_claims_nothing(self) -> None:
        # No parseable edition year → no claim; the caller's yearless/unika
        # fallback fires instead.
        assert edition_claims(None) == ()
        assert edition_claims("Senaste versionen") == ()

    def test_claim_windows_are_nested_in_their_own_year(self) -> None:
        # The `Claim` contract every consumer relies on: a window never crosses a
        # year boundary, and consecutive claims tile without overlapping.
        for name in (
            "Läsåren 1993/1994 - 2024/2025",
            "Komvux HT 1988 - VT 2024",
            "1961-01-01 –– 2025-12-31",
            "2004 - 2006",
        ):
            claims = edition_claims(name)
            assert [y for y, _, _ in claims] == list(
                range(claims[0][0], claims[-1][0] + 1)
            ), name
            for y, lo, hi in claims:
                assert lo[:4] == hi[:4] == f"{y:04d}", (name, y)
                assert lo <= hi, (name, y)


class TestVintageClaim:
    """The reading a DECLARED projection register gets: the version IS the
    vintage, so a name states the horizon its forecast reaches rather than years
    anything was delivered for. `register_edition_claims` routes register ids in
    `_PROJECTION_REGISTERS` here and every other register to `edition_claims`."""

    def test_projection_name_keeps_its_vintage_year(self) -> None:
        # befolkningsframskrivningar (register 310): `2011-2060` is one
        # 2011-vintage forecast, not a 50-year delivery span.
        assert vintage_claim("2011-2060") == ((2011, "2011-01-01", "2011-12-31"),)
        assert register_edition_claims(310, "2009-2060 huvudalternativ") == (
            (2009, "2009-01-01", "2009-12-31"),
        )

    def test_every_other_register_reads_the_full_span(self) -> None:
        # The same name on a non-declared register claims all of it — the guard
        # is the register, never the shape of the name.
        assert register_edition_claims(311, "2011-2060") == edition_claims("2011-2060")
        assert len(register_edition_claims(311, "2011-2060")) == 50

    def test_vintage_still_narrows_within_its_own_year(self) -> None:
        # It is `edition_bounds`' window, not a blind full year: a sub-annual
        # marker on a projection version still narrows inside the vintage year.
        assert vintage_claim("Höstterminen 2024") == (
            (2024, *period_token_to_bounds("HT2024")),
        )

    def test_yearless_claims_nothing(self) -> None:
        assert vintage_claim(None) == ()
        assert vintage_claim("Senaste versionen") == ()


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
        # ids must be canonical ints (shared `canonical_int`, the convention every
        # id-keyed curation loader uses). `int(...)` would coerce each of these to an
        # inert never-matching pin (`why`) instead of a load-time error.
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
        # canonical_int path must keep the same EXIT_CONFIG rejection, not fall
        # through to a raw crash if the `reg is None or var is None` guard ever
        # regresses.
        from reg_meta.errors import RegMetaError
        from reg_meta_build.codelivery import load_codelivery

        bad = tmp_path / "missing.toml"
        bad.write_text(
            '[[resolve]]\nvar_id=1\ncolumn="c"\nkeep="x"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            load_codelivery(bad)
        assert exc.value.code == "codelivery_invalid"


class TestDataTypeClass:
    def test_classes(self) -> None:
        assert _data_type_class("Heltal") == "numeric"
        assert _data_type_class("Sträng (text)") == "text"  # locks the ä→a fold
        assert _data_type_class("Datum") == "other"
        assert _data_type_class(None) == "other"


# from the concept-group fold; nothing persists them since the researcher-facing
# `variable_related_to` edge was retired). ───────────────────────────────────


class TestLooksLikeCodeLabelPair:
    def test_two_namn_columns_are_not_a_pair(self) -> None:
        assert not _looks_like_code_label_pair("Fornamn", "Efternamn")

    def test_code_label_pairs(self) -> None:
        assert _looks_like_code_label_pair("Lid", "LNamn")  # code suffix vs namn
        assert _looks_like_code_label_pair("Sun2000Kod", "Sun2000Namn")  # kod vs namn
        assert _looks_like_code_label_pair("Kommun", "Kommunnamn")  # bare stem vs namn

    def test_order_independent(self) -> None:
        assert _looks_like_code_label_pair("Kommunnamn", "Kommun")


# Two disjoint 3-code value sets — distinct value_set_ids with symmetric diff 6
# (> the cosmetic threshold), so two codings on one column are genuinely different
# and route to the per-year TIMELINE (mirrors test_codelivery_build's coding pair).
_CLAMP_CODING_A = [("11", "Alpha ett"), ("12", "Alpha två"), ("13", "Alpha tre")]
_CLAMP_CODING_B = [("21", "Beta ett"), ("22", "Beta två"), ("23", "Beta tre")]
