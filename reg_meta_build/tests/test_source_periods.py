"""Focused tests for bounded stage-one source-period normalization."""

from __future__ import annotations

import pytest
from reg_meta_build.source_periods import source_scopes


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("2025-12-31", "2025-12-31"),
        ("1 jan 2010", "2010-01-01"),
        ("15 oktober 2024", "2024-10-15"),
        ("  15   OKTOBER\t2008  ", "2008-10-15"),
        ("  1   JAN\t2010  ", "2010-01-01"),
    ),
)
def test_actual_snapshot_dates_keep_day_precision(source: str, expected: str) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.kind == edition_period.kind == "intervals"
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        (expected[:4], expected[:4])
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [(expected, expected)]


@pytest.mark.parametrize("separator", [" - ", "–"])
def test_same_year_iso_range_keeps_exact_edition_period_bounds(separator: str) -> None:
    edition, edition_period, issue = source_scopes(f"2025-03-04{separator}2025-10-06")

    assert issue is None
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        ("2025", "2025")
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [("2025-03-04", "2025-10-06")]


@pytest.mark.parametrize(
    "source",
    (
        "2025-02-29",
        "2025-2-01",
        "2025-13-01",
        "32 oktober 2024",
        "32 jan 2010",
        "1 feb 2010",
        "2025-02-01 - 2025-01-31",
        "2025-01-01 - 2025-02-29",
    ),
)
def test_invalid_actual_dates_are_diagnostic_unknowns(source: str) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == source


@pytest.mark.parametrize(
    ("source", "edition_bounds", "edition_period_bounds"),
    (
        ("2025", ("2025", "2025"), ("2025-01-01", "2025-12-31")),
        (
            "Vårterminen 2025",
            ("2025", "2025"),
            ("2025-01-01", "2025-06-30"),
        ),
        (
            "2005 kvartal 2-4",
            ("2005", "2005"),
            ("2005-04-01", "2005-12-31"),
        ),
    ),
)
def test_single_claim_fallback_keeps_existing_edition_and_edition_period_meanings(
    source: str,
    edition_bounds: tuple[str, str],
    edition_period_bounds: tuple[str, str],
) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.kind == edition_period.kind == "intervals"
    assert (edition.intervals[0].start, edition.intervals[0].end) == edition_bounds
    assert (
        edition_period.intervals[0].start,
        edition_period.intervals[0].end,
    ) == edition_period_bounds


@pytest.mark.parametrize(
    ("source", "edition_period_bounds"),
    (
        ("Läsåret 2014/2015", ("2014-07-01", "2015-06-30")),
        ("läsåret 2014/2015", ("2014-07-01", "2015-06-30")),
        ("LÄSÅRET 2014/2015", ("2014-07-01", "2015-06-30")),
        ("Läsåret2014/2015", ("2014-07-01", "2015-06-30")),
        ("Läsåret  2014 / 2015", ("2014-07-01", "2015-06-30")),
        (
            "Deklarationsår 2020 (beskattningsår 2019)",
            ("2019-01-01", "2019-12-31"),
        ),
    ),
)
def test_school_year_and_income_year_labels_form_one_exact_period(
    source: str, edition_period_bounds: tuple[str, str]
) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.kind == edition_period.kind == "intervals"
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        edition_period_bounds
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [edition_period_bounds]


@pytest.mark.parametrize(
    "source",
    (
        "Läsåret 2014/2016",
        "Deklarationsår 2020",
        "Deklarationsår 2020 (beskattningsår 2020)",
        "Deklarationsår 2020 (beskattningsår 2018)",
    ),
)
def test_school_year_and_income_year_guards_stay_unknown(source: str) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == source


@pytest.mark.parametrize(
    ("source", "edition_period_bounds"),
    (
        ("1997/1998", ("1997-07-01", "1998-06-30")),
        ("1997 / 1998", ("1997-07-01", "1998-06-30")),
    ),
)
def test_bare_split_year_reads_like_lasaret(
    source: str, edition_period_bounds: tuple[str, str]
) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.kind == edition_period.kind == "intervals"
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        edition_period_bounds
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [edition_period_bounds]


def test_non_consecutive_bare_split_year_stays_unknown() -> None:
    edition, edition_period, issue = source_scopes("1997/1999")

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == "1997/1999"


@pytest.mark.parametrize(
    ("source", "edition_period_bounds"),
    (
        ("2011-04 - 2012-03", ("2011-04-01", "2012-03-31")),
        ("2011-04–2012-03", ("2011-04-01", "2012-03-31")),
        ("2012-02 - 2012-02", ("2012-02-01", "2012-02-29")),
    ),
)
def test_month_range_forms_one_precise_period(
    source: str, edition_period_bounds: tuple[str, str]
) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.kind == edition_period.kind == "intervals"
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        edition_period_bounds
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [edition_period_bounds]


def test_reversed_month_range_stays_unknown() -> None:
    edition, edition_period, issue = source_scopes("2012-03 - 2011-04")

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == "2012-03 - 2011-04"


@pytest.mark.parametrize(
    ("source", "edition_period_bounds"),
    (
        (
            "Höstterminen 2020 - Vårterminen 2021",
            ("2020-07-01", "2021-06-30"),
        ),
        (
            "höstterminen 2020–vårterminen 2021",
            ("2020-07-01", "2021-06-30"),
        ),
    ),
)
def test_term_range_reads_like_lasaret(
    source: str, edition_period_bounds: tuple[str, str]
) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.kind == edition_period.kind == "intervals"
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        edition_period_bounds
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [edition_period_bounds]


def test_non_consecutive_term_range_stays_unknown() -> None:
    source = "Höstterminen 2020 - Vårterminen 2022"
    edition, edition_period, issue = source_scopes(source)

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == source


def test_lasaren_school_year_hull_pools_over_the_hull() -> None:
    source = "Läsåren 1977/1978 - 2024/2025"
    edition, edition_period, issue = source_scopes(source)

    assert issue == "pooled_period"
    assert edition.kind == edition_period.kind == "pooled"
    assert edition.label == edition_period.label == source
    # Y-208: whole school years hull like whole calendar years — one marked
    # pooled state over 1 July of the first year to 30 June of the last.
    assert (edition.pooled_start, edition.pooled_end) == (
        "1977-07-01",
        "2025-06-30",
    )
    assert (edition_period.pooled_start, edition_period.pooled_end) == (
        "1977-07-01",
        "2025-06-30",
    )


def test_lasaren_hull_without_consecutive_pairs_keeps_no_bounds() -> None:
    source = "Läsåren 1977/1979 - 2024/2025"
    edition, edition_period, issue = source_scopes(source)

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == source


@pytest.mark.parametrize(
    ("source", "pooled_range"),
    (
        ("2012 - 2014", ("2012-01-01", "2014-12-31")),
        ("1990-2000", ("1990-01-01", "2000-12-31")),
        ("1961-01-01 –– 2025-12-31", ("1961-01-01", "2025-12-31")),
    ),
)
def test_multi_year_claims_stay_pooled(
    source: str, pooled_range: tuple[str, str]
) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue == "pooled_period"
    assert edition.kind == edition_period.kind == "pooled"
    assert edition.label == edition_period.label == source
    # Y-202: a whole-year pooled scope carries its whole-range bounds — the
    # first claim's start through the last claim's end — so resolution can
    # form one marked state without inferring annual availability inside the
    # range.
    assert (edition.pooled_start, edition.pooled_end) == pooled_range
    assert (edition_period.pooled_start, edition_period.pooled_end) == pooled_range


def test_term_structured_pooled_labels_carry_no_resolvable_range() -> None:
    source = "Komvux HT 1988 - VT 2024"
    edition, edition_period, issue = source_scopes(source)

    assert issue == "pooled_period"
    assert edition.kind == edition_period.kind == "pooled"
    assert edition.label == edition_period.label == source
    # Y-202: term/school-year edges are not whole-year delivery evidence —
    # these stay unresolvable, as before.
    assert edition.pooled_start == edition.pooled_end is None
    assert edition_period.pooled_start == edition_period.pooled_end is None


@pytest.mark.parametrize(
    "source",
    (
        "1990, 2000",
        "LISA 2011 och 2019",
        "2025/12/31",
        "2025.12.31",
        "31122025",
    ),
)
def test_ambiguous_or_unsupported_numeric_text_is_not_guessed(source: str) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue == "unparseable_period"
    assert edition.kind == edition_period.kind == "unknown"
    assert edition.label == edition_period.label == source


def test_labels_use_canonical_whitespace_without_losing_source_label_text() -> None:
    source = "  Läsåret\u00a02014/2015\n"

    edition, edition_period, issue = source_scopes(source)

    assert issue is None
    assert edition.label is None and edition_period.label is None
    assert [(interval.start, interval.end) for interval in edition.intervals] == [
        ("2014-07-01", "2015-06-30")
    ]
    assert [
        (interval.start, interval.end) for interval in edition_period.intervals
    ] == [("2014-07-01", "2015-06-30")]
