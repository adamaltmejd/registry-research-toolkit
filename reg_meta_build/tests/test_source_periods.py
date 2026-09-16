"""Focused tests for bounded stage-one source-period normalization."""

from __future__ import annotations

import pytest
from reg_meta_build.source_periods import source_scopes


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("2025-12-31", "2025-12-31"),
        ("15 oktober 2024", "2024-10-15"),
        ("  15   OKTOBER\t2008  ", "2008-10-15"),
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
    "source",
    (
        "Komvux HT 1988 - VT 2024",
        "Läsåret 2024/2025",
        "Läsåren 1977/1978 - 1992/1993",
        "1961-01-01 –– 2025-12-31",
    ),
)
def test_multi_year_claims_stay_pooled(source: str) -> None:
    edition, edition_period, issue = source_scopes(source)

    assert issue == "pooled_period"
    assert edition.kind == edition_period.kind == "pooled"
    assert edition.label == edition_period.label == source


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
    source = "  Läsåret\u00a02024/2025\n"

    edition, edition_period, issue = source_scopes(source)

    assert issue == "pooled_period"
    assert edition.label == edition_period.label == "Läsåret 2024/2025"
