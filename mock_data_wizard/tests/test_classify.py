"""Tests for classify.classify_column.

Covers each branch of the type/n_distinct/n_rows decision tree, plus the
SCB name-pattern rules (lopnr -> id, kommun cap, etc.) and date format
detection from sample values.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from mock_data_wizard.classify import (
    DATE_FORMATS,
    FREQ_CAP,
    classify_column,
    detect_date_format,
    is_known_id,
    known_categorical_cap,
)


# -- name-based patterns --------------------------------------------------


def test_is_known_id_lopnr_match():
    assert is_known_id("LopNr") is True
    assert is_known_id("p1105_lopnr_personnr") is True


def test_is_known_id_no_match():
    assert is_known_id("age") is False
    assert is_known_id("kommun") is False


def test_known_categorical_cap_kommun():
    assert known_categorical_cap("Kommun") == 500
    # exclude rule: "kommunikation" should NOT match the kommun pattern
    assert known_categorical_cap("Kommunikation") is None


def test_known_categorical_cap_ssyk_sun_sni():
    assert known_categorical_cap("SSYK4") == 1000
    assert known_categorical_cap("Sun2000Niva") == 1000
    assert known_categorical_cap("Sun2020Inriktning") == 1000
    assert known_categorical_cap("SNI2007") == 1500


def test_known_categorical_cap_country_and_citizenship():
    assert known_categorical_cap("Fodelseland") == 300
    assert known_categorical_cap("Medborgarskap") == 300


def test_known_categorical_cap_no_match():
    assert known_categorical_cap("age") is None


# -- top-level dispatch: ID via name pattern always wins ------------------


def test_classify_lopnr_returns_id_regardless_of_data():
    # Even if the sample looks tiny and like a categorical, lopnr -> id
    assert (
        classify_column("LopNr", n_rows=8_000_000, n_distinct=5, sample=[1, 2, 3, 1, 2])
        == "id"
    )


# -- name-cap categorical -------------------------------------------------


def test_kommun_under_cap_is_categorical():
    assert (
        classify_column(
            "Kommun", n_rows=8_000_000, n_distinct=290, sample=["0114", "0115"]
        )
        == "categorical"
    )


def test_kommun_over_cap_falls_through_to_data_driven():
    # If something with a kommun-shaped name has 5000 distinct (way over cap),
    # the cap is ignored and we go to the data-driven path. With a string
    # sample and high cardinality, we'd land on high_cardinality.
    out = classify_column(
        "Kommun_freeform",
        n_rows=8_000_000,
        n_distinct=5000,
        sample=["a free string"],
    )
    assert out in ("id", "high_cardinality")


# -- numeric path ---------------------------------------------------------


def test_numeric_column_with_high_cardinality_is_numeric():
    assert (
        classify_column(
            "age",
            n_rows=1000,
            n_distinct=80,
            sample=list(range(80)),
        )
        == "numeric"
    )


def test_numeric_with_few_distinct_is_categorical():
    # 4 distinct in 1000 rows -> well under FREQ_CAP/threshold
    assert (
        classify_column("flag", n_rows=1000, n_distinct=4, sample=[0, 1, 2, 3])
        == "categorical"
    )


def test_numeric_with_almost_unique_is_id():
    # nd > NUMERIC_ID_RATIO * n_rows AND > NUMERIC_ID_MIN -> id
    assert (
        classify_column(
            "PersonNr_int",
            n_rows=1000,
            n_distinct=999,
            sample=list(range(50)),
        )
        == "id"
    )


def test_numeric_yyyymmdd_integer_is_date():
    sample = [20200101, 20200115, 20200201, 20200315, 20210601, 20221231]
    out = classify_column(
        "DateAsInt",
        n_rows=10000,
        n_distinct=2000,
        sample=sample,
    )
    assert out == "date"


def test_numeric_integer_outside_yyyymmdd_range_is_numeric():
    # Plain integers, not date-shaped
    sample = [1, 2, 3, 100, 500, 1000]
    out = classify_column(
        "amount",
        n_rows=10000,
        n_distinct=2000,
        sample=sample,
    )
    assert out == "numeric"


def test_decimal_values_treated_as_numeric():
    # SQL drivers often hand back Decimal for NUMERIC columns
    sample = [Decimal("1.5"), Decimal("2.5"), Decimal("3.0"), Decimal("10.7")]
    out = classify_column("price", n_rows=10000, n_distinct=2000, sample=sample)
    assert out == "numeric"


# -- date path -----------------------------------------------------------


def test_python_date_objects_are_date():
    sample = [date(2020, 1, 1), date(2020, 6, 15), datetime(2021, 3, 10)]
    out = classify_column("d", n_rows=10000, n_distinct=2000, sample=sample)
    assert out == "date"


def test_iso_date_strings_are_date():
    sample = ["2020-01-01", "2020-06-15", "2021-03-10", "2022-12-31"] * 50
    out = classify_column("d", n_rows=10000, n_distinct=2000, sample=sample)
    assert out == "date"


def test_yyyymmdd_strings_are_date():
    sample = ["20200101", "20200615", "20210310", "20221231"] * 50
    out = classify_column("d", n_rows=10000, n_distinct=2000, sample=sample)
    assert out == "date"


def test_detect_date_format_returns_first_matching():
    fmt = detect_date_format(["2020-01-01", "2020-06-15"] * 100)
    assert fmt == "%Y-%m-%d"


def test_detect_date_format_returns_none_for_random_strings():
    assert detect_date_format(["foo", "bar", "baz"] * 100) is None


# -- string path ---------------------------------------------------------


def test_high_cardinality_string():
    # Need n_distinct above the categorical threshold but below the
    # string-ID threshold (which is STRING_ID_RATIO=0.5 * n_rows = 5000
    # for n_rows=10000).
    sample = [f"unique_{i}" for i in range(80)]
    out = classify_column("notes", n_rows=10000, n_distinct=300, sample=sample)
    assert out == "high_cardinality"


def test_string_high_distinct_is_id():
    # Many unique strings -> id (string ID detection)
    sample = [f"ID{i:06d}" for i in range(50)]
    out = classify_column(
        "external_id",
        n_rows=10000,
        n_distinct=9000,  # > STRING_ID_RATIO * n_rows AND > STRING_ID_MIN
        sample=sample,
    )
    assert out == "id"


def test_low_cardinality_string_is_categorical():
    sample = ["A", "B", "C", "B", "A"] * 20
    out = classify_column("grp", n_rows=10000, n_distinct=3, sample=sample)
    assert out == "categorical"


def test_logical_bool_is_categorical():
    sample = [True, False, True, True, False]
    out = classify_column("active", n_rows=10000, n_distinct=2, sample=sample)
    assert out == "categorical"


# -- empty / null edge cases ---------------------------------------------


def test_empty_sample_classifies_as_high_cardinality_when_distinct_high():
    # If the sample is all None and n_distinct is high, fall through to
    # high_cardinality (no type info available; safe default).
    out = classify_column(
        "mystery",
        n_rows=10000,
        n_distinct=8000,
        sample=[None, None, None],
    )
    assert out == "high_cardinality"


def test_empty_sample_classifies_as_categorical_when_distinct_low():
    # Even without sample type info, low n_distinct should still classify
    # as categorical.
    out = classify_column("mystery", n_rows=10000, n_distinct=3, sample=[])
    assert out == "categorical"


# -- threshold edge cases ------------------------------------------------


def test_freq_cap_boundary():
    # n_rows * FREQ_RATIO = 1000, but FREQ_CAP = 50, so threshold = 50
    sample = list(range(50))
    out = classify_column("col", n_rows=100_000, n_distinct=FREQ_CAP, sample=sample)
    assert out == "categorical"

    # Just over threshold -> not categorical
    out2 = classify_column(
        "col", n_rows=100_000, n_distinct=FREQ_CAP + 1, sample=sample
    )
    assert out2 == "numeric"


# -- date formats sanity --------------------------------------------------


def test_all_date_formats_listed_actually_parse():
    # Each format should match at least one canonical example (no broken
    # patterns in the constant).
    examples = {
        "%Y-%m-%d": "2020-01-15",
        "%Y/%m/%d": "2020/01/15",
        "%d/%m/%Y": "15/01/2020",
        "%d-%m-%Y": "15-01-2020",
        "%Y%m%d": "20200115",
    }
    for fmt in DATE_FORMATS:
        assert fmt in examples
        assert detect_date_format([examples[fmt]] * 100) == fmt
