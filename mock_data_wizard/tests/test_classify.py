"""Tests for the surviving classify surface.

After Wave 3, ``classify_column`` was removed -- extract is now
config-driven and the data-driven dispatch tree is gone. What remains
is the name-pattern surface used by ``configure.py`` plus the date
helpers consumed by ``summarize.py``.
"""

from __future__ import annotations

from mock_data_wizard.classify import (
    DATE_FORMATS,
    detect_date_format,
    is_known_id,
    known_categorical_cap,
)


# -- name-based patterns --------------------------------------------------


def test_is_known_id_lopnr_match():
    assert is_known_id("LopNr") is True
    assert is_known_id("p1105_lopnr_personnr") is True


def test_is_known_id_persnr_match():
    """`PersNr` and `PersonNr` classify as id; the segment-anchored regex
    keeps `FelPersonNr` (a non-id flag column — see scan.py) out."""
    assert is_known_id("PersNr") is True
    assert is_known_id("PersonNr") is True
    assert is_known_id("LopNr_PersNr") is True
    assert is_known_id("P1105_PersonNr") is True
    # Negative: FelPersonNr must remain non-id
    assert is_known_id("FelPersonNr") is False
    assert is_known_id("p1105_felpersonnr") is False


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


def test_known_categorical_cap_demographic_categories():
    """Demographic SCB columns where SCB doesn't always wire up a
    classification_id in regmeta. These are the patterns that used to
    live in ``configure.EXTRA_CATEGORICAL`` and were folded into
    ``CATEGORICAL_PATTERNS`` so configure has a single source of truth."""
    assert known_categorical_cap("Kon") == 3  # sex
    assert known_categorical_cap("p_kon") == 3  # underscore-prefixed form
    assert known_categorical_cap("CivilStand") == 10
    assert known_categorical_cap("Lan") == 30  # län (county)
    assert known_categorical_cap("FodelseLand") == 300
    assert known_categorical_cap("Yrke_KOD") == 10000  # generic _kod suffix
    # negative: "kon" must not match in arbitrary substrings
    assert known_categorical_cap("Konsument") is None
    # negative: "lan" anchoring must hold (otherwise "Plan" would match)
    assert known_categorical_cap("Plan") is None


# -- date format detection ------------------------------------------------


def test_detect_date_format_returns_first_matching():
    fmt = detect_date_format(["2020-01-01", "2020-06-15"] * 100)
    assert fmt == "%Y-%m-%d"


def test_detect_date_format_returns_none_for_random_strings():
    assert detect_date_format(["foo", "bar", "baz"] * 100) is None


def test_detect_date_format_returns_none_for_empty_input():
    assert detect_date_format([]) is None


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
