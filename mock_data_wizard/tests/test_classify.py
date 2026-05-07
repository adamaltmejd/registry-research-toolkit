"""Tests for the surviving classify surface.

After Wave 3, ``classify_column`` was removed -- extract is now
config-driven and the data-driven dispatch tree is gone. What remains
is the narrow name-pattern surface used by ``configure.py`` plus the
date helpers consumed by ``summarize.py``.
"""

from __future__ import annotations

from mock_data_wizard.classify import (
    DATE_FORMATS,
    RTB_NAMED_CATEGORICAL,
    detect_date_format,
    is_known_id,
    is_rtb_named_categorical,
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


def test_is_known_id_lopnrbyte_excluded():
    """`LopNrByte` carries `lopnr` in its name but is the RTB pid-change
    flag, not an identifier — the exclude on the lopnr pattern keeps it
    out so the register-scoped categorical rule can take effect."""
    assert is_known_id("LopNrByte") is False
    assert is_known_id("lop_nr_byte") is False


def test_is_rtb_named_categorical_membership():
    """Exact-name match (case-insensitive), only when the register
    string contains 'RTB'. Outside RTB or for variants, returns False."""
    # Membership constant is lowercase; the lookup itself lowercases inputs
    assert "ateranv" in RTB_NAMED_CATEGORICAL
    assert "fodelsearman" in RTB_NAMED_CATEGORICAL

    # Positive matches under various forms of the RTB register string
    assert is_rtb_named_categorical("AterAnv", "RTB") is True
    assert is_rtb_named_categorical("FELPERSONNR", "rtb") is True
    assert (
        is_rtb_named_categorical("LopNrByte", "Registret över totalbefolkningen (RTB)")
        is True
    )
    assert is_rtb_named_categorical("FodelseAr", "RTB") is True
    assert is_rtb_named_categorical("FodelseArMan", "RTB") is True

    # Outside RTB: false (caller falls through to sql_type / manual review)
    assert is_rtb_named_categorical("AterAnv", "LISA") is False
    assert is_rtb_named_categorical("FodelseAr", None) is False

    # Variants with separators or suffixes don't match
    assert is_rtb_named_categorical("ater_anv", "RTB") is False
    assert is_rtb_named_categorical("AterAnvalt", "RTB") is False
    assert is_rtb_named_categorical("FodelseArManed", "RTB") is False


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
