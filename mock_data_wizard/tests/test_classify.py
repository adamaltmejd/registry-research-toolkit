"""Tests for the classifier surface in ``mock_data_wizard.classify``.

Covers name-pattern helpers (``is_known_id``, ``is_rtb_named_categorical``),
date helpers (``DATE_FORMATS``, ``detect_date_format``), and the
classifier primitives the editor uses (``_classify``, ``_sql_type_kind``,
``_regmeta_datatyp_kind``, ``regmeta_implied_type``, ``_regmeta_lookup``,
``_validate_discover_payload``).
"""

from __future__ import annotations

import pytest

from mock_data_wizard.classify import (
    DATE_FORMATS,
    RTB_NAMED_CATEGORICAL,
    RegmetaSignal,
    _classify,
    _regmeta_datatyp_kind,
    _regmeta_lookup,
    _sql_type_kind,
    _validate_discover_payload,
    detect_date_format,
    is_known_id,
    is_rtb_named_categorical,
    regmeta_implied_type,
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
    assert "ateranv" in RTB_NAMED_CATEGORICAL
    assert "fodelsearman" in RTB_NAMED_CATEGORICAL

    assert is_rtb_named_categorical("AterAnv", "RTB") is True
    assert is_rtb_named_categorical("FELPERSONNR", "rtb") is True
    assert (
        is_rtb_named_categorical("LopNrByte", "Registret över totalbefolkningen (RTB)")
        is True
    )
    assert is_rtb_named_categorical("FodelseAr", "RTB") is True
    assert is_rtb_named_categorical("FodelseArMan", "RTB") is True

    assert is_rtb_named_categorical("AterAnv", "LISA") is False
    assert is_rtb_named_categorical("FodelseAr", None) is False

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


# -- _sql_type_kind --------------------------------------------------------


@pytest.mark.parametrize(
    "sql_type, expected",
    [
        ("BIGINT", "numeric"),
        ("bigint", "numeric"),
        ("Integer", "numeric"),
        ("DECIMAL(18,2)", "numeric"),
        ("numeric(10,4)", "numeric"),
        ("DOUBLE", "numeric"),
        ("FLOAT", "numeric"),
        ("MONEY", "numeric"),
        ("DATE", "date"),
        ("TIMESTAMP", "date"),
        ("TIMESTAMP WITH TIME ZONE", "date"),
        ("datetime2", "date"),
        ("VARCHAR", None),
        ("char(4)", None),
        ("nvarchar(255)", None),
        ("text", None),
        ("", None),
        ("   ", None),  # whitespace-only must not raise IndexError
        ("\t\n", None),
        (None, None),
    ],
)
def test_sql_type_kind(sql_type: str | None, expected: str | None):
    assert _sql_type_kind(sql_type) == expected


# -- _regmeta_datatyp_kind ------------------------------------------------


@pytest.mark.parametrize(
    "datatyp, expected",
    [
        ("bigint", "numeric"),
        ("INT", "numeric"),
        ("decimal(18,2)", "numeric"),
        ("DATE", "date"),
        ("datetime2", "date"),
        ("char(4)", None),
        ("varchar", None),
        ("text", None),
        ("", None),
        (None, None),
    ],
)
def test_regmeta_datatyp_kind(datatyp: str | None, expected: str | None):
    assert _regmeta_datatyp_kind(datatyp) == expected


# -- regmeta_implied_type -------------------------------------------------


def test_regmeta_implied_type_value_codes_or_classification_means_categorical():
    assert (
        regmeta_implied_type(
            RegmetaSignal(
                datatyp_kind=None, classification_short_name=None, has_value_codes=True
            )
        )
        == "categorical"
    )
    assert (
        regmeta_implied_type(
            RegmetaSignal(datatyp_kind=None, classification_short_name="SUN2000")
        )
        == "categorical"
    )


def test_regmeta_implied_type_storage_only_returns_storage():
    assert (
        regmeta_implied_type(
            RegmetaSignal(datatyp_kind="numeric", classification_short_name=None)
        )
        == "numeric"
    )
    assert (
        regmeta_implied_type(
            RegmetaSignal(datatyp_kind="date", classification_short_name=None)
        )
        == "date"
    )


def test_regmeta_implied_type_no_evidence_returns_none():
    assert regmeta_implied_type(None) is None
    assert (
        regmeta_implied_type(
            RegmetaSignal(datatyp_kind=None, classification_short_name=None)
        )
        is None
    )


# -- _classify priority chain ---------------------------------------------


_REGMETA_CLASSIFIED = RegmetaSignal(
    datatyp_kind=None, classification_short_name="SUN2000"
)
_REGMETA_VALUE_CODES = RegmetaSignal(
    datatyp_kind="numeric", classification_short_name=None, has_value_codes=True
)
_REGMETA_TEXT_NO_EVIDENCE = RegmetaSignal(
    datatyp_kind=None, classification_short_name=None
)
_REGMETA_NUMERIC_SIG = RegmetaSignal(
    datatyp_kind="numeric", classification_short_name=None
)
_REGMETA_DATE_SIG = RegmetaSignal(datatyp_kind="date", classification_short_name=None)


@pytest.mark.parametrize(
    "name, sql_type, signal, expected",
    [
        # Layer 1: known id name beats everything
        ("LopNr", "BIGINT", None, "id"),
        ("LopNr", "VARCHAR", _REGMETA_CLASSIFIED, "id"),
        # Layer 2: regmeta evidence wins over CSV sql_type
        ("ALKod", "BIGINT", _REGMETA_VALUE_CODES, "categorical"),
        ("Kon", "BIGINT", _REGMETA_VALUE_CODES, "categorical"),
        ("RandomName", "INTEGER", _REGMETA_CLASSIFIED, "categorical"),
        ("Sun2000Inr", "VARCHAR", _REGMETA_CLASSIFIED, "categorical"),
        # Regmeta says numeric → numeric
        ("ForvErs", "BIGINT", _REGMETA_NUMERIC_SIG, "numeric"),
        ("BirthMoment", "VARCHAR", _REGMETA_DATE_SIG, "date"),
        # A char/varchar column without value codes and without a
        # classification falls through — opaque.
        ("MysteryString", "VARCHAR", _REGMETA_TEXT_NO_EVIDENCE, "opaque"),
        # Layer 4: sql_type drives numeric / date for unrecognised names
        ("SammanInk", "BIGINT", None, "numeric"),
        ("SomeAmount", "DECIMAL(18,2)", None, "numeric"),
        ("Whatever", "DOUBLE", None, "numeric"),
        ("InDatum", "DATE", None, "date"),
        ("Tidpunkt", "TIMESTAMP", None, "date"),
        # Layer 5: fallthrough
        ("RandomString", "VARCHAR", None, "opaque"),
        ("Mystery", None, None, "opaque"),
    ],
)
def test_classify_priority_chain(
    name: str,
    sql_type: str | None,
    signal: RegmetaSignal | None,
    expected: str,
):
    assert _classify(name, sql_type, signal) == expected


def test_classify_id_pattern_beats_regmeta_classification():
    """`is_known_id` runs before the regmeta branch — even if regmeta
    flags a column as classified, an `lopnr` name should stay `id`."""
    assert _classify("lopnr", "BIGINT", _REGMETA_CLASSIFIED) == "id"


@pytest.mark.parametrize(
    "name, register, expected",
    [
        ("AterAnv", "RTB", "categorical"),
        ("ateranv", "rtb", "categorical"),
        ("FELPERSONNR", "Registret över totalbefolkningen (RTB)", "categorical"),
        ("LopNrByte", "RTB", "categorical"),
        ("FodelseAr", "RTB", "categorical"),
        ("FodelseArMan", "RTB", "categorical"),
        ("AterAnv", "LISA", "numeric"),
        ("LopNrByte", "LISA", "numeric"),
        ("FelPersonNr", None, "numeric"),
        ("FodelseAr", "LISA", "numeric"),
        ("ater_anv", "RTB", "numeric"),
        ("AterAnvalt", "RTB", "numeric"),
        ("FodelseArManed", "RTB", "numeric"),
    ],
)
def test_classify_rtb_named_categorical(name: str, register: str | None, expected: str):
    assert _classify(name, "BIGINT", None, register) == expected


def test_classify_lopnr_id_excludes_lopnrbyte():
    assert _classify("LopNrByte", "BIGINT", None, "RTB") == "categorical"
    assert _classify("LopNrByte", "BIGINT", None, None) == "numeric"


# -- _regmeta_lookup -------------------------------------------------------


class _FakeRows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.last_sql: str | None = None
        self.last_params: list | None = None

    def execute(self, sql, params):
        self.last_sql = sql
        self.last_params = list(params)
        return _FakeRows(self._rows)


def _row(lower_name, datatyp=None, short_name=None, has_value_codes=0):
    return {
        "lower_name": lower_name,
        "datatyp": datatyp,
        "short_name": short_name,
        "has_value_codes": has_value_codes,
    }


def test_regmeta_lookup_strips_project_prefix():
    """Both raw and prefix-stripped names go into the IN clause so
    P1105_LopNr resolves to LopNr in regmeta."""
    conn = _FakeConn([_row("lopnr", datatyp="bigint")])
    result = _regmeta_lookup(conn, {"P1105_LopNr"}, [34])
    # SQL contains both forms in the IN list
    assert "lopnr" in conn.last_params
    assert "p1105_lopnr" in conn.last_params
    assert result["lopnr"].datatyp_kind == "numeric"


def test_regmeta_lookup_aggregates_classification_majority():
    """Multiple cvids per alias: the most-common classification short_name wins."""
    conn = _FakeConn(
        [
            _row("kon", short_name="GenderA"),
            _row("kon", short_name="GenderA"),
            _row("kon", short_name="GenderB"),
        ]
    )
    result = _regmeta_lookup(conn, {"Kon"}, [34])
    assert result["kon"].classification_short_name == "GenderA"


def test_regmeta_lookup_has_value_codes_any_wins():
    conn = _FakeConn(
        [
            _row("alkod", has_value_codes=0),
            _row("alkod", has_value_codes=1),
        ]
    )
    result = _regmeta_lookup(conn, {"ALKod"}, [34])
    assert result["alkod"].has_value_codes is True


def test_regmeta_lookup_first_non_null_datatyp_wins():
    conn = _FakeConn(
        [
            _row("foo", datatyp=None),
            _row("foo", datatyp="bigint"),
            _row("foo", datatyp="varchar"),
        ]
    )
    result = _regmeta_lookup(conn, {"foo"}, [34])
    assert result["foo"].datatyp_kind == "numeric"


def test_regmeta_lookup_empty_inputs_short_circuit():
    conn = _FakeConn([])
    assert _regmeta_lookup(conn, set(), [34]) == {}
    assert _regmeta_lookup(conn, {"x"}, []) == {}
    assert conn.last_sql is None


# -- _validate_discover_payload -------------------------------------------


def test_validate_discover_payload_accepts_minimal_valid():
    payload = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "LopNr", "sql_type": "BIGINT"}],
            }
        ],
    }
    _validate_discover_payload(payload, "test")


def test_validate_discover_payload_rejects_stats_file_shape():
    """Stats has top-level shape that looks similar; the contract_version
    is the discriminator."""
    payload = {"contract_version": "stats-1.0.0", "sources": []}
    with pytest.raises(ValueError, match="mock_data_discovery.json"):
        _validate_discover_payload(payload, "test")


def test_validate_discover_payload_rejects_duplicate_source_name():
    payload = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {"source_name": "x", "columns": []},
            {"source_name": "x", "columns": []},
        ],
    }
    with pytest.raises(ValueError, match="duplicate source_name"):
        _validate_discover_payload(payload, "test")


def test_validate_discover_payload_rejects_missing_columns_key():
    payload = {
        "contract_version": "discover-1.0.0",
        "sources": [{"source_name": "x"}],
    }
    with pytest.raises(ValueError, match="missing 'columns'"):
        _validate_discover_payload(payload, "test")


def test_validate_discover_payload_rejects_column_without_name():
    payload = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {"source_name": "x", "columns": [{"sql_type": "BIGINT"}]},
        ],
    }
    with pytest.raises(ValueError, match="must be an object with a 'name' key"):
        _validate_discover_payload(payload, "test")
