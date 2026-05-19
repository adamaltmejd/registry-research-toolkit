"""Tests for the classifier surface in ``mock_data_wizard.classify``.

Covers name-pattern helpers (``is_known_id``, ``is_rtb_named_categorical``),
date helpers (``DATE_FORMATS``, ``detect_date_format``), and the
classifier primitives the editor uses (``_classify``, ``_sql_type_kind``,
``_reg_meta_datatyp_kind``, ``reg_meta_implied_type``, ``_reg_meta_lookup``,
``_validate_discover_payload``).
"""

from __future__ import annotations

import pytest

from mock_data_wizard.classify import (
    DATE_FORMATS,
    RTB_NAMED_CATEGORICAL,
    RegMetaSignal,
    _classify,
    _reg_meta_datatyp_kind,
    _reg_meta_lookup,
    _sql_type_kind,
    _validate_discover_payload,
    detect_date_format,
    is_known_id,
    is_rtb_named_categorical,
    reg_meta_implied_type,
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


# -- _reg_meta_datatyp_kind ------------------------------------------------


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
def test_reg_meta_datatyp_kind(datatyp: str | None, expected: str | None):
    assert _reg_meta_datatyp_kind(datatyp) == expected


# -- reg_meta_implied_type -------------------------------------------------


def test_reg_meta_implied_type_value_codes_or_classification_means_categorical():
    assert (
        reg_meta_implied_type(
            RegMetaSignal(
                datatyp_kind=None, classification_short_name=None, has_value_codes=True
            )
        )
        == "categorical"
    )
    assert (
        reg_meta_implied_type(
            RegMetaSignal(datatyp_kind=None, classification_short_name="SUN2000")
        )
        == "categorical"
    )


def test_reg_meta_implied_type_storage_only_returns_storage():
    assert (
        reg_meta_implied_type(
            RegMetaSignal(datatyp_kind="numeric", classification_short_name=None)
        )
        == "numeric"
    )
    assert (
        reg_meta_implied_type(
            RegMetaSignal(datatyp_kind="date", classification_short_name=None)
        )
        == "date"
    )


def test_reg_meta_implied_type_no_evidence_returns_none():
    assert reg_meta_implied_type(None) is None
    assert (
        reg_meta_implied_type(
            RegMetaSignal(datatyp_kind=None, classification_short_name=None)
        )
        is None
    )


# -- _classify priority chain ---------------------------------------------


_REG_META_CLASSIFIED = RegMetaSignal(
    datatyp_kind=None, classification_short_name="SUN2000"
)
_REG_META_VALUE_CODES = RegMetaSignal(
    datatyp_kind="numeric", classification_short_name=None, has_value_codes=True
)
_REG_META_TEXT_NO_EVIDENCE = RegMetaSignal(
    datatyp_kind=None, classification_short_name=None
)
_REG_META_NUMERIC_SIG = RegMetaSignal(
    datatyp_kind="numeric", classification_short_name=None
)
_REG_META_DATE_SIG = RegMetaSignal(datatyp_kind="date", classification_short_name=None)


@pytest.mark.parametrize(
    "name, sql_type, signal, expected",
    [
        # Layer 1: known id name beats everything
        ("LopNr", "BIGINT", None, "id"),
        ("LopNr", "VARCHAR", _REG_META_CLASSIFIED, "id"),
        # Layer 2: reg_meta evidence wins over CSV sql_type
        ("ALKod", "BIGINT", _REG_META_VALUE_CODES, "categorical"),
        ("Kon", "BIGINT", _REG_META_VALUE_CODES, "categorical"),
        ("RandomName", "INTEGER", _REG_META_CLASSIFIED, "categorical"),
        ("Sun2000Inr", "VARCHAR", _REG_META_CLASSIFIED, "categorical"),
        # RegMeta says numeric → numeric
        ("ForvErs", "BIGINT", _REG_META_NUMERIC_SIG, "numeric"),
        ("BirthMoment", "VARCHAR", _REG_META_DATE_SIG, "date"),
        # A char/varchar column without value codes and without a
        # classification falls through — opaque.
        ("MysteryString", "VARCHAR", _REG_META_TEXT_NO_EVIDENCE, "opaque"),
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
    signal: RegMetaSignal | None,
    expected: str,
):
    assert _classify(name, sql_type, signal) == expected


def test_classify_id_pattern_beats_reg_meta_classification():
    """`is_known_id` runs before the reg_meta branch — even if reg_meta
    flags a column as classified, an `lopnr` name should stay `id`."""
    assert _classify("lopnr", "BIGINT", _REG_META_CLASSIFIED) == "id"


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


# -- _reg_meta_lookup -------------------------------------------------------


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


def _row(
    lower_name, datatyp=None, short_name=None, value_set_id=None, regver_name=None
):
    return {
        "lower_name": lower_name,
        "datatyp": datatyp,
        "short_name": short_name,
        "value_set_id": value_set_id,
        "regver_name": regver_name,
    }


def test_reg_meta_lookup_strips_project_prefix():
    """Both raw and prefix-stripped names go into the IN clause so
    P1105_LopNr resolves to LopNr in reg_meta."""
    conn = _FakeConn([_row("lopnr", datatyp="bigint")])
    result = _reg_meta_lookup(conn, {"P1105_LopNr"}, [34])
    # SQL contains both forms in the IN list
    assert "lopnr" in conn.last_params
    assert "p1105_lopnr" in conn.last_params
    assert result["lopnr"].datatyp_kind == "numeric"


def test_reg_meta_lookup_aggregates_classification_majority():
    """Multiple cvids per alias: the most-common classification short_name wins."""
    conn = _FakeConn(
        [
            _row("kon", short_name="GenderA"),
            _row("kon", short_name="GenderA"),
            _row("kon", short_name="GenderB"),
        ]
    )
    result = _reg_meta_lookup(conn, {"Kon"}, [34])
    assert result["kon"].classification_short_name == "GenderA"


def test_reg_meta_lookup_has_value_codes_any_wins():
    conn = _FakeConn(
        [
            _row("alkod", value_set_id=None),
            _row("alkod", value_set_id=7),
        ]
    )
    result = _reg_meta_lookup(conn, {"ALKod"}, [34])
    assert result["alkod"].has_value_codes is True
    assert result["alkod"].n_value_sets == 1


def test_reg_meta_lookup_first_non_null_datatyp_wins():
    conn = _FakeConn(
        [
            _row("foo", datatyp=None),
            _row("foo", datatyp="bigint"),
            _row("foo", datatyp="varchar"),
        ]
    )
    result = _reg_meta_lookup(conn, {"foo"}, [34])
    assert result["foo"].datatyp_kind == "numeric"


def test_reg_meta_lookup_empty_inputs_short_circuit():
    conn = _FakeConn([])
    assert _reg_meta_lookup(conn, set(), [34]) == {}
    assert _reg_meta_lookup(conn, {"x"}, []) == {}
    assert conn.last_sql is None


def test_reg_meta_lookup_relevant_years_scopes_variance_counts():
    """``relevant_years`` filters n_value_sets / n_classifications to
    in-scope instances; yearless rows still count; classification_short_name
    and has_value_codes stay unfiltered (whole-history facts)."""
    conn = _FakeConn(
        [
            _row(
                "lkf",
                short_name="LKF2012",
                value_set_id=10,
                regver_name="lisa.lisa_2018",
            ),
            _row(
                "lkf",
                short_name="LKF2012",
                value_set_id=10,
                regver_name="lisa.lisa_2019",
            ),
            _row(
                "lkf",
                short_name="LKF2025",
                value_set_id=20,
                regver_name="lisa.lisa_2024",
            ),
            _row(
                "lkf",
                short_name="LKF1990",
                value_set_id=30,
                regver_name="lisa.lisa_1990",
            ),
            _row(
                "lkf",
                short_name="LKFX",
                value_set_id=40,
                regver_name=None,
            ),
        ]
    )
    sig = _reg_meta_lookup(conn, {"lkf"}, [34], relevant_years={2018, 2019})["lkf"]
    # In-scope years: 2018, 2019, plus yearless → LKF2012 (×2), LKFX (×1).
    assert sig.n_classifications == 2
    assert sig.n_value_sets == 2
    # Whole-history facts unaffected by the year filter.
    assert sig.has_value_codes is True
    # Majority winner across ALL years (LKF2012 has 2 occurrences).
    assert sig.classification_short_name == "LKF2012"


def test_reg_meta_lookup_relevant_years_none_keeps_full_counts():
    """``relevant_years=None`` is the default and preserves pre-filter
    counts — the snapshot/popup year-scope must opt in explicitly."""
    conn = _FakeConn(
        [
            _row(
                "lkf",
                short_name="LKF2012",
                value_set_id=10,
                regver_name="lisa.lisa_2018",
            ),
            _row(
                "lkf",
                short_name="LKF2025",
                value_set_id=20,
                regver_name="lisa.lisa_2024",
            ),
        ]
    )
    sig = _reg_meta_lookup(conn, {"lkf"}, [34])["lkf"]
    assert sig.n_classifications == 2
    assert sig.n_value_sets == 2


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
