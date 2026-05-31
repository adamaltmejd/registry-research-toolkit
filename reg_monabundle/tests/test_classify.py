"""Tests for the classifier surface in ``reg_monabundle.runtime.classify``.

Covers name-pattern helpers (``is_known_id``, ``is_rtb_named_categorical``),
date helpers (``DATE_FORMATS``, ``detect_date_format``), and the
classifier primitives the editor uses (``_classify``, ``_sql_type_kind``,
``_reg_meta_data_type_kind``, ``reg_meta_implied_type``, ``_reg_meta_lookup``,
``_validate_discover_payload``).
"""

from __future__ import annotations

import sqlite3

import pytest
from reg_monabundle.runtime.classify import (
    DATE_FORMATS,
    RTB_NAMED_CATEGORICAL,
    RegMetaSignal,
    _classify,
    _reg_meta_data_type_kind,
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


# -- _reg_meta_data_type_kind ------------------------------------------------


@pytest.mark.parametrize(
    "data_type, expected",
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
def test_reg_meta_data_type_kind(data_type: str | None, expected: str | None):
    assert _reg_meta_data_type_kind(data_type) == expected


# -- reg_meta_implied_type -------------------------------------------------


def test_reg_meta_implied_type_value_codes_or_classification_means_categorical():
    assert (
        reg_meta_implied_type(
            RegMetaSignal(
                data_type_kind=None,
                classification_short_name=None,
                has_value_codes=True,
            )
        )
        == "categorical"
    )
    assert (
        reg_meta_implied_type(
            RegMetaSignal(data_type_kind=None, classification_short_name="SUN2000")
        )
        == "categorical"
    )


def test_reg_meta_implied_type_storage_only_returns_storage():
    assert (
        reg_meta_implied_type(
            RegMetaSignal(data_type_kind="numeric", classification_short_name=None)
        )
        == "numeric"
    )
    assert (
        reg_meta_implied_type(
            RegMetaSignal(data_type_kind="date", classification_short_name=None)
        )
        == "date"
    )


def test_reg_meta_implied_type_no_evidence_returns_none():
    assert reg_meta_implied_type(None) is None
    assert (
        reg_meta_implied_type(
            RegMetaSignal(data_type_kind=None, classification_short_name=None)
        )
        is None
    )


# -- _classify priority chain ---------------------------------------------


_REG_META_CLASSIFIED = RegMetaSignal(
    data_type_kind=None, classification_short_name="SUN2000"
)
_REG_META_VALUE_CODES = RegMetaSignal(
    data_type_kind="numeric", classification_short_name=None, has_value_codes=True
)
_REG_META_TEXT_NO_EVIDENCE = RegMetaSignal(
    data_type_kind=None, classification_short_name=None
)
_REG_META_NUMERIC_SIG = RegMetaSignal(
    data_type_kind="numeric", classification_short_name=None
)
_REG_META_DATE_SIG = RegMetaSignal(
    data_type_kind="date", classification_short_name=None
)


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
    lower_name,
    data_type=None,
    short_name=None,
    value_set_id=None,
    year=None,
    to_year=None,
):
    # A2.7: `_reg_meta_lookup` reads the state's validity INTERVAL
    # (`valid_from`..`valid_to`). A single `year` maps to a point window
    # (Jan-1..Dec-31); `to_year` widens it to a multi-year span. None →
    # the `0001`..`9999` yearless-fallback open window.
    if year is None:
        valid_from, valid_to = "0001-01-01", "9999-12-31"
    else:
        valid_from = f"{year}-01-01"
        valid_to = f"{to_year if to_year is not None else year}-12-31"
    return {
        "lower_name": lower_name,
        "data_type": data_type,
        "short_name": short_name,
        "value_set_id": value_set_id,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }


def test_reg_meta_lookup_strips_project_prefix():
    """Both raw and prefix-stripped names go into the IN clause so
    P1105_LopNr resolves to LopNr in reg_meta."""
    conn = _FakeConn([_row("lopnr", data_type="bigint")])
    result = _reg_meta_lookup(conn, {"P1105_LopNr"}, [34])
    # SQL contains both forms in the IN list
    assert "lopnr" in conn.last_params
    assert "p1105_lopnr" in conn.last_params
    assert result["lopnr"].data_type_kind == "numeric"


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


def test_reg_meta_lookup_first_non_null_data_type_wins():
    conn = _FakeConn(
        [
            _row("foo", data_type=None),
            _row("foo", data_type="bigint"),
            _row("foo", data_type="varchar"),
        ]
    )
    result = _reg_meta_lookup(conn, {"foo"}, [34])
    assert result["foo"].data_type_kind == "numeric"


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
            _row("lkf", short_name="LKF2012", value_set_id=10, year=2018),
            _row("lkf", short_name="LKF2012", value_set_id=10, year=2019),
            _row("lkf", short_name="LKF2025", value_set_id=20, year=2024),
            _row("lkf", short_name="LKF1990", value_set_id=30, year=1990),
            _row("lkf", short_name="LKFX", value_set_id=40, year=None),
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


def test_reg_meta_lookup_relevant_years_counts_covering_multiyear_state():
    """A2.7 (Codex P2 #149): a multi-year state whose window COVERS a selected
    year is in scope even when its OPENING year isn't selected — interval-overlap,
    not start-year membership (mirrors `get values`). Pre-fix the 2020..2021 era
    opened in 2020 ∉ {2021} and was wrongly dropped, undercounting the variance."""
    conn = _FakeConn(
        [
            # Opens 2020 but spans through 2021 → covers the {2021} filter.
            _row("lkf", short_name="LKF2012", value_set_id=10, year=2020, to_year=2021),
            # A disjoint earlier era that does NOT cover 2021.
            _row("lkf", short_name="LKF2008", value_set_id=20, year=2008, to_year=2009),
        ]
    )
    sig = _reg_meta_lookup(conn, {"lkf"}, [34], relevant_years={2021})["lkf"]
    # Only the covering 2020..2021 state counts; the 2008..2009 era is excluded.
    assert sig.n_value_sets == 1
    assert sig.n_classifications == 1


def test_reg_meta_lookup_relevant_years_none_keeps_full_counts():
    """``relevant_years=None`` is the default and preserves pre-filter
    counts — the snapshot/popup year-scope must opt in explicitly."""
    conn = _FakeConn(
        [
            _row("lkf", short_name="LKF2012", value_set_id=10, year=2018),
            _row("lkf", short_name="LKF2025", value_set_id=20, year=2024),
        ]
    )
    sig = _reg_meta_lookup(conn, {"lkf"}, [34])["lkf"]
    assert sig.n_classifications == 2
    assert sig.n_value_sets == 2


def _real_reg_meta_db() -> sqlite3.Connection:
    """A minimal SHIPPED-shape reg_meta DB for the tables `_reg_meta_lookup`
    joins. Uses a real `sqlite3` connection (the MONA runtime's stdlib backend)
    so the SQL JOIN — including the `register_variant_id` scoping — actually
    runs; the `_FakeConn` fixture returns canned rows and can't exercise it."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE TABLE variable (variable_id INTEGER PRIMARY KEY, register_id INTEGER);"
        "CREATE TABLE variable_alias ("
        "  variable_id INTEGER, register_variant_id INTEGER, delivery_column_name TEXT);"
        "CREATE TABLE variable_state ("
        "  variable_id INTEGER, register_variant_id INTEGER, "
        "  delivery_column_name TEXT, data_type TEXT, "
        "  value_set_id INTEGER, valid_from TEXT, valid_to TEXT, "
        "  classification_id INTEGER);"
        "CREATE TABLE classification (id INTEGER PRIMARY KEY, short_name TEXT);"
    )
    return conn


def test_reg_meta_lookup_multi_variant_vote_not_inflated():
    """PR #149: the `variable_alias`→`variable_state` join must be scoped by
    `register_variant_id`, not `variable_id` alone — else the classification
    badge for a column counts states from OTHER variants/columns of the same
    variable, flipping `most_common(1)`.

    Geometry: variable 1 delivers column `kon` under variant 10 ONLY (1 era,
    GenderA), and a DIFFERENT column `pnr` under variant 11 (2 eras, both
    GenderB). Querying `kon`:

    - Pre-fix (`variable_id`-only join): the `kon` alias (variant 10) cross-joins
      ALL 3 states of the variable — the GenderA@10 era AND both GenderB@11 eras
      — so the badge is GenderB (2 > 1). WRONG: those GenderB eras belong to a
      different column.
    - Scoped (`+ register_variant_id`): the `kon`@10 alias matches only the
      GenderA@10 era → badge GenderA, and `n_classifications` is 1 (the column's
      own family), not 2."""
    conn = _real_reg_meta_db()
    conn.execute("INSERT INTO variable VALUES (1, 34)")
    conn.execute("INSERT INTO classification VALUES (100, 'GenderA')")
    conn.execute("INSERT INTO classification VALUES (200, 'GenderB')")
    # `kon` is delivered under variant 10 only; `pnr` under variant 11.
    conn.execute("INSERT INTO variable_alias VALUES (1, 10, 'Kon')")
    conn.execute("INSERT INTO variable_alias VALUES (1, 11, 'PNr')")
    # Variant 10 era → GenderA (the `kon` column's real family).
    conn.execute(
        "INSERT INTO variable_state VALUES "
        "(1, 10, 'Kon', 'int', NULL, '2020-01-01', '2020-12-31', 100)"
    )
    # Variant 11 eras → GenderB ×2 (the `pnr` column — must NOT vote for `kon`).
    conn.execute(
        "INSERT INTO variable_state VALUES "
        "(1, 11, 'PNr', 'int', NULL, '2020-01-01', '2020-12-31', 200)"
    )
    conn.execute(
        "INSERT INTO variable_state VALUES "
        "(1, 11, 'PNr', 'int', NULL, '2021-01-01', '2021-12-31', 200)"
    )
    conn.commit()

    sig = _reg_meta_lookup(conn, {"Kon"}, [34])["kon"]
    assert sig.classification_short_name == "GenderA"
    # Only the `kon`@10 family — the GenderB@11 eras are scoped out.
    assert sig.n_classifications == 1


def test_reg_meta_lookup_column_pairing_within_variant():
    """A2.7 (Codex P2 #149): within ONE variant, `variable_state` is per-era and
    carries its era's `delivery_column_name`. The state join pairs each alias to
    states delivered under THAT column, so an old column does not count a later
    era's value_set/classification delivered under a DIFFERENT column.

    Geometry: variable 1, variant 10, two eras — column `inkomst` (IncomeA, vs 10)
    then renamed to `ink` (IncomeB, vs 20). Querying `inkomst`:

    - Pre-fix (variant-only join): `inkomst` cross-joins BOTH eras → n_value_sets
      2 and an IncomeA/IncomeB tie. WRONG: vs 20 was delivered as `ink`.
    - Column-paired: `inkomst` matches only the IncomeA/vs-10 era."""
    conn = _real_reg_meta_db()
    conn.execute("INSERT INTO variable VALUES (1, 34)")
    conn.execute("INSERT INTO classification VALUES (100, 'IncomeA')")
    conn.execute("INSERT INTO classification VALUES (200, 'IncomeB')")
    conn.execute("INSERT INTO variable_alias VALUES (1, 10, 'inkomst')")
    conn.execute("INSERT INTO variable_alias VALUES (1, 10, 'ink')")
    # Same variable+variant, two eras, DIFFERENT delivery columns + families.
    conn.execute(
        "INSERT INTO variable_state VALUES "
        "(1, 10, 'inkomst', 'int', 10, '2018-01-01', '2018-12-31', 100)"
    )
    conn.execute(
        "INSERT INTO variable_state VALUES "
        "(1, 10, 'ink', 'int', 20, '2019-01-01', '2019-12-31', 200)"
    )
    conn.commit()

    sig = _reg_meta_lookup(conn, {"inkomst"}, [34])["inkomst"]
    # Only the `inkomst`-era evidence; the later `ink` era is paired out.
    assert sig.classification_short_name == "IncomeA"
    assert sig.n_value_sets == 1
    assert sig.n_classifications == 1


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
