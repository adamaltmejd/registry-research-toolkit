"""Property-based tests for the pure classifier leaves in ``classify``.

These assert *totality* — the helpers must never raise on arbitrary text and
must keep their results inside the documented closed sets — for any input. The
``_reg_meta_data_type_kind`` / ``_sql_type_kind`` totality tests are the
regression guard for the whitespace-only ``IndexError`` (``"   ".split()`` →
``[]``, then ``[0]``); the example suite in ``test_classify.py`` keeps the
specific-case coverage.
"""

from __future__ import annotations

from hypothesis import given, strategies as st
from reg_monabundle.runtime.classify import (
    COLUMN_TYPES,
    DATE_FORMATS,
    RegMetaSignal,
    _classify,
    _reg_meta_data_type_kind,
    _sql_type_kind,
    detect_date_format,
    is_known_id,
    is_rtb_named_categorical,
)

# Text that explicitly stresses the parsing leaves, plus arbitrary unicode and
# None. The whitespace-only members ("", "   ", "\t\n") are the regression seed
# for the IndexError the strip-guard fixes.
type_tokens = st.one_of(
    st.none(),
    st.sampled_from(["", "   ", "\t\n", "(", "()", "DECIMAL(18,2)", "TIMESTAMP"]),
    st.text(),
)


@given(type_tokens)
def test_reg_meta_data_type_kind_total(data_type: str | None) -> None:
    """Never raises (incl. whitespace-only — the regression); result is closed."""
    assert _reg_meta_data_type_kind(data_type) in {"numeric", "date", None}


def test_reg_meta_data_type_kind_no_keyword_regression() -> None:
    """Whitespace-only and leading-"(" inputs — both raised IndexError pre-fix
    (the strip-guard handles the first, the empty-pre-paren-split guard the
    second; "(" leaves no keyword before the paren)."""
    assert _reg_meta_data_type_kind("   ") is None
    assert _reg_meta_data_type_kind("\t\n") is None
    assert _reg_meta_data_type_kind("(") is None
    assert _reg_meta_data_type_kind("(18,2)") is None


@given(type_tokens)
def test_sql_type_kind_total(sql_type: str | None) -> None:
    """Never raises on arbitrary text/None; result is closed."""
    assert _sql_type_kind(sql_type) in {"numeric", "date", None}


def test_sql_type_kind_no_keyword_regression() -> None:
    """Sibling guard — "(" left ``_sql_type_kind`` indexing an empty split too."""
    assert _sql_type_kind("(") is None
    assert _sql_type_kind("(18,2)") is None
    assert _sql_type_kind("   ") is None


@given(st.text())
def test_is_known_id_total(col_name: str) -> None:
    """Never raises on arbitrary text; returns a bool."""
    assert isinstance(is_known_id(col_name), bool)


@given(st.text(), st.one_of(st.none(), st.text()))
def test_is_rtb_named_categorical_total(col_name: str, register: str | None) -> None:
    """Never raises on arbitrary text; returns a bool."""
    assert isinstance(is_rtb_named_categorical(col_name, register), bool)


@given(st.lists(st.text()))
def test_detect_date_format_total(values: list[str]) -> None:
    """Never raises on arbitrary string lists; result is a DATE_FORMATS member
    or None."""
    assert detect_date_format(values) in (*DATE_FORMATS, None)


# A reg_meta evidence signal, or None (no register consulted). Field types
# mirror ``RegMetaSignal`` — broad text on the str fields stresses the
# data_type_kind branch into ``reg_meta_implied_type``.
signals = st.one_of(
    st.none(),
    st.builds(
        RegMetaSignal,
        data_type_kind=st.one_of(st.none(), st.text()),
        classification_short_name=st.one_of(st.none(), st.text()),
        has_value_codes=st.booleans(),
        n_value_sets=st.integers(min_value=0, max_value=5),
        n_classifications=st.integers(min_value=0, max_value=5),
    ),
)


@given(
    st.text(),
    st.one_of(st.none(), st.text()),
    signals,
    st.one_of(st.none(), st.text()),
)
def test_classify_total(
    col_name: str,
    sql_type: str | None,
    signal: RegMetaSignal | None,
    register: str | None,
) -> None:
    """For any inputs the verdict is always a COLUMN_TYPES member; never raises."""
    assert _classify(col_name, sql_type, signal, register) in COLUMN_TYPES
