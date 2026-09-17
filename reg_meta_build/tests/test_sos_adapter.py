"""SOS inline enumeration parsing and catalog structural guards."""

from __future__ import annotations

import sqlite3

import pytest
from reg_meta_build.db import DDL, seed_providers
from reg_meta_build.id import mint
from reg_meta_build.sources.sos import (
    _classify_value_set_text,
)

# All 3 codes present, each with its own valid_from/to.
# (codes live in value_set_member; assert via the adapter's written value set)


class TestClassifyValueSetText:
    """`_classify_value_set_text` is conservative: it returns (code, label)
    pairs ONLY for clean enumerations and ``None`` for anything ambiguous (a
    wrong reject is a no-op — the variable stays code-less, exactly today)."""

    @pytest.mark.parametrize(
        "text",
        [
            None,
            "",
            "   ",
            "Fritext",  # single descriptor, not an enumeration
            "1=ja",  # single pair -> not an enumeration
        ],
    )
    def test_none_empty_or_single_segment_rejected(self, text: str | None) -> None:
        assert _classify_value_set_text(text) is None

    def test_kod_klartext_semicolon(self) -> None:
        assert _classify_value_set_text("1=ja; 0=nej; 9=uppgift saknas") == [
            ("1", "ja"),
            ("0", "nej"),
            ("9", "uppgift saknas"),
        ]

    def test_kod_klartext_newline_is_dominant_form(self) -> None:
        text = (
            "0 = Korrekt personnummer\n4 = Samordningsnummer\n8 = Ogiltigt personnummer"
        )
        assert _classify_value_set_text(text) == [
            ("0", "Korrekt personnummer"),
            ("4", "Samordningsnummer"),
            ("8", "Ogiltigt personnummer"),
        ]

    def test_alpha_codes_with_labels(self) -> None:
        # Letter codes (BM/LK/...) carry labels; labels may contain spaces.
        assert _classify_value_set_text("1=Man; 2=Kvinna") == [
            ("1", "Man"),
            ("2", "Kvinna"),
        ]

    def test_label_may_contain_comma_and_colon(self) -> None:
        # Only the CODE is charset-constrained; the label is free text.
        assert _classify_value_set_text(
            "1=riksavtal; 2=regionalt, flerregionalt: avtal"
        ) == [("1", "riksavtal"), ("2", "regionalt, flerregionalt: avtal")]

    def test_label_may_contain_equals(self) -> None:
        # Partition on the FIRST `=` only; a label may itself contain `=`.
        assert _classify_value_set_text("1=a=b; 2=c") == [("1", "a=b"), ("2", "c")]

    def test_swedish_char_codes_accepted(self) -> None:
        # Codes may carry Swedish letters (_clean_value_code accepts them).
        assert _classify_value_set_text("Å=alternativ; Ö=övrigt") == [
            ("Å", "alternativ"),
            ("Ö", "övrigt"),
        ]

    def test_bare_codes_semicolon(self) -> None:
        # The LOVA styrtabell case: bare codes, no inline labels.
        assert _classify_value_set_text("1;2;3;4;5;9") == [
            ("1", None),
            ("2", None),
            ("3", None),
            ("4", None),
            ("5", None),
            ("9", None),
        ]

    def test_bare_alpha_codes(self) -> None:
        assert _classify_value_set_text("LEG;SPEC") == [("LEG", None), ("SPEC", None)]

    @pytest.mark.parametrize(
        "text",
        [
            "0-744 = antal timmar",  # numeric range in code
            "1964-1968: ICD-7 i Klassifikation",  # year-prose (single segment anyway)
            "0=giltigt pnr; 4=samordningsnummer; strängen är tom",  # trailing prose (mixed =)
            "1;5,6;7;8",  # comma inside a code
            "1;2;1",  # duplicate code
            "01=till moder  02=till fader",  # multi-space (single segment, embedded =)
            "1= ; 2=nej",  # whitespace-only label -> rejected (empty after strip)
        ],
    )
    def test_messy_cells_rejected(self, text: str) -> None:
        assert _classify_value_set_text(text) is None


def test_check_minted_id_bands_fails_on_unminted_sos() -> None:
    from reg_meta_build.validate import ValidationResult, _check_minted_id_bands

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    # A SOS register (provider_id 2) with a LOW (un-minted) id -> band FAIL.
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 2, ?)",
        (42, "BadSos"),
    )
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_minted_id_bands(conn, result, tables)
    assert not result.passed
    assert any("below the minted band" in f for f in result.failures)


def test_check_minted_id_bands_passes_clean() -> None:
    from reg_meta_build.validate import ValidationResult, _check_minted_id_bands

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 1, ?)",
        (7, "ScbReg"),
    )
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 2, ?)",
        (mint("sos", "thr"), "SosReg"),
    )
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_minted_id_bands(conn, result, tables)
    assert result.passed


def _seed_sos_variable(
    conn: sqlite3.Connection, *, reg_name: str, with_state: bool
) -> int:
    """Insert one SOS register + variant + variable; optionally a state row.
    Returns the variable_id."""
    rid = mint("sos", reg_name)
    vid = mint("sos", reg_name, "X")
    variant_id = mint("sos", reg_name, "_default")
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 2, ?)",
        (rid, reg_name),
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, name) "
        "VALUES (?, ?, ?)",
        (variant_id, rid, "_default"),
    )
    conn.execute(
        "INSERT INTO variable (variable_id, register_id, provider_key, "
        "is_sensitive, is_identifier) VALUES (?, ?, 'X', 0, 0)",
        (vid, rid),
    )
    if with_state:
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, "
            "register_variant_id, valid_from, valid_to, value_set_version_label) "
            "VALUES (?, ?, ?, '2000-01-01', '2010-12-31', '')",
            (mint("sos", "state", str(vid)), vid, variant_id),
        )
    return vid


def test_check_sos_stateless_variables_warns_not_fails() -> None:
    # P2#1 validate guard: a SOS variable with ZERO variable_state rows surfaces
    # as an INFO/warn line, NOT a failure — A4.3b must still ship.
    from reg_meta_build.validate import (
        ValidationResult,
        _check_sos_stateless_variables,
    )

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    _seed_sos_variable(conn, reg_name="lova", with_state=False)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_sos_stateless_variables(conn, result, tables)
    # WARN, not FAIL: the gate must still pass.
    assert result.passed, result.format_report()
    report = result.format_report()
    assert "ZERO variable_state" in report
    assert "lova" in report.lower()


def test_check_sos_stateless_variables_clean_when_all_have_states() -> None:
    from reg_meta_build.validate import (
        ValidationResult,
        _check_sos_stateless_variables,
    )

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    _seed_sos_variable(conn, reg_name="thr", with_state=True)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_sos_stateless_variables(conn, result, tables)
    assert result.passed
    assert "ZERO variable_state" not in result.format_report()


def test_check_sos_stateless_variables_skips_scb_only() -> None:
    # No SOS variables -> the check self-skips (SCB-only build) and never warns.
    from reg_meta_build.validate import (
        ValidationResult,
        _check_sos_stateless_variables,
    )

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_sos_stateless_variables(conn, result, tables)
    assert result.passed
    assert "SCB-only build" in result.format_report()


# Seed covering the SCB fixture's "Kön" vardemangdsversion (so SCB classification
# linkage is NON-empty) plus the SOS ICD-10-SE entry the resolving SOS variable
# below points at. ICD-10-SE is provider="sos"; it is SEEDED on the SCB-only
# build too (classifications are always seeded), but the SCB-subset stays
# byte-identical because the SOS feed tags only SOS states.

# A SOS register whose DIAGNOS variable carries an icd-10 `Länk kodverk`, so the
# resolver tags it ICD-10-SE during a combined build with classifications on.
