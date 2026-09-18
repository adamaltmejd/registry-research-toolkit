"""SOS inline enumeration parsing and catalog structural guards."""

from __future__ import annotations

import hashlib
import sqlite3

import pytest
from _sos_fixtures import (
    BU_SPEC_LINED,
    BU_SPEC_MEMBERS,
    BU_SPEC_ONE_LINE,
    BU_SPEC_WRAPPED,
)
from reg_meta_build.db import DDL, seed_providers
from reg_meta_build.id import mint
from reg_meta_build.normalization import normalize_text
from reg_meta_build.sources.sos import (
    _classify_value_set_text,
)

# All 3 codes present, each with its own valid_from/to.
# (codes live in value_set_member; assert via the adapter's written value set)


def test_bu_spec_fixture_cells_match_the_retained_source_evidence() -> None:
    # These digests come from the retained BU source evidence, NOT from the fixture
    # constants: the real workbook is gitignored, so this is what keeps the cells
    # below byte-exact. A failure means a fixture drifted from the original cell —
    # restore the cell rather than recomputing the digest.
    assert [
        hashlib.sha256(cell.encode()).hexdigest()
        for cell in (BU_SPEC_LINED, BU_SPEC_WRAPPED, BU_SPEC_ONE_LINE)
    ] == [
        "809e5e939201350001edb0483b236a5c896d7dec6cab6d0f75c48d9e314b91bf",
        "1b9ab9b4b525bb19b01d62fc47d83b04d177242d7d5b2ec8d8626cef652b0712",
        "0a62d6be738d2763e128c5aac35adba216a489069a2ed7ff0d1ec54e2b1d3b07",
    ]


class TestClassifyValueSetText:
    """`_classify_value_set_text` is conservative: it returns `(pairs, False)`
    ONLY for clean enumerations, `(None, True)` for a delivered list whose
    members the format does not separate, and `(None, False)` for ordinary free
    text (a wrong reject is a no-op — the variable stays code-less, as today)."""

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
        assert _classify_value_set_text(text) == (None, False)

    def test_kod_klartext_semicolon(self) -> None:
        assert _classify_value_set_text("1=ja; 0=nej; 9=uppgift saknas") == (
            [("1", "ja"), ("0", "nej"), ("9", "uppgift saknas")],
            False,
        )

    def test_kod_klartext_newline_is_dominant_form(self) -> None:
        text = (
            "0 = Korrekt personnummer\n4 = Samordningsnummer\n8 = Ogiltigt personnummer"
        )
        assert _classify_value_set_text(text) == (
            [
                ("0", "Korrekt personnummer"),
                ("4", "Samordningsnummer"),
                ("8", "Ogiltigt personnummer"),
            ],
            False,
        )

    def test_alpha_codes_with_labels(self) -> None:
        # Letter codes (BM/LK/...) carry labels; labels may contain spaces.
        assert _classify_value_set_text("1=Man; 2=Kvinna") == (
            [("1", "Man"), ("2", "Kvinna")],
            False,
        )

    def test_label_may_contain_comma_and_colon(self) -> None:
        # Only the CODE is charset-constrained; the label is free text.
        assert _classify_value_set_text(
            "1=riksavtal; 2=regionalt, flerregionalt: avtal"
        ) == ([("1", "riksavtal"), ("2", "regionalt, flerregionalt: avtal")], False)

    def test_label_may_contain_equals(self) -> None:
        # Partition on the FIRST `=` only; a label may itself contain `=`. No clean
        # code precedes that `=`, so it cannot be a swallowed further assignment.
        assert _classify_value_set_text("1=a=b; 2=c") == (
            [("1", "a=b"), ("2", "c")],
            False,
        )

    def test_label_prose_equals_after_a_non_code_token_is_kept(self) -> None:
        # ` (n=` is whitespace-separated but `(n` is not a clean code -> prose.
        assert _classify_value_set_text("1=grupp A (n=25); 2=grupp B") == (
            [("1", "grupp A (n=25)"), ("2", "grupp B")],
            False,
        )
        assert _classify_value_set_text("1=intervallet 2-3 = mellan; 2=annat") == (
            [("1", "intervallet 2-3 = mellan"), ("2", "annat")],
            False,
        )

    def test_newline_delimited_spec_cell_keeps_all_three_members(self) -> None:
        # The BU `SPEC` cell that IS newline-delimited throughout parses in full.
        assert _classify_value_set_text(
            normalize_text(BU_SPEC_LINED, multiline=True)
        ) == (BU_SPEC_MEMBERS, False)

    def test_swedish_char_codes_accepted(self) -> None:
        # Codes may carry Swedish letters (_clean_value_code accepts them).
        assert _classify_value_set_text("Å=alternativ; Ö=övrigt") == (
            [("Å", "alternativ"), ("Ö", "övrigt")],
            False,
        )

    def test_bare_codes_semicolon(self) -> None:
        # The LOVA styrtabell case: bare codes, no inline labels.
        assert _classify_value_set_text("1;2;3;4;5;9") == (
            [
                ("1", None),
                ("2", None),
                ("3", None),
                ("4", None),
                ("5", None),
                ("9", None),
            ],
            False,
        )

    def test_bare_alpha_codes(self) -> None:
        assert _classify_value_set_text("LEG;SPEC") == (
            [("LEG", None), ("SPEC", None)],
            False,
        )

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
            # The BU `SPEC` cell with no newline at all: one segment, so this is
            # free text under the delivered format, exactly as before.
            normalize_text(BU_SPEC_ONE_LINE, multiline=True),
        ],
    )
    def test_messy_cells_rejected(self, text: str) -> None:
        assert _classify_value_set_text(text) == (None, False)

    @pytest.mark.parametrize(
        "text",
        [
            # The wrapped BU `SPEC` cell: its last two assignments share one line,
            # separated by spaces only. Accepting it would emit codes 2 and 3 and
            # bury `4 = ...` in code 3's label.
            normalize_text(BU_SPEC_WRAPPED, multiline=True),
            "1=ja; 2=nej 3=kanske",  # same partial parse, one ordinary space
        ],
    )
    def test_partial_enumeration_is_explicitly_unresolved(self, text: str) -> None:
        # Not silent: a delivered list whose members cannot be separated states
        # no members AND is distinguishable from free text (`(None, False)`).
        assert _classify_value_set_text(text) == (None, True)


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
