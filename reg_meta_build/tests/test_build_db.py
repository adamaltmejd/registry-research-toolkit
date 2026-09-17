"""Tests for build-db pipeline (Phase 1)."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    timeseries_row,
)
from reg_meta.db import SCHEMA_VERSION, get_manifest, open_db
from reg_meta.errors import RegMetaError
from reg_meta.queries import extract_year
from reg_meta_build.db import (
    _CP850_CANON,
    _decode_cp1252,
    _value_set_hash,
)

if TYPE_CHECKING:
    from pathlib import Path

# A single irrelevant Timeseries.csv row (handelse not in the succession set, so
# it's ignored before resolution and emits NO event-derived edge). `write_csv`
# can't take an empty row list (it appends a stray blank line the strict CSV
# parser rejects), so curated-only `[[replaced_by]]` builds use this no-op row to
# isolate the curated pass.
_NO_EVENT_ROWS = [timeseries_row(handelse="Kodändring", entitet="Variabel", id1="100")]


class TestDecodeCP1252:
    def test_plain_ascii(self):
        assert _decode_cp1252("hello") == "hello"

    def test_swedish_chars(self):
        raw = "Kön".encode("cp1252").decode("latin-1")
        assert _decode_cp1252(raw) == "Kön"

    def test_cp850_fixup_0x90(self):
        # 0x90 is É in cp850, undefined in cp1252
        raw = bytes([0x90]).decode("latin-1")
        assert _decode_cp1252(raw) == "É"

    def test_cp850_fixup_0x8f(self):
        raw = bytes([0x8F]).decode("latin-1")
        assert _decode_cp1252(raw) == "Å"

    def test_cp850_fixup_0x9d(self):
        raw = bytes([0x9D]).decode("latin-1")
        assert _decode_cp1252(raw) == "Ø"

    def test_mixed_cp850_and_normal(self):
        # "MURCI<0x90>LAGO" → "MURCIÉLAGO"
        raw = b"MURCI\x90LAGO".decode("latin-1")
        assert _decode_cp1252(raw) == "MURCIÉLAGO"

    def test_ascii_fast_path_matches_full_decode(self):
        # The isascii() short-circuit must be identical to the full decode for
        # every ASCII string (latin-1/cp1252/ASCII agree on 0x00-0x7F).
        for s in ("hello", "ABC_123", "", "a-b.c/d", "0000"):
            assert _decode_cp1252(s) == s

    def test_cp850_canon_induces_decode_equivalence(self):
        # The Vardemängder hot loop keys value_code dedup on a `_CP850_CANON`
        # translate of the RAW latin-1 string instead of decoding every row —
        # correct ONLY if canon-equality == decoded-equality. `_decode_cp1252`
        # and `str.translate(_CP850_CANON)` are both per-character maps that
        # preserve length, so proving the relation per single byte over the full
        # 0..255 space proves it for arbitrary strings (position-wise compare).
        chars = [bytes([b]).decode("latin-1") for b in range(256)]
        decoded = [_decode_cp1252(c) for c in chars]
        canon = [c.translate(_CP850_CANON) for c in chars]
        for i, a in enumerate(chars):
            for j, b in enumerate(chars):
                assert (decoded[i] == decoded[j]) == (canon[i] == canon[j]), (a, b)


class TestBuildDb:
    def test_db_created(self, fixture_db: Path):
        assert fixture_db.exists()

    def test_opens_read_only(self, fixture_db: Path):
        conn = open_db(fixture_db)
        conn.close()

    def test_manifest(self, db_conn: sqlite3.Connection):
        manifest = get_manifest(db_conn)
        assert manifest["schema_version"] == SCHEMA_VERSION
        assert "import_date" in manifest
        row_counts = json.loads(manifest["row_counts"])
        assert row_counts == {"variables": 8, "states": 9}

    def test_register_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register").fetchone()[0]
        assert count == 2  # TESTREG and OTHERREG

    def test_variant_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register_variant").fetchone()[0]
        assert count == 2  # variant 10 and variant 20

    def test_register_version_metadata_tables_ship(self, db_conn: sqlite3.Connection):
        # #799: register_version plus its population/object_type children are
        # shipped read-only metadata for the register/variant catalog page.
        present = {
            r[0]
            for r in db_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('register_version', 'population', 'object_type')"
            ).fetchall()
        }
        assert present == {"register_version", "population", "object_type"}

    def test_register_version_metadata_values_ship(self, db_conn: sqlite3.Connection):
        version = db_conn.execute(
            "SELECT registerversionnamn, registerversionbeskrivning, "
            "registerversionmatinformation "
            "FROM register_version WHERE regver_id = 100"
        ).fetchone()
        assert dict(version) == {
            "registerversionnamn": "2020",
            "registerversionbeskrivning": "Version 2020",
            "registerversionmatinformation": "",
        }
        population = db_conn.execute(
            "SELECT name, definition, comment, date_range FROM population "
            "WHERE regver_id = 100"
        ).fetchone()
        assert dict(population) == {
            "name": "Hela befolkningen",
            "definition": "Alla personer",
            "comment": "",
            "date_range": "2020-12-31",
        }
        object_type = db_conn.execute(
            "SELECT name, definition FROM object_type WHERE regver_id = 100"
        ).fetchone()
        assert dict(object_type) == {
            "name": "Person",
            "definition": "Fysisk person",
        }

    def test_variable_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]
        # reg 1: Kön, TestVar, ÅÄÖVar
        # reg 2: Kön, UniqueVar, ParenVar, ExternVar, LopNr
        assert count == 8

    def test_variable_instance_absent(self, db_conn: sqlite3.Connection):
        """A2.7: `variable_instance` (and its cvid-grained alias staging) is
        BUILT then DROPped before ship — neither must survive in the shipped DB.
        Its cvid-grained metadata is coalesced into `variable_state`."""
        names = {
            r[0]
            for r in db_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('variable_instance', 'variable_alias_build')"
            )
        }
        assert names == set()

    def test_alias_anomaly(self, db_conn: sqlite3.Connection):
        """TestVar (var_id 100, reg 1) should have two aliases: TestCol and
        TestKolumn. A2.7: `variable_alias` is variable_id-keyed; the full
        delivery-column history survives the re-parent."""
        aliases = db_conn.execute(
            "SELECT va.delivery_column_name FROM variable_alias va "
            "JOIN variable v ON va.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '100' "
            "ORDER BY va.delivery_column_name"
        ).fetchall()
        assert [a[0] for a in aliases] == ["TestCol", "TestKolumn"]

    def test_value_items_present(self, db_conn: sqlite3.Connection):
        """Deduplicated value_set_member rows should be present for known
        cvids. After year-projection cvids 1001/1003/2001 share the {Man,
        Kvinna} set; cvid 2002 has its own {Övriga civilstånd} set; cvid 2003
        has its own {Uppgift okänd} set."""
        count = db_conn.execute("SELECT COUNT(*) FROM value_set_member").fetchone()[0]
        # 2 codes for the shared {Man, Kvinna} set
        # + 1 code for {Övriga civilstånd}
        # + 1 code for {Uppgift okänd}
        # = 4 value_set_member rows.
        assert count == 4

    def test_value_code_deduplicated(self, db_conn: sqlite3.Connection):
        """Value codes should be deduplicated across CVIDs."""
        count = db_conn.execute("SELECT COUNT(*) FROM value_code").fetchone()[0]
        # ("1","Man"), ("2","Kvinna"), ("2","Övriga civilstånd"), ("","Uppgift okänd")
        assert count == 4

    def test_value_set_version_label_on_state(self, db_conn: sqlite3.Connection):
        """A2.7: the value-set version label survives on `variable_state` (was
        per-cvid `variable_instance`). Kön var_id 44, reg 1, year 2020. The
        transient `vardemangdsniva` (build-only Swedish scope-guard column) is
        gone with `variable_instance`."""
        row = db_conn.execute(
            "SELECT vs.value_set_version_label FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '44' "
            "  AND vs.valid_from <= '2020-12-31' AND vs.valid_to >= '2020-01-01'"
        ).fetchone()
        assert row["value_set_version_label"] == "Kön"

    def test_sentinel_rows_skipped(self, db_conn: sqlite3.Connection):
        """SCB type-tag rows ("Tal", "Beskrivande text") must not produce
        value_code rows; sentinel-only eras must end up with NULL value_set_id.
        A2.7: checked on `variable_state` (was per-cvid). The 2022 era of Kön
        (var 44, reg 1, was cvid 1004) and ÅÄÖVar (var 200, reg 1, was cvid 1005)
        are sentinel-only → NULL value_set."""
        rows = db_conn.execute(
            "SELECT code FROM value_code WHERE code IN ('Tal', 'Beskrivande text')"
        ).fetchall()
        assert rows == []
        for var_id in (44, 200):
            row = db_conn.execute(
                "SELECT vs.value_set_id FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = CAST(? AS TEXT) "
                "  AND vs.valid_from <= '2022-12-31' AND vs.valid_to >= '2022-01-01'",
                (var_id,),
            ).fetchone()
            assert row is not None, f"var {var_id} 2022 state should exist"
            assert row["value_set_id"] is None, f"var {var_id} 2022 value_set_id"

    def test_sentinel_only_state_has_no_version_label(
        self, db_conn: sqlite3.Connection
    ):
        """A2.7: an era whose only Vardemangder rows were sentinels carries no
        real version label — on `variable_state` that surfaces as the empty
        DEFAULT '' (never the sentinel string). Checked for the 2022 eras of
        Kön (var 44) and ÅÄÖVar (var 200). (The build-only `vardemangdsniva`
        column is gone with `variable_instance`.)"""
        for var_id in (44, 200):
            row = db_conn.execute(
                "SELECT vs.value_set_version_label FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = CAST(? AS TEXT) "
                "  AND vs.valid_from <= '2022-12-31' AND vs.valid_to >= '2022-01-01'",
                (var_id,),
            ).fetchone()
            assert row["value_set_version_label"] == "", f"var {var_id}"

    def test_real_code_with_sentinel_shape_survives(self, db_conn: sqlite3.Connection):
        """A row where kod==version==niva but kod is not a known sentinel is a
        real code (UniqueVar, var 300 reg 2, kod="2", label="Övriga civilstånd").
        It must be preserved, with its version label. A2.7: read via
        `variable_state` (was per-cvid 2002)."""
        code_rows = db_conn.execute(
            "SELECT vc.code, vc.label "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE v.register_id = 2 AND v.provider_key = '300'"
        ).fetchall()
        assert [(r["code"], r["label"]) for r in code_rows] == [
            ("2", "Övriga civilstånd")
        ]
        meta = db_conn.execute(
            "SELECT vs.value_set_version_label FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 2 AND v.provider_key = '300'"
        ).fetchone()
        assert meta["value_set_version_label"] == "2"

    def test_empty_vardekod_survives(self, db_conn: sqlite3.Connection):
        """Empty vardekod with a label ("Uppgift okänd") is a legitimate code,
        not pollution. Must survive. A2.7: read via `variable_state` (ParenVar,
        var 301 reg 2, was cvid 2003)."""
        rows = db_conn.execute(
            "SELECT vc.code, vc.label "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE v.register_id = 2 AND v.provider_key = '301'"
        ).fetchall()
        assert [(r["code"], r["label"]) for r in rows] == [("", "Uppgift okänd")]

    def test_fully_empty_row_dropped(self, db_conn: sqlite3.Connection):
        """A row with empty kod, label, and item carries no information; the
        era must end up with NULL value_set_id and the empty version label.
        A2.7: TestVar (var 100 reg 1, was cvid 1002) read via `variable_state`."""
        row = db_conn.execute(
            "SELECT vs.value_set_id, vs.value_set_version_label FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '100'"
        ).fetchone()
        assert row["value_set_id"] is None
        assert row["value_set_version_label"] == ""

    def test_source_resolved_exact(self, db_conn: sqlite3.Connection):
        """OTHERREG Kön has kalla=TESTREG which matches register name exactly."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND provider_key = '44'"
        ).fetchone()
        assert row["source_register_id"] == 1
        assert row["source_label"] == "TESTREG"

    def test_source_resolved_parens(self, db_conn: sqlite3.Connection):
        """OTHERREG ParenVar has kalla with parenthesized abbreviation."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND provider_key = '301'"
        ).fetchone()
        assert row["source_register_id"] == 1
        assert row["source_label"] == "TESTREG"

    def test_source_null_for_own_variables(self, db_conn: sqlite3.Connection):
        """TESTREG's own variables have no source."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 1 AND provider_key = '44'"
        ).fetchone()
        assert row["source_register_id"] is None
        assert row["source_label"] is None

    def test_source_unresolved_stores_raw_text(self, db_conn: sqlite3.Connection):
        """ExternVar has kalla=Försäkringskassan which doesn't match any register."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND provider_key = '302'"
        ).fetchone()
        assert row["source_register_id"] is None
        assert row["source_label"] == "Försäkringskassan"

    def test_source_null_for_no_kalla(self, db_conn: sqlite3.Connection):
        """UniqueVar has no kalla — both source fields should be NULL."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND provider_key = '300'"
        ).fetchone()
        assert row["source_register_id"] is None
        assert row["source_label"] is None

    def test_e2e_variable_state_lineage_edge(self, db_conn: sqlite3.Connection):
        """End-to-end: the full build_db pipeline materializes a
        `variable_state_lineage` edge. OTHERREG's Kön (consumer, sourced from
        TESTREG, year 2021) joins TESTREG's value-set-bearing Kön state
        (2020-2021, in the only source variant `individer`); the
        interval intersection clips to the consumer's 2021. Proves pipeline
        ordering + `slug_root` plumbing, not just the unit-level linker."""
        rows = db_conn.execute(
            "SELECT l.valid_from, l.valid_to, "
            "       cr.slug AS consumer_register, cv.slug AS consumer_slug, "
            "       sr.slug AS source_register, sv.slug AS source_slug "
            "FROM variable_state_lineage l "
            "JOIN variable_state cs ON l.consumer_state_id = cs.state_id "
            "JOIN variable cv ON cs.variable_id = cv.variable_id "
            "JOIN register cr ON cv.register_id = cr.register_id "
            "JOIN variable_state ss ON l.source_state_id = ss.state_id "
            "JOIN variable sv ON ss.variable_id = sv.variable_id "
            "JOIN register sr ON sv.register_id = sr.register_id"
        ).fetchall()
        assert len(rows) == 1
        edge = rows[0]
        assert edge["consumer_register"] == "otherreg"
        assert edge["consumer_slug"] == "kon"
        assert edge["source_register"] == "testreg"
        assert edge["source_slug"] == "kon"
        assert edge["valid_from"] == "2021-01-01"
        assert edge["valid_to"] == "2021-12-31"

    def test_e2e_lineage_no_source_state_warning(self, db_conn: sqlite3.Connection):
        """ParenVar (OTHERREG, sourced from TESTREG) has no matching TESTREG
        variable slug, so the lineage pass emits a `no_source_state` warning —
        the consumer binding that resolves to no source state, surfaced
        explicitly (A2.7: `variable_state_lineage` is the sole lineage)."""
        rows = db_conn.execute(
            "SELECT warning_kind FROM variable_state_lineage_warning "
            "ORDER BY consumer_state_id, warning_kind"
        ).fetchall()
        assert any(r["warning_kind"] == "no_source_state" for r in rows)

    def test_code_variable_map_populated(self, db_conn: sqlite3.Connection):
        """code_variable_map is variable_id-grained: one row per
        (code, owning variable). The default fixture has no A2.2 split, so each
        (register, var_id) is a single variable and the count is unchanged from
        the old (register, var_id) grain. Split-sibling isolation (a code in only
        one sibling's value set must map to only that sibling) is the regression
        guarded end-to-end in reg_meta/tests/test_search_split_siblings.py."""
        count = db_conn.execute("SELECT COUNT(*) FROM code_variable_map").fetchone()[0]
        # Kön: 2 codes × 2 variables (reg 1 var 44, reg 2 var 44) = 4
        # cvid 2002 (UniqueVar): ("2","Övriga civilstånd") = 1
        # cvid 2003 (ParenVar): ("","Uppgift okänd") = 1
        assert count == 6
        # Every row's variable_id resolves (the FK + NOT NULL grain hold).
        unresolved = db_conn.execute(
            "SELECT COUNT(*) FROM code_variable_map cvm "
            "LEFT JOIN variable v ON cvm.variable_id = v.variable_id "
            "WHERE v.variable_id IS NULL"
        ).fetchone()[0]
        assert unresolved == 0

    def test_unika_summary_dropped(self, db_conn: sqlite3.Connection):
        """A2.1: unika_summary is build-time only — both A1.2 (sensitivity
        flags) and A2.1 (variable_state coalescer) have consumed it before
        the build commits, so the shipped DB carries no row for it. Asserting
        the table is gone (not just empty) catches a future regression where
        the DROP TABLE step is reordered after the commit."""
        row = db_conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'unika_summary'"
        ).fetchone()
        assert row is None

    def test_sensitivity_kanslig_variabel(self, db_conn: sqlite3.Connection):
        """A1.2: TestVar (register_id=1, var_id=100) has kanslig_variabel='1'
        (the real SCB export encoding) in unika_summary → is_sensitive=1,
        is_identifier=0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '100'"
        ).fetchone()
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0

    def test_sensitivity_kanslig_variabel_ibland(self, db_conn: sqlite3.Connection):
        """A1.2: ÅÄÖVar (register_id=1, var_id=200) has only
        kanslig_variabel_ibland='1' in unika_summary — the "22 edge cases"
        fold into is_sensitive per the mapping rule."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '200'"
        ).fetchone()
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0

    def test_sensitivity_identitetsvariabel(self, db_conn: sqlite3.Connection):
        """A1.2: UniqueVar (register_id=2, var_id=300) has identitetsvariabel='Ja'
        in unika_summary → is_identifier=1. 'Ja' is the LEGACY literal, kept here
        deliberately to exercise the lift's `IN ('1','Ja')` defensive match; the
        real export uses '1'. The kanslig columns are '0', so is_sensitive stays 0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 2 AND provider_key = '300'"
        ).fetchone()
        assert row["is_sensitive"] == 0
        assert row["is_identifier"] == 1

    def test_sensitivity_all_nej(self, db_conn: sqlite3.Connection):
        """A1.2 negative case: Kön (register_id=1, var_id=44) has all three
        unika_summary flags = '0' → both columns stay 0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '44'"
        ).fetchone()
        assert row["is_sensitive"] == 0
        assert row["is_identifier"] == 0

    def test_sensitivity_no_unika_row(self, db_conn: sqlite3.Connection):
        """A1.2: variables with no matching unika_summary row AND not declared in
        Identifierare.csv default to 0/0 (the DDL DEFAULT). ParenVar=2/301 and
        ExternVar=2/302 have neither — they must stay 0/0."""
        rows = db_conn.execute(
            "SELECT register_id, CAST(provider_key AS INTEGER) AS var_id, "
            "is_sensitive, is_identifier "
            "FROM variable WHERE (register_id, provider_key) IN ((2, '301'), (2, '302'))"
        ).fetchall()
        assert len(rows) == 2
        for row in rows:
            assert row["is_sensitive"] == 0, row["var_id"]
            assert row["is_identifier"] == 0, row["var_id"]

    def test_identifier_from_identifierare_without_unika(
        self, db_conn: sqlite3.Connection
    ):
        """Change 1 (unika ∪ Identifierare.csv): LopNr=2/303 has NO unika_summary
        row but IS declared in Identifierare.csv, so is_identifier=1 from the
        declared list alone. is_sensitive stays 0 (Identifierare carries no
        sensitivity signal — that comes only from unika)."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 2 AND provider_key = '303'"
        ).fetchone()
        assert row["is_identifier"] == 1
        assert row["is_sensitive"] == 0

    # ------------------------------------------------------------------
    # A2.1 — variable_state coalescer
    # ------------------------------------------------------------------

    def test_variable_state_rows_present(self, db_conn: sqlite3.Connection):
        """A2.1: the coalescer materializes at least one variable_state row per
        `variable`. A2.7: cross-checked against `variable` (was the dropped
        `variable_instance`) — every fixture variable has >= 1 era, so a
        coalescer that never runs or handles only a subset is caught."""
        vids = db_conn.execute("SELECT variable_id FROM variable").fetchall()
        assert len(vids) > 0
        for (vid,) in vids:
            n = db_conn.execute(
                "SELECT COUNT(*) FROM variable_state WHERE variable_id = ?",
                (vid,),
            ).fetchone()[0]
            assert n >= 1, f"no variable_state for variable_id {vid}"

    def test_variable_state_valid_from_to_full_iso(self, db_conn: sqlite3.Connection):
        """Every valid_from / valid_to is a 10-char YYYY-MM-DD string.
        The CHECK constraint guards this at write time; this test catches
        the data layer in case a future migration loosens the CHECK."""
        rows = db_conn.execute(
            "SELECT valid_from, valid_to FROM variable_state"
        ).fetchall()
        assert rows
        for r in rows:
            assert len(r["valid_from"]) == 10
            assert len(r["valid_to"]) == 10
            assert r["valid_from"][4] == "-" and r["valid_from"][7] == "-"
            assert r["valid_to"][4] == "-" and r["valid_to"][7] == "-"
            # Lexical comparison is chronological for full-date ISO strings.
            assert r["valid_from"] <= r["valid_to"]

    def test_variable_state_year_expansion(self, db_conn: sqlite3.Connection):
        """A2.1: unika_summary year "2022" expands to '2022-01-01'..'2022-12-31'.
        Asserted against ÅÄÖVar (register_id=1, var_id=200), which has
        a single unika row VersionForsta=VersionSista='2022'."""
        rows = db_conn.execute(
            "SELECT valid_from, valid_to FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '200'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["valid_from"] == "2022-01-01"
        assert rows[0]["valid_to"] == "2022-12-31"

    def test_variable_state_year_range_min_max(self, db_conn: sqlite3.Connection):
        """A2.1: Kön in TESTREG appears across 2020/2021/2022 split into two
        shape groups by the value_set: cvids 1001/1003 carry the
        year-projected `Kön` value_set (regver years 2020+2021); cvid 1004
        has NULL value_set (sentinel-only Vardemangder rows for 2022).

        Each group claims its OWN observed years — not the full unika
        lifetime — per the non-overlap invariant and the Codex P1
        fix on PR #130. Without clamping, both groups would inherit
        unika's 2020-2022 range and overlap on 2020-2021 with no
        `value_set_version_label` discriminator, which the A2.5 point
        resolver can't unambiguously narrow."""
        rows = db_conn.execute(
            "SELECT valid_from, valid_to, value_set_id "
            "FROM variable_state vs JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '44' "
            "ORDER BY value_set_id NULLS LAST"
        ).fetchall()
        assert len(rows) >= 1
        # The value-set-bearing group covers cvids 1001 (2020) + 1003 (2021).
        # var_max_regver for the variable = 2022 (cvid 1004), so this
        # group is NOT the latest era and ends at its own regver_max.
        with_set = [r for r in rows if r["value_set_id"] is not None]
        assert with_set, "expected a Kön state with a value_set"
        assert with_set[0]["valid_from"] == "2020-01-01"
        assert with_set[0]["valid_to"] == "2021-12-31"
        # The NULL-value_set group covers cvid 1004 (2022). It IS the
        # latest era (regver_max=2022=var_max), and the unika row is
        # bounded (VersionSista='2022'), so this group spans 2022 only.
        without_set = [r for r in rows if r["value_set_id"] is None]
        assert without_set, "expected a Kön state without a value_set"
        assert without_set[0]["valid_from"] == "2022-01-01"
        assert without_set[0]["valid_to"] == "2022-12-31"

    def test_variable_state_delivery_column_name(self, db_conn: sqlite3.Connection):
        """delivery_column_name on variable_state is the denormalized
        latest alias. For TestVar (cvid 1002) with aliases ['TestCol',
        'TestKolumn'] both attached to the same regver, the lexically
        smaller alias wins by deterministic tie-break."""
        row = db_conn.execute(
            "SELECT delivery_column_name FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '100'"
        ).fetchone()
        assert row is not None
        assert row["delivery_column_name"] == "TestCol"

    def test_variable_state_regver_fallback(self, db_conn: sqlite3.Connection):
        """A2.1: ParenVar (register_id=2, var_id=301) has NO unika row in the
        fixture; the coalescer falls back to register_version.registerversionnamn
        ("2021") to derive the valid range. Confirms the fallback path is
        wired correctly."""
        row = db_conn.execute(
            "SELECT valid_from, valid_to FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 2 AND v.provider_key = '301'"
        ).fetchone()
        assert row is not None
        assert row["valid_from"] == "2021-01-01"
        assert row["valid_to"] == "2021-12-31"

    def test_variable_state_value_set_version_label_preserved(
        self, db_conn: sqlite3.Connection
    ):
        """A2.1: value_set_version_label rides through the coalescer onto
        variable_state — it's the multi-vintage discriminator that
        permits overlapping states. UniqueVar's instance gets the "2"
        label from Vardemangder; assert it surfaces on the state row."""
        row = db_conn.execute(
            "SELECT value_set_version_label FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 2 AND v.provider_key = '300'"
        ).fetchone()
        assert row is not None
        # Matches what _import_vardemangder writes onto variable_instance
        # (see VARDEMANGDER_REAL_SHAPED_ROWS: kod="2", label="Övriga
        # civilstånd"). Group key carries the label through unchanged.
        assert row["value_set_version_label"] == "2"

    def test_variable_state_grain_split(self, db_conn: sqlite3.Connection):
        """A2.1: when cvids for the same (register_id, register_variant_id, var_id)
        differ on transient grain (vardemangdsniva on variable_instance),
        the coalescer keeps them as distinct variable_state rows so A2.2
        can triage. Fixture Kön cvid 1004 has no Vardemangder row (sentinel)
        so its grain / value_set_id end up NULL — that's a different group
        key from cvids 1001/1003 which carry a real value_set."""
        # Two rows for register_id=1, var_id=44 (Kön): one with value_set,
        # one without. Both share register_variant_id=10 (same variant).
        rows = db_conn.execute(
            "SELECT value_set_id, value_set_version_label "
            "FROM variable_state vs JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '44'"
        ).fetchall()
        # At least one row has a value_set. The exact split depends on
        # year-projection covering cvid 1004 — which it doesn't (validity
        # windows in the fixture stop at the 5001/5003 items, not 1004's
        # sentinel-only ItemIds), so 1004 falls into a separate group.
        with_set = [r for r in rows if r["value_set_id"] is not None]
        assert with_set, "expected at least one Kön state with a value_set"
        # Either grain split produced a NULL-value_set companion, or every
        # cvid in the group had the same value_set. Both outcomes are valid
        # at A2.1 (triage is A2.2); the test only fails if the coalescer
        # silently collapses distinct value_set_ids into one state.
        if len(rows) > 1:
            value_set_ids = {r["value_set_id"] for r in rows}
            assert len(value_set_ids) == len(rows), (
                "coalescer collapsed distinct value_set_ids "
                f"into one variable_state row: {value_set_ids}"
            )

    def test_variable_state_fk_to_variable(self, db_conn: sqlite3.Connection):
        """Every variable_state row points at a real variable row via the
        A2.1.5 synthetic `variable_id` FK. PRAGMA foreign_key_check is already
        invoked at build time; this is a regression-level sanity check."""
        orphans = db_conn.execute(
            "SELECT vs.state_id FROM variable_state vs "
            "LEFT JOIN variable v ON v.variable_id = vs.variable_id "
            "WHERE v.variable_id IS NULL"
        ).fetchall()
        assert orphans == []

    def test_variable_state_count_summary(self, db_conn: sqlite3.Connection):
        """The explicit fixture's manifest describes its complete graph."""
        from_table = db_conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[
            0
        ]
        import json as _json

        manifest = db_conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'row_counts'"
        ).fetchone()
        assert manifest is not None
        stats = _json.loads(manifest["value"])
        assert stats["states"] == from_table

    def test_identifierare_imported(self, db_conn: sqlite3.Connection):
        row = db_conn.execute(
            "SELECT variabelnamn FROM identifier_semantics WHERE var_id = 303"
        ).fetchone()
        assert row["variabelnamn"] == "LopNr"

    def test_variable_state_no_open_ended_in_default_fixture(
        self, db_conn: sqlite3.Connection
    ):
        """Sanity gate: the default fixture has every unika row populated
        on both sides, so no variable_state row should carry the
        open-ended sentinel. This pins the fixture invariant — if a future
        contributor adds an open-ended unika row to the standard fixture,
        the dedicated open-ended test below stops being the only signal
        and we want loud failure here, not silent drift."""
        sentinel_rows = db_conn.execute(
            "SELECT COUNT(*) FROM variable_state WHERE valid_to = '9999-12-31'"
        ).fetchone()[0]
        assert sentinel_rows == 0

    def test_timeseries_imported(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM timeseries_event").fetchone()[0]
        assert count == 1

    def test_fts_register(self, db_conn: sqlite3.Connection):
        rows = db_conn.execute(
            "SELECT register_id FROM register_fts WHERE register_fts MATCH 'Testning'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["register_id"] == 1

    def test_fts_variable(self, db_conn: sqlite3.Connection):
        rows = db_conn.execute(
            "SELECT provider_key FROM variable_fts WHERE variable_fts MATCH 'testvariabel'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["provider_key"] == "100"

    def test_provider_seed(self, db_conn: sqlite3.Connection):
        # provider_id values are stable across releases — downstream pins
        # against them (PROVIDER_ID_SCB = 1, PROVIDER_ID_SOS = 2,
        # PROVIDER_ID_FOHM = 3, PROVIDER_ID_FK = 4). Every seeded provider is
        # present regardless of
        # which adapters this build ran.
        rows = db_conn.execute(
            "SELECT provider_id, slug, name FROM provider ORDER BY provider_id"
        ).fetchall()
        assert [(r["provider_id"], r["slug"]) for r in rows] == [
            (1, "scb"),
            (2, "sos"),
            (3, "fohm"),
            (4, "fk"),
            (5, "lakemedelsverket"),
            (6, "pliktverket"),
            (7, "riksarkivet"),
            (8, "umu"),
        ]

    def test_scb_registers_tagged_scb(self, db_conn: sqlite3.Connection):
        rows = db_conn.execute(
            "SELECT p.slug FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id"
        ).fetchall()
        assert rows  # fixture has registers
        assert {r["slug"] for r in rows} == {"scb"}

    def test_slugs_populated_post_build(self, db_conn: sqlite3.Connection):
        # The fixture builds with a curated slug TOML covering both
        # registers + variants. Strict-built DBs must have every slug populated
        # — `populate_slugs` raises otherwise — so this also guards the strict
        # invariant. (A2.6: register_version has no slug column and is dropped
        # before ship, so there's no version-slug assertion.)
        assert (
            db_conn.execute(
                "SELECT COUNT(*) FROM register WHERE slug IS NULL"
            ).fetchone()[0]
            == 0
        )
        assert (
            db_conn.execute(
                "SELECT COUNT(*) FROM register_variant WHERE slug IS NULL"
            ).fetchone()[0]
            == 0
        )

    def test_no_synthetic_default_variant_rows_persisted(
        self, db_conn: sqlite3.Connection
    ):
        # The `_default` placeholder for variant-less registers is
        # synthesized at FQID-resolve time (catalog.py), never persisted.
        # Every register_variant row in the DB must be a real source row
        # — i.e. `name` (renamed from `registervariantnamn`) populated.
        synthetic = db_conn.execute(
            "SELECT COUNT(*) FROM register_variant WHERE name IS NULL"
        ).fetchone()[0]
        assert synthetic == 0

    def test_default_variant_slug_only_when_curated(self, db_conn: sqlite3.Connection):
        # With synthesis moved to resolve-time, `_default` in this column can
        # only mean curator action. The current curation snapshot has none;
        # update this expected count when the name-mirror sweep lands.
        count = db_conn.execute(
            "SELECT COUNT(*) FROM register_variant WHERE slug = '_default'"
        ).fetchone()[0]
        assert count == 0

    def test_seed_providers_idempotent(self, tmp_path: Path):
        # `seed_providers` is a public helper that may run more than once
        # against the same DB; a plain INSERT would raise IntegrityError on the
        # fixed PKs the second time.
        import sqlite3 as _sqlite3

        from reg_meta_build.db import DDL, seed_providers

        conn = _sqlite3.connect(str(tmp_path / "idem.db"))
        conn.row_factory = _sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        seed_providers(conn)  # must not raise
        rows = conn.execute(
            "SELECT provider_id, slug FROM provider ORDER BY provider_id"
        ).fetchall()
        assert [(r["provider_id"], r["slug"]) for r in rows] == [
            (1, "scb"),
            (2, "sos"),
            (3, "fohm"),
            (4, "fk"),
            (5, "lakemedelsverket"),
            (6, "pliktverket"),
            (7, "riksarkivet"),
            (8, "umu"),
        ]
        conn.close()

    def test_seed_providers_rejects_mismatch(self, tmp_path: Path):
        # A pre-existing row with the wrong slug means the DB came from
        # somewhere else (corruption, partial migration). Failing fast is
        # safer than silently overwriting via UPSERT.
        import sqlite3 as _sqlite3

        from reg_meta_build.db import DDL, seed_providers

        conn = _sqlite3.connect(str(tmp_path / "mismatch.db"))
        conn.row_factory = _sqlite3.Row
        conn.executescript(DDL)
        conn.execute(
            "INSERT INTO provider (provider_id, slug, name) VALUES (1, 'wrong', 'X')"
        )
        with pytest.raises(RuntimeError, match="already present"):
            seed_providers(conn)
        conn.close()


class TestBuildDbErrors:
    def test_db_not_found(self, tmp_path: Path):
        with pytest.raises(RegMetaError) as exc_info:
            open_db(tmp_path / "nonexistent.db")
        assert exc_info.value.code == "db_not_found"


class TestSchemaCompat:
    """open_db rejects databases whose schema is incompatible with the code.

    The check compares the major/minor components of SCHEMA_VERSION (in db.py)
    against the schema_version stored in the database's import_manifest table.
    Majors must match exactly, the DB minor must be >= the code minor, and
    patch is ignored. Bump SCHEMA_VERSION's major for breaking changes and the
    minor when the code starts reading a new column so that older DBs are
    rejected up front with a clear error instead of failing later with a
    cryptic SQL error.
    """

    @staticmethod
    def _make_db(tmp_path: Path, schema_version: str) -> Path:
        """Create a minimal SQLite db with a given schema_version in its manifest."""
        db_path = tmp_path / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute(
            "INSERT INTO import_manifest VALUES ('schema_version', ?)",
            (schema_version,),
        )
        conn.commit()
        conn.close()
        return db_path

    def test_compatible_same_version(self, tmp_path: Path):
        db = self._make_db(tmp_path, SCHEMA_VERSION)
        conn = open_db(db)
        conn.close()

    def test_compatible_minor_bump(self, tmp_path: Path):
        """A minor version bump in the db is still compatible."""
        major = SCHEMA_VERSION.split(".")[0]
        db = self._make_db(tmp_path, f"{major}.99.0")
        conn = open_db(db)
        conn.close()

    def test_incompatible_major_mismatch(self, tmp_path: Path):
        major = int(SCHEMA_VERSION.split(".")[0])
        db = self._make_db(tmp_path, f"{major + 1}.0.0")
        with pytest.raises(RegMetaError) as exc_info:
            open_db(db)
        assert exc_info.value.code == "schema_incompatible"

    def test_incompatible_old_major(self, tmp_path: Path):
        major = int(SCHEMA_VERSION.split(".")[0])
        if major == 0:
            pytest.skip("major is already 0")
        db = self._make_db(tmp_path, f"{major - 1}.0.0")
        with pytest.raises(RegMetaError) as exc_info:
            open_db(db)
        assert exc_info.value.code == "schema_incompatible"

    def test_incompatible_old_minor(self, tmp_path: Path):
        """A DB with the same major but a lower minor is rejected.

        Guards against regressions like v0.5.1's published DB asset (schema
        2.0.0) being used with code expecting schema 2.1.0 — the old bug
        surfaced as a runtime `no such column` error instead of a clean
        schema_incompatible error.
        """
        major, minor = (int(x) for x in SCHEMA_VERSION.split(".")[:2])
        if minor == 0:
            pytest.skip("minor is already 0")
        db = self._make_db(tmp_path, f"{major}.{minor - 1}.0")
        with pytest.raises(RegMetaError) as exc_info:
            open_db(db)
        assert exc_info.value.code == "schema_incompatible"

    def test_check_schema_false_skips(self, tmp_path: Path):
        """check_schema=False bypasses the compatibility check."""
        major = int(SCHEMA_VERSION.split(".")[0])
        db = self._make_db(tmp_path, f"{major + 1}.0.0")
        conn = open_db(db, check_schema=False)
        conn.close()

    def test_missing_manifest_table(self, tmp_path: Path):
        """A database without import_manifest is rejected."""
        db_path = tmp_path / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE dummy (x TEXT)")
        conn.commit()
        conn.close()
        with pytest.raises(RegMetaError) as exc_info:
            open_db(db_path)
        assert exc_info.value.code == "schema_incompatible"

    def test_missing_schema_version_key(self, tmp_path: Path):
        """A manifest without schema_version is rejected."""
        db_path = tmp_path / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO import_manifest VALUES ('import_date', '2024-01-01')")
        conn.commit()
        conn.close()
        with pytest.raises(RegMetaError) as exc_info:
            open_db(db_path)
        assert exc_info.value.code == "schema_incompatible"

    def test_unparseable_schema_version(self, tmp_path: Path):
        """A manifest with garbage schema_version is rejected."""
        db = self._make_db(tmp_path, "not-a-version")
        with pytest.raises(RegMetaError) as exc_info:
            open_db(db)
        assert exc_info.value.code == "schema_incompatible"


# ---------------------------------------------------------------------------
# Year projection
# ---------------------------------------------------------------------------


class TestExtractYear:
    """``extract_year`` matches a 1900-2099 year as a standalone 4-digit
    token; values outside that range or embedded in longer digit runs return
    None (yearless fallback for projection)."""

    def test_extracts_year_from_lisa_2018(self):
        assert extract_year("LISA 2018") == 2018

    def test_returns_none_for_out_of_range(self):
        assert extract_year("Komvux 1234-poäng") is None
        assert extract_year("1234") is None

    def test_returns_none_for_yearless_names(self):
        assert extract_year("Person-År") is None
        assert extract_year("Födelseland") is None

    def test_returns_none_for_empty(self):
        assert extract_year("") is None

    def test_rejects_year_inside_longer_digit_run(self):
        # 19999 is not a year; the regex must not match the prefix 1999.
        assert extract_year("v19999") is None


class TestValueSetHash:
    """``_value_set_hash`` is content-addressed sha256 over sorted
    (vardekod, vardebenamning) pairs with length-prefixed encoding."""

    def test_returns_32_byte_digest(self):
        h = _value_set_hash([("1", "Man"), ("2", "Kvinna")])
        assert isinstance(h, bytes)
        assert len(h) == 32

    def test_is_order_independent(self):
        a = _value_set_hash([("1", "Man"), ("2", "Kvinna")])
        b = _value_set_hash([("2", "Kvinna"), ("1", "Man")])
        assert a == b

    def test_distinguishes_different_sets(self):
        a = _value_set_hash([("1", "Man")])
        b = _value_set_hash([("1", "Man"), ("2", "Kvinna")])
        assert a != b

    def test_length_prefixed_encoding_avoids_collision(self):
        # Without length prefixes, ("ab", "c") and ("a", "bc") could collide.
        a = _value_set_hash([("ab", "c")])
        b = _value_set_hash([("a", "bc")])
        assert a != b


class TestMemberHashCheckConstraint:
    """The DDL declares ``CHECK (length(member_hash) = 32)`` — non-32-byte
    blobs must be rejected."""

    def test_short_blob_rejected(self, fixture_db: Path):
        conn = sqlite3.connect(fixture_db)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO value_set (member_hash) VALUES (?)", (b"\x00" * 31,)
            )
        conn.close()


class TestValueSetDedup:
    """Two cvids with the same year-projected code list must share one
    value_set; cvids with different lists must not."""

    @staticmethod
    def _value_set_id(conn, *, register_id: int, var_id: int, year: int):
        """A2.7: the value_set_id a variable's covering state carries (was
        `variable_instance.value_set_id` per cvid — dropped before ship)."""
        return conn.execute(
            "SELECT vs.value_set_id FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = ? AND CAST(v.provider_key AS INTEGER) = ? "
            "  AND CAST(substr(vs.valid_from, 1, 4) AS INTEGER) <= ? "
            "  AND CAST(substr(vs.valid_to, 1, 4) AS INTEGER) >= ? "
            "  AND vs.value_set_id IS NOT NULL "
            "LIMIT 1",
            (register_id, var_id, year, year),
        ).fetchone()

    def test_identical_sets_share_value_set_id(self, db_conn: sqlite3.Connection):
        # Kön var_id 44 in TESTREG (reg 1, was cvid 1003) and OTHERREG (reg 2,
        # was cvid 2001) both end up with {Man, Kvinna} after projection → one
        # content-addressed value_set.
        a = self._value_set_id(db_conn, register_id=1, var_id=44, year=2021)
        b = self._value_set_id(db_conn, register_id=2, var_id=44, year=2021)
        assert a is not None
        assert a[0] == b[0]

    def test_different_sets_get_different_ids(self, db_conn: sqlite3.Connection):
        # var_id 300 has {Övriga civilstånd} (was cvid 2002); var_id 301 has
        # {Uppgift okänd} (was cvid 2003); different sets → different ids.
        a = self._value_set_id(db_conn, register_id=2, var_id=300, year=2021)
        b = self._value_set_id(db_conn, register_id=2, var_id=301, year=2021)
        assert a is not None
        assert b is not None
        assert a[0] != b[0]

    def test_member_hash_unique(self, db_conn: sqlite3.Connection):
        dups = db_conn.execute(
            "SELECT COUNT(*) FROM value_set GROUP BY member_hash HAVING COUNT(*) > 1"
        ).fetchall()
        assert dups == []


class TestSameAsBuildIntegration:
    """End-to-end coverage that `build_db` correctly wires up
    `relations.materialize_same_as` from the curated `relations.toml`: tables
    created, populated when the file carries a `type = "same_as"` edge, skipped
    under `--skip-slugs`, and the resolver can traverse against the resulting DB.
    """

    def test_tables_present_in_fixture_db(self, db_conn: sqlite3.Connection) -> None:
        # Fixture has no same_as entries; the tables should still exist
        # (created by the schema DDL) and be empty.
        tables = {
            r[0]
            for r in db_conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name IN ("
                "'variable_same_as', 'classification_same_as', "
                "'classification_derived_from')"
            ).fetchall()
        }
        assert tables == {
            "variable_same_as",
            "classification_same_as",
            "classification_derived_from",
        }
        var_count = db_conn.execute("SELECT COUNT(*) FROM variable_same_as").fetchone()[
            0
        ]
        cls_count = db_conn.execute(
            "SELECT COUNT(*) FROM classification_same_as"
        ).fetchone()[0]
        derived_count = db_conn.execute(
            "SELECT COUNT(*) FROM classification_derived_from"
        ).fetchone()[0]
        assert (var_count, cls_count, derived_count) == (0, 0, 0)

    def test_empty_same_as_emits_no_manifest_keys(
        self, db_conn: sqlite3.Connection
    ) -> None:
        # #522 byte-identity: the `*_same_as_curated` manifest keys are emitted
        # ONLY when non-zero. With the curated same_as file empty (the fixture),
        # they must be ABSENT from `row_counts` — an always-present `…: 0` pair
        # would change the manifest blob vs the released DB and trip dbdiff.
        row = db_conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'row_counts'"
        ).fetchone()
        row_counts = json.loads(row[0])
        assert "variable_same_as_curated" not in row_counts
        assert "classification_same_as_curated" not in row_counts
        assert "classification_derived_from_curated" not in row_counts

    @staticmethod
    def _write_slug_dir(slug_dir: Path) -> None:
        """Slug TOML mirroring the shared fixture's two registers."""
        (slug_dir / "scb.toml").write_text(
            '[register."1"]\nslug = "testreg"\n'
            '[register."2"]\nslug = "otherreg"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            '[register_variant."2.20"]\nslug = "foretag"\n'
            '[variable."1.100"]\nslug = "legacy-kon"\n',
            encoding="utf-8",
        )
        (slug_dir / "classifications.toml").write_text("", encoding="utf-8")

    @staticmethod
    def _write_relations(tmp_path: Path) -> Path:
        """A curated `relations.toml` with one `type = "same_as"` edge from
        TESTREG's real `kon` to the live synthetic `legacy-kon` slug."""
        path = tmp_path / "relations.toml"
        path.write_text(
            '[[edge]]\ntype = "same_as"\n'
            'a = "scb/testreg/kon"\nb = "scb/testreg/legacy-kon"\n',
            encoding="utf-8",
        )
        return path
