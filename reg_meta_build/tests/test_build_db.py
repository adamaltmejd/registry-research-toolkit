"""Tests for build-db pipeline (Phase 1)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from _csv_fixtures import (
    PIPE,
    REGISTERINFORMATION_HEADER,
    REGISTERINFORMATION_ROWS,
    UNIKA_ROWS,
    VARDEMANGDER_REAL_ROWS,
    VARDEMANGDER_ROWS,
    _ri_row,
    _var_row,
    timeseries_row,
    write_csv,
    write_scb_input,
)
from reg_meta.db import SCHEMA_VERSION, get_manifest, open_db
from reg_meta.errors import RegMetaError
from reg_meta.queries import extract_year
from reg_meta_build.db import (
    _CP850_CANON,
    _decode_cp1252,
    _value_set_hash,
    build_db,
)

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

    def test_validator_open_leaves_no_wal_sidecars(self, tmp_path: Path):
        # The build connection's clean close deletes the WAL `-wal`/`-shm`
        # sidecars, but the post-build validator re-opens the tmp DB read-only
        # and SQLite re-creates them; the atomic rename moves only the base
        # file, so without cleanup they orphan as `reg_meta.db.tmp-wal`/`-shm`.
        # The shared `fixture_db` passes no hook, so it can't catch this — drive
        # build_db with a read-only-opening hook (the real CLI path).
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        input_dir.mkdir()
        write_scb_input(input_dir)

        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
            pre_rename_hook=lambda p: open_db(p).close(),
        )

        orphans = sorted(p.name for p in db_dir.iterdir() if ".db.tmp" in p.name)
        assert orphans == []

    def test_manifest(self, db_conn: sqlite3.Connection):
        manifest = get_manifest(db_conn)
        assert manifest["schema_version"] == SCHEMA_VERSION
        assert "import_date" in manifest

    def test_register_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register").fetchone()[0]
        assert count == 2  # TESTREG and OTHERREG

    def test_variant_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register_variant").fetchone()[0]
        assert count == 2  # variant 10 and variant 20

    def test_register_version_tables_dropped(self, db_conn: sqlite3.Connection):
        # A2.6: register_version (+ its FK children population/object_type) are
        # build-time-only and DROPped before ship (like unika_summary). The
        # coalescer's year fallback + the lineage linkers consume register_version
        # earlier in the build; nothing in the shipped catalog reads it.
        present = {
            r[0]
            for r in db_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('register_version', 'population', 'object_type')"
            ).fetchall()
        }
        assert present == set()

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
        (2020-2021, `individer` variant pinned via `[lineage_defaults]`); the
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
        """A2.1: total variable_state row count matches the manifest's
        coalesce_stats.n_variable_states. Catches a future regression
        where the stats accounting drifts from the SQL truth."""
        from_table = db_conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[
            0
        ]
        import json as _json

        manifest = db_conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'coalesce_stats'"
        ).fetchone()
        assert manifest is not None
        stats = _json.loads(manifest["value"])
        assert stats["n_variable_states"] == from_table

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
        # `seed_providers` is a public helper used by both `build_db` and
        # mock_data_wizard's reg_meta fixture; a plain INSERT would raise
        # IntegrityError on the fixed PKs the second time.
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

    def test_atomic_replace(self, fixture_db: Path):
        """Rebuilding should replace the DB atomically."""
        input_dir = fixture_db.parent.parent / "input_rebuild"
        scb_dir = input_dir / "SCB"
        scb_dir.mkdir(parents=True, exist_ok=True)
        write_csv(
            scb_dir / "Registerinformation.csv",
            REGISTERINFORMATION_HEADER,
            REGISTERINFORMATION_ROWS[:1],
        )

        db_dir = fixture_db.parent.parent / "db_rebuild"
        db_dir.mkdir(exist_ok=True)

        result = build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        assert Path(result["db_path"]).exists()

        # Rebuild with same data should work
        result2 = build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        assert Path(result2["db_path"]).exists()


class TestBuildDbErrors:
    def test_missing_input_dir(self, tmp_path: Path):
        with pytest.raises(RegMetaError) as exc_info:
            build_db(input_dir=tmp_path / "nonexistent", db_dir=tmp_path)
        assert exc_info.value.code == "input_dir_not_found"

    def test_missing_scb_dir(self, tmp_path: Path):
        # input_dir exists but no SCB/ subdirectory
        with pytest.raises(RegMetaError) as exc_info:
            build_db(input_dir=tmp_path, db_dir=tmp_path)
        assert exc_info.value.code == "scb_dir_not_found"

    def test_missing_fohm_dir(self, tmp_path: Path):
        # #422: a fohm-only build whose input_dir exists but has no
        # Folkhalsomyndigheten/ subdirectory fails with the per-provider
        # curated-dir error (mirrors test_missing_scb_dir for the curated path).
        input_dir = tmp_path / "input"
        input_dir.mkdir()  # exists (passes input_dir.is_dir()), but no curated subdir
        with pytest.raises(RegMetaError) as exc_info:
            build_db(
                providers=("fohm",),
                input_dir=input_dir,
                db_dir=tmp_path / "db",
                skip_classifications=True,
            )
        assert exc_info.value.code == "fohm_dir_not_found"

    def test_missing_backbone(self, tmp_path: Path):
        scb_dir = tmp_path / "SCB"
        scb_dir.mkdir()
        with pytest.raises(RegMetaError) as exc_info:
            build_db(input_dir=tmp_path, db_dir=tmp_path)
        assert exc_info.value.code == "csv_missing_backbone"

    def test_empty_csv(self, tmp_path: Path):
        scb_dir = tmp_path / "SCB"
        scb_dir.mkdir()
        (scb_dir / "Registerinformation.csv").write_bytes(b"")
        with pytest.raises(RegMetaError) as exc_info:
            build_db(input_dir=tmp_path, db_dir=tmp_path)
        assert exc_info.value.code == "csv_empty"

    def test_bad_header(self, tmp_path: Path):
        scb_dir = tmp_path / "SCB"
        scb_dir.mkdir()
        (scb_dir / "Registerinformation.csv").write_bytes(b"Wrong|Header\r\n")
        with pytest.raises(RegMetaError) as exc_info:
            build_db(input_dir=tmp_path, db_dir=tmp_path)
        assert exc_info.value.code == "csv_bad_header"

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


class TestVardemangderDrift:
    """Any kod==version row where kod is in neither the SENTINELS nor the
    REAL_SHAPED allowlist must hard-fail the build, so SCB additions don't
    silently slip into value_code or get incorrectly dropped."""

    def test_default_fixture_does_not_drift(self, tmp_path: Path) -> None:
        # Sanity: the shared default fixture (which other tests build on)
        # contains kod="2" — a known real-shaped code in the allowlist — and
        # must not raise. Guards against drift creeping into the shared set.
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )

    def test_drift_raises_on_unknown_kod(self, tmp_path: Path) -> None:
        # "ZZZ" is in neither allowlist; build must fail with an actionable
        # error pointing the maintainer at the two allowlists.
        drift_rows = list(VARDEMANGDER_REAL_ROWS) + [
            "|".join(["ZZZ", "ZZZ", "ZZZ", "Future placeholder", "2002", "5102"]),
        ]
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir, vardemangder_rows=drift_rows)
        with pytest.raises(RegMetaError) as exc_info:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                skip_classifications=True,
                skip_slugs=True,
            )
        assert exc_info.value.code == "vardemangder_drift"
        assert exc_info.value.exit_code == 10
        assert "'ZZZ'" in exc_info.value.message
        assert "_VARDEMANGDER_SENTINELS" in exc_info.value.remediation
        assert "_VARDEMANGDER_REAL_SHAPED" in exc_info.value.remediation

    def test_drift_raises_on_niva_divergent_sentinel(self, tmp_path: Path) -> None:
        # Skip rule requires kod==version==niva. A row with kod=version="Tal"
        # but niva diverging is a novel SCB shape — the build must surface it
        # rather than silently drop it. Failure mode that the upstream
        # reviewers (Codex, Copilot) flagged as a risk.
        # Column order: version|niva|kod|label|cvid|item
        drift_rows = list(VARDEMANGDER_REAL_ROWS) + [
            "|".join(["Tal", "1", "Tal", "Some label", "2002", "5102"]),
        ]
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir, vardemangder_rows=drift_rows)
        with pytest.raises(RegMetaError) as exc_info:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                skip_classifications=True,
                skip_slugs=True,
            )
        assert exc_info.value.code == "vardemangder_drift"
        assert "'Tal'" in exc_info.value.message
        # Remediation must surface the "already in SENTINELS" case so the
        # maintainer doesn't try to add Tal a second time.
        assert "niva!=version" in exc_info.value.remediation


class TestVardemangderRequiresValidDates:
    """Year-projection guarantees `get values` returns the year-correct set.
    That guarantee silently degrades to the historical union if
    VardemangderValidDates.csv is absent — so the build must fail fast when
    Vardemangder.csv is present without it."""

    def test_missing_valid_dates_with_vardemangder_raises(self, tmp_path: Path) -> None:
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(
            input_dir,
            include=(
                "registerinformation",
                "unika",
                "identifierare",
                "timeseries",
                "vardemangder",
                # valid_dates intentionally omitted
            ),
        )
        with pytest.raises(RegMetaError) as exc_info:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                skip_classifications=True,
                skip_slugs=True,
            )
        assert exc_info.value.code == "csv_missing_validity"
        assert "VardemangderValidDates.csv" in exc_info.value.message


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


def _projection_input(tmp_path: Path, vardemangder_rows, valid_dates_rows) -> Path:
    """Helper: write SCB fixture with custom Vardemangder + ValidDates rows
    for projection-correctness tests. Returns input dir."""
    input_dir = tmp_path / "input"
    write_scb_input(
        input_dir,
        vardemangder_rows=vardemangder_rows,
        valid_dates_rows=valid_dates_rows,
    )
    return input_dir


def _projected_codes(
    conn: sqlite3.Connection, *, register_id: int, var_id: int, year: int
) -> list[str]:
    """A2.7: the projected code list for a variable's value-set, read through
    `variable_state` (was `variable_instance.value_set_id` per cvid — that table
    is dropped before ship). Picks the state covering `year`. The projection
    tests build a single coded cvid per variable, so exactly one state carries a
    value_set; this returns its codes (sorted, NULLs dropped)."""
    rows = conn.execute(
        "SELECT vc.code FROM variable_state vs "
        "JOIN variable v ON vs.variable_id = v.variable_id "
        "LEFT JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
        "LEFT JOIN value_code vc ON vsm.code_id = vc.code_id "
        "WHERE v.register_id = ? AND CAST(v.provider_key AS INTEGER) = ? "
        "  AND CAST(substr(vs.valid_from, 1, 4) AS INTEGER) <= ? "
        "  AND CAST(substr(vs.valid_to, 1, 4) AS INTEGER) >= ? "
        "ORDER BY vc.code",
        (register_id, var_id, year, year),
    ).fetchall()
    return [r["code"] for r in rows if r["code"] is not None]


class TestYearProjection:
    """Each test builds a DB with a tailored Vardemangder + ValidDates fixture
    and asserts what survives projection."""

    def test_excludes_out_of_window(self, tmp_path: Path):
        # cvid 1001 has year 2020. Item 8000 has validity 2030-2099, which
        # does not cover 2020 — Man must be excluded.
        rows = [
            PIPE.join(["Kön", "1", "1", "Man-future", "1001", "8000"]),
            PIPE.join(["Kön", "1", "2", "Kvinna", "1001", ""]),
        ]
        valid = [PIPE.join(["8000", "2030-01-01", "2099-12-31"])]
        db_dir = tmp_path / "db"
        build_db(
            input_dir=_projection_input(tmp_path, rows, valid),
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = open_db(db_dir / "reg_meta.db")
        codes = _projected_codes(conn, register_id=1, var_id=44, year=2020)
        conn.close()
        # Man is excluded (window 2030+ doesn't cover cvid year 2020).
        # Kvinna is included (untracked → always-valid).
        assert codes == ["2"]

    def test_includes_codes_with_no_validity(self, tmp_path: Path):
        # All Vardemangder ItemIds are untracked (none has a row in
        # VardemangderValidDates.csv) → every union pair is "always valid".
        rows = [
            PIPE.join(["Kön", "1", "1", "Man", "1001", "8001"]),
            PIPE.join(["Kön", "1", "2", "Kvinna", "1001", "8002"]),
        ]
        # Single placeholder row for an unrelated item_id keeps the CSV valid.
        valid = [PIPE.join(["99999", "2000-01-01", "2099-12-31"])]
        db_dir = tmp_path / "db"
        build_db(
            input_dir=_projection_input(tmp_path, rows, valid),
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = open_db(db_dir / "reg_meta.db")
        codes = _projected_codes(conn, register_id=1, var_id=44, year=2020)
        conn.close()
        assert codes == ["1", "2"]

    def test_includes_subyear_overlap(self, tmp_path: Path):
        # cvid 1001 year=2020. Item with start 2020-09-01 — sub-year cutoff
        # but the year 2020 still overlaps the window.
        rows = [
            PIPE.join(["Kön", "1", "1", "Man", "1001", "8003"]),
        ]
        valid = [PIPE.join(["8003", "2020-09-01", "2030-12-31"])]
        db_dir = tmp_path / "db"
        build_db(
            input_dir=_projection_input(tmp_path, rows, valid),
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = open_db(db_dir / "reg_meta.db")
        codes = _projected_codes(conn, register_id=1, var_id=44, year=2020)
        conn.close()
        assert codes == ["1"]

    def test_yearless_cvid_includes_all_union_pairs(self, tmp_path: Path):
        # cvid 9001's regver name "Person-År" has no extractable year. The
        # projection rule's yearless fallback must include the code even
        # though the tracked window 2030-2099 covers no plausible year.
        yearless_row = _ri_row(
            "TESTREG",
            "Testregistret",
            "Testning",
            "Personer",
            "Personer",
            "Alla personer",
            "Nej",
            "Person-År",  # versionname — no year token
            "Personer per år",
            "",
            "Godkänd",
            "2020-01-01",
            "2020-12-31",
            "Hela befolkningen",
            "Alla personer",
            "",
            "2020-12-31",
            "Person",
            "Fysisk person",
            "Kön",
            "Personens kön",
            "Kön enligt folkbokföring",
            "",
            "",
            "",
            "",
            "",
            "",
            "Kon",
            "int",
            "1",
            "9001",  # cvid
            "9",
            "90",
            "900",
            "44",
        )
        ri_rows = list(REGISTERINFORMATION_ROWS) + [yearless_row]
        vm_rows = [PIPE.join(["Kön", "1", "1", "Man", "9001", "8006"])]
        valid = [PIPE.join(["8006", "2030-01-01", "2099-12-31"])]
        input_dir = tmp_path / "input"
        write_scb_input(
            input_dir,
            registerinformation_rows=ri_rows,
            vardemangder_rows=vm_rows,
            valid_dates_rows=valid,
        )
        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = open_db(db_dir / "reg_meta.db")
        # The yearless cvid 9001 lands in its own synthetic register (id 9,
        # variant 90, from `yearless_row`) and coalesces to a state with the
        # 0001..9999 fallback window, so any probe year selects it.
        codes = _projected_codes(conn, register_id=9, var_id=44, year=2020)
        conn.close()
        # Yearless cvids fall back to the historical union — the tracked
        # window's exclusion does NOT apply because there's no year to test.
        assert codes == ["1"]

    def test_mixed_tracked_untracked_tracked_wins(self, tmp_path: Path):
        # cvid 1001 year=2020. Same (cvid, code) appears with TWO ItemIds:
        # one tracked (validity 2030+), one untracked. The conservative rule:
        # tracked window decides; untracked sibling does NOT relax the
        # constraint. Code must be EXCLUDED.
        rows = [
            PIPE.join(["Kön", "1", "1", "Man", "1001", "8004"]),  # tracked
            PIPE.join(["Kön", "1", "1", "Man", "1001", "8005"]),  # untracked
        ]
        valid = [PIPE.join(["8004", "2030-01-01", "2099-12-31"])]
        db_dir = tmp_path / "db"
        build_db(
            input_dir=_projection_input(tmp_path, rows, valid),
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = open_db(db_dir / "reg_meta.db")
        # Man should NOT be in the 2020 state's value_set (tracked window 2030+
        # doesn't cover year 2020; the untracked sibling 8005 doesn't relax it).
        kods = _projected_codes(conn, register_id=1, var_id=44, year=2020)
        conn.close()
        # Either no codes (value_set_id NULL because all union excluded), or the
        # LEFT JOIN yields NULL. The "Man" code must not appear.
        assert "1" not in kods


class TestSameAsBuildIntegration:
    """End-to-end coverage that `build_db` correctly wires up
    `materialize_same_as_edges`: tables created, populated when the
    slug TOML carries `same_as`, skipped under `--skip-slugs`, and the
    resolver can traverse against the resulting DB."""

    def test_tables_present_in_fixture_db(self, db_conn: sqlite3.Connection) -> None:
        # Fixture has no same_as entries; the tables should still exist
        # (created by the schema DDL) and be empty.
        tables = {
            r[0]
            for r in db_conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name IN ('variable_same_as', 'classification_same_as')"
            ).fetchall()
        }
        assert tables == {"variable_same_as", "classification_same_as"}
        var_count = db_conn.execute("SELECT COUNT(*) FROM variable_same_as").fetchone()[
            0
        ]
        cls_count = db_conn.execute(
            "SELECT COUNT(*) FROM classification_same_as"
        ).fetchone()[0]
        assert (var_count, cls_count) == (0, 0)

    @staticmethod
    def _write_slug_dir_with_same_as(slug_dir: Path) -> None:
        """Slug TOML mirroring the shared fixture's two registers, plus a
        same_as edge from TESTREG's `kon` (var_id 44, real) to a phantom
        slug `legacy-kon`. Querying the phantom under TESTREG misses
        directly and forces a BFS hop onto the real `kon` row."""
        (slug_dir / "scb.toml").write_text(
            '[register."1"]\nslug = "testreg"\n'
            '[register."2"]\nslug = "otherreg"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            '[register_variant."2.20"]\nslug = "foretag"\n'
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "testreg", '
            'variable_slug = "legacy-kon" }]\n',
            encoding="utf-8",
        )
        (slug_dir / "classifications.toml").write_text("", encoding="utf-8")

    def test_end_to_end_resolves_via_same_as(self, tmp_path: Path) -> None:
        from reg_meta.catalog import Catalog, ResolvedVariable

        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        write_scb_input(input_dir)
        self._write_slug_dir_with_same_as(slug_dir)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            slug_dir=slug_dir,
        )
        conn = open_db(db_dir / "reg_meta.db")
        try:
            # 1 TOML edge → 2 DB rows.
            rows = conn.execute(
                "SELECT a_variable, b_variable FROM variable_same_as "
                "ORDER BY a_variable"
            ).fetchall()
            assert [(r[0], r[1]) for r in rows] == [
                ("kon", "legacy-kon"),
                ("legacy-kon", "kon"),
            ]
            # Querying the phantom slug misses directly → BFS traverses to
            # the real `kon` row. via_same_as carries the traversal path,
            # confirming the build wired materialize_same_as_edges into
            # the resolver's data plane end-to-end.
            r = Catalog(conn).resolve("scb/testreg/legacy-kon")
            assert isinstance(r, ResolvedVariable)
            assert r.via_same_as is not None
            assert len(r.via_same_as) == 1
            assert str(r.via_same_as[0]) == "scb/testreg/kon"
            # Caller's FQID preserved on the returned record.
            assert str(r.fqid) == "scb/testreg/legacy-kon"
            # A2.5 longitudinal: the delivery column lives on the state, not the
            # (now-removed) per-edition binding.
            assert any(s.delivery_column_name == "Kon" for s in r.states)
        finally:
            conn.close()

    def test_skip_slugs_skips_same_as_materialization(self, tmp_path: Path) -> None:
        # Build with skip_slugs=True and a same_as TOML present. The
        # materializer would hard-error on missing register slugs, so
        # build_db's `if skip_slugs` branch must skip it entirely. We
        # verify by building successfully and confirming both edge tables
        # stayed empty.
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        write_scb_input(input_dir)
        self._write_slug_dir_with_same_as(slug_dir)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
            slug_dir=slug_dir,
        )
        conn = open_db(db_dir / "reg_meta.db")
        try:
            var_count = conn.execute(
                "SELECT COUNT(*) FROM variable_same_as"
            ).fetchone()[0]
            cls_count = conn.execute(
                "SELECT COUNT(*) FROM classification_same_as"
            ).fetchone()[0]
            assert (var_count, cls_count) == (0, 0)
        finally:
            conn.close()


class TestOperationalDefinitionFold:
    """SCB ships `VariabelOperationell_definition` as a refinement
    over `Variabelbeskrivning`. The Model A schema drops the dedicated column
    and folds the operational definition into `description` when it's distinct
    and non-empty. These tests lock the fold contract — empty op stays empty,
    duplicate op is skipped, distinct op appends with a blank-line separator,
    and a re-built DB doesn't double-fold (the `op not in desc` substring guard).
    """

    @staticmethod
    def _build_and_fetch(tmp_path: Path, *, vardesc: str, varopdef: str) -> str:
        """Build a minimal DB with one variable carrying the given description
        and operational definition; return the persisted `variable.description`.
        """
        ri_row = _ri_row(
            "FOLDREG",
            "Foldregistret",
            "Folding test",
            "Individer",
            "Individer",
            "Alla individer",
            "Nej",
            "2020",
            "Version 2020",
            "",
            "Godkänd",
            "2020-01-01",
            "2020-12-31",
            "Hela befolkningen",
            "Alla personer",
            "",
            "2020-12-31",
            "Person",
            "Fysisk person",
            "FoldVar",
            "Definition",
            vardesc,
            varopdef,
            "",
            "",
            "",
            "",
            "",
            "FoldCol",
            "int",
            "1",
            "9001",
            "999",
            "9001",
            "9001",
            "777",
        )
        # `REGISTERINFORMATION_ROWS` carries the baseline fixtures the build
        # pipeline expects to find (registers referenced by `unika_summary`
        # etc.). Append our row instead of replacing — the build is strict.
        ri_rows = list(REGISTERINFORMATION_ROWS) + [ri_row]
        input_dir = tmp_path / "input"
        write_scb_input(input_dir, registerinformation_rows=ri_rows)
        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = open_db(db_dir / "reg_meta.db")
        try:
            row = conn.execute(
                "SELECT description FROM variable "
                "WHERE register_id = 999 AND provider_key = '777'"
            ).fetchone()
            return row["description"]
        finally:
            conn.close()

    def test_empty_operational_definition_leaves_description_untouched(
        self, tmp_path: Path
    ):
        """Empty `VariabelOperationell_definition` → description unchanged."""
        result = self._build_and_fetch(
            tmp_path, vardesc="Plain description", varopdef=""
        )
        assert result == "Plain description"

    def test_distinct_operational_definition_appended_with_blank_line(
        self, tmp_path: Path
    ):
        """Non-empty op_def distinct from desc → appended with `\\n\\n` separator."""
        result = self._build_and_fetch(
            tmp_path,
            vardesc="Plain description",
            varopdef="Operational refinement",
        )
        assert result == "Plain description\n\nOperational refinement"

    def test_duplicate_operational_definition_not_appended(self, tmp_path: Path):
        """When `op == desc` exactly, the fold is skipped (no `desc\\n\\ndesc`)."""
        result = self._build_and_fetch(
            tmp_path,
            vardesc="Same content",
            varopdef="Same content",
        )
        assert result == "Same content"

    def test_substring_operational_definition_not_appended(self, tmp_path: Path):
        """When `op` is already a substring of `desc`, the fold must skip —
        otherwise rebuilds (which can see a `description` already carrying
        the merged operational text) would accumulate duplicated tails.
        This is the `op not in desc` guard. Pipe-delimited CSV doesn't
        carry literal newlines, so we use a single-line description that
        embeds the operational text verbatim — the substring relationship
        is what the guard actually checks."""
        result = self._build_and_fetch(
            tmp_path,
            vardesc="Operational refinement is part of this longer description",
            varopdef="Operational refinement",
        )
        assert result == "Operational refinement is part of this longer description"

    def test_operational_definition_with_empty_description(self, tmp_path: Path):
        """When desc is empty but op is set, the merged value is just op
        (no leading `\\n\\n`)."""
        result = self._build_and_fetch(
            tmp_path,
            vardesc="",
            varopdef="Operational refinement",
        )
        assert result == "Operational refinement"


class TestVariableStateOpenEnded:
    """A2.1: SCB's `VersionSista` is the upper bound of a variable's
    validity. When SCB leaves the cell blank, that means "still active"
    — the coalescer must preserve that signal as the open-ended sentinel
    `valid_to = '9999-12-31'`, NOT fall back to the latest observed
    `register_version` year. Falling back would silently clamp a
    currently-live variable to the latest observed export year and break
    A2.5's period resolver for any future-period query.

    Codex caught this on PR #130: my initial implementation took
    `unika_max if not None else regver_max`, which lost the open-ended
    semantics whenever any unika row matched the group with a blank
    `VersionSista`. The fix tracks an `unika_matched` sticky bit
    distinct from `unika_max is None`, so the regver fallback only fires
    when unika never spoke for the group at all.
    """

    @staticmethod
    def _build_with_open_unika(tmp_path: Path) -> Path:
        """Build a minimal DB whose unika fixture has one row with blank
        VersionSista, plus a baseline row with both bounds populated for
        comparison."""
        from _csv_fixtures import (
            IDENTIFIERARE_HEADER,
            IDENTIFIERARE_ROWS,
            PIPE,
            REGISTERINFORMATION_HEADER,
            REGISTERINFORMATION_ROWS,
            TIMESERIES_HEADER,
            TIMESERIES_ROWS,
            UNIKA_HEADER,
            VALID_DATES_HEADER,
            VALID_DATES_ROWS,
            VARDEMANGDER_HEADER,
            VARDEMANGDER_ROWS,
            _ri_row,
            write_csv,
        )

        # Append one extra variable to Registerinformation: register_id=1,
        # var_id=900 ("StillActiveVar"). Living in TESTREG/register_variant 10 to
        # avoid creating a new variant.
        open_ri = _ri_row(
            "TESTREG",
            "Testregistret",
            "Testning",
            "Individer",
            "Individer",
            "Alla individer",
            "Nej",
            "2020",  # SCB's earliest version where StillActiveVar appears
            "Version 2020",
            "",
            "Godkänd",
            "2020-01-01",
            "2020-12-31",
            "Hela befolkningen",
            "Alla personer",
            "",
            "2020-12-31",
            "Person",
            "Fysisk person",
            "StillActiveVar",
            "A variable that is still being collected",
            "Active variable description",
            "",
            "",
            "",
            "",
            "",
            "",
            "ActiveCol",
            "varchar",
            "10",
            "9100",  # cvid
            "1",  # register_id
            "10",  # register_variant_id (existing Individer variant in TESTREG)
            "100",  # regver_id (existing 2020 version)
            "900",  # var_id
        )
        ri_rows = list(REGISTERINFORMATION_ROWS) + [open_ri]

        # Custom unika fixture: standard rows + one with blank VersionSista
        # for our StillActiveVar. The blank-VersionSista row is what
        # exercises the bug-fix path.
        unika_rows = [
            PIPE.join(
                [
                    "TESTREG",
                    "Testregistret",
                    "Individer",
                    "Individer",
                    "Kön",
                    "Kon",
                    "2020",
                    "2022",
                    "Nej",
                    "Nej",
                    "Nej",
                ]
            ),
            PIPE.join(
                [
                    "TESTREG",
                    "Testregistret",
                    "Individer",
                    "Individer",
                    "StillActiveVar",
                    "ActiveCol",
                    "2020",
                    "",  # ← open-ended: SCB hasn't sunset this variable
                    "Nej",
                    "Nej",
                    "Nej",
                ]
            ),
        ]

        input_dir = tmp_path / "input"
        scb = input_dir / "SCB"
        scb.mkdir(parents=True, exist_ok=True)
        write_csv(scb / "Registerinformation.csv", REGISTERINFORMATION_HEADER, ri_rows)
        write_csv(scb / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, unika_rows)
        write_csv(scb / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS)
        write_csv(scb / "Timeseries.csv", TIMESERIES_HEADER, TIMESERIES_ROWS)
        write_csv(scb / "Vardemangder.csv", VARDEMANGDER_HEADER, VARDEMANGDER_ROWS)
        write_csv(
            scb / "VardemangderValidDates.csv", VALID_DATES_HEADER, VALID_DATES_ROWS
        )

        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        return db_dir / "reg_meta.db"

    def test_open_ended_unika_preserves_sentinel(self, tmp_path: Path):
        """StillActiveVar's unika row has VersionForsta='2020' and
        VersionSista=''. Expected: valid_from='2020-01-01',
        valid_to='9999-12-31'. The sentinel signals "still active" to
        the A2.5 resolver. If this regressed, future-period queries
        would silently miss the row."""
        db_path = self._build_with_open_unika(tmp_path)
        conn = open_db(db_path)
        try:
            row = conn.execute(
                "SELECT valid_from, valid_to FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '900'"
            ).fetchone()
            assert row is not None
            assert row["valid_from"] == "2020-01-01"
            assert row["valid_to"] == "9999-12-31"
        finally:
            conn.close()

    def test_open_ended_increments_manifest_stat(self, tmp_path: Path):
        """A2.1: `coalesce_stats.n_open_top_from_unika` counts states whose
        upper bound came from a unika row that left VersionSista blank.
        With exactly one such row in this fixture (StillActiveVar), the
        counter must be at least 1."""
        import json as _json

        db_path = self._build_with_open_unika(tmp_path)
        conn = open_db(db_path)
        try:
            manifest = conn.execute(
                "SELECT value FROM import_manifest WHERE key = 'coalesce_stats'"
            ).fetchone()
            assert manifest is not None
            stats = _json.loads(manifest["value"])
            assert stats["n_open_top_from_unika"] >= 1
        finally:
            conn.close()


class TestVariableStateMultiShape:
    """A2.1: Codex P1 on PR #130 — when a variable changes shape across
    versions (different data_type / data_length / value_set_id), the
    coalescer splits it into multiple groups. Each group must claim
    only its OWN observed years, not the full unika lifetime. Without
    this clamp, every shape group would inherit the variable's whole
    lifetime range and produce overlapping `variable_state` rows with
    no `value_set_version_label` discriminator — the A2.5 point
    resolver would return multiple states for periods where only one
    shape actually existed.

    The fix: use each group's `regver_min`/`regver_max` as the
    authoritative range; reserve the open-ended sentinel for the
    latest-era group (whose `regver_max` matches the variable's
    overall max regver year).
    """

    @staticmethod
    def _build_multi_shape(tmp_path: Path) -> Path:
        """Build a DB where var_id=910 ('ShiftingVar') changes shape:
        - regver_id=110 (year 2020): data_type='int', data_length='1'
        - regver_id=111 (year 2021): data_type='int', data_length='1'
        - regver_id=112 (year 2022): data_type='varchar', data_length='5'
        - regver_id=113 (year 2023): data_type='varchar', data_length='5'

        Unika carries one row: VersionForsta='2020', VersionSista='2023'.
        Buggy behavior would produce 2 states each spanning 2020-2023;
        correct behavior produces (2020-01-01, 2021-12-31) for the int
        era and (2022-01-01, 2023-12-31) for the varchar era."""
        from _csv_fixtures import (
            IDENTIFIERARE_HEADER,
            IDENTIFIERARE_ROWS,
            PIPE,
            REGISTERINFORMATION_HEADER,
            REGISTERINFORMATION_ROWS,
            TIMESERIES_HEADER,
            TIMESERIES_ROWS,
            UNIKA_HEADER,
            VALID_DATES_HEADER,
            VALID_DATES_ROWS,
            VARDEMANGDER_HEADER,
            VARDEMANGDER_ROWS,
            _ri_row,
            write_csv,
        )

        # Four ShiftingVar rows — two int eras, two varchar eras. No
        # Vardemangder entries so value_set_id stays NULL across all
        # four cvids; the shape distinction is purely data_type /
        # data_length.
        shifting_rows = []
        for year, regver_id, cvid, dt, dl in [
            ("2020", 110, 9200, "int", "1"),
            ("2021", 111, 9201, "int", "1"),
            ("2022", 112, 9202, "varchar", "5"),
            ("2023", 113, 9203, "varchar", "5"),
        ]:
            shifting_rows.append(
                _ri_row(
                    "TESTREG",
                    "Testregistret",
                    "Testning",
                    "Individer",
                    "Individer",
                    "Alla individer",
                    "Nej",
                    year,
                    f"Version {year}",
                    "",
                    "Godkänd",
                    f"{year}-01-01",
                    f"{year}-12-31",
                    "Hela befolkningen",
                    "Alla personer",
                    "",
                    f"{year}-12-31",
                    "Person",
                    "Fysisk person",
                    "ShiftingVar",
                    "A variable whose shape changes across eras",
                    "Mid-life shape drift",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "ShiftCol",
                    dt,
                    dl,
                    str(cvid),
                    "1",
                    "10",
                    str(regver_id),
                    "910",
                )
            )

        ri_rows = list(REGISTERINFORMATION_ROWS) + shifting_rows
        unika_rows = [
            # ShiftingVar's single unika row covers the full lifetime.
            # The coalescer must NOT fan this 2020-2023 range out to
            # every shape group.
            PIPE.join(
                [
                    "TESTREG",
                    "Testregistret",
                    "Individer",
                    "Individer",
                    "ShiftingVar",
                    "ShiftCol",
                    "2020",
                    "2023",
                    "Nej",
                    "Nej",
                    "Nej",
                ]
            ),
        ]

        input_dir = tmp_path / "input"
        scb = input_dir / "SCB"
        scb.mkdir(parents=True, exist_ok=True)
        write_csv(scb / "Registerinformation.csv", REGISTERINFORMATION_HEADER, ri_rows)
        write_csv(scb / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, unika_rows)
        write_csv(scb / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS)
        write_csv(scb / "Timeseries.csv", TIMESERIES_HEADER, TIMESERIES_ROWS)
        write_csv(scb / "Vardemangder.csv", VARDEMANGDER_HEADER, VARDEMANGDER_ROWS)
        write_csv(
            scb / "VardemangderValidDates.csv", VALID_DATES_HEADER, VALID_DATES_ROWS
        )

        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        return db_dir / "reg_meta.db"

    def test_groups_clamped_to_observed_years(self, tmp_path: Path):
        """ShiftingVar splits into int (2020-2021) and varchar (2022-2023)
        groups. Each must report ONLY its observed years, not the full
        unika lifetime (2020-2023). Pre-fix, both rows would have spanned
        2020-2023 and overlapped on every year.

        The latest-era group (varchar, regver_max=2023=var_max) has a
        bounded unika row (`VersionSista='2023'`), so it does NOT carry
        the open-ended sentinel — its valid_to is 2023-12-31."""
        db_path = self._build_multi_shape(tmp_path)
        conn = open_db(db_path)
        try:
            rows = conn.execute(
                "SELECT data_type, data_length, valid_from, valid_to "
                "FROM variable_state vs JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '910' "
                "ORDER BY valid_from"
            ).fetchall()
            assert len(rows) == 2
            int_era, varchar_era = rows[0], rows[1]
            assert int_era["data_type"] == "int"
            assert int_era["data_length"] == "1"
            assert int_era["valid_from"] == "2020-01-01"
            assert int_era["valid_to"] == "2021-12-31"
            assert varchar_era["data_type"] == "varchar"
            assert varchar_era["data_length"] == "5"
            assert varchar_era["valid_from"] == "2022-01-01"
            assert varchar_era["valid_to"] == "2023-12-31"
        finally:
            conn.close()

    def test_groups_non_overlapping(self, tmp_path: Path):
        """Invariant: variable_state rows for the same variable are
        non-overlapping unless explicitly discriminated by
        value_set_version_label. Multi-shape ShiftingVar has no
        discriminator, so its two states must NOT overlap. This is the
        load-bearing property A2.5's point resolver relies on."""
        db_path = self._build_multi_shape(tmp_path)
        conn = open_db(db_path)
        try:
            rows = conn.execute(
                "SELECT valid_from, valid_to, value_set_version_label "
                "FROM variable_state vs JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '910' "
                "ORDER BY valid_from"
            ).fetchall()
            assert len(rows) == 2
            # A2.1.5: value_set_version_label is NOT NULL DEFAULT '' (the
            # coalescer coalesces NULL → ''), so undiscriminated states carry ''.
            assert rows[0]["value_set_version_label"] == ""
            assert rows[1]["value_set_version_label"] == ""
            # Strict non-overlap: row 0 ends strictly before row 1 starts.
            # Lexical comparison is chronological for full-date ISO strings.
            assert rows[0]["valid_to"] < rows[1]["valid_from"]
        finally:
            conn.close()


class TestVariableStateTypeFold:
    """#526: SCB's per-delivery `Datatyp`/`Datalängd` is low-trust passthrough;
    the value set is the reliable categorical signal. The coalescer no longer
    splits a state on type/length wobble when the VALUE SET is stable — a
    char↔varchar flip, a length-only change, or an SCB-error class flip
    (float(53)-on-categorical) all fold into ONE `variable_state`. Valueless
    columns keep type+length as their only shape signal, but with the text
    family (char/varchar/n…) canonicalized so a char↔varchar wobble still folds
    while a genuine class flip (date→int) splits.
    """

    @staticmethod
    def _build(
        tmp_path: Path,
        eras: list[tuple[str, int, int, str, str, list[tuple[str, str]] | None]],
        *,
        var_id: int = 920,
        column: str = "FoldCol",
        varname: str = "FoldVar",
    ) -> Path:
        """Build a DB with ONE fresh variable delivered across `eras`. Each era
        is (year, regver_id, cvid, data_type, data_length, value_members) where
        `value_members` is None for a valueless delivery, else a list of
        (kod, label) pairs. Same members across eras ⇒ same `value_set_id`
        (content-hash dedup), so those eras share a value set. Value codes use an
        UNTRACKED ItemID (no VardemangderValidDates row) ⇒ always-valid ⇒ the
        instance keeps a non-NULL value_set_id."""
        ri_rows: list[str] = list(REGISTERINFORMATION_ROWS)
        vardemangder_rows: list[str] = list(VARDEMANGDER_ROWS)
        for year, regver_id, cvid, dt, dl, members in eras:
            ri_rows.append(
                _ri_row(
                    "TESTREG",
                    "Testregistret",
                    "Testning",
                    "Individer",
                    "Individer",
                    "Alla individer",
                    "Nej",
                    year,
                    f"Version {year}",
                    "",
                    "Godkänd",
                    f"{year}-01-01",
                    f"{year}-12-31",
                    "Hela befolkningen",
                    "Alla personer",
                    "",
                    f"{year}-12-31",
                    "Person",
                    "Fysisk person",
                    varname,
                    "A variable exercising the #526 type-fold",
                    "Type-fold fixture",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    column,
                    dt,
                    dl,
                    str(cvid),
                    "1",
                    "10",
                    str(regver_id),
                    str(var_id),
                )
            )
            if members is not None:
                for kod, label in members:
                    # Untracked ItemID "" ⇒ always-valid (no projection cutoff).
                    vardemangder_rows.append(
                        PIPE.join([varname, "1", kod, label, str(cvid), ""])
                    )

        # One bounded unika row spanning the full lifetime; the coalescer must
        # NOT fan it out across folded eras.
        years = [e[0] for e in eras]
        unika_rows = list(UNIKA_ROWS) + [
            PIPE.join(
                [
                    "TESTREG",
                    "Testregistret",
                    "Individer",
                    "Individer",
                    varname,
                    column,
                    min(years),
                    max(years),
                    "Nej",
                    "Nej",
                    "Nej",
                ]
            )
        ]

        input_dir = tmp_path / "input"
        write_scb_input(
            input_dir,
            registerinformation_rows=ri_rows,
            vardemangder_rows=vardemangder_rows,
            unika_rows=unika_rows,
        )
        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        return db_dir / "reg_meta.db"

    @staticmethod
    def _states(conn: sqlite3.Connection, var_id: int) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT data_type, data_length, value_set_id, valid_from, valid_to "
            "FROM variable_state vs JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = ? "
            "ORDER BY valid_from",
            (str(var_id),),
        ).fetchall()

    @staticmethod
    def _coalesce_stats(conn: sqlite3.Connection) -> dict:
        row = conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'coalesce_stats'"
        ).fetchone()
        assert row is not None
        return json.loads(row["value"])

    _MAN_KVINNA = [("1", "Man"), ("2", "Kvinna")]

    def test_value_set_anchored_char_varchar_folds(self, tmp_path: Path):
        """The reported case: same variable, two editions, SAME value set,
        data_type varchar→char at the SAME length ⇒ exactly ONE state."""
        db_path = self._build(
            tmp_path,
            eras=[
                ("2020", 920, 9300, "varchar", "1", self._MAN_KVINNA),
                ("2021", 921, 9301, "char", "1", self._MAN_KVINNA),
            ],
        )
        conn = open_db(db_path)
        try:
            states = self._states(conn, 920)
            assert len(states) == 1
            assert states[0]["value_set_id"] is not None
            # Folded lifetime spans both editions.
            assert states[0]["valid_from"] == "2020-01-01"
            assert states[0]["valid_to"] == "2021-12-31"
        finally:
            conn.close()

    def test_value_set_anchored_length_only_folds(self, tmp_path: Path):
        """Length-only change under a stable value set ⇒ ONE state."""
        db_path = self._build(
            tmp_path,
            eras=[
                ("2020", 920, 9300, "varchar", "2", self._MAN_KVINNA),
                ("2021", 921, 9301, "varchar", "8", self._MAN_KVINNA),
            ],
        )
        conn = open_db(db_path)
        try:
            assert len(self._states(conn, 920)) == 1
        finally:
            conn.close()

    def test_value_set_anchored_class_flip_folds_and_counts(self, tmp_path: Path):
        """An SCB-error class flip (varchar→float) under a stable value set ⇒
        ONE state, and the manifest's `n_type_class_folds` counts it."""
        db_path = self._build(
            tmp_path,
            eras=[
                ("2020", 920, 9300, "varchar", "1", self._MAN_KVINNA),
                ("2021", 921, 9301, "float", "53", self._MAN_KVINNA),
            ],
        )
        conn = open_db(db_path)
        try:
            assert len(self._states(conn, 920)) == 1
            stats = self._coalesce_stats(conn)
            assert stats["n_type_folds"] >= 1
            assert stats["n_type_class_folds"] >= 1
        finally:
            conn.close()

    def test_valueless_class_change_still_splits(self, tmp_path: Path):
        """A valueless variable with a genuine class change (char→int, no value
        set) keeps its split: type is the only shape signal there ⇒ TWO states.
        `n_type_class_folds` does NOT count valueless groups."""
        db_path = self._build(
            tmp_path,
            eras=[
                ("2020", 920, 9300, "char", "8", None),
                ("2021", 921, 9301, "int", "0", None),
            ],
        )
        conn = open_db(db_path)
        try:
            states = self._states(conn, 920)
            assert len(states) == 2
            assert all(s["value_set_id"] is None for s in states)
            stats = self._coalesce_stats(conn)
            assert stats["n_type_class_folds"] == 0
        finally:
            conn.close()

    def test_valueless_char_varchar_same_length_folds(self, tmp_path: Path):
        """Valueless char↔varchar at the same length folds to ONE state — the
        text family canonicalizes even on the valueless path."""
        db_path = self._build(
            tmp_path,
            eras=[
                ("2020", 920, 9300, "char", "1", None),
                ("2021", 921, 9301, "varchar", "1", None),
            ],
        )
        conn = open_db(db_path)
        try:
            states = self._states(conn, 920)
            assert len(states) == 1
            assert states[0]["value_set_id"] is None
        finally:
            conn.close()

    def test_latest_era_type_displayed(self, tmp_path: Path):
        """The surviving merged state's displayed data_type is the LATEST
        edition's delivery type (highest regver_id), not the first row's."""
        db_path = self._build(
            tmp_path,
            eras=[
                ("2020", 920, 9300, "varchar", "1", self._MAN_KVINNA),
                ("2021", 921, 9301, "char", "1", self._MAN_KVINNA),
            ],
        )
        conn = open_db(db_path)
        try:
            states = self._states(conn, 920)
            assert len(states) == 1
            # regver_id 921 (char) is the latest era → char wins over varchar.
            assert states[0]["data_type"] == "char"
            assert states[0]["data_length"] == "1"
        finally:
            conn.close()


class TestVariableStateRenameMidLife:
    """A2.1: Codex P2 on PR #130, commit d8d8125 — when SCB renames a
    variable mid-life (Variabelnamn changes between editions for the
    same VarId), `unika_summary` carries one row per
    (register_id, register_variant_id, kolumnnamn, variabelnamn) tuple. The
    canonical `variable.name` is the first-non-empty Variabelnamn the
    importer sees, which pins to the OLD name. The coalescer's unika
    lookup uses the per-cvid raw `variabelnamn` (stored on
    variable_instance for this purpose) so the post-rename unika row
    — often the still-active one with blank VersionSista — actually
    matches and the open-ended sentinel propagates onto the latest-era
    state row.
    """

    @staticmethod
    def _build_with_rename(tmp_path: Path) -> Path:
        """Build a DB where var_id=920 (RenamedVar) has its Variabelnamn
        change between editions:
        - 2020 (regver=120): Variabelnamn='OriginalName', ColX
        - 2024 (regver=121): Variabelnamn='RenamedName', ColX (open-ended,
          still active per SCB)
        Unika carries two rows — one per Variabelnamn:
        - ('ColX', 'OriginalName'): 2020-2020 (the renamed-away era)
        - ('ColX', 'RenamedName'): 2024-blank (the currently-active era)
        """
        from _csv_fixtures import (
            IDENTIFIERARE_HEADER,
            IDENTIFIERARE_ROWS,
            PIPE,
            REGISTERINFORMATION_HEADER,
            REGISTERINFORMATION_ROWS,
            TIMESERIES_HEADER,
            TIMESERIES_ROWS,
            UNIKA_HEADER,
            VALID_DATES_HEADER,
            VALID_DATES_ROWS,
            VARDEMANGDER_HEADER,
            VARDEMANGDER_ROWS,
            _ri_row,
            write_csv,
        )

        rename_rows = []
        for year, regver_id, cvid, varname in [
            ("2020", 120, 9300, "OriginalName"),
            ("2024", 121, 9301, "RenamedName"),
        ]:
            rename_rows.append(
                _ri_row(
                    "TESTREG",
                    "Testregistret",
                    "Testning",
                    "Individer",
                    "Individer",
                    "Alla individer",
                    "Nej",
                    year,
                    f"Version {year}",
                    "",
                    "Godkänd",
                    f"{year}-01-01",
                    f"{year}-12-31",
                    "Hela befolkningen",
                    "Alla personer",
                    "",
                    f"{year}-12-31",
                    "Person",
                    "Fysisk person",
                    varname,  # ← Variabelnamn changes across editions
                    "A renamed variable",
                    "Mid-life rename",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "ColX",  # Same delivery column throughout
                    "int",
                    "1",
                    str(cvid),
                    "1",
                    "10",
                    str(regver_id),
                    "920",
                )
            )

        ri_rows = list(REGISTERINFORMATION_ROWS) + rename_rows
        unika_rows = [
            # The renamed-away era — bounded
            PIPE.join(
                [
                    "TESTREG",
                    "Testregistret",
                    "Individer",
                    "Individer",
                    "OriginalName",
                    "ColX",
                    "2020",
                    "2020",
                    "Nej",
                    "Nej",
                    "Nej",
                ]
            ),
            # The still-active renamed era — open-ended (blank VersionSista)
            PIPE.join(
                [
                    "TESTREG",
                    "Testregistret",
                    "Individer",
                    "Individer",
                    "RenamedName",
                    "ColX",
                    "2024",
                    "",  # ← blank: still active
                    "Nej",
                    "Nej",
                    "Nej",
                ]
            ),
        ]

        input_dir = tmp_path / "input"
        scb = input_dir / "SCB"
        scb.mkdir(parents=True, exist_ok=True)
        write_csv(scb / "Registerinformation.csv", REGISTERINFORMATION_HEADER, ri_rows)
        write_csv(scb / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, unika_rows)
        write_csv(scb / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS)
        write_csv(scb / "Timeseries.csv", TIMESERIES_HEADER, TIMESERIES_ROWS)
        write_csv(scb / "Vardemangder.csv", VARDEMANGDER_HEADER, VARDEMANGDER_ROWS)
        write_csv(
            scb / "VardemangderValidDates.csv", VALID_DATES_HEADER, VALID_DATES_ROWS
        )

        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        return db_dir / "reg_meta.db"

    def test_rename_preserves_open_ended_sentinel(self, tmp_path: Path):
        """Latest-era state for the renamed variable picks up the
        `RenamedName` unika row's blank `VersionSista` → sentinel.
        Pre-fix, the coalescer joined on the canonical `variable.name`
        (pinned to `OriginalName` by first-non-empty), so the lookup
        against `unika_summary` keyed on `RenamedName` missed and the
        state fell back to `regver_max = 2024` instead of the open-ended
        sentinel."""
        db_path = self._build_with_rename(tmp_path)
        conn = open_db(db_path)
        try:
            # The variable's canonical name (first-non-empty) is 'OriginalName'
            # since the 2020 row is processed first. The current shape (still
            # active in 2024) is the latest era — single group since shape
            # didn't change, just the name. Latest era → open-ended.
            row = conn.execute(
                "SELECT valid_from, valid_to FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '920'"
            ).fetchone()
            assert row is not None
            assert row["valid_from"] == "2020-01-01"
            # Without the per-cvid variabelnamn fix, this would be
            # '2024-12-31' (clamped to regver_max because the RenamedName
            # unika row missed the lookup).
            assert row["valid_to"] == "9999-12-31"
        finally:
            conn.close()


class TestReplacedByEdges:
    """Succession edges materialized from `timeseries_event`.

    Reworked onto the two-level model: `variable_replaced_by` is **variable
    grain** — 3-part `(provider, register, variable)` endpoints, no variant —
    and the variable slot carries the STORED `variable.slug`, not
    a build-time derivation. `register_replaced_by` / `variant_replaced_by` are
    unchanged from the original draft.

    Default fixture geometry (`_csv_fixtures.REGISTERINFORMATION_ROWS`), with the
    stored variable slugs `populate_variable_slugs` assigns:
      - register 1 = TESTREG ('testreg'), register 2 = OTHERREG ('otherreg')
      - variant 10 = TESTREG/Individer ('individer'),
        variant 20 = OTHERREG/Företag ('foretag')
      - variables: (reg1, var_id 44)→'kon', (reg1, 100)→'testcol',
        (reg1, 200)→'aaocol', (reg2, 44)→'kon', (reg2, 300)→'uniqcol',
        (reg2, 301)→'parencol', (reg2, 302)→'extcol'
      - var_id 44 (Kön) lives in BOTH registers (a cross-register ambiguity
        case); 100/200 are unique to reg1, 300/301/302 unique to reg2
      - cvids: 1002 = (reg1, var 100 → 'testcol'); 2002 = (reg2, var 300 →
        'uniqcol')
    """

    @staticmethod
    def _write_slugs(slug_dir: Path, scb_extra: str = "") -> None:
        """Minimal slug TOML for the two fixture registers + variants. Variable
        slugs auto-derive via `populate_variable_slugs` into scb.auto.toml.

        ``scb_extra`` is appended verbatim to ``scb.toml`` — used by the #440
        curated-`[[replaced_by]]` tests to inject succession rows.
        """
        (slug_dir / "scb.toml").write_text(
            '[register."1"]\nslug = "testreg"\n'
            '[register."2"]\nslug = "otherreg"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            '[register_variant."2.20"]\nslug = "foretag"\n' + scb_extra,
            encoding="utf-8",
        )
        (slug_dir / "classifications.toml").write_text("", encoding="utf-8")

    @classmethod
    def _build(
        cls,
        tmp_path: Path,
        timeseries_rows: list[str],
        registerinformation_rows: list[str] | None = None,
        scb_extra: str = "",
    ) -> Path:
        """Build a DB with custom Timeseries.csv rows. Returns the DB path.

        ``registerinformation_rows=None`` uses the standard fixture; pass a
        custom list (e.g. to inject a triage split) to vary the variables.
        ``scb_extra`` is appended to ``scb.toml`` (e.g. `[[replaced_by]]` rows).
        """
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        write_scb_input(
            input_dir,
            registerinformation_rows=registerinformation_rows,
            timeseries_rows=timeseries_rows,
        )
        cls._write_slugs(slug_dir, scb_extra=scb_extra)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            slug_dir=slug_dir,
        )
        return db_dir / "reg_meta.db"

    @staticmethod
    def _stats(conn: sqlite3.Connection) -> dict[str, int]:
        """Read the manifest `replaced_by_stats` blob."""
        row = conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'replaced_by_stats'"
        ).fetchone()
        assert row is not None
        return json.loads(row["value"])

    def test_register_ersatt_av_emits_register_replaced_by(
        self, tmp_path: Path
    ) -> None:
        """Register-grain Ersatt av → one register_replaced_by row, no
        variant/variable edges."""
        rows = [timeseries_row(entitet="Register", id1="1", id2="2")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_provider, predecessor_register, "
                "successor_provider, successor_register, effective_year, note "
                "FROM register_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "scb",
                "testreg",
                "scb",
                "otherreg",
                None,
                "auto:timeseries_event",
            )
            assert (
                conn.execute("SELECT COUNT(*) FROM variant_replaced_by").fetchone()[0]
                == 0
            )
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
        finally:
            conn.close()

    def test_variant_ersatt_av_emits_variant_replaced_by(self, tmp_path: Path) -> None:
        """RegisterVariant Ersatt av → one variant edge (variant grain keeps
        its variant endpoints — only the variable grain lost them)."""
        rows = [timeseries_row(entitet="RegisterVariant", id1="10", id2="20")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_provider, predecessor_register, predecessor_variant, "
                "successor_provider, successor_register, successor_variant, "
                "effective_year, note FROM variant_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "scb",
                "testreg",
                "individer",
                "scb",
                "otherreg",
                "foretag",
                None,
                "auto:timeseries_event",
            )
        finally:
            conn.close()

    def test_variabel_grain_emits_variable_replaced_by(self, tmp_path: Path) -> None:
        """Variabel (var_id grain) Ersatt av → one 3-part variable edge.

        var_id 100 (TestVar, unique to reg1) → var_id 300 (UniqueVar, unique to
        reg2). Each resolves to its register-unique variable; the endpoint
        carries the STORED `variable.slug` ('testcol' / 'uniqcol') and NO
        variant. (The original draft couldn't use var_id 100 — its two aliases
        derived to two slugs at build time; A2.1.5 stores one canonical slug, so
        it now just works.)
        """
        rows = [timeseries_row(entitet="Variabel", id1="100", id2="300")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_provider, predecessor_register, predecessor_variable, "
                "successor_provider, successor_register, successor_variable, note "
                "FROM variable_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "scb",
                "testreg",
                "testcol",
                "scb",
                "otherreg",
                "uniqcol",
                "auto:timeseries_event",
            )
        finally:
            conn.close()

    def test_aktuell_variabel_emits_variable_replaced_by(self, tmp_path: Path) -> None:
        """AktuellVariabel (cvid grain) → variable_replaced_by, 3-part.

        cvid 1002 (reg1 var 100 → 'testcol') → cvid 2002 (reg2 var 300 →
        'uniqcol'). Lands the same edge a Variabel 100→300 row would, since both
        grains resolve to the same register-scoped variable.
        """
        rows = [timeseries_row(entitet="AktuellVariabel", id1="1002", id2="2002")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_register, predecessor_variable, "
                "successor_register, successor_variable FROM variable_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == ("testreg", "testcol", "otherreg", "uniqcol")
        finally:
            conn.close()

    def test_variabel_ambiguous_across_registers_skipped(self, tmp_path: Path) -> None:
        """A bare Variabel var_id appearing in >1 register can't pick a register
        target, so it's skipped as unresolved (never fails the build). var_id 44
        (Kön) lives in both TESTREG and OTHERREG.

        Distinct from `n_skipped_ambiguous_variable` (an A2.2 split: one
        (register, var_id) → several sibling variables), exercised next.
        """
        rows = [timeseries_row(entitet="Variabel", id1="44", id2="300")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
            stats = self._stats(conn)
            assert stats["n_skipped_unresolved"] == 1
            assert stats["n_skipped_ambiguous_variable"] == 0
        finally:
            conn.close()

    def test_split_var_id_skipped_as_ambiguous(self, tmp_path: Path) -> None:
        """A triage SPLIT (#139) makes one (register, var_id) map to
        several sibling variables sharing `provider_key`. A bare `Variabel`
        succession id carries no discriminator, so it can't pick a sibling and
        is skipped under `n_skipped_ambiguous_variable`, distinct from a plain
        unresolved id. (The `AktuellVariabel` cvid grain *can* disambiguate via
        its `variable_id` stamp — see `test_split_cvid_resolved_via_variable_id_stamp`.)

        Reuses #139's canonical split fixture: Hemkommun + Skolkommun (disjoint
        column stems) under one var_id → two sibling variables.
        """
        # year 2019 is free under variant 10 (the default fixture uses 2020-2022),
        # so populate_slugs hits no register_version slug collision; both rows
        # share 2019 → a same-year collision under var_id 920 → split.
        split_rows = [
            _var_row(colname="Hemkommun", cvid=9300, var_id=920, year="2019"),
            _var_row(colname="Skolkommun", cvid=9301, var_id=920, year="2019"),
        ]
        # id1=920 is the split var_id (ambiguous); id2=300 resolves cleanly, so
        # only the predecessor side trips the ambiguity.
        rows = [timeseries_row(entitet="Variabel", id1="920", id2="300")]
        db_path = self._build(
            tmp_path,
            rows,
            registerinformation_rows=REGISTERINFORMATION_ROWS + split_rows,
        )
        conn = open_db(db_path)
        try:
            # Precondition: the split actually fired (2 siblings share '920').
            sibs = conn.execute(
                "SELECT COUNT(*) FROM variable "
                "WHERE register_id = 1 AND provider_key = '920'"
            ).fetchone()[0]
            assert sibs == 2, "fixture should produce a triage split"
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
            stats = self._stats(conn)
            assert stats["n_skipped_ambiguous_variable"] == 1
            assert stats["n_skipped_unresolved"] == 0
        finally:
            conn.close()

    def test_split_cvid_resolved_via_variable_id_stamp(self, tmp_path: Path) -> None:
        """An `AktuellVariabel` (cvid) succession event over an A2.2-split var_id
        DOES resolve — unlike the bare `Variabel` id above. The cvid names one
        instance, and the coalescer stamps its owning sibling onto
        `variable_instance.variable_id` (PR #150), so the edge resolves to that
        exact sibling — no column-tie, no ambiguity skip. The edge carries that
        sibling's stored slug.

        Same Hemkommun/Skolkommun split fixture; the succession event uses cvid
        9300 (the Hemkommun sibling) as predecessor.
        """
        split_rows = [
            _var_row(colname="Hemkommun", cvid=9300, var_id=920, year="2019"),
            _var_row(colname="Skolkommun", cvid=9301, var_id=920, year="2019"),
        ]
        # id1=9300 = the Hemkommun split sibling (cvid); id2=2002 = reg2 var 300
        # ('uniqcol'), unambiguous.
        rows = [timeseries_row(entitet="AktuellVariabel", id1="9300", id2="2002")]
        db_path = self._build(
            tmp_path,
            rows,
            registerinformation_rows=REGISTERINFORMATION_ROWS + split_rows,
        )
        conn = open_db(db_path)
        try:
            # The split fired, and the Hemkommun sibling has its own stored slug.
            hemkommun_slug = conn.execute(
                "SELECT DISTINCT v.slug FROM variable v "
                "JOIN variable_state vs ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = 1 AND vs.delivery_column_name = 'Hemkommun'"
            ).fetchone()
            assert hemkommun_slug is not None, "fixture should produce a split sibling"
            edges = conn.execute(
                "SELECT predecessor_register, predecessor_variable, "
                "successor_register, successor_variable FROM variable_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "testreg",
                hemkommun_slug[0],
                "otherreg",
                "uniqcol",
            )
            stats = self._stats(conn)
            assert stats["n_skipped_ambiguous_variable"] == 0
            assert stats["n_variable_replaced_by"] == 1
        finally:
            conn.close()

    def test_emitted_variable_slug_is_a_live_variable(self, tmp_path: Path) -> None:
        """Each variable endpoint names a slug that exists as a stored
        `variable.slug` in its register — exactly what the resolver matches on
        (`_resolve_variable_identity` reads the same column). Asserted at the DB
        level rather than via a `Catalog.resolve` round-trip: the binding grammar
        is mid-flip (3-seg lands in A2.6), and stored-slug presence is precisely
        the resolver's match condition.
        """
        rows = [timeseries_row(entitet="Variabel", id1="100", id2="300")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edge = conn.execute(
                "SELECT predecessor_register, predecessor_variable, "
                "successor_register, successor_variable FROM variable_replaced_by"
            ).fetchone()
            for reg_slug, var_slug in ((edge[0], edge[1]), (edge[2], edge[3])):
                hit = conn.execute(
                    "SELECT 1 FROM variable v "
                    "JOIN register r ON v.register_id = r.register_id "
                    "WHERE r.slug = ? AND v.slug = ?",
                    (reg_slug, var_slug),
                ).fetchone()
                assert hit is not None, (reg_slug, var_slug)
        finally:
            conn.close()

    def test_ersatter_inverse_direction_collapsed(self, tmp_path: Path) -> None:
        """Paired (Ersatt av, Ersätter) rows over the same transition → exactly
        ONE edge. SCB ships both directions; the collapse is load-bearing."""
        rows = [
            timeseries_row(
                handelse="Ersatt av", entitet="RegisterVariant", id1="10", id2="20"
            ),
            timeseries_row(
                handelse="Ersätter", entitet="RegisterVariant", id1="20", id2="10"
            ),
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_variant, successor_variant FROM variant_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            # Predecessor = the original (variant 10 → individer), successor 20 → foretag.
            assert tuple(edges[0]) == ("individer", "foretag")
            stats = self._stats(conn)
            # The genuine Ersätter row is the one that collapsed — counted as
            # inverse, NOT as a plain duplicate.
            assert stats["n_skipped_collapsed_inverse"] == 1
            assert stats["n_skipped_duplicate"] == 0
        finally:
            conn.close()

    def test_duplicate_ersatt_av_not_counted_as_inverse(self, tmp_path: Path) -> None:
        """Two identical `Ersatt av` rows (NOT an Ersätter pair) → one edge, the
        second counted as `n_skipped_duplicate`, not `n_skipped_collapsed_inverse`.
        The inverse counter must mean what its name says."""
        rows = [
            timeseries_row(
                handelse="Ersatt av", entitet="RegisterVariant", id1="10", id2="20"
            ),
            timeseries_row(
                handelse="Ersatt av", entitet="RegisterVariant", id1="10", id2="20"
            ),
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM variant_replaced_by").fetchone()[0]
                == 1
            )
            stats = self._stats(conn)
            assert stats["n_skipped_duplicate"] == 1
            assert stats["n_skipped_collapsed_inverse"] == 0
        finally:
            conn.close()

    def test_inverse_collapse_robust_to_row_order(self, tmp_path: Path) -> None:
        """The inverse/duplicate split is independent of SCB's row order: even
        when the `Ersätter` row precedes its canonical `Ersatt av` twin, the
        collapse is still counted as `n_skipped_collapsed_inverse`, never as a
        plain duplicate. The materializer processes `Ersatt av` first (ORDER BY),
        so the edge always lands from the canonical direction. Without that
        ordering this fixture would mislabel the collapse as a duplicate.
        """
        rows = [
            timeseries_row(
                handelse="Ersätter", entitet="RegisterVariant", id1="20", id2="10"
            ),
            timeseries_row(
                handelse="Ersatt av", entitet="RegisterVariant", id1="10", id2="20"
            ),
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_variant, successor_variant FROM variant_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == ("individer", "foretag")
            stats = self._stats(conn)
            assert stats["n_skipped_collapsed_inverse"] == 1
            assert stats["n_skipped_duplicate"] == 0
        finally:
            conn.close()

    def test_unresolvable_id_skipped_not_failed(self, tmp_path: Path) -> None:
        """An id pointing at a non-existent entity → no edge, stat increments,
        build does NOT raise."""
        rows = [timeseries_row(entitet="Register", id1="1", id2="9999")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM register_replaced_by").fetchone()[0]
                == 0
            )
            assert self._stats(conn)["n_skipped_unresolved"] == 1
        finally:
            conn.close()

    def test_malformed_ids_skipped_not_failed(self, tmp_path: Path) -> None:
        """Empty and non-integer ids on otherwise-relevant rows are skipped as
        unresolved (the `not id1_raw` and `except ValueError` branches), never
        failing the build — SCB ships empty ids routinely (one direction of a
        pair). A valid control row still produces its edge.
        """
        rows = [
            timeseries_row(entitet="Register", id1="1", id2=""),  # empty id2
            timeseries_row(entitet="Register", id1="1", id2="not-int"),  # non-integer
            timeseries_row(entitet="Register", id1="1", id2="2"),  # valid control
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM register_replaced_by").fetchone()[0]
                == 1
            )
            stats = self._stats(conn)
            assert stats["n_skipped_unresolved"] == 2  # empty + non-integer
            assert stats["n_timeseries_event_rows_scanned"] == 3
        finally:
            conn.close()

    def test_effective_year_is_null(self, tmp_path: Path) -> None:
        """Timeseries.csv has no year column, so effective_year lands NULL on
        every auto-derived row. Pins the contract: if SCB ever ships a year,
        the schema/manifest both need updating — this assertion is the canary.
        """
        rows = [timeseries_row(entitet="Register", id1="1", id2="2")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            year = conn.execute(
                "SELECT effective_year FROM register_replaced_by"
            ).fetchone()[0]
            assert year is None
        finally:
            conn.close()

    def test_note_is_auto_timeseries_event(self, tmp_path: Path) -> None:
        """Every auto-derived row across all three tables carries
        note = 'auto:timeseries_event' (distinguishes from future A4 TOML rows)."""
        rows = [
            timeseries_row(entitet="Register", id1="1", id2="2"),
            timeseries_row(entitet="RegisterVariant", id1="10", id2="20"),
            timeseries_row(entitet="AktuellVariabel", id1="1002", id2="2002"),
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            for table in (
                "register_replaced_by",
                "variant_replaced_by",
                "variable_replaced_by",
            ):
                notes = {
                    r[0]
                    for r in conn.execute(f"SELECT note FROM {table}").fetchall()  # noqa: S608 -- table name is a literal
                }
                assert notes == {"auto:timeseries_event"}, table
        finally:
            conn.close()

    def test_replaced_by_carries_beskrivning(self, tmp_path: Path) -> None:
        """#142: `timeseries_event.Beskrivning` (the human transition reason) is
        carried verbatim into the edge, ALONGSIDE the `auto:timeseries_event`
        provenance marker in `note`. Mirrors the real `slk` SUN edge
        ("2001 byttes SUN96 till SUN2000") with a crafted row on the
        AktuellVariabel grain (cvid 1002 → 2002)."""
        rows = [
            timeseries_row(
                entitet="AktuellVariabel",
                id1="1002",
                id2="2002",
                beskrivning="2001 byttes SUN96 till SUN2000",
            )
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            row = conn.execute(
                "SELECT beskrivning, note FROM variable_replaced_by"
            ).fetchone()
            # Both retained: the description AND the provenance marker.
            assert row["beskrivning"] == "2001 byttes SUN96 till SUN2000"
            assert row["note"] == "auto:timeseries_event"
        finally:
            conn.close()

    def test_replaced_by_beskrivning_null_when_empty(self, tmp_path: Path) -> None:
        """An empty Beskrivning (the SCB common case) lands NULL, not ''."""
        rows = [timeseries_row(entitet="AktuellVariabel", id1="1002", id2="2002")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            besk = conn.execute(
                "SELECT beskrivning FROM variable_replaced_by"
            ).fetchone()[0]
            assert besk is None
        finally:
            conn.close()

    def test_replaced_by_effective_year_for_aktuell_variabel(
        self, tmp_path: Path
    ) -> None:
        """#142: effective_year on the AktuellVariabel grain = the SUCCESSOR
        cvid's edition year. cvid 2002 (OTHERREG, 'uniqcol') is delivered in the
        2021 edition (`_csv_fixtures` OTHERREG version slug = '2021'), so the
        1002 → 2002 edge carries effective_year = 2021. (Mirrors the slk
        acceptance shape; the fixture pins the year via the regver slug.)"""
        rows = [
            timeseries_row(
                entitet="AktuellVariabel",
                id1="1002",
                id2="2002",
                beskrivning="2001 byttes SUN96 till SUN2000",
            )
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            year = conn.execute(
                "SELECT effective_year FROM variable_replaced_by"
            ).fetchone()[0]
            assert year == 2021  # successor cvid 2002's edition year
        finally:
            conn.close()

    def test_replaced_by_bare_variabel_effective_year_null(
        self, tmp_path: Path
    ) -> None:
        """#142 asymmetry: the bare `Variabel` grain has no cvid → no edition →
        effective_year NULL (only the AktuellVariabel grain names an edition).
        Variabel 100 → 300 lands the same edge as the cvid grain, but without a
        year."""
        rows = [timeseries_row(entitet="Variabel", id1="100", id2="300")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            year = conn.execute(
                "SELECT effective_year FROM variable_replaced_by"
            ).fetchone()[0]
            assert year is None
        finally:
            conn.close()

    def test_replaced_by_surfaces_through_successors(self, tmp_path: Path) -> None:
        """#142 end-to-end: the beskrivning + effective_year land on
        `Catalog.successors()` (VariableRef.reason / .effective_year)."""
        from reg_meta.catalog import Catalog

        rows = [
            timeseries_row(
                entitet="AktuellVariabel",
                id1="1002",
                id2="2002",
                beskrivning="2001 byttes SUN96 till SUN2000",
            )
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            # testcol (reg1 var 100) → uniqcol (reg2 var 300). Resolve the
            # predecessor binding and read its outbound successor.
            succ = Catalog(conn).successors("scb/testreg/testcol")
            assert len(succ) == 1
            assert succ[0].variable == "uniqcol"
            assert succ[0].reason == "2001 byttes SUN96 till SUN2000"
            assert succ[0].effective_year == 2021
            # And the inverse direction: predecessors() of the successor.
            pred = Catalog(conn).predecessors("scb/otherreg/uniqcol")
            assert len(pred) == 1
            assert pred[0].variable == "testcol"
            assert pred[0].reason == "2001 byttes SUN96 till SUN2000"
        finally:
            conn.close()

    def test_replaced_by_stats_in_manifest(self, tmp_path: Path) -> None:
        """Manifest carries replaced_by_stats with the full reworked key set."""
        rows = [timeseries_row(entitet="Register", id1="1", id2="2")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            stats = self._stats(conn)
            assert set(stats.keys()) == {
                "n_timeseries_event_rows_scanned",
                "n_register_replaced_by",
                "n_variant_replaced_by",
                "n_variable_replaced_by",
                "n_skipped_unresolved",
                "n_skipped_ambiguous_variable",
                "n_skipped_collapsed_inverse",
                "n_skipped_duplicate",
                # #440 curated-TOML pass
                "n_curated_register_replaced_by",
                "n_curated_variable_replaced_by",
                "n_curated_skipped_duplicate",
                "n_curated_skipped_inactive_provider",
            }
            assert stats["n_register_replaced_by"] == 1
            # Scanned counts only Ersatt av/Ersätter rows on the four target
            # entitets — the single Register row in this fixture matches.
            assert stats["n_timeseries_event_rows_scanned"] == 1
        finally:
            conn.close()

    def test_no_self_loops(self, tmp_path: Path) -> None:
        """Defensive: id1 == id2 → skipped (never inserted). A self-loop edge is
        meaningless and would corrupt graph traversal."""
        rows = [timeseries_row(entitet="Register", id1="1", id2="1")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM register_replaced_by").fetchone()[0]
                == 0
            )
            assert self._stats(conn)["n_skipped_unresolved"] == 1
        finally:
            conn.close()

    def test_variable_grain_self_loop_skipped(self, tmp_path: Path) -> None:
        """Slug-grain self-loop: two DISTINCT ids that resolve to the SAME
        variable must not produce a `predecessor == successor` edge. The raw-id
        guard (`id1 == id2`) can't catch this because the variable grain
        collapses ids to slugs. The default fixture's var_id 44 (Kön) spans three
        cvids (1001, 1003, 1004) under one variable → slug 'kon'; a succession
        between two of them is a self-loop only visible after resolution.
        """
        rows = [timeseries_row(entitet="AktuellVariabel", id1="1001", id2="1003")]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            # Precondition: cvids 1001 and 1003 are distinct eras of one variable
            # (var_id 44, reg 1 → slug 'kon'). A2.7: `variable_instance` is
            # dropped before ship, so check the surviving `variable` row.
            slug = conn.execute(
                "SELECT slug FROM variable WHERE register_id = 1 AND provider_key = '44'"
            ).fetchone()[0]
            assert slug == "kon", "var_id 44 must map to slug 'kon'"
            # No self-edge emitted; counted as a skipped self-loop.
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
            assert self._stats(conn)["n_skipped_unresolved"] == 1
        finally:
            conn.close()

    def test_irrelevant_handelse_ignored(self, tmp_path: Path) -> None:
        """Rows with handelse not in (Ersatt av, Ersätter) → ignored before
        resolution (so they don't inflate n_skipped_unresolved). The default
        `TIMESERIES_ROWS` ships one such row (Kodändring)."""
        rows = [
            timeseries_row(
                handelse="Kodändring", entitet="Variabel", id1="100", id2=""
            ),
            timeseries_row(entitet="Register", id1="1", id2="2"),
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM register_replaced_by").fetchone()[0]
                == 1
            )
            stats = self._stats(conn)
            assert stats["n_timeseries_event_rows_scanned"] == 1
            assert stats["n_skipped_unresolved"] == 0
        finally:
            conn.close()

    def test_irrelevant_entitet_ignored(self, tmp_path: Path) -> None:
        """Rows with entitet outside the four target shapes → ignored (e.g.
        RegisterVersion, whose succession is handled elsewhere)."""
        rows = [
            timeseries_row(entitet="RegisterVersion", id1="100", id2="101"),
            timeseries_row(entitet="Register", id1="1", id2="2"),
        ]
        db_path = self._build(tmp_path, rows)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM register_replaced_by").fetchone()[0]
                == 1
            )
            assert (
                conn.execute("SELECT COUNT(*) FROM variant_replaced_by").fetchone()[0]
                == 0
            )
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
            # Scanned count includes only target-entitet rows.
            assert self._stats(conn)["n_timeseries_event_rows_scanned"] == 1
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # #440 curated inline `[[replaced_by]]` edges. These materialize ALONGSIDE
    # the event-derived edges, into the SAME tables, sharing the dedup seen-set.
    # ------------------------------------------------------------------

    def test_curated_variable_edge_live_predecessor(self, tmp_path: Path) -> None:
        """A within-provider curated variable edge whose predecessor is LIVE
        (both ends resolve) → one `variable_replaced_by` row with the curated
        provenance marker in `note` and the row's transition reason in
        `beskrivning`. testcol (reg1) → uniqcol (reg2)."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/uniqcol"\n'
            'note = "renamed in 2012"\n'
            "effective_year = 2012\n"
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_provider, predecessor_register, "
                "predecessor_variable, successor_provider, successor_register, "
                "successor_variable, effective_year, note, beskrivning "
                "FROM variable_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "scb",
                "testreg",
                "testcol",
                "scb",
                "otherreg",
                "uniqcol",
                2012,
                "curated:slug_toml",
                "renamed in 2012",
            )
            stats = self._stats(conn)
            assert stats["n_curated_variable_replaced_by"] == 1
            assert stats["n_variable_replaced_by"] == 1
        finally:
            conn.close()

    def test_curated_register_edge(self, tmp_path: Path) -> None:
        """A curated register-grain edge → one `register_replaced_by` row, grain
        inferred from the 2-segment FQID."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg"\n'
            'successor = "scb/otherreg"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_register, successor_register, "
                "effective_year, note FROM register_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == ("testreg", "otherreg", None, "curated:slug_toml")
            assert self._stats(conn)["n_curated_register_replaced_by"] == 1
        finally:
            conn.close()

    def test_curated_edge_dead_predecessor_still_inserted(self, tmp_path: Path) -> None:
        """The whole point of #440: a curated edge whose PREDECESSOR has no live
        row is still inserted verbatim (slug-anchored). The successor (uniqcol)
        resolves; the predecessor names a register/variable not in the build."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/retired-reg/retired-var"\n'
            'successor = "scb/otherreg/uniqcol"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_register, predecessor_variable, "
                "successor_register, successor_variable FROM variable_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "retired-reg",
                "retired-var",
                "otherreg",
                "uniqcol",
            )
        finally:
            conn.close()

    def test_curated_cross_provider_edge(self, tmp_path: Path) -> None:
        """A cross-provider curated edge (predecessor under a DIFFERENT provider
        than the live successor) → inserted at the correct grain. The predecessor
        provider `sos` doesn't exist in this SCB-only build (dead predecessor);
        cross-provider succession is exactly what `timeseries_event` cannot
        carry."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "sos/par/diagnos"\n'
            'successor = "scb/testreg/testcol"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_provider, predecessor_register, "
                "predecessor_variable, successor_provider, successor_register, "
                "successor_variable FROM variable_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "sos",
                "par",
                "diagnos",
                "scb",
                "testreg",
                "testcol",
            )
        finally:
            conn.close()

    def test_curated_unresolved_successor_fails_fast(self, tmp_path: Path) -> None:
        """A curated successor that does NOT resolve to a live slugged entity is
        a curation error → EXIT_CONFIG (unlike the best-effort event path).

        The successor's provider IS in this build (scb) — only the slug is
        missing; that's the real curation error the inactive-provider skip below
        must NOT swallow."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/ghost-var"\n'
        )
        with pytest.raises(RegMetaError) as exc:
            self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        assert exc.value.code == "replaced_by_unresolved_successor"

    def test_curated_inactive_provider_successor_skipped(self, tmp_path: Path) -> None:
        """A curated edge whose SUCCESSOR's provider isn't in this (scb-only)
        build is SKIPPED, not failed — a partial build genuinely lacks the sos
        tables, so it can't resolve an sos successor. The build completes (and
        validates), no edge is inserted, and the new skip counter is 1.

        Without the provider gate this raised `replaced_by_unresolved_successor`
        and crashed the partial build (the Codex P2 this test pins)."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "sos/par/diagnos"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
            stats = self._stats(conn)
            assert stats["n_curated_skipped_inactive_provider"] == 1
            assert stats["n_curated_variable_replaced_by"] == 0
        finally:
            conn.close()

    def test_curated_active_provider_successor_materializes(
        self, tmp_path: Path
    ) -> None:
        """The flip side of the inactive-provider skip: an edge whose successor's
        provider IS active (scb, in this scb-only build) still materializes —
        the gate doesn't over-skip."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/uniqcol"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 1
            )
            stats = self._stats(conn)
            assert stats["n_curated_variable_replaced_by"] == 1
            assert stats["n_curated_skipped_inactive_provider"] == 0
        finally:
            conn.close()

    def test_curated_edge_dedups_against_event_derived(self, tmp_path: Path) -> None:
        """A curated edge duplicating an event-derived edge collapses (no double
        row, counted as a curated skip). The event pass emits testcol→uniqcol
        first; the curated row over the SAME slug-PK is deduped via the shared
        seen-set."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/uniqcol"\n'
        )
        # var_id 100 → 300 is testcol → uniqcol (same edge the curated row names).
        rows = [timeseries_row(entitet="Variabel", id1="100", id2="300")]
        db_path = self._build(tmp_path, timeseries_rows=rows, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute("SELECT note FROM variable_replaced_by").fetchall()
            assert len(edges) == 1
            # The event-derived edge wins (it ran first); the curated dup collapsed.
            assert edges[0][0] == "auto:timeseries_event"
            stats = self._stats(conn)
            assert stats["n_curated_skipped_duplicate"] == 1
            assert stats["n_curated_variable_replaced_by"] == 0
            assert stats["n_variable_replaced_by"] == 1
        finally:
            conn.close()

    def test_curated_edge_dedups_against_curated(self, tmp_path: Path) -> None:
        """Two curated `[[replaced_by]]` rows naming the SAME predecessor/
        successor collapse to one `variable_replaced_by` row via the shared
        seen-set. Without that seen-set update on the curated branch the second
        INSERT would hit the slug-PK constraint and crash the build."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/uniqcol"\n'
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/uniqcol"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 1
            )
            stats = self._stats(conn)
            assert stats["n_curated_skipped_duplicate"] == 1
            assert stats["n_curated_variable_replaced_by"] == 1
        finally:
            conn.close()

    def test_curated_register_edge_dead_predecessor_inserted(
        self, tmp_path: Path
    ) -> None:
        """Register-grain twin of `test_curated_edge_dead_predecessor_still_inserted`:
        a 2-segment edge whose PREDECESSOR register is not live (successor is) is
        still inserted verbatim, exercising the `succ.kind is FqidKind.REGISTER`
        branch's slug-anchored insert. With no `note`, `beskrivning` stays NULL
        and `note` carries the curated provenance marker (the register branch is
        a separate INSERT from the variable branch the variable-grain test locks)."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/retired-reg"\n'
            'successor = "scb/otherreg"\n'
        )
        db_path = self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_provider, predecessor_register, "
                "successor_provider, successor_register, "
                "effective_year, note, beskrivning FROM register_replaced_by"
            ).fetchall()
            assert len(edges) == 1
            assert tuple(edges[0]) == (
                "scb",
                "retired-reg",
                "scb",
                "otherreg",
                None,
                "curated:slug_toml",
                None,
            )
            assert self._stats(conn)["n_curated_register_replaced_by"] == 1
        finally:
            conn.close()

    def test_curated_edge_closing_cycle_with_event_edge_fails(
        self, tmp_path: Path
    ) -> None:
        """The combined-graph cycle check (Codex P2): an event-derived edge A→B
        plus a curated edge B→A close a cycle that NEITHER source sees alone. The
        event pass emits testcol→uniqcol (var_id 100→300); the curated row names
        the reverse uniqcol→testcol. Both successors resolve, so this is caught by
        the cycle check, not the unresolved-successor guard. The build fails fast
        with `replaced_by_cycle` and no partial rows survive."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/otherreg/uniqcol"\n'
            'successor = "scb/testreg/testcol"\n'
        )
        rows = [timeseries_row(entitet="Variabel", id1="100", id2="300")]
        with pytest.raises(RegMetaError) as exc:
            self._build(tmp_path, timeseries_rows=rows, scb_extra=extra)
        assert exc.value.code == "replaced_by_cycle"

    def test_curated_only_two_cycle_fails_via_materializer(
        self, tmp_path: Path
    ) -> None:
        """A curated-only 2-cycle (no event edge) still fails — the materializer
        runs the cycle check over the curated-to-insert edges too, since the
        load-time helper no longer fires. testcol→uniqcol + uniqcol→testcol both
        resolve, so only the cycle check catches them."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/testreg/testcol"\n'
            'successor = "scb/otherreg/uniqcol"\n'
            "\n[[replaced_by]]\n"
            'predecessor = "scb/otherreg/uniqcol"\n'
            'successor = "scb/testreg/testcol"\n'
        )
        with pytest.raises(RegMetaError) as exc:
            self._build(tmp_path, timeseries_rows=_NO_EVENT_ROWS, scb_extra=extra)
        assert exc.value.code == "replaced_by_cycle"

    def test_curated_edge_not_closing_cycle_materializes(self, tmp_path: Path) -> None:
        """The cycle check doesn't over-reject: an event-derived edge A→B plus a
        curated edge B→C (extending the chain, not closing a loop) both land. The
        event pass emits testcol→uniqcol; the curated row uniqcol→parencol adds a
        forward edge, so two distinct `variable_replaced_by` rows result."""
        extra = (
            "\n[[replaced_by]]\n"
            'predecessor = "scb/otherreg/uniqcol"\n'
            'successor = "scb/otherreg/parencol"\n'
        )
        rows = [timeseries_row(entitet="Variabel", id1="100", id2="300")]
        db_path = self._build(tmp_path, timeseries_rows=rows, scb_extra=extra)
        conn = open_db(db_path)
        try:
            edges = conn.execute(
                "SELECT predecessor_variable, successor_variable, note "
                "FROM variable_replaced_by ORDER BY note"
            ).fetchall()
            assert len(edges) == 2
            assert tuple(edges[0]) == ("testcol", "uniqcol", "auto:timeseries_event")
            assert tuple(edges[1]) == ("uniqcol", "parencol", "curated:slug_toml")
            stats = self._stats(conn)
            assert stats["n_curated_variable_replaced_by"] == 1
            assert stats["n_variable_replaced_by"] == 2
        finally:
            conn.close()


class TestProvenanceDbRotation:
    """A4.2: the universal DB and the sibling provenance DB rotate to `.prev`
    in lockstep, and a provenance-write failure never poisons the universal DB.
    """

    def _build_once(self, input_dir: Path, db_dir: Path, **kwargs) -> None:
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
            **kwargs,
        )

    def test_both_dbs_rotate_in_lockstep(self, tmp_path: Path) -> None:
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir)

        self._build_once(input_dir, db_dir)
        universal = db_dir / "reg_meta.db"
        prov = db_dir / "reg_meta.provenance.db"
        gen1_universal = universal.read_bytes()
        gen1_prov = prov.read_bytes()

        # Second build rotates gen-1 aside into `.prev`.
        self._build_once(input_dir, db_dir)
        universal_prev = db_dir / "reg_meta.db.prev"
        prov_prev = db_dir / "reg_meta.provenance.db.prev"

        assert universal.exists() and prov.exists()
        assert universal_prev.exists() and prov_prev.exists()
        # `.prev` carries gen-1 (rotation moved gen-1 aside, not gen-2).
        assert universal_prev.read_bytes() == gen1_universal
        assert prov_prev.read_bytes() == gen1_prov

    def test_universal_db_survives_provenance_failure(self, tmp_path: Path) -> None:
        """A provenance write failure must NOT flip the build exit code or
        leave the universal DB unswapped — the non-fatal try/except contract."""
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir)

        def _boom(_tmp_path: Path) -> None:
            raise RuntimeError("injected provenance failure")

        # build_db must return normally (no raise) despite the failure.
        self._build_once(input_dir, db_dir, provenance_pre_rename_hook=_boom)

        universal = db_dir / "reg_meta.db"
        prov = db_dir / "reg_meta.provenance.db"
        assert universal.exists(), "universal DB must still be swapped in"
        # The provenance tmp was injected-to-fail before rename, so no live
        # provenance DB was produced this build.
        assert not prov.exists()
        # And the universal DB is a valid, populated SQLite file.
        conn = open_db(universal)
        try:
            assert conn.execute("SELECT COUNT(*) FROM register").fetchone()[0] >= 1
        finally:
            conn.close()
