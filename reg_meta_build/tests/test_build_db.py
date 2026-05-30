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
    VARDEMANGDER_REAL_ROWS,
    _ri_row,
    _var_row,
    timeseries_row,
    write_csv,
    write_scb_input,
)
from reg_meta.db import SCHEMA_VERSION, get_manifest, open_db
from reg_meta.errors import RegMetaError
from reg_meta.queries import extract_year
from reg_meta_build.db import _decode_cp1252, _value_set_hash, build_db


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

    def test_register_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register").fetchone()[0]
        assert count == 2  # TESTREG and OTHERREG

    def test_variant_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register_variant").fetchone()[0]
        assert count == 2  # variant 10 and variant 20

    def test_version_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM register_version").fetchone()[0]
        assert count == 4  # 2020, 2021, 2022 for reg 1 + 2021 for reg 2

    def test_variable_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]
        # Kön, TestVar, ÅÄÖVar in reg 1; Kön, UniqueVar, ParenVar, ExternVar in reg 2
        assert count == 7

    def test_instance_count(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM variable_instance").fetchone()[0]
        # CVIDs: 1001, 1002, 1003, 1004, 1005, 2001, 2002, 2003, 2004
        assert count == 9

    def test_alias_anomaly(self, db_conn: sqlite3.Connection):
        """CVID 1002 should have two aliases: TestCol and TestKolumn."""
        aliases = db_conn.execute(
            "SELECT delivery_column_name FROM variable_alias WHERE cvid = 1002 ORDER BY delivery_column_name"
        ).fetchall()
        assert [a[0] for a in aliases] == ["TestCol", "TestKolumn"]

    def test_value_items_filtered(self, db_conn: sqlite3.Connection):
        """Unknown CVID 9999 must not produce a value_set link."""
        row = db_conn.execute(
            "SELECT 1 FROM variable_instance WHERE cvid = 9999"
        ).fetchone()
        # cvid 9999 is filtered before reaching the importer; it has no
        # variable_instance row at all (Registerinformation.csv has no entry).
        assert row is None

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

    def test_value_set_info_on_instance(self, db_conn: sqlite3.Connection):
        """Variable instances with values should have vardemangdsversion/niva set."""
        row = db_conn.execute(
            "SELECT value_set_version_label, vardemangdsniva FROM variable_instance "
            "WHERE cvid = 1001"
        ).fetchone()
        assert row["value_set_version_label"] == "Kön"
        assert row["vardemangdsniva"] == "1"

    def test_sentinel_rows_skipped(self, db_conn: sqlite3.Connection):
        """SCB type-tag rows ("Tal", "Beskrivande text") must not produce
        value_code rows; sentinel-only cvids must end up with NULL value_set_id."""
        rows = db_conn.execute(
            "SELECT code FROM value_code WHERE code IN ('Tal', 'Beskrivande text')"
        ).fetchall()
        assert rows == []
        for cvid in (1004, 1005):
            row = db_conn.execute(
                "SELECT value_set_id FROM variable_instance WHERE cvid = ?",
                (cvid,),
            ).fetchone()
            assert row is not None, f"cvid {cvid} should exist"
            assert row["value_set_id"] is None, f"cvid {cvid} value_set_id"

    def test_sentinel_only_cvid_has_null_metadata(self, db_conn: sqlite3.Connection):
        """A cvid whose only Vardemangder rows were sentinels must end up with
        NULL vardemangdsversion/niva — not the sentinel string."""
        for cvid in (1004, 1005):
            row = db_conn.execute(
                "SELECT value_set_version_label, vardemangdsniva "
                "FROM variable_instance WHERE cvid = ?",
                (cvid,),
            ).fetchone()
            assert row["value_set_version_label"] is None, f"cvid {cvid}"
            assert row["vardemangdsniva"] is None, f"cvid {cvid}"

    def test_real_code_with_sentinel_shape_survives(self, db_conn: sqlite3.Connection):
        """A row where kod==version==niva but kod is not a known sentinel is a
        real code (e.g. cvid 2002, kod="2", label="Övriga civilstånd"). It must
        be preserved, including its version metadata."""
        code_rows = db_conn.execute(
            "SELECT vc.code, vc.label "
            "FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 2002"
        ).fetchall()
        assert [(r["code"], r["label"]) for r in code_rows] == [
            ("2", "Övriga civilstånd")
        ]
        meta = db_conn.execute(
            "SELECT value_set_version_label, vardemangdsniva "
            "FROM variable_instance WHERE cvid = 2002"
        ).fetchone()
        assert meta["value_set_version_label"] == "2"
        assert meta["vardemangdsniva"] == "2"

    def test_empty_vardekod_survives(self, db_conn: sqlite3.Connection):
        """Empty vardekod with a label ("Uppgift okänd") is a legitimate code,
        not pollution. Must survive."""
        rows = db_conn.execute(
            "SELECT vc.code, vc.label "
            "FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 2003"
        ).fetchall()
        assert [(r["code"], r["label"]) for r in rows] == [("", "Uppgift okänd")]

    def test_fully_empty_row_dropped(self, db_conn: sqlite3.Connection):
        """A row with empty kod, label, and item carries no information; the
        cvid must end up with NULL value_set_id and NULL version metadata."""
        row = db_conn.execute(
            "SELECT value_set_id, value_set_version_label, vardemangdsniva "
            "FROM variable_instance WHERE cvid = 1002"
        ).fetchone()
        assert row["value_set_id"] is None
        assert row["value_set_version_label"] is None
        assert row["vardemangdsniva"] is None

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

    def test_consumer_side_binding_linked(self, db_conn: sqlite3.Connection):
        """OTHERREG Kön (cvid 2001, year 2021) is sourced from TESTREG. Its
        via_source_id should point at TESTREG's Kön cvid for year 2021
        (cvid 1003) per §5.6."""
        row = db_conn.execute(
            "SELECT via_source_id FROM variable_instance WHERE cvid = 2001"
        ).fetchone()
        assert row["via_source_id"] == 1003

    def test_canonical_binding_has_null_via_source(self, db_conn: sqlite3.Connection):
        """TESTREG Kön owns the variable concept — its instances are canonical
        and must keep via_source_id NULL."""
        rows = db_conn.execute(
            "SELECT cvid, via_source_id FROM variable_instance "
            "WHERE register_id = 1 AND var_id = 44"
        ).fetchall()
        assert rows
        assert all(r["via_source_id"] is None for r in rows)

    def test_no_link_when_source_period_missing(self, db_conn: sqlite3.Connection):
        """ParenVar (cvid 2003) has variable_register_kalla=TESTREG but TESTREG
        has no matching variable slug — via_source_id stays NULL."""
        row = db_conn.execute(
            "SELECT via_source_id FROM variable_instance WHERE cvid = 2003"
        ).fetchone()
        assert row["via_source_id"] is None

    def test_linker_keys_by_full_period_not_year(self, tmp_path: Path):
        """Source `HT2020` must not link to consumer `VT2020` despite sharing
        the embedded year. Keying on bare year would have produced the wrong
        via_source_id (PR #80 review P1)."""
        ht_source = _ri_row(
            "TESTREG",
            "Testregistret",
            "Testning",
            "Individer",
            "Individer",
            "Alla individer",
            "Nej",
            "HT2020",
            "Version HT2020",
            "",
            "Godkänd",
            "2020-07-01",
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
            "7001",
            "1",
            "10",
            "700",
            "44",
        )
        vt_consumer = _ri_row(
            "OTHERREG",
            "Annat register",
            "Annat syfte",
            "Företag",
            "Företag",
            "Alla företag",
            "Ja",
            "VT2020",
            "Version VT2020",
            "",
            "Godkänd",
            "2020-01-01",
            "2020-06-30",
            "Alla företag",
            "Samtliga företag",
            "",
            "2020-06-30",
            "Företag",
            "Juridisk person",
            "Kön",
            "Ägarkön",
            "Kön på ägare",
            "",
            "",
            "Testregistret",
            "TESTREG",
            "",
            "",
            "KON",
            "int",
            "1",
            "7002",
            "2",
            "20",
            "701",
            "44",
        )
        ri_rows = list(REGISTERINFORMATION_ROWS) + [ht_source, vt_consumer]
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
        row = conn.execute(
            "SELECT via_source_id FROM variable_instance WHERE cvid = 7002"
        ).fetchone()
        conn.close()
        # VT2020 consumer must NOT link to HT2020 source — they share the year
        # but the period grammar treats them as distinct.
        assert row["via_source_id"] is None

    def test_linker_uses_slug_to_disambiguate_collision_siblings(self, tmp_path: Path):
        """Regression for Codex P1 on PR #94: two source siblings whose names
        both `derive_period` to `2018` (e.g. `LISA 2018 huvudfil` +
        `LISA 2018 tilläggsfil`) must be disambiguated by their curated
        `register_version.slug`, not collapsed to whichever appears first.

        Builds a slugged DB by hand, populates the slug column on both
        siblings, calls `link_consumer_side_bindings`, and verifies the
        consumer's `via_source_id` points at the *correct* sibling.
        """
        import sqlite3 as _sql

        from reg_meta_build.db import DDL, link_consumer_side_bindings, seed_providers

        conn = _sql.connect(":memory:")
        conn.row_factory = _sql.Row
        conn.executescript(DDL)
        seed_providers(conn)
        # Source register `src` (id=1) with one variant, two sibling versions
        # whose registerversionnamn both derive_period to "2018", but whose
        # `register_version.slug` is curator-disambiguated.
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (1, 1, 'src')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id) VALUES (10, 1)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (100, 10, '2018', 'LISA 2018 huvudfil'),"
            "       (101, 10, 'tillagg-2018', 'LISA 2018 tilläggsfil')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, slug) "
            "VALUES (1, '44', 'kon')"
        )
        conn.executemany(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id) VALUES (?, ?, ?, ?, ?)",
            [(1000, 1, 10, 100, 44), (1001, 1, 10, 101, 44)],
        )
        conn.executemany(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
            [(1000, "Kon"), (1001, "Kon")],
        )
        # Consumer register `cons` (id=2) with one variant + one version whose
        # slug matches the tilläggsfil sibling. variable.source_register_id = 1
        # marks this as consumer-side; the linker must pick cvid 1001, not 1000.
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) "
            "VALUES (2, 1, 'cons')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id) VALUES (20, 2)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (200, 20, 'tillagg-2018', 'Cons 2018 tilläggsfil')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, source_register_id, slug) "
            "VALUES (2, '44', 1, 'kon')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id) "
            "VALUES (2000, 2, 20, 200, 44)"
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (2000, 'Kon')"
        )
        conn.commit()

        n = link_consumer_side_bindings(conn)
        assert n == 1
        row = conn.execute(
            "SELECT via_source_id FROM variable_instance WHERE cvid = 2000"
        ).fetchone()
        assert row["via_source_id"] == 1001  # tilläggsfil, NOT 1000 (huvudfil)

    def test_linker_no_match_when_consumer_slug_differs_from_source_siblings(
        self, tmp_path: Path
    ):
        """Strict-slug rule: if a consumer's slug doesn't exactly match any
        source sibling's slug, no edge forms. Caller must curate matching
        slugs on both sides — the linker does not fall back to a fuzzier
        key. Real example: IoT 2020+ has `preliminar-version-2020` +
        `slutlig-version-2020` curated siblings; a consumer with bare slug
        "2020" doesn't disambiguate which it pulls from, so the lineage
        stays NULL until curation chooses a canonical sibling.
        """
        import sqlite3 as _sql

        from reg_meta_build.db import DDL, link_consumer_side_bindings, seed_providers

        conn = _sql.connect(":memory:")
        conn.row_factory = _sql.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (1, 1, 'iot')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id) VALUES (10, 1)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (100, 10, 'preliminar-version-2020', 'IoT preliminär version 2020'),"
            "       (101, 10, 'slutlig-version-2020', 'IoT slutlig version 2020')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, slug) "
            "VALUES (1, '44', 'socbidrhb')"
        )
        conn.executemany(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id) VALUES (?, ?, ?, ?, ?)",
            [(1000, 1, 10, 100, 44), (1001, 1, 10, 101, 44)],
        )
        conn.executemany(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
            [(1000, "SocBidrHB"), (1001, "SocBidrHB")],
        )
        # Consumer LISA 2020 has bare slug "2020" — no IoT sibling has that
        # exact slug, so the linker correctly produces no edge.
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) "
            "VALUES (2, 1, 'lisa')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id) VALUES (20, 2)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (200, 20, '2020', 'LISA 2020')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, source_register_id, slug) "
            "VALUES (2, '44', 1, 'socbidrhb')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id) "
            "VALUES (2000, 2, 20, 200, 44)"
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (2000, 'SocBidrHB')"
        )
        conn.commit()

        n = link_consumer_side_bindings(conn)
        assert n == 0
        row = conn.execute(
            "SELECT via_source_id FROM variable_instance WHERE cvid = 2000"
        ).fetchone()
        assert row["via_source_id"] is None

    def test_linker_keys_on_stored_slug_not_delivery_column(self, tmp_path: Path):
        """A2.1.5 (Codex P1): two source variables sharing a generic delivery
        column (`Kolumn1`) get DISTINCT stored slugs (name-fallback). The linker
        must key on `variable.slug`, not `derive_variable_slug(kolumnnamn)` —
        otherwise both collapse to `kolumn1` and the consumer attaches to the
        wrong source cvid.
        """
        import sqlite3 as _sql

        from reg_meta_build.db import DDL, link_consumer_side_bindings, seed_providers

        conn = _sql.connect(":memory:")
        conn.row_factory = _sql.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (1, 1, 'src')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id) VALUES (10, 1)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (100, 10, '2018', 'Src 2018')"
        )
        # Two source variables, both delivered as `Kolumn1`, distinct stored slugs.
        conn.executemany(
            "INSERT INTO variable (register_id, provider_key, slug) VALUES (?, ?, ?)",
            [(1, "44", "kolumn1-a"), (1, "55", "kolumn1-b")],
        )
        conn.executemany(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id) VALUES (?, ?, ?, ?, ?)",
            [(1000, 1, 10, 100, 44), (1001, 1, 10, 100, 55)],
        )
        conn.executemany(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
            [(1000, "Kolumn1"), (1001, "Kolumn1")],
        )
        # Consumer of the SECOND source variable (stored slug kolumn1-b).
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (2, 1, 'cons')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id) VALUES (20, 2)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (200, 20, '2018', 'Cons 2018')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, source_register_id, slug) "
            "VALUES (2, '55', 1, 'kolumn1-b')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id) "
            "VALUES (2000, 2, 20, 200, 55)"
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (2000, 'Kolumn1')"
        )
        conn.commit()

        n = link_consumer_side_bindings(conn)
        assert n == 1
        # Links to cvid 1001 (kolumn1-b), NOT 1000 (kolumn1-a). The old
        # derive-from-kolumnnamn keying collapsed both to `kolumn1` → cvid 1000.
        row = conn.execute(
            "SELECT via_source_id FROM variable_instance WHERE cvid = 2000"
        ).fetchone()
        assert row["via_source_id"] == 1001

    def test_code_variable_map_populated(self, db_conn: sqlite3.Connection):
        """code_variable_map should have distinct (code, register, variable) combos."""
        count = db_conn.execute("SELECT COUNT(*) FROM code_variable_map").fetchone()[0]
        # Kön: 2 codes × 2 registers (reg 1, reg 2; both have var_id 44) = 4
        # cvid 2002 (var_id 300): ("2","Övriga civilstånd") = 1
        # cvid 2003 (var_id 301): ("","Uppgift okänd") = 1
        assert count == 6

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
        """A1.2: TestVar (register_id=1, var_id=100) has kanslig_variabel='Ja'
        in unika_summary → is_sensitive=1, is_identifier=0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '100'"
        ).fetchone()
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0

    def test_sensitivity_kanslig_variabel_ibland(self, db_conn: sqlite3.Connection):
        """A1.2: ÅÄÖVar (register_id=1, var_id=200) has only
        kanslig_variabel_ibland='Ja' in unika_summary — the "22 edge cases"
        fold into is_sensitive per the mapping rule."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '200'"
        ).fetchone()
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0

    def test_sensitivity_identitetsvariabel(self, db_conn: sqlite3.Connection):
        """A1.2: UniqueVar (register_id=2, var_id=300) has identitetsvariabel='Ja'
        in unika_summary → is_identifier=1. The kanslig columns are 'Nej', so
        is_sensitive stays 0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 2 AND provider_key = '300'"
        ).fetchone()
        assert row["is_sensitive"] == 0
        assert row["is_identifier"] == 1

    def test_sensitivity_all_nej(self, db_conn: sqlite3.Connection):
        """A1.2 negative case: Kön (register_id=1, var_id=44) has all three
        unika_summary flags = 'Nej' → both columns stay 0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND provider_key = '44'"
        ).fetchone()
        assert row["is_sensitive"] == 0
        assert row["is_identifier"] == 0

    def test_sensitivity_no_unika_row(self, db_conn: sqlite3.Connection):
        """A1.2: variables without a matching unika_summary row default to 0
        (the DDL DEFAULT). Several fixture variables (ParenVar=2/301,
        ExternVar=2/302) have no unika_summary entry — they must stay 0/0."""
        rows = db_conn.execute(
            "SELECT register_id, CAST(provider_key AS INTEGER) AS var_id, "
            "is_sensitive, is_identifier "
            "FROM variable WHERE (register_id, provider_key) IN ((2, '301'), (2, '302'))"
        ).fetchall()
        assert len(rows) == 2
        for row in rows:
            assert row["is_sensitive"] == 0, row["var_id"]
            assert row["is_identifier"] == 0, row["var_id"]

    # ------------------------------------------------------------------
    # A2.1 — variable_state coalescer
    # ------------------------------------------------------------------

    def test_variable_state_rows_present(self, db_conn: sqlite3.Connection):
        """A2.1: the coalescer materializes at least one variable_state row
        per `(register_id, register_variant_id, var_id)` that has any
        `variable_instance` row. Sanity check against silent regressions
        where the coalescer never runs or only handles a subset."""
        # Distinct (register_id, register_variant_id, var_id) triples in instance.
        triples = db_conn.execute(
            "SELECT DISTINCT register_id, register_variant_id, var_id FROM variable_instance"
        ).fetchall()
        assert len(triples) > 0
        for t in triples:
            n = db_conn.execute(
                "SELECT COUNT(*) FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = ? AND vs.register_variant_id = ? AND v.provider_key = CAST(? AS TEXT)",
                (t["register_id"], t["register_variant_id"], t["var_id"]),
            ).fetchone()[0]
            assert n >= 1, f"no variable_state for {tuple(t)}"

    def test_variable_state_valid_from_to_full_iso(self, db_conn: sqlite3.Connection):
        """§5.1: every valid_from / valid_to is a 10-char YYYY-MM-DD string.
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
        lifetime — per the §5.1 non-overlap invariant and the Codex P1
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
        """§5.1: delivery_column_name on variable_state is the denormalized
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
        variable_state — it's the §5.7 multi-vintage discriminator that
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
            "SELECT variabelnamn FROM identifier_semantics WHERE var_id = 44"
        ).fetchone()
        assert row["variabelnamn"] == "Kön"

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
        # against them (PROVIDER_ID_SCB = 1, PROVIDER_ID_SOS = 2).
        rows = db_conn.execute(
            "SELECT provider_id, slug, name FROM provider ORDER BY provider_id"
        ).fetchall()
        assert [(r["provider_id"], r["slug"]) for r in rows] == [
            (1, "scb"),
            (2, "sos"),
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
        # registers + variants; version slugs auto-derive from YYYY names.
        # Strict-built DBs must have every slug populated — `populate_slugs`
        # raises otherwise — so this also guards the strict invariant.
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
        assert (
            db_conn.execute(
                "SELECT COUNT(*) FROM register_version WHERE slug IS NULL"
            ).fetchone()[0]
            == 0
        )

    def test_no_synthetic_default_variant_rows_persisted(
        self, db_conn: sqlite3.Connection
    ):
        # §5.1: the `_default` placeholder for variant-less registers is
        # synthesized at FQID-resolve time (catalog.py), never persisted.
        # Every register_variant row in the DB must be a real source row
        # — i.e. `name` (renamed from `registervariantnamn` per §5.11) populated.
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

    def test_identical_sets_share_value_set_id(self, db_conn: sqlite3.Connection):
        # cvids 1003 and 2001 both end up with {Man, Kvinna} after projection
        # (default fixture VALID_DATES_ROWS covers their years).
        rows = db_conn.execute(
            "SELECT cvid, value_set_id FROM variable_instance "
            "WHERE cvid IN (1003, 2001)"
        ).fetchall()
        ids = {r["cvid"]: r["value_set_id"] for r in rows}
        assert ids[1003] is not None
        assert ids[1003] == ids[2001]

    def test_different_sets_get_different_ids(self, db_conn: sqlite3.Connection):
        # cvid 2002 has {Övriga civilstånd}; cvid 2003 has {Uppgift okänd};
        # different sets → different ids.
        rows = db_conn.execute(
            "SELECT cvid, value_set_id FROM variable_instance "
            "WHERE cvid IN (2002, 2003)"
        ).fetchall()
        ids = {r["cvid"]: r["value_set_id"] for r in rows}
        assert ids[2002] is not None
        assert ids[2003] is not None
        assert ids[2002] != ids[2003]

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
        codes = conn.execute(
            "SELECT vc.code FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 1001 ORDER BY vc.code"
        ).fetchall()
        conn.close()
        # Man is excluded (window 2030+ doesn't cover cvid year 2020).
        # Kvinna is included (untracked → always-valid).
        assert [r["code"] for r in codes] == ["2"]

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
        codes = conn.execute(
            "SELECT vc.code FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 1001 ORDER BY vc.code"
        ).fetchall()
        conn.close()
        assert [r["code"] for r in codes] == ["1", "2"]

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
        codes = conn.execute(
            "SELECT vc.code FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 1001 ORDER BY vc.code"
        ).fetchall()
        conn.close()
        assert [r["code"] for r in codes] == ["1"]

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
        codes = conn.execute(
            "SELECT vc.code FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 9001 ORDER BY vc.code"
        ).fetchall()
        conn.close()
        # Yearless cvids fall back to the historical union — the tracked
        # window's exclusion does NOT apply because there's no year to test.
        assert [r["code"] for r in codes] == ["1"]

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
        # Man should NOT be in cvid 1001's value_set (tracked window 2030+
        # doesn't cover year 2020; the untracked sibling 8005 doesn't relax it).
        codes = conn.execute(
            "SELECT vc.code FROM variable_instance vi "
            "LEFT JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "LEFT JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = 1001"
        ).fetchall()
        conn.close()
        # Either no codes (value_set_id NULL because all union excluded), or
        # vardekod is None from the LEFT JOIN. The "Man" code must not appear.
        kods = [r["code"] for r in codes if r["code"] is not None]
        assert "1" not in kods


class TestSameAsBuildIntegration:
    """End-to-end coverage that `build_db` correctly wires up
    `materialize_same_as_edges` (§5.5): tables created, populated when the
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
        from reg_meta.catalog import Catalog, ResolvedVariableBinding

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
            r = Catalog(conn).resolve("scb/testreg/individer/2020/legacy-kon")
            assert isinstance(r, ResolvedVariableBinding)
            assert r.via_same_as is not None
            assert len(r.via_same_as) == 1
            assert str(r.via_same_as[0]) == "scb/testreg/individer/2020/kon"
            # Caller's FQID preserved on the returned record.
            assert str(r.fqid) == "scb/testreg/individer/2020/legacy-kon"
            assert r.delivery_column_name == "Kon"
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
    """A1.1 / §5.11: SCB ships `VariabelOperationell_definition` as a refinement
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
        """§5.1 invariant: variable_state rows for the same variable are
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
    """A2.3: succession edges materialized from `timeseries_event` (§5.5).

    Reworked onto the two-level model: `variable_replaced_by` is **variable
    grain** — 3-part `(provider, register, variable)` endpoints, no variant —
    and the variable slot carries the STORED `variable.slug` (A2.1.5 §5.3), not
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
    def _write_slugs(slug_dir: Path) -> None:
        """Minimal slug TOML for the two fixture registers + variants. Variable
        slugs auto-derive via `populate_variable_slugs` into scb.auto.toml."""
        (slug_dir / "scb.toml").write_text(
            '[register."1"]\nslug = "testreg"\n'
            '[register."2"]\nslug = "otherreg"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            '[register_variant."2.20"]\nslug = "foretag"\n',
            encoding="utf-8",
        )
        (slug_dir / "classifications.toml").write_text("", encoding="utf-8")

    @classmethod
    def _build(
        cls,
        tmp_path: Path,
        timeseries_rows: list[str],
        registerinformation_rows: list[str] | None = None,
    ) -> Path:
        """Build a DB with custom Timeseries.csv rows. Returns the DB path.

        ``registerinformation_rows=None`` uses the standard fixture; pass a
        custom list (e.g. to inject a §5.7 triage split) to vary the variables.
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
        cls._write_slugs(slug_dir)
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
        """A §5.7 triage SPLIT (A2.2, #139) makes one (register, var_id) map to
        several sibling variables sharing `provider_key`. A bare `Variabel`
        succession id carries no discriminator, so it can't pick a sibling and
        is skipped under `n_skipped_ambiguous_variable`, distinct from a plain
        unresolved id. (The `AktuellVariabel` cvid grain *can* disambiguate via
        its delivery column — see `test_split_cvid_resolved_via_delivery_column`.)

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
            assert sibs == 2, "fixture should produce a §5.7 triage split"
            assert (
                conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0]
                == 0
            )
            stats = self._stats(conn)
            assert stats["n_skipped_ambiguous_variable"] == 1
            assert stats["n_skipped_unresolved"] == 0
        finally:
            conn.close()

    def test_split_cvid_resolved_via_delivery_column(self, tmp_path: Path) -> None:
        """An `AktuellVariabel` (cvid) succession event over an A2.2-split var_id
        DOES resolve — unlike the bare `Variabel` id above. The cvid names one
        instance, and split siblings own disjoint delivery columns, so the cvid's
        column (`variable_alias`) selects the sibling. No skip; the edge carries
        that sibling's stored slug.

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
        (`_resolve_binding_direct` reads the same column). Asserted at the DB
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
