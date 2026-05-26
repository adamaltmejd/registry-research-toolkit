"""Tests for build-db pipeline (Phase 1)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from _csv_fixtures import (
    PIPE,
    REGISTERINFORMATION_HEADER,
    REGISTERINFORMATION_ROWS,
    VARDEMANGDER_REAL_ROWS,
    _ri_row,
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
            "WHERE register_id = 2 AND var_id = 44"
        ).fetchone()
        assert row["source_register_id"] == 1
        assert row["source_label"] == "TESTREG"

    def test_source_resolved_parens(self, db_conn: sqlite3.Connection):
        """OTHERREG ParenVar has kalla with parenthesized abbreviation."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND var_id = 301"
        ).fetchone()
        assert row["source_register_id"] == 1
        assert row["source_label"] == "TESTREG"

    def test_source_null_for_own_variables(self, db_conn: sqlite3.Connection):
        """TESTREG's own variables have no source."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 1 AND var_id = 44"
        ).fetchone()
        assert row["source_register_id"] is None
        assert row["source_label"] is None

    def test_source_unresolved_stores_raw_text(self, db_conn: sqlite3.Connection):
        """ExternVar has kalla=Försäkringskassan which doesn't match any register."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND var_id = 302"
        ).fetchone()
        assert row["source_register_id"] is None
        assert row["source_label"] == "Försäkringskassan"

    def test_source_null_for_no_kalla(self, db_conn: sqlite3.Connection):
        """UniqueVar has no kalla — both source fields should be NULL."""
        row = db_conn.execute(
            "SELECT source_register_id, source_label FROM variable "
            "WHERE register_id = 2 AND var_id = 300"
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
            "INSERT INTO register_variant (regvar_id, register_id) VALUES (10, 1)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (100, 10, '2018', 'LISA 2018 huvudfil'),"
            "       (101, 10, 'tillagg-2018', 'LISA 2018 tilläggsfil')"
        )
        conn.execute("INSERT INTO variable (register_id, var_id) VALUES (1, 44)")
        conn.executemany(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id) VALUES (?, ?, ?, ?, ?)",
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
            "INSERT INTO register_variant (regvar_id, register_id) VALUES (20, 2)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (200, 20, 'tillagg-2018', 'Cons 2018 tilläggsfil')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, var_id, source_register_id) "
            "VALUES (2, 44, 1)"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id) "
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
            "INSERT INTO register_variant (regvar_id, register_id) VALUES (10, 1)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (100, 10, 'preliminar-version-2020', 'IoT preliminär version 2020'),"
            "       (101, 10, 'slutlig-version-2020', 'IoT slutlig version 2020')"
        )
        conn.execute("INSERT INTO variable (register_id, var_id) VALUES (1, 44)")
        conn.executemany(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id) VALUES (?, ?, ?, ?, ?)",
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
            "INSERT INTO register_variant (regvar_id, register_id) VALUES (20, 2)"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (200, 20, '2020', 'LISA 2020')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, var_id, source_register_id) "
            "VALUES (2, 44, 1)"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id) "
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

    def test_code_variable_map_populated(self, db_conn: sqlite3.Connection):
        """code_variable_map should have distinct (code, register, variable) combos."""
        count = db_conn.execute("SELECT COUNT(*) FROM code_variable_map").fetchone()[0]
        # Kön: 2 codes × 2 registers (reg 1, reg 2; both have var_id 44) = 4
        # cvid 2002 (var_id 300): ("2","Övriga civilstånd") = 1
        # cvid 2003 (var_id 301): ("","Uppgift okänd") = 1
        assert count == 6

    def test_unika_joined(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM unika_summary").fetchone()[0]
        # 3 baseline rows (TESTREG Kön, TESTREG TestVar, OTHERREG Kön)
        # + 2 A1.2 sensitivity-flag fixtures (TESTREG ÅÄÖVar, OTHERREG UniqueVar).
        assert count == 5

    def test_sensitivity_kanslig_variabel(self, db_conn: sqlite3.Connection):
        """A1.2: TestVar (register_id=1, var_id=100) has kanslig_variabel='Ja'
        in unika_summary → is_sensitive=1, is_identifier=0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND var_id = 100"
        ).fetchone()
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0

    def test_sensitivity_kanslig_variabel_ibland(self, db_conn: sqlite3.Connection):
        """A1.2: ÅÄÖVar (register_id=1, var_id=200) has only
        kanslig_variabel_ibland='Ja' in unika_summary — the "22 edge cases"
        fold into is_sensitive per the mapping rule."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND var_id = 200"
        ).fetchone()
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0

    def test_sensitivity_identitetsvariabel(self, db_conn: sqlite3.Connection):
        """A1.2: UniqueVar (register_id=2, var_id=300) has identitetsvariabel='Ja'
        in unika_summary → is_identifier=1. The kanslig columns are 'Nej', so
        is_sensitive stays 0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 2 AND var_id = 300"
        ).fetchone()
        assert row["is_sensitive"] == 0
        assert row["is_identifier"] == 1

    def test_sensitivity_all_nej(self, db_conn: sqlite3.Connection):
        """A1.2 negative case: Kön (register_id=1, var_id=44) has all three
        unika_summary flags = 'Nej' → both columns stay 0."""
        row = db_conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable "
            "WHERE register_id = 1 AND var_id = 44"
        ).fetchone()
        assert row["is_sensitive"] == 0
        assert row["is_identifier"] == 0

    def test_sensitivity_no_unika_row(self, db_conn: sqlite3.Connection):
        """A1.2: variables without a matching unika_summary row default to 0
        (the DDL DEFAULT). Several fixture variables (ParenVar=2/301,
        ExternVar=2/302) have no unika_summary entry — they must stay 0/0."""
        rows = db_conn.execute(
            "SELECT register_id, var_id, is_sensitive, is_identifier "
            "FROM variable WHERE (register_id, var_id) IN ((2, 301), (2, 302))"
        ).fetchall()
        assert len(rows) == 2
        for row in rows:
            assert row["is_sensitive"] == 0, row["var_id"]
            assert row["is_identifier"] == 0, row["var_id"]

    def test_identifierare_imported(self, db_conn: sqlite3.Connection):
        row = db_conn.execute(
            "SELECT variabelnamn FROM identifier_semantics WHERE var_id = 44"
        ).fetchone()
        assert row["variabelnamn"] == "Kön"

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
            "SELECT var_id FROM variable_fts WHERE variable_fts MATCH 'testvariabel'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["var_id"] == 100

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
                "WHERE register_id = 999 AND var_id = 777"
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
