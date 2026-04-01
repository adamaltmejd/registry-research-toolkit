"""Tests for build-db pipeline (Phase 1)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from regmeta.db import build_db, open_db, get_manifest, _decode_cp1252
from regmeta.errors import RegmetaError

from _csv_fixtures import (
    REGISTERINFORMATION_HEADER,
    REGISTERINFORMATION_ROWS,
    write_csv,
)


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
        assert manifest["schema_version"] == "2.1.0"
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
            "SELECT kolumnnamn FROM variable_alias WHERE cvid = 1002 ORDER BY kolumnnamn"
        ).fetchall()
        assert [a[0] for a in aliases] == ["TestCol", "TestKolumn"]

    def test_value_items_filtered(self, db_conn: sqlite3.Connection):
        """Value items for unknown CVID 9999 should be excluded."""
        count = db_conn.execute(
            "SELECT COUNT(*) FROM cvid_value_code WHERE cvid = 9999"
        ).fetchone()[0]
        assert count == 0

    def test_value_items_present(self, db_conn: sqlite3.Connection):
        """Deduplicated junction rows for known CVIDs should be imported."""
        count = db_conn.execute("SELECT COUNT(*) FROM cvid_value_code").fetchone()[0]
        # 2 codes (Man, Kvinna) × 3 CVIDs (1001, 1003, 2001) = 6 distinct pairs
        assert count == 6

    def test_value_code_deduplicated(self, db_conn: sqlite3.Connection):
        """Value codes should be deduplicated across CVIDs."""
        count = db_conn.execute("SELECT COUNT(*) FROM value_code").fetchone()[0]
        # Codes: (1, Man), (2, Kvinna) = 2 unique (9999 CVID filtered out)
        assert count == 2

    def test_value_set_info_on_instance(self, db_conn: sqlite3.Connection):
        """Variable instances with values should have vardemangdsversion/niva set."""
        row = db_conn.execute(
            "SELECT vardemangdsversion, vardemangdsniva FROM variable_instance "
            "WHERE cvid = 1001"
        ).fetchone()
        assert row["vardemangdsversion"] == "Kön"
        assert row["vardemangdsniva"] == "1"

    def test_validity_dates_imported(self, db_conn: sqlite3.Connection):
        """Validity date ranges should be imported per item_id."""
        count = db_conn.execute("SELECT COUNT(*) FROM value_item_validity").fetchone()[
            0
        ]
        assert count == 2  # Items 5001 and 5003
        row = db_conn.execute(
            "SELECT * FROM value_item_validity WHERE item_id = 5001"
        ).fetchone()
        assert row["valid_from"] == "2000-01-01"
        assert row["valid_to"] == "2010-12-31"
        row2 = db_conn.execute(
            "SELECT * FROM value_item_validity WHERE item_id = 5003"
        ).fetchone()
        assert row2["valid_from"] == "2015-01-01"
        assert row2["valid_to"] == "2025-12-31"

    def test_value_item_populated(self, db_conn: sqlite3.Connection):
        """value_item should contain only items with validity records."""
        rows = db_conn.execute(
            "SELECT item_id, cvid, code_id FROM value_item ORDER BY item_id, cvid"
        ).fetchall()
        # 5001 appears for CVIDs 1001, 1003, 2001; 5003 only for CVID 1001
        item_cvids = [(r["item_id"], r["cvid"]) for r in rows]
        assert (5001, 1001) in item_cvids
        assert (5001, 1003) in item_cvids
        assert (5001, 2001) in item_cvids
        assert (5003, 1001) in item_cvids
        assert len(rows) == 4

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

    def test_code_variable_map_populated(self, db_conn: sqlite3.Connection):
        """code_variable_map should have distinct (code, register, variable) combos."""
        count = db_conn.execute("SELECT COUNT(*) FROM code_variable_map").fetchone()[0]
        # 2 codes × 2 registers (reg 1 and reg 2 both have var_id 44) = 4
        assert count == 4

    def test_unika_joined(self, db_conn: sqlite3.Connection):
        count = db_conn.execute("SELECT COUNT(*) FROM unika_summary").fetchone()[0]
        assert count == 3

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

    def test_atomic_replace(self, fixture_db: Path):
        """Rebuilding should replace the DB atomically."""
        csv_dir = fixture_db.parent.parent / "csv_rebuild"
        csv_dir.mkdir(exist_ok=True)
        write_csv(
            csv_dir / "Registerinformation.csv",
            REGISTERINFORMATION_HEADER,
            REGISTERINFORMATION_ROWS[:1],
        )

        db_dir = fixture_db.parent.parent / "db_rebuild"
        db_dir.mkdir(exist_ok=True)

        result = build_db(csv_dir=csv_dir, db_dir=db_dir)
        assert Path(result["db_path"]).exists()

        # Rebuild with same data should work
        result2 = build_db(csv_dir=csv_dir, db_dir=db_dir)
        assert Path(result2["db_path"]).exists()


class TestBuildDbErrors:
    def test_missing_csv_dir(self, tmp_path: Path):
        with pytest.raises(RegmetaError) as exc_info:
            build_db(csv_dir=tmp_path / "nonexistent", db_dir=tmp_path)
        assert exc_info.value.code == "csv_dir_not_found"

    def test_missing_backbone(self, tmp_path: Path):
        csv_dir = tmp_path / "empty_csv"
        csv_dir.mkdir()
        with pytest.raises(RegmetaError) as exc_info:
            build_db(csv_dir=csv_dir, db_dir=tmp_path)
        assert exc_info.value.code == "csv_missing_backbone"

    def test_empty_csv(self, tmp_path: Path):
        csv_dir = tmp_path / "bad_csv"
        csv_dir.mkdir()
        (csv_dir / "Registerinformation.csv").write_bytes(b"")
        with pytest.raises(RegmetaError) as exc_info:
            build_db(csv_dir=csv_dir, db_dir=tmp_path)
        assert exc_info.value.code == "csv_empty"

    def test_bad_header(self, tmp_path: Path):
        csv_dir = tmp_path / "bad_header"
        csv_dir.mkdir()
        (csv_dir / "Registerinformation.csv").write_bytes(b"Wrong|Header\r\n")
        with pytest.raises(RegmetaError) as exc_info:
            build_db(csv_dir=csv_dir, db_dir=tmp_path)
        assert exc_info.value.code == "csv_bad_header"

    def test_db_not_found(self, tmp_path: Path):
        with pytest.raises(RegmetaError) as exc_info:
            open_db(tmp_path / "nonexistent.db")
        assert exc_info.value.code == "db_not_found"
