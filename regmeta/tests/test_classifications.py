"""Tests for classification seed loading, build-time population, and CLI."""

from __future__ import annotations

import json
import subprocess
import sqlite3
import sys
from pathlib import Path

import pytest

from regmeta.classifications import load_seed, load_valid_codes
from regmeta.db import build_db
from regmeta.errors import RegmetaError

from _csv_fixtures import PIPE, write_scb_input


# CVID 1004 has vardemangdsversion = "Kon-2" (a fake successor) so we can
# exercise the supersedes chain end to end. CVID 9999 ("Unknown") still
# falls outside the backbone and never makes it into cvid_value_code.
EXTENDED_VARDEMANGDER_ROWS = [
    PIPE.join(["Kön", "1", "1", "Man", "1001", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "1001", "5002"]),
    PIPE.join(["Kön", "1", "1", "Man", "1003", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "1003", "5002"]),
    PIPE.join(["Kön", "1", "1", "Man", "2001", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "2001", "5002"]),
    # CVID 1004 (Kön version 2022) gets the successor classification.
    PIPE.join(["Kon-2", "1", "10", "Female", "1004", ""]),
    PIPE.join(["Kon-2", "1", "20", "Male", "1004", ""]),
    PIPE.join(["Kon-2", "1", "30", "Other", "1004", ""]),
    PIPE.join(["Unknown", "1", "99", "Phantom", "9999", "5099"]),
]


# Two classifications: TESTKON tags CVIDs 1001/1003/2001 (vardemangdsversion
# "Kön"); TESTKON2 supersedes TESTKON and tags CVID 1004 (vardemangdsversion
# "Kon-2"). With both pointing at real strings the build invariants pass.
TEST_SEED_TOML = """\
[[classification]]
short_name = "TESTKON"
name = "Test classification for gender codes"
name_en = "Test"
publisher = "TEST"
version = "1"
valid_from = 2000
url = "https://example.com/"
vardemangdsversion = ["Kön"]

[[classification]]
short_name = "TESTKON2"
name = "Successor"
publisher = "TEST"
version = "2"
valid_from = 2022
supersedes = "TESTKON"
vardemangdsversion = ["Kon-2"]
"""


def _make_input_dir(tmp_path: Path) -> Path:
    """Create <tmp_path>/input/SCB/ with the standard test fixture CSVs."""
    input_dir = tmp_path / "input"
    write_scb_input(input_dir, vardemangder_rows=EXTENDED_VARDEMANGDER_ROWS)
    return input_dir


# ---------------------------------------------------------------------------
# Seed file loading and validation
# ---------------------------------------------------------------------------


class TestLoadSeed:
    def test_valid_seed(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'vardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        entries = load_seed(seed)
        assert len(entries) == 1
        assert entries[0]["short_name"] == "A"

    def test_empty_seed_rejected(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text("", encoding="utf-8")
        with pytest.raises(RegmetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_empty"

    def test_missing_required_field(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n', encoding="utf-8"
        )
        with pytest.raises(RegmetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"

    def test_duplicate_short_name(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'vardemangdsversion = ["x"]\n'
            '[[classification]]\nshort_name = "A"\nname = "Other"\n'
            'vardemangdsversion = ["y"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegmetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "Duplicate" in ei.value.message

    def test_duplicate_vardemangdsversion(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'vardemangdsversion = ["x"]\n'
            '[[classification]]\nshort_name = "B"\nname = "B"\n'
            'vardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegmetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "belongs to exactly one" in ei.value.remediation

    def test_supersedes_unknown_fails(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'supersedes = "GHOST"\n'
            'vardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegmetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "GHOST" in ei.value.message

    def test_valid_codes_file_must_be_string(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            "valid_codes_file = 123\n"
            'vardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegmetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "valid_codes_file" in ei.value.message


# ---------------------------------------------------------------------------
# Valid-codes CSV loader
# ---------------------------------------------------------------------------


class TestLoadValidCodes:
    def _csv(self, tmp_path: Path, body: str) -> Path:
        path = tmp_path / "codes.csv"
        path.write_text(body, encoding="utf-8")
        return path

    def test_loads_simple(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\nB,Bravo\n")
        assert load_valid_codes(path) == {"A": "Alpha", "B": "Bravo"}

    def test_strips_whitespace(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\n  A  ,  Alpha label  \n")
        assert load_valid_codes(path) == {"A": "Alpha label"}

    def test_skips_blank_lines(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\n\n,\nB,Bravo\n")
        assert load_valid_codes(path) == {"A": "Alpha", "B": "Bravo"}

    def test_bad_header(self, tmp_path: Path):
        path = self._csv(tmp_path, "code,label\nA,Alpha\n")
        with pytest.raises(RegmetaError) as ei:
            load_valid_codes(path)
        assert ei.value.code == "classification_csv_invalid"

    def test_duplicate_code(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\nA,Apple\n")
        with pytest.raises(RegmetaError) as ei:
            load_valid_codes(path)
        assert "duplicate" in ei.value.message.lower()

    def test_empty_data(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\n")
        with pytest.raises(RegmetaError):
            load_valid_codes(path)


# ---------------------------------------------------------------------------
# Build-time population against test CSV fixtures
# ---------------------------------------------------------------------------


class TestPopulateClassifications:
    def _build_with_seed(self, tmp_path: Path, seed_toml: str) -> tuple[Path, Path]:
        input_dir = _make_input_dir(tmp_path)

        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed)
        return db_dir / "regmeta.db", seed

    def test_classification_inserted(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT short_name, publisher, code_count FROM classification "
            "ORDER BY short_name"
        ).fetchall()
        by_name = {r["short_name"]: r for r in rows}
        assert set(by_name) == {"TESTKON", "TESTKON2"}
        assert by_name["TESTKON"]["publisher"] == "TEST"
        # TESTKON matches 3 CVIDs that share codes (1, "Man") and (2, "Kvinna")
        # → 2 deduped codes in classification_code.
        assert by_name["TESTKON"]["code_count"] == 2

    def test_variable_instance_tagged(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        tagged = conn.execute(
            "SELECT COUNT(*) AS n FROM variable_instance WHERE classification_id IS NOT NULL"
        ).fetchone()["n"]
        # Three CVIDs map to TESTKON (Kön: 1001, 1003, 2001) plus one to
        # TESTKON2 (Kon-2: 1004) — four tagged in total.
        assert tagged == 4

    def test_level_computed_from_code_length(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT vc.vardekod, cc.level "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'TESTKON' "
            "ORDER BY vc.vardekod"
        ).fetchall()
        # Codes are "1" and "2", both numeric, both length 1.
        assert [(r["vardekod"], r["level"]) for r in rows] == [("1", 1), ("2", 1)]

    def test_supersedes_resolved(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT s.short_name AS predecessor "
            "FROM classification c "
            "JOIN classification s ON c.supersedes_id = s.id "
            "WHERE c.short_name = 'TESTKON2'"
        ).fetchone()
        assert row["predecessor"] == "TESTKON"

    def test_classification_fts_populated(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        rows = conn.execute(
            "SELECT short_name FROM classification_fts "
            "WHERE classification_fts MATCH 'Test'"
        ).fetchall()
        assert len(rows) >= 1

    def test_seed_drift_fails_build(self, tmp_path: Path):
        seed = (
            '[[classification]]\nshort_name = "GHOST"\nname = "No such label"\n'
            'vardemangdsversion = ["this-string-never-appears"]\n'
        )
        with pytest.raises(RegmetaError) as ei:
            self._build_with_seed(tmp_path, seed)
        assert ei.value.code == "classification_seed_drift"

    def test_valid_codes_csv_marks_codes(self, tmp_path: Path):
        """A canonical CSV with one observed code and one unobserved code
        should mark observed=valid, observed-only-non-canonical=invalid, and
        insert canonical-but-unobserved as a new value_code.
        """
        # CSV: '1' is observed (Man), 'Z' is canonical-but-unobserved.
        # '2' (Kvinna) is observed-only — not in CSV → is_valid=0.
        input_dir = _make_input_dir(tmp_path)
        cls_dir = input_dir / "classifications"
        cls_dir.mkdir()
        (cls_dir / "testkon.csv").write_text(
            "vardekod,vardebenamning\n1,Man\nZ,Other\n", encoding="utf-8"
        )
        seed_toml = (
            '[[classification]]\nshort_name = "TESTKON"\nname = "Test"\n'
            'valid_codes_file = "testkon.csv"\n'
            'vardemangdsversion = ["Kön"]\n'
        )

        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            seed_path=seed,
        )

        conn = sqlite3.connect(db_dir / "regmeta.db")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT vc.vardekod, cc.is_valid "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'TESTKON' "
            "ORDER BY vc.vardekod"
        ).fetchall()
        by_code = {r["vardekod"]: r["is_valid"] for r in rows}
        assert by_code == {"1": 1, "2": 0, "Z": 1}

        cnt = conn.execute(
            "SELECT valid_code_count FROM classification WHERE short_name='TESTKON'"
        ).fetchone()[0]
        assert cnt == 2  # '1' and 'Z'

    def test_valid_codes_file_missing_fails(self, tmp_path: Path):
        seed_toml = (
            '[[classification]]\nshort_name = "TESTKON"\nname = "Test"\n'
            'valid_codes_file = "nope.csv"\n'
            'vardemangdsversion = ["Kön"]\n'
        )
        input_dir = _make_input_dir(tmp_path)
        (input_dir / "classifications").mkdir()
        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        with pytest.raises(RegmetaError) as ei:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                seed_path=seed,
            )
        assert ei.value.code == "classification_csv_not_found"

    def test_valid_codes_file_no_dir_fails(self, tmp_path: Path):
        seed_toml = (
            '[[classification]]\nshort_name = "TESTKON"\nname = "Test"\n'
            'valid_codes_file = "x.csv"\n'
            'vardemangdsversion = ["Kön"]\n'
        )
        # No <input_dir>/classifications/ subdir → seed entry with
        # valid_codes_file should error.
        input_dir = _make_input_dir(tmp_path)
        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        with pytest.raises(RegmetaError) as ei:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                seed_path=seed,
            )
        assert ei.value.code == "classification_csv_dir_missing"

    def test_no_csv_keeps_is_valid_null(self, tmp_path: Path):
        """Classifications without a CSV: every is_valid is NULL,
        valid_code_count is NULL.
        """
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT is_valid FROM classification_code").fetchall()
        assert all(r["is_valid"] is None for r in rows)
        vcc = conn.execute(
            "SELECT valid_code_count FROM classification WHERE short_name='TESTKON'"
        ).fetchone()[0]
        assert vcc is None

    def test_empty_classification_fails_build(self, tmp_path: Path):
        # "Unknown" is a vardemangdsversion present in VARDEMANGDER_ROWS, but
        # its CVID (9999) isn't in the backbone, so the value code never makes
        # it into cvid_value_code. A seed tagging "Unknown" ends up with zero
        # codes — the build must fail rather than silently ship it.
        seed = (
            '[[classification]]\nshort_name = "GHOST"\nname = "Empty"\n'
            'vardemangdsversion = ["Unknown"]\n'
        )
        with pytest.raises(RegmetaError) as ei:
            self._build_with_seed(tmp_path, seed)
        # Either drift (the "Unknown" vardemangdsversion is dropped during
        # backbone import because its CVID is unknown) or classification_empty.
        assert ei.value.code in {"classification_empty", "classification_seed_drift"}


# ---------------------------------------------------------------------------
# CLI commands — get classification
# ---------------------------------------------------------------------------


def _run_json(db_dir: Path, args: list[str]) -> tuple[dict, int]:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "regmeta",
            "--db",
            str(db_dir),
            "--format",
            "json",
            *args,
        ],
        capture_output=True,
        text=True,
    )
    out = proc.stdout.strip()
    # JSON errors still produce JSON on stdout; just parse.
    return json.loads(out), proc.returncode


@pytest.fixture(scope="module")
def classification_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("cls")
    input_dir = _make_input_dir(tmp)

    seed = tmp / "classifications.toml"
    seed.write_text(TEST_SEED_TOML, encoding="utf-8")

    db_dir = tmp / "db"
    db_dir.mkdir()
    build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed)

    # Query commands require a doc DB alongside.
    from regmeta.doc_db import build_doc_db

    docs_src = tmp / "docs" / "stub"
    docs_src.mkdir(parents=True)
    (docs_src / "Stub.md").write_text(
        "---\nvariable: Stub\ndisplay_name: Stub\ntags:\n  - type/variable\n---\n\nBody.\n",
        encoding="utf-8",
    )
    build_doc_db(tmp / "docs", db_dir)
    return db_dir


class TestCli:
    def test_list(self, classification_db: Path):
        data, code = _run_json(classification_db, ["get", "classification", "--list"])
        assert code == 0
        names = {c["short_name"] for c in data["classifications"]}
        assert names == {"TESTKON", "TESTKON2"}

    def test_by_short_name(self, classification_db: Path):
        data, code = _run_json(classification_db, ["get", "classification", "TESTKON"])
        assert code == 0
        assert data["short_name"] == "TESTKON"
        assert data["code_count"] == 2

    def test_codes(self, classification_db: Path):
        data, code = _run_json(
            classification_db, ["get", "classification", "TESTKON", "--codes"]
        )
        assert code == 0
        codes = data["codes"]
        assert [c["vardekod"] for c in codes] == ["1", "2"]
        assert all(c["level"] == 1 for c in codes)

    def test_only_valid_requires_codes(self, classification_db: Path):
        data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--only-valid"],
        )
        assert code == 2  # EXIT_USAGE

    def test_only_valid_empty_for_no_csv(self, classification_db: Path):
        # The fixture seed has no valid_codes_file, so --only-valid returns []
        # (is_valid is NULL everywhere → no rows match is_valid=1).
        data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--codes", "--only-valid"],
        )
        assert code == 0
        assert data["codes"] == []

    def test_codes_filtered_by_level(self, classification_db: Path):
        data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--codes", "--level", "2"],
        )
        assert code == 0
        # No level-2 codes in TESTKON (all are length 1).
        assert data["codes"] == []

    def test_variables(self, classification_db: Path):
        data, code = _run_json(
            classification_db, ["get", "classification", "TESTKON", "--variables"]
        )
        assert code == 0
        variables = data["variables"]
        # var_id 44 (Kön) appears in two registers in the fixture.
        var_ids = {v["var_id"] for v in variables}
        assert 44 in var_ids

    def test_not_found(self, classification_db: Path):
        data, code = _run_json(
            classification_db, ["get", "classification", "NONEXISTENT"]
        )
        assert code == 16  # EXIT_NOT_FOUND
        assert data["error"]["code"] == "not_found"

    def test_level_requires_codes(self, classification_db: Path):
        data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--level", "1"],
        )
        assert code == 2  # EXIT_USAGE

    def test_list_with_positional_fails(self, classification_db: Path):
        data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--list"],
        )
        assert code == 2  # EXIT_USAGE

    def test_varinfo_includes_classifications(self, classification_db: Path):
        data, code = _run_json(classification_db, ["get", "varinfo", "44"])
        assert code == 0
        variables = data.get("variables", [data])
        # var_id 44 spans TESTKON (early years) and TESTKON2 (year 2022 in
        # TESTREG) — exactly the multi-classification case the schema is
        # designed to handle.
        for v in variables:
            assert "classifications" in v
            names = {c["short_name"] for c in v["classifications"]}
            assert names <= {"TESTKON", "TESTKON2"}
            assert names  # at least one
            for inst in v["instances"]:
                if inst.get("classification"):
                    assert inst["classification"] in {"TESTKON", "TESTKON2"}
