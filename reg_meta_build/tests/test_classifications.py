"""Tests for classification seed loading, build-time population, and CLI."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import PIPE, write_scb_input
from reg_meta.errors import RegMetaError
from reg_meta_build.classifications import load_seed, load_valid_codes
from reg_meta_build.db import build_db

if TYPE_CHECKING:
    from pathlib import Path

# CVID 1004 has vardemangdsversion = "Kon-2" (a fake successor) so we can
# exercise the supersedes chain end to end. CVID 9999 ("Unknown") still
# falls outside the backbone and never makes it into value_set_member.
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
        with pytest.raises(RegMetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_empty"

    def test_missing_required_field(self, tmp_path: Path):
        # short_name + name are required; vardemangdsversion is optional, so
        # omit `name` to trigger the missing-required-field path.
        seed = tmp_path / "c.toml"
        seed.write_text('[[classification]]\nshort_name = "A"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"

    def test_vardemangdsversion_optional(self, tmp_path: Path):
        # An entry with no vardemangdsversion is valid (provider-seeded
        # canonical-codes-only classification — tags no instances).
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n', encoding="utf-8"
        )
        entries = load_seed(seed)
        assert len(entries) == 1
        assert "vardemangdsversion" not in entries[0]

    def test_duplicate_short_name(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'vardemangdsversion = ["x"]\n'
            '[[classification]]\nshort_name = "A"\nname = "Other"\n'
            'vardemangdsversion = ["y"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as ei:
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
        with pytest.raises(RegMetaError) as ei:
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
        with pytest.raises(RegMetaError) as ei:
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
        with pytest.raises(RegMetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "valid_codes_file" in ei.value.message

    def test_provider_must_be_string(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            "provider = 99\n"
            'vardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "provider" in ei.value.message


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
        path = self._csv(tmp_path, "foo,bar\nA,Alpha\n")
        with pytest.raises(RegMetaError) as ei:
            load_valid_codes(path)
        assert ei.value.code == "classification_csv_invalid"

    def test_universal_header_accepted(self, tmp_path: Path):
        # The SOS CSVs ship `code,label` (+ extra trailing columns we ignore).
        path = self._csv(
            tmp_path,
            "code,label,label_en,parent_code\nA,Alpha,Alpha-en,\nB,Bravo,,A\n",
        )
        assert load_valid_codes(path) == {"A": "Alpha", "B": "Bravo"}

    def test_duplicate_code(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\nA,Apple\n")
        with pytest.raises(RegMetaError) as ei:
            load_valid_codes(path)
        assert "duplicate" in ei.value.message.lower()

    def test_empty_data(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\n")
        with pytest.raises(RegMetaError):
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
        build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed, skip_slugs=True)
        return db_dir / "reg_meta.db", seed

    def _populate_direct(
        self,
        tmp_path: Path,
        seed_toml: str,
        csvs: dict[str, str],
        *,
        providers: frozenset[str] | None,
    ) -> tuple[sqlite3.Connection, tuple[int, frozenset[str]]]:
        """Call populate_classifications directly on an empty in-memory schema.

        Lets us assert the (n_seeded, skipped) return tuple and the provider
        gate without the full build_db machinery (which never surfaces the
        return value or accepts providers=None).
        """
        from reg_meta_build.classifications import populate_classifications
        from reg_meta_build.db import DDL

        cls_dir = tmp_path / "cls"
        cls_dir.mkdir(parents=True, exist_ok=True)
        for name, body in csvs.items():
            (cls_dir / name).write_text(body, encoding="utf-8")
        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        result = populate_classifications(
            conn, seed, valid_codes_dir=cls_dir, providers=providers
        )
        return conn, result

    def test_provider_gating_and_return_shape(self, tmp_path: Path):
        """populate_classifications skips provider-tagged entries absent from the
        active set, seeds no-provider entries always, and returns the
        (n_seeded, skipped_short_names) tuple. providers=None seeds everything.
        """
        seed_toml = (
            '[[classification]]\nshort_name = "SCBONLY"\nname = "SCB only"\n'
            'valid_codes_file = "scb.csv"\n'
            '[[classification]]\nshort_name = "SOSENT"\nname = "SOS entry"\n'
            'provider = "sos"\nvalid_codes_file = "sos.csv"\n'
        )
        csvs = {"scb.csv": "code,label\nA,Alpha\n", "sos.csv": "code,label\nX,Xray\n"}

        # providers={scb}: the sos entry is skipped, the no-provider entry seeded.
        conn, (n_seeded, skipped) = self._populate_direct(
            tmp_path / "a", seed_toml, csvs, providers=frozenset({"scb"})
        )
        assert (n_seeded, skipped) == (1, frozenset({"SOSENT"}))
        assert {
            r[0] for r in conn.execute("SELECT short_name FROM classification")
        } == {"SCBONLY"}

        # providers=None: everything seeded, nothing skipped.
        conn2, (n2, skipped2) = self._populate_direct(
            tmp_path / "b", seed_toml, csvs, providers=None
        )
        assert (n2, skipped2) == (2, frozenset())
        assert {
            r[0] for r in conn2.execute("SELECT short_name FROM classification")
        } == {"SCBONLY", "SOSENT"}

    def test_supersedes_across_provider_skip_boundary(self, tmp_path: Path):
        """A seeded entry that supersedes a provider-skipped one inserts without
        KeyError; its supersedes_id stays NULL (the predecessor isn't in the
        build)."""
        seed_toml = (
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'supersedes = "B"\nvalid_codes_file = "a.csv"\n'
            '[[classification]]\nshort_name = "B"\nname = "B"\n'
            'provider = "sos"\nvalid_codes_file = "b.csv"\n'
        )
        csvs = {"a.csv": "code,label\nA,Alpha\n", "b.csv": "code,label\nB,Bravo\n"}
        conn, (n_seeded, skipped) = self._populate_direct(
            tmp_path, seed_toml, csvs, providers=frozenset({"scb"})
        )
        assert n_seeded == 1
        assert "B" in skipped
        row = conn.execute(
            "SELECT short_name, supersedes_id FROM classification"
        ).fetchone()
        assert row[0] == "A"
        assert row[1] is None

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

    def test_variable_state_tagged(self, tmp_path: Path):
        """A2.7: `variable_instance` is dropped before ship; classification
        tagging is backfilled onto `variable_state.classification_id`
        (correlated by (variable_id, value_set_id)). Assert each tagged family
        landed on >= 1 state and both classifications are represented."""
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        # variable_instance is gone from the shipped DB.
        assert (
            conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='variable_instance'"
            ).fetchone()
            is None
        )
        rows = conn.execute(
            "SELECT c.short_name, COUNT(*) AS n_states "
            "FROM variable_state vs "
            "JOIN classification c ON vs.classification_id = c.id "
            "GROUP BY c.short_name ORDER BY c.short_name"
        ).fetchall()
        tagged = {r["short_name"]: r["n_states"] for r in rows}
        # Both seeded classifications tag at least one coalesced state: TESTKON
        # (Kön, the 1001/1003/2001 instances) and TESTKON2 (Kon-2, 1004).
        assert set(tagged) == {"TESTKON", "TESTKON2"}
        assert all(n >= 1 for n in tagged.values())

    def test_classification_linkage_is_stable(self, tmp_path: Path):
        """A4.4e CI proxy for the full-corpus byte-identity gate: the
        provider-blind feed + backfill round-trip through a real `build_db` must
        yield a NON-empty, REPRODUCIBLE variable→classification linkage. The
        orchestrator runs the real-data gate; this guards the small SCB fixture.
        """
        from reg_meta_build.db import dump_classification_linkage

        dir_a, dir_b = tmp_path / "a", tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()
        db1, _ = self._build_with_seed(dir_a, TEST_SEED_TOML)
        db2, _ = self._build_with_seed(dir_b, TEST_SEED_TOML)

        with sqlite3.connect(db1) as c1, sqlite3.connect(db2) as c2:
            linkage1 = dump_classification_linkage(c1)
            linkage2 = dump_classification_linkage(c2)

        assert linkage1, "fixture must produce at least one tagged variable_state"
        assert linkage1 == linkage2
        # Sanity: both seeded classifications appear in the linkage.
        with sqlite3.connect(db1) as conn:
            cls_ids = {
                r[0]
                for r in conn.execute(
                    "SELECT id FROM classification WHERE short_name "
                    "IN ('TESTKON', 'TESTKON2')"
                )
            }
        assert {cid for _, _, cid in linkage1} >= cls_ids

    def test_level_computed_from_code_length(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT vc.code, cc.level "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'TESTKON' "
            "ORDER BY vc.code"
        ).fetchall()
        # Codes are "1" and "2", both numeric, both length 1.
        assert [(r["code"], r["level"]) for r in rows] == [("1", 1), ("2", 1)]

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
        with pytest.raises(RegMetaError) as ei:
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
            skip_slugs=True,
        )

        conn = sqlite3.connect(db_dir / "reg_meta.db")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT vc.code, cc.is_valid "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'TESTKON' "
            "ORDER BY vc.code"
        ).fetchall()
        by_code = {r["code"]: r["is_valid"] for r in rows}
        assert by_code == {"1": 1, "2": 0, "Z": 1}

        cnt = conn.execute(
            "SELECT valid_code_count FROM classification WHERE short_name='TESTKON'"
        ).fetchone()[0]
        assert cnt == 2  # '1' and 'Z'

    def test_provider_seeded_canonical_only_clears_empty_guard(self, tmp_path: Path):
        """The SOS shape: a provider-tagged, active entry with valid_codes_file
        but NO vardemangdsversion seeds its canonical codes from the CSV
        (universal `code,label` header), tags zero instances, and clears the
        classification_empty guard.
        """
        input_dir = _make_input_dir(tmp_path)
        cls_dir = input_dir / "classifications"
        cls_dir.mkdir()
        # `code,label` header (+ extra SOS columns) — the universal shape.
        (cls_dir / "canon.csv").write_text(
            "code,label,label_en\nA01,Alpha,Alpha-en\nB02,Bravo,Bravo-en\n",
            encoding="utf-8",
        )
        # provider="scb" is in the default build's active set, so the entry is
        # seeded (the gate keeps it; the canonical-only path is what's tested).
        seed_toml = (
            '[[classification]]\nshort_name = "CANON"\nname = "Canonical only"\n'
            'provider = "scb"\nvalid_codes_file = "canon.csv"\n'
        )
        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed, skip_slugs=True)

        conn = sqlite3.connect(db_dir / "reg_meta.db")
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT code_count, valid_code_count FROM classification "
            "WHERE short_name = 'CANON'"
        ).fetchone()
        # Both canonical codes seeded from the CSV — clears the empty guard.
        assert (row["code_count"], row["valid_code_count"]) == (2, 2)
        codes = conn.execute(
            "SELECT vc.code, cc.is_valid "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'CANON' ORDER BY vc.code"
        ).fetchall()
        assert [(r["code"], r["is_valid"]) for r in codes] == [("A01", 1), ("B02", 1)]
        # Zero instances tagged: no observed variable_state points at CANON.
        tagged = conn.execute(
            "SELECT COUNT(*) FROM variable_state vs "
            "JOIN classification c ON vs.classification_id = c.id "
            "WHERE c.short_name = 'CANON'"
        ).fetchone()[0]
        assert tagged == 0

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
        with pytest.raises(RegMetaError) as ei:
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
        with pytest.raises(RegMetaError) as ei:
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

    def test_shared_vardekod_binds_canonical_label(self, tmp_path: Path):
        """Two classifications can share a vardekod with different canonical
        labels. The canonical-but-unobserved CC row must reference the
        value_code row whose label matches the *current* classification's
        CSV — not some other classification's label for the same code.

        Reproduces a P1 bug where step 5 picked MIN(code_id) over all
        value_code rows with the matching vardekod, ignoring the label.
        """
        # TESTKON observes code "1" → label "Man" (from EXTENDED_VARDEMANGDER_ROWS).
        # TESTKON2 uses "Kon-2" (codes 10/20/30) — does NOT observe "1".
        # Both classifications declare canonical "1" but with different labels.
        input_dir = _make_input_dir(tmp_path)
        cls_dir = input_dir / "classifications"
        cls_dir.mkdir()
        (cls_dir / "testkon.csv").write_text(
            "vardekod,vardebenamning\n1,Man\n2,Kvinna\n", encoding="utf-8"
        )
        # TESTKON2 canonical "1" → "Stockholm" (deliberately unobserved here);
        # the bug bound it to TESTKON's "Man" label.
        (cls_dir / "testkon2.csv").write_text(
            "vardekod,vardebenamning\n1,Stockholm\n10,Female\n",
            encoding="utf-8",
        )
        seed_toml = (
            '[[classification]]\nshort_name = "TESTKON"\nname = "Test"\n'
            'valid_codes_file = "testkon.csv"\n'
            'vardemangdsversion = ["Kön"]\n'
            '[[classification]]\nshort_name = "TESTKON2"\nname = "Test 2"\n'
            'valid_codes_file = "testkon2.csv"\n'
            'vardemangdsversion = ["Kon-2"]\n'
        )
        seed = tmp_path / "classifications.toml"
        seed.write_text(seed_toml, encoding="utf-8")
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed, skip_slugs=True)

        conn = sqlite3.connect(db_dir / "reg_meta.db")
        conn.row_factory = sqlite3.Row
        # TESTKON2's CC row for vardekod "1" must reference the "Stockholm"
        # value_code, not the "Man" one observed by TESTKON.
        row = conn.execute(
            "SELECT vc.code, vc.label, cc.is_valid "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'TESTKON2' AND vc.code = '1'"
        ).fetchone()
        assert row is not None
        assert row["label"] == "Stockholm"
        assert row["is_valid"] == 1

    def test_valid_code_count_counts_distinct_vardekods(self, tmp_path: Path):
        """valid_code_count should reflect canonical *codes*, not CC rows.

        When value_code holds multiple labels for one vardekod (label drift in
        observed data), step 6 marks every label variant as is_valid=1
        (intentional — validity is per-code). The cached count must still
        report the canonical CSV cardinality, so it uses COUNT(DISTINCT
        vardekod) rather than COUNT(*).
        """
        # Add a second-label variant for code "1": (1, "Man") observed AND
        # (1, "Manlig") observed via CVID 1001. Without the fix this inflates
        # valid_code_count from 2 to 3.
        rows = list(EXTENDED_VARDEMANGDER_ROWS) + [
            PIPE.join(["Kön", "1", "1", "Manlig", "1001", "5010"])
        ]
        input_dir = tmp_path / "input"
        write_scb_input(input_dir, vardemangder_rows=rows)
        cls_dir = input_dir / "classifications"
        cls_dir.mkdir()
        (cls_dir / "testkon.csv").write_text(
            "vardekod,vardebenamning\n1,Man\n2,Kvinna\n", encoding="utf-8"
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
        build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed, skip_slugs=True)

        conn = sqlite3.connect(db_dir / "reg_meta.db")
        conn.row_factory = sqlite3.Row
        # Sanity: three CC rows for TESTKON (one per distinct value_code:
        # "1"/"Man", "1"/"Manlig", "2"/"Kvinna") all with is_valid=1.
        cc_rows = conn.execute(
            "SELECT vc.code, vc.label, cc.is_valid "
            "FROM classification_code cc "
            "JOIN value_code vc ON cc.code_id = vc.code_id "
            "JOIN classification c ON cc.classification_id = c.id "
            "WHERE c.short_name = 'TESTKON' "
            "ORDER BY vc.code, vc.label"
        ).fetchall()
        assert [(r["code"], r["label"], r["is_valid"]) for r in cc_rows] == [
            ("1", "Man", 1),
            ("1", "Manlig", 1),
            ("2", "Kvinna", 1),
        ]
        # valid_code_count must be 2 (distinct canonical vardekods) — NOT 3.
        cnt = conn.execute(
            "SELECT valid_code_count FROM classification WHERE short_name='TESTKON'"
        ).fetchone()[0]
        assert cnt == 2

    def test_multi_successor_does_not_duplicate_listing(self, tmp_path: Path):
        """A classification superseded by more than one successor must appear
        once in list_classifications, with all successor short_names in
        superseded_by. Pre-fix the LEFT JOIN multiplied the parent row.
        """
        from reg_meta.queries import _classification_by_id, list_classifications

        db_path = tmp_path / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        # Minimal schema — only what list_classifications touches.
        conn.executescript(
            """
            CREATE TABLE classification (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                short_name TEXT NOT NULL UNIQUE,
                slug TEXT UNIQUE,
                name TEXT NOT NULL,
                name_en TEXT, publisher TEXT,
                valid_from INTEGER, valid_to INTEGER,
                description TEXT, url TEXT,
                supersedes_id INTEGER REFERENCES classification(id),
                code_count INTEGER NOT NULL DEFAULT 0,
                valid_code_count INTEGER
            );
            """
        )
        conn.execute(
            "INSERT INTO classification (short_name, slug, name) "
            "VALUES ('OLD', 'old1996', 'Old')"
        )
        old_id = conn.execute(
            "SELECT id FROM classification WHERE short_name='OLD'"
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO classification (short_name, slug, name, supersedes_id) "
            "VALUES ('NEW_A', 'newa2000', 'New A', ?)",
            (old_id,),
        )
        conn.execute(
            "INSERT INTO classification (short_name, slug, name, supersedes_id) "
            "VALUES ('NEW_B', 'newb2000', 'New B', ?)",
            (old_id,),
        )
        conn.commit()

        listed = list_classifications(conn)
        # OLD must appear exactly once; superseded_by carries both successors.
        old_rows = [c for c in listed if c["short_name"] == "OLD"]
        assert len(old_rows) == 1
        assert old_rows[0]["superseded_by"] == "NEW_A,NEW_B"
        # A2.6.1: --list rows carry the canonical 2-seg FQID built from the
        # selected slug — the list SELECT must include c.slug or every row
        # loses its address (Codex P2 on #148).
        assert old_rows[0]["fqid"] == "class/old1996"
        # And _classification_by_id (fetchone path) is also stable.
        single = _classification_by_id(conn, old_id)
        assert single["superseded_by"] == "NEW_A,NEW_B"

    def test_missing_seed_fails_build(self, tmp_path: Path, monkeypatch):
        """build-db must error when no seed is available — silently shipping
        a DB without classifications would let downstream queries return all
        NULL FKs without warning. Wheel installs run `reg-meta update`, not
        `build-db`, so this path is unreachable in production.
        """
        from reg_meta_build import db as build_db_mod

        monkeypatch.setattr(build_db_mod, "repo_seed_path", lambda: None)
        input_dir = _make_input_dir(tmp_path)
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        with pytest.raises(RegMetaError) as ei:
            build_db(input_dir=input_dir, db_dir=db_dir)
        assert ei.value.code == "classification_seed_not_found"


# ---------------------------------------------------------------------------
# PR2 / #446: adapter classification candidate feed (_feed_classification_candidates)
# ---------------------------------------------------------------------------


class TestFeedClassificationCandidates:
    """Unit-pins the adapter candidate feed: it resolves short_name →
    classification_id against the populated `classification` table and INSERTs the
    SCB-shaped `(variable_id, value_set_id, classification_id)` rows. Unknown
    short_names are dropped silently (no row, no raise) so a provider-skipped
    classification (or a typo) can't abort the build. Feeds SOS and curated
    thin-provider candidates alike — the resolver is provider-blind."""

    @staticmethod
    def _conn() -> sqlite3.Connection:
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        conn.execute(
            "INSERT INTO classification (short_name, name, publisher) "
            "VALUES ('ICD-10-SE', 'ICD-10-SE', 'Socialstyrelsen')"
        )
        return conn

    @staticmethod
    def _rows(conn: sqlite3.Connection) -> list[tuple]:
        return conn.execute(
            "SELECT variable_id, value_set_id, classification_id "
            "FROM classification_candidate ORDER BY variable_id, value_set_id"
        ).fetchall()

    def test_empty_inserts_nothing(self) -> None:
        from reg_meta_build.db import _feed_classification_candidates

        conn = self._conn()
        assert _feed_classification_candidates(conn, []) == 0
        assert self._rows(conn) == []

    def test_known_short_name_inserted(self) -> None:
        from reg_meta_build.db import _feed_classification_candidates

        conn = self._conn()
        icd_id = conn.execute(
            "SELECT id FROM classification WHERE short_name = 'ICD-10-SE'"
        ).fetchone()[0]
        # One code-less (value_set_id None) + one code-bearing candidate.
        n = _feed_classification_candidates(
            conn, [(920, None, "ICD-10-SE"), (921, 5000, "ICD-10-SE")]
        )
        assert n == 2
        assert self._rows(conn) == [(920, None, icd_id), (921, 5000, icd_id)]

    def test_unknown_short_name_dropped_without_raise(self) -> None:
        from reg_meta_build.db import _feed_classification_candidates

        conn = self._conn()
        # No raise; the unknown short_name simply contributes no row.
        n = _feed_classification_candidates(conn, [(920, None, "NOPE")])
        assert n == 0
        assert self._rows(conn) == []

    def test_mixed_inserts_only_known(self) -> None:
        from reg_meta_build.db import _feed_classification_candidates

        conn = self._conn()
        icd_id = conn.execute(
            "SELECT id FROM classification WHERE short_name = 'ICD-10-SE'"
        ).fetchone()[0]
        n = _feed_classification_candidates(
            conn,
            [(920, None, "ICD-10-SE"), (921, None, "KVA"), (922, 7, "NOPE")],
        )
        # KVA + NOPE are absent from the classification table → dropped.
        assert n == 1
        assert self._rows(conn) == [(920, None, icd_id)]


# ---------------------------------------------------------------------------
# #416: code-set-containment detector + curated link loader
# ---------------------------------------------------------------------------


class _Graph:
    """Tiny in-memory build graph for the #416 detector: provider/register/
    variant + variables + value sets (code,label members) + classifications with
    canonical codes, all keyed by caller-chosen ids so a test can assert against
    them directly. Mirrors `TestFeedClassificationCandidates`'s full-DDL `:memory:`
    approach (the detector reads value_set_member / classification_code /
    variable_state / classification_candidate, none of which a focused subset
    could shortcut)."""

    def __init__(self) -> None:
        from reg_meta_build.db import DDL

        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(DDL)
        self.conn.execute(
            "INSERT INTO provider (provider_id, slug, name) VALUES (1, 'scb', 'SCB')"
        )
        self.conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'ULF', 'ulf')"
        )
        self.conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug) "
            "VALUES (1, 1, '_default')"
        )
        self._code_id = 0

    def add_classification(
        self,
        cls_id: int,
        short_name: str,
        codes: list[tuple[str, str]],
        supersedes_id: int | None = None,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> None:
        """Seed a classification whose canonical code set is `codes` (each a
        (code, label) pair). is_valid=1 (canonical); level is the digit-length for
        all-digit codes, NULL otherwise — same rule as the build. `supersedes_id`
        (older predecessor on the vintage chain) and `valid_from`/`valid_to` (INTEGER
        years, NULLABLE = unbounded) feed the #494 vintage-period reclaim."""
        self.conn.execute(
            "INSERT INTO classification "
            "(id, short_name, name, supersedes_id, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cls_id, short_name, short_name, supersedes_id, valid_from, valid_to),
        )
        for code, label in codes:
            code_id = self._intern_code(code, label)
            level = len(code) if code.isdigit() else None
            self.conn.execute(
                "INSERT INTO classification_code "
                "(classification_id, code_id, level, is_valid) VALUES (?, ?, ?, 1)",
                (cls_id, code_id, level),
            )

    def add_no_csv_classification(
        self,
        cls_id: int,
        short_name: str,
        codes: list[tuple[str, str]],
        supersedes_id: int | None = None,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> None:
        """Seed a classification with `is_valid=NULL` canonical rows — the no-CSV
        shape (ICD-10-SE in production): its observed codes ARE its code set, so
        the detector's `is_valid IS NOT 0` filter must keep them. Same level rule
        and same `supersedes_id`/`valid_from`/`valid_to` vintage fields as
        `add_classification`."""
        self.conn.execute(
            "INSERT INTO classification "
            "(id, short_name, name, supersedes_id, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cls_id, short_name, short_name, supersedes_id, valid_from, valid_to),
        )
        for code, label in codes:
            code_id = self._intern_code(code, label)
            level = len(code) if code.isdigit() else None
            self.conn.execute(
                "INSERT INTO classification_code "
                "(classification_id, code_id, level, is_valid) VALUES (?, ?, ?, NULL)",
                (cls_id, code_id, level),
            )

    def add_value_set(self, value_set_id: int, codes: list[tuple[str, str]]) -> None:
        # member_hash is UNIQUE NOT NULL (32 bytes); derive a deterministic one.
        member_hash = hashlib.sha256(repr((value_set_id, codes)).encode()).digest()
        self.conn.execute(
            "INSERT INTO value_set (value_set_id, member_hash) VALUES (?, ?)",
            (value_set_id, member_hash),
        )
        for code, label in codes:
            code_id = self._intern_code(code, label)
            self.conn.execute(
                "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
                (value_set_id, code_id),
            )

    def add_variable_state(
        self,
        variable_id: int,
        value_set_id: int | None,
        slug: str | None = None,
        valid_from: str = "2020-01-01",
        valid_to: str = "9999-12-31",
    ) -> None:
        """A variable + a single `variable_state` carrying `value_set_id`. The slug
        lets the curated loader resolve `scb/ulf/<slug>`. `valid_from` is exposed
        so a test can attach a SECOND state to the same variable without tripping
        the `variable_state` UNIQUE (variable_id, register_variant_id, valid_from,
        value_set_version_label) constraint. `valid_to` defaults to the open-ended
        '9999-12-31' sentinel; a test sets a closed period to exercise the #494
        vintage-period overlap (both are TEXT 'YYYY-MM-DD')."""
        existing = self.conn.execute(
            "SELECT 1 FROM variable WHERE variable_id = ?", (variable_id,)
        ).fetchone()
        if existing is None:
            self.conn.execute(
                "INSERT INTO variable (variable_id, register_id, provider_key, slug) "
                "VALUES (?, 1, ?, ?)",
                (variable_id, str(variable_id), slug or f"v{variable_id}"),
            )
        self.conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, value_set_id) "
            "VALUES (?, 1, ?, ?, ?)",
            (variable_id, valid_from, valid_to, value_set_id),
        )

    def _intern_code(self, code: str, label: str) -> int:
        row = self.conn.execute(
            "SELECT code_id FROM value_code WHERE code = ? AND label = ?",
            (code, label),
        ).fetchone()
        if row is not None:
            return row[0]
        self._code_id += 1
        self.conn.execute(
            "INSERT INTO value_code (code_id, code, label) VALUES (?, ?, ?)",
            (self._code_id, code, label),
        )
        return self._code_id

    def candidates(self) -> list[tuple]:
        return self.conn.execute(
            "SELECT variable_id, value_set_id, classification_id "
            "FROM classification_candidate "
            "ORDER BY variable_id, value_set_id, classification_id"
        ).fetchall()

    def tagged_classification(self, variable_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT classification_id FROM variable_state WHERE variable_id = ?",
            (variable_id,),
        ).fetchone()
        return row[0] if row else None


def _numeric_codes(prefix: str, n: int, width: int) -> list[tuple[str, str]]:
    """`n` zero-padded numeric (code, label) pairs of fixed digit `width`."""
    return [(str(i).zfill(width), f"{prefix} {i}") for i in range(1, n + 1)]


class TestLinkValueSetClassifications:
    """The code-set-containment detector (#416): additive producer of
    `classification_candidate` rows for value sets whose inline codes match one
    classification, never overriding an existing state-key candidate."""

    def test_confident_single_family_over_15_codes(self) -> None:
        from reg_meta_build.classifications import link_value_set_classifications
        from reg_meta_build.db import _backfill_state_classifications

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(10, "ICD-10-SE", codes)
        g.add_value_set(100, codes)  # identical → containment 1.0, n_codes 20
        g.add_variable_state(900, 100)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert counts["variables_linked"] == 1
        assert g.candidates() == [(900, 100, 10)]

        # The backfill then tags the state from the emitted candidate.
        _backfill_state_classifications(g.conn)
        assert g.tagged_classification(900) == 10

    def test_single_family_under_15_codes_label_agree_links(self) -> None:
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("SUN", 10, 3)  # < 15 codes
        g.add_classification(11, "SUN2000", codes)
        # Identical (code,label) → label_agree 1.0 ≥ 0.90.
        g.add_value_set(101, codes)
        g.add_variable_state(901, 101)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert g.candidates() == [(901, 101, 11)]

    def test_single_family_under_15_codes_label_disagree_not_linked(self) -> None:
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("SUN", 10, 3)  # < 15 codes
        g.add_classification(12, "SUN2000", codes)
        # Same CODES (containment 1.0, single-family) but RELABELED → label_agree 0.
        relabeled = [(code, f"renamed {code}") for code, _ in codes]
        g.add_value_set(102, relabeled)
        g.add_variable_state(902, 102)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 0
        assert counts["single_below_threshold"] == 1
        assert g.candidates() == []

    def test_label_agree_counts_distinct_kods_not_pairs(self) -> None:
        # A <15-code single-family set where ONE code is carried under TWO labels
        # that both match canonical (kod, label). Distinct-kod agreement is 8/10 =
        # 0.80 (< 0.90 → must NOT link), but the OLD COUNT(*) pair-count was 9/10 =
        # 0.90 (would have linked). Guards Fix 1: numerator = COUNT(DISTINCT v.kod).
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # Canonical: 10 distinct 3-digit codes 001..010 each under one label, PLUS
        # a SECOND canonical row for 001 under label "LX" (a code legitimately
        # carrying two canonical labels).
        canon = [(str(i).zfill(3), f"L{i}") for i in range(1, 11)]
        canon.append(("001", "LX"))
        g.add_classification(40, "SUN2000", canon)

        # Value set (n_codes = 10 distinct kods, containment 1.0, single-family):
        #  - 001..008 under their matching labels L1..L8  → 8 distinct matching kods
        #  - 009, 010 RELABELED → 2 distinct non-matching kods
        #  - 001 ALSO under "LX" (the duplicate): a second matching PAIR, same kod
        members = [(str(i).zfill(3), f"L{i}") for i in range(1, 9)]
        members += [("009", "renamed 009"), ("010", "renamed 010")]
        members.append(("001", "LX"))  # duplicate kod, second matching label
        g.add_value_set(140, members)
        g.add_variable_state(940, 140)

        counts = link_value_set_classifications(g.conn)
        # Distinct-kod agreement 0.80 < 0.90 → not confident; pair-count 0.90 would
        # have falsely linked under the old metric.
        assert counts["value_sets_linked"] == 0
        assert counts["single_below_threshold"] == 1
        assert g.candidates() == []

    def test_multi_family_ambiguous_not_linked(self) -> None:
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # One 10-code 4-digit set ≥0.90-contained in BOTH classifications: the two
        # classifications share the value set's codes (plus a distinct extra each).
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        g.add_classification(14, "FAM_B", shared + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(903, 103)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 0
        assert counts["multi_family"] == 1
        assert g.candidates() == []

    def test_additive_guard_does_not_override_existing_candidate(self) -> None:
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(15, "ICD-10-SE", codes)
        g.add_classification(16, "OTHER", codes)  # would also match, but...
        g.add_value_set(104, codes)
        g.add_variable_state(904, 104)
        # Pre-existing candidate for the SAME state key (e.g. a name-based/SCB
        # link). The detector must NOT add a second row for (904, 104).
        g.conn.execute(
            "INSERT INTO classification_candidate "
            "(variable_id, value_set_id, classification_id) VALUES (904, 104, 16)"
        )

        link_value_set_classifications(g.conn)
        # Only the pre-existing candidate remains; no detector row was added.
        assert g.candidates() == [(904, 104, 16)]

    def test_grain_filter_matches_4_digit_family_not_2_digit(self) -> None:
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # A 4-digit family (SSYK4-like) and a 2-digit family (SNI2-like) whose
        # canonical CODE STRINGS overlap: the same digit string "12" is a 2-digit
        # SNI code AND the prefix of the 4-digit SSYK codes "1201".."1220". Without
        # a grain filter the 4-digit value set's "1201" wouldn't match the 2-digit
        # "12" anyway (full-string match), so to make the test bite we put the
        # IDENTICAL 4-digit strings in BOTH families but at DIFFERENT declared
        # levels — SNI2 lists them as level-2 noise, SSYK4 as level-4. The value
        # set is all-4-digit (dom_level=4), so the grain filter keeps only the
        # level-4 canonical rows → SSYK4 alone, never the level-2 SNI2 rows.
        four_digit = _numeric_codes("SSYK", 20, 4)  # 0001..0020, level 4
        g.add_classification(17, "SSYK4", four_digit)
        # SNI2 carries the SAME code strings but declared at level 2 (canonical
        # rows inserted with level=2 to model a coarser-grain family that happens
        # to share the strings). Build the classification_code rows directly so we
        # control `level`.
        g.add_classification(18, "SNI2", [])
        for code, label in four_digit:
            code_id = g._intern_code(code, label)
            g.conn.execute(
                "INSERT INTO classification_code "
                "(classification_id, code_id, level, is_valid) VALUES (18, ?, 2, 1)",
                (code_id,),
            )
        g.add_value_set(105, four_digit)
        g.add_variable_state(905, 105)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert counts["multi_family"] == 0
        # The single emitted candidate points at the 4-digit (level-4) family.
        assert g.candidates() == [(905, 105, 17)]

    def test_idempotent_rerun_does_not_double_candidates(self) -> None:
        """Running the detector TWICE on the same DB must not duplicate the
        candidate: the second run's additive NOT EXISTS guard sees its own
        first-run row and skips emission. value_sets_linked stays 1 (it counts the
        confident population, which is stable across re-runs)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(30, "ICD-10-SE", codes)
        g.add_value_set(120, codes)
        g.add_variable_state(920, 120)

        first = link_value_set_classifications(g.conn)
        second = link_value_set_classifications(g.conn)
        assert first["value_sets_linked"] == 1
        assert second["value_sets_linked"] == 1
        # Exactly one candidate for the state key — the re-run added nothing.
        assert g.candidates() == [(920, 120, 30)]

    def test_additive_guard_is_per_state_key_not_per_value_set(self) -> None:
        """The additive guard is keyed on `(variable_id, value_set_id)`, not on the
        value set alone: two variables share ONE confident (single-family) value
        set, and only the FIRST already has a candidate. The detector must leave the
        first untouched AND link the second (unclaimed) variable's state key.

        cls_id 99 is a sentinel not in the classification table, so the pre-existing
        candidate is distinguishable from anything the detector would emit (31)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(31, "ICD-10-SE", codes)  # single family → confident
        g.add_value_set(121, codes)
        g.add_variable_state(921, 121)  # state key already claimed below
        g.add_variable_state(922, 121)  # unclaimed → detector should link it
        # Pre-existing candidate for ONLY the first variable's state key.
        g.conn.execute(
            "INSERT INTO classification_candidate "
            "(variable_id, value_set_id, classification_id) VALUES (921, 121, 99)"
        )

        link_value_set_classifications(g.conn)
        # First variable's candidate is untouched (99); second is freshly linked (31).
        assert g.candidates() == [(921, 121, 99), (922, 121, 31)]

    def test_confident_count_independent_of_guard_skipped_emission(self) -> None:
        """`value_sets_linked` counts the CONFIDENT population, not emitted rows: a
        single-family ≥15-code set whose ONLY state key already has a candidate is
        counted (== 1) even though the additive guard skips emission. Pins the
        documented count-is-confident-population semantics."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(36, "ICD-10-SE", codes)  # single family → confident
        g.add_value_set(125, codes)
        g.add_variable_state(926, 125)
        # Pre-existing candidate (sentinel cls 99) on the set's only state key.
        g.conn.execute(
            "INSERT INTO classification_candidate "
            "(variable_id, value_set_id, classification_id) VALUES (926, 125, 99)"
        )

        counts = link_value_set_classifications(g.conn)
        # Counted as confident even though no row was emitted (guard held).
        assert counts["value_sets_linked"] == 1
        assert g.candidates() == [(926, 125, 99)]

    def test_no_csv_null_is_valid_codes_still_link(self) -> None:
        """A no-CSV classification carries `is_valid=NULL` canonical rows (its
        observed codes ARE its code set — the ICD-10-SE production case). The
        detector's `is_valid IS NOT 0` filter must keep NULL rows so a value set
        enumerating those codes links."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_no_csv_classification(33, "ICD-10-SE", codes)
        g.add_value_set(122, codes)
        g.add_variable_state(923, 122)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert g.candidates() == [(923, 122, 33)]

    def test_alphanumeric_codes_dom_level_null_link(self) -> None:
        """ICD-shaped alphanumeric codes (`A01`, `B99`, not all-digit) give
        `dom_level=NULL`, disabling the grain filter. A ≥15-code single-family set
        of such codes still auto-links on size — exercises the `dom_level IS NULL`
        match branch (the real ICD shape)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # 20 alphanumeric codes; none is all-digit → dom_level NULL.
        codes = [
            (f"{chr(65 + i // 10)}{i % 10}{i % 10}", f"ICD {i}") for i in range(20)
        ]
        g.add_classification(34, "ICD-10-SE", codes)
        g.add_value_set(123, codes)
        g.add_variable_state(924, 123)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert g.candidates() == [(924, 123, 34)]

    def test_below_min_codes_floor_emits_nothing(self) -> None:
        """A value set with fewer than `_MIN_CODES` (8) distinct codes never enters
        `_vs_cls` at all — even a perfect single-family match. So it produces no
        candidate AND is counted in NEITHER the single-below-threshold nor the
        multi-family tally (those size the ≥8-code curation tail)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = _numeric_codes("SUN", 7, 3)  # 7 < _MIN_CODES
        g.add_classification(35, "SUN2000", codes)
        g.add_value_set(124, codes)
        g.add_variable_state(925, 124)

        counts = link_value_set_classifications(g.conn)
        assert g.candidates() == []
        assert counts["value_sets_linked"] == 0
        assert counts["single_below_threshold"] == 0
        assert counts["multi_family"] == 0

    def test_confident_single_family_unaffected_by_vintage_step(self) -> None:
        """Regression guard: the confident single-family path must be untouched by
        the new vintage step. A ≥15-code single-family set on a classification that
        IS on a supersedes chain (valid_from set) still links to that exact cls via
        the confident emit — the vintage step only touches MULTI-family sets, and
        this set never enters `_vs_multi_onechain` (single candidate)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        codes = [(str(i).zfill(4), f"ICD {i}") for i in range(1, 21)]  # 0001..0020
        # On a chain (chain root + a successor whose code STRINGS are disjoint), but
        # the value set matches only ONE vintage's codes → single-family → confident
        # path, not the vintage step. (Codes, not labels, are the match key, so the
        # successor must use disjoint code strings to stay single-family.)
        g.add_classification(50, "ICD-10", codes, valid_from=2000, valid_to=2010)
        successor_codes = [(str(i).zfill(4), f"ICD {i}") for i in range(50, 70)]
        g.add_classification(
            51,
            "ICD-11",
            successor_codes,
            supersedes_id=50,
            valid_from=2011,
        )
        g.add_value_set(150, codes)
        g.add_variable_state(950, 150)

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert counts["multi_family"] == 0
        assert counts["vintage_value_sets_linked"] == 0
        assert g.candidates() == [(950, 150, 50)]

    def test_same_chain_collapse_latest_overlapping_wins(self) -> None:
        """A value set ≥0.90-contained in TWO chain vintages (SNI2002 [2002,2007],
        SNI2007 [2008,unbounded]), short (<15 codes → multi-family residue, NOT
        confident), variable state open-ended (year 9999) → vintage reclaim links it
        to the LATER vintage; backfill tags it; counts show the reclaim."""
        from reg_meta_build.classifications import link_value_set_classifications
        from reg_meta_build.db import _backfill_state_classifications

        g = _Graph()
        # 10 shared 4-digit codes (< 15 → never confident). Each vintage adds a
        # distinct extra so they are DISTINCT classifications both ≥0.90-containing.
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            60,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            61,
            "SNI2007",
            shared + [("9002", "2007 only")],
            supersedes_id=60,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(160, shared)
        g.add_variable_state(960, 160)  # open-ended → s_end 9999, overlaps both

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 0  # not confident (multi-family)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        # Latest overlapping vintage wins.
        assert g.candidates() == [(960, 160, 61)]

        _backfill_state_classifications(g.conn)
        assert g.tagged_classification(960) == 61

    def test_closed_period_picks_older_vintage(self) -> None:
        """Same two-vintage chain, but a CLOSED state period that overlaps only the
        OLDER vintage [2002,2007] → links to the older one (60), not the latest."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            62,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            63,
            "SNI2007",
            shared + [("9002", "2007 only")],
            supersedes_id=62,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(161, shared)
        # State period 2003–2006 overlaps ONLY SNI2002 [2002,2007].
        g.add_variable_state(961, 161, valid_from="2003-01-01", valid_to="2006-12-31")

        counts = link_value_set_classifications(g.conn)
        assert counts["vintage_value_sets_linked"] == 1
        assert g.candidates() == [(961, 161, 62)]

    def test_off_chain_candidate_stays_ambiguous(self) -> None:
        """A value set ≥0.90-contained in SNI2007 AND an off-chain SSYK (a different
        chain root) is a genuine cross-family coincidence → NOT vintage-linked; it
        stays counted in multi_family with vintage_value_sets_linked == 0."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(
            64,
            "SNI2007",
            shared + [("9001", "sni only")],
            valid_from=2008,
            valid_to=None,
        )
        # Off-chain: its own root (supersedes_id NULL, no successor → distinct root).
        g.add_classification(
            65,
            "SSYK2012",
            shared + [("9002", "ssyk only")],
            valid_from=2014,
            valid_to=None,
        )
        g.add_value_set(162, shared)
        g.add_variable_state(962, 162)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_no_overlap_no_link(self) -> None:
        """Candidates all on one chain, but the state period overlaps NO candidate
        vintage → no emit (residual, safe by omission). Stays counted as
        multi-family; vintage_value_sets_linked == 0."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            66,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            67,
            "SNI2007",
            shared + [("9002", "2007 only")],
            supersedes_id=66,
            valid_from=2008,
            valid_to=2015,
        )
        g.add_value_set(163, shared)
        # State period 2018–2020 overlaps NEITHER vintage.
        g.add_variable_state(963, 163, valid_from="2018-01-01", valid_to="2020-12-31")

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_vintage_additive_guard_does_not_override(self) -> None:
        """An existing candidate (a curated/feed claim, sentinel cls 99) for the
        pair is NOT overwritten by the vintage step — the same additive NOT EXISTS
        guard the confident emit uses."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            68,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            69,
            "SNI2007",
            shared + [("9002", "2007 only")],
            supersedes_id=68,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(164, shared)
        g.add_variable_state(964, 164)
        g.conn.execute(
            "INSERT INTO classification_candidate "
            "(variable_id, value_set_id, classification_id) VALUES (964, 164, 99)"
        )

        link_value_set_classifications(g.conn)
        # Pre-existing claim untouched; vintage step added nothing.
        assert g.candidates() == [(964, 164, 99)]

    def test_multi_state_span_aggregation_picks_latest(self) -> None:
        """One (variable_id, value_set_id) with TWO states — 2003–2006 (overlaps the
        OLDER vintage) and 2010–open (overlaps the LATER) — spans both vintages, so
        the AGGREGATE span [2003, 9999] resolves to the LATEST overlapping vintage.
        Exactly ONE row is emitted (emit grain = pair), so the backfill min()-fold
        has nothing to fight."""
        from reg_meta_build.classifications import link_value_set_classifications
        from reg_meta_build.db import _backfill_state_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            70,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            71,
            "SNI2007",
            shared + [("9002", "2007 only")],
            supersedes_id=70,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(165, shared)
        # Two states on ONE (variable_id, value_set_id) pair.
        g.add_variable_state(965, 165, valid_from="2003-01-01", valid_to="2006-12-31")
        g.add_variable_state(965, 165, valid_from="2010-01-01")  # open-ended

        counts = link_value_set_classifications(g.conn)
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        # Exactly ONE candidate row for the pair → backfill min() doesn't fight it.
        assert g.candidates() == [(965, 165, 71)]

        _backfill_state_classifications(g.conn)
        # Both states of the pair adopt the one resolved vintage.
        tagged = g.conn.execute(
            "SELECT DISTINCT classification_id FROM variable_state "
            "WHERE variable_id = 965"
        ).fetchall()
        assert tagged == [(71,)]


class TestCuratedClassificationLinks:
    """The curated tail loader (#416): load-time validation + delete-then-insert
    precedence at materialize."""

    def test_materialize_links_every_value_set_state_key(self) -> None:
        """One curated entry on a variable with TWO `variable_state` rows carrying
        DIFFERENT `value_set_id`s links BOTH state keys: n_inserted == 2 and a
        candidate is written for each (the loader iterates the variable's distinct
        value-set states, not just one)."""
        from reg_meta_build.classification_links import (
            CuratedClassificationLink,
            materialize_classification_links,
        )

        g = _Graph()
        g.add_classification(40, "ICD-10-SE", _numeric_codes("ICD", 5, 4))
        g.add_value_set(130, _numeric_codes("ICD", 5, 4))
        g.add_value_set(131, _numeric_codes("ICD", 5, 4))
        g.add_variable_state(930, 130, slug="ha0611m")
        # Second state on the SAME variable; distinct valid_from avoids the
        # variable_state UNIQUE collision (same variable, same NULL version label).
        g.add_variable_state(930, 131, slug="ha0611m", valid_from="2021-01-01")

        entry = CuratedClassificationLink(
            provider="scb",
            register="ulf",
            variable="ha0611m",
            classification="ICD-10-SE",
            note=None,
        )
        n = materialize_classification_links(
            g.conn, (entry,), providers=frozenset({"scb"})
        )
        assert n == 2
        assert g.candidates() == [(930, 130, 40), (930, 131, 40)]

    def test_materialize_skips_code_less_only_variable(self) -> None:
        """A variable whose only states have `value_set_id IS NULL` has no value-set
        state key to target, so a curated link writes NOTHING: n_inserted == 0, no
        candidate, no error (a curated classification link is about the variable's
        inline code set)."""
        from reg_meta_build.classification_links import (
            CuratedClassificationLink,
            materialize_classification_links,
        )

        g = _Graph()
        g.add_classification(41, "ICD-10-SE", _numeric_codes("ICD", 5, 4))
        g.add_variable_state(931, None, slug="ha0611m")

        entry = CuratedClassificationLink(
            provider="scb",
            register="ulf",
            variable="ha0611m",
            classification="ICD-10-SE",
            note=None,
        )
        n = materialize_classification_links(
            g.conn, (entry,), providers=frozenset({"scb"})
        )
        assert n == 0
        assert g.candidates() == []

    def test_curated_override_takes_precedence(self) -> None:
        from reg_meta_build.classification_links import (
            CuratedClassificationLink,
            materialize_classification_links,
        )

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(20, "ICD-10-SE", codes)
        g.add_classification(21, "WRONG", codes)
        g.add_value_set(110, codes)
        g.add_variable_state(910, 110, slug="ha0611m")
        # A pre-existing (auto/feed) candidate pointing at the WRONG classification.
        g.conn.execute(
            "INSERT INTO classification_candidate "
            "(variable_id, value_set_id, classification_id) VALUES (910, 110, 21)"
        )

        entry = CuratedClassificationLink(
            provider="scb",
            register="ulf",
            variable="ha0611m",
            classification="ICD-10-SE",
            note=None,
        )
        n = materialize_classification_links(
            g.conn, (entry,), providers=frozenset({"scb"})
        )
        assert n == 1
        # The WRONG row is gone (delete-then-insert); ICD-10-SE (20) wins.
        assert g.candidates() == [(910, 110, 20)]

    def test_provider_not_built_is_skipped_not_failed(self) -> None:
        from reg_meta_build.classification_links import (
            CuratedClassificationLink,
            materialize_classification_links,
        )

        g = _Graph()
        entry = CuratedClassificationLink(
            provider="sos",  # not in the build
            register="r",
            variable="v",
            classification="ICD-10-SE",
            note=None,
        )
        n = materialize_classification_links(
            g.conn, (entry,), providers=frozenset({"scb"})
        )
        assert n == 0
        assert g.candidates() == []

    def test_unresolved_variable_fails(self) -> None:
        from reg_meta_build.classification_links import (
            CuratedClassificationLink,
            materialize_classification_links,
        )

        g = _Graph()
        g.add_classification(22, "ICD-10-SE", _numeric_codes("ICD", 5, 4))
        entry = CuratedClassificationLink(
            provider="scb",
            register="ulf",
            variable="nope",  # no such variable
            classification="ICD-10-SE",
            note=None,
        )
        with pytest.raises(RegMetaError) as ei:
            materialize_classification_links(
                g.conn, (entry,), providers=frozenset({"scb"})
            )
        assert ei.value.code == "classification_links_unresolved"

    def test_unresolved_classification_fails(self) -> None:
        from reg_meta_build.classification_links import (
            CuratedClassificationLink,
            materialize_classification_links,
        )

        g = _Graph()
        g.add_value_set(111, _numeric_codes("ICD", 5, 4))
        g.add_variable_state(911, 111, slug="ha0611m")
        entry = CuratedClassificationLink(
            provider="scb",
            register="ulf",
            variable="ha0611m",
            classification="NO-SUCH-CLS",
            note=None,
        )
        with pytest.raises(RegMetaError) as ei:
            materialize_classification_links(
                g.conn, (entry,), providers=frozenset({"scb"})
            )
        assert ei.value.code == "classification_links_unresolved"

    def test_load_bad_fqid_fails(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        path = tmp_path / "classification_links.toml"
        path.write_text(
            '[[link]]\nvariable = "scb/ulf"\nclassification = "ICD-10-SE"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as ei:
            load_classification_links(path)
        assert ei.value.code == "classification_links_invalid"

    def test_load_missing_classification_fails(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        path = tmp_path / "classification_links.toml"
        path.write_text('[[link]]\nvariable = "scb/ulf/ha0611m"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as ei:
            load_classification_links(path)
        assert ei.value.code == "classification_links_invalid"

    def test_load_duplicate_variable_fails(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        path = tmp_path / "classification_links.toml"
        path.write_text(
            '[[link]]\nvariable = "scb/ulf/ha0611m"\nclassification = "ICD-10-SE"\n'
            '[[link]]\nvariable = "scb/ulf/ha0611m"\nclassification = "OTHER"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as ei:
            load_classification_links(path)
        assert ei.value.code == "classification_links_invalid"

    def test_load_empty_or_missing_is_clean(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        assert load_classification_links(None) == ()
        assert load_classification_links(tmp_path / "absent.toml") == ()
        empty = tmp_path / "classification_links.toml"
        empty.write_text("# only comments\n", encoding="utf-8")
        assert load_classification_links(empty) == ()

    def test_repo_toml_loads_clean_and_empty(self) -> None:
        """The shipped maintainer artifact parses and currently carries no
        entries (residue curation is deferred)."""
        from reg_meta_build.classification_links import (
            load_classification_links,
            repo_classification_links_path,
        )

        path = repo_classification_links_path()
        assert path is not None, "classification_links.toml must ship in the repo"
        assert load_classification_links(path) == ()


# ---------------------------------------------------------------------------
# PR2: the merged kva.csv round-trips through populate_classifications
# ---------------------------------------------------------------------------


class TestKvaMergedCsv:
    def test_kva_csv_round_trips_without_duplicate_codes(self, tmp_path: Path):
        """The real merged `sos/kva.csv` (KMÅ ∪ KKÅ, deduped on the 50 shared
        chapter headers) loads into ONE `KVA` classification with codes and
        WITHOUT a duplicate-code `RegMetaError` — proving the merge deduped."""
        from reg_meta_build.classifications import (
            populate_classifications,
            repo_seed_path,
        )
        from reg_meta_build.db import DDL

        cls_dir = repo_seed_path().parent / "input_data" / "classifications"
        assert (cls_dir / "sos" / "kva.csv").is_file(), "merged kva.csv must exist"

        seed = tmp_path / "classifications.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "KVA"\nname = "KVÅ"\n'
            'provider = "sos"\nvalid_codes_file = "sos/kva.csv"\n',
            encoding="utf-8",
        )
        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        # Does not raise classification_csv_invalid (duplicate vardekod).
        n_seeded, skipped = populate_classifications(
            conn, seed, valid_codes_dir=cls_dir, providers=frozenset({"sos"})
        )
        assert (n_seeded, skipped) == (1, frozenset())
        row = conn.execute(
            "SELECT short_name, code_count FROM classification"
        ).fetchone()
        assert row[0] == "KVA"
        assert row[1] > 0, "KVA must seed canonical codes"


# ---------------------------------------------------------------------------
# CLI commands — get classification
# ---------------------------------------------------------------------------


def _run_json(db_dir: Path, args: list[str]) -> tuple[dict, int]:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "reg_meta",
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
    build_db(input_dir=input_dir, db_dir=db_dir, seed_path=seed, skip_slugs=True)

    # Query commands require a doc DB alongside.
    from reg_meta_build.doc_db import build_doc_db

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
        assert [c["code"] for c in codes] == ["1", "2"]
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
