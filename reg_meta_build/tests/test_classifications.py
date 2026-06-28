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
# "Kön"); TESTKON2 tags CVID 1004 (vardemangdsversion "Kon-2"). With both
# pointing at real strings the build invariants pass. Succession is no longer
# seed-declared (it lives in `classification_replaced_by`); `derive_supersedes_
# from_edges` is unit-tested directly below.
TEST_SEED_TOML = """\
[[classification]]
short_name = "TESTKON"
name = "Test classification for gender codes"
name_en = "Test"
publisher = "TEST"
version = "1"
valid_from = 2000
url = "https://example.com/"
valid_codes_file = "testkon.csv"
vardemangdsversion = ["Kön"]

[[classification]]
short_name = "TESTKON2"
name = "Successor"
publisher = "TEST"
version = "2"
valid_from = 2022
valid_codes_file = "testkon2.csv"
vardemangdsversion = ["Kon-2"]
"""

# CSVs for TEST_SEED_TOML, written alongside the seed by `_build_with_seed`.
# Codes mirror EXACTLY the observed codes so no canonical-but-unobserved rows
# are inserted (keeps `code_count` assertions stable while satisfying the
# always-seed `valid_codes_file` requirement).
TEST_SEED_CSVS = {
    "testkon.csv": "vardekod,vardebenamning\n1,Man\n2,Kvinna\n",
    "testkon2.csv": "vardekod,vardebenamning\n10,Female\n20,Male\n30,Other\n",
}


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
            'valid_codes_file = "a.csv"\n'
            'vardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        entries = load_seed(seed)
        assert len(entries) == 1
        assert entries[0]["short_name"] == "A"

    def test_missing_valid_codes_file_rejected(self, tmp_path: Path):
        # Every classification must carry a valid_codes_file — it is what makes
        # always-seed safe on a thin --providers build (the CSV supplies codes
        # so the empty guard never trips). Omitting it fails fast.
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "valid_codes_file" in ei.value.message
        assert "A" in ei.value.message

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
        # An entry with no vardemangdsversion is valid (canonical-codes-only
        # classification — tags no instances). valid_codes_file is still
        # required.
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'valid_codes_file = "a.csv"\n',
            encoding="utf-8",
        )
        entries = load_seed(seed)
        assert len(entries) == 1
        assert "vardemangdsversion" not in entries[0]

    def test_duplicate_short_name(self, tmp_path: Path):
        seed = tmp_path / "c.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'valid_codes_file = "a.csv"\nvardemangdsversion = ["x"]\n'
            '[[classification]]\nshort_name = "A"\nname = "Other"\n'
            'valid_codes_file = "a2.csv"\nvardemangdsversion = ["y"]\n',
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
            'valid_codes_file = "a.csv"\nvardemangdsversion = ["x"]\n'
            '[[classification]]\nshort_name = "B"\nname = "B"\n'
            'valid_codes_file = "b.csv"\nvardemangdsversion = ["x"]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as ei:
            load_seed(seed)
        assert ei.value.code == "classification_seed_invalid"
        assert "belongs to exactly one" in ei.value.remediation

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
            'valid_codes_file = "a.csv"\n'
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
    def _build_with_seed(
        self,
        tmp_path: Path,
        seed_toml: str,
        csvs: dict[str, str] = TEST_SEED_CSVS,
    ) -> tuple[Path, Path]:
        input_dir = _make_input_dir(tmp_path)

        # Every classification needs a valid_codes_file (always-seed guarantee).
        # Write the canonical CSVs under <input_dir>/classifications/ where
        # build_db resolves valid_codes_dir.
        cls_dir = input_dir / "classifications"
        cls_dir.mkdir(parents=True, exist_ok=True)
        for name, body in csvs.items():
            (cls_dir / name).write_text(body, encoding="utf-8")

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
        built_providers: frozenset[str] | None,
        instance_labels: tuple[str, ...] = (),
    ) -> tuple[sqlite3.Connection, int]:
        """Call populate_classifications directly on an empty in-memory schema.

        Lets us assert the n_seeded return and the #597 drift scoping without
        the full build_db machinery (which never surfaces the return value or
        accepts built_providers=None). ``instance_labels`` seeds a minimal
        ``variable_instance`` row per label so a seed ``vardemangdsversion``
        string can be made to MATCH (the drift check only reads
        ``value_set_version_label``).
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
        # Dummy NOT NULL fields — only value_set_version_label is read by the
        # drift check; value_set_id stays NULL (no codes from instances).
        conn.executemany(
            "INSERT INTO variable_instance (cvid, register_id, register_variant_id, "
            "regver_id, var_id, value_set_version_label) VALUES (?, 1, 1, 1, 1, ?)",
            [(i, label) for i, label in enumerate(instance_labels, start=1)],
        )
        n_seeded = populate_classifications(
            conn,
            seed,
            valid_codes_dir=cls_dir,
            built_providers=built_providers,
        )
        return conn, n_seeded

    def test_every_classification_seeded_regardless_of_providers(self, tmp_path: Path):
        """Every classification is seeded regardless of `built_providers` —
        classifications are shared standards with git-tracked CSVs, so a
        provider-tagged entry (e.g. a SOS-tagged one) is seeded even on a build
        that excludes its provider AND references it nowhere. `built_providers`
        no longer gates seeding; it only scopes the #597 drift demotion.
        """
        seed_toml = (
            '[[classification]]\nshort_name = "SCBONLY"\nname = "SCB only"\n'
            'valid_codes_file = "scb.csv"\n'
            '[[classification]]\nshort_name = "SOSENT"\nname = "SOS entry"\n'
            'provider = "sos"\nvalid_codes_file = "sos.csv"\n'
        )
        csvs = {"scb.csv": "code,label\nA,Alpha\n", "sos.csv": "code,label\nX,Xray\n"}

        # built_providers={scb} (sos excluded, nothing references SOSENT): both
        # entries are still seeded.
        conn, n_seeded = self._populate_direct(
            tmp_path / "a", seed_toml, csvs, built_providers=frozenset({"scb"})
        )
        assert n_seeded == 2
        assert {
            r[0] for r in conn.execute("SELECT short_name FROM classification")
        } == {"SCBONLY", "SOSENT"}

        # built_providers=None (full build): everything seeded too.
        conn2, n2 = self._populate_direct(
            tmp_path / "b", seed_toml, csvs, built_providers=None
        )
        assert n2 == 2
        assert {
            r[0] for r in conn2.execute("SELECT short_name FROM classification")
        } == {"SCBONLY", "SOSENT"}

    def test_populate_leaves_supersedes_id_null(self, tmp_path: Path):
        """The seed no longer declares succession: every inserted classification
        leaves `supersedes_id` NULL. It becomes a DERIVED projection of
        `classification_replaced_by` later in the build
        (`derive_supersedes_from_edges`), so `populate_classifications` itself
        never sets it — even when the seed carries a stray (now-ignored)
        `supersedes` key."""
        seed_toml = (
            '[[classification]]\nshort_name = "A"\nname = "A"\n'
            'supersedes = "B"\nvalid_codes_file = "a.csv"\n'
            '[[classification]]\nshort_name = "B"\nname = "B"\n'
            'valid_codes_file = "b.csv"\n'
        )
        csvs = {"a.csv": "code,label\nA,Alpha\n", "b.csv": "code,label\nB,Bravo\n"}
        conn, n_seeded = self._populate_direct(
            tmp_path, seed_toml, csvs, built_providers=None
        )
        assert n_seeded == 2
        rows = conn.execute("SELECT supersedes_id FROM classification").fetchall()
        assert all(r[0] is None for r in rows)

    def test_untagged_demoted_when_scb_absent(self, tmp_path: Path):
        """#597: an UNTAGGED classification (label-source = scb) whose
        vardemangdsversion matches NO instance is demoted (no error) and still
        seeded when SCB is NOT built — its CSV keeps it above the empty guard."""
        seed_toml = (
            '[[classification]]\nshort_name = "ABSENT"\nname = "Absent"\n'
            'valid_codes_file = "absent.csv"\n'
            'vardemangdsversion = ["never-occurs"]\n'
        )
        csvs = {"absent.csv": "code,label\nA,Alpha\n"}
        # scb (the untagged label-source) is absent → demote, don't raise.
        conn, n_seeded = self._populate_direct(
            tmp_path,
            seed_toml,
            csvs,
            built_providers=frozenset({"sos"}),
        )
        assert n_seeded == 1
        assert {
            r[0] for r in conn.execute("SELECT short_name FROM classification")
        } == {"ABSENT"}

    def test_untagged_strict_when_scb_present(self, tmp_path: Path):
        """#597 P2 #2: the SAME untagged classification (label-source = scb) with
        NO matching instance must RAISE on a subset build that INCLUDES scb —
        the label-source IS built, so an unmatched string is a real typo, not the
        expected absence of an un-built source."""
        seed_toml = (
            '[[classification]]\nshort_name = "ABSENT"\nname = "Absent"\n'
            'valid_codes_file = "absent.csv"\n'
            'vardemangdsversion = ["never-occurs"]\n'
        )
        csvs = {"absent.csv": "code,label\nA,Alpha\n"}
        with pytest.raises(RegMetaError) as ei:
            self._populate_direct(
                tmp_path,
                seed_toml,
                csvs,
                built_providers=frozenset({"scb"}),
            )
        assert ei.value.code == "classification_seed_drift"

    def test_tagged_shared_seeded_and_demoted_when_source_unbuilt(self, tmp_path: Path):
        """#597: a classification tagged provider="sos" is SEEDED on a
        --providers fk build (sos not built), and its unmatched sos labels are
        DEMOTED (label-source sos ∉ {fk}) → no raise. Seeding no longer depends
        on a built provider referencing it."""
        seed_toml = (
            '[[classification]]\nshort_name = "ICD-10-SE"\nname = "ICD"\n'
            'provider = "sos"\nvalid_codes_file = "icd.csv"\n'
            'vardemangdsversion = ["sos-only-label"]\n'
        )
        csvs = {"icd.csv": "code,label\nA01,Alpha\n"}
        conn, n_seeded = self._populate_direct(
            tmp_path,
            seed_toml,
            csvs,
            built_providers=frozenset({"fk"}),
        )
        assert n_seeded == 1
        assert {
            r[0] for r in conn.execute("SELECT short_name FROM classification")
        } == {"ICD-10-SE"}

    def test_mixed_classification_hard_errors(self, tmp_path: Path):
        """#597: a classification with one MATCHED and one unmatched version
        string still hard-errors even when its label-source isn't built — a real
        typo/stale on a partly-present source isn't masked by the demote path."""
        seed_toml = (
            '[[classification]]\nshort_name = "MIXED"\nname = "Mixed"\n'
            'provider = "sos"\nvalid_codes_file = "mixed.csv"\n'
            'vardemangdsversion = ["present", "typo-never-occurs"]\n'
        )
        csvs = {"mixed.csv": "code,label\nA,Alpha\n"}
        with pytest.raises(RegMetaError) as ei:
            self._populate_direct(
                tmp_path,
                seed_toml,
                csvs,
                built_providers=frozenset({"fk"}),
                instance_labels=("present",),
            )
        assert ei.value.code == "classification_seed_drift"
        # Only the unmatched string is named, not the matched one.
        assert "typo-never-occurs" in ei.value.message
        assert "present" not in ei.value.message

    def test_full_build_still_strict_on_drift(self, tmp_path: Path):
        """#597: an unmatched seed on a full build (built_providers=None) still
        hard-errors classification_seed_drift — the label-source is always
        treated as built."""
        seed_toml = (
            '[[classification]]\nshort_name = "ABSENT"\nname = "Absent"\n'
            'valid_codes_file = "absent.csv"\n'
            'vardemangdsversion = ["never-occurs"]\n'
        )
        csvs = {"absent.csv": "code,label\nA,Alpha\n"}
        with pytest.raises(RegMetaError) as ei:
            self._populate_direct(
                tmp_path,
                seed_toml,
                csvs,
                built_providers=None,
            )
        assert ei.value.code == "classification_seed_drift"

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

    def test_classification_fts_populated(self, tmp_path: Path):
        db, _ = self._build_with_seed(tmp_path, TEST_SEED_TOML)
        conn = sqlite3.connect(db)
        rows = conn.execute(
            "SELECT short_name FROM classification_fts "
            "WHERE classification_fts MATCH 'Test'"
        ).fetchall()
        assert len(rows) >= 1

    def test_seed_drift_fails_build(self, tmp_path: Path):
        # #597: the SCB-only fixture build (build_db default providers=("scb",))
        # HAS scb built. GHOST is an UNTAGGED classification (label-source = scb)
        # with a wholly-stale string. scb IS its label-source AND is built →
        # source_built → HARD drift, so the build raises even on this subset.
        seed = (
            '[[classification]]\nshort_name = "GHOST"\nname = "No such label"\n'
            'valid_codes_file = "ghost.csv"\n'
            'vardemangdsversion = ["this-string-never-appears"]\n'
        )
        with pytest.raises(RegMetaError) as ei:
            self._build_with_seed(
                tmp_path, seed, {"ghost.csv": "vardekod,vardebenamning\nG,Ghost\n"}
            )
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
                skip_slugs=True,
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
                skip_slugs=True,
            )
        assert ei.value.code == "classification_csv_dir_missing"

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
        # skip_slugs: a seed test, not a slug test; without it the #556
        # canonical-seed preflight fires first (repo scb.toml + scb_canonical-less
        # tmp input).
        with pytest.raises(RegMetaError) as ei:
            build_db(input_dir=input_dir, db_dir=db_dir, skip_slugs=True)
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
        slug: str | None = None,
    ) -> None:
        """Seed a classification whose canonical code set is `codes` (each a
        (code, label) pair). is_valid=1 (canonical); level is the digit-length for
        all-digit codes, NULL otherwise — same rule as the build. `supersedes_id`
        (older predecessor on the vintage chain) and `valid_from`/`valid_to` (INTEGER
        years, NULLABLE = unbounded) feed the #494 vintage-period reclaim. `slug`
        defaults to `short_name.lower()` — non-NULL because the real build runs this
        pass AFTER `populate_slugs`, and the #494 reclaim's stem guard derives the
        vintage family from the slug (e.g. `sni2002`/`sni2007` → stem `sni`)."""
        self.conn.execute(
            "INSERT INTO classification "
            "(id, short_name, name, slug, supersedes_id, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                cls_id,
                short_name,
                short_name,
                slug if slug is not None else short_name.lower(),
                supersedes_id,
                valid_from,
                valid_to,
            ),
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
        slug: str | None = None,
    ) -> None:
        """Seed a classification with `is_valid=NULL` canonical rows — a no-CSV
        shape where observed codes ARE the code set, so the detector's
        `is_valid IS NOT 0` filter must keep them. No classification ships this
        shape today (every classification now has a CSV); this is a defensive
        guard for the NULL-tolerant filter, which we deliberately keep. Same
        level rule and same `supersedes_id`/`valid_from`/`valid_to`/`slug` fields
        as `add_classification`."""
        self.conn.execute(
            "INSERT INTO classification "
            "(id, short_name, name, slug, supersedes_id, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                cls_id,
                short_name,
                short_name,
                slug if slug is not None else short_name.lower(),
                supersedes_id,
                valid_from,
                valid_to,
            ),
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

    def test_label_agree_projection_shared_by_step5_and_residue(self) -> None:
        """#738 cross-caller invariant: step 5's confident-link query and the #513
        residue diagnostic read the SAME `_vs_label_agree` projection, so the
        `label_agree` they compute for one (value_set, cls) shape must agree. Step 5
        only acts on SINGLE-family sets and the diagnostic only surfaces MULTI-family
        sets, so the same set can't be in both paths; instead this builds, for each
        of two label_agree regimes, a single-family peer (step 5) and a multi-family
        peer (diagnostic) carrying the SAME label_agree shape against FAM_A — one
        code group fully labelled by FAM_A and a peer group with the same agreement
        ratio shared with FAM_B. It then asserts step 5's link/no-link decision is
        consistent with the label_agree the diagnostic reports for FAM_A. Fails if
        either caller re-inlines a divergent label_agree formula.

        Both regimes use n_codes = 10 (in [_MIN_CODES=8, _CONFIDENT_MIN_CODES=15) so
        containment passes AND the code-count floor doesn't bypass the label gate),
        and label_agree values are CLEARLY off the 0.90 threshold (1.0 / 0.8) to
        avoid float-boundary fragility:
          - ABOVE (1.0, all labels match): single-family peer is confidently linked
            and the diagnostic reports label_agree 1.0 for FAM_A.
          - BELOW (0.8, 8/10 labels match): single-family peer is NOT linked and the
            diagnostic reports label_agree 0.8 for FAM_A.

        The single-family and multi-family peers use DISJOINT code ranges so the
        single peer stays single-family (only FAM_A contains its codes) while the
        multi peer is multi-family (FAM_A AND FAM_B contain its codes); both code
        groups carry matching labels under FAM_A so the label_agree shape is shared.
        """
        from reg_meta_build.classifications import (
            dump_classification_residue,
            link_value_set_classifications,
        )

        def labelled(rng: range, prefix: str) -> list[tuple[str, str]]:
            return [(str(i).zfill(4), f"{prefix} {i}") for i in rng]

        g = _Graph()

        # --- ABOVE regime (label_agree 1.0) -------------------------------------
        # `hi_single` (0001..0010) is the single-family group; `hi_multi`
        # (0011..0020) is the multi-family group. FAM_A_HI canonically carries BOTH
        # under matching labels (label_agree 1.0 for either group); FAM_B_HI carries
        # ONLY the multi group, so only the multi peer is contained in two families.
        hi_single = labelled(range(1, 11), "HI")
        hi_multi = labelled(range(11, 21), "HI")
        g.add_classification(70, "FAM_A_HI", hi_single + hi_multi)
        g.add_classification(71, "FAM_B_HI", hi_multi)
        # Single-family peer: only FAM_A_HI contains these codes → label_agree 1.0
        # and step 5 must confidently link.
        g.add_value_set(170, hi_single)
        g.add_variable_state(970, 170)
        # Multi-family peer (FAM_A_HI + FAM_B_HI): same label_agree 1.0 shape against
        # FAM_A → diagnostic residue, reporting FAM_A_HI label_agree 1.0.
        g.add_value_set(171, hi_multi)
        g.add_variable_state(971, 171)

        # --- BELOW regime (label_agree 0.8) -------------------------------------
        # `lo_single` (0021..0030) and `lo_multi` (0031..0040) groups; FAM_A_LO
        # carries both under matching labels. Each peer relabels 2 of its 10 codes →
        # label_agree 8/10 = 0.8 against FAM_A_LO. FAM_B_LO carries ONLY the multi
        # group's codes, so only the multi peer is multi-family.
        lo_single = labelled(range(21, 31), "LO")
        lo_multi = labelled(range(31, 41), "LO")
        g.add_classification(72, "FAM_A_LO", lo_single + lo_multi)
        g.add_classification(73, "FAM_B_LO", lo_multi)

        def relabel_tail(codes: list[tuple[str, str]]) -> list[tuple[str, str]]:
            # 8 matching labels + 2 relabeled (codes unchanged → containment 1.0).
            return codes[:8] + [(c, f"renamed {c}") for c, _ in codes[8:]]

        # Single-family peer: only FAM_A_LO contains these codes → label_agree 0.8 <
        # 0.90 AND n_codes 10 < 15 → step 5 must NOT link.
        g.add_value_set(172, relabel_tail(lo_single))
        g.add_variable_state(972, 172)
        # Multi-family peer (FAM_A_LO + FAM_B_LO): same 0.8 shape against FAM_A →
        # diagnostic residue, reporting FAM_A_LO label_agree 0.8.
        g.add_value_set(173, relabel_tail(lo_multi))
        g.add_variable_state(973, 173)

        # Step 5: only the ABOVE single-family peer links (label_agree 1.0); the BELOW
        # one declines (label_agree 0.8). The multi-family peers are ambiguous → never
        # linked. So exactly one value set is confidently linked.
        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 1
        assert g.candidates() == [(970, 170, 70)]

        # Diagnostic: the multi-family peers are residue; read back FAM_A's reported
        # label_agree for each and assert it matches the regime step 5 decided on.
        result = dump_classification_residue(g.conn)
        residue_by_vs = {rvs.value_set_id: rvs for rvs in result.value_sets}

        def fam_a_label_agree(value_set_id: int, short_name: str) -> float:
            rvs = residue_by_vs[value_set_id]
            (cand,) = [c for c in rvs.candidates if c.short_name == short_name]
            return cand.label_agree

        # ABOVE: FAM_A_HI label_agree 1.0 — the value step 5 confidently linked on.
        assert fam_a_label_agree(171, "FAM_A_HI") == pytest.approx(1.0)
        # BELOW: FAM_A_LO label_agree 0.8 — the value step 5 declined on. Both callers
        # see 0.8: step 5 declines AND the diagnostic reports 0.8 (a re-inlined
        # divergent formula would break one of these).
        assert fam_a_label_agree(173, "FAM_A_LO") == pytest.approx(0.8)

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
        observed codes ARE its code set). No classification ships this shape today
        (every classification now has a CSV); this is a defensive-guard regression
        test that the detector's NULL-tolerant `is_valid IS NOT 0` filter — which
        we deliberately keep — still lets a value set enumerating those codes
        link."""
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
        this set never enters `_vs_dominant_chain` (single candidate)."""
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

    def test_same_root_different_stem_dimensions_stay_ambiguous(self) -> None:
        """#579 stem guard: the curated sun1996 → {niva, inriktning, grupp} split puts
        ORTHOGONAL SUN dimensions under ONE chain root, so the chain-root guard alone
        would collapse a code-ambiguous LABEL-LESS value set spanning two of those
        dimensions to one. The family key is (chain root AND slug stem): sun-niva2000
        (stem `sun-niva`) and sun-inriktning2000 (stem `sun-inriktning`) share the
        sun1996 root but have DIFFERENT stems → NO reclaim; the set stays in the
        residue for curation. Contrast `test_same_chain_collapse_latest_overlapping_wins`
        (sni2002/sni2007, same `sni` stem → still collapses)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # Root of the split; its OWN codes are disjoint so it is not a candidate — the
        # value set matches the two dimensions, not the umbrella root.
        g.add_classification(
            90,
            "SUN1996",
            _numeric_codes("ROOT", 10, 4),
            slug="sun1996",
            valid_from=1996,
            valid_to=None,
        )
        # Two dimensions chained onto the sun1996 root → SAME chain root, DIFFERENT
        # slug stems (`sun-niva` vs `sun-inriktning`).
        shared = _numeric_codes("SUN", 10, 4)
        g.add_classification(
            91,
            "SUN-niva2000",
            shared + [("9001", "niva only")],
            slug="sun-niva2000",
            supersedes_id=90,
            valid_from=2000,
            valid_to=None,
        )
        g.add_classification(
            92,
            "SUN-inriktning2000",
            shared + [("9002", "inriktning only")],
            slug="sun-inriktning2000",
            supersedes_id=90,
            valid_from=2000,
            valid_to=None,
        )
        g.add_value_set(190, shared)  # contained in BOTH dimensions → multi-family
        g.add_variable_state(990, 190)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # Same root but two stems → two single-edition families, no multi-vintage
        # chain → it never qualifies as a dominant chain in _vs_dominant_chain.
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_two_multi_vintage_chains_label_dominant_wins(self) -> None:
        """#897 label-dominant relaxation: a value set is ≥0.90-contained in TWO
        multi-vintage chains on DIFFERENT roots — an LKF county chain (LKF1996 →
        LKF1998) whose canonical labels MATCH the value set (label_agree 1.0) and an
        SSYK chain (SSYK2012 → SSYK2019) carrying the SAME codes under DIFFERENT labels
        (label_agree 0). The OLD `HAVING COUNT(*) = 1` gate bailed to residue (two
        multi-vintage chains); the new gate selects the LABEL-DOMINANT chain — LKF
        clears the 0.90 floor AND strictly dominates SSYK's 0 — and 7c picks LKF's
        latest overlapping edition (LKF1998)."""
        from reg_meta_build.classifications import link_value_set_classifications
        from reg_meta_build.db import _backfill_state_classifications

        g = _Graph()
        shared = _numeric_codes("LKF", 10, 2)  # < 15 → never confident
        # LKF chain: the value set's labels are LKF's canonical labels → label_agree 1.0.
        g.add_classification(
            70,
            "LKF1996",
            shared + [("90", "lkf96 only")],
            valid_from=1996,
            valid_to=1997,
        )
        g.add_classification(
            71,
            "LKF1998",
            shared + [("91", "lkf98 only")],
            supersedes_id=70,
            valid_from=1998,
            valid_to=None,
        )
        # SSYK chain (4-digit-year slugs → one `ssyk` stem family): SAME codes, but
        # DIFFERENT labels → label_agree 0 vs the value set.
        relabeled = [(code, f"ssyk {code}") for code, _ in shared]
        g.add_classification(
            72,
            "SSYK2012",
            relabeled + [("92", "ssyk2012 only")],
            valid_from=2012,
            valid_to=2018,
        )
        g.add_classification(
            73,
            "SSYK2019",
            relabeled + [("93", "ssyk2019 only")],
            supersedes_id=72,
            valid_from=2019,
            valid_to=None,
        )
        g.add_value_set(170, shared)  # LKF labels → agrees with LKF, not SSYK
        g.add_variable_state(970, 170)  # open-ended → overlaps LKF1998 and SSYK2019

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 0  # not confident (multi-family)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        # Latest overlapping edition of the LABEL-DOMINANT (LKF) chain.
        assert g.candidates() == [(970, 170, 71)]

        _backfill_state_classifications(g.conn)
        assert g.tagged_classification(970) == 71

    def test_two_multi_vintage_chains_label_tie_stays_ambiguous(self) -> None:
        """#897: when TWO multi-vintage chains are LABEL-LESS (the value set's labels
        match NEITHER chain → both fam_max_la 0), no chain strictly dominates → the
        value set stays in residue (no link). This is the SSYK-coincidence class the
        absolute floor already drops for single chains, here generalized: a tie can
        never produce a winner."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        # Two DIFFERENT-root multi-vintage chains sharing the codes.
        g.add_classification(
            80, "SNI2002", shared + [("9001", "a")], valid_from=2002, valid_to=2007
        )
        g.add_classification(
            81,
            "SNI2007",
            shared + [("9002", "b")],
            supersedes_id=80,
            valid_from=2008,
            valid_to=None,
        )
        g.add_classification(
            82, "SSYK2012", shared + [("9003", "c")], valid_from=2012, valid_to=2018
        )
        g.add_classification(
            83,
            "SSYK2019",
            shared + [("9004", "d")],
            supersedes_id=82,
            valid_from=2019,
            valid_to=None,
        )
        # Value set relabels every shared code → label_agree 0 against BOTH chains.
        relabeled = [(code, f"vs {code}") for code, _ in shared]
        g.add_value_set(180, relabeled)
        g.add_variable_state(980, 180)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_two_multi_vintage_chains_high_label_tie_stays_ambiguous(self) -> None:
        """#897: a tie that CLEARS the absolute floor must still stay ambiguous — the
        gate is STRICT dominance, not "both confident". Two multi-vintage chains share
        the codes AND both label-agree 1.0 (the value set's labels match BOTH because
        the chains carry identical (code, label) canon). Neither strictly exceeds the
        other → no winner → residue. Guards that the 0.90 floor alone can't reclaim a
        label tie."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("Y", 10, 4)
        # Both chains carry the SHARED codes under the SAME labels as the value set →
        # label_agree 1.0 on both, so fam_max_la ties at 1.0.
        g.add_classification(
            84, "SNI2002", shared + [("9001", "a")], valid_from=2002, valid_to=2007
        )
        g.add_classification(
            85,
            "SNI2007",
            shared + [("9002", "b")],
            supersedes_id=84,
            valid_from=2008,
            valid_to=None,
        )
        g.add_classification(
            86, "SSYK2012", shared + [("9003", "c")], valid_from=2012, valid_to=2018
        )
        g.add_classification(
            87,
            "SSYK2019",
            shared + [("9004", "d")],
            supersedes_id=86,
            valid_from=2019,
            valid_to=None,
        )
        g.add_value_set(181, shared)  # labels match BOTH chains → 1.0 vs 1.0 tie
        g.add_variable_state(981, 181)

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

        counts = link_value_set_classifications(g.conn)
        # Pre-existing claim untouched; vintage step added nothing.
        assert g.candidates() == [(964, 164, 99)]
        # The counts must reflect ACTUAL emits (Codex P2): the pair satisfies the
        # one-chain vintage heuristic but its (variable_id, value_set_id) was already
        # claimed, so the emit guard skips it — it is NOT counted as reclaimed. The
        # pre-fix code counted off `_vs_vintage` and would report 1 here.
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["vintage_variables_linked"] == 0
        # Residual is grain-precise (#494 Codex P2 FIX 2): the value set's only pair
        # already HAS a candidate (the curated cls 99), so it is NOT in the curation
        # residue even though the vintage step reclaimed nothing — it was resolved by
        # the curated/feed claim. (Under the retired `multi_family -
        # vintage_value_sets_linked` subtraction this read 1.)
        assert counts["multi_family"] == 1
        assert counts["multi_family_after"] == 0

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

    def test_disjoint_states_skip_gap_vintage(self) -> None:
        """FIX 1 (#494 Codex P2): a pair with TWO DISJOINT states straddling a CLOSED
        gap vintage, where the gap vintage is the LATEST that overlaps the aggregate
        span — so the RETIRED span logic would have emitted it. States 2003–2006 and
        2018–2020; candidates SNI2002 [2002,2007] (covers state1) and a CLOSED SNI2010
        [2010,2015] (covers NEITHER state — sits in the temporal hole). No candidate
        covers the 2018–2020 state. The aggregate MIN/MAX span [2003,2020] "overlaps"
        SNI2010 (2010<=2020 AND 2015>=2003), and SNI2010 has the higher valid_from, so
        the OLD span logic would have ranked SNI2010 rn=1 and emitted it — tagging the
        variable with a vintage NONE of its states fall in. Per-state overlap anchors
        to a real window: only SNI2002 overlaps a real state (2003–2006), so SNI2002 is
        emitted, NOT the gap edition. The gap edition is a YEAR-tailed vintage (slug
        `sni2010`, same `sni` stem) so the #579 stem guard keeps it on the family."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            80,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            81,
            "SNI2010",  # CLOSED edition in the hole between the two states
            shared + [("9002", "gap only")],
            supersedes_id=80,
            valid_from=2010,
            valid_to=2015,
        )
        g.add_value_set(180, shared)
        # Two DISJOINT states: 2003–2006 (covered by SNI2002) and 2018–2020 (covered by
        # NEITHER edition). The gap edition's window 2010–2015 touches NO real state.
        g.add_variable_state(980, 180, valid_from="2003-01-01", valid_to="2006-12-31")
        g.add_variable_state(980, 180, valid_from="2018-01-01", valid_to="2020-12-31")

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 1
        # Emitted vintage overlaps a REAL state (SNI2002 overlaps 2003–2006), NOT the
        # gap edition 81 (overlaps the aggregate span but no real state). The retired
        # span logic would have emitted (980, 180, 81).
        assert g.candidates() == [(980, 180, 80)]

    def test_latest_among_real_overlapping_not_latest_overlapping_span(self) -> None:
        """FIX 1 focused variant: the ONLY candidate overlapping any REAL state is the
        OLDER one. States 2003–2006 only; candidates SNI2002 [2002,2007] and a later
        SNI2016 [2016, unbounded]. The aggregate span is [2003, 2006] here (single
        state), but the point is the pick is "latest among real-overlapping" — only
        SNI2002 overlaps the 2003–2006 state, so it wins over the later edition. Both
        editions share the `sni` stem so the #579 stem guard keeps them on one family."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        g.add_classification(
            83,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            84,
            "SNI2016",
            shared + [("9003", "late only")],
            supersedes_id=83,
            valid_from=2016,
            valid_to=None,
        )
        g.add_value_set(181, shared)
        g.add_variable_state(981, 181, valid_from="2003-01-01", valid_to="2006-12-31")

        counts = link_value_set_classifications(g.conn)
        assert counts["vintage_value_sets_linked"] == 1
        # Only SNI2002 overlaps a real state; the later edition does not.
        assert g.candidates() == [(981, 181, 83)]

    def test_shared_value_set_partial_reclaim_residual_count(self) -> None:
        """FIX 2 (#494 Codex P2): a multi-family value_set shared by TWO variables.
        Variable A's state (2008–open) overlaps a vintage → reclaimed. Variable B's
        state (2018–2020) overlaps NO candidate vintage → NOT reclaimed. The value set
        therefore still has an unresolved (variable_id, value_set_id) pair, so it stays
        in the residue: vintage_value_sets_linked == 1 (A reclaimed) but
        multi_family_after == 1 (the value set is still residual, NOT 0 — the naive
        `multi_family - vintage_value_sets_linked` would wrongly report 0)."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        # Two chain vintages, both CLOSED before 2018 so variable B's 2018–2020 state
        # overlaps neither.
        g.add_classification(
            85,
            "SNI2002",
            shared + [("9001", "2002 only")],
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            86,
            "SNI2007",
            shared + [("9002", "2007 only")],
            supersedes_id=85,
            valid_from=2008,
            valid_to=2015,
        )
        g.add_value_set(182, shared)
        # Variable A: state 2008–2015 overlaps SNI2007 → reclaimed.
        g.add_variable_state(982, 182, valid_from="2008-01-01", valid_to="2015-12-31")
        # Variable B (same value set): state 2018–2020 overlaps NEITHER → not reclaimed.
        g.add_variable_state(983, 182, valid_from="2018-01-01", valid_to="2020-12-31")

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        assert counts["vintage_value_sets_linked"] == 1  # variable A reclaimed
        # The value set still has an unresolved pair (variable B) → residual, NOT 0.
        assert counts["multi_family_after"] == 1
        # Only A got a candidate; B has none.
        assert g.candidates() == [(982, 182, 86)]

    def test_dominant_chain_reclaims_past_single_off_chain_stray(self) -> None:
        """Dominant-chain rule (#514): the real LABELED LKF county residue. A county
        code set matches >=2 editions of an LKF-like chain (one multi-vintage chain)
        PLUS a SINGLE off-chain stray (an SNI2007 division that HAS a DB predecessor
        SNI2002 which does NOT match the codes, so only SNI2007 is a candidate →
        single-edition stray). The OLD all-on-chain rule blocked on that one off-chain
        candidate; the dominant-chain rule reclaims, because EXACTLY ONE family is a
        multi-vintage chain.

        Labels now MATTER (the conditional absolute floor): with an off-chain stray
        present the dominant family must label-agree at the confident bar. This is the
        REAL labeled-LKF case — the value set's (code,label) pairs equal the LKF
        editions' canonical pairs (both built from the same `LKF` prefix via
        `_numeric_codes`, so label_agree = 1.0 >= 0.90), while the SNI stray carries
        DIFFERENT labels (label_agree 0). So the dominant clears the floor AND beats the
        stray. The emitted candidate is the LATEST LKF edition overlapping the state,
        NOT the off-chain stray. (Contrast the label-LESS SSYK shape, which now stays
        ambiguous: `test_dominant_chain_labelless_with_stray_stays_ambiguous`.)"""
        from reg_meta_build.classifications import link_value_set_classifications
        from reg_meta_build.db import _backfill_state_classifications

        g = _Graph()
        # The value set's (code,label) pairs EQUAL the LKF editions' pairs (same `LKF`
        # prefix → identical labels) → label_agree 1.0 against LKF, clearing the floor.
        county_lkf = _numeric_codes("LKF", 10, 4)
        # Multi-vintage LKF chain: two year-editions, both >=0.90-containing the set.
        g.add_classification(
            200,
            "LKF2015",
            county_lkf + [("9001", "lkf2015 only")],
            slug="lkf2015",
            valid_from=2015,
            valid_to=2017,
        )
        g.add_classification(
            201,
            "LKF2018",
            county_lkf + [("9002", "lkf2018 only")],
            slug="lkf2018",
            supersedes_id=200,
            valid_from=2018,
            valid_to=None,
        )
        # Off-chain stray: SNI2007 (own chain root, distinct stem) ALSO contains the
        # 10 codes (>=0.90 → a real candidate the OLD rule blocked on) but carries
        # DIFFERENT labels (label_agree 0). Its DB predecessor SNI2002 has DISJOINT
        # codes → NOT a candidate, so only ONE SNI edition is a candidate → a permitted
        # single-edition stray (NOT a DB-standalone test: SNI2007 has a predecessor).
        sni_codes = [
            (str(i).zfill(4), f"SNI {i}") for i in range(1, 11)
        ]  # SAME kods, SNI labels
        sni_disjoint = [(str(i).zfill(4), f"SNI {i}") for i in range(5000, 5010)]
        g.add_classification(
            210,
            "SNI2002",
            sni_disjoint,
            slug="sni2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            211,
            "SNI2007",
            sni_codes + [("9003", "sni only")],  # same county kods, SNI labels → la 0
            slug="sni2007",
            supersedes_id=210,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(200, county_lkf)  # labels EQUAL LKF → label_agree 1.0 vs LKF
        g.add_variable_state(920, 200)  # open-ended → overlaps both LKF editions

        counts = link_value_set_classifications(g.conn)
        # 3 candidates (2 LKF + 1 SNI) → multi-family, not confident.
        assert counts["value_sets_linked"] == 0
        assert counts["multi_family"] == 1
        # One multi-vintage chain (LKF, label_agree 1.0 >= 0.90 floor) + a single-edition
        # SNI stray (label_agree 0) → dominant clears the floor AND beats the stray → reclaim.
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        # LATEST dominant-chain (LKF) edition overlapping the open state — NOT the
        # off-chain SNI2007 stray.
        assert g.candidates() == [(920, 200, 201)]

        _backfill_state_classifications(g.conn)
        assert g.tagged_classification(920) == 201

    def test_dominant_chain_labelless_with_stray_stays_ambiguous(self) -> None:
        """THE KEY PRECISION TEST (#514 conditional absolute floor — the SSYK shape).
        A LABEL-LESS dominant chain (>=2 editions whose codes the value set matches, but
        the value set carries its OWN labels → label_agree 0 against every edition) PLUS
        a single off-chain stray (different root, single edition). The real-data failure
        this guards: short generic code sets (1–9 response scales) coincidentally match
        BOTH SSYK editions (here ssyk2008/ssyk2012, stem `ssyk`), so SSYK looks
        "dominant", and being label-less they sailed
        through the old COALESCE-0 relative lever and got reclaimed to the OCCUPATIONAL
        SSYK. With an off-chain stray present the absolute floor now REQUIRES the
        dominant family to label-agree >= _CONFIDENT_LABEL_AGREE; label_agree 0 cannot
        clear it → stays ambiguous. Mirrors `test_dominant_chain_reclaims_past_single_off
        _chain_stray` (the LABELED LKF case) which DOES clear the floor."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # The classifications carry the `SSYK` codes; the value set matches the SAME
        # kods but under its OWN labels (`SCALE` prefix) → label_agree 0 everywhere.
        ssyk_codes = _numeric_codes("SSYK", 10, 4)
        vs_codes = _numeric_codes("SCALE", 10, 4)  # SAME kods, DIFFERENT labels
        # DOMINANT (label-less) chain: two SSYK editions sharing the `ssyk` stem (both
        # 4-digit-year slugs, so `classification_slug_stem` strips the year-tail and they
        # bucket as ONE vintage family), both >=0.90-containing the set.
        g.add_classification(
            500,
            "SSYK2008",
            ssyk_codes + [("9001", "ssyk2008 only")],
            slug="ssyk2008",
            valid_from=2008,
            valid_to=2013,
        )
        g.add_classification(
            501,
            "SSYK2012",
            ssyk_codes + [("9002", "ssyk2012 only")],
            slug="ssyk2012",
            supersedes_id=500,
            valid_from=2014,
            valid_to=None,
        )
        # Off-chain stray: own root + stem, single edition, also contains the codes.
        g.add_classification(
            502,
            "SNI2007",
            ssyk_codes + [("9003", "sni only")],
            slug="sni2007",
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(
            500, vs_codes
        )  # same kods, value-set-only labels → label_agree 0
        g.add_variable_state(940, 500)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # Label-less dominant chain + off-chain stray → absolute floor blocks the
        # coincidence (label_agree 0 < 0.90).
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_allonchain_labelless_still_reclaims(self) -> None:
        """Regression guard that the conditional floor is CONDITIONAL on off-chain
        strays — it must NOT over-reach into #494's label-free all-on-chain behavior.
        Pure all-on-chain: >=2 editions of ONE chain, NO off-chain stray, LABEL-LESS
        (the value set's labels differ from the editions' → label_agree 0). With no
        off-chain stray the floor stays on its NOT-EXISTS branch (codes alone are
        decisive), so it STILL reclaims. If the floor over-reached to require a positive
        label bar unconditionally, this label-less all-on-chain set would wrongly stay
        ambiguous — so this pins #494's label-free behavior."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # One chain, two editions; the value set matches the codes but under its OWN
        # labels → label_agree 0. NO off-chain family exists.
        chain_codes = _numeric_codes("SNI", 10, 4)
        vs_codes = _numeric_codes("VSLBL", 10, 4)  # SAME kods, DIFFERENT labels
        g.add_classification(
            520,
            "SNI2002",
            chain_codes + [("9001", "2002 only")],
            slug="sni2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            521,
            "SNI2007",
            chain_codes + [("9002", "2007 only")],
            slug="sni2007",
            supersedes_id=520,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(
            520, vs_codes
        )  # same kods, value-set-only labels → label_agree 0
        g.add_variable_state(950, 520)  # open-ended → overlaps both editions

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # No off-chain stray → all-on-chain branch → label-free reclaim (#494 preserved).
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        assert g.candidates() == [(950, 520, 521)]

    def test_two_multi_vintage_chains_stay_ambiguous(self) -> None:
        """Two distinct families are EACH a multi-vintage chain (>=2 matched editions
        each) → a genuine cross-family span, NOT a dominant chain → stays in the
        residue. `dominant` requires EXACTLY ONE multi-vintage family, so two such
        families disqualify the value set."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # Shared codes both families contain (label-less value set, so label lever is
        # not the gate here — the structural exactly-one rule is).
        shared = _numeric_codes("CODE", 10, 4)
        # Chain A: two editions, both contain the set.
        g.add_classification(
            220,
            "ACHAIN2002",
            shared + [("9001", "a2002")],
            slug="achain2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            221,
            "ACHAIN2008",
            shared + [("9002", "a2008")],
            slug="achain2008",
            supersedes_id=220,
            valid_from=2008,
            valid_to=None,
        )
        # Chain B: a DIFFERENT root + stem, also two editions both containing the set.
        g.add_classification(
            230,
            "BCHAIN2002",
            shared + [("9003", "b2002")],
            slug="bchain2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            231,
            "BCHAIN2008",
            shared + [("9004", "b2008")],
            slug="bchain2008",
            supersedes_id=230,
            valid_from=2008,
            valid_to=None,
        )
        g.add_value_set(210, shared)
        g.add_variable_state(930, 210)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # Two multi-vintage chains → NOT exactly one → no reclaim.
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_label_lever_blocks_when_off_chain_stray_label_agrees_better(self) -> None:
        """Label lever (#514, precision): the dominant chain is structurally dominant
        (>=2 editions) but its codes carry MISMATCHED labels (label_agree 0), while a
        single off-chain stray has EXACTLY matching (code,label) pairs (label_agree
        1.0). Because the stray label-agrees strictly better than the dominant family,
        the lever BLOCKS the reclaim — a coincidental dominant chain must not beat a
        stray that actually matches labels."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # The value set's (code, label) pairs are the TRUTH the label lever compares to.
        truth = _numeric_codes("TRUE", 10, 4)
        # Dominant chain: SAME codes but DIFFERENT labels → label_agree 0 on both.
        mislabeled = [(code, f"WRONG {i}") for i, (code, _) in enumerate(truth, 1)]
        g.add_classification(
            240,
            "CHAIN2002",
            mislabeled + [("9001", "c2002")],
            slug="chain2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            241,
            "CHAIN2008",
            mislabeled + [("9002", "c2008")],
            slug="chain2008",
            supersedes_id=240,
            valid_from=2008,
            valid_to=None,
        )
        # Off-chain stray: own root + stem, EXACT (code,label) match → label_agree 1.0.
        g.add_classification(
            250,
            "STRAY2010",
            truth + [("9003", "stray only")],
            slug="stray2010",
            valid_from=2010,
            valid_to=None,
        )
        g.add_value_set(220, truth)
        g.add_variable_state(940, 220)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # Dominant chain by structure, but the stray label-agrees better → blocked.
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_same_root_multi_vintage_subdimension_stays_ambiguous(self) -> None:
        """#514 off-chain precision (Fix 1): a permitted stray must be OFF-CHAIN — a
        DIFFERENT chain root than the dominant family. Here a LABEL-LESS value set
        (its codes carry value-set-only labels, so its label_agree against every
        candidate is 0) matches a 2-edition `sun-niva` chain (the DOMINANT
        multi-vintage chain) PLUS a single `sun-inriktning` edition that shares the
        SAME sun1996 chain root but a DIFFERENT slug stem. That stray is NOT off-chain
        — it is the #579 orthogonal SUN dimension — so the `NOT EXISTS` guard
        disqualifies the set: it stays ambiguous rather than collapsing the orthogonal
        dimension onto sun-niva. With every label_agree at 0, the label lever is on the
        COALESCE-0 path and cannot move the outcome, so the STRUCTURAL same-root guard
        is provably the SOLE gate keeping the set ambiguous.

        Regression test for Fix 1: WITHOUT the `NOT EXISTS` guard, the 2-edition
        sun-niva chain would be the sole multi-vintage family (the lone sun-inriktning
        edition is single-edition) → it would wrongly reclaim."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # Codes the classifications carry. The value set matches the SAME codes but
        # under its OWN labels (`vs_codes`, distinct "SUNVS" prefix → same kods,
        # DIFFERENT labels), so every candidate's label_agree is 0 → the label lever
        # stays on the COALESCE-0 path and only the STRUCTURAL same-root guard can
        # keep the set ambiguous.
        shared = _numeric_codes("SUN", 10, 4)
        vs_codes = _numeric_codes("SUNVS", 10, 4)  # SAME codes, DIFFERENT labels
        # Umbrella root of the curated #579 split; its own codes are disjoint so it is
        # not itself a candidate — the value set matches the two dimensions, not the root.
        g.add_classification(
            300,
            "SUN1996",
            _numeric_codes("ROOT", 10, 4),
            slug="sun1996",
            valid_from=1996,
            valid_to=None,
        )
        # DOMINANT dimension: a 2-edition sun-niva chain, both >=0.90-containing the set
        # (same sun1996 root, stem `sun-niva`).
        g.add_classification(
            301,
            "SUN-niva2000",
            shared + [("9101", "niva2000 only")],
            slug="sun-niva2000",
            supersedes_id=300,
            valid_from=2000,
            valid_to=2009,
        )
        g.add_classification(
            302,
            "SUN-niva2010",
            shared + [("9102", "niva2010 only")],
            slug="sun-niva2010",
            supersedes_id=301,
            valid_from=2010,
            valid_to=None,
        )
        # Orthogonal sub-dimension: a SINGLE sun-inriktning edition, same sun1996 root,
        # DIFFERENT stem `sun-inriktning`. Shares the dominant's root → disqualifies.
        g.add_classification(
            303,
            "SUN-inriktning2000",
            shared + [("9103", "inriktning only")],
            slug="sun-inriktning2000",
            supersedes_id=300,
            valid_from=2000,
            valid_to=None,
        )
        g.add_value_set(
            300, vs_codes
        )  # same kods, value-set-only labels → label_agree 0
        g.add_variable_state(950, 300)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # Same-root sun-inriktning stray → dominant is NOT unique on its root → no reclaim.
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []

    def test_relative_lever_above_floor(self) -> None:
        """The RELATIVE label lever still bites ABOVE the conditional absolute floor.
        With an off-chain stray present, BOTH gates apply: the dominant family must
        clear the absolute floor (`>= _CONFIDENT_LABEL_AGREE`) AND label-agree at least
        as well as every off-chain stray.

        Case A (reclaims): the dominant chain label-agrees at 1.0 (clears the floor) and
        the off-chain stray at a LOWER 0.8 → dominant beats the stray → reclaim to the
        latest chain edition.

        Case B (blocked above the floor): the dominant chain label-agrees at exactly the
        floor (0.9 — it DOES clear the absolute bar) but the off-chain stray label-agrees
        HIGHER (1.0) → the relative lever blocks it even though the floor is met. A
        coincidental dominant chain must never beat a stray that matches labels better.
        """
        from reg_meta_build.classifications import link_value_set_classifications

        # --- Case A: dominant 1.0, stray 0.8 → reclaim ---
        g = _Graph()
        # 10-code value set whose (code,label) pairs the dominant chain reproduces
        # EXACTLY (label_agree 1.0). The off-chain stray reproduces only 8 of the 10
        # labels (codes 9..10 relabeled) → label_agree 0.8.
        value_codes = _numeric_codes("VS", 10, 4)
        stray_tail = value_codes[:8] + [
            (str(i).zfill(4), f"STRAYLBL {i}") for i in range(9, 11)
        ]
        # DOMINANT chain: 2 editions, exact (code,label) match → label_agree 1.0.
        g.add_classification(
            310,
            "CHAIN2002",
            value_codes + [("9201", "c2002 only")],
            slug="chain2002",
            valid_from=2002,
            valid_to=2009,
        )
        g.add_classification(
            311,
            "CHAIN2010",
            value_codes + [("9202", "c2010 only")],
            slug="chain2010",
            supersedes_id=310,
            valid_from=2010,
            valid_to=None,
        )
        # Off-chain stray: DIFFERENT root + stem, 0.8 label_agree (< dominant's 1.0).
        g.add_classification(
            312,
            "STRAY2005",
            stray_tail + [("9203", "stray only")],
            slug="stray2005",
            valid_from=2005,
            valid_to=None,
        )
        g.add_value_set(310, value_codes)
        g.add_variable_state(960, 310)

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # Dominant 1.0 clears the floor AND beats the stray's 0.8 → reclaim to latest edition.
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        assert g.candidates() == [(960, 310, 311)]

        # --- Case B: dominant at floor (0.9), stray higher (1.0) → blocked ---
        g2 = _Graph()
        # Dominant reproduces 9 of 10 labels (code 10 relabeled) → label_agree 0.9 (==
        # the floor, so it CLEARS the absolute bar). The off-chain stray reproduces all
        # 10 → label_agree 1.0 (> dominant) → the relative lever blocks.
        value_codes2 = _numeric_codes("VS", 10, 4)
        dom_tail = value_codes2[:9] + [("0010", "DOMLBL 10")]
        g2.add_classification(
            410,
            "CHAIN2002",
            dom_tail + [("9201", "c2002 only")],
            slug="chain2002",
            valid_from=2002,
            valid_to=2009,
        )
        g2.add_classification(
            411,
            "CHAIN2010",
            dom_tail + [("9202", "c2010 only")],
            slug="chain2010",
            supersedes_id=410,
            valid_from=2010,
            valid_to=None,
        )
        g2.add_classification(
            412,
            "STRAY2005",
            value_codes2 + [("9203", "stray only")],  # exact match → label_agree 1.0
            slug="stray2005",
            valid_from=2005,
            valid_to=None,
        )
        g2.add_value_set(310, value_codes2)
        g2.add_variable_state(960, 310)

        counts2 = link_value_set_classifications(g2.conn)
        assert counts2["multi_family"] == 1
        # Dominant 0.9 meets the floor but the stray's 1.0 beats it → relative lever blocks.
        assert counts2["vintage_value_sets_linked"] == 0
        assert counts2["multi_family_after"] == 1
        assert g2.candidates() == []

    def test_dominant_chain_reclaims_past_multiple_off_chain_strays(self) -> None:
        """The "any number of off-chain strays" clause: a dominant chain (2 editions)
        plus TWO distinct single-edition off-chain strays, each a DIFFERENT root from
        the dominant AND from each other, still reclaims to the latest dominant edition.
        Every family shares the `shared` codes verbatim with the value set, so the
        dominant chain and both strays all label_agree at 1.0 — the label lever passes
        by TIE, not by margin. The pinned behavior is the STRUCTURAL exactly-one-chain
        rule reclaiming past MULTIPLE distinct-root strays: neither stray shares the
        dominant's root, so the dominant chain is the sole multi-vintage family."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        shared = _numeric_codes("CODE", 10, 4)
        # DOMINANT chain: 2 editions, both >=0.90-containing the set.
        g.add_classification(
            320,
            "LKF2015",
            shared + [("9301", "lkf2015 only")],
            slug="lkf2015",
            valid_from=2015,
            valid_to=2017,
        )
        g.add_classification(
            321,
            "LKF2018",
            shared + [("9302", "lkf2018 only")],
            slug="lkf2018",
            supersedes_id=320,
            valid_from=2018,
            valid_to=None,
        )
        # Stray A: own root + stem, single edition, also contains the codes.
        g.add_classification(
            322,
            "SNI2007",
            shared + [("9303", "sni only")],
            slug="sni2007",
            valid_from=2008,
            valid_to=None,
        )
        # Stray B: a DIFFERENT root + stem from both the dominant and stray A.
        g.add_classification(
            323,
            "MDC2012",
            shared + [("9304", "mdc only")],
            slug="mdc2012",
            valid_from=2012,
            valid_to=None,
        )
        g.add_value_set(320, shared)
        g.add_variable_state(970, 320)  # open-ended → overlaps both LKF editions

        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1
        # One multi-vintage chain + two off-chain strays (distinct roots) → reclaim.
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        # LATEST dominant-chain edition, NOT either off-chain stray.
        assert g.candidates() == [(970, 320, 321)]

    def test_dominant_chain_period_picks_label_matching_edition(self) -> None:
        """#513 county-reform period nuance: the codes match EVERY edition of an LKF
        chain, but the LABELS changed at the 1997 reform, so per-state period-overlap
        (7c) + the absolute label floor together must land the label-CORRECT edition.

        Swedish county (LKF) names changed at the 1997 reform: pre-1997 editions carry
        OLD names (Malmohus, Goteborgs och Bohus, Kopparberg); post-1997 editions carry
        NEW names (Skane, Vastra Gotaland, Dalarna). The CODES (01..25) are stable
        across all editions, so a county value set is code-contained by EVERY LKF
        edition - but its labels match only the period-appropriate ones. Modeled here as
        a 2-edition LKF chain with the SAME codes but DIFFERENT labels per era:
        LKF1996 [1996,1997] with OLD labels, LKF1998 [1998,unbounded] with NEW labels.
        The value set carries the NEW (post-reform) labels -> label_agree 1.0 vs
        LKF1998, 0.0 vs LKF1996; both code-contain it (multi-vintage chain,
        n_matched=2). One off-chain single-edition SNI stray (different root, DIFFERENT
        labels) puts this on the #514 stray-present path where the conditional absolute
        label floor applies.

        The point: 7c picks the edition by per-state PERIOD-OVERLAP, not naive
        latest-on-chain. The state window is POST-1997 ([2000,9999]), which LKF1996
        [1996,1997] does NOT overlap -> LKF1996 is ineligible; only LKF1998 overlaps, so
        the period-overlap pick coincides with the label-matching edition. The floor
        passes because `fam_max_la` = MAX over the LKF family = 1.0 (from LKF1998's NEW
        labels) >= 0.90, even though LKF1996's label_agree is 0."""
        from reg_meta_build.classifications import link_value_set_classifications
        from reg_meta_build.db import _backfill_state_classifications

        g = _Graph()
        # SAME stable county codes on both editions; DIFFERENT labels per era.
        old_county = _numeric_codes("OLD", 10, 4)  # pre-reform names
        new_county = _numeric_codes("NEW", 10, 4)  # post-reform names (same codes)
        g.add_classification(
            220,
            "LKF1996",
            old_county + [("9001", "lkf1996 only")],
            slug="lkf1996",
            valid_from=1996,
            valid_to=1997,
        )
        g.add_classification(
            221,
            "LKF1998",
            new_county + [("9002", "lkf1998 only")],
            slug="lkf1998",
            supersedes_id=220,
            valid_from=1998,
            valid_to=None,
        )
        # Off-chain single-edition stray: SNI2007 (own root, distinct stem) ALSO
        # code-contains the 10 county codes but carries DIFFERENT (SNI) labels
        # (label_agree 0). Its DB predecessor SNI2002 has DISJOINT codes -> NOT a
        # candidate, so only ONE SNI edition is a candidate -> a permitted single-edition
        # stray. This activates the #514 conditional absolute floor.
        sni_match = [(str(i).zfill(4), f"SNI {i}") for i in range(1, 11)]  # same kods
        sni_disjoint = [(str(i).zfill(4), f"SNI {i}") for i in range(5000, 5010)]
        g.add_classification(
            230,
            "SNI2002",
            sni_disjoint,
            slug="sni2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            231,
            "SNI2007",
            sni_match + [("9003", "sni only")],
            slug="sni2007",
            supersedes_id=230,
            valid_from=2008,
            valid_to=None,
        )
        # The value set carries the NEW (post-reform) labels -> label_agree 1.0 vs
        # LKF1998, 0.0 vs LKF1996; code-contained by BOTH LKF editions (n_matched=2).
        g.add_value_set(240, new_county)
        # POST-1997 state window: overlaps LKF1998 [1998,unbounded] but NOT
        # LKF1996 [1996,1997].
        g.add_variable_state(940, 240, valid_from="2000-01-01", valid_to="9999-12-31")

        counts = link_value_set_classifications(g.conn)
        # 3 candidates (2 LKF + 1 SNI) -> multi-family, not confident.
        assert counts["value_sets_linked"] == 0
        assert counts["multi_family"] == 1
        # Reclaims: fam_max_la = MAX over the LKF family = 1.0 (LKF1998's NEW labels)
        # >= 0.90 floor, even though LKF1996's label_agree is 0.
        assert counts["vintage_value_sets_linked"] == 1
        assert counts["vintage_variables_linked"] == 1
        assert counts["multi_family_after"] == 0
        # Per-state period-overlap picks LKF1998 (post-reform, label-matching,
        # period-overlapping) - LKF1996 [1996,1997] does NOT overlap the post-2000
        # state, so it is ineligible - and NOT the off-chain SNI stray.
        emitted = g.candidates()
        assert emitted == [(940, 240, 221)]
        assert 221 in {c for _, _, c in emitted}  # LKF1998 chosen
        assert 220 not in {c for _, _, c in emitted}  # LKF1996 ineligible (no overlap)

        _backfill_state_classifications(g.conn)
        assert g.tagged_classification(940) == 221

    def test_period_eligible_edition_must_clear_floor(self) -> None:
        """#514 Codex P2: the per-edition label gate (7c) blocks a false reclaim when
        7b's value-set-level floor passes via a LATER edition but per-state period-
        overlap picks an EARLIER, label-DISAGREEING edition.

        This is the inverse of `test_dominant_chain_period_picks_label_matching_edition`:
        same county-reform shape (post-reform labels: label_agree 1.0 vs LKF1998, 0.0 vs
        LKF1996; code-contained by BOTH editions -> multi-vintage chain n_matched=2; one
        off-chain single-edition SNI stray puts it on the #514 stray-present path). 7b's
        floor passes because `fam_max_la` = MAX over the LKF family = 1.0 (LKF1998) >=
        0.90. But here the state window is PRE-1998 ([1990,1996]), which overlaps LKF1996
        [1996,1997] but NOT LKF1998 [1998,unbounded] -> 7c's period pick is LKF1996,
        whose OWN label_agree is 0. 7b's family-max floor decouples from that chosen
        edition, so the OLD code would emit LKF1996 (a false reclaim). The new per-edition
        gate re-applies the floor on the PERIOD-CHOSEN edition's own label_agree: LKF1996
        (0.0) fails `>= _CONFIDENT_LABEL_AGREE`, so it is ineligible, no other edition
        overlaps the pre-1998 state, and the pair emits NOTHING -> stays residual."""
        from reg_meta_build.classifications import link_value_set_classifications

        g = _Graph()
        # SAME stable county codes on both editions; DIFFERENT labels per era.
        old_county = _numeric_codes("OLD", 10, 4)  # pre-reform names
        new_county = _numeric_codes("NEW", 10, 4)  # post-reform names (same codes)
        g.add_classification(
            220,
            "LKF1996",
            old_county + [("9001", "lkf1996 only")],
            slug="lkf1996",
            valid_from=1996,
            valid_to=1997,
        )
        g.add_classification(
            221,
            "LKF1998",
            new_county + [("9002", "lkf1998 only")],
            slug="lkf1998",
            supersedes_id=220,
            valid_from=1998,
            valid_to=None,
        )
        # Off-chain single-edition stray (SNI2007, own root/stem; DB predecessor
        # SNI2002 has DISJOINT codes -> single candidate edition). Activates the #514
        # conditional floor path.
        sni_match = [(str(i).zfill(4), f"SNI {i}") for i in range(1, 11)]  # same kods
        sni_disjoint = [(str(i).zfill(4), f"SNI {i}") for i in range(5000, 5010)]
        g.add_classification(
            230,
            "SNI2002",
            sni_disjoint,
            slug="sni2002",
            valid_from=2002,
            valid_to=2007,
        )
        g.add_classification(
            231,
            "SNI2007",
            sni_match + [("9003", "sni only")],
            slug="sni2007",
            supersedes_id=230,
            valid_from=2008,
            valid_to=None,
        )
        # The value set carries the NEW (post-reform) labels -> label_agree 1.0 vs
        # LKF1998, 0.0 vs LKF1996; code-contained by BOTH LKF editions (n_matched=2).
        g.add_value_set(240, new_county)
        # PRE-1998 state window: overlaps LKF1996 [1996,1997] but NOT LKF1998
        # [1998,unbounded], so per-state period-overlap (7c) picks LKF1996 (label_agree
        # 0) - the label-DISAGREEING edition.
        g.add_variable_state(940, 240, valid_from="1990-01-01", valid_to="1996-12-31")

        counts = link_value_set_classifications(g.conn)
        assert counts["value_sets_linked"] == 0
        assert counts["multi_family"] == 1
        # No reclaim: the period-eligible edition (LKF1996) fails the per-edition floor
        # (own label_agree 0 < 0.90), so the false reclaim is BLOCKED; no other LKF
        # edition overlaps the pre-1998 state -> the pair emits nothing, stays residual.
        assert counts["vintage_value_sets_linked"] == 0
        assert counts["multi_family_after"] == 1
        assert g.candidates() == []


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

    def test_repo_toml_loads_clean(self) -> None:
        """The shipped maintainer artifact parses and carries the #494 part-2
        curated residue (13 entries). Asserting the exact set so future TOML
        drift is caught here.

        Reconstructs the repo path directly (not via
        `repo_classification_links_path()`): the session-scoped
        `_no_repo_curation` autouse fixture patches that getter to `None` so
        synthetic builds see no curation, which would otherwise mask the shipped
        file from this assertion."""
        from pathlib import Path

        from reg_meta_build.classification_links import load_classification_links

        path = Path(__file__).resolve().parent.parent / "classification_links.toml"
        assert path.is_file(), "classification_links.toml must ship in the repo"
        expected = {
            ("scb", "ureg", "isced2011niva"): "ISCED2011",
            ("scb", "ureg", "isced-f-2013"): "ISCED-F2013",
            ("scb", "arbetskraftsbarometern", "sektorkod"): "SEKTOR2000",
            ("scb", "fortroendevalda", "sektor"): "SEKTOR2000",
            ("scb", "kommunalekonomisk-utjamning", "sektor"): "SEKTOR2000",
            ("scb", "lisa", "ast-sektorkod"): "SEKTOR2000",
            ("scb", "lisa", "org-sektorkod"): "SEKTOR2000",
            ("scb", "lisa", "sektorkod"): "SEKTOR2000",
            ("scb", "rams", "institutionell-sektorkod"): "SEKTOR2000",
            ("scb", "yrkesreg", "sektor-ku1"): "SEKTOR2000",
            ("scb", "yrkesreg", "sektorkod"): "SEKTOR2000",
            ("scb", "yrkesreg", "sektorkod-2"): "SEKTOR2000",
            ("scb", "yrkesreg", "sektorkod-storsta-forvarvskalla"): "SEKTOR2000",
        }
        links = load_classification_links(path)
        assert {
            (e.provider, e.register, e.variable): e.classification for e in links
        } == expected


# ---------------------------------------------------------------------------
# #513: classification-linkage residue diagnostic (read-only worklist)
# ---------------------------------------------------------------------------


class TestDumpClassificationResidue:
    """The #416 residue diagnostic: a read-only recompute of the multi-family,
    still-unclassified value sets the auto-detector leaves for curation. On a
    `_Graph` a fresh value-set state has `classification_id IS NULL`, so a
    multi-family value set is residual until something tags its state."""

    def test_multi_family_unclassified_is_residual_with_evidence(self) -> None:
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        # One 10-code 4-digit set ≥0.90-contained in BOTH standalone families
        # (each adds a distinct extra). Mirrors test_multi_family_ambiguous_not_linked.
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        g.add_classification(14, "FAM_B", shared + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(903, 103, slug="famvar")

        result = dump_classification_residue(g.conn)
        assert result.total == 1
        rvs = result.value_sets[0]
        assert rvs.value_set_id == 103
        assert rvs.n_codes == 10
        names = {c.short_name for c in rvs.candidates}
        assert names == {"FAM_A", "FAM_B"}
        # Identical (code,label) on the shared 10 → label_agree 1.0; containment 1.0.
        for c in rvs.candidates:
            assert c.containment == pytest.approx(1.0)
            assert c.label_agree == pytest.approx(1.0)
            assert c.standalone is True
        # The single unclassified state carries the variable FQID + name.
        assert [s.fqid for s in rvs.states] == ["scb/ulf/famvar"]
        assert [s.variable_id for s in rvs.states] == [903]

    def test_two_standalone_both_above_floor_is_ambiguous_not_safe(self) -> None:
        """Two STANDALONE candidates BOTH at label_agree 1.0: the safe gate needs
        EXACTLY ONE standalone above the floor with all others below, so two
        qualifying standalones is AMBIGUOUS (a human must pick the family)."""
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        g.add_classification(14, "FAM_B", shared + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(903, 103, slug="famvar")

        result = dump_classification_residue(g.conn)
        assert result.safe_count == 0
        assert result.value_sets[0].safe is False

    def test_single_label_unambiguous_standalone_is_safe(self) -> None:
        """The curatable tier: EXACTLY ONE candidate is a standalone with
        label_agree ≥ 0.90 and the OTHER is below it. FAM_A keeps matching labels
        (label_agree 1.0); FAM_B is RELABELED on the shared codes (label_agree 0)
        — both ≥0.90-CONTAINED (codes match) so still multi-family, but only FAM_A
        is label-unambiguous → safe, and FAM_A is the [[link]] target."""
        from reg_meta_build.classifications import (
            dump_classification_residue,
            render_residue_toml,
        )

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        # FAM_B contains the same CODES (containment ≥0.90 → still a candidate) but
        # under DIFFERENT labels → label_agree 0 on the value set's labels.
        relabeled = [(code, f"relabel {code}") for code, _ in shared]
        g.add_classification(14, "FAM_B", relabeled + [("9002", "B only")])
        g.add_value_set(103, shared)  # carries FAM_A's labels
        g.add_variable_state(903, 103, slug="famvar")

        result = dump_classification_residue(g.conn)
        assert result.total == 1
        assert result.safe_count == 1
        rvs = result.value_sets[0]
        assert rvs.safe is True
        by_name = {c.short_name: c for c in rvs.candidates}
        assert by_name["FAM_A"].label_agree == pytest.approx(1.0)
        assert by_name["FAM_B"].label_agree == pytest.approx(0.0)

        # The worklist emits the safe candidate as a copyable [[link]] block.
        toml = render_residue_toml(result)
        assert "=== SAFE subset" in toml
        assert "[[link]]" in toml
        assert 'variable = "scb/ulf/famvar"' in toml
        assert 'classification = "FAM_A"' in toml

    def test_standalone_on_chain_is_not_standalone(self) -> None:
        """A candidate on a `supersedes_id` vintage chain is NOT standalone (neither
        the predecessor nor the successor). A value set ambiguous across two chain
        vintages whose state escaped vintage reclaim stays residual but never safe."""
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        shared = _numeric_codes("SNI", 10, 4)
        # Two chain vintages (60←61). The value-set state's period overlaps NEITHER,
        # so vintage reclaim can't help — but here we never run the detector; the
        # state is simply unclassified, and both candidates are chain members.
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
        g.add_variable_state(960, 160, slug="snivar")

        result = dump_classification_residue(g.conn)
        rvs = result.value_sets[0]
        assert {c.short_name: c.standalone for c in rvs.candidates} == {
            "SNI2002": False,
            "SNI2007": False,
        }
        assert rvs.safe is False
        assert result.safe_count == 0

    def test_classified_state_excludes_value_set_from_residue(self) -> None:
        """A multi-family value set whose ONLY state is already classified
        (`classification_id` set — e.g. a curated/feed link) is NOT residual: the
        residue signal is the SHIPPED `classification_id IS NULL`, not a build
        scratch table."""
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        g.add_classification(14, "FAM_B", shared + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(903, 103, slug="famvar")
        # Tag the only state → no longer unclassified.
        g.conn.execute(
            "UPDATE variable_state SET classification_id = 13 WHERE variable_id = 903"
        )

        result = dump_classification_residue(g.conn)
        assert result.total == 0
        assert result.value_sets == ()

    def test_single_family_value_set_never_residual(self) -> None:
        """A value set with ONE candidate classification is single-family, never
        multi-family residue — even unclassified it is absent from the worklist
        (the detector's confident/below-threshold tiers own it, not curation)."""
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        codes = _numeric_codes("ICD", 20, 4)
        g.add_classification(10, "ICD-10-SE", codes)
        g.add_value_set(100, codes)
        g.add_variable_state(900, 100, slug="icdvar")

        result = dump_classification_residue(g.conn)
        assert result.total == 0

    def test_partial_residue_one_classified_one_null(self) -> None:
        """A multi-family value set shared by TWO variables — one state classified,
        one NULL — is STILL residual (it has ≥1 unclassified state), and ONLY the
        unclassified state appears in the worklist."""
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        g.add_classification(14, "FAM_B", shared + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(982, 103, slug="varA")
        g.add_variable_state(983, 103, slug="varB")
        # Classify only varA's state.
        g.conn.execute(
            "UPDATE variable_state SET classification_id = 13 WHERE variable_id = 982"
        )

        result = dump_classification_residue(g.conn)
        assert result.total == 1
        rvs = result.value_sets[0]
        # Only the unclassified varB state is listed.
        assert [s.variable_id for s in rvs.states] == [983]

    def test_read_only_does_not_mutate_or_leave_temp_tables(self) -> None:
        """The diagnostic NEVER mutates: candidate rows untouched and the shared
        `_vs_cls` temp tables are dropped (no leak that would collide with a later
        detector run on the same connection)."""
        from reg_meta_build.classifications import (
            dump_classification_residue,
            link_value_set_classifications,
        )

        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        g.add_classification(14, "FAM_B", shared + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(903, 103, slug="famvar")

        before = g.candidates()
        dump_classification_residue(g.conn)
        assert g.candidates() == before  # no rows written
        # Temp tables are gone — a subsequent detector run rebuilds them cleanly.
        leaked = g.conn.execute(
            "SELECT name FROM sqlite_temp_master "
            "WHERE name LIKE '_vs%' OR name = '_canon_codes' OR name = '_residue_vs'"
        ).fetchall()
        assert leaked == []
        # And the detector still runs (multi-family → no link, as expected).
        counts = link_value_set_classifications(g.conn)
        assert counts["multi_family"] == 1

    def test_variable_on_two_safe_value_sets_emits_one_link(
        self, tmp_path: Path
    ) -> None:
        """A variable that is an unclassified state on TWO safe value sets — both
        resolving to the SAME standalone classification — must emit ONE `[[link]]`,
        not one per state: `load_classification_links` rejects a duplicate `variable`.
        Asserts the worklist round-trips through the loader WITHOUT a duplicate-
        `variable` error. (Fails against a per-state renderer: it emits two identical
        `variable = "scb/ulf/dualvar"` blocks.)"""
        from reg_meta_build.classification_links import load_classification_links
        from reg_meta_build.classifications import (
            dump_classification_residue,
            render_residue_toml,
        )

        g = _Graph()
        # FAM_A is the single label-unambiguous standalone for BOTH value sets: it
        # contains both code groups under matching labels (label_agree 1.0 each).
        s1 = _numeric_codes("S1", 10, 4)  # 0001..0010
        s2 = [(str(i).zfill(4), f"S2 {i}") for i in range(11, 21)]  # 0011..0020
        g.add_classification(20, "FAM_A", s1 + s2 + [("9000", "A only")])
        # Two relabeled B families, one per value set → each value set is multi-family
        # but FAM_A is the only label-unambiguous candidate (FAM_B* label_agree 0).
        g.add_classification(21, "FAM_B1", [(c, f"relabel {c}") for c, _ in s1])
        g.add_classification(22, "FAM_B2", [(c, f"relabel {c}") for c, _ in s2])
        g.add_value_set(201, s1)
        g.add_value_set(202, s2)
        # ONE variable, TWO states (distinct value sets, distinct valid_from to clear
        # the variable_state UNIQUE constraint).
        g.add_variable_state(950, 201, slug="dualvar", valid_from="2010-01-01")
        g.add_variable_state(950, 202, slug="dualvar", valid_from="2015-01-01")

        result = dump_classification_residue(g.conn)
        assert result.safe_count == 2  # both value sets safe

        toml = render_residue_toml(result)
        # Exactly one [[link]] for the variable — not one per state.
        assert toml.count('variable = "scb/ulf/dualvar"') == 1
        # Round-trips through the curated loader without a duplicate-variable raise.
        path = tmp_path / "residue.toml"
        path.write_text(toml, encoding="utf-8")
        links = load_classification_links(path)
        assert [
            (e.provider, e.register, e.variable, e.classification) for e in links
        ] == [("scb", "ulf", "dualvar", "FAM_A")]

    def test_variable_safe_on_two_value_sets_conflicting_class_is_flagged(
        self, tmp_path: Path
    ) -> None:
        """When a variable's two safe value sets resolve to DIFFERENT standalone
        classifications, that is a genuine conflict: it is NOT emitted as a copyable
        `[[link]]` (which would mislead a verbatim copy) but routed to the ambiguous
        section as a comment-flagged conflict. The worklist still round-trips."""
        from reg_meta_build.classification_links import load_classification_links
        from reg_meta_build.classifications import (
            dump_classification_residue,
            render_residue_toml,
        )

        g = _Graph()
        s1 = _numeric_codes("S1", 10, 4)
        s2 = [(str(i).zfill(4), f"S2 {i}") for i in range(11, 21)]
        # value_set 201 → safe FAM_A; value_set 202 → safe FAM_C (different family).
        g.add_classification(20, "FAM_A", s1 + [("9000", "A only")])
        g.add_classification(21, "FAM_B1", [(c, f"relabel {c}") for c, _ in s1])
        g.add_classification(23, "FAM_C", s2 + [("9003", "C only")])
        g.add_classification(24, "FAM_B2", [(c, f"relabel {c}") for c, _ in s2])
        g.add_value_set(201, s1)
        g.add_value_set(202, s2)
        g.add_variable_state(951, 201, slug="conflvar", valid_from="2010-01-01")
        g.add_variable_state(951, 202, slug="conflvar", valid_from="2015-01-01")

        result = dump_classification_residue(g.conn)
        assert result.safe_count == 2

        toml = render_residue_toml(result)
        # No copyable [[link]] for the conflicting variable, and it is flagged.
        assert 'variable = "scb/ulf/conflvar"' not in toml
        assert "CONFLICT" in toml
        assert "conflvar" in toml
        # Whatever links DID emit still load (here: none).
        path = tmp_path / "residue.toml"
        path.write_text(toml, encoding="utf-8")
        assert load_classification_links(path) == ()

    def test_mixed_state_variable_is_not_copyable_safe_only_variable_is(
        self, tmp_path: Path
    ) -> None:
        """P2: a curated `[[link]]` is VARIABLE-grain —
        `materialize_classification_links` applies the chosen classification to EVERY
        value-set state of the variable. So a variable with a safe value set but ALSO
        another (ambiguous) state must NOT be emitted as a copyable link (a
        variable-wide link would mis-tag the ambiguous state); it is comment-flagged
        MIXED-STATE. A variable whose ONLY state is the safe one IS copyable. The
        emitted worklist round-trips through the loader, applying ONLY the safe-only
        variable's link.

        (Fails against the pre-P2 renderer: it emits a copyable `[[link]]` for the
        mixed-state variable too, over-applying the classification variable-wide.)"""
        from reg_meta_build.classification_links import load_classification_links
        from reg_meta_build.classifications import (
            dump_classification_residue,
            render_residue_toml,
        )

        g = _Graph()
        s1 = _numeric_codes("S1", 10, 4)  # 0001..0010
        s2 = [(str(i).zfill(4), f"S2 {i}") for i in range(11, 21)]  # 0011..0020
        # value_set 201 (s1) is SAFE: FAM_A label-matches (standalone), FAM_B1 is
        # relabeled (label_agree 0) → exactly one label-unambiguous standalone.
        g.add_classification(20, "FAM_A", s1 + [("9000", "A only")])
        g.add_classification(21, "FAM_B1", [(c, f"relabel {c}") for c, _ in s1])
        g.add_value_set(201, s1)
        # value_set 202 (s2) is AMBIGUOUS: TWO standalones both label-match s2 →
        # not safe (the safe gate needs exactly one standalone above the floor).
        g.add_classification(22, "FAM_C", s2 + [("9002", "C only")])
        g.add_classification(23, "FAM_D", s2 + [("9003", "D only")])
        g.add_value_set(202, s2)

        # mixedvar: state on the SAFE 201 AND the AMBIGUOUS 202 → mixed-state.
        g.add_variable_state(960, 201, slug="mixedvar", valid_from="2010-01-01")
        g.add_variable_state(960, 202, slug="mixedvar", valid_from="2015-01-01")
        # safevar: ONLY a state on the SAFE 201 → cleanly copyable.
        g.add_variable_state(961, 201, slug="safevar", valid_from="2010-01-01")

        result = dump_classification_residue(g.conn)
        # 201 is safe, 202 is ambiguous.
        assert result.safe_count == 1
        # mixedvar (960) flagged; safevar (961) is NOT.
        assert 960 in result.mixed_state_variable_ids
        assert 961 not in result.mixed_state_variable_ids

        toml = render_residue_toml(result)
        # The mixed-state variable is comment-only and flagged; the safe-only one is
        # a copyable [[link]].
        assert 'variable = "scb/ulf/mixedvar"' not in toml
        assert "MIXED-STATE" in toml
        assert "mixedvar" in toml
        assert 'variable = "scb/ulf/safevar"' in toml
        assert 'classification = "FAM_A"' in toml

        # The worklist round-trips: ONLY the safe-only variable's link loads (the
        # mixed-state variable was never emitted as a copyable block).
        path = tmp_path / "residue.toml"
        path.write_text(toml, encoding="utf-8")
        links = load_classification_links(path)
        assert [
            (e.provider, e.register, e.variable, e.classification) for e in links
        ] == [("scb", "ulf", "safevar", "FAM_A")]

    def test_mixed_state_other_state_classified_to_different_class(self) -> None:
        """P2 variant: a variable with a safe value set whose OTHER state is already
        classified to a DIFFERENT classification is mixed-state — a variable-wide
        link would re-point that state. (An other-state classified to the SAME target
        is NOT mixed.)"""
        from reg_meta_build.classifications import dump_classification_residue

        g = _Graph()
        s1 = _numeric_codes("S1", 10, 4)
        s2 = [(str(i).zfill(4), f"S2 {i}") for i in range(11, 21)]
        g.add_classification(20, "FAM_A", s1 + [("9000", "A only")])
        g.add_classification(21, "FAM_B1", [(c, f"relabel {c}") for c, _ in s1])
        # An unrelated single-family value set, pre-classified to FAM_E.
        g.add_classification(30, "FAM_E", s2)
        g.add_value_set(201, s1)  # safe (FAM_A)
        g.add_value_set(202, s2)
        g.add_variable_state(970, 201, slug="diffvar", valid_from="2010-01-01")
        g.add_variable_state(970, 202, slug="diffvar", valid_from="2015-01-01")
        # Classify diffvar's 202 state to FAM_E (different from the safe target FAM_A).
        g.conn.execute(
            "UPDATE variable_state SET classification_id = 30 "
            "WHERE variable_id = 970 AND value_set_id = 202"
        )

        result = dump_classification_residue(g.conn)
        assert result.safe_count == 1
        assert 970 in result.mixed_state_variable_ids

    def test_unslugged_safe_variable_is_not_copyable(self, tmp_path: Path) -> None:
        """P3: a NULL slug segment (a `--skip-slugs` / partial build) makes the FQID
        carry an empty segment (e.g. `scb/ulf/`), which `load_classification_links`
        rejects. The SAFE renderer must NOT emit such a variable as a copyable
        `[[link]]` — it is comment-flagged UNSLUGGED — so the advertised copyable
        worklist always loads.

        (Fails against the pre-P3 renderer: it emits `variable = "scb/ulf/"`, which
        the loader then refuses.)"""
        from reg_meta_build.classification_links import load_classification_links
        from reg_meta_build.classifications import (
            dump_classification_residue,
            render_residue_toml,
        )

        g = _Graph()
        s1 = _numeric_codes("S1", 10, 4)
        g.add_classification(20, "FAM_A", s1 + [("9000", "A only")])
        g.add_classification(21, "FAM_B1", [(c, f"relabel {c}") for c, _ in s1])
        g.add_value_set(201, s1)  # safe (FAM_A)
        g.add_variable_state(980, 201, slug="unsluggedvar")
        # Simulate a partial / --skip-slugs build: NULL the variable's slug segment.
        g.conn.execute("UPDATE variable SET slug = NULL WHERE variable_id = 980")

        result = dump_classification_residue(g.conn)
        assert result.safe_count == 1
        # The FQID renders with an empty variable segment.
        rvs = result.value_sets[0]
        assert rvs.states[0].fqid == "scb/ulf/"

        toml = render_residue_toml(result)
        # No copyable [[link]] block emitted (the header comment mentions "[[link]]";
        # an EMITTED block is a standalone `[[link]]` line); it is flagged UNSLUGGED.
        assert "[[link]]" not in toml.splitlines()
        assert "UNSLUGGED" in toml
        # The advertised worklist still loads (nothing copyable to over-apply).
        path = tmp_path / "residue.toml"
        path.write_text(toml, encoding="utf-8")
        assert load_classification_links(path) == ()


class TestClassificationResidueCli:
    """The `classification-residue` CLI subcommand: a built DB in, a JSON counts
    summary out, and a `[[link]]`-shaped worklist `classification_links.py`'s loader
    accepts (so a confirmed safe candidate copies in verbatim)."""

    def _residue_db(self, tmp_path: Path) -> Path:
        """A schema-valid file DB carrying a SAFE residue: a multi-family value set
        where exactly one standalone candidate (FAM_A) is label-unambiguous (the
        other, FAM_B, shares the codes but RELABELED → label_agree 0). Built off the
        same DDL `_Graph` uses, plus the `import_manifest` schema_version `open_db`
        checks."""
        from reg_meta.db import SCHEMA_VERSION

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        g = _Graph()
        shared = _numeric_codes("X", 10, 4)
        g.add_classification(13, "FAM_A", shared + [("9001", "A only")])
        relabeled = [(code, f"relabel {code}") for code, _ in shared]
        g.add_classification(14, "FAM_B", relabeled + [("9002", "B only")])
        g.add_value_set(103, shared)
        g.add_variable_state(903, 103, slug="famvar")
        # `open_db` checks import_manifest's schema_version (same major.minor).
        g.conn.execute(
            "INSERT INTO import_manifest (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        # Persist the in-memory graph to the file DB the CLI opens. COMMIT first:
        # the sqlite online-backup API stalls indefinitely while the source
        # connection holds an open write transaction (the un-committed inserts).
        g.conn.commit()
        dest = sqlite3.connect(db_dir / "reg_meta.db")
        g.conn.backup(dest)
        dest.close()
        g.conn.close()
        return db_dir

    def test_cli_emits_summary_and_loadable_worklist(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from reg_meta_build.classification_links import load_classification_links
        from reg_meta_build.cli import run

        db_dir = self._residue_db(tmp_path)
        out_toml = tmp_path / "residue.toml"
        exit_code = run(
            ["--db", str(db_dir), "classification-residue", "-o", str(out_toml)]
        )
        assert exit_code == 0

        summary = json.loads(capsys.readouterr().out)
        assert summary["total"] == 1
        assert summary["safe_count"] == 1
        assert summary["ambiguous_count"] == 0
        assert summary["output_toml"] == str(out_toml.resolve())

        # The emitted worklist's [[link]] block re-parses through the curated loader
        # — a confirmed safe candidate copies into classification_links.toml verbatim.
        links = load_classification_links(out_toml)
        assert [
            (e.provider, e.register, e.variable, e.classification) for e in links
        ] == [("scb", "ulf", "famvar", "FAM_A")]

    def test_cli_carries_toml_in_payload_without_output_flag(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Without -o the JSON summary still prints and carries the worklist TOML in
        the payload (mirrors same-as-candidates / concept-group-candidates)."""
        from reg_meta_build.cli import run

        db_dir = self._residue_db(tmp_path)
        exit_code = run(["--db", str(db_dir), "classification-residue"])
        assert exit_code == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["total"] == 1
        assert "toml" in summary
        assert "[[link]]" in summary["toml"]
        assert "output_toml" not in summary


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
        n_seeded = populate_classifications(
            conn, seed, valid_codes_dir=cls_dir, built_providers=frozenset({"sos"})
        )
        assert n_seeded == 1
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

    # TEST_SEED_TOML declares a valid_codes_file per entry (always-seed
    # requirement); write the CSVs where build_db resolves valid_codes_dir.
    cls_dir = input_dir / "classifications"
    cls_dir.mkdir(parents=True, exist_ok=True)
    for name, body in TEST_SEED_CSVS.items():
        (cls_dir / name).write_text(body, encoding="utf-8")

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

    def test_only_valid_returns_canonical_codes(self, classification_db: Path):
        # TESTKON's CSV declares codes 1 and 2 as canonical (is_valid=1), so
        # --only-valid returns both.
        data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--codes", "--only-valid"],
        )
        assert code == 0
        assert [c["code"] for c in data["codes"]] == ["1", "2"]

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


class TestDeriveSupersedesFromEdges:
    """`supersedes_id` is a DERIVED projection of `classification_replaced_by`
    (#579), not a seed field. The function reads slug-anchored edges and writes
    each successor's predecessor id back onto `classification.supersedes_id`."""

    def _conn(self, edges: list[tuple[str, str]], slugs: list[str]):
        """Minimal schema: `classification` (id/slug/supersedes_id) plus the
        slug-anchored `classification_replaced_by` edge table. Insert one
        classification per slug, then the edges, leaving supersedes_id unset."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE classification (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                short_name TEXT NOT NULL UNIQUE,
                slug TEXT UNIQUE,
                name TEXT NOT NULL,
                supersedes_id INTEGER REFERENCES classification(id)
            );
            CREATE TABLE classification_replaced_by (
                predecessor_slug TEXT NOT NULL,
                successor_slug   TEXT NOT NULL,
                effective_year   INTEGER,
                note             TEXT,
                PRIMARY KEY (predecessor_slug, successor_slug)
            ) WITHOUT ROWID;
            """
        )
        for slug in slugs:
            conn.execute(
                "INSERT INTO classification (short_name, slug, name) VALUES (?, ?, ?)",
                (slug.upper(), slug, slug),
            )
        conn.executemany(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug) VALUES (?, ?)",
            edges,
        )
        return conn

    def _supersedes(self, conn) -> dict[str, str | None]:
        """{slug: predecessor_slug or None} after the derive pass."""
        rows = conn.execute(
            "SELECT c.slug AS slug, p.slug AS pred "
            "FROM classification c "
            "LEFT JOIN classification p ON c.supersedes_id = p.id"
        ).fetchall()
        return {r["slug"]: r["pred"] for r in rows}

    def test_linear_chain(self):
        from reg_meta_build.classifications import derive_supersedes_from_edges

        # ssyk1996 → ssyk2012; root keeps NULL.
        conn = self._conn(
            edges=[("ssyk1996", "ssyk2012")], slugs=["ssyk1996", "ssyk2012"]
        )
        n = derive_supersedes_from_edges(conn)
        assert n == 1
        assert self._supersedes(conn) == {"ssyk1996": None, "ssyk2012": "ssyk1996"}

    def test_one_to_many_split_sun1996(self):
        """The curated #579 sun1996 split: ONE predecessor fans out to THREE
        successors. Each 2000 dimension's supersedes_id points back at sun1996,
        and sun1996 itself stays a root (NULL)."""
        from reg_meta_build.classifications import derive_supersedes_from_edges

        conn = self._conn(
            edges=[
                ("sun1996", "sun-niva2000"),
                ("sun1996", "sun-inriktning2000"),
                ("sun1996", "sun-grupp2000"),
            ],
            slugs=[
                "sun1996",
                "sun-niva2000",
                "sun-inriktning2000",
                "sun-grupp2000",
            ],
        )
        n = derive_supersedes_from_edges(conn)
        assert n == 3
        assert self._supersedes(conn) == {
            "sun1996": None,
            "sun-niva2000": "sun1996",
            "sun-inriktning2000": "sun1996",
            "sun-grupp2000": "sun1996",
        }
        # Read-side back-pointer: superseded_by(sun1996) = all three successors.
        # (`_classification_by_id` needs the full read schema; assert directly on
        # the GROUP_CONCAT-over-supersedes_id the read side uses instead.)
        sun1996_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun1996'"
        ).fetchone()["id"]
        superseded_by = conn.execute(
            "SELECT GROUP_CONCAT(short_name) FROM ("
            "  SELECT short_name FROM classification "
            "  WHERE supersedes_id = ? ORDER BY short_name)",
            (sun1996_id,),
        ).fetchone()[0]
        assert superseded_by == "SUN-GRUPP2000,SUN-INRIKTNING2000,SUN-NIVA2000"

    def test_multiple_predecessors_deterministic(self):
        """A hypothetical merge (>1 predecessor edge into one successor — none
        today) resolves to the deterministic-first predecessor by
        `ORDER BY predecessor_slug`, so the projection is reproducible."""
        from reg_meta_build.classifications import derive_supersedes_from_edges

        conn = self._conn(
            edges=[("bbb", "merged"), ("aaa", "merged")],
            slugs=["aaa", "bbb", "merged"],
        )
        derive_supersedes_from_edges(conn)
        assert self._supersedes(conn)["merged"] == "aaa"

    def test_reset_drops_stale_supersedes(self):
        """The pass is a pure function of the edge table: a pre-existing
        supersedes_id with no backing edge is cleared to NULL."""
        from reg_meta_build.classifications import derive_supersedes_from_edges

        conn = self._conn(edges=[], slugs=["a", "b"])
        a_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'a'"
        ).fetchone()["id"]
        conn.execute(
            "UPDATE classification SET supersedes_id = ? WHERE slug = 'b'", (a_id,)
        )
        n = derive_supersedes_from_edges(conn)
        assert n == 0
        assert self._supersedes(conn) == {"a": None, "b": None}

    def test_dead_predecessor_leaves_successor_null(self):
        """#579 (forward-looking): a curated edge whose `predecessor_slug` has NO
        live `classification` row (a cross-provider / retired predecessor, allowed
        verbatim by the `relations.toml` arm) leaves the live successor's
        supersedes_id NULL — the `SET` subquery's `JOIN p.slug = e.predecessor_slug`
        finds no live row, so the value derived is NULL. (The UPDATE's `WHERE EXISTS`
        still MATCHES the successor row, so it's set to NULL explicitly, not skipped.)
        Mirrors the validator's `missing_ptr` carve-out: a dead-only-predecessor
        successor legitimately keeps NULL."""
        from reg_meta_build.classifications import derive_supersedes_from_edges

        # Edge ('dead-pred' -> 'live-succ') but only 'live-succ' is a live row.
        conn = self._conn(edges=[("dead-pred", "live-succ")], slugs=["live-succ"])
        derive_supersedes_from_edges(conn)
        assert self._supersedes(conn) == {"live-succ": None}
