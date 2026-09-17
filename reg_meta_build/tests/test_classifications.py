"""Tests for classification seed loading, build-time population, and CLI."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import PIPE
from reg_meta.errors import RegMetaError
from reg_meta_build.classifications import load_seed, load_valid_codes

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


# ---------------------------------------------------------------------------
# PR2 / #446: adapter classification candidate feed (_feed_classification_candidates)
# ---------------------------------------------------------------------------


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


class TestCuratedClassificationLinks:
    """Structural validation of accepted classification-link declarations."""

    def test_load_bad_fqid_fails(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        path = tmp_path / "classifications.toml"
        path.write_text(
            '[[link]]\nvariable = "scb/ulf"\nclassification = "ICD-10-SE"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as ei:
            load_classification_links(path)
        assert ei.value.code == "classification_links_invalid"

    def test_load_missing_classification_fails(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        path = tmp_path / "classifications.toml"
        path.write_text('[[link]]\nvariable = "scb/ulf/ha0611m"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as ei:
            load_classification_links(path)
        assert ei.value.code == "classification_links_invalid"

    def test_load_duplicate_variable_fails(self, tmp_path: Path) -> None:
        from reg_meta_build.classification_links import load_classification_links

        path = tmp_path / "classifications.toml"
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
        empty = tmp_path / "classifications.toml"
        empty.write_text("# only comments\n", encoding="utf-8")
        assert load_classification_links(empty) == ()

    def test_repo_toml_loads_clean(self) -> None:
        """The shipped maintainer artifact parses and carries the #494 part-2
        curated residue (13 entries). Asserting the exact set so future TOML
        drift is caught here.

        Loads the shipped file directly, independently of synthetic catalog
        fixtures."""
        from pathlib import Path

        from reg_meta_build.classification_links import load_classification_links

        path = (
            Path(__file__).resolve().parent.parent / "curation" / "classifications.toml"
        )
        assert path.is_file(), "curation/classifications.toml must ship in the repo"
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
        # Repeating the maintained diagnostic rebuilds its temporary tables.
        again = dump_classification_residue(g.conn)
        assert again.total == 1
        assert g.candidates() == before

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
        # — a confirmed safe candidate copies into curation/classifications.toml.
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
# Repo CSV snapshots round-trip through the seed loader
# ---------------------------------------------------------------------------


class TestRepoClassificationCsvSnapshots:
    def test_icd11_and_sni2025_csvs_load_from_repo_seed(self):
        from reg_meta_build._curation import repo_curation_path

        seed_path = repo_curation_path("classifications.toml")
        assert seed_path is not None
        entries = {entry["short_name"]: entry for entry in load_seed(seed_path)}
        cls_dir = seed_path.parent.parent / "input_data" / "classifications"

        assert entries["ICD-11-SE"]["valid_codes_file"] == "sos/icd-11-se.csv"
        assert entries["ICD-11-SE"]["valid_from"] == 2027
        assert "valid_to" not in entries["ICD-10-SE"]
        icd11 = load_valid_codes(cls_dir / "sos" / "icd-11-se.csv")
        assert icd11["1A00"] == "Kolera"
        assert all(icd11.values())
        with (cls_dir / "sos" / "icd-11-se.csv").open(encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert sum(1 for row in rows if row["parent_code"]) > 0

        assert entries["SNI2025"]["valid_codes_file"] == "sni2025.csv"
        assert "valid_from" not in entries["SNI2025"]
        assert "valid_to" not in entries["SNI2007"]
        sni2025 = load_valid_codes(cls_dir / "sni2025.csv")
        assert sni2025["A"] == "Jordbruk, skogsbruk och fiske"
        assert sni2025["01110"] == (
            "Odling av spannmål (utom ris), baljväxter och oljeväxter"
        )

    def test_kva_csv_round_trips_without_duplicate_codes(self, tmp_path: Path):
        """The real merged `sos/kva.csv` (KMÅ ∪ KKÅ, deduped on the 50 shared
        chapter headers) loads into ONE `KVA` classification with codes and
        WITHOUT a duplicate-code `RegMetaError` — proving the merge deduped."""
        from reg_meta_build._curation import repo_curation_path
        from reg_meta_build.resolved_catalog import (
            ResolvedClassification,
            ResolvedClassificationCode,
            write_resolved_catalog,
        )

        seed_path = repo_curation_path("classifications.toml")
        assert seed_path is not None
        cls_dir = seed_path.parent.parent / "input_data" / "classifications"
        assert (cls_dir / "sos" / "kva.csv").is_file(), "merged kva.csv must exist"

        seed = tmp_path / "classifications.toml"
        seed.write_text(
            '[[classification]]\nshort_name = "KVA"\nname = "KVÅ"\n'
            'provider = "sos"\nvalid_codes_file = "sos/kva.csv"\n',
            encoding="utf-8",
        )
        (entry,) = load_seed(seed)
        codes = load_valid_codes(cls_dir / entry["valid_codes_file"])
        book = ResolvedClassification(
            slug="kva",
            short_name=entry["short_name"],
            name=entry["name"],
            codes=tuple(
                ResolvedClassificationCode(code=code, label=label)
                for code, label in codes.items()
            ),
        )
        output = write_resolved_catalog(
            (),
            tmp_path / "catalog.db",
            manifest={"fixture": "canonical-kva"},
            diagnostic=True,
            classifications=(book,),
        )
        conn = sqlite3.connect(output)
        n_seeded = conn.execute("SELECT count(*) FROM classification").fetchone()[0]
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
        check=False,  # tests assert on returncode; nonzero exits are expected
    )
    out = proc.stdout.strip()
    # JSON errors still produce JSON on stdout; just parse.
    return json.loads(out), proc.returncode


@pytest.fixture(scope="module")
def classification_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedClassificationCode,
        ResolvedCodeSet,
        ResolvedRegister,
        ResolvedState,
        ResolvedVariable,
        ResolvedVariant,
        write_resolved_catalog,
    )

    tmp = tmp_path_factory.mktemp("cls")
    books = tuple(
        ResolvedClassification(
            slug=short_name.lower(),
            short_name=short_name,
            name=name,
            codes=tuple(
                ResolvedClassificationCode(code=code, label=label, level=len(code))
                for code, label in members
            ),
        )
        for short_name, name, members in (
            (
                "TESTKON",
                "Test classification for gender codes",
                (("1", "Man"), ("2", "Kvinna")),
            ),
            (
                "TESTKON2",
                "Successor",
                (("10", "Female"), ("20", "Male"), ("30", "Other")),
            ),
        )
    )
    variables = tuple(
        ResolvedVariable(
            register=ResolvedRegister(
                provider="scb", slug=register, name=register.upper()
            ),
            provider_key="44",
            slug="kon",
            name="Kön",
            definition=None,
            description=None,
            operational_definition=None,
            measurement_unit=None,
            is_identifier=False,
            is_sensitive=False,
            states=tuple(
                ResolvedState(
                    variant=ResolvedVariant(slug="individer", name="Individer"),
                    valid_from=f"{year}-01-01",
                    valid_to=f"{year}-12-31",
                    delivery_column_name="Kon",
                    data_type="integer",
                    data_length="2",
                    operational_definition=None,
                    provenance=None,
                    classification=book.slug,
                    value_set=ResolvedCodeSet(
                        members=tuple((c.code, c.label) for c in book.codes)
                    ),
                )
                for year, book in ((2020, books[0]), (2022, books[1]))
                if register == "testreg" or year == 2020
            ),
        )
        for register in ("testreg", "otherreg")
    )
    db_dir = tmp / "db"
    write_resolved_catalog(
        variables, db_dir / "reg_meta.db", manifest={}, classifications=books
    )

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
        _data, code = _run_json(
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
        _data, code = _run_json(
            classification_db,
            ["get", "classification", "TESTKON", "--level", "1"],
        )
        assert code == 2  # EXIT_USAGE

    def test_list_with_positional_fails(self, classification_db: Path):
        _data, code = _run_json(
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
