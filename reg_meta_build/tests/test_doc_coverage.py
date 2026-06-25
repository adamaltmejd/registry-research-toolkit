"""Tests for the read-only `doc-coverage` diagnostic (#400 PR1).

The diagnostic diffs doc-documented columns (the ingested SCB doc library, the
`doc` table) against the built catalog's `variable_alias.delivery_column_name`
set, per register, and lists the documented-but-metadata-missing columns.

Pairs the real synthetic catalog (`fixture_db` → registers slug `testreg` with
columns {AaoCol, Kon, TestCol, TestKolumn} and slug `otherreg` with {ExtCol, KON,
LopNr, ParenCol, UniqCol}) with a doc DB built from controlled markdown via
`build_doc_db`, so both the catalog and doc schemas are exercised end to end.

Regression guard for the real-data slug-join bug: the join is
`register.slug == doc.register` (the doc subdir IS the slug), NOT
`lower(register.name) == doc.register`. In real data `register.name` is the full
descriptive SCB string (e.g. "Longitudinell ... (LISA)") while the doc subdir is
the register slug ("lisa"), so a name-join matches nothing. The fixture catalog
ships `name` == "TESTREG"/"OTHERREG" (which would lower-match the slug and hide
the bug), so `doc_db_dir` REWRITES each `register.name` to a full descriptive
string distinct from its slug — a name-join then fails and only the slug-join
surfaces the documented-missing columns.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING

import pytest
from reg_meta.db import open_db
from reg_meta.doc_db import open_doc_db
from reg_meta_build.doc_coverage import (
    compute_doc_coverage,
    render_doc_coverage_toml,
)
from reg_meta_build.doc_db import build_doc_db

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


def _write_doc(reg_dir: Path, stem: str, *, variable: str | None, **front: str) -> None:
    """Write one doc markdown file under `reg_dir` (the register subdir).

    `variable` is the documented column (`doc.variable`); a None means the doc
    documents no single column (an appendix). Extra frontmatter keys
    (`display_name`, `source`) are passed through. A non-empty body is required —
    `build_doc_db` skips body-less docs.

    The filename is prefixed with the register subdir name because `doc.filename`
    is UNIQUE across the WHOLE table (not per-register), so two registers
    documenting the same column (e.g. `Kon`) would otherwise collide on insert."""
    filename = f"{reg_dir.name}__{stem}.md"
    lines = ["---"]
    if variable is not None:
        lines.append(f"variable: {variable}")
    for key, val in front.items():
        lines.append(f'{key}: "{val}"')
    lines.append("---")
    lines.append("")
    lines.append(f"Body for {stem}.")
    (reg_dir / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")


# Full descriptive SCB-style names, DISTINCT from each register's slug and the
# doc subdir, so a `lower(register.name) == doc.register` join would match
# nothing — only the correct `register.slug == doc.register` join works. Keyed by
# slug (the column the rewrite targets via register.slug).
_DESCRIPTIVE_NAMES = {
    "testreg": "Testregistret för longitudinella studier (TESTREG)",
    "otherreg": "Annat register med företagsuppgifter (OTHERREG)",
}


def _rewrite_register_names_distinct_from_slug(db_path: Path) -> None:
    """Rewrite each `register.name` to a full descriptive string distinct from its
    slug, mimicking real SCB data where `register.name` is the long literal (e.g.
    "Longitudinell ... (LISA)") and the doc subdir is the slug ("lisa"). This is
    what makes the slug-join the only join that works — a name-join now fails."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        for slug, name in _DESCRIPTIVE_NAMES.items():
            conn.execute("UPDATE register SET name = ? WHERE slug = ?", (name, slug))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def doc_db_dir(fixture_db: Path, tmp_path: Path) -> Path:
    """A `--db` dir holding BOTH the fixture catalog `reg_meta.db` and a
    doc DB built from controlled markdown for registers `testreg` / `otherreg`
    (subdir names == `register.slug` → the `register.slug == doc.register` join).
    Returns the dir.

    The catalog DB is copied out of the session-scoped fixture dir so this
    test's doc DB does not clobber the shared stub doc DB other tests rely on —
    and so we can rewrite each `register.name` to a full descriptive string
    DISTINCT from the slug (the slug-join regression guard) without mutating the
    shared fixture."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    catalog_db = db_dir / "reg_meta.db"
    shutil.copy(fixture_db, catalog_db)
    _rewrite_register_names_distinct_from_slug(catalog_db)

    docs = tmp_path / "docs"
    testreg = docs / "testreg"
    testreg.mkdir(parents=True)
    # Documented AND present (case-insensitive: doc `kon` vs catalog `Kon`).
    _write_doc(
        testreg,
        "Kon",
        variable="kon",
        display_name="Kön",
        source="testreg-bakgrund.md",
    )
    # Documented but MISSING from the catalog's delivery columns.
    _write_doc(
        testreg,
        "MissingOne",
        variable="MissingOne",
        display_name="Saknad kolumn 1",
        source="testreg-bakgrund.md",
    )
    _write_doc(
        testreg,
        "MissingTwo",
        variable="MissingTwo",
        display_name="Saknad kolumn 2",
    )
    # An appendix doc with no `variable` — must be ignored (documents no column).
    _write_doc(testreg, "_appendix", variable=None, display_name="Bilaga")

    otherreg = docs / "otherreg"
    otherreg.mkdir()
    # Present (case-insensitive: doc `kon` vs catalog `KON`).
    _write_doc(otherreg, "Kon", variable="Kon", display_name="Kön (annat)")
    # Missing.
    _write_doc(otherreg, "GapCol", variable="GapCol", display_name="Lucka")

    # Overwrites any stub doc DB the fixture left in db_dir.
    build_doc_db(docs, db_dir)
    return db_dir


def _catalog_conn(db_dir: Path) -> sqlite3.Connection:
    return open_db(db_dir / "reg_meta.db")


def _doc_conn(db_dir: Path) -> sqlite3.Connection:
    return open_doc_db(db_dir / "reg_meta_docs.db")


class TestComputeDocCoverage:
    def test_documented_but_missing_column_surfaces(self, doc_db_dir: Path) -> None:
        catalog, doc = _catalog_conn(doc_db_dir), _doc_conn(doc_db_dir)
        try:
            result = compute_doc_coverage(catalog, doc)
        finally:
            catalog.close()
            doc.close()

        cols = {(m.register, m.column) for m in result.missing}
        assert ("testreg", "MissingOne") in cols
        assert ("testreg", "MissingTwo") in cols
        assert ("otherreg", "GapCol") in cols
        # Slug-join regression pin: the doc_db_dir fixture rewrote register.name
        # to a long descriptive string ("Testregistret ... (TESTREG)") that does
        # NOT lower-match the doc subdir "testreg". The documented-missing column
        # still surfaces, so the join must be register.slug == doc.register; a
        # name-join would leave `testreg` unmapped and this set empty.
        assert _DESCRIPTIVE_NAMES["testreg"].lower() != "testreg"
        assert result.unmapped_doc_registers == ()
        missing_one = next(m for m in result.missing if m.column == "MissingOne")
        assert missing_one.display_name == "Saknad kolumn 1"
        # build_doc_db canonicalizes the source by stripping a trailing `.md`.
        assert missing_one.source == "testreg-bakgrund"
        assert missing_one.filename == "testreg__MissingOne.md"

    def test_documented_and_present_column_is_excluded(self, doc_db_dir: Path) -> None:
        """A documented column that IS in the catalog's delivery columns is
        excluded — and the match is CASE-INSENSITIVE (doc `kon` vs catalog
        `Kon`/`KON`). An appendix doc with no `variable` is also ignored."""
        catalog, doc = _catalog_conn(doc_db_dir), _doc_conn(doc_db_dir)
        try:
            result = compute_doc_coverage(catalog, doc)
        finally:
            catalog.close()
            doc.close()

        cols = {(m.register, m.column.lower()) for m in result.missing}
        assert ("testreg", "kon") not in cols
        assert ("otherreg", "kon") not in cols
        # The appendix (no variable) never surfaces as a column.
        assert all(m.column for m in result.missing)

    def test_per_register_grouping_and_totals(self, doc_db_dir: Path) -> None:
        catalog, doc = _catalog_conn(doc_db_dir), _doc_conn(doc_db_dir)
        try:
            result = compute_doc_coverage(catalog, doc)
        finally:
            catalog.close()
            doc.close()

        assert result.total == 3
        assert result.per_register_counts == {"testreg": 2, "otherreg": 1}
        # No unmapped registers — both doc subdirs match a register.slug.
        assert result.unmapped_doc_registers == ()
        # Deterministic sort by (register, column, filename).
        keys = [(m.register, m.column) for m in result.missing]
        assert keys == sorted(keys)

    def test_all_covered_yields_empty_result(
        self, fixture_db: Path, tmp_path: Path
    ) -> None:
        """When every documented column is present in the catalog, the result is
        empty and the worklist says so."""
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        shutil.copy(fixture_db, db_dir / "reg_meta.db")

        docs = tmp_path / "docs"
        testreg = docs / "testreg"
        testreg.mkdir(parents=True)
        # Every documented column exists in TESTREG (case-insensitive).
        for col in ("Kon", "AAOCOL", "testcol", "TestKolumn"):
            _write_doc(testreg, col, variable=col, display_name=col)
        build_doc_db(docs, db_dir)

        catalog, doc = _catalog_conn(db_dir), _doc_conn(db_dir)
        try:
            result = compute_doc_coverage(catalog, doc)
        finally:
            catalog.close()
            doc.close()

        assert result.total == 0
        assert result.missing == ()
        assert result.per_register_counts == {}
        toml = render_doc_coverage_toml(result)
        assert "(no missing columns)" in toml

    def test_unmapped_doc_register_is_reported_not_dropped(
        self, fixture_db: Path, tmp_path: Path
    ) -> None:
        """A doc register that maps to NO catalog register is surfaced in
        `unmapped_doc_registers`, not silently dropped."""
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        shutil.copy(fixture_db, db_dir / "reg_meta.db")

        docs = tmp_path / "docs"
        nowhere = docs / "nowhere"
        nowhere.mkdir(parents=True)
        _write_doc(nowhere, "SomeCol", variable="SomeCol", display_name="Nånstans")
        build_doc_db(docs, db_dir)

        catalog, doc = _catalog_conn(db_dir), _doc_conn(db_dir)
        try:
            result = compute_doc_coverage(catalog, doc)
        finally:
            catalog.close()
            doc.close()

        assert result.unmapped_doc_registers == ("nowhere",)
        # Its columns are NOT enumerated as phantom-missing (no register to diff).
        assert result.total == 0
        toml = render_doc_coverage_toml(result)
        assert "nowhere" in toml


class TestRenderDocCoverageToml:
    def test_worklist_is_review_only_and_carries_evidence(
        self, doc_db_dir: Path
    ) -> None:
        catalog, doc = _catalog_conn(doc_db_dir), _doc_conn(doc_db_dir)
        try:
            result = compute_doc_coverage(catalog, doc)
        finally:
            catalog.close()
            doc.close()

        toml = render_doc_coverage_toml(result)
        # Header flags it as a non-loadable review artifact.
        assert "REVIEW artifact" in toml
        assert "#400 PR2" in toml
        # Entries are commented out (nothing uncommented to copy verbatim).
        assert "# [[column]]" in toml
        assert "\n[[column]]" not in toml
        # Evidence and the documented columns appear.
        assert "MissingOne" in toml
        assert "Saknad kolumn 1" in toml
        assert "GapCol" in toml


class TestDocCoverageCli:
    def test_cli_emits_summary_and_worklist(
        self, doc_db_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from reg_meta_build.cli import run

        out_toml = tmp_path / "doc-coverage.toml"
        exit_code = run(["--db", str(doc_db_dir), "doc-coverage", "-o", str(out_toml)])
        assert exit_code == 0

        summary = json.loads(capsys.readouterr().out)
        assert summary["total"] == 3
        assert summary["per_register_counts"] == {"testreg": 2, "otherreg": 1}
        assert summary["unmapped_doc_registers"] == []
        assert summary["output_toml"] == str(out_toml.resolve())
        assert "# [[column]]" in out_toml.read_text(encoding="utf-8")

    def test_cli_carries_toml_in_payload_without_output_flag(
        self, doc_db_dir: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from reg_meta_build.cli import run

        exit_code = run(["--db", str(doc_db_dir), "doc-coverage"])
        assert exit_code == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["total"] == 3
        assert "toml" in summary
        assert "# [[column]]" in summary["toml"]
        assert "output_toml" not in summary
