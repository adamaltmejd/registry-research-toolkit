"""Tests for mock data generation."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from mock_data_wizard.enrich import RegisterCandidate, enrich
from mock_data_wizard.generate import _remove_stale_files, generate
from mock_data_wizard.stats import parse_stats


def test_generates_csv_files(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, output_dir=out_dir)

    assert len(manifest.files) == 1
    csv_path = out_dir / "persons.csv"
    assert csv_path.exists()

    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1000
    assert set(reader.fieldnames) == {
        "LopNr",
        "Kon",
        "FodelseAr",
        "Kommun",
        "Datum",
        "Namn",
    }


def test_deterministic_output(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)

    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    m1 = generate(stats, enriched, seed=42, output_dir=out1)
    m2 = generate(stats, enriched, seed=42, output_dir=out2)

    assert len(m1.files) == len(m2.files)
    for f1, f2 in zip(m1.files, m2.files):
        assert f1.sha256 == f2.sha256, f"SHA mismatch for {f1.source_name}"


def test_different_seeds_differ(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)

    out1 = tmp_path / "seed1"
    out2 = tmp_path / "seed2"
    m1 = generate(stats, enriched, seed=42, output_dir=out1)
    m2 = generate(stats, enriched, seed=99, output_dir=out2)

    # At least one file should differ
    any_diff = any(f1.sha256 != f2.sha256 for f1, f2 in zip(m1.files, m2.files))
    assert any_diff


def test_sample_pct(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, sample_pct=0.1, output_dir=out_dir)

    assert manifest.files[0].row_count == 100

    with (out_dir / "persons.csv").open() as f:
        reader = csv.reader(f)
        rows = list(reader)
    assert len(rows) == 101  # header + 100 data rows


def test_manifest_json(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    manifest_path = out_dir / "manifest.json"
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())

    # Top-level v3 fields
    assert data["schema_version"] == "3"
    assert "generated_at" in data
    assert data["seed"] == 42

    # Per-source v3 fields
    assert len(data["files"]) == 1
    f = data["files"][0]
    assert f["source_name"] == "persons.csv"
    assert f["source_type"] == "file"
    assert f["source_detail"]["path"].endswith("persons.csv")
    assert f["output_file"] == "persons.csv"
    assert "sha256" in f
    assert set(f["columns"]) == {"LopNr", "Kon", "FodelseAr", "Kommun", "Datum", "Namn"}
    assert f["column_count"] == 6
    assert f["delimiter"] == ","
    assert f["encoding"] == "utf-8"
    assert isinstance(f["header_hash"], str) and len(f["header_hash"]) == 64
    # No enrichment → register_hint is None, file has no year in name
    assert f["register_hint"] is None
    assert f["register_hint_candidates"] == []
    assert f["year_hint"] is None


def test_manifest_register_hint(stats_path: Path, tmp_path: Path):
    """register_hint from enrichment is propagated to manifest."""
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    enriched[0].register_hint = 34
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, output_dir=out_dir)
    assert manifest.files[0].register_hint == 34


def test_manifest_register_hint_none(stats_path: Path, tmp_path: Path):
    """register_hint=None from enrichment is propagated as None."""
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    enriched[0].register_hint = None
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, output_dir=out_dir)
    assert manifest.files[0].register_hint is None


def test_manifest_register_hint_candidates(stats_path: Path, tmp_path: Path):
    """register_hint_candidates from enrichment is serialized into the manifest."""
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    enriched[0].register_hint = None
    enriched[0].register_hint_candidates = [
        RegisterCandidate(register_id=366, match_count=2, total_nonid_cols=6),
        RegisterCandidate(register_id=190, match_count=1, total_nonid_cols=6),
    ]
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)
    data = json.loads((out_dir / "manifest.json").read_text())
    cands = data["files"][0]["register_hint_candidates"]
    assert cands == [
        {"register_id": 366, "match_count": 2, "total_nonid_cols": 6},
        {"register_id": 190, "match_count": 1, "total_nonid_cols": 6},
    ]


def test_manifest_year_hint(tmp_path: Path, stats_path: Path):
    """Year hint is extracted from source name containing a 4-digit year."""
    stats = parse_stats(stats_path)
    # Rename the source to include a year
    stats.sources[0].source_name = "LISA_2022.csv"
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, output_dir=out_dir)
    assert manifest.files[0].year_hint == 2022


def test_categorical_values(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "persons.csv").open() as f:
        reader = csv.DictReader(f)
        kon_values = {row["Kon"] for row in reader}

    # Kon should only contain values from the frequency table
    assert kon_values <= {"1", "2"}


def test_nullable_has_blanks(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "persons.csv").open() as f:
        reader = csv.DictReader(f)
        kommun_values = [row["Kommun"] for row in reader]

    # Kommun has 5% null rate, so some should be blank
    blank_count = sum(1 for v in kommun_values if v == "")
    assert blank_count > 0


def test_non_nullable_no_blanks(stats_path: Path, tmp_path: Path):
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "persons.csv").open() as f:
        reader = csv.DictReader(f)
        kon_values = [row["Kon"] for row in reader]

    assert all(v != "" for v in kon_values)


def test_shared_id_pool(multi_file_stats_path: Path, tmp_path: Path):
    stats = parse_stats(multi_file_stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    # Both files should use IDs from the same pool
    with (out_dir / "file_a.csv").open() as f:
        ids_a = {row["LopNr"] for row in csv.DictReader(f)}
    with (out_dir / "file_b.csv").open() as f:
        ids_b = {row["LopNr"] for row in csv.DictReader(f)}

    # All IDs should be integers from the shared pool (1..500)
    for id_val in ids_a | ids_b:
        assert int(id_val) in range(1, 501)

    # IDs from both files should come from the same pool (some overlap expected)
    assert len(ids_a | ids_b) <= 500


def test_id_uniqueness_when_pool_sufficient(
    multi_file_stats_path: Path, tmp_path: Path
):
    """IDs must not repeat within a file when the pool is >= row count."""
    stats = parse_stats(multi_file_stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    # file_a: 500 rows from pool of 500 — every ID must be unique
    with (out_dir / "file_a.csv").open() as f:
        ids_a = [row["LopNr"] for row in csv.DictReader(f)]
    assert len(ids_a) == len(set(ids_a)), "file_a has duplicate IDs"

    # file_b: 300 rows from pool of 500 — every ID must be unique
    with (out_dir / "file_b.csv").open() as f:
        ids_b = [row["LopNr"] for row in csv.DictReader(f)]
    assert len(ids_b) == len(set(ids_b)), "file_b has duplicate IDs"


def test_id_uniqueness_single_file(stats_path: Path, tmp_path: Path):
    """Single-source register with n_distinct == row_count must have unique IDs."""
    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "persons.csv").open() as f:
        ids = [row["LopNr"] for row in csv.DictReader(f)]
    assert len(ids) == len(set(ids)), "persons.csv has duplicate IDs"


def test_multi_file_output_order(multi_file_stats_path: Path, tmp_path: Path):
    stats = parse_stats(multi_file_stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, output_dir=out_dir)

    # Sources should be in lexical order
    names = [f.source_name for f in manifest.files]
    assert names == sorted(names)


def test_stale_files_kept_by_default(stats_path: Path, tmp_path: Path):
    """Re-running generate without force keeps stale files on disk."""
    out_dir = tmp_path / "output"
    out_dir.mkdir()

    (out_dir / "old_file.csv").write_text("stale")
    (out_dir / "another_old.csv").write_text("stale")

    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    generate(stats, enriched, seed=42, output_dir=out_dir)  # no force=

    remaining = {p.name for p in out_dir.iterdir()}
    # Stale files survive — warn-and-keep is the default.
    assert "old_file.csv" in remaining
    assert "another_old.csv" in remaining
    # New mock and manifest are present.
    assert "persons.csv" in remaining
    assert "manifest.json" in remaining


def test_stale_files_removed_with_force(stats_path: Path, tmp_path: Path):
    """`force=True` deletes files from a previous run that aren't produced this time."""
    out_dir = tmp_path / "output"
    out_dir.mkdir()

    (out_dir / "old_file.csv").write_text("stale")
    (out_dir / "another_old.csv").write_text("stale")

    stats = parse_stats(stats_path)
    enriched = enrich(stats)
    generate(stats, enriched, seed=42, output_dir=out_dir, force=True)

    remaining = {p.name for p in out_dir.iterdir()}
    assert "old_file.csv" not in remaining
    assert "another_old.csv" not in remaining
    assert "persons.csv" in remaining
    assert "manifest.json" in remaining


def test_remove_stale_preserves_manifest(tmp_path: Path):
    (tmp_path / "manifest.json").write_text("{}")
    (tmp_path / "stale.csv").write_text("x")
    removed = _remove_stale_files(tmp_path, written_files=set())
    assert removed == ["stale.csv"]
    assert (tmp_path / "manifest.json").exists()


def test_remove_stale_keeps_current_files(tmp_path: Path):
    (tmp_path / "keep.csv").write_text("x")
    (tmp_path / "drop.csv").write_text("x")
    removed = _remove_stale_files(tmp_path, written_files={"keep.csv"})
    assert removed == ["drop.csv"]
    assert (tmp_path / "keep.csv").exists()


# ---------------------------------------------------------------------------
# Population spine tests
# ---------------------------------------------------------------------------


def _enrich_with_spine(stats, var_id_map: dict[str, int] | None = None):
    """Enrich stats and optionally set var_ids for spine columns."""
    enriched = enrich(stats)
    if var_id_map:
        for ef in enriched:
            for ec in ef.columns:
                if ec.column_name in var_id_map:
                    ec.var_id = var_id_map[ec.column_name]
    return enriched


def test_spine_consistency(spine_stats_path: Path, tmp_path: Path):
    """Shared birth-invariant column has identical values per individual across sources."""
    stats = parse_stats(spine_stats_path)
    enriched = _enrich_with_spine(stats, {"Kon": 44})
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "pop.csv").open() as f:
        pop = {row["LopNr"]: row["Kon"] for row in csv.DictReader(f)}
    with (out_dir / "edu.csv").open() as f:
        edu = {row["LopNr"]: row["Kon"] for row in csv.DictReader(f)}

    common = set(pop) & set(edu)
    assert len(common) > 0
    for id_val in common:
        assert pop[id_val] == edu[id_val], f"Kon mismatch for LopNr={id_val}"


def test_non_spine_column_independent(spine_stats_path: Path, tmp_path: Path):
    """Columns without a spine var_id are generated normally."""
    stats = parse_stats(spine_stats_path)
    enriched = _enrich_with_spine(stats, {"Kon": 44})
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "edu.csv").open() as f:
        grades = {row["Grade"] for row in csv.DictReader(f)}
    assert grades <= {"7", "8", "9"}


def test_spine_deterministic(spine_stats_path: Path, tmp_path: Path):
    """Same seed produces identical spine output."""
    stats = parse_stats(spine_stats_path)
    enriched = _enrich_with_spine(stats, {"Kon": 44})

    m1 = generate(stats, enriched, seed=42, output_dir=tmp_path / "r1")
    m2 = generate(stats, enriched, seed=42, output_dir=tmp_path / "r2")

    for f1, f2 in zip(m1.files, m2.files):
        assert f1.sha256 == f2.sha256, f"SHA mismatch for {f1.source_name}"


def test_no_enrichment_no_spine(spine_stats_path: Path, tmp_path: Path):
    """Without var_ids, Kon is generated independently — mismatches expected."""
    stats = parse_stats(spine_stats_path)
    enriched = enrich(stats)  # no var_ids
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    with (out_dir / "pop.csv").open() as f:
        pop = {row["LopNr"]: row["Kon"] for row in csv.DictReader(f)}
    with (out_dir / "edu.csv").open() as f:
        edu = {row["LopNr"]: row["Kon"] for row in csv.DictReader(f)}

    common = set(pop) & set(edu)
    mismatches = sum(1 for v in common if pop[v] != edu[v])
    assert mismatches > 0, "Expected independent generation to produce some mismatches"


def test_spine_authority_uses_largest_population(
    spine_stats_path: Path, tmp_path: Path
):
    """Spine uses the authority source's distribution (pop.csv has more individuals)."""
    stats = parse_stats(spine_stats_path)
    enriched = _enrich_with_spine(stats, {"Kon": 44})
    out_dir = tmp_path / "output"
    generate(stats, enriched, seed=42, output_dir=out_dir)

    # All 500 IDs in the spine should have Kon values from the authority (pop.csv)
    with (out_dir / "pop.csv").open() as f:
        pop = {row["LopNr"]: row["Kon"] for row in csv.DictReader(f)}

    # Every individual in pop should have a valid Kon value from the spine
    assert all(v in {"1", "2"} for v in pop.values())
    assert len(pop) == 500
