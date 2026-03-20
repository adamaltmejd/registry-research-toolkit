"""Tests for mock data generation."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from mock_data_wizard.enrich import enrich
from mock_data_wizard.generate import generate
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
        assert f1.sha256 == f2.sha256, f"SHA mismatch for {f1.file_name}"


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
    assert data["seed"] == 42
    assert len(data["files"]) == 1
    assert data["files"][0]["file_name"] == "persons.csv"
    assert "sha256" in data["files"][0]


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


def test_multi_file_output_order(multi_file_stats_path: Path, tmp_path: Path):
    stats = parse_stats(multi_file_stats_path)
    enriched = enrich(stats)
    out_dir = tmp_path / "output"
    manifest = generate(stats, enriched, seed=42, output_dir=out_dir)

    # Files should be in lexical order
    file_names = [f.file_name for f in manifest.files]
    assert file_names == sorted(file_names)
