"""Tests for stats enrichment."""

from __future__ import annotations

from pathlib import Path

import pytest
from regmeta.errors import RegmetaError

from mock_data_wizard.enrich import enrich
from mock_data_wizard.stats import parse_stats


def test_enrich_without_db(stats_path: Path):
    """Enrichment without db_path returns unenriched results."""
    stats = parse_stats(stats_path)
    result = enrich(stats)
    assert len(result) == 1
    assert len(result[0].columns) == 6
    assert result[0].file_name == "persons.csv"
    for col in result[0].columns:
        assert col.register_id is None
        assert col.value_codes is None


def test_enrich_nonexistent_db_raises(stats_path: Path):
    """Enrichment raises when db_path is given but doesn't exist."""
    stats = parse_stats(stats_path)
    with pytest.raises(RegmetaError):
        enrich(stats, db_path=Path("/nonexistent/db"))


def test_enrich_preserves_stats(stats_path: Path):
    stats = parse_stats(stats_path)
    result = enrich(stats)
    cols = {c.column_name: c for c in result[0].columns}
    assert cols["Kon"].inferred_type == "categorical"
    assert cols["Kon"].stats["frequencies"]["1"] == 500
    assert cols["FodelseAr"].inferred_type == "numeric"
    assert cols["FodelseAr"].stats["mean"] == 1975


def test_enrich_multi_file(multi_file_stats_path: Path):
    stats = parse_stats(multi_file_stats_path)
    result = enrich(stats)
    assert len(result) == 2
    names = {f.file_name for f in result}
    assert names == {"file_a.csv", "file_b.csv"}
