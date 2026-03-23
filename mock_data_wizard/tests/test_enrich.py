"""Tests for stats enrichment."""

from __future__ import annotations

from pathlib import Path

import pytest
from regmeta.errors import RegmetaError

from mock_data_wizard.enrich import (
    EnrichedColumn,
    EnrichedFile,
    _check_value_code_drift,
    enrich,
)
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


def _make_enriched(
    file_name: str,
    col_name: str,
    frequencies: dict[str, int],
    value_codes: dict[str, str] | None,
) -> EnrichedFile:
    return EnrichedFile(
        file_name=file_name,
        relative_path=file_name,
        row_count=100,
        columns=[
            EnrichedColumn(
                column_name=col_name,
                inferred_type="categorical",
                nullable=False,
                null_rate=0.0,
                n_distinct=len(frequencies),
                stats={"frequencies": frequencies},
                value_codes=value_codes,
            )
        ],
    )


def test_drift_warns_on_unknown_codes():
    ef = _make_enriched(
        "f.csv", "Kon", {"1": 50, "2": 40, "3": 10}, {"1": "Man", "2": "Kvinna"}
    )
    warnings = _check_value_code_drift([ef])
    assert len(warnings) == 1
    assert "3" in warnings[0]
    assert "f.csv/Kon" in warnings[0]


def test_drift_no_warning_when_all_codes_match():
    ef = _make_enriched("f.csv", "Kon", {"1": 50, "2": 50}, {"1": "Man", "2": "Kvinna"})
    assert _check_value_code_drift([ef]) == []


def test_drift_ignores_other_bucket():
    ef = _make_enriched(
        "f.csv", "Kon", {"1": 50, "_other": 10}, {"1": "Man", "2": "Kvinna"}
    )
    assert _check_value_code_drift([ef]) == []


def test_drift_skipped_without_value_codes():
    ef = _make_enriched("f.csv", "Status", {"A": 50, "B": 50}, None)
    assert _check_value_code_drift([ef]) == []


def test_enrich_resolves_from_db(stats_path: Path, regmeta_db: Path):
    """Enrichment against a real regmeta DB resolves columns and fetches value codes."""
    stats = parse_stats(stats_path)
    result = enrich(stats, register="TESTREG", db_path=regmeta_db)
    cols = {c.column_name: c for c in result[0].columns}

    kon = cols["Kon"]
    assert kon.register_id == 1
    assert kon.var_id == 44
    assert kon.variable_name == "Kön"
    assert kon.value_codes == {"1": "Man", "2": "Kvinna"}
