"""Tests for stats JSON parsing and validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.stats import (
    StatsValidationError,
    parse_stats,
)


def test_parse_minimal(stats_path: Path):
    result = parse_stats(stats_path)
    assert result.contract_version == "2.0.0"
    assert len(result.sources) == 1
    assert result.sources[0].source_name == "persons.csv"
    assert result.sources[0].source_type == "file"
    assert result.sources[0].source_detail["path"].endswith("persons.csv")
    assert len(result.sources[0].columns) == 6
    assert result.sources[0].row_count == 1000


def test_parse_multi_file(multi_file_stats_path: Path):
    result = parse_stats(multi_file_stats_path)
    assert len(result.sources) == 2
    assert len(result.shared_columns) == 1
    assert result.shared_columns[0].column_name == "LopNr"
    assert result.shared_columns[0].sources == ["file_a.csv", "file_b.csv"]
    assert result.shared_columns[0].max_n_distinct == 500


def test_column_types(stats_path: Path):
    result = parse_stats(stats_path)
    cols = {c.column_name: c for c in result.sources[0].columns}
    assert cols["LopNr"].inferred_type == "id"
    assert cols["Kon"].inferred_type == "categorical"
    assert cols["FodelseAr"].inferred_type == "numeric"
    assert cols["Datum"].inferred_type == "date"
    assert cols["Namn"].inferred_type == "high_cardinality"


def test_nullable(stats_path: Path):
    result = parse_stats(stats_path)
    cols = {c.column_name: c for c in result.sources[0].columns}
    assert cols["LopNr"].nullable is False
    assert cols["Kommun"].nullable is True
    assert cols["Kommun"].null_rate == 0.05


def test_missing_contract_version(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"sources": []}))
    with pytest.raises(StatsValidationError, match="contract_version"):
        parse_stats(p)


def test_wrong_major_version(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text(
        json.dumps(
            {
                "contract_version": "1.0.0",
                "sources": [
                    {
                        "source_name": "x.csv",
                        "source_type": "file",
                        "source_detail": {"path": "x.csv"},
                        "row_count": 1,
                        "columns": [{"column_name": "a", "inferred_type": "numeric"}],
                    }
                ],
            }
        )
    )
    with pytest.raises(StatsValidationError, match="Unsupported"):
        parse_stats(p)


def test_empty_sources(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"contract_version": "2.0.0", "sources": []}))
    with pytest.raises(StatsValidationError, match="No sources"):
        parse_stats(p)


def test_invalid_type(tmp_path: Path):
    p = tmp_path / "bad.json"
    data = {
        "contract_version": "2.0.0",
        "sources": [
            {
                "source_name": "x.csv",
                "source_type": "file",
                "source_detail": {"path": "x.csv"},
                "row_count": 10,
                "columns": [{"column_name": "a", "inferred_type": "bogus"}],
            }
        ],
    }
    p.write_text(json.dumps(data))
    with pytest.raises(StatsValidationError, match="Invalid inferred_type"):
        parse_stats(p)


def test_invalid_source_type(tmp_path: Path):
    p = tmp_path / "bad.json"
    data = {
        "contract_version": "2.0.0",
        "sources": [
            {
                "source_name": "x",
                "source_type": "parquet",
                "source_detail": {},
                "row_count": 10,
                "columns": [{"column_name": "a", "inferred_type": "numeric"}],
            }
        ],
    }
    p.write_text(json.dumps(data))
    with pytest.raises(StatsValidationError, match="Invalid source_type"):
        parse_stats(p)


def test_invalid_json(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text("not json at all")
    with pytest.raises(StatsValidationError, match="Invalid JSON"):
        parse_stats(p)


def test_parse_panels_block(tmp_path: Path):
    """#23: mdw_step3_stats.json's panels block round-trips through parse_stats."""
    data = {
        "contract_version": "2.0.0",
        "sources": [
            {
                "source_name": "x.csv",
                "source_type": "file",
                "source_detail": {"path": "x.csv"},
                "row_count": 10,
                "columns": [{"column_name": "a", "inferred_type": "id"}],
            }
        ],
        "panels": [
            {
                "panel_id": "merged",
                "layout": "merged_table",
                "source": "x.csv",
                "panel_key": "a",
                "time_key": "ar",
                "by_period": [
                    {"period": 2018, "n_rows": 100, "n_panel_ids": 80},
                    {"period": 2019, "n_rows": 110, "n_panel_ids": 85},
                ],
            },
            {
                "panel_id": "split",
                "layout": "separate_files",
                "panel_key": "a",
                "by_period": [
                    {
                        "period": 2020,
                        "source": "y.csv",
                        "n_rows": 50,
                        "n_panel_ids": 40,
                    },
                ],
            },
        ],
    }
    p = tmp_path / "mdw_step3_stats.json"
    p.write_text(json.dumps(data))
    result = parse_stats(p)
    assert len(result.panels) == 2
    merged = next(pn for pn in result.panels if pn.panel_id == "merged")
    assert merged.layout == "merged_table"
    assert merged.source == "x.csv"
    assert merged.time_key == "ar"
    assert [bp.period for bp in merged.by_period] == [2018, 2019]
    split = next(pn for pn in result.panels if pn.panel_id == "split")
    assert split.layout == "separate_files"
    assert split.by_period[0].source == "y.csv"


def test_parse_panels_default_to_empty(stats_path: Path):
    """A mdw_step3_stats.json without a panels block parses with panels=[]."""
    result = parse_stats(stats_path)
    assert result.panels == []


def test_no_columns_in_source(tmp_path: Path):
    p = tmp_path / "bad.json"
    data = {
        "contract_version": "2.0.0",
        "sources": [
            {
                "source_name": "x.csv",
                "source_type": "file",
                "source_detail": {"path": "x.csv"},
                "row_count": 10,
                "columns": [],
            }
        ],
    }
    p.write_text(json.dumps(data))
    with pytest.raises(StatsValidationError, match="no columns"):
        parse_stats(p)
