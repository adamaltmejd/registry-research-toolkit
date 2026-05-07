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
    assert cols["Namn"].inferred_type == "opaque"


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
    """mdw_step3_stats.json's panels block round-trips through parse_stats."""
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
                "panel_key": "a",
                "members": [{"source": "x.csv", "time_key": "ar"}],
                "by_period": [
                    {
                        "period": 2018,
                        "source": "x.csv",
                        "n_rows": 100,
                        "n_panel_ids": 80,
                    },
                    {
                        "period": 2019,
                        "source": "x.csv",
                        "n_rows": 110,
                        "n_panel_ids": 85,
                    },
                ],
            },
            {
                "panel_id": "split",
                "panel_key": "a",
                "members": [{"source": "y.csv", "period": 2020}],
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
    assert len(merged.members) == 1
    assert merged.members[0].source == "x.csv"
    assert merged.members[0].time_key == "ar"
    assert [bp.period for bp in merged.by_period] == [2018, 2019]
    split = next(pn for pn in result.panels if pn.panel_id == "split")
    assert split.members[0].period == 2020
    assert split.by_period[0].source == "y.csv"


def _stats_with_panel_member(tmp_path: Path, member: dict) -> Path:
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
                "panel_id": "p",
                "panel_key": "a",
                "members": [member],
                "by_period": [
                    {
                        "period": 2018,
                        "source": "x.csv",
                        "n_rows": 1,
                        "n_panel_ids": 1,
                    }
                ],
            }
        ],
    }
    p = tmp_path / "stats.json"
    p.write_text(json.dumps(data))
    return p


def test_parse_panels_rejects_null_period(tmp_path: Path):
    """``{"period": null}`` passes the period-xor-time_key gate but
    must not flow into PanelMemberRef as ``period=None`` — generation
    later assumes a real scalar."""
    p = _stats_with_panel_member(tmp_path, {"source": "x.csv", "period": None})
    with pytest.raises(StatsValidationError, match="period"):
        parse_stats(p)


def test_parse_panels_rejects_empty_time_key(tmp_path: Path):
    p = _stats_with_panel_member(tmp_path, {"source": "x.csv", "time_key": ""})
    with pytest.raises(StatsValidationError, match="time_key"):
        parse_stats(p)


def test_parse_panels_rejects_bool_period(tmp_path: Path):
    """``True`` is technically an int subclass; reject it explicitly so
    period stays unambiguously a year/quarter scalar."""
    p = _stats_with_panel_member(tmp_path, {"source": "x.csv", "period": True})
    with pytest.raises(StatsValidationError, match="period"):
        parse_stats(p)


def test_parse_panels_default_to_empty(stats_path: Path):
    """A mdw_step3_stats.json without a panels block parses with panels=[]."""
    result = parse_stats(stats_path)
    assert result.panels == []


def _stats_with_by_period_entry(tmp_path: Path, by_period_entry: dict) -> Path:
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
                "panel_id": "p",
                "panel_key": "a",
                "members": [{"source": "x.csv", "period": 2018}],
                "by_period": [by_period_entry],
            }
        ],
    }
    p = tmp_path / "stats.json"
    p.write_text(json.dumps(data))
    return p


def test_parse_panels_rejects_null_by_period_period(tmp_path: Path):
    """``by_period[].period = null`` would propagate into PanelPeriod as
    ``period=None`` and break pool keying in generate."""
    p = _stats_with_by_period_entry(
        tmp_path,
        {"period": None, "source": "x.csv", "n_rows": 1, "n_panel_ids": 1},
    )
    with pytest.raises(StatsValidationError, match="by_period.period"):
        parse_stats(p)


def test_parse_panels_rejects_bool_by_period_period(tmp_path: Path):
    p = _stats_with_by_period_entry(
        tmp_path,
        {"period": True, "source": "x.csv", "n_rows": 1, "n_panel_ids": 1},
    )
    with pytest.raises(StatsValidationError, match="by_period.period"):
        parse_stats(p)


def test_parse_panels_rejects_empty_by_period_source(tmp_path: Path):
    p = _stats_with_by_period_entry(
        tmp_path,
        {"period": 2018, "source": "", "n_rows": 1, "n_panel_ids": 1},
    )
    with pytest.raises(StatsValidationError, match="by_period.source"):
        parse_stats(p)


def test_parse_panels_rejects_non_int_by_period_n_rows(tmp_path: Path):
    p = _stats_with_by_period_entry(
        tmp_path,
        {"period": 2018, "source": "x.csv", "n_rows": "10", "n_panel_ids": 1},
    )
    with pytest.raises(StatsValidationError, match="n_rows"):
        parse_stats(p)


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
