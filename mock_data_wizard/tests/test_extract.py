"""End-to-end and unit tests for extract.py.

Two top-level entry points: ``run_discover`` (metadata-only walk) and
``run_extract_typed`` (typed pipeline against ``mdw_config.json``).
``main`` dispatches between them via ``mode=``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.config import parse_config
from mock_data_wizard.extract import (
    _shared_columns,
    main,
    run_discover,
    run_extract_typed,
)
from mock_data_wizard.sources import file_source


def _write_csv(p: Path, content: str) -> None:
    p.write_text(content, encoding="utf-8")


# -- run_discover end-to-end ---------------------------------------------


def test_run_discover_file_source_writes_metadata(tmp_path: Path):
    _write_csv(
        tmp_path / "people.csv",
        "lopnr,age,kommun\n1,25,0114\n2,30,0114\n3,42,0115\n",
    )
    out = tmp_path / "discover.json"
    src = file_source(str(tmp_path), include=["people.csv"])
    result = run_discover([src], out)

    on_disk = json.loads(out.read_text(encoding="utf-8"))
    assert on_disk == result
    assert result["contract_version"] == "discover-1.0.0"
    src_out = result["sources"][0]
    assert src_out["source_name"] == "people.csv"
    assert src_out["row_count"] == 3
    col_names = [c["name"] for c in src_out["columns"]]
    assert col_names == ["lopnr", "age", "kommun"]
    # DuckDB DESCRIBE gives sql_type and null
    for c in src_out["columns"]:
        assert "sql_type" in c
        assert "nullable" in c


def test_run_discover_pii_scan_passes_clean_payload(tmp_path: Path):
    _write_csv(tmp_path / "data.csv", "x,y\n1,2\n3,4\n")
    out = tmp_path / "discover.json"
    src = file_source(str(tmp_path), include=["data.csv"])
    result = run_discover([src], out)
    assert result["pii_scan"]["matches_found"] == 0


def test_run_discover_raises_when_no_data(tmp_path: Path):
    src = file_source(str(tmp_path), include=["nonexistent.csv"])
    with pytest.raises(RuntimeError, match="No data sources"):
        run_discover([src], tmp_path / "discover.json")


# -- run_extract_typed end-to-end ----------------------------------------


def test_run_extract_typed_writes_valid_stats_json(tmp_path: Path):
    _write_csv(
        tmp_path / "people.csv",
        "lopnr,age,kommun,name\n"
        "1,25,0114,alice\n2,30,0114,bob\n3,42,0115,carol\n4,55,0114,dave\n"
        "5,29,0115,eve\n6,38,0114,frank\n7,47,0115,grace\n8,33,0114,heidi\n",
    )
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "people.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "age": {"type": "numeric", "numeric_subtype": "integer"},
                    "kommun": {"type": "categorical"},
                    "name": {"type": "high_cardinality"},
                }
            },
        }
    )
    src = file_source(str(tmp_path), include=["people.csv"])
    out = tmp_path / "stats.json"
    result = run_extract_typed([src], out, config, seed=0)

    on_disk = json.loads(out.read_text(encoding="utf-8"))
    assert on_disk == result

    assert result["contract_version"] == "2.0.0"
    src_out = result["sources"][0]
    assert src_out["source_name"] == "people.csv"
    assert src_out["row_count"] == 8
    by_name = {c["column_name"]: c for c in src_out["columns"]}
    assert by_name["lopnr"]["inferred_type"] == "id"
    assert by_name["age"]["inferred_type"] == "numeric"
    assert by_name["kommun"]["inferred_type"] == "categorical"
    assert by_name["name"]["inferred_type"] == "high_cardinality"
    assert all(c["source_of_type"] == "override" for c in by_name.values())


def test_run_extract_typed_errors_on_unconfigured_column(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    # Missing 'age' override.
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "data.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}}
            },
        }
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    with pytest.raises(RuntimeError, match="no type override"):
        run_extract_typed([src], tmp_path / "stats.json", config)


def test_run_extract_typed_records_shared_columns(tmp_path: Path):
    _write_csv(tmp_path / "a.csv", "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n")
    _write_csv(tmp_path / "b.csv", "lopnr,sex\n1,M\n2,F\n3,M\n4,F\n5,M\n6,F\n")
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "a.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "age": {"type": "numeric", "numeric_subtype": "integer"},
                },
                "b.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "sex": {"type": "categorical"},
                },
            },
        }
    )
    src = file_source(str(tmp_path), include=["a.csv", "b.csv"])
    result = run_extract_typed([src], tmp_path / "stats.json", config, seed=1)
    shared = {s["column_name"]: s for s in result["shared_columns"]}
    assert "lopnr" in shared
    assert sorted(shared["lopnr"]["sources"]) == ["a.csv", "b.csv"]
    assert "age" not in shared
    assert "sex" not in shared


def test_run_extract_typed_where_narrows_row_count(tmp_path: Path):
    """End-to-end: stats.json reflects the FILTERED set, not the source set."""
    _write_csv(
        tmp_path / "events.csv",
        "lopnr,ar,kommun\n"
        "1,2013,0114\n2,2014,0114\n3,2015,0115\n"
        "4,2016,0114\n5,2017,0115\n6,2018,0114\n7,2019,0115\n8,2020,0114\n",
    )
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "events.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "ar": {"type": "numeric", "numeric_subtype": "integer"},
                    "kommun": {"type": "categorical"},
                }
            },
        }
    )
    src = file_source(str(tmp_path), include=["events.csv"], where="ar > 2015")
    result = run_extract_typed([src], tmp_path / "stats.json", config, seed=0)

    src_out = result["sources"][0]
    assert src_out["row_count"] == 5  # filtered, not 8
    assert src_out["source_detail"]["where"] == "ar > 2015"


def test_run_extract_typed_raises_when_no_data(tmp_path: Path):
    config = parse_config({"version": 1, "column_types": {}})
    src = file_source(str(tmp_path), include=["nonexistent.csv"])
    with pytest.raises(RuntimeError, match="No data sources"):
        run_extract_typed([src], tmp_path / "stats.json", config)


# -- _shared_columns -----------------------------------------------------


def test_shared_columns_keeps_only_2plus_sources():
    src_results = [
        {
            "source_name": "a",
            "columns": [
                {"column_name": "x", "n_distinct": 5},
                {"column_name": "lopnr", "n_distinct": 100},
            ],
        },
        {
            "source_name": "b",
            "columns": [
                {"column_name": "y", "n_distinct": 8},
                {"column_name": "lopnr", "n_distinct": 200},
            ],
        },
    ]
    out = _shared_columns(src_results)
    assert len(out) == 1
    assert out[0]["column_name"] == "lopnr"
    assert sorted(out[0]["sources"]) == ["a", "b"]
    assert out[0]["max_n_distinct"] == 200


def test_shared_columns_dedups_when_same_source_twice():
    src_results = [
        {
            "source_name": "a",
            "columns": [{"column_name": "lopnr", "n_distinct": 50}],
        },
        {
            "source_name": "a",
            "columns": [{"column_name": "lopnr", "n_distinct": 60}],
        },
    ]
    assert _shared_columns(src_results) == []


# -- main() flow ---------------------------------------------------------


def test_main_discover_writes_discover_json(tmp_path: Path):
    _write_csv(tmp_path / "x.csv", "a,b\n1,2\n3,4\n")
    src = file_source(str(tmp_path), include=["x.csv"])
    out = main([src], output_dir=tmp_path, mode="discover")
    assert out is not None
    assert (tmp_path / "discover.json").exists()
    assert out["contract_version"] == "discover-1.0.0"


def test_main_extract_requires_mdw_config(tmp_path: Path):
    _write_csv(tmp_path / "x.csv", "a\n1\n")
    src = file_source(str(tmp_path), include=["x.csv"])
    with pytest.raises(RuntimeError, match="mdw_config.json"):
        main([src], output_dir=tmp_path, mode="extract")


def test_main_extract_runs_typed_pipeline(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    (tmp_path / "mdw_config.json").write_text(
        json.dumps(
            {
                "version": 1,
                "column_types": {
                    "data.csv": {
                        "lopnr": {"type": "id", "id_subtype": "integer"},
                        "age": {"type": "numeric", "numeric_subtype": "integer"},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    result = main([src], output_dir=tmp_path, mode="extract", seed=0)
    assert result["sources"][0]["row_count"] == 6
    cols = {c["column_name"]: c for c in result["sources"][0]["columns"]}
    assert cols["age"]["inferred_type"] == "numeric"
    assert cols["age"]["source_of_type"] == "override"


def test_main_rejects_unknown_mode(tmp_path: Path):
    with pytest.raises(ValueError, match="must be 'discover' or 'extract'"):
        main([], output_dir=tmp_path, mode="weird")


# -- deterministic classification sample (#18) ---------------------------


def test_classifier_seed_is_threaded_through_main(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    (tmp_path / "mdw_config.json").write_text(
        json.dumps(
            {
                "version": 1,
                "column_types": {
                    "data.csv": {
                        "lopnr": {"type": "id"},
                        "age": {"type": "numeric"},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    result = main(
        [src],
        output_dir=tmp_path,
        mode="extract",
        seed=0,
        classifier_seed=42,
    )
    assert result["sources"][0]["row_count"] == 6


def test_table_and_view_paths_produce_identical_stats(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """#22: VIEW vs TABLE materialisation must yield identical stats.json
    for the same fixture (only `generated_at` is allowed to differ)."""
    _write_csv(
        tmp_path / "people.csv",
        "lopnr,age,kommun,name\n"
        "1,25,0114,alice\n2,30,0114,bob\n3,42,0115,carol\n4,55,0114,dave\n"
        "5,29,0115,eve\n6,38,0114,frank\n7,47,0115,grace\n8,33,0114,heidi\n",
    )
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "people.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "age": {"type": "numeric", "numeric_subtype": "integer"},
                    "kommun": {"type": "categorical"},
                    "name": {"type": "high_cardinality"},
                }
            },
        }
    )
    src = file_source(str(tmp_path), include=["people.csv"])

    monkeypatch.delenv("MDW_MEMORY_THRESHOLD_MB", raising=False)
    table_result = run_extract_typed(
        [src], tmp_path / "stats_table.json", config, seed=0
    )

    monkeypatch.setenv("MDW_MEMORY_THRESHOLD_MB", "0")
    view_result = run_extract_typed([src], tmp_path / "stats_view.json", config, seed=0)

    table_result.pop("generated_at", None)
    view_result.pop("generated_at", None)
    assert table_result == view_result


# -- mdw_config.json overrides (#19) -------------------------------------


def test_extract_inline_hint_skips_sample(tmp_path: Path, monkeypatch):
    """Inline subtype hint -> _sample_values is NOT called for that column."""
    from mock_data_wizard import extract

    _write_csv(
        tmp_path / "data.csv",
        "lopnr,name\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n",
    )
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "data.csv": {
                    "name": {"type": "id", "id_subtype": "string"},
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                }
            },
        }
    )

    sample_calls: list[str] = []
    real_sample = extract._sample_values

    def spy(conn, table, col, dialect, *args, **kwargs):
        sample_calls.append(col)
        return real_sample(conn, table, col, dialect, *args, **kwargs)

    monkeypatch.setattr(extract, "_sample_values", spy)

    src = file_source(str(tmp_path), include=["data.csv"])
    result = run_extract_typed([src], tmp_path / "stats.json", config, seed=0)
    cols = {c["column_name"]: c for c in result["sources"][0]["columns"]}
    assert cols["name"]["stats"]["id_subtype"] == "string"
    assert cols["lopnr"]["stats"]["id_subtype"] == "integer"
    assert sample_calls == []  # both columns had inline hints


def test_extract_override_without_inline_hint_runs_sample(tmp_path: Path, monkeypatch):
    from mock_data_wizard import extract

    _write_csv(
        tmp_path / "data.csv",
        "lopnr,name\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n",
    )
    config = parse_config(
        {
            "version": 1,
            "column_types": {
                "data.csv": {
                    "lopnr": {"type": "id"},
                    "name": {"type": "id"},
                }
            },
        }
    )

    sample_calls: list[str] = []
    real_sample = extract._sample_values

    def spy(conn, table, col, dialect, *args, **kwargs):
        sample_calls.append(col)
        return real_sample(conn, table, col, dialect, *args, **kwargs)

    monkeypatch.setattr(extract, "_sample_values", spy)

    src = file_source(str(tmp_path), include=["data.csv"])
    run_extract_typed([src], tmp_path / "stats.json", config, seed=0)
    assert "name" in sample_calls
    assert "lopnr" in sample_calls


def test_main_raises_on_invalid_mdw_config(tmp_path: Path):
    _write_csv(tmp_path / "data.csv", "x\n1\n")
    (tmp_path / "mdw_config.json").write_text(
        json.dumps(
            {"version": 1, "column_types": {"data.csv": {"x": {"type": "blob"}}}}
        ),
        encoding="utf-8",
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    with pytest.raises(ValueError, match="expected one of"):
        main([src], output_dir=tmp_path, mode="extract", seed=0)
