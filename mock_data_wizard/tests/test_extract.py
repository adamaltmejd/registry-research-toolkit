"""End-to-end and unit tests for extract.py.

Two top-level entry points: ``run_discover`` (metadata-only walk) and
``run_extract_typed`` (typed pipeline against ``mock_data_config.json``).
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
    out = tmp_path / "mock_data_discovery.json"
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
    out = tmp_path / "mock_data_discovery.json"
    src = file_source(str(tmp_path), include=["data.csv"])
    result = run_discover([src], out)
    assert result["pii_scan"]["matches_found"] == 0


def test_run_discover_raises_when_no_data(tmp_path: Path):
    src = file_source(str(tmp_path), include=["nonexistent.csv"])
    with pytest.raises(RuntimeError, match="No data sources"):
        run_discover([src], tmp_path / "mock_data_discovery.json")


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
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "people.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "age": {"type": "numeric", "numeric_subtype": "integer"},
                    "kommun": {"type": "categorical"},
                    "name": {"type": "opaque"},
                }
            },
        }
    )
    src = file_source(str(tmp_path), include=["people.csv"])
    out = tmp_path / "mock_data_stats.json"
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
    assert by_name["name"]["inferred_type"] == "opaque"
    assert all(c["source_of_type"] == "override" for c in by_name.values())


def test_run_extract_typed_errors_on_unconfigured_column(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    # Missing 'age' override.
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "data.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}}
            },
        }
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    with pytest.raises(RuntimeError, match="no type override"):
        run_extract_typed([src], tmp_path / "mock_data_stats.json", config)


def test_run_extract_typed_records_shared_columns(tmp_path: Path):
    _write_csv(tmp_path / "a.csv", "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n")
    _write_csv(tmp_path / "b.csv", "lopnr,sex\n1,M\n2,F\n3,M\n4,F\n5,M\n6,F\n")
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
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
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=1)
    shared = {s["column_name"]: s for s in result["shared_columns"]}
    assert "lopnr" in shared
    assert sorted(shared["lopnr"]["sources"]) == ["a.csv", "b.csv"]
    assert "age" not in shared
    assert "sex" not in shared


def test_run_extract_typed_where_narrows_row_count(tmp_path: Path):
    """End-to-end: mock_data_stats.json reflects the FILTERED set, not the source set."""
    _write_csv(
        tmp_path / "events.csv",
        "lopnr,ar,kommun\n"
        "1,2013,0114\n2,2014,0114\n3,2015,0115\n"
        "4,2016,0114\n5,2017,0115\n6,2018,0114\n7,2019,0115\n8,2020,0114\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
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
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)

    src_out = result["sources"][0]
    assert src_out["row_count"] == 5  # filtered, not 8
    assert src_out["source_detail"]["where"] == "ar > 2015"


def test_run_extract_typed_raises_when_no_data(tmp_path: Path):
    config = parse_config({"contract_version": "mdw-config-3.0.0", "column_types": {}})
    src = file_source(str(tmp_path), include=["nonexistent.csv"])
    with pytest.raises(RuntimeError, match="No data sources"):
        run_extract_typed([src], tmp_path / "mock_data_stats.json", config)


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
    assert (tmp_path / "mock_data_discovery.json").exists()
    assert out["contract_version"] == "discover-1.0.0"


def test_main_extract_requires_mdw_config(tmp_path: Path):
    _write_csv(tmp_path / "x.csv", "a\n1\n")
    src = file_source(str(tmp_path), include=["x.csv"])
    with pytest.raises(RuntimeError, match="mock_data_config.json"):
        main([src], output_dir=tmp_path, mode="extract")


def test_main_extract_runs_typed_pipeline(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(
            {
                "contract_version": "mdw-config-3.0.0",
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
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(
            {
                "contract_version": "mdw-config-3.0.0",
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
    """#22: VIEW vs TABLE materialisation must yield identical mock_data_stats.json
    for the same fixture (only `generated_at` is allowed to differ)."""
    _write_csv(
        tmp_path / "people.csv",
        "lopnr,age,kommun,name\n"
        "1,25,0114,alice\n2,30,0114,bob\n3,42,0115,carol\n4,55,0114,dave\n"
        "5,29,0115,eve\n6,38,0114,frank\n7,47,0115,grace\n8,33,0114,heidi\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "people.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "age": {"type": "numeric", "numeric_subtype": "integer"},
                    "kommun": {"type": "categorical"},
                    "name": {"type": "opaque"},
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


# -- mock_data_config.json overrides (#19) -------------------------------------


def test_extract_inline_hint_skips_sample(tmp_path: Path, monkeypatch):
    """Inline subtype hint -> _sample_values is NOT called for that column."""
    from mock_data_wizard import extract

    _write_csv(
        tmp_path / "data.csv",
        "lopnr,name\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
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
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
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
            "contract_version": "mdw-config-3.0.0",
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
    run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    assert "name" in sample_calls
    assert "lopnr" in sample_calls


# -- year detection (#24) -------------------------------------------------


def test_run_discover_emits_year_when_detectable(tmp_path: Path):
    """Discover detects year from filename and stamps it in source_detail."""
    _write_csv(tmp_path / "lisa_2018.csv", "x,y\n1,2\n3,4\n")
    src = file_source(str(tmp_path), include=["lisa_2018.csv"])
    result = run_discover([src], tmp_path / "mock_data_discovery.json")
    detail = result["sources"][0]["source_detail"]
    assert detail["year"] == 2018


def test_run_discover_omits_year_when_none_detectable(tmp_path: Path):
    _write_csv(tmp_path / "people.csv", "x\n1\n2\n")
    src = file_source(str(tmp_path), include=["people.csv"])
    result = run_discover([src], tmp_path / "mock_data_discovery.json")
    assert "year" not in result["sources"][0]["source_detail"]


def test_run_extract_typed_emits_year_in_source_detail(tmp_path: Path):
    """Extract carries the year through to mock_data_stats.json -- regex fallback
    when no config override is supplied."""
    _write_csv(
        tmp_path / "rtb2019.csv",
        "lopnr\n" + "\n".join(str(i) for i in range(1, 21)) + "\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "rtb2019.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}}
            },
        }
    )
    src = file_source(str(tmp_path), include=["rtb2019.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    detail = result["sources"][0]["source_detail"]
    assert detail["year"] == 2019


def test_run_extract_typed_uses_config_year_over_regex(tmp_path: Path):
    """Config-supplied year wins over a wrong filename guess."""
    # Filename has 2030 but the user knows the real year is 2025.
    _write_csv(
        tmp_path / "weird_2030.csv",
        "lopnr\n" + "\n".join(str(i) for i in range(1, 21)) + "\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "weird_2030.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}}
            },
            "sources": {"weird_2030.csv": {"year": 2025}},
        }
    )
    src = file_source(str(tmp_path), include=["weird_2030.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    assert result["sources"][0]["source_detail"]["year"] == 2025


# -- panels (#23) --------------------------------------------------------


def test_run_extract_typed_emits_panel_with_literal_time_key_members(tmp_path: Path):
    """A panel made of file-members reads ``n_entity_ids`` from each
    member's entity-key column and ``n_rows`` from each member source."""
    _write_csv(
        tmp_path / "lisa_2018.csv",
        "lopnr\n" + "\n".join(str(i) for i in range(1, 14)) + "\n",
    )
    _write_csv(
        tmp_path / "lisa_2019.csv",
        "lopnr\n" + "\n".join(str(i) for i in range(1, 17)) + "\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "lisa_2018.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}},
                "lisa_2019.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}},
            },
            "panels": [
                {
                    "panel_id": "lisa",
                    "entity_key": "lopnr",
                    "members": [
                        {"source": "lisa_2018.csv", "time_key": 2018},
                        {"source": "lisa_2019.csv", "time_key": 2019},
                    ],
                }
            ],
        }
    )
    src = file_source(str(tmp_path), include=["lisa_2018.csv", "lisa_2019.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    panels = result["panels"]
    assert len(panels) == 1
    p = panels[0]
    assert p["panel_id"] == "lisa"
    assert p["entity_key"] == "lopnr"
    assert p["members"] == [
        {"source": "lisa_2018.csv", "time_key": 2018},
        {"source": "lisa_2019.csv", "time_key": 2019},
    ]
    by_period = {bp["period"]: bp for bp in p["by_period"]}
    assert by_period[2018]["n_rows"] == 13
    assert by_period[2018]["n_entity_ids"] == 13
    assert by_period[2018]["source"] == "lisa_2018.csv"
    assert by_period[2019]["n_rows"] == 16
    assert by_period[2019]["n_entity_ids"] == 16


def test_run_extract_typed_emits_panel_with_column_time_key_member(tmp_path: Path):
    """A panel with a single column-member runs an extra GROUP BY on
    the source and emits per-period n_rows / n_entity_ids."""
    rows = []
    for ar in (2018, 2019, 2020):
        for lopnr in range(1, 13):
            rows.append(f"{lopnr},{ar}")
    _write_csv(tmp_path / "swecov.csv", "lopnr,ar\n" + "\n".join(rows) + "\n")
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "swecov.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "ar": {"type": "numeric", "numeric_subtype": "integer"},
                }
            },
            "panels": [
                {
                    "panel_id": "swecov_inpatient",
                    "entity_key": "lopnr",
                    "members": [{"source": "swecov.csv", "time_key": "ar"}],
                }
            ],
        }
    )
    src = file_source(str(tmp_path), include=["swecov.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    panels = result["panels"]
    assert len(panels) == 1
    p = panels[0]
    assert p["members"] == [{"source": "swecov.csv", "time_key": "ar"}]
    by_period = {bp["period"]: bp for bp in p["by_period"]}
    assert set(by_period) == {2018, 2019, 2020}
    for bp in by_period.values():
        assert bp["n_rows"] == 12
        assert bp["n_entity_ids"] == 12
        assert bp["source"] == "swecov.csv"


def test_run_extract_typed_suppresses_panel_periods_below_k(tmp_path: Path):
    """A period with n_entity_ids < SUPPRESS_K (=10) is dropped from
    the panels block: tiny panel cohorts are identifying."""
    rows = ["lopnr,ar"]
    rows.extend(f"{i},2018" for i in range(1, 6))
    rows.extend(f"{i},2019" for i in range(1, 13))
    _write_csv(tmp_path / "swecov.csv", "\n".join(rows) + "\n")
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "swecov.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "ar": {"type": "numeric", "numeric_subtype": "integer"},
                }
            },
            "panels": [
                {
                    "panel_id": "swecov",
                    "entity_key": "lopnr",
                    "members": [{"source": "swecov.csv", "time_key": "ar"}],
                }
            ],
        }
    )
    src = file_source(str(tmp_path), include=["swecov.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    p = result["panels"][0]
    periods = {bp["period"] for bp in p["by_period"]}
    assert periods == {2019}


def test_run_extract_typed_panel_handles_string_time_key(tmp_path: Path):
    """A column-member keyed by quarter / month strings (e.g.
    ``"2019-Q1"``) must not crash extract. Periods are preserved as
    strings in mock_data_stats.json.
    """
    rows = ["lopnr,quarter"]
    for q in ("2019-Q1", "2019-Q2"):
        rows.extend(f"{i},{q}" for i in range(1, 13))
    _write_csv(tmp_path / "swecov.csv", "\n".join(rows) + "\n")
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "swecov.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "quarter": {"type": "categorical"},
                }
            },
            "panels": [
                {
                    "panel_id": "swecov_q",
                    "entity_key": "lopnr",
                    "members": [{"source": "swecov.csv", "time_key": "quarter"}],
                }
            ],
        }
    )
    src = file_source(str(tmp_path), include=["swecov.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    p = result["panels"][0]
    periods = {bp["period"] for bp in p["by_period"]}
    assert periods == {"2019-Q1", "2019-Q2"}


def test_run_extract_typed_emits_mixed_member_panel(tmp_path: Path):
    """Mixed file-and-column members in one panel: history file with a
    time_key column + a fresh per-year file. by_period spans both."""
    history_rows = ["lopnr,ar"]
    for ar in (2022, 2023):
        history_rows.extend(f"{i},{ar}" for i in range(1, 13))
    _write_csv(tmp_path / "tax_history.csv", "\n".join(history_rows) + "\n")
    _write_csv(
        tmp_path / "tax_2024.csv",
        "lopnr\n" + "\n".join(str(i) for i in range(1, 14)) + "\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "tax_history.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                    "ar": {"type": "numeric", "numeric_subtype": "integer"},
                },
                "tax_2024.csv": {
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                },
            },
            "panels": [
                {
                    "panel_id": "tax",
                    "entity_key": "lopnr",
                    "members": [
                        {"source": "tax_history.csv", "time_key": "ar"},
                        {"source": "tax_2024.csv", "time_key": 2024},
                    ],
                }
            ],
        }
    )
    src = file_source(str(tmp_path), include=["tax_history.csv", "tax_2024.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    p = result["panels"][0]
    by_period = {bp["period"]: bp for bp in p["by_period"]}
    assert set(by_period) == {2022, 2023, 2024}
    assert by_period[2022]["source"] == "tax_history.csv"
    assert by_period[2024]["source"] == "tax_2024.csv"
    assert by_period[2024]["n_rows"] == 13


def test_run_extract_typed_raises_when_panel_loses_all_members(tmp_path: Path):
    """When every declared member of a panel is missing from the
    extract output (typo, filtered-out source), surface the error here
    rather than emit ``members: []`` and break later in stats parsing.
    """
    _write_csv(
        tmp_path / "lisa_2018.csv",
        "lopnr\n" + "\n".join(str(i) for i in range(1, 14)) + "\n",
    )
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "lisa_2018.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}},
                "lisa_2019.csv": {"lopnr": {"type": "id", "id_subtype": "integer"}},
            },
            "panels": [
                {
                    "panel_id": "lisa",
                    "entity_key": "lopnr",
                    "members": [
                        # Both sources misspelled / not in the include list.
                        {"source": "lisa_2018_typo.csv", "time_key": 2018},
                        {"source": "lisa_2019_typo.csv", "time_key": 2019},
                    ],
                }
            ],
        }
    )
    src = file_source(str(tmp_path), include=["lisa_2018.csv"])
    with pytest.raises(RuntimeError, match="no member sources matched"):
        run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)


def test_coerce_period_normalises_value():
    from mock_data_wizard.extract import _coerce_period

    assert _coerce_period(2018) == 2018
    assert _coerce_period("2018") == 2018  # numeric strings -> int
    assert _coerce_period("2019-Q1") == "2019-Q1"
    assert _coerce_period("2019-01-15") == "2019-01-15"
    # bool is an int subclass — must not silently become 0/1
    assert _coerce_period(True) == "True"


def test_run_extract_typed_panels_block_empty_when_no_panels_declared(tmp_path: Path):
    _write_csv(tmp_path / "x.csv", "a\n" + "\n".join(str(i) for i in range(20)) + "\n")
    config = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {"x.csv": {"a": {"type": "id", "id_subtype": "integer"}}},
        }
    )
    src = file_source(str(tmp_path), include=["x.csv"])
    result = run_extract_typed([src], tmp_path / "mock_data_stats.json", config, seed=0)
    assert result["panels"] == []


def test_resolve_year_falls_back_to_name_regex():
    from mock_data_wizard.extract import _resolve_year

    assert _resolve_year("lisa_2018", None) == 2018
    assert _resolve_year("RTB2019", None) == 2019
    assert _resolve_year("Fodelseuppg_20241231", None) == 2024  # first 4 digits
    assert _resolve_year("SWECOV_SOS_OV", None) is None
    assert _resolve_year("plain", None) is None


def test_main_raises_on_invalid_mdw_config(tmp_path: Path):
    _write_csv(tmp_path / "data.csv", "x\n1\n")
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_types": {"data.csv": {"x": {"type": "blob"}}},
            }
        ),
        encoding="utf-8",
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    with pytest.raises(ValueError, match="expected one of"):
        main([src], output_dir=tmp_path, mode="extract", seed=0)
