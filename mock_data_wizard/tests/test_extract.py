"""End-to-end and unit tests for extract.py.

The pipeline tests run real DuckDB against tmp CSVs. Discovery and
sidecar tests don't touch any DB.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.extract import (
    _shared_columns,
    emit_sources_skeleton,
    find_latest_sources_file,
    load_sources_file,
    main,
    run_extract,
)
from mock_data_wizard.sources import file_source


def _write_csv(p: Path, content: str) -> None:
    p.write_text(content, encoding="utf-8")


# -- run_extract end-to-end ----------------------------------------------


def test_run_extract_file_source_writes_valid_stats_json(tmp_path: Path):
    _write_csv(
        tmp_path / "people.csv",
        # 8 rows, 4 cols: lopnr (id), age (numeric), kommun (cat), name (high-card)
        "lopnr,age,kommun,name\n"
        "1,25,0114,alice\n"
        "2,30,0114,bob\n"
        "3,42,0115,carol\n"
        "4,55,0114,dave\n"
        "5,29,0115,eve\n"
        "6,38,0114,frank\n"
        "7,47,0115,grace\n"
        "8,33,0114,heidi\n",
    )
    out = tmp_path / "stats.json"
    src = file_source(str(tmp_path), include=["people.csv"])
    result = run_extract([src], out, seed=0)

    # File on disk matches the returned dict
    on_disk = json.loads(out.read_text(encoding="utf-8"))
    assert on_disk == result

    # Contract-level structure
    assert result["contract_version"] == "2.0.0"
    assert "generated_at" in result
    assert len(result["sources"]) == 1

    src_out = result["sources"][0]
    assert src_out["source_name"] == "people.csv"
    assert src_out["source_type"] == "file"
    assert src_out["row_count"] == 8

    by_name = {c["column_name"]: c for c in src_out["columns"]}
    assert set(by_name) == {"lopnr", "age", "kommun", "name"}
    assert by_name["lopnr"]["inferred_type"] == "id"
    assert by_name["age"]["inferred_type"] == "numeric"
    assert by_name["kommun"]["inferred_type"] == "categorical"
    # name with 8 distinct out of 8 rows is high-card or id depending on
    # threshold; both are acceptable for this fixture.
    assert by_name["name"]["inferred_type"] in ("id", "high_cardinality")


def test_run_extract_records_shared_columns(tmp_path: Path):
    _write_csv(
        tmp_path / "a.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    _write_csv(
        tmp_path / "b.csv",
        "lopnr,sex\n1,M\n2,F\n3,M\n4,F\n5,M\n6,F\n",
    )
    out = tmp_path / "stats.json"
    src = file_source(str(tmp_path), include=["a.csv", "b.csv"])
    result = run_extract([src], out, seed=1)

    shared = {s["column_name"]: s for s in result["shared_columns"]}
    assert "lopnr" in shared
    assert sorted(shared["lopnr"]["sources"]) == ["a.csv", "b.csv"]
    # age and sex appear only in one source each
    assert "age" not in shared
    assert "sex" not in shared


def test_run_extract_raises_when_no_data(tmp_path: Path):
    # Source with include= that matches nothing produces zero handles.
    src = file_source(str(tmp_path), include=["nonexistent.csv"])
    with pytest.raises(RuntimeError, match="No data sources"):
        run_extract([src], tmp_path / "stats.json")


def test_run_extract_where_narrows_row_count_and_records_clause(tmp_path: Path):
    """End-to-end: stats.json reflects the FILTERED set, not the source set."""
    _write_csv(
        tmp_path / "events.csv",
        # 8 rows total, 5 with ar > 2015
        "lopnr,ar,kommun\n"
        "1,2013,0114\n2,2014,0114\n3,2015,0115\n"
        "4,2016,0114\n5,2017,0115\n6,2018,0114\n7,2019,0115\n8,2020,0114\n",
    )
    src = file_source(str(tmp_path), include=["events.csv"], where="ar > 2015")
    out = tmp_path / "stats.json"
    result = run_extract([src], out, seed=0)

    src_out = result["sources"][0]
    assert src_out["row_count"] == 5  # filtered, not 8
    assert src_out["source_detail"]["where"] == "ar > 2015"
    # Classification should still work end-to-end through the derived table.
    by_name = {c["column_name"]: c for c in src_out["columns"]}
    assert by_name["lopnr"]["inferred_type"] == "id"
    assert by_name["kommun"]["inferred_type"] == "categorical"


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
    # Same source name appearing twice (rare but possible) shouldn't count
    # as two for the 2+ rule.
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


# -- discovery skeleton --------------------------------------------------


def test_emit_sources_skeleton_for_file_source(tmp_path: Path):
    _write_csv(tmp_path / "alpha.csv", "x\n1\n")
    _write_csv(tmp_path / "beta.csv", "x\n2\n")
    out_dir = tmp_path / "out"
    src = file_source(str(tmp_path))
    skel = emit_sources_skeleton([src], out_dir)
    assert skel.exists()
    text = skel.read_text(encoding="utf-8")
    assert "SOURCES = [" in text
    assert "file_source(" in text
    assert "'alpha.csv'" in text
    assert "'beta.csv'" in text
    assert text.endswith("\n")


def test_emit_sources_skeleton_handles_unknown_source(tmp_path: Path):
    out_dir = tmp_path / "out"
    skel = emit_sources_skeleton(["not a source"], out_dir)
    assert "unknown source skipped" in skel.read_text(encoding="utf-8")


# -- sidecar load / latest -----------------------------------------------


def test_find_latest_sources_file_picks_lexicographic_max(tmp_path: Path):
    (tmp_path / "mdw_sources_20260101_120000.py").write_text("SOURCES = []\n")
    (tmp_path / "mdw_sources_20260427_103254.py").write_text("SOURCES = []\n")
    latest = find_latest_sources_file(tmp_path)
    assert latest is not None
    assert latest.name == "mdw_sources_20260427_103254.py"


def test_find_latest_sources_file_none_when_empty(tmp_path: Path):
    assert find_latest_sources_file(tmp_path) is None


def test_load_sources_file_executes_and_returns_sources(tmp_path: Path):
    sidecar = tmp_path / "mdw_sources_20260427.py"
    sidecar.write_text(
        "SOURCES = [\n"
        f'    file_source(path={str(tmp_path)!r}, include=("a.csv",)),\n'
        "]\n",
        encoding="utf-8",
    )
    out = load_sources_file(sidecar)
    assert len(out) == 1
    assert out[0].path == str(tmp_path)
    assert out[0].include == ("a.csv",)


# -- main() flow ---------------------------------------------------------


def test_main_discovery_mode_writes_skeleton_and_returns_none(tmp_path: Path):
    _write_csv(tmp_path / "x.csv", "a\n1\n")
    out = main([file_source(str(tmp_path))], output_dir=tmp_path)
    assert out is None
    skeletons = list(tmp_path.glob("mdw_sources_*.py"))
    assert len(skeletons) == 1


def test_main_loads_sidecar_and_runs_pipeline(tmp_path: Path):
    _write_csv(tmp_path / "data.csv", "x\n1\n2\n3\n4\n5\n6\n")
    sidecar = tmp_path / "mdw_sources_20260427_120000.py"
    sidecar.write_text(
        "SOURCES = [\n"
        f'    file_source(path={str(tmp_path)!r}, include=("data.csv",)),\n'
        "]\n",
        encoding="utf-8",
    )
    # Pass an empty in-script sources list -- sidecar overrides it.
    result = main([], output_dir=tmp_path, seed=0)
    assert result is not None
    assert result["sources"][0]["source_name"] == "data.csv"


def test_main_rejects_sidecar_with_discovery_source(tmp_path: Path):
    sidecar = tmp_path / "mdw_sources_20260427_120000.py"
    sidecar.write_text(
        f"SOURCES = [file_source(path={str(tmp_path)!r})]\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="discovery-mode source"):
        main([], output_dir=tmp_path)


# -- deterministic classification sample (#18) ---------------------------


def _classify_with_seed(tmp_path: Path, csv_name: str, seed: int) -> dict[str, str]:
    """Run extract on a fixture and return {column_name: inferred_type}."""
    out = tmp_path / f"stats_{seed}.json"
    src = file_source(str(tmp_path), include=[csv_name])
    result = run_extract([src], out, seed=0, classifier_seed=seed)
    src_out = result["sources"][0]
    return {c["column_name"]: c["inferred_type"] for c in src_out["columns"]}


def test_classifier_sample_is_deterministic_across_reruns(tmp_path: Path):
    # 1500 rows so the 1000-row sample actually has to choose.
    rows = ["mixed,kommun"]
    for i in range(1, 1301):
        rows.append(f"{i},0114")
    for i in range(1301, 1501):
        rows.append(f"x{i},0115")
    _write_csv(tmp_path / "data.csv", "\n".join(rows) + "\n")

    classifications_a = _classify_with_seed(tmp_path, "data.csv", seed=0)
    classifications_b = _classify_with_seed(tmp_path, "data.csv", seed=0)
    assert classifications_a == classifications_b, (
        "same fixture + same seed must produce identical classifications"
    )


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

    src = file_source(str(tmp_path), include=["people.csv"])

    monkeypatch.delenv("MDW_MEMORY_THRESHOLD_MB", raising=False)
    table_path = tmp_path / "stats_table.json"
    table_result = run_extract([src], table_path, seed=0)

    monkeypatch.setenv("MDW_MEMORY_THRESHOLD_MB", "0")
    view_path = tmp_path / "stats_view.json"
    view_result = run_extract([src], view_path, seed=0)

    table_result.pop("generated_at", None)
    view_result.pop("generated_at", None)
    assert table_result == view_result


def test_classifier_seed_is_threaded_through_main(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    result = main([src], output_dir=tmp_path, seed=0, classifier_seed=42)
    assert result is not None
    assert result["sources"][0]["row_count"] == 6


# -- mdw_config.json overrides (#19) -------------------------------------


def _write_mdw_config(tmp_path: Path, payload: dict) -> None:
    (tmp_path / "mdw_config.json").write_text(json.dumps(payload), encoding="utf-8")


def test_extract_stamps_source_of_type_auto_without_config(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,age\n1,20\n2,30\n3,40\n4,50\n5,60\n6,70\n",
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    result = run_extract([src], tmp_path / "stats.json", seed=0)
    cols = {c["column_name"]: c for c in result["sources"][0]["columns"]}
    assert all(c["source_of_type"] == "auto" for c in cols.values())


def test_extract_honors_column_type_override(tmp_path: Path):
    _write_csv(
        tmp_path / "data.csv",
        "lopnr,name\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n",
    )
    _write_mdw_config(
        tmp_path,
        {
            "version": 1,
            "column_types": {
                "data.csv": {"name": {"type": "categorical"}},
            },
        },
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    result = main([src], output_dir=tmp_path, seed=0)
    assert result is not None
    cols = {c["column_name"]: c for c in result["sources"][0]["columns"]}
    assert cols["name"]["inferred_type"] == "categorical"
    assert cols["name"]["source_of_type"] == "override"
    assert cols["lopnr"]["source_of_type"] == "auto"


def test_extract_inline_hint_skips_sample(tmp_path: Path, monkeypatch):
    """Inline subtype hint -> _sample_values is NOT called for that column."""
    from mock_data_wizard import extract

    _write_csv(
        tmp_path / "data.csv",
        "lopnr,name\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n",
    )
    _write_mdw_config(
        tmp_path,
        {
            "version": 1,
            "column_types": {
                "data.csv": {
                    "name": {"type": "id", "id_subtype": "string"},
                    "lopnr": {"type": "id", "id_subtype": "integer"},
                },
            },
        },
    )

    sample_calls: list[str] = []
    real_sample = extract._sample_values

    def spy(conn, table, col, dialect, *args, **kwargs):
        sample_calls.append(col)
        return real_sample(conn, table, col, dialect, *args, **kwargs)

    monkeypatch.setattr(extract, "_sample_values", spy)

    src = file_source(str(tmp_path), include=["data.csv"])
    result = main([src], output_dir=tmp_path, seed=0)
    assert result is not None
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
    _write_mdw_config(
        tmp_path,
        {
            "version": 1,
            "column_types": {"data.csv": {"name": {"type": "id"}}},
        },
    )

    sample_calls: list[str] = []
    real_sample = extract._sample_values

    def spy(conn, table, col, dialect, *args, **kwargs):
        sample_calls.append(col)
        return real_sample(conn, table, col, dialect, *args, **kwargs)

    monkeypatch.setattr(extract, "_sample_values", spy)

    src = file_source(str(tmp_path), include=["data.csv"])
    main([src], output_dir=tmp_path, seed=0)
    assert "name" in sample_calls
    assert "lopnr" in sample_calls


def test_main_raises_on_invalid_mdw_config(tmp_path: Path):
    _write_csv(tmp_path / "data.csv", "x\n1\n")
    _write_mdw_config(
        tmp_path,
        {"version": 1, "column_types": {"data.csv": {"x": {"type": "blob"}}}},
    )
    src = file_source(str(tmp_path), include=["data.csv"])
    with pytest.raises(ValueError, match="expected one of"):
        main([src], output_dir=tmp_path, seed=0)
