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
