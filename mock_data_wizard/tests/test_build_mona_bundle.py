"""Tests for the MONA bundle amalgamator.

Runs the bundler end-to-end into a tmp dir, parses the result, and
spawns a subprocess that runs the bundle against a tiny CSV. Verifies
mdw_step3_stats.json comes out with the expected shape.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from mock_data_wizard import _bundle


def _build_bundle_to(out_path: Path) -> Path:
    return _bundle.build_bundle(out_path)


def test_bundle_parses_as_python(tmp_path: Path):
    out = _build_bundle_to(tmp_path / "bundle.py")
    src = out.read_text(encoding="utf-8")
    ast.parse(src)
    assert "from __future__ import annotations" in src
    assert src.count("from __future__ import annotations") == 1


def test_bundle_exposes_expected_top_level_names(tmp_path: Path):
    out = _build_bundle_to(tmp_path / "bundle.py")
    tree = ast.parse(out.read_text(encoding="utf-8"))
    names = {
        n.name
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef))
    }
    # The user-facing surface that the runner block calls
    for required in (
        "file_source",
        "sql_source",
        "main",
        "run_discover",
        "run_extract_typed",
    ):
        assert required in names, f"missing top-level def: {required}"


def test_bundle_has_runner_block(tmp_path: Path):
    out = _build_bundle_to(tmp_path / "bundle.py")
    text = out.read_text(encoding="utf-8")
    assert "def configure():" in text
    assert 'if __name__ == "__main__":' in text
    assert "SOURCES = configure()" in text
    assert "result = main(" in text
    # MODE is env-var-overridable; check the assignment shape.
    assert 'MDW_MODE", "discover"' in text
    # MBS-batch stdout footgun mitigation must be present
    assert '.upper().startswith("MBS")' in text


def test_bundle_configure_lives_above_module_bodies(tmp_path: Path):
    """The user-edited block must come before any 'from sources.py' code so
    it's the first thing visible when the file is opened."""
    out = _build_bundle_to(tmp_path / "bundle.py")
    text = out.read_text(encoding="utf-8")
    configure_pos = text.index("def configure():")
    classify_pos = text.index("# classify.py")
    assert configure_pos < classify_pos, (
        "configure() must be at the top, before the bundled module bodies"
    )


def test_bundle_does_not_carry_intra_package_imports(tmp_path: Path):
    out = _build_bundle_to(tmp_path / "bundle.py")
    text = out.read_text(encoding="utf-8")
    # No 'from .X import Y'
    for line in text.splitlines():
        s = line.lstrip()
        assert not s.startswith("from ."), f"intra-pkg import leaked: {line!r}"
        assert "from mock_data_wizard" not in s, f"package import leaked: {line!r}"


def _patch_configure(bundle: Path) -> None:
    text = bundle.read_text(encoding="utf-8")
    patched = text.replace(
        "def configure():\n    return []",
        "def configure():\n    return [\n        "
        "file_source(path=str(Path(__file__).resolve().parent), "
        'include=("data.csv",))\n    ]',
        1,
    )
    assert patched != text, "configure() patch did not apply"
    bundle.write_text(patched, encoding="utf-8")


def _run_bundle(
    bundle: Path, cwd: Path, *, mode: str | None = None
) -> subprocess.CompletedProcess:
    env = None
    if mode is not None:
        env = {**os.environ, "MDW_MODE": mode}
    result = subprocess.run(
        [sys.executable, str(bundle)],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    if result.returncode != 0:
        pytest.fail(
            f"bundle exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def test_bundle_discover_mode_writes_discover_json(tmp_path: Path):
    """MODE=discover -> mdw_step1_discovery.json with metadata only (no inferred_type)."""
    bundle = _build_bundle_to(tmp_path / "mdw_runner.py")
    _patch_configure(bundle)
    # MODE = "discover" is the default; no patch needed.

    (tmp_path / "data.csv").write_text(
        "lopnr,age,kommun\n"
        "1,25,0114\n2,30,0114\n3,42,0115\n4,55,0114\n"
        "5,29,0115\n6,38,0114\n7,47,0115\n8,33,0114\n",
        encoding="utf-8",
    )
    _run_bundle(bundle, cwd=tmp_path)

    discover_path = tmp_path / "mdw_step1_discovery.json"
    assert discover_path.exists()
    discover = json.loads(discover_path.read_text(encoding="utf-8"))
    assert discover["contract_version"] == "discover-1.0.0"
    src = discover["sources"][0]
    assert src["source_name"] == "data.csv"
    assert src["row_count"] == 8
    col_names = [c["name"] for c in src["columns"]]
    assert col_names == ["lopnr", "age", "kommun"]
    # Metadata only -- no inferred_type / stats present
    for col in src["columns"]:
        assert "inferred_type" not in col
        assert "sql_type" in col


def test_bundle_extract_mode_writes_stats_from_config(tmp_path: Path):
    """MODE=extract reads mdw_step2_config.json and emits typed mdw_step3_stats.json."""
    bundle = _build_bundle_to(tmp_path / "mdw_runner.py")
    _patch_configure(bundle)

    (tmp_path / "data.csv").write_text(
        "lopnr,age,kommun\n"
        "1,25,0114\n2,30,0114\n3,42,0115\n4,55,0114\n"
        "5,29,0115\n6,38,0114\n7,47,0115\n8,33,0114\n",
        encoding="utf-8",
    )
    (tmp_path / "mdw_step2_config.json").write_text(
        json.dumps(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_types": {
                    "data.csv": {
                        "lopnr": {"type": "id", "id_subtype": "integer"},
                        "age": {"type": "numeric", "numeric_subtype": "integer"},
                        "kommun": {"type": "categorical"},
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    _run_bundle(bundle, cwd=tmp_path, mode="extract")
    stats_path = tmp_path / "mdw_step3_stats.json"
    assert stats_path.exists()
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    assert stats["contract_version"] == "2.0.0"
    src = stats["sources"][0]
    assert src["row_count"] == 8
    by_name = {c["column_name"]: c for c in src["columns"]}
    assert by_name["lopnr"]["inferred_type"] == "id"
    assert by_name["age"]["inferred_type"] == "numeric"
    assert by_name["kommun"]["inferred_type"] == "categorical"
    assert all(c["source_of_type"] == "override" for c in by_name.values())
