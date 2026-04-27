"""Tests for the MONA bundle amalgamator.

Runs the bundler end-to-end into a tmp dir, parses the result, and
spawns a subprocess that runs the bundle against a tiny CSV. Verifies
stats.json comes out with the expected shape.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = REPO_ROOT / "mock_data_wizard" / "scripts" / "build_mona_bundle.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("bmb", BUILDER_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_bundle_to(out_path: Path) -> Path:
    return _load_builder().build_bundle(out_path)


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
    for required in ("file_source", "sql_source", "main", "run_extract"):
        assert required in names, f"missing top-level def: {required}"


def test_bundle_has_runner_block(tmp_path: Path):
    out = _build_bundle_to(tmp_path / "bundle.py")
    text = out.read_text(encoding="utf-8")
    assert "SOURCES = [" in text
    assert 'if __name__ == "__main__":' in text
    assert "main(SOURCES, output_dir=" in text
    # MBS-batch stdout footgun mitigation must be present
    assert 'socket.gethostname().upper().startswith("MBS")' in text


def test_bundle_does_not_carry_intra_package_imports(tmp_path: Path):
    out = _build_bundle_to(tmp_path / "bundle.py")
    text = out.read_text(encoding="utf-8")
    # No 'from .X import Y'
    for line in text.splitlines():
        s = line.lstrip()
        assert not s.startswith("from ."), f"intra-pkg import leaked: {line!r}"
        assert "from mock_data_wizard" not in s, f"package import leaked: {line!r}"


def test_bundle_runs_against_a_real_csv(tmp_path: Path):
    """Spawn the bundle as a subprocess and check stats.json comes out."""
    bundle = _build_bundle_to(tmp_path / "mock_data_wizard_extract.py")

    # Patch SOURCES = [ ... examples ... ] with one real file_source
    text = bundle.read_text(encoding="utf-8")
    patched = text.replace(
        "SOURCES = [\n    # Examples",
        "SOURCES = [\n    file_source(path=str(Path(__file__).resolve().parent), "
        'include=("data.csv",)),\n    # Examples',
        1,
    )
    assert patched != text, "SOURCES patch did not apply"
    bundle.write_text(patched, encoding="utf-8")

    (tmp_path / "data.csv").write_text(
        "lopnr,age,kommun\n"
        "1,25,0114\n2,30,0114\n3,42,0115\n4,55,0114\n"
        "5,29,0115\n6,38,0114\n7,47,0115\n8,33,0114\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(bundle)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        pytest.fail(
            f"bundle exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    stats_path = tmp_path / "stats.json"
    assert stats_path.exists(), f"stats.json not produced; stderr:\n{result.stderr}"
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    assert stats["contract_version"] == "2.0.0"
    src = stats["sources"][0]
    assert src["source_name"] == "data.csv"
    assert src["row_count"] == 8
    by_name = {c["column_name"]: c for c in src["columns"]}
    assert by_name["lopnr"]["inferred_type"] == "id"
    assert by_name["age"]["inferred_type"] == "numeric"
    assert by_name["kommun"]["inferred_type"] == "categorical"
