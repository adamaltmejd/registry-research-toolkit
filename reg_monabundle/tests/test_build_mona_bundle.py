"""Tests for the MONA bundle amalgamator.

Runs the bundler end-to-end into a tmp dir, parses the result, and
spawns a subprocess that runs the bundle against a tiny CSV. Verifies
mock_data_stats.json comes out with the expected shape.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

import reg_monabundle

if TYPE_CHECKING:
    from pathlib import Path


def _build_bundle_to(out_path: Path) -> Path:
    # ``build_bundle`` defaults ``runtime_pkg_dir`` /
    # ``runtime_module_order`` to ``reg_monabundle.runtime`` post-2c.
    return reg_monabundle.build_bundle(out_path)


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
        assert "from reg_monabundle" not in s, f"package import leaked: {line!r}"
        assert "from mock_data_wizard" not in s, f"package import leaked: {line!r}"


def test_bundle_carries_no_pydantic_or_reg_schema(tmp_path: Path):
    """§9.6 boundary gate: the MONA bundle must carry NO pydantic and NO
    reg_schema — neither as source text nor as a live AST import.

    This is the real §9.6 CI gate. It would have caught the A3.1
    transient (reg_schema pulled in Pydantic via the amalgamated
    ``project_data`` slice). Note the subprocess ``_run_bundle`` tests
    run in a Pydantic-having env, so they do NOT prove pydantic-free; the
    source-scan + AST check below is the proof.

    Both an embedded-spec bundle and a sidecar bundle are checked — the
    embedded path serializes a validated spec into the bundle, the
    sidecar path doesn't; neither may reintroduce a forbidden import.
    """
    from _project_data_fixtures import make_project_data

    project_data = make_project_data(
        sources=[
            {
                "name": "data.csv",
                "bindings": [
                    {"display_name": "lopnr", "type": "id", "id_subtype": "integer"},
                ],
            }
        ]
    )
    for label, kwargs in (
        ("sidecar", {}),
        ("embedded", {"project_data": project_data}),
    ):
        out = reg_monabundle.build_bundle(tmp_path / f"{label}.py", **kwargs)
        src = out.read_text(encoding="utf-8")

        # 1. Line scan — catches the import in any form, incl. inside a
        # ``try:``/aliased/multi-name statement that the AST walk below
        # also covers, but the raw text is the most legible failure.
        for line in src.splitlines():
            s = line.lstrip()
            assert not s.startswith("import pydantic"), (
                f"[{label}] pydantic import leaked: {line!r}"
            )
            assert not s.startswith("from pydantic"), (
                f"[{label}] pydantic import leaked: {line!r}"
            )
            assert not s.startswith("import reg_schema"), (
                f"[{label}] reg_schema import leaked: {line!r}"
            )
            assert not s.startswith("from reg_schema"), (
                f"[{label}] reg_schema import leaked: {line!r}"
            )

        # 2. AST import check — the structural proof. Walks every
        # Import / ImportFrom node (any nesting depth) and asserts no
        # module is pydantic or reg_schema (exact or dotted submodule).
        tree = ast.parse(src)
        forbidden: list[str] = []
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
            for mod in modules:
                if mod in ("pydantic", "reg_schema") or mod.startswith(
                    ("pydantic.", "reg_schema.")
                ):
                    forbidden.append(mod)
        assert not forbidden, (
            f"[{label}] bundle carries forbidden imports {sorted(set(forbidden))} "
            f"— the §9.6 boundary requires the bundle to be free of pydantic "
            f"and reg_schema. Validation is the build-time gate "
            f"(spec_loader.validate_project_data); the runtime deserializes "
            f"via loadedspec_from_dict."
        )


def test_bundle_defines_every_dropped_relative_import_target(tmp_path: Path):
    """Catch the class of bug where the slicer drops a relative import
    (``from ._util import foo``) but the source module isn't in
    ``DEFAULT_RUNTIME_MODULE_ORDER`` — the bundle then calls ``foo``
    without a top-level definition and crashes with ``NameError`` if
    that branch ever runs.

    Walks each runtime source module, collects every name pulled in
    via ``from .<x> import …``, then verifies those names all appear
    as top-level definitions in the bundle AST. The slicer drops the
    relative import either way, so the failure mode is silent without
    this check.
    """
    from reg_monabundle.build import DEFAULT_RUNTIME_DIR, DEFAULT_RUNTIME_MODULE_ORDER

    expected: set[str] = set()
    for mod in DEFAULT_RUNTIME_MODULE_ORDER:
        mod_tree = ast.parse(
            (DEFAULT_RUNTIME_DIR / f"{mod}.py").read_text(encoding="utf-8")
        )
        for node in ast.walk(mod_tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.level > 0
                and not (
                    # TYPE_CHECKING-only imports never execute and don't
                    # need a bundle definition.
                    any(
                        isinstance(p, ast.If)
                        and isinstance(p.test, ast.Name)
                        and p.test.id == "TYPE_CHECKING"
                        for p in ast.walk(mod_tree)
                        if isinstance(p, ast.If) and node in ast.walk(p)
                    )
                )
            ):
                for alias in node.names:
                    expected.add(alias.asname or alias.name)

    out = _build_bundle_to(tmp_path / "bundle.py")
    tree = ast.parse(out.read_text(encoding="utf-8"))

    top_level: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            top_level.add(node.name)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    top_level.add(tgt.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            top_level.add(node.target.id)

    missing = sorted(expected - top_level)
    assert not missing, (
        f"bundle references names from dropped relative imports that have "
        f"no top-level definition: {missing}. The source module is in "
        f"``DEFAULT_RUNTIME_MODULE_ORDER`` but the module it pulls from "
        f"(e.g. ``_util``) is not — add it to the order or inline the helpers."
    )


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
    """MODE=discover -> mock_data_discovery.json with metadata only (no inferred_type)."""
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

    discover_path = tmp_path / "mock_data_discovery.json"
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


def test_bundle_extract_mode_writes_stats_from_project_data(tmp_path: Path):
    """MODE=extract reads project_data.json from the bundle directory
    (sidecar mode) and emits typed mock_data_stats.json."""
    from _project_data_fixtures import make_project_data, write_project_data

    bundle = _build_bundle_to(tmp_path / "mdw_runner.py")
    _patch_configure(bundle)

    (tmp_path / "data.csv").write_text(
        "lopnr,age,kommun\n"
        "1,25,0114\n2,30,0114\n3,42,0115\n4,55,0114\n"
        "5,29,0115\n6,38,0114\n7,47,0115\n8,33,0114\n",
        encoding="utf-8",
    )
    write_project_data(
        tmp_path,
        make_project_data(
            sources=[
                {
                    "name": "data.csv",
                    "bindings": [
                        {
                            "display_name": "lopnr",
                            "type": "id",
                            "id_subtype": "integer",
                        },
                        {
                            "display_name": "age",
                            "type": "numeric",
                            "numeric_subtype": "integer",
                        },
                        {"display_name": "kommun", "type": "categorical"},
                    ],
                }
            ]
        ),
    )

    _run_bundle(bundle, cwd=tmp_path, mode="extract")
    stats_path = tmp_path / "mock_data_stats.json"
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


def test_bundle_extract_mode_embedded_project_data(tmp_path: Path):
    """MODE=extract with an embedded project_data.json wins over any
    sidecar — the runner parses _PROJECT_DATA_JSON and hands a
    LoadedSpec straight to extract.main()."""
    from _project_data_fixtures import make_project_data

    project_data = make_project_data(
        sources=[
            {
                "name": "data.csv",
                "bindings": [
                    {"display_name": "lopnr", "type": "id", "id_subtype": "integer"},
                    {
                        "display_name": "age",
                        "type": "numeric",
                        "numeric_subtype": "integer",
                    },
                ],
            }
        ]
    )
    bundle = reg_monabundle.build_bundle(
        tmp_path / "mdw_runner.py",
        project_data=project_data,
    )
    _patch_configure(bundle)

    (tmp_path / "data.csv").write_text(
        "lopnr,age\n1,25\n2,30\n3,42\n4,55\n5,29\n6,38\n7,47\n8,33\n",
        encoding="utf-8",
    )
    # NO sidecar — embedded must be enough.
    _run_bundle(bundle, cwd=tmp_path, mode="extract")
    stats = json.loads((tmp_path / "mock_data_stats.json").read_text("utf-8"))
    by_name = {c["column_name"]: c for c in stats["sources"][0]["columns"]}
    assert by_name["lopnr"]["inferred_type"] == "id"
    assert by_name["age"]["inferred_type"] == "numeric"
