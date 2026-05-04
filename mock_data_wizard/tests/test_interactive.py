"""Tests for the Phase 1 interactive default flow.

Stage detection is pure-input → pure-output; the per-stage helpers are
exercised by patching ``builtins.input`` with a canned-answer iterator
(matches how the configure tests stub regmeta).
"""

from __future__ import annotations

import ast
import json
from argparse import Namespace
from pathlib import Path

import pytest

from mock_data_wizard import interactive
from mock_data_wizard.interactive import (
    Stage,
    _detect_stage,
    _render_configure_body,
)

from .conftest import MINIMAL_STATS


# -- _detect_stage matrix --------------------------------------------------


def test_detect_stage_empty_dir(tmp_path: Path):
    assert _detect_stage(tmp_path) is Stage.BUILD


def test_detect_stage_bundle_only(tmp_path: Path):
    (tmp_path / "mock_data_wizard_extract.py").write_text("# stub", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.DISCOVER_INSTRUCTIONS


def test_detect_stage_discover(tmp_path: Path):
    (tmp_path / "mock_data_wizard_extract.py").write_text("# stub", encoding="utf-8")
    (tmp_path / "discover.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.CONFIGURE


def test_detect_stage_discover_without_bundle(tmp_path: Path):
    """Bundle absence at later stages is fine — the user may have only
    kept the JSON artifacts after a copy-back from MONA."""
    (tmp_path / "discover.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.CONFIGURE


def test_detect_stage_config(tmp_path: Path):
    (tmp_path / "discover.json").write_text("{}", encoding="utf-8")
    (tmp_path / "mdw_config.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.EXTRACT_INSTRUCTIONS


def test_detect_stage_stats(tmp_path: Path):
    (tmp_path / "stats.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.GENERATE


def test_detect_stage_done_requires_populated_mock_data(tmp_path: Path):
    (tmp_path / "stats.json").write_text("{}", encoding="utf-8")
    (tmp_path / "mock_data").mkdir()
    # Empty mock_data/ directory does NOT advance to DONE.
    assert _detect_stage(tmp_path) is Stage.GENERATE
    (tmp_path / "mock_data" / "x.csv").write_text("a", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.DONE


# -- _render_configure_body ------------------------------------------------


def test_render_configure_body_sql_only():
    body = _render_configure_body(dsn="P1105")
    assert body == (
        "def configure():\n    return [\n        sql_source(dsn='P1105'),\n    ]"
    )
    ast.parse(body)


def test_render_configure_body_file_only():
    body = _render_configure_body(file_path="/data/csvs")
    assert "file_source(path='/data/csvs')" in body
    ast.parse(body)


def test_render_configure_body_both():
    body = _render_configure_body(dsn="P1105", file_path="/data")
    assert "sql_source(dsn='P1105')" in body
    assert "file_source(path='/data')" in body
    ast.parse(body)


def test_render_configure_body_unc_path_round_trips():
    """UNC paths with backslashes and dollar-signs must survive ``repr``
    quoting and re-parse as a Python string literal."""
    unc = r"\\micro.intra\projekt\P1105$\P1105_Data"
    body = _render_configure_body(file_path=unc)
    tree = ast.parse(body)
    # Walk the AST: find the file_source call's path kwarg, assert its
    # constant value matches the input verbatim.
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            for kw in node.keywords:
                if (
                    kw.arg == "path"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, str)
                ):
                    found.append(kw.value.value)
    assert unc in found


def test_render_configure_body_neither_raises():
    with pytest.raises(ValueError, match="at least one"):
        _render_configure_body()


# -- Stage 1: build bundle -------------------------------------------------


def _canned_inputs(monkeypatch, answers: list[str]) -> None:
    it = iter(answers)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(it))


def _extract_configure_body(src: str) -> str:
    """Pull just the user-edited ``configure()`` function out of the bundle.

    The bundle also defines ``sql_source`` / ``file_source`` (constructors
    from sources.py), so substring checks against the whole bundle would
    false-match. AST-extracting the configure body lets us assert against
    the user's choices only.
    """
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "configure":
            return ast.unparse(node)
    raise AssertionError("no configure() function in bundle")


def test_stage1_build_writes_bundle_with_dsn(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "y",  # SQL source? yes
            "P1105",  # DSN
            "n",  # file source? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    bundle = tmp_path / "mock_data_wizard_extract.py"
    assert bundle.exists()
    src = bundle.read_text(encoding="utf-8")
    body = _extract_configure_body(src)
    assert "sql_source(dsn='P1105')" in body
    assert "file_source" not in body


def test_stage1_build_with_file_source(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "n",  # SQL source? no
            "y",  # file source? yes
            r"\\micro.intra\projekt\P1105$\P1105_Data",
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    src = (tmp_path / "mock_data_wizard_extract.py").read_text(encoding="utf-8")
    body = _extract_configure_body(src)
    assert "sql_source" not in body
    assert "file_source(path=" in body
    assert "P1105_Data" in body


def test_stage1_aborts_when_no_sources(tmp_path: Path, monkeypatch, capsys):
    _canned_inputs(monkeypatch, ["n", "n"])
    rc = interactive._stage1_build(tmp_path)
    assert rc == 1
    assert not (tmp_path / "mock_data_wizard_extract.py").exists()
    assert "at least one source" in capsys.readouterr().err.lower()


def test_stage1_refuses_to_overwrite_without_confirm(tmp_path: Path, monkeypatch):
    bundle = tmp_path / "mock_data_wizard_extract.py"
    bundle.write_text("# user's hand-edited bundle", encoding="utf-8")
    original = bundle.read_bytes()
    _canned_inputs(
        monkeypatch,
        [
            "y",  # SQL source? yes
            "P1105",  # DSN
            "n",  # file source? no
            "n",  # rebuild? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 1
    assert bundle.read_bytes() == original


# -- Stage 3: configure ----------------------------------------------------


def _write_discover(tmp_path: Path, sources: list[dict]) -> Path:
    p = tmp_path / "discover.json"
    p.write_text(
        json.dumps({"contract_version": "discover-1.0.0", "sources": sources}),
        encoding="utf-8",
    )
    return p


def test_stage3_configure_no_register(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "Kommun", "sql_type": "char(4)"},
                ],
            }
        ],
    )
    _canned_inputs(monkeypatch, [""])  # skip register prompt
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    config = tmp_path / "mdw_config.json"
    assert config.exists()
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["lisa_2018"]["LopNr"] == {"type": "id"}
    assert payload["column_types"]["lisa_2018"]["Kommun"] == {"type": "categorical"}


def test_stage3_aborts_on_existing_config(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [{"source_name": "a", "columns": [{"name": "x", "sql_type": "int"}]}],
    )
    config = tmp_path / "mdw_config.json"
    config.write_text("{}", encoding="utf-8")
    _canned_inputs(
        monkeypatch,
        ["", "n"],  # register: skip, overwrite: no
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 1
    assert config.read_text(encoding="utf-8") == "{}"  # untouched


def test_stage3_overwrites_when_confirmed(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [{"source_name": "a", "columns": [{"name": "lopnr", "sql_type": "int"}]}],
    )
    config = tmp_path / "mdw_config.json"
    config.write_text("{}", encoding="utf-8")
    _canned_inputs(
        monkeypatch,
        ["", "y"],  # register: skip, overwrite: yes
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


# -- Stage 2 / 4: instructions only ----------------------------------------


def test_stage2_prints_discover_instructions(tmp_path: Path, monkeypatch, capsys):
    (tmp_path / "mock_data_wizard_extract.py").write_text("# stub", encoding="utf-8")
    _canned_inputs(monkeypatch, ["n"])  # don't rebuild
    rc = interactive._stage2_instructions(tmp_path)
    assert rc == 0
    out = capsys.readouterr().out
    assert "mock_data_wizard_extract.py" in out
    assert 'MODE = "discover"' in out
    assert "discover.json" in out


def test_stage4_prints_extract_instructions(tmp_path: Path, capsys):
    (tmp_path / "mdw_config.json").write_text("{}", encoding="utf-8")
    rc = interactive._stage4_instructions(tmp_path)
    assert rc == 0
    out = capsys.readouterr().out
    assert "mdw_config.json" in out
    assert 'MODE = "extract"' in out
    assert "stats.json" in out


# -- Stage 5: dispatch to _cmd_generate ------------------------------------


def test_stage5_dispatches_to_generate(tmp_path: Path, monkeypatch):
    """Stage 5 should construct a Namespace with Phase 1 defaults and
    call ``_cmd_generate`` once. Mock the dispatch target to capture
    the call args without exercising the full generate pipeline."""
    (tmp_path / "stats.json").write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0
    args = captured["args"]
    assert args.stats == str(tmp_path / "stats.json")
    assert args.seed == 42
    assert args.sample_pct == 1.0
    assert args.output_dir == str(tmp_path / "mock_data")
    assert args.no_regmeta is False
    assert args.register is None
    assert args.yes is False
    assert args.force is False
    assert args.verbose is False


# -- cli.main no-args dispatch --------------------------------------------


def test_main_no_args_non_tty_falls_back_to_help(monkeypatch, capsys):
    """Piped stdin (or redirected stderr) → print help, do not enter
    interactive flow."""
    from mock_data_wizard import cli as cli_mod

    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)

    sentinel: dict = {"called": False}

    def boom(_cwd):  # pragma: no cover - must not be called
        sentinel["called"] = True
        return 99

    monkeypatch.setattr("mock_data_wizard.interactive.run", boom, raising=True)

    rc = cli_mod.main([])
    assert rc == 0
    assert sentinel["called"] is False
    assert "usage:" in capsys.readouterr().out


def test_main_no_args_tty_invokes_interactive(monkeypatch):
    from mock_data_wizard import cli as cli_mod

    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)

    captured: dict = {}

    def fake_run(cwd: Path) -> int:
        captured["cwd"] = cwd
        return 0

    monkeypatch.setattr("mock_data_wizard.interactive.run", fake_run, raising=True)
    rc = cli_mod.main([])
    assert rc == 0
    assert captured["cwd"] == Path.cwd()


def test_main_no_interactive_flag_prints_help_on_tty(monkeypatch, capsys):
    from mock_data_wizard import cli as cli_mod

    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)

    sentinel: dict = {"called": False}

    def boom(_cwd):  # pragma: no cover - must not be called
        sentinel["called"] = True
        return 99

    monkeypatch.setattr("mock_data_wizard.interactive.run", boom, raising=True)

    rc = cli_mod.main(["--no-interactive"])
    assert rc == 0
    assert sentinel["called"] is False
    assert "usage:" in capsys.readouterr().out
