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
    _normalize_project_number,
    _render_configure_body,
)

from .conftest import MINIMAL_STATS


# -- _detect_stage matrix --------------------------------------------------


def test_detect_stage_empty_dir(tmp_path: Path):
    assert _detect_stage(tmp_path) is Stage.BUILD


def test_detect_stage_bundle_only(tmp_path: Path):
    (tmp_path / "mdw_runner.py").write_text("# stub", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.DISCOVER_INSTRUCTIONS


def test_detect_stage_discover(tmp_path: Path):
    (tmp_path / "mdw_runner.py").write_text("# stub", encoding="utf-8")
    (tmp_path / "mdw_step1_discovery.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.CONFIGURE


def test_detect_stage_discover_without_bundle(tmp_path: Path):
    """Bundle absence at later stages is fine — the user may have only
    kept the JSON artifacts after a copy-back from MONA."""
    (tmp_path / "mdw_step1_discovery.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.CONFIGURE


def test_detect_stage_config(tmp_path: Path):
    (tmp_path / "mdw_step1_discovery.json").write_text("{}", encoding="utf-8")
    (tmp_path / "mdw_step2_config.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.EXTRACT_INSTRUCTIONS


def test_detect_stage_stats(tmp_path: Path):
    (tmp_path / "mdw_step3_stats.json").write_text("{}", encoding="utf-8")
    assert _detect_stage(tmp_path) is Stage.GENERATE


def test_detect_stage_done_requires_populated_mock_data(tmp_path: Path):
    (tmp_path / "mdw_step3_stats.json").write_text("{}", encoding="utf-8")
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
    body = _render_configure_body(file_paths=["/data/csvs"])
    # file_source calls in the wizard-generated bundle pin
    # encoding='latin-1' explicitly — DESIGN.md § MONA upload, the
    # batch host's locale.getpreferredencoding() is cp1252.
    assert "file_source(path='/data/csvs', encoding='latin-1')" in body
    ast.parse(body)


def test_render_configure_body_both():
    body = _render_configure_body(dsn="P1105", file_paths=["/data"])
    assert "sql_source(dsn='P1105')" in body
    assert "file_source(path='/data', encoding='latin-1')" in body
    ast.parse(body)


def test_render_configure_body_multiple_file_paths():
    body = _render_configure_body(file_paths=["/data/a", "/data/b", "/data/c"])
    # Each path should appear as its own file_source(...) call, in order.
    a = body.index("file_source(path='/data/a', encoding='latin-1')")
    b = body.index("file_source(path='/data/b', encoding='latin-1')")
    c = body.index("file_source(path='/data/c', encoding='latin-1')")
    assert a < b < c
    ast.parse(body)


def test_render_configure_body_pins_latin1_encoding():
    """Every emitted ``file_source(...)`` must carry
    ``encoding='latin-1'``. The bundle is built for MONA, where SCB
    CSVs are cp1252; ``file_source``'s own default of utf-8 is wrong
    there (silently fails on `Födelseår` / `Län` etc)."""
    body = _render_configure_body(file_paths=["/a", "/b"])
    tree = ast.parse(body)
    encodings: list[str] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "file_source"
        ):
            continue
        for kw in node.keywords:
            if (
                kw.arg == "encoding"
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, str)
            ):
                encodings.append(kw.value.value)
    assert encodings == ["latin-1", "latin-1"]


def test_render_configure_body_unc_path_round_trips():
    """UNC paths with backslashes and dollar-signs must survive ``repr``
    quoting and re-parse as a Python string literal."""
    unc = r"\\micro.intra\projekt\P1105$\P1105_Data"
    body = _render_configure_body(file_paths=[unc])
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


def _file_source_paths(body: str) -> list[str]:
    """Return all ``path=`` literals passed to ``file_source(...)`` in *body*.

    Parsing the AST avoids tripping on ``ast.unparse`` re-escaping
    backslashes in UNC / Windows paths. Calls are sorted by
    ``(lineno, col_offset)`` so order matches source order regardless
    of ``ast.walk``'s traversal.
    """
    matches: list[tuple[int, int, str]] = []
    tree = ast.parse(body)
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "file_source"
        ):
            continue
        for kw in node.keywords:
            if (
                kw.arg == "path"
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, str)
            ):
                matches.append((node.lineno, node.col_offset, kw.value.value))
    matches.sort()
    return [p for _, _, p in matches]


def test_stage1_build_writes_bundle_with_dsn(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "y",  # SQL source? yes (DSN = project number)
            "n",  # file source? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    bundle = tmp_path / "mdw_runner.py"
    assert bundle.exists()
    src = bundle.read_text(encoding="utf-8")
    body = _extract_configure_body(src)
    assert "sql_source(dsn='P1105')" in body
    assert "file_source" not in body


def test_stage1_build_normalizes_bare_digits_to_p_prefix(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "1105",  # bare digits → normalized to P1105
            "y",  # SQL? yes
            "n",  # file? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    body = _extract_configure_body(
        (tmp_path / "mdw_runner.py").read_text(encoding="utf-8")
    )
    assert "sql_source(dsn='P1105')" in body


def test_stage1_build_custom_dsn(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "c",  # custom DSN
            "MyCustomDSN",  # custom DSN value
            "n",  # file? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    body = _extract_configure_body(
        (tmp_path / "mdw_runner.py").read_text(encoding="utf-8")
    )
    assert "sql_source(dsn='MyCustomDSN')" in body


def test_stage1_build_with_file_source(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "n",  # SQL source? no
            "y",  # file source? yes (uses default UNC for P1105)
            "n",  # add another? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    src = (tmp_path / "mdw_runner.py").read_text(encoding="utf-8")
    body = _extract_configure_body(src)
    assert "sql_source" not in body
    assert _file_source_paths(body) == [r"\\micro.intra\projekt\P1105$\P1105_Data"]


def test_stage1_build_custom_file_path(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "n",  # SQL? no
            "c",  # custom path
            r"D:\some\other\path",
            "n",  # add another? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    body = _extract_configure_body(
        (tmp_path / "mdw_runner.py").read_text(encoding="utf-8")
    )
    assert _file_source_paths(body) == [r"D:\some\other\path"]


def test_stage1_build_multiple_file_paths(tmp_path: Path, monkeypatch):
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "n",  # SQL? no
            "y",  # file source? yes (default UNC)
            "y",  # add another? yes
            r"D:\extra\one",
            "y",  # add another? yes
            r"D:\extra\two",
            "n",  # add another? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    body = _extract_configure_body(
        (tmp_path / "mdw_runner.py").read_text(encoding="utf-8")
    )
    assert _file_source_paths(body) == [
        r"\\micro.intra\projekt\P1105$\P1105_Data",
        r"D:\extra\one",
        r"D:\extra\two",
    ]


def test_stage1_build_no_extra_prompt_when_file_skipped(tmp_path: Path, monkeypatch):
    """Picking ``n`` for the file question must NOT trigger the
    "add another?" loop — otherwise the SQL-only flow stalls waiting
    for input that the canned-input tests don't supply."""
    _canned_inputs(
        monkeypatch,
        [
            "P1105",
            "y",  # SQL: yes
            "n",  # file: no — should skip the add-another loop entirely
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 0
    body = _extract_configure_body(
        (tmp_path / "mdw_runner.py").read_text(encoding="utf-8")
    )
    assert _file_source_paths(body) == []


def test_stage1_aborts_when_no_sources(tmp_path: Path, monkeypatch, capsys):
    _canned_inputs(monkeypatch, ["P1105", "n", "n"])
    rc = interactive._stage1_build(tmp_path)
    assert rc == 1
    assert not (tmp_path / "mdw_runner.py").exists()
    assert "at least one source" in capsys.readouterr().err.lower()


def test_stage1_refuses_to_overwrite_without_confirm(tmp_path: Path, monkeypatch):
    bundle = tmp_path / "mdw_runner.py"
    bundle.write_text("# user's hand-edited bundle", encoding="utf-8")
    original = bundle.read_bytes()
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "y",  # SQL source? yes
            "n",  # file source? no
            "n",  # rebuild? no
        ],
    )
    rc = interactive._stage1_build(tmp_path)
    assert rc == 1
    assert bundle.read_bytes() == original


def test_stage1_force_overwrites_without_prompt(tmp_path: Path, monkeypatch):
    bundle = tmp_path / "mdw_runner.py"
    bundle.write_text("# user's hand-edited bundle", encoding="utf-8")
    _canned_inputs(
        monkeypatch,
        [
            "P1105",  # project number
            "y",  # SQL source? yes
            "n",  # file source? no
            # No rebuild prompt — force=True skips it.
        ],
    )
    rc = interactive._stage1_build(tmp_path, force=True)
    assert rc == 0
    assert bundle.read_bytes() != b"# user's hand-edited bundle"


# -- _normalize_project_number --------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("P1405", "P1405"),
        ("p1405", "P1405"),
        ("1405", "P1405"),
        ("  P1405  ", "P1405"),
        ("0001", "P0001"),
        ("", None),
        ("P12", None),  # too few digits
        ("P12345", None),  # too many digits
        ("12a4", None),
        ("PP1405", None),
        ("project-1405", None),
    ],
)
def test_normalize_project_number(raw: str, expected: str | None):
    assert _normalize_project_number(raw) == expected


# -- Stage 3: configure ----------------------------------------------------


def _write_discover(tmp_path: Path, sources: list[dict]) -> Path:
    p = tmp_path / "mdw_step1_discovery.json"
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
    # No existing config (no overwrite prompt). Single source with no
    # cluster (size < 2) and no time_key column — no panel prompts. Both
    # columns classify cleanly — no ambiguous prompts. Suppress_k is the
    # final yes/no.
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "n",  # suppress_k overrides? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    config = tmp_path / "mdw_step2_config.json"
    assert config.exists()
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["lisa_2018"]["LopNr"] == {"type": "id"}
    assert payload["column_types"]["lisa_2018"]["Kommun"] == {"type": "categorical"}


def test_stage3_aborts_on_existing_config(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [{"source_name": "a", "columns": [{"name": "x", "sql_type": "int"}]}],
    )
    config = tmp_path / "mdw_step2_config.json"
    config.write_text("{}", encoding="utf-8")
    # Overwrite-prompt fires first; declining must return before any
    # other prompt (otherwise we'd burn the user's hand-edited config).
    _canned_inputs(monkeypatch, ["n"])
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 1
    assert config.read_text(encoding="utf-8") == "{}"  # untouched


def test_stage3_overwrites_when_confirmed(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [{"source_name": "a", "columns": [{"name": "lopnr", "sql_type": "int"}]}],
    )
    config = tmp_path / "mdw_step2_config.json"
    config.write_text("{}", encoding="utf-8")
    _canned_inputs(
        monkeypatch,
        [
            "y",  # overwrite: yes
            "",  # register: skip
            "n",  # suppress_k overrides? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


def test_stage3_force_overwrites_without_prompt(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [{"source_name": "a", "columns": [{"name": "lopnr", "sql_type": "int"}]}],
    )
    config = tmp_path / "mdw_step2_config.json"
    config.write_text("{}", encoding="utf-8")
    # force=True skips the overwrite prompt only; the rest of the
    # interview still runs.
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "n",  # suppress_k overrides? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path, force=True)
    assert rc == 0
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


# -- Phase 2 helpers (pure-function unit tests) ---------------------------


@pytest.mark.parametrize(
    "names, expected",
    [
        # Single SCB-style prefix shared across multiple sources, with
        # year suffixes as the SCB-style marker.
        (["lisa_2018", "lisa_2019", "lisa_2020"], "LISA"),
        # `dbo.` and `scb_` prefixes are stripped before matching the
        # stem; either qualifies as an SCB-style marker on its own.
        (["dbo.scb_rams_2020"], "RAMS"),
        (["scb_lisa_2018", "scb_lisa_2019"], "LISA"),
        # No SCB-style marker (no dbo./scb_/year suffix) → no
        # suggestion. A bare `mydata` or `registry_main` looks like a
        # register stem to the regex but isn't one; suppressing avoids
        # pushing a regmeta failure at the user.
        (["mydata"], None),
        (["registry_main"], None),
        # Mixed prefixes → no confident suggestion.
        (["lisa_2018", "rams_2019"], None),
        # A name starting with a digit doesn't match the regex; the
        # whole dataset should fall through to None rather than emit a
        # spurious suggestion from the other sources.
        (["lisa_2018", "2020_extras"], None),
        ([], None),
    ],
)
def test_suggest_register(names: list[str], expected: str | None):
    discover = {"sources": [{"source_name": n, "columns": []} for n in names]}
    assert interactive._suggest_register(discover) == expected


def test_detect_separate_file_panels_returns_clusters_of_size_2_or_more():
    discover = {
        "sources": [
            {"source_name": "lisa_2018", "columns": []},
            {"source_name": "lisa_2019", "columns": []},
            {"source_name": "lisa_2020", "columns": []},
            # Singleton — must not be surfaced.
            {"source_name": "rams_2020", "columns": []},
            # No year suffix — must not be surfaced.
            {"source_name": "spine", "columns": []},
        ]
    }
    clusters = interactive._detect_separate_file_panels(discover)
    assert len(clusters) == 1
    cluster = clusters[0]
    assert cluster["prefix"] == "lisa"
    # Members sorted ascending by period.
    assert [m["period"] for m in cluster["members"]] == [2018, 2019, 2020]
    assert [m["source"] for m in cluster["members"]] == [
        "lisa_2018",
        "lisa_2019",
        "lisa_2020",
    ]


def test_detect_separate_file_panels_handles_separator_variants():
    """Both `lisa_2018` and `lisa-2018` should cluster under `lisa`."""
    discover = {
        "sources": [
            {"source_name": "lisa_2018", "columns": []},
            {"source_name": "lisa-2019", "columns": []},
        ]
    }
    clusters = interactive._detect_separate_file_panels(discover)
    assert len(clusters) == 1
    assert clusters[0]["prefix"] == "lisa"


def test_find_time_key_in_source():
    src = {
        "columns": [
            {"name": "LopNr"},
            {"name": "AR"},
            {"name": "Belopp"},
        ]
    }
    assert interactive._find_time_key_in_source(src) == "AR"

    no_match = {"columns": [{"name": "LopNr"}, {"name": "Kommun"}]}
    assert interactive._find_time_key_in_source(no_match) is None

    # Case-insensitive match — `INDATUM` and `indatum` should both hit.
    upper = {"columns": [{"name": "INDATUM"}]}
    assert interactive._find_time_key_in_source(upper) == "INDATUM"


def test_shared_id_column_returns_unique_match():
    members = [
        {"columns": [{"name": "LopNr"}, {"name": "Kommun"}]},
        {"columns": [{"name": "LopNr"}, {"name": "Yrke"}]},
    ]
    assert interactive._shared_id_column(members) == "LopNr"


def test_shared_id_column_none_when_zero_or_multiple():
    # Zero shared id columns.
    assert (
        interactive._shared_id_column(
            [{"columns": [{"name": "LopNr"}]}, {"columns": [{"name": "Kon"}]}]
        )
        is None
    )
    # Two shared id columns — ambiguous, no default.
    members = [
        {"columns": [{"name": "LopNr"}, {"name": "PersonNr"}]},
        {"columns": [{"name": "LopNr"}, {"name": "PersonNr"}]},
    ]
    assert interactive._shared_id_column(members) is None


def test_ambiguous_columns_picks_kod_typ_and_digit_suffixes():
    payload = {
        "column_types": {
            "lisa_2018": {
                # Ambiguous — `_kod`, `_typ`, trailing digit
                "Yrke_kod": {"type": "high_cardinality"},
                "Niva_typ": {"type": "high_cardinality"},
                "SNI3": {"type": "high_cardinality"},
                # Not ambiguous — already typed.
                "Kommun": {"type": "categorical"},
                "LopNr": {"type": "id"},
                # Plain high_cardinality with no suffix — leave alone.
                "FreeText": {"type": "high_cardinality"},
            }
        }
    }
    out = interactive._ambiguous_columns(payload)
    cols = sorted(c for _, c in out)
    assert cols == ["Niva_typ", "SNI3", "Yrke_kod"]


# -- Stage 3: panel detection interview ------------------------------------


def test_stage3_separate_files_panel_emitted_when_confirmed(
    tmp_path: Path, monkeypatch
):
    """A `lisa_2018`/`lisa_2019` cluster confirmed by the user should
    emit a `panels: [{layout: "separate_files", ...}]` block; the
    panel_key default is the unique shared id column."""
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "LopNr", "sql_type": "int"}],
            },
            {
                "source_name": "lisa_2019",
                "columns": [{"name": "LopNr", "sql_type": "int"}],
            },
        ],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "y",  # treat as panel? yes
            "",  # panel_id (default: lisa)
            "",  # panel_key (default: LopNr)
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["panels"] == [
        {
            "panel_id": "lisa",
            "layout": "separate_files",
            "panel_key": "LopNr",
            "members": [
                {"source": "lisa_2018", "period": 2018},
                {"source": "lisa_2019", "period": 2019},
            ],
        }
    ]
    # The output must round-trip through parse_config.
    from mock_data_wizard.config import parse_config

    parse_config(payload)


def test_stage3_panel_declined_does_not_emit_panels_block(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [
            {"source_name": "lisa_2018", "columns": [{"name": "LopNr"}]},
            {"source_name": "lisa_2019", "columns": [{"name": "LopNr"}]},
        ],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "n",  # treat as panel? no
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert "panels" not in payload


def test_stage3_merged_table_panel_emitted_when_confirmed(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "registry_main",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "AR", "sql_type": "int"},
                    {"name": "Belopp", "sql_type": "decimal"},
                ],
            }
        ],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "y",  # set up merged_table panel? yes
            "",  # panel_key (default: LopNr)
            "",  # panel_id (default: registry_main)
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["panels"] == [
        {
            "panel_id": "registry_main",
            "layout": "merged_table",
            "panel_key": "LopNr",
            "source": "registry_main",
            "time_key": "AR",
        }
    ]


def test_stage3_separate_files_panel_skips_merged_table_for_same_source(
    tmp_path: Path, monkeypatch
):
    """A source already claimed by a separate-files cluster must not
    also be offered as a merged_table candidate (single-source = single
    panel; otherwise the config validator rejects it)."""
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    # `AR` would normally trigger the merged_table prompt.
                    {"name": "AR", "sql_type": "int"},
                ],
            },
            {
                "source_name": "lisa_2019",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "AR", "sql_type": "int"},
                ],
            },
        ],
    )
    # If the merged_table loop weren't skipping claimed sources, we'd
    # need extra canned answers and the test would StopIteration.
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "y",  # treat lisa_*  as panel? yes
            "",  # panel_id default
            "",  # panel_key default
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert len(payload["panels"]) == 1
    assert payload["panels"][0]["layout"] == "separate_files"


# -- Stage 3: ambiguous-column review --------------------------------------


def test_stage3_ambiguous_review_flips_to_categorical(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "src",
                "columns": [
                    # `_kod` suffix + VARCHAR → high_cardinality fallback;
                    # the wizard should ask whether to flip it.
                    {"name": "Yrke_kod", "sql_type": "varchar"},
                ],
            }
        ],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "c",  # flip to categorical
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_types"]["src"]["Yrke_kod"] == {"type": "categorical"}


def test_stage3_ambiguous_review_default_keeps_high_cardinality(
    tmp_path: Path, monkeypatch
):
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "src",
                "columns": [{"name": "Niva_typ", "sql_type": "varchar"}],
            }
        ],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "",  # ambiguous prompt: default ('keep')
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_types"]["src"]["Niva_typ"] == {"type": "high_cardinality"}


# -- Stage 3: suppress_k walkthrough ---------------------------------------


def test_stage3_suppress_k_writes_column_options(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "LopNr", "sql_type": "int"}],
            }
        ],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "y",  # suppress_k overrides? yes
            "lisa_*:Diagnos",  # spec
            "20",  # k
            "",  # blank → finish
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_options"] == {"lisa_*": {"Diagnos": {"suppress_k": 20}}}


def test_stage3_suppress_k_rejects_below_threshold(tmp_path: Path, monkeypatch):
    """k ≤ 10 matches (or undercuts) the project default — the schema
    floor is 10, but the wizard reserves overrides for "raise above
    default" and treats 10 as a typo."""
    _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "LopNr", "sql_type": "int"}]}],
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # register: skip
            "y",  # suppress_k? yes
            "x:Diagnos",  # spec
            "5",  # k=5 → rejected
            # Re-prompt for next entry; blank → finish.
            "",
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    # Bad entry rejected; `column_options` should not be in the output.
    assert "column_options" not in payload


# -- Stage 3: register suggestion ------------------------------------------


def test_stage3_register_suggestion_accepted_pre_classifies(
    tmp_path: Path, monkeypatch
):
    """When all sources share a SCB-style prefix, the register prompt
    pre-fills it. Pressing enter accepts the suggestion and routes
    through the regmeta classification path."""
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "Sun2000Inr", "sql_type": "varchar"},
                    {"name": "MysteryCode", "sql_type": "varchar"},
                ],
            }
        ],
    )
    # Stub the regmeta classification path so we don't need a live DB.
    from mock_data_wizard import configure as cfg_mod

    class FakeConn:
        def close(self):
            pass

    monkeypatch.setattr(
        "regmeta.resolve_register_ids",
        lambda conn, register: [34] if register == "LISA" else [],
        raising=True,
    )
    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn(), raising=True)
    monkeypatch.setattr(
        "regmeta.db.db_path_from_args",
        lambda _x: Path("/fake/regmeta.db"),
        raising=True,
    )
    monkeypatch.setattr(
        cfg_mod, "_classification_lookup", lambda *a, **k: {"sun2000inr"}
    )
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept suggested LISA
            # `MysteryCode` falls back to high_cardinality (varchar, no
            # name pattern, no classification). The ambiguous regex
            # doesn't fire — `mysterycode` has no `_kod`/`_typ`/digit
            # suffix — so no per-column prompt.
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_types"]["lisa_2018"]["Sun2000Inr"] == {"type": "categorical"}


def test_stage3_register_dash_skips_regmeta(tmp_path: Path, monkeypatch):
    """Even with a strong suggestion, `-` at the register prompt must
    skip regmeta entirely — required when the regmeta DB is unreachable."""
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "LopNr", "sql_type": "int"}],
            }
        ],
    )

    def boom(*a, **k):  # pragma: no cover — must not be called
        raise AssertionError("regmeta path must be skipped for `-`")

    monkeypatch.setattr("regmeta.open_db", boom, raising=True)
    _canned_inputs(monkeypatch, ["-", "n"])  # skip regmeta, no suppress_k
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0


# -- Stage 2 / 4: instructions only ----------------------------------------


def test_stage2_prints_discover_instructions(tmp_path: Path, monkeypatch, capsys):
    (tmp_path / "mdw_runner.py").write_text("# stub", encoding="utf-8")
    _canned_inputs(monkeypatch, ["n"])  # don't rebuild
    rc = interactive._stage2_instructions(tmp_path)
    assert rc == 0
    out = capsys.readouterr().out
    assert "mdw_runner.py" in out
    assert "mdw_step1_discovery.json" in out
    # The MODE/upload-cap noise was trimmed in #36 follow-up — make sure
    # it doesn't creep back in.
    assert "10 MB" not in out
    assert 'MODE = "discover"' not in out


def test_stage4_prints_extract_instructions(tmp_path: Path, capsys):
    (tmp_path / "mdw_step2_config.json").write_text("{}", encoding="utf-8")
    rc = interactive._stage4_instructions(tmp_path)
    assert rc == 0
    out = capsys.readouterr().out
    assert "mdw_step2_config.json" in out
    assert 'MODE = "extract"' in out
    assert "mdw_step3_stats.json" in out


# -- Stage 5: dispatch to _cmd_generate ------------------------------------


def test_stage5_dispatches_to_generate_with_defaults(tmp_path: Path, monkeypatch):
    """Phase 3 interview: pressing enter at every prompt should produce
    the same Namespace as the Phase 1 stub did, except ``yes=True`` (the
    wizard owns confirmation) and ``force=False`` (no stale dir)."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )
    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(
        monkeypatch,
        [
            "",  # seed (default 42)
            "",  # sample_pct (default 1.0)
            "",  # regmeta enrichment (default Y)
            "",  # register filter (skip)
            "",  # output dir (default mock_data)
        ],
    )
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0
    args = captured["args"]
    assert args.stats == str(tmp_path / "mdw_step3_stats.json")
    assert args.seed == 42
    assert args.sample_pct == 1.0
    assert args.output_dir == str(tmp_path / "mock_data")
    assert args.no_regmeta is False
    assert args.register is None
    # The wizard already collected confirmations — _cmd_generate should
    # not re-prompt.
    assert args.yes is True
    assert args.force is False
    assert args.verbose is False


def test_stage5_force_skips_interview(tmp_path: Path, monkeypatch):
    """`mock-data-wizard --force` at Stage 5 must dispatch with all
    defaults and zero prompts — otherwise the auto-confirm contract
    breaks (the user gave us blanket permission, not per-prompt)."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )
    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    def boom(_p):  # pragma: no cover — must not be called
        raise AssertionError("force=True must not prompt")

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    monkeypatch.setattr("builtins.input", boom)
    rc = interactive._stage5_generate(tmp_path, force=True)
    assert rc == 0
    args = captured["args"]
    assert args.seed == 42
    assert args.sample_pct == 1.0
    assert args.no_regmeta is False
    assert args.register is None
    assert args.yes is True


def test_stage5_collects_per_flag_answers(tmp_path: Path, monkeypatch):
    """Non-default answers should propagate through to _cmd_generate."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )
    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(
        monkeypatch,
        [
            "7",  # seed
            "0.1",  # sample_pct
            "n",  # regmeta enrichment? no
            # No register prompt because regmeta is off.
            "custom_out",  # output dir
        ],
    )
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0
    args = captured["args"]
    assert args.seed == 7
    assert args.sample_pct == 0.1
    assert args.no_regmeta is True
    assert args.register is None
    assert args.output_dir == str(tmp_path / "custom_out")


def test_stage5_register_prompt_only_with_regmeta(tmp_path: Path, monkeypatch):
    """Saying no to regmeta enrichment must skip the register-filter
    prompt entirely; otherwise canned-input tests would hang waiting
    for a value that the user never sees."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )

    def fake_cmd_generate(_args: Namespace) -> int:
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(
        monkeypatch,
        ["", "", "n", ""],  # seed, sample_pct, regmeta=n, output_dir
    )
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0


def test_stage5_register_filter_propagates(tmp_path: Path, monkeypatch):
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )
    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(monkeypatch, ["", "", "y", "LISA", ""])
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0
    assert captured["args"].register == "LISA"


def test_stage5_stale_file_prompt_when_dir_populated(tmp_path: Path, monkeypatch):
    """Existing files in mock_data/ should trigger the delete-stale
    prompt; answering yes sets force=True so _cmd_generate cleans up."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )
    mock_dir = tmp_path / "mock_data"
    mock_dir.mkdir()
    (mock_dir / "stale.csv").write_text("a", encoding="utf-8")

    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(
        monkeypatch,
        [
            "",  # seed
            "",  # sample_pct
            "",  # regmeta
            "",  # register
            "",  # output_dir (mock_data — same as the populated dir)
            "y",  # delete stale? yes
        ],
    )
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0
    assert captured["args"].force is True


def test_stage5_seed_reprompts_on_bad_input(tmp_path: Path, monkeypatch):
    """Invalid seed must re-prompt rather than abort — typo recovery."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )

    def fake_cmd_generate(_args: Namespace) -> int:
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(
        monkeypatch,
        [
            "abc",  # bad seed → re-prompt
            "99",  # valid seed
            "",  # sample_pct
            "",  # regmeta
            "",  # register
            "",  # output_dir
        ],
    )
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0


def test_stage5_sample_pct_reprompts_on_out_of_range(tmp_path: Path, monkeypatch):
    """sample_pct > 1 or ≤ 0 must re-prompt rather than dispatch with garbage."""
    (tmp_path / "mdw_step3_stats.json").write_text(
        json.dumps(MINIMAL_STATS), encoding="utf-8"
    )
    captured: dict = {}

    def fake_cmd_generate(args: Namespace) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr("mock_data_wizard.cli._cmd_generate", fake_cmd_generate)
    _canned_inputs(
        monkeypatch,
        [
            "",  # seed
            "2.0",  # sample_pct out of range → re-prompt
            "0",  # 0 also out of range → re-prompt
            "0.5",  # valid
            "",  # regmeta
            "",  # register
            "",  # output_dir
        ],
    )
    rc = interactive._stage5_generate(tmp_path)
    assert rc == 0
    assert captured["args"].sample_pct == 0.5


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

    def fake_run(cwd: Path, *, force: bool = False) -> int:
        captured["cwd"] = cwd
        captured["force"] = force
        return 0

    monkeypatch.setattr("mock_data_wizard.interactive.run", fake_run, raising=True)
    rc = cli_mod.main([])
    assert rc == 0
    assert captured["cwd"] == Path.cwd()
    assert captured["force"] is False


def test_main_force_flag_threads_to_interactive(monkeypatch):
    from mock_data_wizard import cli as cli_mod

    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)

    captured: dict = {}

    def fake_run(cwd: Path, *, force: bool = False) -> int:
        captured["force"] = force
        return 0

    monkeypatch.setattr("mock_data_wizard.interactive.run", fake_run, raising=True)
    rc = cli_mod.main(["--force"])
    assert rc == 0
    assert captured["force"] is True


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
