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
    _is_column_overridden,
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


def _stub_no_regmeta_guesses(monkeypatch) -> None:
    """Force ``guess_register_per_family`` to return all-``None`` guesses.

    Tests that don't care about regmeta auto-classification install this
    so the family menu shows every family with ``register_id=None`` and
    ``build_config`` skips the regmeta path entirely.
    """
    from mock_data_wizard import configure as cfg_mod
    from mock_data_wizard.classify import is_known_id

    def _fake(families, **_kw):
        out = {}
        for fid, sources in families.items():
            first = sources[0]
            cols = first.get("columns", [])
            nonid = [c["name"] for c in cols if not is_known_id(c["name"])]
            out[fid] = cfg_mod.FamilyGuess(
                family_id=fid,
                sources=[s["source_name"] for s in sources],
                columns=[(c["name"], c.get("sql_type")) for c in cols],
                register_id=None,
                register_name=None,
                confidence="none",
                match_count=0,
                nonid_count=len(nonid),
            )
        return out

    monkeypatch.setattr(cfg_mod, "guess_register_per_family", _fake)


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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
            "n",  # suppress_k overrides? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    config = tmp_path / "mdw_step2_config.json"
    assert config.exists()
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["lisa_2018"]["LopNr"] == {"type": "id"}
    # No regmeta guess for this family → no register set → Kommun
    # lands at text (char → fallthrough). User reviews.
    assert payload["column_types"]["lisa_2018"]["Kommun"] == {"type": "text"}


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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "y",  # overwrite: yes
            "",  # accept all families
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
    # force=True skips both the overwrite prompt AND the family
    # interview; only the post-passes (panels, ambiguous, suppress_k)
    # run.
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "n",  # suppress_k overrides? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path, force=True)
    assert rc == 0
    payload = json.loads(config.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


# -- Phase 2 helpers (pure-function unit tests) ---------------------------


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


def test_detect_separate_file_panels_preserves_sql_schema_dot():
    """SQL table source names like `dbo.scb_rams_2018` use the dot as
    a schema separator, not a file extension. The year-suffix match
    must run on the raw name first so the trailing `2018` is seen."""
    discover = {
        "sources": [
            {"source_name": "dbo.scb_rams_2018", "columns": []},
            {"source_name": "dbo.scb_rams_2019", "columns": []},
        ]
    }
    clusters = interactive._detect_separate_file_panels(discover)
    assert len(clusters) == 1
    assert clusters[0]["prefix"] == "dbo.scb_rams"
    years = sorted(m["period"] for m in clusters[0]["members"])
    assert years == [2018, 2019]


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


def test_shared_id_column_none_when_no_overlap():
    """Zero id columns shared across members → None (no panel key
    candidate at all)."""
    assert (
        interactive._shared_id_column(
            [{"columns": [{"name": "LopNr"}]}, {"columns": [{"name": "Kon"}]}]
        )
        is None
    )


def test_shared_id_column_prefers_personnr_when_multiple():
    """Multiple shared id columns: prefer the person-derived id over
    a record-level surrogate like ``LopNr``, since PersonNr (or its
    composite forms) is what spans a panel."""
    # Bare PersonNr wins over LopNr
    members = [
        {"columns": [{"name": "LopNr"}, {"name": "PersonNr"}]},
        {"columns": [{"name": "LopNr"}, {"name": "PersonNr"}]},
    ]
    assert interactive._shared_id_column(members) == "PersonNr"

    # Composite LopNr_PersonNr wins over both LopNr and PersonNr
    members = [
        {
            "columns": [
                {"name": "LopNr"},
                {"name": "PersonNr"},
                {"name": "LopNr_PersonNr"},
            ]
        },
        {
            "columns": [
                {"name": "LopNr"},
                {"name": "PersonNr"},
                {"name": "LopNr_PersonNr"},
            ]
        },
    ]
    assert interactive._shared_id_column(members) == "LopNr_PersonNr"


def test_shared_id_column_falls_back_to_alpha_sort():
    """When several ids are shared but none match the personnr-style
    preference list, return the alphabetically-first to keep the
    default deterministic across runs."""
    members = [
        {"columns": [{"name": "LopNr_Apa"}, {"name": "LopNr_Banan"}]},
        {"columns": [{"name": "LopNr_Apa"}, {"name": "LopNr_Banan"}]},
    ]
    assert interactive._shared_id_column(members) == "LopNr_Apa"


def test_ambiguous_columns_picks_kod_typ_and_digit_suffixes():
    payload = {
        "column_types": {
            "lisa_2018": {
                # Ambiguous — `_kod`, `_typ`, trailing digit
                "Yrke_kod": {"type": "text"},
                "Niva_typ": {"type": "text"},
                "SNI3": {"type": "text"},
                # Not ambiguous — already typed.
                "Kommun": {"type": "categorical"},
                "LopNr": {"type": "id"},
                # Plain text with no suffix — leave alone.
                "FreeText": {"type": "text"},
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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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
                    # `_kod` suffix + VARCHAR → text fallback;
                    # the wizard should ask whether to flip it.
                    {"name": "Yrke_kod", "sql_type": "varchar"},
                ],
            }
        ],
    )
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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


def test_stage3_ambiguous_review_default_keeps_text(tmp_path: Path, monkeypatch):
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "src",
                "columns": [{"name": "Niva_typ", "sql_type": "varchar"}],
            }
        ],
    )
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
            "",  # ambiguous prompt: default ('keep')
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_types"]["src"]["Niva_typ"] == {"type": "text"}


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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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
    _stub_no_regmeta_guesses(monkeypatch)
    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
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


def test_stage3_auto_guessed_register_pre_classifies(tmp_path: Path, monkeypatch):
    """When ``guess_register_per_family`` returns a register, columns
    flagged by ``_regmeta_lookup`` get typed as ``categorical`` via the
    regmeta path — even if their name pattern wouldn't have classified
    them.
    """
    from mock_data_wizard import configure as cfg_mod

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

    # Stub the family-guess pass: pretend regmeta confidently picked LISA
    # for the lone family, with `Sun2000Inr` carrying a classification.
    def _fake_guess(families, **_kw):
        out = {}
        for fid, sources in families.items():
            cols = sources[0].get("columns", [])
            out[fid] = cfg_mod.FamilyGuess(
                family_id=fid,
                sources=[s["source_name"] for s in sources],
                columns=[(c["name"], c.get("sql_type")) for c in cols],
                register_id=34,
                register_name="LISA",
                confidence="high",
                match_count=2,
                nonid_count=2,
                regmeta_signals={
                    "Sun2000Inr": cfg_mod.RegmetaSignal(
                        datatyp_kind=None,
                        classification_short_name="SUN2000",
                    )
                },
            )
        return out

    monkeypatch.setattr(cfg_mod, "guess_register_per_family", _fake_guess)

    # Stub the regmeta path that build_config takes when register_per_source
    # is non-empty.
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
        cfg_mod,
        "_regmeta_lookup",
        lambda *a, **k: {
            "sun2000inr": cfg_mod.RegmetaSignal(
                datatyp_kind=None, classification_short_name="SUN2000"
            )
        },
    )

    _canned_inputs(
        monkeypatch,
        [
            "",  # accept all families
            # `MysteryCode` falls back to text (no name
            # pattern, no classification). Ambiguous regex doesn't fire
            # — no `_kod`/`_typ`/digit suffix.
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_types"]["lisa_2018"]["Sun2000Inr"] == {"type": "categorical"}
    assert payload["column_types"]["lisa_2018"]["MysteryCode"] == {"type": "text"}


def test_stage3_skips_regmeta_when_no_family_resolves(tmp_path: Path, monkeypatch):
    """When the auto-guess pass finds no register match for any family,
    ``build_config`` must not open regmeta at all — the user gets
    name-pattern classification only.
    """
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "LopNr", "sql_type": "int"}],
            }
        ],
    )
    _stub_no_regmeta_guesses(monkeypatch)

    def boom(*a, **k):  # pragma: no cover — must not be called
        raise AssertionError("regmeta path must be skipped when no family resolves")

    monkeypatch.setattr("regmeta.open_db", boom, raising=True)
    _canned_inputs(monkeypatch, ["", "n"])  # accept families, no suppress_k
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


def test_format_source_list_year_range_after_extension_strip():
    """Files with ``.csv`` extensions should still cluster under a
    common stem and surface the actual year range."""
    sources = [
        "Äp9_2003.csv",
        "Äp9_2004.csv",
        "Äp9_2005.csv",
        "Äp9_2006.csv",
    ]
    assert interactive._format_source_list(sources) == "Äp9_2003–2006"


def test_format_source_list_collapses_gapped_year_ranges():
    """Non-contiguous years split into separate ranges so missing-year
    gaps (e.g. COVID-cancelled tests in 2020/2021) stay visible."""
    sources = [
        "Kursprov_gymn_VT2017.csv",
        "Kursprov_gymn_VT2018.csv",
        "Kursprov_gymn_VT2019.csv",
        "Kursprov_gymn_VT2022.csv",
        "Kursprov_gymn_VT2023.csv",
        "Kursprov_gymn_VT2024.csv",
    ]
    assert interactive._format_source_list(sources) == (
        "Kursprov_gymn_VT_2017–2019, 2022–2024"
    )


def test_format_source_list_singleton_keeps_filename():
    assert interactive._format_source_list(["Äp9_2003.csv"]) == "Äp9_2003.csv"


def test_format_source_list_lists_full_filenames_for_mixed_input():
    """Files that don't fit the ``<stem>_YYYY`` shape keep their full
    filenames; year-stem files among them still collapse to a range."""
    sources = ["foo_2003.csv", "foo_2004.csv", "foo_extras.csv"]
    assert interactive._format_source_list(sources) == "foo_2003–2004, foo_extras.csv"


def test_format_source_list_lists_all_when_no_year_collapse():
    """Heterogeneous filenames without a shared year stem render in
    full so the inspector header shows what's actually in the group
    instead of a truncated common-prefix stub."""
    sources = [
        "Distansutb_grund_HT20_VT21.csv",
        "Distansutb_grund_VT20.csv",
        "Distansutb_gymn_HT20_VT21.csv",
        "Distansutb_gymn_VT20.csv",
    ]
    assert interactive._format_source_list(sources) == (
        "Distansutb_grund_HT20_VT21.csv, Distansutb_grund_VT20.csv, "
        "Distansutb_gymn_HT20_VT21.csv, Distansutb_gymn_VT20.csv"
    )


def test_collapse_year_ranges_handles_gaps_and_singletons():
    assert interactive._collapse_year_ranges([2003, 2004, 2005]) == "2003–2005"
    assert interactive._collapse_year_ranges([2003]) == "2003"
    assert (
        interactive._collapse_year_ranges([2003, 2004, 2008, 2009, 2012])
        == "2003–2004, 2008–2009, 2012"
    )
    assert interactive._collapse_year_ranges([]) == ""


def test_detect_separate_file_panels_handles_csv_extension():
    """The same ``.csv`` extension fix has to flow through panel
    detection or every CSV-extension family is silently ineligible."""
    discover = {
        "sources": [
            {"source_name": "Äp9_2003.csv", "columns": []},
            {"source_name": "Äp9_2004.csv", "columns": []},
            {"source_name": "Äp9_2005.csv", "columns": []},
        ]
    }
    clusters = interactive._detect_separate_file_panels(discover)
    assert len(clusters) == 1
    assert clusters[0]["prefix"] == "Äp9"
    assert [m["period"] for m in clusters[0]["members"]] == [2003, 2004, 2005]
    # Members keep the original filename incl. extension so downstream
    # references still resolve.
    assert [m["source"] for m in clusters[0]["members"]] == [
        "Äp9_2003.csv",
        "Äp9_2004.csv",
        "Äp9_2005.csv",
    ]


def test_refresh_regmeta_signals_replaces_with_new_register_signals(monkeypatch):
    """After a register change, the inspector calls
    `_refresh_regmeta_signals` to overwrite stale signals from the
    auto-guess with a fresh lookup against the new register."""
    from mock_data_wizard import configure as cfg_mod

    grp = interactive.RegisterGroup(
        group_id="reg-34",
        register_id=34,
        register_name="LISA",
        confidence="partial",
    )
    grp.sources = ["lisa_2018"]
    grp.columns_by_source = {
        "lisa_2018": [("Sun2000Inr", "varchar"), ("Mystery", "int")],
    }
    # Stale signals from a previous register
    grp.regmeta_signals = {
        "Sun2000Inr": cfg_mod.RegmetaSignal(
            datatyp_kind=None, classification_short_name="OLD-TAG"
        ),
        "GoneCol": cfg_mod.RegmetaSignal(
            datatyp_kind="numeric", classification_short_name=None
        ),
    }

    seen: dict = {}

    sun_sig = cfg_mod.RegmetaSignal(
        datatyp_kind=None, classification_short_name="SUN2000-INRIKTNING"
    )
    mystery_sig = cfg_mod.RegmetaSignal(
        datatyp_kind="numeric", classification_short_name=None
    )

    def fake_lookup(conn, col_names, register_ids):
        seen["col_names"] = set(col_names)
        seen["register_ids"] = list(register_ids)
        return {"sun2000inr": sun_sig, "mystery": mystery_sig}

    class FakeConn:
        def close(self):
            pass

    monkeypatch.setattr(cfg_mod, "_regmeta_lookup", fake_lookup)
    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn(), raising=True)
    monkeypatch.setattr(
        "regmeta.db.db_path_from_args", lambda _p: "/fake.db", raising=True
    )

    interactive._refresh_regmeta_signals(grp, 99)

    # Lookup ran against the new register id with the group's columns
    assert seen["register_ids"] == [99]
    assert {"Sun2000Inr", "Mystery"} == seen["col_names"]
    # Signals now reflect the new register; the old GoneCol entry is gone.
    assert grp.regmeta_signals == {"Sun2000Inr": sun_sig, "Mystery": mystery_sig}


def test_is_column_overridden_detects_session_overrides():
    """The override marker fires when any source in the group has the
    column in `column_overrides`, and stays False otherwise."""
    sources = ["lisa_2018", "lisa_2019"]

    # Empty overrides → never marked
    assert _is_column_overridden("Kommun", sources, {}) is False

    # Override on one source in the group → marked
    overrides = {("lisa_2018", "Kommun"): "categorical"}
    assert _is_column_overridden("Kommun", sources, overrides) is True

    # Override only on a source outside the group → not marked
    other_sources = ["rams_2018"]
    assert _is_column_overridden("Kommun", other_sources, overrides) is False

    # Different column → not marked
    assert _is_column_overridden("FodelseLand", sources, overrides) is False


def test_format_source_list_does_not_extract_day_from_yyyymmdd():
    """`_20241231` is a date stamp, not year=1231 — the regex must not
    treat the trailing `1231` as a year suffix, so these stay as
    literal filenames in the inspector header."""
    sources = [
        "FlerGen_Adopforaldrar_20241231.csv",
        "FlerGen_Bioforaldrar_20241231.csv",
    ]
    label = interactive._format_source_list(sources)
    assert "1231" not in label.replace("20241231", "")
    assert label == (
        "FlerGen_Adopforaldrar_20241231.csv, FlerGen_Bioforaldrar_20241231.csv"
    )


def test_format_source_list_handles_no_separator_year_suffix():
    """`..._HT2011` has no underscore between `HT` and the year, but
    the negative-lookbehind regex still matches because there's no
    digit immediately before `2011`."""
    sources = [
        "Kursprov_gymn_HT2011.csv",
        "Kursprov_gymn_HT2012.csv",
        "Kursprov_gymn_HT2013.csv",
    ]
    label = interactive._format_source_list(sources)
    assert label == "Kursprov_gymn_HT_2011–2013"


# -- Inspector ⚠ conflict marker -------------------------------------------


def test_regmeta_cell_no_signal_renders_blank():
    from mock_data_wizard.interactive import _regmeta_cell

    assert _regmeta_cell(None, "text", is_overridden=False) == ""
    assert _regmeta_cell(None, "categorical", is_overridden=True) == ""


def test_regmeta_cell_no_override_keeps_short_name_or_check():
    from mock_data_wizard.configure import RegmetaSignal
    from mock_data_wizard.interactive import _regmeta_cell

    classified = RegmetaSignal(
        datatyp_kind=None, classification_short_name="SUN2000-GRUPP"
    )
    bare_codes = RegmetaSignal(
        datatyp_kind=None, classification_short_name=None, has_value_codes=True
    )

    # No override: render the short_name when one exists, else ✓
    assert _regmeta_cell(classified, "categorical", False) == "SUN2000-GRUPP"
    assert _regmeta_cell(bare_codes, "categorical", False) == "✓"


def test_regmeta_cell_override_matching_implied_keeps_short_name():
    """An override that agrees with regmeta's implied type is not a
    conflict — the cell still shows the short_name, no ⚠."""
    from mock_data_wizard.configure import RegmetaSignal
    from mock_data_wizard.interactive import _regmeta_cell

    sig = RegmetaSignal(datatyp_kind="numeric", classification_short_name=None)
    # Manual override to numeric matches what regmeta says → no warning
    assert _regmeta_cell(sig, "numeric", is_overridden=True) == "✓"


def test_regmeta_cell_override_conflicts_emits_warning():
    """Manual override that disagrees with regmeta's implied type
    emits ⚠. Short_name is preserved alongside the warning when one
    exists, so the user can see both."""
    from mock_data_wizard.configure import RegmetaSignal
    from mock_data_wizard.interactive import _regmeta_cell

    classified = RegmetaSignal(
        datatyp_kind=None, classification_short_name="SUN2000-GRUPP"
    )
    bare_codes = RegmetaSignal(
        datatyp_kind=None, classification_short_name=None, has_value_codes=True
    )
    numeric = RegmetaSignal(datatyp_kind="numeric", classification_short_name=None)

    # regmeta says categorical, user picks numeric → conflict
    assert _regmeta_cell(classified, "numeric", is_overridden=True) == "⚠ SUN2000-GRUPP"
    # bare value codes → no short_name to show alongside ⚠
    assert _regmeta_cell(bare_codes, "text", is_overridden=True) == "⚠"
    # regmeta says numeric, user picks categorical → conflict
    assert _regmeta_cell(numeric, "categorical", is_overridden=True) == "⚠"


def test_regmeta_cell_override_when_regmeta_has_no_opinion():
    """When regmeta has the column in its DB but with no semantic
    signal (no value codes, no classification, text/unknown datatyp),
    a manual override doesn't conflict — show the bare ✓."""
    from mock_data_wizard.configure import RegmetaSignal
    from mock_data_wizard.interactive import _regmeta_cell

    sig = RegmetaSignal(datatyp_kind=None, classification_short_name=None)
    assert _regmeta_cell(sig, "categorical", is_overridden=True) == "✓"
    assert _regmeta_cell(sig, "numeric", is_overridden=True) == "✓"


# -- Inspector override flow -----------------------------------------------


def test_inspect_register_group_applies_column_override(tmp_path: Path, monkeypatch):
    """End-to-end: stub guess_register_per_family, drive
    `_stage3_configure` through the family menu → group inspector →
    type override → enter back → write. The chosen override must land
    in the written mdw_step2_config.json."""
    from mock_data_wizard import configure as cfg_mod

    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "MysteryCode", "sql_type": "varchar"},
                ],
            }
        ],
    )

    def _fake_guess(families, **_kw):
        out = {}
        for fid, sources in families.items():
            cols = sources[0].get("columns", [])
            out[fid] = cfg_mod.FamilyGuess(
                family_id=fid,
                sources=[s["source_name"] for s in sources],
                columns=[(c["name"], c.get("sql_type")) for c in cols],
                register_id=34,
                register_name="LISA",
                confidence="high",
                match_count=1,
                nonid_count=1,
                regmeta_signals={},  # MysteryCode unknown to regmeta
            )
        return out

    monkeypatch.setattr(cfg_mod, "guess_register_per_family", _fake_guess)

    # build_config doesn't need to hit regmeta — every register that
    # appears in register_per_source is already in precomputed_signals
    # (with an empty signal map for "LISA").
    def boom(*_a, **_kw):  # pragma: no cover
        raise AssertionError("regmeta DB must not be opened")

    monkeypatch.setattr("regmeta.open_db", boom, raising=True)

    _canned_inputs(
        monkeypatch,
        [
            "1",  # inspect group [1]
            "2",  # column #2 (MysteryCode); LopNr is column #1
            "c",  # → categorical
            "",  # back out of inspector
            "",  # accept all groups
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    cols = payload["column_types"]["lisa_2018"]
    assert cols["LopNr"] == {"type": "id"}
    # The override flipped MysteryCode from its auto-classified
    # text to categorical and was applied at write time.
    assert cols["MysteryCode"] == {"type": "categorical"}


def test_inspect_register_group_apply_then_back_preserves_override(
    tmp_path: Path, monkeypatch
):
    """Re-entering the inspector for the same group on a second pass
    shows the * marker on the previously-overridden column. The marker
    is what the user relies on to spot prior decisions, so verify the
    override is not silently re-classified."""
    from mock_data_wizard import configure as cfg_mod

    _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "MysteryCode", "sql_type": "varchar"},
                ],
            }
        ],
    )

    def _fake_guess(families, **_kw):
        out = {}
        for fid, sources in families.items():
            cols = sources[0].get("columns", [])
            out[fid] = cfg_mod.FamilyGuess(
                family_id=fid,
                sources=[s["source_name"] for s in sources],
                columns=[(c["name"], c.get("sql_type")) for c in cols],
                register_id=34,
                register_name="LISA",
                confidence="high",
                match_count=1,
                nonid_count=1,
                regmeta_signals={},
            )
        return out

    monkeypatch.setattr(cfg_mod, "guess_register_per_family", _fake_guess)
    monkeypatch.setattr(
        "regmeta.open_db",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            AssertionError("regmeta DB must not be opened")
        ),
        raising=True,
    )

    _canned_inputs(
        monkeypatch,
        [
            "1",  # inspect group
            "2",  # MysteryCode
            "c",  # → categorical
            "",  # back to menu
            "1",  # inspect again (second pass)
            "",  # back without changing
            "",  # accept all
            "n",  # suppress_k? no
        ],
    )
    rc = interactive._stage3_configure(tmp_path)
    assert rc == 0
    payload = json.loads(
        (tmp_path / "mdw_step2_config.json").read_text(encoding="utf-8")
    )
    assert payload["column_types"]["lisa_2018"]["MysteryCode"] == {
        "type": "categorical"
    }


def test_collect_precomputed_signals_lowercases_inner_keys():
    """`build_config` looks up signals with lowercased column names —
    the cache passed in must use the same convention even though
    `RegisterGroup.regmeta_signals` is keyed by original case for
    inspector display."""
    from mock_data_wizard.configure import RegmetaSignal
    from mock_data_wizard.interactive import RegisterGroup, _collect_precomputed_signals

    sig = RegmetaSignal(datatyp_kind=None, classification_short_name="SUN2000")
    grp = RegisterGroup(
        group_id="reg-34",
        register_id=34,
        register_name="LISA",
        confidence="high",
    )
    grp.regmeta_signals = {"Sun2000Inr": sig}

    out = _collect_precomputed_signals({"reg-34": grp})
    assert out == {"LISA": {"sun2000inr": sig}}


def test_collect_precomputed_signals_falls_back_to_register_id_string():
    """When `register_name` is None but `register_id` is set, the cache
    key uses str(register_id) so it lines up with what
    `_stage3_configure` puts into `register_per_source` in the same
    edge case."""
    from mock_data_wizard.interactive import RegisterGroup, _collect_precomputed_signals

    grp = RegisterGroup(
        group_id="reg-34",
        register_id=34,
        register_name=None,
        confidence="partial",
    )
    out = _collect_precomputed_signals({"reg-34": grp})
    assert "34" in out
    assert "LISA" not in out


def test_collect_precomputed_signals_merges_duplicate_register_keys():
    """Two groups pointing at the same register (e.g. after the user
    re-points one to match another) must contribute their signals to a
    single merged map — otherwise `build_config`'s cache short-circuit
    would skip the DB lookup and silently drop the columns one of the
    groups had fetched evidence for."""
    from mock_data_wizard.configure import RegmetaSignal
    from mock_data_wizard.interactive import RegisterGroup, _collect_precomputed_signals

    sig_a = RegmetaSignal(datatyp_kind=None, classification_short_name="A")
    sig_b = RegmetaSignal(datatyp_kind=None, classification_short_name="B")
    g1 = RegisterGroup(
        group_id="reg-34", register_id=34, register_name="LISA", confidence="high"
    )
    g1.regmeta_signals = {"ColA": sig_a}
    g2 = RegisterGroup(
        group_id="reg-34-x", register_id=34, register_name="LISA", confidence="partial"
    )
    g2.regmeta_signals = {"ColB": sig_b}

    out = _collect_precomputed_signals({"g1": g1, "g2": g2})
    assert out == {"LISA": {"cola": sig_a, "colb": sig_b}}


def test_build_config_uses_precomputed_signals_without_db(monkeypatch):
    """When `precomputed_signals` covers every register that
    `register_per_source` references, `build_config` must not open the
    regmeta DB at all."""
    from mock_data_wizard.configure import RegmetaSignal, build_config

    def boom(*_a, **_kw):  # pragma: no cover
        raise AssertionError("regmeta DB must not be opened")

    monkeypatch.setattr("regmeta.open_db", boom, raising=True)
    monkeypatch.setattr("regmeta.resolve_register_ids", boom, raising=True)

    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "Sun2000Inr", "sql_type": "varchar"},
                ],
            }
        ],
    }
    out = build_config(
        discover,
        register_per_source={"lisa_2018": "LISA"},
        precomputed_signals={
            "LISA": {
                "sun2000inr": RegmetaSignal(
                    datatyp_kind=None, classification_short_name="SUN2000"
                )
            }
        },
    )
    cols = out["column_types"]["lisa_2018"]
    assert cols["LopNr"] == {"type": "id"}
    assert cols["Sun2000Inr"] == {"type": "categorical"}
