"""Tests for ``_serialize`` plus the wire-format golden fixture.

The unit tests cover each nested dataclass in isolation so a field
rename causes a focused failure. The golden-fixture test serialises a
deterministic synthetic project and diffs against
``tests/data/state_snapshot.golden.json`` — that file is the wire
contract shared with the Svelte frontend's Vitest contract test
(``mock_data_wizard/web/src/lib/types.test.ts``).

Updating the golden:

    pytest mock_data_wizard/tests/test_serialize.py::test_golden_fixture_matches \\
        --update-golden

The frontend test reads the same JSON; rebuild ``static/`` after any
schema-shape change.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard import editor
from mock_data_wizard._serialize import (
    _column_info_to_dict,
    _column_type_override_to_dict,
    _editor_warning_to_dict,
    _mdw_config_to_dict,
    _panel_candidate_to_dict,
    _panel_member_to_dict,
    _panel_to_dict,
    _regmeta_signal_to_dict,
    _register_group_view_to_dict,
    state_snapshot_to_dict,
)
from mock_data_wizard.classify import RegmetaSignal
from mock_data_wizard.config import (
    ColumnTypeOverride,
    MDWConfig,
    Panel,
    PanelMember,
)
from mock_data_wizard.editor import (
    ColumnInfo,
    EditorWarning,
    RegisterGroupView,
    StateSnapshot,
)
from mock_data_wizard.panels import PanelCandidate


GOLDEN_PATH = Path(__file__).parent / "data" / "state_snapshot.golden.json"


# -- Per-type unit tests --------------------------------------------------


def test_column_type_override_drops_unset_hints():
    o = ColumnTypeOverride(type="numeric", numeric_subtype="integer")
    assert _column_type_override_to_dict(o) == {
        "type": "numeric",
        "numeric_subtype": "integer",
    }


def test_column_type_override_emits_only_relevant_hint():
    """``id_subtype`` on a numeric override would mean a stale field
    survived a type change. Serializer must not include it."""
    o = ColumnTypeOverride(type="opaque", id_subtype=None, numeric_subtype=None)
    assert _column_type_override_to_dict(o) == {"type": "opaque"}


def test_column_type_override_date_format_round_trip():
    o = ColumnTypeOverride(type="date", date_format="%Y%m%d")
    assert _column_type_override_to_dict(o) == {
        "type": "date",
        "date_format": "%Y%m%d",
    }


def test_panel_member_period_only():
    m = PanelMember(source="lisa_2018", period=2018)
    assert _panel_member_to_dict(m) == {"source": "lisa_2018", "period": 2018}


def test_panel_member_time_key_only():
    m = PanelMember(source="par", time_key="INDATUM")
    assert _panel_member_to_dict(m) == {"source": "par", "time_key": "INDATUM"}


def test_panel_to_dict():
    panel = Panel(
        panel_id="lisa",
        panel_key="P1105_LopNr_PersonNr",
        members=(
            PanelMember(source="lisa_2018", period=2018),
            PanelMember(source="lisa_2019", period=2019),
        ),
    )
    assert _panel_to_dict(panel) == {
        "panel_id": "lisa",
        "panel_key": "P1105_LopNr_PersonNr",
        "members": [
            {"source": "lisa_2018", "period": 2018},
            {"source": "lisa_2019", "period": 2019},
        ],
    }


def test_regmeta_signal_to_dict():
    sig = RegmetaSignal(
        datatyp_kind="numeric",
        classification_short_name="SUN2020-GRUPP",
        has_value_codes=True,
    )
    assert _regmeta_signal_to_dict(sig) == {
        "datatyp_kind": "numeric",
        "classification_short_name": "SUN2020-GRUPP",
        "has_value_codes": True,
    }


def test_regmeta_signal_nullable_fields():
    sig = RegmetaSignal(
        datatyp_kind=None, classification_short_name=None, has_value_codes=False
    )
    assert _regmeta_signal_to_dict(sig) == {
        "datatyp_kind": None,
        "classification_short_name": None,
        "has_value_codes": False,
    }


def test_panel_candidate_to_dict():
    cand = PanelCandidate(
        members=(
            {"source": "lisa_2018", "period": 2018},
            {"source": "lisa_2019", "period": 2019},
        ),
        suggested_panel_id="lisa",
        suggested_panel_key="LopNr",
    )
    assert _panel_candidate_to_dict(cand) == {
        "members": [
            {"source": "lisa_2018", "period": 2018},
            {"source": "lisa_2019", "period": 2019},
        ],
        "suggested_panel_id": "lisa",
        "suggested_panel_key": "LopNr",
    }


def test_panel_candidate_breaks_aliasing():
    """The serialiser must shallow-copy each member dict so a frontend
    pickling the response can't mutate the editor's view."""
    member = {"source": "x", "period": 2018}
    cand = PanelCandidate(members=(member,))
    out = _panel_candidate_to_dict(cand)
    out["members"][0]["source"] = "MUTATED"
    assert member["source"] == "x"


def test_column_info_to_dict_full():
    info = ColumnInfo(
        name="Salary",
        sql_type="DECIMAL",
        current_type="numeric",
        hint={"numeric_subtype": "integer"},
        provenance="manual",
        regmeta_signal=RegmetaSignal(
            datatyp_kind="numeric",
            classification_short_name=None,
            has_value_codes=False,
        ),
        regmeta_implied_type="numeric",
    )
    out = _column_info_to_dict(info)
    assert out == {
        "name": "Salary",
        "sql_type": "DECIMAL",
        "current_type": "numeric",
        "hint": {"numeric_subtype": "integer"},
        "provenance": "manual",
        "regmeta_signal": {
            "datatyp_kind": "numeric",
            "classification_short_name": None,
            "has_value_codes": False,
        },
        "regmeta_implied_type": "numeric",
    }


def test_column_info_to_dict_nullable_signal():
    info = ColumnInfo(
        name="Mystery",
        sql_type=None,
        current_type="opaque",
        hint=None,
        provenance="auto",
        regmeta_signal=None,
        regmeta_implied_type=None,
    )
    out = _column_info_to_dict(info)
    assert out["regmeta_signal"] is None
    assert out["hint"] is None
    assert out["sql_type"] is None


def test_editor_warning_to_dict():
    w = EditorWarning(
        code="discover_drift",
        message="payload differs",
        context={"stored_hash": "abc", "current_hash": "def"},
    )
    out = _editor_warning_to_dict(w)
    assert out == {
        "code": "discover_drift",
        "message": "payload differs",
        "context": {"stored_hash": "abc", "current_hash": "def"},
    }
    # Defensive copy — mutating the result must not touch the warning.
    out["context"]["stored_hash"] = "MUTATED"
    assert w.context["stored_hash"] == "abc"


def test_register_group_view_to_dict_with_panel_candidate():
    info = ColumnInfo(
        name="LopNr",
        sql_type="BIGINT",
        current_type="id",
        hint={"id_subtype": "integer"},
        provenance="auto",
        regmeta_signal=None,
        regmeta_implied_type=None,
    )
    g = RegisterGroupView(
        group_id="reg-1",
        register_id=1,
        register_name="LISA",
        confidence="high",
        sources=("lisa_2018",),
        columns_by_source={"lisa_2018": (info,)},
        schema_variants=1,
        panel_candidate=PanelCandidate(
            members=({"source": "lisa_2018", "period": 2018},),
            suggested_panel_id="lisa",
            suggested_panel_key="LopNr",
        ),
    )
    out = _register_group_view_to_dict(g)
    assert out["sources"] == ["lisa_2018"]
    assert out["columns_by_source"]["lisa_2018"][0]["name"] == "LopNr"
    assert out["panel_candidate"]["suggested_panel_id"] == "lisa"


def test_register_group_view_to_dict_without_panel_candidate():
    g = RegisterGroupView(
        group_id="noreg-x",
        register_id=None,
        register_name=None,
        confidence="none",
        sources=("x",),
        columns_by_source={"x": ()},
        schema_variants=1,
        panel_candidate=None,
    )
    assert _register_group_view_to_dict(g)["panel_candidate"] is None


def test_mdw_config_to_dict_minimum():
    config = MDWConfig(contract_version="mdw-config-3.0.0")
    out = _mdw_config_to_dict(config)
    assert out == {
        "contract_version": "mdw-config-3.0.0",
        "discover_hash": None,
        "column_types": {},
        "column_options": {},
        "sources": {},
        "panels": [],
        "manual_columns": [],
    }


def test_mdw_config_to_dict_full():
    config = MDWConfig(
        contract_version="mdw-config-3.0.0",
        column_types={
            "lisa_2018": {"LopNr": ColumnTypeOverride(type="id", id_subtype="integer")}
        },
        column_options={"lisa_2018": {"Salary": {"suppress_k": 20}}},
        sources={"lisa_2018": {"year": 2018, "register": "LISA"}},
        panels=(
            Panel(
                panel_id="lisa",
                panel_key="LopNr",
                members=(PanelMember(source="lisa_2018", period=2018),),
            ),
        ),
        manual_columns=(("lisa_2018", "LopNr"),),
        discover_hash="abc",
    )
    out = _mdw_config_to_dict(config)
    assert out["sources"]["lisa_2018"]["year"] == 2018
    assert out["manual_columns"] == [["lisa_2018", "LopNr"]]
    assert out["panels"][0]["panel_id"] == "lisa"
    assert out["column_options"]["lisa_2018"]["Salary"]["suppress_k"] == 20


def test_state_snapshot_to_dict_smoke():
    """Top-level shape: every documented key present."""
    snap = StateSnapshot(
        config=MDWConfig(contract_version="mdw-config-3.0.0"),
        groups=(),
        discover=None,
        warnings=(),
        snapshot_version="deadbeef",
    )
    out = state_snapshot_to_dict(snap)
    assert set(out.keys()) == {
        "config",
        "groups",
        "discover",
        "warnings",
        "snapshot_version",
    }
    assert out["snapshot_version"] == "deadbeef"
    assert out["discover"] is None


def test_state_snapshot_is_json_serialisable():
    """The whole point: the result must round-trip through json.dumps."""
    snap = StateSnapshot(
        config=MDWConfig(contract_version="mdw-config-3.0.0"),
        groups=(),
        discover={"contract_version": "discover-1.0.0", "sources": []},
        warnings=(EditorWarning(code="x", message="y", context={"k": 1}),),
        snapshot_version="cafe",
    )
    out = state_snapshot_to_dict(snap)
    json.dumps(out)  # raises if any non-JSON value slipped through


# -- Golden fixture --------------------------------------------------------


def _build_golden_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> StateSnapshot:
    """Produce a deterministic StateSnapshot covering every nested type:
    register-assigned + unassigned groups, manual + auto provenance, an
    inline hint, a regmeta signal, a panel candidate, and a drift
    warning. Regmeta lookups are stubbed so the result is reproducible
    without a live DB."""
    # Register stub: lisa_2018 + lisa_2019 share register "LISA" (id=1);
    # extras gets nothing.
    from mock_data_wizard.registers import Register

    monkeypatch.setattr(
        editor,
        "_autodetect_register_per_source",
        lambda discover, db_path: {
            "lisa_2018": "LISA",
            "lisa_2019": "LISA",
            "extras": None,
        },
    )
    monkeypatch.setattr(
        editor,
        "_resolve_signals_for_register",
        lambda register, cols, db_path: (
            {
                "salary": RegmetaSignal(
                    datatyp_kind="numeric",
                    classification_short_name=None,
                    has_value_codes=False,
                ),
                "kon": RegmetaSignal(
                    datatyp_kind=None,
                    classification_short_name="KON",
                    has_value_codes=True,
                ),
            }
            if register == "LISA"
            else {}
        ),
    )
    monkeypatch.setattr(
        editor,
        "resolve_register",
        lambda name, db_path=None: (
            Register(id=1, name="LISA") if name == "LISA" else None
        ),
    )

    sources = [
        {
            "source_name": "lisa_2018",
            "source_type": "file",
            "source_detail": {"path": "/data/lisa_2018.csv"},
            "row_count": 1000,
            "columns": [
                {"name": "LopNr", "sql_type": "BIGINT", "nullable": False},
                {"name": "Salary", "sql_type": "DECIMAL", "nullable": True},
                {"name": "Kon", "sql_type": "VARCHAR", "nullable": True},
            ],
        },
        {
            "source_name": "lisa_2019",
            "source_type": "file",
            "source_detail": {"path": "/data/lisa_2019.csv"},
            "row_count": 1100,
            "columns": [
                {"name": "LopNr", "sql_type": "BIGINT", "nullable": False},
                {"name": "Salary", "sql_type": "DECIMAL", "nullable": True},
                {"name": "Kon", "sql_type": "VARCHAR", "nullable": True},
            ],
        },
        {
            "source_name": "extras",
            "source_type": "file",
            "source_detail": {"path": "/data/extras.csv"},
            "row_count": 50,
            "columns": [
                {"name": "Mystery", "sql_type": "VARCHAR", "nullable": True},
            ],
        },
    ]
    discover_path = tmp_path / "mock_data_discovery.json"
    discover_path.write_text(
        json.dumps({"contract_version": "discover-1.0.0", "sources": sources}),
        encoding="utf-8",
    )

    snap = editor.init_if_missing(tmp_path, discover_path)

    # Mark Salary on lisa_2018 as a manual override with an inline hint.
    snap = editor.set_column_type(
        tmp_path,
        ["lisa_2018"],
        "Salary",
        "numeric",
        expected_version=snap.snapshot_version,
        hint={"numeric_subtype": "integer"},
    )

    # Force a discover_drift warning by re-reading with a mismatched
    # discover hash. Easiest path: hand-edit the on-disk hash.
    config_path = tmp_path / "mock_data_config.json"
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    payload["discover_hash"] = "0" * 64
    config_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return editor.get_state(tmp_path, discover_path=discover_path)


def test_golden_fixture_matches(tmp_path, monkeypatch, request):
    """Serialise the canonical fixture and diff against the committed
    golden. The golden is the wire contract shared with the frontend."""
    snap = _build_golden_snapshot(tmp_path, monkeypatch)
    produced = state_snapshot_to_dict(snap)

    # Snapshot version is content-addressed and varies with the random
    # `tmp_path`; replace with a stable placeholder for diffing.
    produced["snapshot_version"] = "<elided>"

    text = json.dumps(produced, indent=2, ensure_ascii=False) + "\n"

    if request.config.getoption("--update-golden"):
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(text, encoding="utf-8")
        return

    assert GOLDEN_PATH.exists(), (
        f"{GOLDEN_PATH} is missing. Regenerate with "
        f"`pytest tests/test_serialize.py::test_golden_fixture_matches "
        f"--update-golden`."
    )
    expected = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert produced == expected, (
        "wire-format drift detected. If this is intentional, regenerate "
        "the golden with --update-golden and rebuild the frontend."
    )
