"""Tests for the editor API.

Covers ``get_state``, ``init_if_missing``, the mutating operations,
the snapshot_version concurrency contract, manual-override
preservation, discover-drift detection, and graceful
regmeta-DB-absent behaviour.

The regmeta DB is mocked via monkeypatching ``mock_data_wizard.editor``
helpers so tests don't depend on a live regmeta install.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from mock_data_wizard import editor
from mock_data_wizard.classify import RegmetaSignal
from mock_data_wizard.config import Panel, PanelMember
from mock_data_wizard.editor import (
    NotInitializedError,
    StaleStateError,
    StateSnapshot,
    ValidationError,
    get_state,
    init_if_missing,
    put_panel,
    remove_panel,
    set_column_options,
    set_column_type,
    set_group_register,
    set_source_metadata,
)


# -- Fixtures --------------------------------------------------------------


def _discover(sources):
    return {
        "contract_version": "discover-1.0.0",
        "sources": sources,
    }


def _write_discover(path: Path, sources) -> Path:
    target = path / "mock_data_discovery.json"
    target.write_text(json.dumps(_discover(sources)), encoding="utf-8")
    return target


@pytest.fixture(autouse=True)
def _no_regmeta(monkeypatch):
    """Default: regmeta DB unavailable. Tests that need signals override
    via local monkeypatching."""
    monkeypatch.setattr(
        editor,
        "_autodetect_register_per_source",
        lambda discover, db_path: {
            src["source_name"]: None for src in discover.get("sources", [])
        },
    )
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})
    monkeypatch.setattr(editor, "resolve_register", lambda name, db_path=None: None)


# -- get_state -------------------------------------------------------------


def test_get_state_raises_when_config_absent(tmp_path: Path):
    with pytest.raises(NotInitializedError):
        get_state(tmp_path)


def test_get_state_succeeds_without_discover(tmp_path: Path):
    payload = {"contract_version": "mdw-config-3.0.0"}
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    snap = get_state(tmp_path)
    assert isinstance(snap, StateSnapshot)
    assert snap.discover is None
    assert snap.warnings == ()


def test_get_state_loads_default_discover_when_present(tmp_path: Path):
    _write_discover(tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}])
    payload = {"contract_version": "mdw-config-3.0.0"}
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    snap = get_state(tmp_path)
    assert snap.discover is not None
    assert snap.discover["sources"][0]["source_name"] == "x"


def test_get_state_warns_on_discover_drift(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "LopNr", "sql_type": "BIGINT"}]}],
    )
    payload = {
        "contract_version": "mdw-config-3.0.0",
        "discover_hash": "WRONG_HASH_FOR_THIS_DISCOVER",
    }
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    snap = get_state(tmp_path, discover_path=discover_path)
    codes = [w.code for w in snap.warnings]
    assert "discover_drift" in codes


def test_get_state_no_drift_warning_on_reorder(tmp_path: Path):
    """Re-ordering sources (or columns) must not trigger drift."""
    src_a = {"source_name": "a", "columns": [{"name": "x", "sql_type": "INT"}]}
    src_b = {"source_name": "b", "columns": [{"name": "y", "sql_type": "INT"}]}
    discover_a = _write_discover(tmp_path, [src_a, src_b])
    init_if_missing(tmp_path, discover_a)
    discover_b = _write_discover(tmp_path, [src_b, src_a])
    snap2 = get_state(tmp_path, discover_path=discover_b)
    assert all(w.code != "discover_drift" for w in snap2.warnings)


# -- init_if_missing -------------------------------------------------------


def test_init_dense_classification(tmp_path: Path):
    """Every column gets a type entry; default to opaque."""
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "src",
                "columns": [
                    {"name": "LopNr", "sql_type": "BIGINT"},
                    {"name": "Salary", "sql_type": "DECIMAL"},
                    {"name": "Mystery", "sql_type": "VARCHAR"},
                ],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    cols = snap.config.column_types["src"]
    assert set(cols.keys()) == {"LopNr", "Salary", "Mystery"}
    assert cols["LopNr"].type == "id"
    assert cols["Salary"].type == "numeric"
    assert cols["Mystery"].type == "opaque"


def test_init_idempotent_without_overwrite(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap1 = init_if_missing(tmp_path, discover_path)
    snap2 = init_if_missing(tmp_path, discover_path)
    assert snap1.snapshot_version == snap2.snapshot_version


def test_init_overwrite_replaces_config(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    init_if_missing(tmp_path, discover_path)
    # Externally edit to add a manual override.
    cfg_path = tmp_path / "mock_data_config.json"
    payload = json.loads(cfg_path.read_text())
    payload["manual_columns"] = [["x", "LopNr"]]
    cfg_path.write_text(json.dumps(payload), encoding="utf-8")
    # Overwrite wipes everything.
    snap = init_if_missing(tmp_path, discover_path, overwrite=True)
    assert snap.config.manual_columns == ()


def test_init_writes_3_0_0_with_discover_hash(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    assert snap.config.contract_version == "mdw-config-3.0.0"
    assert snap.config.discover_hash is not None
    assert len(snap.config.discover_hash) == 64  # sha256 hex


def test_init_persists_year_from_source_name(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "lisa_2018", "columns": [{"name": "LopNr"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    assert snap.config.sources["lisa_2018"]["year"] == 2018


def test_init_raises_on_empty_discover(tmp_path: Path):
    discover_path = _write_discover(tmp_path, [])
    with pytest.raises(ValidationError, match="no sources"):
        init_if_missing(tmp_path, discover_path)


# -- set_column_type -------------------------------------------------------


def test_set_column_type_marks_manual(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "x",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap2 = set_column_type(
        tmp_path,
        "x",
        "Mystery",
        "categorical",
        expected_version=snap.snapshot_version,
    )
    assert snap2.config.column_types["x"]["Mystery"].type == "categorical"
    assert ("x", "Mystery") in snap2.config.manual_columns


def test_set_column_type_rejects_unknown_pair(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found in discover"):
        set_column_type(
            tmp_path,
            "x",
            "DoesNotExist",
            "id",
            expected_version=snap.snapshot_version,
        )


def test_set_column_type_rejects_unknown_type(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="expected one of"):
        set_column_type(
            tmp_path,
            "x",
            "LopNr",
            "blob",
            expected_version=snap.snapshot_version,
        )


def test_set_column_type_hint_unchanged_dropped_when_invalid(tmp_path: Path):
    """date → numeric: existing date_format hint becomes invalid; UNCHANGED
    drops it silently rather than carrying a junk field."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "BirthDate", "sql_type": "DATE"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_type(
        tmp_path,
        "x",
        "BirthDate",
        "date",
        hint={"date_format": "%Y%m%d"},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_types["x"]["BirthDate"].date_format == "%Y%m%d"
    snap = set_column_type(
        tmp_path,
        "x",
        "BirthDate",
        "numeric",
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_types["x"]["BirthDate"].date_format is None
    assert snap.config.column_types["x"]["BirthDate"].numeric_subtype is None


def test_set_column_type_hint_none_clears(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Salary", "sql_type": "BIGINT"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_type(
        tmp_path,
        "x",
        "Salary",
        "numeric",
        hint={"numeric_subtype": "integer"},
        expected_version=snap.snapshot_version,
    )
    snap = set_column_type(
        tmp_path,
        "x",
        "Salary",
        "numeric",
        hint=None,
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_types["x"]["Salary"].numeric_subtype is None


def test_set_column_type_rejects_invalid_hint_key(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Salary", "sql_type": "BIGINT"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not valid for type"):
        set_column_type(
            tmp_path,
            "x",
            "Salary",
            "numeric",
            hint={"date_format": "%Y%m%d"},
            expected_version=snap.snapshot_version,
        )


def test_set_column_type_drops_options_on_type_change(tmp_path: Path):
    """Type-specific options must be cleared when the type changes
    (mirrors set_group_register's reclassification path)."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Code", "sql_type": "VARCHAR"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_options(
        tmp_path,
        "x",
        "Code",
        {"suppress_k": 20},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_options["x"]["Code"] == {"suppress_k": 20}

    # Flip type opaque → id; column_options should be dropped.
    snap = set_column_type(
        tmp_path,
        "x",
        "Code",
        "id",
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_types["x"]["Code"].type == "id"
    assert "x" not in snap.config.column_options


def test_set_column_type_preserves_options_when_type_unchanged(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Code", "sql_type": "VARCHAR"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_options(
        tmp_path,
        "x",
        "Code",
        {"suppress_k": 20},
        expected_version=snap.snapshot_version,
    )
    # Re-assert the existing type — options should survive.
    snap = set_column_type(
        tmp_path,
        "x",
        "Code",
        "opaque",
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_options["x"]["Code"] == {"suppress_k": 20}


# -- set_group_register ----------------------------------------------------


def test_set_group_register_drops_options_on_type_change(tmp_path: Path, monkeypatch):
    """Per session decision: type change during reclassify drops options."""
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "src1",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    # Set a column option on Mystery (currently opaque).
    snap = set_column_options(
        tmp_path,
        "src1",
        "Mystery",
        {"suppress_k": 20},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_options["src1"]["Mystery"] == {"suppress_k": 20}

    # Now have set_group_register reclassify under a register that
    # types Mystery as categorical (via mocked signal).
    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(
        editor,
        "_resolve_signals_for_register",
        lambda reg, cols, db_path: {
            "mystery": RegmetaSignal(
                datatyp_kind=None,
                classification_short_name="SUN2000",
            )
        },
    )

    # Source has no register yet; group_id is "noreg-src1".
    snap = set_group_register(
        tmp_path,
        "noreg-src1",
        "LISA",
        expected_version=snap.snapshot_version,
    )
    # Mystery's type changed opaque → categorical → options dropped.
    assert snap.config.column_types["src1"]["Mystery"].type == "categorical"
    assert "src1" not in snap.config.column_options


def test_set_group_register_preserves_manual_override(tmp_path: Path, monkeypatch):
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "src1",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_type(
        tmp_path,
        "src1",
        "Mystery",
        "numeric",
        expected_version=snap.snapshot_version,
    )

    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(
        editor,
        "_resolve_signals_for_register",
        lambda reg, cols, db_path: {
            "mystery": RegmetaSignal(
                datatyp_kind=None,
                classification_short_name="SUN2000",
            )
        },
    )

    snap = set_group_register(
        tmp_path,
        "noreg-src1",
        "LISA",
        expected_version=snap.snapshot_version,
    )
    # Manual override survives by default.
    assert snap.config.column_types["src1"]["Mystery"].type == "numeric"
    assert ("src1", "Mystery") in snap.config.manual_columns


def test_set_group_register_reclassify_manual_clears_override(
    tmp_path: Path, monkeypatch
):
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "src1",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_type(
        tmp_path,
        "src1",
        "Mystery",
        "numeric",
        expected_version=snap.snapshot_version,
    )

    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(
        editor,
        "_resolve_signals_for_register",
        lambda reg, cols, db_path: {
            "mystery": RegmetaSignal(
                datatyp_kind=None,
                classification_short_name="SUN2000",
            )
        },
    )

    snap = set_group_register(
        tmp_path,
        "noreg-src1",
        "LISA",
        expected_version=snap.snapshot_version,
        reclassify_manual=True,
    )
    # Now reclassified to categorical and removed from manual_columns.
    assert snap.config.column_types["src1"]["Mystery"].type == "categorical"
    assert ("src1", "Mystery") not in snap.config.manual_columns


def test_set_group_register_rejects_unresolvable_register(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="did not resolve"):
        set_group_register(
            tmp_path,
            "noreg-x",
            "BogusRegister",
            expected_version=snap.snapshot_version,
        )


def test_set_group_register_rejects_unknown_group(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found"):
        set_group_register(
            tmp_path,
            "noreg-does_not_exist",
            None,
            expected_version=snap.snapshot_version,
        )


def test_set_group_register_rejects_stale_noreg_for_assigned_source(
    tmp_path: Path, monkeypatch
):
    """A source that already has a register assigned must not match a
    stale `noreg-<name>` group_id — the previous fallthrough silently
    re-assigned the register."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)

    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(
        editor, "_resolve_signals_for_register", lambda reg, cols, db_path: {}
    )

    # First, legitimately assign a register to x.
    snap = set_group_register(
        tmp_path,
        "noreg-x",
        "LISA",
        expected_version=snap.snapshot_version,
    )
    assert snap.config.sources["x"]["register"] == "LISA"

    # Now reusing the stale `noreg-x` group_id must not match.
    with pytest.raises(ValidationError, match="not found"):
        set_group_register(
            tmp_path,
            "noreg-x",
            None,
            expected_version=snap.snapshot_version,
        )


# -- set_source_metadata ---------------------------------------------------


def test_set_source_metadata_year_round_trips(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_source_metadata(
        tmp_path, "x", year=2024, expected_version=snap.snapshot_version
    )
    assert snap.config.sources["x"]["year"] == 2024


def test_set_source_metadata_year_none_means_no_year(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "lisa_2018", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_source_metadata(
        tmp_path, "lisa_2018", year=None, expected_version=snap.snapshot_version
    )
    configured, year = snap.config.source_year("lisa_2018")
    assert (configured, year) == (True, None)


# -- set_column_options ---------------------------------------------------


def test_set_column_options_set_and_clear(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "Salary"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_options(
        tmp_path,
        "x",
        "Salary",
        {"suppress_k": 20},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.lookup_options("x", "Salary") == {"suppress_k": 20}
    snap = set_column_options(
        tmp_path,
        "x",
        "Salary",
        None,
        expected_version=snap.snapshot_version,
    )
    assert snap.config.lookup_options("x", "Salary") == {}


def test_set_column_options_rejects_unknown_key(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "Salary"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="unknown option"):
        set_column_options(
            tmp_path,
            "x",
            "Salary",
            {"sneaky": 1},
            expected_version=snap.snapshot_version,
        )


# -- put_panel / remove_panel ---------------------------------------------


def test_put_panel_adds_and_replaces(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "lisa_2018", "columns": [{"name": "LopNr"}]},
            {"source_name": "lisa_2019", "columns": [{"name": "LopNr"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    panel = Panel(
        panel_id="lisa",
        panel_key="LopNr",
        members=(
            PanelMember(source="lisa_2018", period=2018),
            PanelMember(source="lisa_2019", period=2019),
        ),
    )
    snap = put_panel(tmp_path, panel, expected_version=snap.snapshot_version)
    assert any(p.panel_id == "lisa" for p in snap.config.panels)

    # Replace with a new period set.
    panel2 = Panel(
        panel_id="lisa",
        panel_key="LopNr",
        members=(PanelMember(source="lisa_2018", period=2018),),
    )
    snap = put_panel(tmp_path, panel2, expected_version=snap.snapshot_version)
    panels = [p for p in snap.config.panels if p.panel_id == "lisa"]
    assert len(panels) == 1
    assert len(panels[0].members) == 1


def test_remove_panel(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "lisa_2018", "columns": [{"name": "LopNr"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    panel = Panel(
        panel_id="lisa",
        panel_key="LopNr",
        members=(PanelMember(source="lisa_2018", period=2018),),
    )
    snap = put_panel(tmp_path, panel, expected_version=snap.snapshot_version)
    assert any(p.panel_id == "lisa" for p in snap.config.panels)
    snap = remove_panel(tmp_path, "lisa", expected_version=snap.snapshot_version)
    assert all(p.panel_id != "lisa" for p in snap.config.panels)


def test_put_panel_rejects_source_collision(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "LopNr"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    panel1 = Panel(
        panel_id="p1",
        panel_key="LopNr",
        members=(PanelMember(source="x", period=2018),),
    )
    snap = put_panel(tmp_path, panel1, expected_version=snap.snapshot_version)
    panel2 = Panel(
        panel_id="p2",
        panel_key="LopNr",
        members=(PanelMember(source="x", period=2019),),
    )
    with pytest.raises(ValidationError, match="reference source"):
        put_panel(tmp_path, panel2, expected_version=snap.snapshot_version)


# -- StaleStateError ------------------------------------------------------


def test_stale_state_error_blocks_mutation(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    # Externally mutate the file.
    cfg_path = tmp_path / "mock_data_config.json"
    payload = json.loads(cfg_path.read_text())
    payload["manual_columns"] = [["x", "LopNr"]]
    cfg_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(StaleStateError):
        set_source_metadata(
            tmp_path, "x", year=2024, expected_version=snap.snapshot_version
        )


def test_snapshot_version_changes_after_mutation(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap2 = set_source_metadata(
        tmp_path, "x", year=2024, expected_version=snap.snapshot_version
    )
    assert snap.snapshot_version != snap2.snapshot_version


def test_concurrent_mutations_serialize_without_clobber(tmp_path: Path, monkeypatch):
    """Two threads mutating different columns from the same snapshot
    must serialize via the cross-process file lock. Without the lock,
    both `_verify_version` calls pass against the same on-disk hash and
    the second `os.replace` silently clobbers the first; with the lock,
    one mutation wins and the other raises StaleStateError."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "A"}, {"name": "B"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    v0 = snap.snapshot_version

    # Widen the read+write window so both threads enter the critical
    # section before either replaces the file. The sleep happens after
    # _verify_version but before os.replace, which is the racy window.
    real_atomic_write = editor._atomic_write

    def slow_atomic_write(path, payload):
        time.sleep(0.05)
        real_atomic_write(path, payload)

    monkeypatch.setattr(editor, "_atomic_write", slow_atomic_write)

    barrier = threading.Barrier(2)
    results: dict[str, Any] = {}

    def mutate(key: str, col: str) -> None:
        barrier.wait()
        try:
            results[key] = set_column_type(
                tmp_path, "x", col, "id", expected_version=v0
            )
        except StaleStateError as exc:
            results[key] = exc

    t1 = threading.Thread(target=mutate, args=("a", "A"))
    t2 = threading.Thread(target=mutate, args=("b", "B"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    successes = [v for v in results.values() if not isinstance(v, Exception)]
    failures = [v for v in results.values() if isinstance(v, StaleStateError)]
    assert len(successes) == 1
    assert len(failures) == 1


def test_set_column_options_rejects_unknown_source(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "Salary"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found in discover"):
        set_column_options(
            tmp_path,
            "ghost_source",
            "Salary",
            {"suppress_k": 20},
            expected_version=snap.snapshot_version,
        )


def test_set_column_options_rejects_unknown_column(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "Salary"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found in discover"):
        set_column_options(
            tmp_path,
            "x",
            "ghost_column",
            {"suppress_k": 20},
            expected_version=snap.snapshot_version,
        )


# -- Regmeta-absent graceful behaviour ------------------------------------


def test_init_succeeds_with_no_regmeta(tmp_path: Path):
    """Regmeta DB unavailable (default fixture) — every register is None,
    classifier still runs against sql_type / id-name patterns."""
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "x",
                "columns": [
                    {"name": "LopNr", "sql_type": "BIGINT"},
                    {"name": "Salary", "sql_type": "DECIMAL"},
                ],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    assert snap.config.sources.get("x", {}).get("register") is None
    assert snap.config.column_types["x"]["LopNr"].type == "id"
    assert snap.config.column_types["x"]["Salary"].type == "numeric"


def test_groups_unassigned_become_singletons(tmp_path: Path):
    """Per principle 7: each unassigned source gets its own
    `noreg-<source_name>` group rather than being lumped together."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "a", "columns": [{"name": "LopNr"}]},
            {"source_name": "b", "columns": [{"name": "LopNr"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    group_ids = {g.group_id for g in snap.groups}
    assert group_ids == {"noreg-a", "noreg-b"}


# -- Pre-3.0.0 rejection on get_state -------------------------------------


def test_get_state_rejects_pre_3_0_0_config(tmp_path: Path):
    payload = {"contract_version": "mdw-config-2.0.0"}
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="Regenerate.*editor"):
        get_state(tmp_path)
