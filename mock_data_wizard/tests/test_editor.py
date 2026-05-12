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
    set_source_registers,
    set_source_metadata,
    unset_column_manual_override,
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
        ["x"],
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
            ["x"],
            "DoesNotExist",
            "id",
            expected_version=snap.snapshot_version,
        )


def test_set_column_type_rejects_scalar_string_source(tmp_path: Path):
    """A bare ``str`` satisfies ``Sequence[str]`` structurally; without
    the runtime guard ``list("src")`` would silently iterate per
    character. Caller misuse must fail loudly."""
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not a single str"):
        set_column_type(
            tmp_path,
            "x",  # type: ignore[arg-type]
            "LopNr",
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
            ["x"],
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
        ["x"],
        "BirthDate",
        "date",
        hint={"date_format": "%Y%m%d"},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_types["x"]["BirthDate"].date_format == "%Y%m%d"
    snap = set_column_type(
        tmp_path,
        ["x"],
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
        ["x"],
        "Salary",
        "numeric",
        hint={"numeric_subtype": "integer"},
        expected_version=snap.snapshot_version,
    )
    snap = set_column_type(
        tmp_path,
        ["x"],
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
            ["x"],
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
        ["x"],
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
        ["x"],
        "Code",
        "opaque",
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_options["x"]["Code"] == {"suppress_k": 20}


def test_set_column_type_bulk_applies_to_all_sources(tmp_path: Path):
    """Bulk apply: every targeted source gets the new type, every pair
    lands in manual_columns, and the snapshot_version advances exactly
    once for the whole call."""
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": f"y{yr}",
                "columns": [{"name": "Kommun", "sql_type": "VARCHAR"}],
            }
            for yr in (2018, 2019, 2020)
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    v_before = snap.snapshot_version
    snap = set_column_type(
        tmp_path,
        ["y2018", "y2019", "y2020"],
        "Kommun",
        "id",
        expected_version=v_before,
        hint={"id_subtype": "string"},
    )
    for sn in ("y2018", "y2019", "y2020"):
        entry = snap.config.column_types[sn]["Kommun"]
        assert entry.type == "id"
        assert entry.id_subtype == "string"
        assert (sn, "Kommun") in snap.config.manual_columns
    # One write → one new version.
    assert snap.snapshot_version != v_before


def test_set_column_type_bulk_drops_column_options_per_source(tmp_path: Path):
    """A type change should drop column_options for every targeted source
    that had options, identically to the single-source path."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "a", "columns": [{"name": "Code", "sql_type": "VARCHAR"}]},
            {"source_name": "b", "columns": [{"name": "Code", "sql_type": "VARCHAR"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_options(
        tmp_path,
        "a",
        "Code",
        {"suppress_k": 20},
        expected_version=snap.snapshot_version,
    )
    snap = set_column_options(
        tmp_path,
        "b",
        "Code",
        {"suppress_k": 20},
        expected_version=snap.snapshot_version,
    )
    snap = set_column_type(
        tmp_path,
        ["a", "b"],
        "Code",
        "id",
        expected_version=snap.snapshot_version,
    )
    assert "a" not in snap.config.column_options
    assert "b" not in snap.config.column_options


def test_set_column_type_bulk_atomic_on_bad_pair(tmp_path: Path):
    """If any (source, column) pair is unknown, the entire bulk call
    aborts with no on-disk change."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "a", "columns": [{"name": "Code"}]},
            {"source_name": "b", "columns": [{"name": "Other"}]},  # no "Code"
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    v_before = snap.snapshot_version
    with pytest.raises(ValidationError, match="not found in discover"):
        set_column_type(
            tmp_path,
            ["a", "b"],
            "Code",
            "id",
            expected_version=v_before,
        )
    snap_after = editor.get_state(tmp_path)
    assert snap_after.snapshot_version == v_before
    # `a` was the valid pair but must NOT have been written before the
    # second pair's validation failed: the partial apply would have set
    # type="id" and added the manual marker.
    code_a = snap_after.config.column_types.get("a", {}).get("Code")
    assert code_a is None or code_a.type != "id"
    assert ("a", "Code") not in snap_after.config.manual_columns


def test_set_column_type_rejects_empty_sources(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "C"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="non-empty"):
        set_column_type(tmp_path, [], "C", "id", expected_version=snap.snapshot_version)


def test_set_column_type_rejects_duplicate_sources(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "C"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="duplicates"):
        set_column_type(
            tmp_path,
            ["x", "x"],
            "C",
            "id",
            expected_version=snap.snapshot_version,
        )


def test_set_column_type_noop_does_not_mark_manual(tmp_path: Path):
    """Re-asserting the same type+hint must not promote auto → manual.
    Re-saving without changes is a common cancel-by-mistake; silently
    flipping provenance is surprising."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "LopNr", "sql_type": "BIGINT"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    auto_type = snap.config.column_types["x"]["LopNr"].type
    v_before = snap.snapshot_version
    snap2 = set_column_type(
        tmp_path,
        ["x"],
        "LopNr",
        auto_type,
        expected_version=v_before,
    )
    # Re-asserting the auto type is a no-op: no manual marker, no
    # snapshot bump.
    assert ("x", "LopNr") not in snap2.config.manual_columns
    assert snap2.snapshot_version == v_before


def test_set_column_type_noop_preserves_existing_manual_marker(tmp_path: Path):
    """Re-saving an already-manual cell with the same value keeps the
    manual marker (it was set on a prior actual change)."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_type(
        tmp_path,
        ["x"],
        "Mystery",
        "categorical",
        expected_version=snap.snapshot_version,
    )
    assert ("x", "Mystery") in snap.config.manual_columns
    v_after_first = snap.snapshot_version
    # Re-save same value — marker stays, no version bump.
    snap2 = set_column_type(
        tmp_path,
        ["x"],
        "Mystery",
        "categorical",
        expected_version=v_after_first,
    )
    assert ("x", "Mystery") in snap2.config.manual_columns
    assert snap2.snapshot_version == v_after_first


def test_set_column_type_partial_noop_only_marks_changed_sources(tmp_path: Path):
    """Bulk apply where some sources match and some don't: only the
    sources whose value actually changes get the manual marker."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "a", "columns": [{"name": "K", "sql_type": "VARCHAR"}]},
            {"source_name": "b", "columns": [{"name": "K", "sql_type": "VARCHAR"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    # First, edit only `a` to id (manual).
    snap = set_column_type(
        tmp_path,
        ["a"],
        "K",
        "id",
        expected_version=snap.snapshot_version,
    )
    assert ("a", "K") in snap.config.manual_columns
    assert ("b", "K") not in snap.config.manual_columns
    # Now bulk-apply id to both. `a` is already id → no-op for `a`;
    # `b` changes → manual marker added.
    snap2 = set_column_type(
        tmp_path,
        ["a", "b"],
        "K",
        "id",
        expected_version=snap.snapshot_version,
    )
    assert ("a", "K") in snap2.config.manual_columns
    assert ("b", "K") in snap2.config.manual_columns
    # Snapshot did advance (b changed).
    assert snap2.snapshot_version != snap.snapshot_version


# -- unset_column_manual_override -----------------------------------------


def test_unset_column_manual_override_clears_marker_and_reclassifies(
    tmp_path: Path, monkeypatch
):
    """Removing the manual marker should re-run classification so the
    cell's type returns to the auto value."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    auto_type = snap.config.column_types["x"]["Mystery"].type
    # Force a manual override to a different type.
    snap = set_column_type(
        tmp_path,
        ["x"],
        "Mystery",
        "id",
        hint={"id_subtype": "string"},
        expected_version=snap.snapshot_version,
    )
    assert ("x", "Mystery") in snap.config.manual_columns
    assert snap.config.column_types["x"]["Mystery"].type == "id"
    # Unset.
    snap2 = unset_column_manual_override(
        tmp_path,
        ["x"],
        "Mystery",
        expected_version=snap.snapshot_version,
    )
    assert ("x", "Mystery") not in snap2.config.manual_columns
    assert snap2.config.column_types["x"]["Mystery"].type == auto_type
    # column_options for this cell should be dropped if the type changed.
    assert "Mystery" not in snap2.config.column_options.get("x", {})


def test_unset_column_manual_override_silent_skip_on_non_manual(tmp_path: Path):
    """Pairs that aren't currently manual are silently skipped — no-op,
    no version bump."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "K", "sql_type": "VARCHAR"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    v_before = snap.snapshot_version
    snap2 = unset_column_manual_override(
        tmp_path,
        ["x"],
        "K",
        expected_version=v_before,
    )
    assert snap2.snapshot_version == v_before
    assert ("x", "K") not in snap2.config.manual_columns


def test_unset_column_manual_override_partial_targets(tmp_path: Path):
    """When the call lists a mix of manual and non-manual sources, only
    the manual ones are reclassified; non-manual ones are untouched."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "a", "columns": [{"name": "K", "sql_type": "VARCHAR"}]},
            {"source_name": "b", "columns": [{"name": "K", "sql_type": "VARCHAR"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    # Manual on `a` only.
    snap = set_column_type(
        tmp_path,
        ["a"],
        "K",
        "id",
        expected_version=snap.snapshot_version,
    )
    assert ("a", "K") in snap.config.manual_columns
    # Unset across both; only `a` is affected.
    snap2 = unset_column_manual_override(
        tmp_path,
        ["a", "b"],
        "K",
        expected_version=snap.snapshot_version,
    )
    assert ("a", "K") not in snap2.config.manual_columns
    assert ("b", "K") not in snap2.config.manual_columns
    assert snap2.snapshot_version != snap.snapshot_version


def test_unset_column_manual_override_rejects_empty_sources(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "C"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="non-empty"):
        unset_column_manual_override(
            tmp_path, [], "C", expected_version=snap.snapshot_version
        )


def test_unset_column_manual_override_rejects_unknown_pair(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "C"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found in discover"):
        unset_column_manual_override(
            tmp_path,
            ["x"],
            "DoesNotExist",
            expected_version=snap.snapshot_version,
        )


def test_unset_column_manual_override_rejects_str_source_names(tmp_path: Path):
    """A bare string would split into single chars under ``list(...)`` — guard
    against it the same way ``set_column_type`` does."""
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "C"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="must be a sequence"):
        unset_column_manual_override(
            tmp_path,
            "x",  # type: ignore[arg-type]
            "C",
            expected_version=snap.snapshot_version,
        )


def test_unset_column_manual_override_atomic_on_partial_unknown(tmp_path: Path):
    """Validation is all-or-nothing: a single unknown pair aborts the call
    with no on-disk changes, even when other pairs would succeed."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "a", "columns": [{"name": "K", "sql_type": "VARCHAR"}]},
            {"source_name": "b", "columns": [{"name": "K", "sql_type": "VARCHAR"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = set_column_type(
        tmp_path, ["a"], "K", "id", expected_version=snap.snapshot_version
    )
    v_before = snap.snapshot_version
    with pytest.raises(ValidationError, match="not found in discover"):
        unset_column_manual_override(
            tmp_path,
            ["a", "ghost"],
            "K",
            expected_version=v_before,
        )
    # Marker on `a` is preserved; no partial apply.
    snap_after = get_state(tmp_path)
    assert snap_after.snapshot_version == v_before
    assert ("a", "K") in snap_after.config.manual_columns


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
        lambda reg, cols, db_path, **_kw: {
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
        ["src1"],
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
        lambda reg, cols, db_path, **_kw: {
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
        ["src1"],
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
        lambda reg, cols, db_path, **_kw: {
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
        editor, "_resolve_signals_for_register", lambda reg, cols, db_path, **_kw: {}
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


# -- set_source_registers --------------------------------------------------


def _multi_source_project(tmp_path: Path) -> Path:
    """Three sources, all initialised with no register."""
    return _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            },
            {
                "source_name": "lisa_2019",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            },
            {
                "source_name": "lisa_aux",
                "columns": [{"name": "Mystery", "sql_type": "VARCHAR"}],
            },
        ],
    )


def test_set_source_registers_partial_exclusion(tmp_path: Path, monkeypatch):
    """Excluding one source from a group clears its register; the
    remaining sources receive the new (or unchanged) register."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})

    # First assign LISA to all three.
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA", "lisa_2019": "LISA", "lisa_aux": "LISA"},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.sources["lisa_2018"]["register"] == "LISA"
    assert snap.config.sources["lisa_aux"]["register"] == "LISA"

    # Then peel `lisa_aux` off — it should land in its own noreg group.
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA", "lisa_2019": "LISA", "lisa_aux": None},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.sources["lisa_2018"]["register"] == "LISA"
    assert snap.config.sources["lisa_2019"]["register"] == "LISA"
    assert snap.config.sources["lisa_aux"].get("register") in (None, "")
    group_ids = {g.group_id for g in snap.groups}
    assert "noreg-lisa_aux" in group_ids


def test_set_source_registers_atomic_on_validation_error(tmp_path: Path, monkeypatch):
    """A single unresolvable register aborts the whole call before any
    write. The on-disk file is unchanged."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    initial_version = snap.snapshot_version

    def selective_resolve(name, db_path=None):
        if name == "LISA":
            return editor.Register(id=34, name="LISA")
        return None

    monkeypatch.setattr(editor, "resolve_register", selective_resolve)
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})

    with pytest.raises(ValidationError, match="did not resolve"):
        set_source_registers(
            tmp_path,
            {"lisa_2018": "LISA", "lisa_2019": "BogusRegister"},
            expected_version=snap.snapshot_version,
        )
    # No change on disk.
    snap = get_state(tmp_path)
    assert snap.snapshot_version == initial_version
    assert snap.config.sources.get("lisa_2018", {}).get("register") in (None, "")


def test_set_source_registers_rejects_unknown_source(tmp_path: Path):
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found"):
        set_source_registers(
            tmp_path,
            {"phantom_source": None},
            expected_version=snap.snapshot_version,
        )


def test_set_source_registers_no_op_returns_same_version(tmp_path: Path, monkeypatch):
    """All assignments equal current + no reclassify_manual → no write,
    snapshot_version stable. Matches set_column_type's idempotency."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    initial_version = snap.snapshot_version
    # All sources start with no register; assigning None to all is a no-op.
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": None, "lisa_2019": None, "lisa_aux": None},
        expected_version=initial_version,
    )
    assert snap.snapshot_version == initial_version


def test_set_source_registers_reclassify_only_changed(tmp_path: Path, monkeypatch):
    """Reclassification runs only on sources whose register actually
    changed. A source whose register stays the same is not touched even
    if the same name appears in the assignments dict."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})

    # First set `lisa_2018` to LISA.
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA"},
        expected_version=snap.snapshot_version,
    )

    # Now mark Mystery on lisa_2018 manually; ensure the next call with
    # an unchanged register preserves it.
    snap = set_column_type(
        tmp_path,
        ["lisa_2018"],
        "Mystery",
        "numeric",
        expected_version=snap.snapshot_version,
    )
    assert ("lisa_2018", "Mystery") in snap.config.manual_columns

    # Re-asserting the same register value on `lisa_2018` while flipping
    # `lisa_2019` to LISA should not touch lisa_2018's manual.
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA", "lisa_2019": "LISA"},
        expected_version=snap.snapshot_version,
    )
    assert snap.config.column_types["lisa_2018"]["Mystery"].type == "numeric"
    assert ("lisa_2018", "Mystery") in snap.config.manual_columns
    assert snap.config.sources["lisa_2019"]["register"] == "LISA"


def test_set_source_registers_reclassify_manual_noop_without_manuals(
    tmp_path: Path, monkeypatch
):
    """reclassify_manual=True with no register change AND no manual
    overrides in the requested sources is a no-op — the flag's only
    observable effect is dropping manual overrides, so without any to
    drop the snapshot must stay stable."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA"},
        expected_version=snap.snapshot_version,
    )
    stable_version = snap.snapshot_version
    # No manuals, no register move — should not advance the version.
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA"},
        expected_version=stable_version,
        reclassify_manual=True,
    )
    assert snap.snapshot_version == stable_version


def test_set_source_registers_reclassify_manual_force(tmp_path: Path, monkeypatch):
    """With reclassify_manual=True, an unchanged-register source still
    gets its manual columns reclassified — matches set_group_register."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(
        editor,
        "_resolve_signals_for_register",
        lambda reg, cols, db_path, **_kw: {
            "mystery": RegmetaSignal(
                datatyp_kind=None,
                classification_short_name="SUN2000",
            )
        },
    )
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA"},
        expected_version=snap.snapshot_version,
    )
    snap = set_column_type(
        tmp_path,
        ["lisa_2018"],
        "Mystery",
        "numeric",
        expected_version=snap.snapshot_version,
    )
    assert ("lisa_2018", "Mystery") in snap.config.manual_columns

    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA"},
        expected_version=snap.snapshot_version,
        reclassify_manual=True,
    )
    # Re-classified to categorical and dropped from manual_columns.
    assert snap.config.column_types["lisa_2018"]["Mystery"].type == "categorical"
    assert ("lisa_2018", "Mystery") not in snap.config.manual_columns


def test_set_source_registers_preserves_panel_membership(tmp_path: Path, monkeypatch):
    """A source's panel slot does not move when its register is cleared
    — panels live independently of register."""
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    fake_register = editor.Register(id=34, name="LISA")
    monkeypatch.setattr(
        editor, "resolve_register", lambda name, db_path=None: fake_register
    )
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})
    snap = set_source_registers(
        tmp_path,
        {"lisa_2018": "LISA", "lisa_2019": "LISA", "lisa_aux": "LISA"},
        expected_version=snap.snapshot_version,
    )
    snap = put_panel(
        tmp_path,
        Panel(
            panel_id="lisa_main",
            entity_key="LopNr",
            members=(
                PanelMember(source="lisa_2018", time_key=2018),
                PanelMember(source="lisa_2019", time_key=2019),
                PanelMember(source="lisa_aux", time_key=2020),
            ),
        ),
        expected_version=snap.snapshot_version,
    )

    # Peel lisa_aux off — its panel slot must survive.
    snap = set_source_registers(
        tmp_path,
        {"lisa_aux": None},
        expected_version=snap.snapshot_version,
    )
    panel = next(p for p in snap.config.panels if p.panel_id == "lisa_main")
    assert {m.source for m in panel.members} == {
        "lisa_2018",
        "lisa_2019",
        "lisa_aux",
    }


def test_set_source_registers_rejects_invalid_assignments_shape(tmp_path: Path):
    discover_path = _multi_source_project(tmp_path)
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="non-empty"):
        set_source_registers(
            tmp_path,
            {},
            expected_version=snap.snapshot_version,
        )
    with pytest.raises(ValidationError, match="must be a string or None"):
        set_source_registers(
            tmp_path,
            {"lisa_2018": 42},  # type: ignore[dict-item]
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
        entity_key="LopNr",
        members=(
            PanelMember(source="lisa_2018", time_key=2018),
            PanelMember(source="lisa_2019", time_key=2019),
        ),
    )
    snap = put_panel(tmp_path, panel, expected_version=snap.snapshot_version)
    assert any(p.panel_id == "lisa" for p in snap.config.panels)

    # Replace with a new period set.
    panel2 = Panel(
        panel_id="lisa",
        entity_key="LopNr",
        members=(PanelMember(source="lisa_2018", time_key=2018),),
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
        entity_key="LopNr",
        members=(PanelMember(source="lisa_2018", time_key=2018),),
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
        entity_key="LopNr",
        members=(PanelMember(source="x", time_key=2018),),
    )
    snap = put_panel(tmp_path, panel1, expected_version=snap.snapshot_version)
    panel2 = Panel(
        panel_id="p2",
        entity_key="LopNr",
        members=(PanelMember(source="x", time_key=2019),),
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
                tmp_path, ["x"], col, "id", expected_version=v0
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


# -- Review-order sort (issue #68) ----------------------------------------


def test_groups_sorted_by_review_need(tmp_path: Path, monkeypatch):
    """Cards needing the most review surface first: confidence tier asc
    (none → partial → high), then descending unmatched-categorical count,
    then register_name ascending as the stable tertiary key.

    Setup: two register groups (one HIGH, one PARTIAL) plus a noreg
    singleton (always confidence "none"). The tier ordering puts the
    noreg group first, the partial group second, the high group last."""
    from mock_data_wizard.registers import Register

    monkeypatch.setattr(
        editor,
        "_autodetect_register_per_source",
        lambda discover, db_path: {
            "lisa_2018": "LISA",
            "par_2018": "PAR",
            "loose": None,
        },
    )
    # LISA: Kon has a regmeta classification → high confidence.
    # PAR : Kon has no signal → partial confidence (Salary matches, Kon does not).
    monkeypatch.setattr(
        editor,
        "_resolve_signals_for_register",
        lambda register, cols, db_path, **_kw: (
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
            else {
                "salary": RegmetaSignal(
                    datatyp_kind="numeric",
                    classification_short_name=None,
                    has_value_codes=False,
                ),
            }
        ),
    )
    monkeypatch.setattr(
        editor,
        "resolve_register",
        lambda name, db_path=None: (
            Register(id=1, name="LISA")
            if name == "LISA"
            else Register(id=2, name="PAR")
            if name == "PAR"
            else None
        ),
    )

    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr"},
                    {"name": "Salary", "sql_type": "DECIMAL"},
                    {"name": "Kon", "sql_type": "VARCHAR"},
                ],
            },
            {
                "source_name": "par_2018",
                "columns": [
                    {"name": "LopNr"},
                    {"name": "Salary", "sql_type": "DECIMAL"},
                    {"name": "Kon", "sql_type": "VARCHAR"},
                ],
            },
            {"source_name": "loose", "columns": [{"name": "LopNr"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)

    assert [g.group_id for g in snap.groups] == [
        "noreg-loose",  # tier "none"
        "reg-2",  # tier "partial" (PAR)
        "reg-1",  # tier "high" (LISA)
    ]


def test_review_sort_key_orders_within_tier():
    """Within one confidence tier: descending unmatched-categorical count
    first, then register_name (or group_id) ascending as the stable
    tertiary key."""
    from mock_data_wizard.editor import _review_sort_key

    def _g(
        group_id: str,
        register_name: str | None,
        confidence: str,
        unmatched_cats: int,
    ):
        # Pad with as many uncategorised-categorical ColumnInfos as we
        # need to inflate the unmatched count; current_type "categorical"
        # + no regmeta_signal is the predicate's positive case.
        cols = tuple(
            editor.ColumnInfo(
                name=f"c{i}",
                sql_type="VARCHAR",
                current_type="categorical",
                hint=None,
                provenance="auto",
                regmeta_signal=None,
                regmeta_implied_type=None,
            )
            for i in range(unmatched_cats)
        )
        return editor.RegisterGroupView(
            group_id=group_id,
            register_id=None,
            register_name=register_name,
            confidence=confidence,  # type: ignore[arg-type]
            sources=("s",),
            columns_by_source={"s": cols},
            schema_variants=0,
            panel_candidate=None,
            member_hints={},
        )

    groups = [
        _g("reg-3", "LISA", "partial", unmatched_cats=1),
        _g("reg-1", "BEFOLKNING", "partial", unmatched_cats=5),
        _g("reg-2", "FASTPAK", "partial", unmatched_cats=5),
        _g("reg-4", "HIGHREG", "high", unmatched_cats=99),  # tier dominates
    ]
    ordered = sorted(groups, key=_review_sort_key)
    # reg-1 and reg-2 tie on unmatched=5; register_name breaks the tie.
    # reg-3 has fewer unmatched within the same tier so it sorts last
    # among "partial". The "high"-tier reg-4 sorts last overall.
    assert [g.group_id for g in ordered] == ["reg-1", "reg-2", "reg-3", "reg-4"]


# -- Pre-3.0.0 rejection on get_state -------------------------------------


def test_get_state_rejects_pre_3_0_0_config(tmp_path: Path):
    payload = {"contract_version": "mdw-config-2.0.0"}
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="Regenerate.*editor"):
        get_state(tmp_path)


# -- Cross-platform import + Windows-mutation diagnostics -----------------


def test_config_lock_raises_clear_error_when_fcntl_missing(tmp_path: Path, monkeypatch):
    """On Windows, ``fcntl`` is unavailable. Read paths must still work
    (the lock is only acquired on mutation); when a mutator tries to
    acquire the lock, it must fail with a clear, actionable message
    rather than ``ModuleNotFoundError: No module named 'fcntl'``."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "fcntl":
            raise ModuleNotFoundError("No module named 'fcntl'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(NotImplementedError, match="POSIX"):
        with editor._config_lock(tmp_path):
            pass


# -- set_source_metadata source validation --------------------------------


def test_set_source_metadata_rejects_unknown_source(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path, [{"source_name": "x", "columns": [{"name": "LopNr"}]}]
    )
    snap = init_if_missing(tmp_path, discover_path)
    with pytest.raises(ValidationError, match="not found in discover"):
        set_source_metadata(
            tmp_path,
            "ghost_source",
            year=2024,
            expected_version=snap.snapshot_version,
        )


# -- Discover-hash determinism --------------------------------------------


def test_compute_discover_hash_is_order_invariant():
    """Hash must depend only on (source_name, [(col, sql_type)]) sets,
    not on insertion order of sources or columns. Otherwise a re-run of
    discover that emits the same content in a different order would
    spuriously trigger ``discover_drift``."""
    payload_a = _discover(
        [
            {
                "source_name": "a",
                "columns": [
                    {"name": "LopNr", "sql_type": "BIGINT"},
                    {"name": "Salary", "sql_type": "DECIMAL"},
                ],
            },
            {"source_name": "b", "columns": [{"name": "Year", "sql_type": "INT"}]},
        ]
    )
    payload_b = _discover(
        [
            {"source_name": "b", "columns": [{"name": "Year", "sql_type": "INT"}]},
            {
                "source_name": "a",
                "columns": [
                    {"name": "Salary", "sql_type": "DECIMAL"},
                    {"name": "LopNr", "sql_type": "BIGINT"},
                ],
            },
        ]
    )
    assert editor._compute_discover_hash(payload_a) == editor._compute_discover_hash(
        payload_b
    )


def test_compute_discover_hash_ignores_row_count_and_nullable():
    """row_count, nullable, and source_detail vary across MONA runs but
    don't change the schema; the hash must ignore them."""
    base = {
        "source_name": "x",
        "columns": [{"name": "LopNr", "sql_type": "BIGINT"}],
    }
    payload_a = _discover([base])
    payload_b = _discover(
        [
            {
                **base,
                "row_count": 1_000_000,
                "source_detail": {"year": 2018},
                "columns": [{"name": "LopNr", "sql_type": "BIGINT", "nullable": True}],
            }
        ]
    )
    assert editor._compute_discover_hash(payload_a) == editor._compute_discover_hash(
        payload_b
    )


def test_compute_discover_hash_changes_on_schema_change():
    """A column rename or sql_type change must flip the hash so
    ``discover_drift`` fires."""
    payload_a = _discover(
        [{"source_name": "x", "columns": [{"name": "LopNr", "sql_type": "BIGINT"}]}]
    )
    payload_b = _discover(
        [{"source_name": "x", "columns": [{"name": "LopNr", "sql_type": "INT"}]}]
    )
    assert editor._compute_discover_hash(payload_a) != editor._compute_discover_hash(
        payload_b
    )


# -- parse_panel_payload --------------------------------------------------


def test_parse_panel_payload_round_trips():
    raw = {
        "panel_id": "lisa",
        "entity_key": "LopNr",
        "members": [
            {"source": "lisa_2018", "time_key": 2018},
            {"source": "lisa_2019", "time_key": 2019},
        ],
    }
    panel = editor.parse_panel_payload(raw)
    assert panel.panel_id == "lisa"
    assert panel.entity_key == "LopNr"
    assert tuple(m.source for m in panel.members) == ("lisa_2018", "lisa_2019")
    assert tuple(m.time_key for m in panel.members) == (2018, 2019)


def test_parse_panel_payload_rejects_unknown_keys():
    raw = {
        "panel_id": "p",
        "entity_key": "k",
        "members": [{"source": "a", "time_key": 2020}],
        "extra": "noise",
    }
    with pytest.raises(ValidationError, match="unknown key"):
        editor.parse_panel_payload(raw)


def test_parse_panel_payload_rejects_member_without_time_key():
    raw = {
        "panel_id": "p",
        "entity_key": "k",
        "members": [{"source": "a"}],
    }
    with pytest.raises(ValidationError, match="missing required key 'time_key'"):
        editor.parse_panel_payload(raw)


# -- put_panel rename via previous_panel_id ------------------------------


def test_put_panel_rename_drops_previous_id(tmp_path: Path):
    """Renaming a panel via ``previous_panel_id`` must atomically drop
    the old entry — otherwise the source-overlap check rejects the write."""
    discover_path = _write_discover(
        tmp_path,
        [
            {"source_name": "lisa_2018", "columns": [{"name": "LopNr"}]},
            {"source_name": "lisa_2019", "columns": [{"name": "LopNr"}]},
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    original = Panel(
        panel_id="lisa",
        entity_key="LopNr",
        members=(
            PanelMember(source="lisa_2018", time_key=2018),
            PanelMember(source="lisa_2019", time_key=2019),
        ),
    )
    snap = put_panel(tmp_path, original, expected_version=snap.snapshot_version)
    renamed = Panel(
        panel_id="lisa_v2",
        entity_key="LopNr",
        members=original.members,
    )
    snap = put_panel(
        tmp_path,
        renamed,
        expected_version=snap.snapshot_version,
        previous_panel_id="lisa",
    )
    panel_ids = [p.panel_id for p in snap.config.panels]
    assert panel_ids == ["lisa_v2"]


def test_put_panel_previous_panel_id_equal_to_new_is_noop(tmp_path: Path):
    """When ``previous_panel_id == panel_id`` the rename branch must not
    fire (otherwise we'd drop the panel and immediately re-add it, which
    is wasteful but more importantly hides bugs around in-place edits)."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "LopNr"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    panel = Panel(
        panel_id="p",
        entity_key="LopNr",
        members=(PanelMember(source="x", time_key=2018),),
    )
    snap = put_panel(tmp_path, panel, expected_version=snap.snapshot_version)
    snap = put_panel(
        tmp_path,
        Panel(
            panel_id="p",
            entity_key="LopNr",
            members=(PanelMember(source="x", time_key=2019),),
        ),
        expected_version=snap.snapshot_version,
        previous_panel_id="p",
    )
    panels = [p for p in snap.config.panels if p.panel_id == "p"]
    assert len(panels) == 1
    assert panels[0].members[0].time_key == 2019


def test_put_panel_previous_panel_id_unknown_id_is_silent(tmp_path: Path):
    """Renaming-from a nonexistent id is silently a plain insert. Locks
    in a forgiving contract: a UI race that double-fires the rename
    shouldn't 500."""
    discover_path = _write_discover(
        tmp_path,
        [{"source_name": "x", "columns": [{"name": "LopNr"}]}],
    )
    snap = init_if_missing(tmp_path, discover_path)
    snap = put_panel(
        tmp_path,
        Panel(
            panel_id="new",
            entity_key="LopNr",
            members=(PanelMember(source="x", time_key=2018),),
        ),
        expected_version=snap.snapshot_version,
        previous_panel_id="never_existed",
    )
    assert [p.panel_id for p in snap.config.panels] == ["new"]


def test_put_panel_accepts_manual_designation_with_nonstandard_time_key(
    tmp_path: Path,
):
    """Manual designation flow: the auto-detector misses time-key columns
    that aren't named AR/INDATUM/year/period, but ``put_panel`` must
    still accept a hand-built panel pointing at any column name. Mirrors
    the issue-63 case where the user designates a panel for a source
    whose time column is e.g. ``tax_year``."""
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "tax_history",
                "columns": [
                    {"name": "LopNr"},
                    {"name": "tax_year"},
                ],
            }
        ],
    )
    snap = init_if_missing(tmp_path, discover_path)
    # No candidate detected for this group (no date token, no recognised
    # time-key column). The user designates a panel anyway.
    assert snap.groups[0].panel_candidate is None
    snap = put_panel(
        tmp_path,
        Panel(
            panel_id="tax_panel",
            entity_key="LopNr",
            members=(PanelMember(source="tax_history", time_key="tax_year"),),
        ),
        expected_version=snap.snapshot_version,
    )
    [panel] = [p for p in snap.config.panels if p.panel_id == "tax_panel"]
    assert panel.entity_key == "LopNr"
    assert panel.members[0].time_key == "tax_year"


# -- get_column_values ---------------------------------------------------


def test_get_column_values_validation_error_on_empty_column():
    with pytest.raises(ValidationError, match="non-empty string"):
        editor.get_column_values("TESTREG", "")


def test_get_column_values_returns_none_when_regmeta_missing(monkeypatch):
    """When ``_open_regmeta_conn`` yields None, the function must return
    ``kind="none"`` rather than raise — the popover then shows the empty
    state. Mirrors the "regmeta degrades gracefully" stance elsewhere."""
    from contextlib import contextmanager

    @contextmanager
    def _no_conn(_):
        yield None

    monkeypatch.setattr(editor, "_open_regmeta_conn", _no_conn)
    result = editor.get_column_values("TESTREG", "Kon")
    assert result.kind == "none"
    assert result.title == "Kon"
    assert result.codes == ()


def test_get_column_values_returns_none_when_register_unresolved(
    regmeta_db: Path,
):
    result = editor.get_column_values(
        "DOES_NOT_EXIST", "Kon", db_path=regmeta_db.parent
    )
    assert result.kind == "none"


def test_get_column_values_returns_none_when_register_is_none(regmeta_db: Path):
    """``register=None`` (no register set on the group) returns the empty
    envelope so the UI can render a clean "no codes" state."""
    result = editor.get_column_values(None, "Kon", db_path=regmeta_db.parent)
    assert result.kind == "none"


def test_get_column_values_returns_none_for_unknown_column(regmeta_db: Path):
    result = editor.get_column_values(
        "TESTREG", "NotAColumn", db_path=regmeta_db.parent
    )
    assert result.kind == "none"


def test_get_column_values_per_instance_path(regmeta_db: Path):
    """The fixture has Kon under TESTREG with codes 1=Man, 2=Kvinna and
    no classification attached → per-instance values path."""
    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    assert result.title == "Kon"
    code_map = {c.code: c.label for c in result.codes}
    assert code_map == {"1": "Man", "2": "Kvinna"}


def test_get_column_values_dedupes_duplicate_codes(regmeta_db: Path):
    """Same vardekod with two distinct labels (year-over-year relabel)
    must collapse to one row in the result — the table renderer keys on
    code and would crash on duplicates."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a second cvid for var_id=44 under the same register with a
    # different label for code "1" (e.g. relabel from "Man" to "Male").
    conn.executescript(
        """
        INSERT INTO register_version (regver_id, regvar_id, registerversionnamn)
            VALUES (101, 10, '2021');
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva)
            VALUES (1002, 1, 10, 101, 44, 'int', '1', 'Kön', '1');
        INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1002, 'Kon');
        """
    )
    assign_value_set(conn, 1002, [("1", "Male"), ("2", "Female")])
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    codes = [c.code for c in result.codes]
    # Each code appears exactly once even though SQL DISTINCT yields four
    # (vardekod, vardebenamning) rows.
    assert codes == sorted(set(codes))
    assert set(codes) == {"1", "2"}


def test_get_column_values_handles_prefix_fallback_alias(regmeta_db: Path):
    """A MONA-prefixed column name (e.g. ``P1105_Kon``) must resolve to
    the alias ``Kon`` via prefix-strip — both the signal lookup AND the
    per-instance SQL must use the same resolved alias."""
    result = editor.get_column_values("TESTREG", "P1105_Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    code_map = {c.code: c.label for c in result.codes}
    assert code_map == {"1": "Man", "2": "Kvinna"}


def test_get_column_values_classification_path(regmeta_db: Path):
    """When a classification short_name is attached to the variable
    instance, the canonical classification code list wins over per-
    instance value codes."""
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (1, 'KON_CLS', 'Kön klassifikation',
                    'Standard sex classification', 2);
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 1, code_id, NULL, 1 FROM value_code
            WHERE vardekod IN ('1', '2');
        UPDATE variable_instance SET classification_id = 1 WHERE cvid = 1001;
        """
    )
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "classification"
    assert result.title == "KON_CLS"
    assert result.description == "Standard sex classification"
    code_map = {c.code: c.label for c in result.codes}
    assert code_map == {"1": "Man", "2": "Kvinna"}


# -- variance signal (issue #64) -----------------------------------------


def _add_extra_cvid(
    conn,
    cvid: int,
    regver_id: int,
    regversion_name: str,
    classification_id: int | None = None,
    value_set_id: int | None = None,
) -> None:
    """Test helper. Add a second cvid for var_id=44 (the conftest Kön
    fixture) under TESTREG so we can simulate multi-year columns."""
    conn.execute(
        "INSERT INTO register_version (regver_id, regvar_id, registerversionnamn) "
        "VALUES (?, 10, ?)",
        (regver_id, regversion_name),
    )
    conn.execute(
        "INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id, "
        "var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva, "
        "classification_id, value_set_id) "
        "VALUES (?, 1, 10, ?, 44, 'int', '1', 'Kön', '1', ?, ?)",
        (cvid, regver_id, classification_id, value_set_id),
    )
    conn.execute(
        "INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (?, 'Kon')", (cvid,)
    )


def test_regmeta_signal_counts_distinct_value_sets_and_classifications(
    regmeta_db: Path,
):
    """The fixture has one cvid with one value_set; n_value_sets=1,
    n_classifications=0 (no classification attached)."""
    import sqlite3

    from mock_data_wizard.classify import _regmeta_lookup

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    signals = _regmeta_lookup(conn, {"Kon"}, [1])
    conn.close()
    sig = signals["kon"]
    assert sig.n_value_sets == 1
    assert sig.n_classifications == 0
    assert sig.has_value_codes is True


def test_regmeta_signal_counts_multiple_value_sets_and_classifications(
    regmeta_db: Path,
):
    """Two cvids with two distinct value_sets and two distinct
    classifications → n_value_sets=2, n_classifications=2."""
    import sqlite3

    from mock_data_wizard.classify import _regmeta_lookup
    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (1, 'CLS_A', 'A', 'desc', 2);
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (2, 'CLS_B', 'B', 'desc', 2);
        UPDATE variable_instance SET classification_id = 1 WHERE cvid = 1001;
        """
    )
    _add_extra_cvid(
        conn, cvid=1002, regver_id=101, regversion_name="2021", classification_id=2
    )
    # Distinct value_set for the new cvid: different label so the hash
    # diverges.
    assign_value_set(conn, 1002, [("1", "Male"), ("2", "Female")])
    conn.commit()
    signals = _regmeta_lookup(conn, {"Kon"}, [1])
    conn.close()
    sig = signals["kon"]
    assert sig.n_value_sets == 2
    assert sig.n_classifications == 2
    # most-common is undefined when counts tie; assert it's one of them.
    assert sig.classification_short_name in {"CLS_A", "CLS_B"}


def test_get_column_values_tier_1_no_variance(regmeta_db: Path):
    """Single value_set, no classification → tier 1, no note."""
    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    assert result.tier == "1"
    assert result.note is None
    assert result.classifications == ()
    assert result.picked_classification is None


def test_get_column_values_tier_2_multiple_value_sets_no_collision(
    regmeta_db: Path,
):
    """Two value_sets that share labels (no collision) → tier 2.

    Default rendering is the union; ``value_sets`` carries both groups
    in chronological order so the popup can show a picker.
    """
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="2021")
    # Widen the code set: add code "3" with a fresh label.
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("3", "Annat")])
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    assert result.tier == "2"
    assert result.note is not None
    assert "Value code sets differ" in result.note
    assert {c.code for c in result.codes} == {"1", "2", "3"}
    # Tier 2 default = union (no value-set filter applied).
    assert result.picked_value_set is None
    # Two value-set groups, ordered chronologically by year_min.
    assert len(result.value_sets) == 2
    assert [g.year_min for g in result.value_sets] == [2020, 2021]
    assert result.value_sets[0].cvid_count == 1
    assert result.value_sets[1].cvid_count == 1


def test_get_column_values_tier_3a_label_collision(regmeta_db: Path):
    """Same vardekod with two distinct vardebenamning → tier 3a.

    Default rendering is the most-common value-set (most cvids) so the
    labels are self-consistent. Tie on cvid count → chronological order
    via ``value_sets`` sort key, so the earlier year wins.
    """
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="2021")
    # Same codes, different labels (year-over-year relabel).
    assign_value_set(conn, 1002, [("1", "Male"), ("2", "Female")])
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    assert result.tier == "3a"
    assert result.note is not None
    assert "different meanings" in result.note
    # Default = a concrete value-set, not the union.
    assert result.picked_value_set is not None
    # Labels rendered are self-consistent (no mix of Man/Male).
    code_map = {c.code: c.label for c in result.codes}
    assert code_map in (
        {"1": "Man", "2": "Kvinna"},
        {"1": "Male", "2": "Female"},
    )
    assert len(result.value_sets) == 2


def test_get_column_values_tier_3a_requires_multiple_value_sets(
    regmeta_db: Path,
):
    """Tier 3a's note tells the user to pick another value-set below —
    only actionable when there's more than one set. Within one set,
    label divergence per code (e.g. SCB has multiple value_code rows
    sharing one vardekod with different vardebenamning) can't be
    resolved via picking; the response must drop to tier 1 instead of
    advertising a non-existent picker.
    """
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Within the existing single value_set (cvid=1001), attach a second
    # value_code row for vardekod "1" with a different label. This
    # represents within-set divergence: same vardekod, different
    # vardebenamning, both in value_set_id assigned to cvid 1001.
    # Determine the existing value_set_id from the fixture.
    row = conn.execute(
        "SELECT value_set_id FROM variable_instance WHERE cvid = 1001"
    ).fetchone()
    value_set_id = row["value_set_id"]
    conn.execute(
        "INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (?, ?, ?)",
        (9001, "1", "Male"),
    )
    conn.execute(
        "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
        (value_set_id, 9001),
    )
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "values"
    # With only one value_set, picking can't help — must NOT advertise
    # 3a's picker note.
    assert result.tier == "1"
    assert result.note is None
    assert len(result.value_sets) == 1


def test_get_column_values_tier_3b_classification_picker(regmeta_db: Path):
    """Two distinct classifications across years → tier 3b with picker."""
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (1, 'CLS_A', 'A', 'Class A', 2);
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (2, 'CLS_B', 'B', 'Class B', 2);
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 1, code_id, NULL, 1 FROM value_code WHERE vardekod IN ('1', '2');
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 2, code_id, NULL, 1 FROM value_code WHERE vardekod IN ('1', '2');
        UPDATE variable_instance SET classification_id = 1 WHERE cvid = 1001;
        """
    )
    _add_extra_cvid(
        conn, cvid=1002, regver_id=101, regversion_name="2021", classification_id=2
    )
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "classification"
    assert result.tier == "3b"
    assert result.note is not None
    assert "Showing" in result.note
    # Picker shows both classifications. Sort key is (year_min is None,
    # year_min, short_name) — fixture has CLS_A on regver "2020" and
    # CLS_B on regver "2021", so CLS_A wins on year_min.
    assert [g.short_name for g in result.classifications] == ["CLS_A", "CLS_B"]
    assert result.classifications[0].year_min == 2020
    assert result.classifications[0].year_max == 2020
    assert result.classifications[1].year_min == 2021
    assert result.classifications[1].year_max == 2021
    assert result.picked_classification in {"CLS_A", "CLS_B"}


def test_get_column_values_picked_classification_honored(regmeta_db: Path):
    """User picks the non-default classification; the popup re-fetches
    that classification's codes."""
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a third code that only lives in CLS_B so we can prove we
    # rendered the right classification.
    conn.executescript(
        """
        INSERT INTO value_code (vardekod, vardebenamning) VALUES ('9', 'OnlyB');
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (1, 'CLS_A', 'A', 'Class A', 2);
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (2, 'CLS_B', 'B', 'Class B', 3);
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 1, code_id, NULL, 1 FROM value_code WHERE vardekod IN ('1', '2');
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 2, code_id, NULL, 1 FROM value_code WHERE vardekod IN ('1', '2', '9');
        UPDATE variable_instance SET classification_id = 1 WHERE cvid = 1001;
        """
    )
    _add_extra_cvid(
        conn, cvid=1002, regver_id=101, regversion_name="2021", classification_id=2
    )
    conn.commit()
    conn.close()

    picked = editor.get_column_values(
        "TESTREG",
        "Kon",
        picked_classification="CLS_B",
        db_path=regmeta_db.parent,
    )
    assert picked.tier == "3b"
    assert picked.picked_classification == "CLS_B"
    assert {c.code for c in picked.codes} == {"1", "2", "9"}


def test_get_column_values_picked_classification_invalid_falls_back(
    regmeta_db: Path,
):
    """Bad picks degrade silently to the default winner rather than
    erroring — picker state shouldn't break the popup."""
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (1, 'CLS_A', 'A', 'Class A', 2);
        INSERT INTO classification (id, short_name, name, description, code_count)
            VALUES (2, 'CLS_B', 'B', 'Class B', 2);
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 1, code_id, NULL, 1 FROM value_code WHERE vardekod IN ('1', '2');
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
            SELECT 2, code_id, NULL, 1 FROM value_code WHERE vardekod IN ('1', '2');
        UPDATE variable_instance SET classification_id = 1 WHERE cvid = 1001;
        """
    )
    _add_extra_cvid(
        conn, cvid=1002, regver_id=101, regversion_name="2021", classification_id=2
    )
    conn.commit()
    conn.close()

    result = editor.get_column_values(
        "TESTREG",
        "Kon",
        picked_classification="NOT_A_REAL_CLS",
        db_path=regmeta_db.parent,
    )
    assert result.kind == "classification"
    # Falls back to a candidate from the list (most-common winner).
    assert result.picked_classification in {"CLS_A", "CLS_B"}


# -- value-set picker (issue #64, follow-up) -----------------------------


def test_get_column_values_picked_value_set_honored(regmeta_db: Path):
    """Tier 2 default is the union; passing ``picked_value_set`` filters
    to that group's codes only."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="2021")
    # Widen the code set for the new year so we can prove which group
    # the picker rendered.
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("3", "Annat")])
    conn.commit()
    conn.close()

    default = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert default.tier == "2"
    assert {c.code for c in default.codes} == {"1", "2", "3"}

    # Pick the 2020 group (cvid=1001) → only its codes show.
    early = next(g for g in default.value_sets if g.year_min == 2020)
    picked = editor.get_column_values(
        "TESTREG",
        "Kon",
        picked_value_set=early.value_set_id,
        db_path=regmeta_db.parent,
    )
    assert picked.tier == "2"
    assert picked.picked_value_set == early.value_set_id
    assert {c.code for c in picked.codes} == {"1", "2"}


def test_get_column_values_picked_value_set_invalid_falls_back(regmeta_db: Path):
    """Bad ``picked_value_set`` degrades to the tier default rather than
    blanking the popup."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="2021")
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("3", "Annat")])
    conn.commit()
    conn.close()

    result = editor.get_column_values(
        "TESTREG",
        "Kon",
        picked_value_set=99999,
        db_path=regmeta_db.parent,
    )
    assert result.tier == "2"
    # Tier 2 default = union (not the bad pick).
    assert result.picked_value_set is None
    assert {c.code for c in result.codes} == {"1", "2", "3"}


def test_get_column_values_relevant_years_filters_value_sets(
    regmeta_db: Path,
):
    """Project that only loaded 2024 files should not see 2020 value-sets
    in the picker. The 2020-only group drops out; the 2024 group becomes
    the lone option (tier collapses to 1, picker disappears in the UI)."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="2024")
    # 2024 introduces a new code "3" that didn't exist in 2020.
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("3", "Annat")])
    conn.commit()
    conn.close()

    result = editor.get_column_values(
        "TESTREG", "Kon", relevant_years=[2024], db_path=regmeta_db.parent
    )
    assert result.kind == "values"
    # Filtered to one group → tier 1 (no variance once we scope to 2024).
    assert result.tier == "1"
    # Picker collapses; only the 2024 group survives.
    assert len(result.value_sets) == 1
    assert result.value_sets[0].year_min == 2024
    # Codes are 2024-specific (includes the new "3"), not the union.
    assert {c.code for c in result.codes} == {"1", "2", "3"}


def test_get_column_values_relevant_years_falls_back_when_no_overlap(
    regmeta_db: Path,
):
    """When no value-set covers the project's year, fall back to the
    full set and surface a note explaining why."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="2021")
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("3", "Annat")])
    conn.commit()
    conn.close()

    # Project years (2030, 2031) don't overlap any regmeta value-set.
    result = editor.get_column_values(
        "TESTREG",
        "Kon",
        relevant_years=[2030, 2031],
        db_path=regmeta_db.parent,
    )
    assert result.kind == "values"
    # Full set retained as fallback.
    assert len(result.value_sets) == 2
    assert result.note is not None
    assert "no value-set covering your project's years" in result.note
    assert "2030" in result.note and "2031" in result.note


def test_get_column_values_relevant_years_keeps_yearless_groups(
    regmeta_db: Path,
):
    """Groups with no parseable year survive the filter — we can't
    disprove their relevance and excluding them would hide otherwise
    useful codes for projects against yearless regmeta versions."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a yearless cvid (regver name "provisorisk" → extract_year → None).
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="provisorisk")
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("9", "Okänd")])
    conn.commit()
    conn.close()

    result = editor.get_column_values(
        "TESTREG", "Kon", relevant_years=[2020], db_path=regmeta_db.parent
    )
    # 2020 group + yearless group both kept.
    assert len(result.value_sets) == 2
    years = {g.year_min for g in result.value_sets}
    assert years == {2020, None}


def test_value_set_groups_chronological_with_unparsable_year_last(
    regmeta_db: Path,
):
    """``_fetch_value_set_groups`` orders by year_min asc, with yearless
    groups (regver name without a parseable year) sinking to the end."""
    import sqlite3

    from .conftest import assign_value_set

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # cvid 1002: yearless registerversionnamn → year_min is None.
    _add_extra_cvid(conn, cvid=1002, regver_id=101, regversion_name="provisorisk")
    assign_value_set(conn, 1002, [("1", "Man"), ("2", "Kvinna"), ("3", "Annat")])
    # cvid 1003: 2019 → should sort before 2020.
    _add_extra_cvid(conn, cvid=1003, regver_id=102, regversion_name="2019")
    assign_value_set(conn, 1003, [("1", "Man"), ("2", "Kvinna"), ("9", "Okänd")])
    conn.commit()
    conn.close()

    result = editor.get_column_values("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.tier == "2"
    years = [g.year_min for g in result.value_sets]
    assert years == [2019, 2020, None]


# -- _dedupe_codes -------------------------------------------------------


def test_dedupe_codes_preserves_first_seen_order():
    pairs = iter([("2", "Two"), ("1", "One"), ("2", "Other"), ("3", "Three")])
    out = editor._dedupe_codes(pairs)
    assert tuple(c.code for c in out) == ("2", "1", "3")
    # First-seen label wins.
    assert {c.code: c.label for c in out} == {"2": "Two", "1": "One", "3": "Three"}


# -- get_column_varinfo --------------------------------------------------


def test_get_column_varinfo_returns_none_when_regmeta_missing(monkeypatch):
    """Same graceful-degradation stance as get_column_values: when regmeta
    is unavailable the editor returns the empty envelope rather than
    raising — and tags it ``none_reason="unavailable"`` so the UI can
    distinguish "regmeta missing" from "column not in regmeta"."""
    from contextlib import contextmanager

    @contextmanager
    def _no_conn(_):
        yield None

    monkeypatch.setattr(editor, "_open_regmeta_conn", _no_conn)
    result = editor.get_column_varinfo("TESTREG", "Kon")
    assert result.kind == "none"
    assert result.none_reason == "unavailable"
    assert result.primary is None


def test_get_column_varinfo_returns_none_when_register_is_none(regmeta_db: Path):
    """Cross-register lookup is intentionally out of scope (issue #71):
    a column without a register pinned can mean too many different
    things. Return ``kind="none"`` with ``no_register`` so the editor
    can prompt the user to assign one."""
    result = editor.get_column_varinfo(None, "Kon", db_path=regmeta_db.parent)
    assert result.kind == "none"
    assert result.none_reason == "no_register"


def test_get_column_varinfo_returns_none_for_unknown_column(regmeta_db: Path):
    result = editor.get_column_varinfo(
        "TESTREG", "NotAColumn", db_path=regmeta_db.parent
    )
    assert result.kind == "none"
    assert result.none_reason == "not_found"


def test_get_column_varinfo_returns_none_when_register_unresolved(
    regmeta_db: Path,
):
    result = editor.get_column_varinfo(
        "DOES_NOT_EXIST", "Kon", db_path=regmeta_db.parent
    )
    assert result.kind == "none"
    assert result.none_reason == "not_found"


def test_get_column_varinfo_strips_mona_prefix(regmeta_db: Path):
    """MONA-prefixed columns (e.g. ``P1105_Kon``) aren't stored in regmeta
    under that name — they're aliased to ``Kon``. Mirror
    ``get_column_values`` and retry with the stripped form so the
    editor surfaces varinfo for prefixed datasets too."""
    result = editor.get_column_varinfo(
        "TESTREG", "P1105_Kon", db_path=regmeta_db.parent
    )
    assert result.kind == "single"
    assert result.primary is not None
    assert result.primary.variabelnamn == "Kön"


def test_get_column_varinfo_propagates_non_not_found_regmeta_errors(
    monkeypatch, regmeta_db: Path
):
    """Only ``code="not_found"`` is the documented "normal outcome" for
    a popover. Other RegmetaErrors (usage_error, ambiguous_alias, …)
    must propagate so they surface in the UI rather than being silently
    converted to an empty envelope."""
    from regmeta import errors as regmeta_errors

    def _raise_usage(*_a, **_kw):
        raise regmeta_errors.RegmetaError(
            exit_code=regmeta_errors.EXIT_USAGE,
            code="usage_error",
            error_class="query",
            message="bad call",
            remediation="fix it",
        )

    import regmeta.queries

    monkeypatch.setattr(regmeta.queries, "get_varinfo", _raise_usage)
    with pytest.raises(regmeta_errors.RegmetaError):
        editor.get_column_varinfo("TESTREG", "Kon", db_path=regmeta_db.parent)


def test_get_column_varinfo_rejects_blank_column(regmeta_db: Path):
    with pytest.raises(editor.ValidationError):
        editor.get_column_varinfo("TESTREG", "   ", db_path=regmeta_db.parent)


def test_get_column_varinfo_single_variant(regmeta_db: Path):
    """The fixture has one ``Kon`` variable under TESTREG with one cvid:
    the result must be ``kind="single"`` and surface the canonical
    description fields."""
    result = editor.get_column_varinfo("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "single"
    assert result.primary is not None
    assert result.primary.variabelnamn == "Kön"
    assert result.primary.variabeldefinition == "Kön enligt folkbokföring"
    assert result.primary.var_id == 44
    assert result.primary.register_name == "TESTREG"
    assert result.primary_instances == 1
    assert result.total_instances == 1
    assert result.alternatives == ()


def test_get_column_varinfo_divergent_picks_most_common_primary(regmeta_db: Path):
    """When SCB has recycled a column name across two var_ids under the
    same register, the response is ``kind="divergent"`` with the
    higher-cvid-count variable as primary and the rest as alternatives."""
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a SECOND variable (var_id=45) also aliased to "Kon", with
    # fewer cvids than the original (1 vs 2) so the existing var wins.
    conn.executescript(
        """
        INSERT INTO variable (register_id, var_id, variabelnamn, variabeldefinition)
            VALUES (1, 45, 'Hushållsställning',
                    'Personens ställning i hushållet');
        INSERT INTO register_version (regver_id, regvar_id, registerversionnamn)
            VALUES (110, 10, '2010');
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva)
            VALUES (1100, 1, 10, 110, 45, 'int', '1', 'HH', '1');
        INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1100, 'Kon');
        -- second cvid for the primary (var_id=44) so it has 2 vs 1
        INSERT INTO register_version (regver_id, regvar_id, registerversionnamn)
            VALUES (120, 10, '2021');
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva)
            VALUES (1200, 1, 10, 120, 44, 'int', '1', 'Kön', '1');
        INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1200, 'Kon');
        """
    )
    conn.commit()
    conn.close()

    result = editor.get_column_varinfo("TESTREG", "Kon", db_path=regmeta_db.parent)
    assert result.kind == "divergent"
    assert result.primary is not None
    assert result.primary.var_id == 44
    assert result.primary_instances == 2
    assert result.total_instances == 3
    assert len(result.alternatives) == 1
    alt = result.alternatives[0]
    assert alt.description.var_id == 45
    assert alt.description.variabelnamn == "Hushållsställning"
    assert alt.instances == 1
