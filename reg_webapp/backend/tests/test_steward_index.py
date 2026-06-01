"""Steward catalog index build + boot behaviour (§9.1 / §6.8.3).

Covers:

(a) a steward catalog filters the universe (register_variant coord → bindings);
(b) ``CatalogIndex.admits`` — the membership probe ``fqid_outside_steward_catalog``
    (A5.2b-ii) will consult;
(c) **boot-survives-drift**: a steward catalog referencing an FQID ABSENT from
    the fixture DB still BOOTS — a warning is emitted, the binding drops from the
    index, ``app.state.catalog_index`` is populated, and ``/api/context`` exposes
    the warning — NOT a crash;
(d) the ``global`` deployment has no index and no catalog filter.

A filtered steward is selected at boot via ``REG_WEBAPP_STEWARD`` +
``REG_WEBAPP_STEWARDS_DIR`` (the static per-deployment selection seam, §9.1),
both pointed at a tmp stewards dir holding a minimal ``ifau`` catalog.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import reg_meta.db
from fastapi.testclient import TestClient
from reg_meta.catalog import Catalog
from reg_schema.project_data import ProjectData
from reg_schema.validation import ValidationIssue
from reg_webapp.app import create_app
from reg_webapp.catalog_index import build_catalog_index
from reg_webapp.semantic import validate_semantic

from reg_webapp.stewards import load_catalog_index, load_steward

if TYPE_CHECKING:
    from pathlib import Path

_IFAU_TOML = """\
id = "ifau"
name = "IFAU"
long_name = "Institute for Evaluation of Labour Market and Education Policy"
hostname = "ifau.example.org"
"""


def _steward_project(sources: list[dict]) -> dict:
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "ifau-catalog",
        "sources": sources,
    }


# The fixture DB resolves scb/lisa/individer-15plus (binding scb/lisa/kon, state
# 2018+) and scb/rams/standard (binding scb/rams/syss).
_CLEAN_SOURCES = [
    {
        "name": "lisa",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "value_set": "class/sun2020",
            }
        ],
    },
    {
        "name": "rams",
        "register_variant": "scb/rams/standard",
        "period": 2019,
        "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
    },
]


def _write_steward(stewards_dir: Path, steward_id: str, sources: list[dict]) -> None:
    base = stewards_dir / steward_id
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(_IFAU_TOML, encoding="utf-8")
    (base / "steward.project_data.json").write_text(
        json.dumps(_steward_project(sources)), encoding="utf-8"
    )


def _write_global(stewards_dir: Path) -> None:
    base = stewards_dir / "global"
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(
        'id = "global"\nname = "Global"\nlong_name = "Full universe"\n'
        'hostname = "global.example.org"\n',
        encoding="utf-8",
    )


@pytest.fixture
def catalog(catalog_db):
    conn = reg_meta.db.open_db(catalog_db, check_schema=False)
    try:
        yield Catalog(conn)
    finally:
        conn.close()


# ── (a) the index filters the universe ─────────────────────────────────────


def test_index_maps_variant_coord_to_bindings(catalog):
    project = ProjectData.model_validate(_steward_project(_CLEAN_SOURCES))
    result = validate_semantic(project, catalog, caller="steward")
    assert result.ok and result.issues == ()
    index = build_catalog_index(project, result.issues)

    assert set(index.bindings_by_variant) == {
        "scb/lisa/individer-15plus",
        "scb/rams/standard",
    }
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {"scb/lisa/kon"}
    )
    assert index.bindings_by_variant["scb/rams/standard"] == frozenset(
        {"scb/rams/syss"}
    )
    # Period-range map keyed by register FQID.
    assert index.period_range_by_register == {
        "scb/lisa": ("2018", "2018"),
        "scb/rams": ("2019", "2019"),
    }
    assert index.drift_warnings == ()


# ── (b) membership probe (fqid_outside_steward_catalog backing) ────────────


def test_index_admits_known_and_rejects_unknown(catalog):
    project = ProjectData.model_validate(_steward_project(_CLEAN_SOURCES))
    result = validate_semantic(project, catalog, caller="steward")
    index = build_catalog_index(project, result.issues)
    assert index.admits("scb/lisa/kon")
    assert index.admits("scb/rams/syss")
    # In the universe but NOT in this steward's catalog → not admitted.
    assert not index.admits("scb/rams/nosuchbinding")


# ── drift drops a binding from the index (unit) ────────────────────────────


def test_drift_drops_binding_keeps_others(catalog):
    sources = [
        {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [
                {"variable": "scb/lisa/kon", "type": "categorical"},
                # Absent from the fixture DB → steward-mode warning → dropped.
                {"variable": "scb/lisa/ghostvar", "type": "numeric"},
            ],
        }
    ]
    project = ProjectData.model_validate(_steward_project(sources))
    result = validate_semantic(project, catalog, caller="steward")
    # ok stays True (drift downgraded), but there IS a warning.
    assert result.ok
    assert any(w.code == "fqid_unresolved" for w in result.issues)

    index = build_catalog_index(project, result.issues)
    # The resolvable binding survives; the ghost is dropped.
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {"scb/lisa/kon"}
    )
    assert len(index.drift_warnings) == 1


def test_build_index_drops_only_warning_level_issues():
    """build_catalog_index drops a binding (+ surfaces drift) ONLY for a
    warning-level issue — the three §6.8.3 steward-downgraded resolution failures.
    An info `binding_state_drifts_within_period` (the binding RESOLVED, it just
    spans a transition) and a non-downgraded error `binding_value_set_version_
    ambiguous` (a researcher-author-time concern) must NOT drop the binding nor
    appear as drift. Pure walk over `issues` — no DB needed."""
    sources = [
        {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [
                {"variable": "scb/lisa/aaa", "type": "categorical"},  # warning → drop
                {"variable": "scb/lisa/bbb", "type": "categorical"},  # info → keep
                {"variable": "scb/lisa/ccc", "type": "categorical"},  # error → keep
            ],
        }
    ]
    project = ProjectData.model_validate(_steward_project(sources))
    issues = (
        ValidationIssue(
            level="warning",
            code="fqid_unresolved",
            path="/sources/0/bindings/0/variable",
            message="gone from reg_meta",
        ),
        ValidationIssue(
            level="info",
            code="binding_state_drifts_within_period",
            path="/sources/0/bindings/1/variable",
            message="spans a transition",
        ),
        ValidationIssue(
            level="error",
            code="binding_value_set_version_ambiguous",
            path="/sources/0/bindings/2/variable",
            message="pin a version",
        ),
    )
    index = build_catalog_index(project, issues)
    admitted = index.bindings_by_variant["scb/lisa/individer-15plus"]
    assert "scb/lisa/aaa" not in admitted  # warning dropped it
    assert "scb/lisa/bbb" in admitted  # info must NOT drop
    assert "scb/lisa/ccc" in admitted  # non-downgraded error must NOT drop
    assert [w.code for w in index.drift_warnings] == ["fqid_unresolved"]


def test_unresolved_variant_drops_whole_source(catalog):
    sources = [
        {
            "name": "ghost",
            "register_variant": "scb/lisa/ghostvariant",
            "period": 2018,
            "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
        }
    ]
    project = ProjectData.model_validate(_steward_project(sources))
    result = validate_semantic(project, catalog, caller="steward")
    index = build_catalog_index(project, result.issues)
    # The variant didn't resolve → the source's bindings are all unauthorable.
    assert index.bindings_by_variant.get("scb/lisa/ghostvariant") == frozenset()


# ── (d) the global deployment has no index ─────────────────────────────────


def test_global_steward_has_no_index(catalog):
    steward = load_steward("global")
    assert not steward.has_catalog_filter
    assert load_catalog_index(steward, catalog) is None


# ── boot-path integration: filtered steward + drift survival ───────────────


@pytest.fixture
def _filtered_steward_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at a tmp stewards dir and select the ``ifau`` steward."""
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    return stewards


def test_boot_with_filtered_steward_populates_index(catalog_db, _filtered_steward_dir):
    _write_steward(_filtered_steward_dir, "ifau", _CLEAN_SOURCES)
    app = create_app()
    with TestClient(app) as client:
        index = app.state.catalog_index
        assert index is not None
        assert "scb/lisa/individer-15plus" in index.bindings_by_variant
        resp = client.get("/api/context")
    assert resp.status_code == 200
    body = resp.json()
    assert body["steward"]["id"] == "ifau"
    assert body["catalog_drift_warnings"] == []


def test_boot_survives_catalog_drift(catalog_db, _filtered_steward_dir):
    """⚠️ §6.8.3 boot-availability: a steward catalog referencing an FQID reg_meta
    no longer admits must BOOT — the steward-mode downgrade makes it a warning,
    the binding drops from the index, startup does NOT crash, and the drift is
    surfaced on /api/context."""
    drifted = [
        {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [
                {"variable": "scb/lisa/kon", "type": "categorical"},
                {"variable": "scb/lisa/ghostvar", "type": "numeric"},
            ],
        }
    ]
    _write_steward(_filtered_steward_dir, "ifau", drifted)

    app = create_app()
    # Entering the context runs the lifespan — it must NOT raise on the drift.
    with TestClient(app) as client:
        index = app.state.catalog_index
        assert index is not None
        # The ghost binding dropped; the resolvable one survives.
        assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
            {"scb/lisa/kon"}
        )
        resp = client.get("/api/context")

    assert resp.status_code == 200
    warnings = resp.json()["catalog_drift_warnings"]
    assert len(warnings) == 1
    assert warnings[0]["code"] == "fqid_unresolved"
    assert "ghostvar" in warnings[0]["message"]


def test_boot_global_has_no_index(catalog_db, tmp_path, monkeypatch):
    """The global deployment (no steward.project_data.json) boots with a None
    index and an empty drift list."""
    stewards = tmp_path / "stewards"
    _write_global(stewards)
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "global")

    app = create_app()
    with TestClient(app) as client:
        assert app.state.catalog_index is None
        resp = client.get("/api/context")
    assert resp.status_code == 200
    assert resp.json()["catalog_drift_warnings"] == []
