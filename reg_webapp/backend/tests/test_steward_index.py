"""Steward catalog index build + boot behaviour.

See DESIGN.md → Steward layering and the in-memory catalog index (stewards.py +
catalog_index.py).

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
``REG_WEBAPP_STEWARDS_DIR`` (the static per-deployment selection seam),
both pointed at a tmp stewards dir holding a minimal ``ifau`` catalog.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
import reg_meta.db
import reg_webapp.steward_catalog as steward_catalog_module
from _steward_helpers import (
    CLEAN_SOURCES as _CLEAN_SOURCES,
    steward_project as _steward_project,
    write_global as _write_global,
    write_steward as _write_steward,
)
from fastapi.testclient import TestClient
from reg_meta.catalog import Catalog, CatalogSizes
from reg_schema.project_data import ProjectData
from reg_schema.validation import ValidationIssue
from reg_webapp.app import create_app
from reg_webapp.catalog_index import CatalogIndex, build_catalog_index
from reg_webapp.semantic import validate_semantic
from reg_webapp.steward_catalog import StewardBootCatalog

from reg_webapp.stewards import (
    StewardCatalogError,
    load_catalog_index,
    load_steward,
)

if TYPE_CHECKING:
    from pathlib import Path


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
    index = build_catalog_index(project, result.issues, catalog)

    assert set(index.bindings_by_variant) == {
        "scb/lisa/individer-15plus",
        "scb/rams/standard",
    }
    # #206: values are (FQID, resolved delivery column) pairs — the steward
    # bindings carry no `representation`, so each resolves to its unique column.
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    assert index.bindings_by_variant["scb/rams/standard"] == frozenset(
        {("scb/rams/syss", "Syss")}
    )
    # Period-range map keyed by register FQID.
    assert index.period_range_by_register == {
        "scb/lisa": ("2018", "2018"),
        "scb/rams": ("2019", "2019"),
    }
    assert index.drift_warnings == ()
    assert index.catalog_period_span == (2018, 2019)


def test_catalog_period_span_extracts_years_from_mixed_period_tokens():
    index = CatalogIndex(
        bindings_by_variant={},
        period_range_by_register={
            "scb/lisa": ("HT1995", "2020-Q3"),
            "sos/patient": ("2018", "2018-12-31"),
        },
        drift_warnings=(),
    )

    assert index.catalog_period_span == (1995, 2020)


def test_catalog_period_span_is_null_when_any_token_has_no_year():
    index = CatalogIndex(
        bindings_by_variant={},
        period_range_by_register={
            "scb/lisa": ("2018", "2018"),
            "sos/patient": ("_default", "_default"),
        },
        drift_warnings=(),
    )

    assert index.catalog_period_span is None


def test_catalog_period_span_is_null_for_token_only_periods():
    index = CatalogIndex(
        bindings_by_variant={},
        period_range_by_register={"scb/lisa": ("_default", "_default")},
        drift_warnings=(),
    )

    assert index.catalog_period_span is None


def test_index_period_range_uses_range_and_list_endpoints(catalog):
    project = ProjectData.model_validate(
        _steward_project(
            [
                {
                    "name": "lisa",
                    "register_variant": "scb/lisa/individer-15plus",
                    "period": {"from": 2018, "to": 2020},
                    "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
                },
                {
                    "name": "rams",
                    "register_variant": "scb/rams/standard",
                    "period": [2019, {"from": 2021, "to": 2022}],
                    "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
                },
            ]
        )
    )
    result = validate_semantic(project, catalog, caller="steward")
    assert result.ok

    index = build_catalog_index(project, result.issues, catalog)

    assert index.period_range_by_register == {
        "scb/lisa": ("2018", "2020"),
        "scb/rams": ("2019", "2022"),
    }
    assert index.catalog_period_span == (2018, 2022)


def test_index_period_range_preserves_mixed_token_year_high_end(catalog):
    project = ProjectData.model_validate(
        _steward_project(
            [
                {
                    "name": "lisa",
                    "register_variant": "scb/lisa/individer-15plus",
                    "period": ["HT1995", 2005, 2020],
                    "bindings": [{"variable": "scb/lisa/ghostvar", "type": "numeric"}],
                },
            ]
        )
    )
    issues = (
        ValidationIssue(
            level="warning",
            code="fqid_unresolved",
            path="/sources/0/bindings/0/variable",
            message="gone from reg_meta",
        ),
    )

    index = build_catalog_index(project, issues, catalog)

    assert index.period_range_by_register == {"scb/lisa": ("HT1995", "2020")}
    assert index.catalog_period_span == (1995, 2020)


def test_index_period_range_preserves_yearless_token_as_unknown_span(catalog):
    project = ProjectData.model_validate(
        _steward_project(
            [
                {
                    "name": "lisa_default",
                    "register_variant": "scb/lisa/individer-15plus",
                    "period": "_default",
                    "bindings": [{"variable": "scb/lisa/ghostvar", "type": "numeric"}],
                },
                {
                    "name": "lisa_2018",
                    "register_variant": "scb/lisa/individer-15plus",
                    "period": 2018,
                    "bindings": [{"variable": "scb/lisa/ghostvar", "type": "numeric"}],
                },
            ]
        )
    )
    issues = (
        ValidationIssue(
            level="warning",
            code="fqid_unresolved",
            path="/sources/0/bindings/0/variable",
            message="gone from reg_meta",
        ),
        ValidationIssue(
            level="warning",
            code="fqid_unresolved",
            path="/sources/1/bindings/0/variable",
            message="gone from reg_meta",
        ),
    )

    index = build_catalog_index(project, issues, catalog)

    assert index.period_range_by_register == {"scb/lisa": ("_default", "_default")}
    assert index.catalog_period_span is None


def test_steward_boot_catalog_matches_index_and_reuses_resolve_at(
    catalog, monkeypatch: pytest.MonkeyPatch
):
    """The startup adapter must preserve steward validation/index semantics while
    avoiding the duplicate period-resolution pass during index construction."""

    class CountingCatalog:
        def __init__(self, wrapped: Catalog) -> None:
            self.wrapped = wrapped
            self.resolve_at_calls = 0
            self.resolve_calls = 0

        def __getattr__(self, name: str):
            return getattr(self.wrapped, name)

        def list_variants(self, provider_slug: str, register_slug: str):
            return self.wrapped.list_variants(provider_slug, register_slug)

        def resolve(self, fqid):
            self.resolve_calls += 1
            return self.wrapped.resolve(fqid)

        def resolve_at(self, fqid, period, *, variant=None, value_set_version=None):
            self.resolve_at_calls += 1
            return self.wrapped.resolve_at(
                fqid,
                period,
                variant=variant,
                value_set_version=value_set_version,
            )

    project = ProjectData.model_validate(_steward_project(_CLEAN_SOURCES))
    expected = build_catalog_index(
        project,
        validate_semantic(project, catalog, caller="steward").issues,
        catalog,
    )

    counting = CountingCatalog(catalog)
    boot_catalog = StewardBootCatalog(counting)
    # Force the preload path through multiple tiny batches; the production
    # regression was exceeding SQLite's traditional host-parameter limit.
    monkeypatch.setattr(steward_catalog_module, "_SQLITE_PARAM_LIMIT", 3)
    boot_catalog.preload_project(project)
    result = validate_semantic(project, boot_catalog, caller="steward")
    actual = build_catalog_index(project, result.issues, boot_catalog)

    assert result.ok and result.issues == ()
    assert actual == expected
    # The boot adapter uses a minimal state projection from the same SQLite
    # connection, so neither validation nor index construction hydrates full
    # public VariableState models through Catalog.resolve_at.
    assert counting.resolve_at_calls == 0
    # The categorical binding's value_set still resolves through the real catalog;
    # variable bindings use the adapter's minimal semantic projection instead of
    # full ResolvedVariable hydration.
    assert counting.resolve_calls == 1


def test_steward_boot_catalog_matches_same_as_replacement_hints(catalog_db):
    conn = sqlite3.connect(catalog_db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "INSERT INTO variable_same_as "
            "(a_provider, a_register, a_variable, b_provider, b_register, b_variable) "
            "VALUES ('scb', 'lisa', 'kon-alias', 'scb', 'lisa', 'kon')"
        )
        conn.commit()
        catalog = Catalog(conn)
        source = {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2020,
            "bindings": [{"variable": "scb/lisa/kon-alias", "type": "categorical"}],
        }
        project = ProjectData.model_validate(_steward_project([source]))

        real_result = validate_semantic(project, catalog, caller="steward")
        boot_catalog = StewardBootCatalog(catalog)
        boot_result = validate_semantic(project, boot_catalog, caller="steward")

        assert boot_result.issues == real_result.issues
        assert [issue.code for issue in boot_result.issues] == ["variable_replaced"]
        assert boot_result.issues[0].successor_fqid == "scb/rams/syss"
        assert build_catalog_index(project, boot_result.issues, boot_catalog) == (
            build_catalog_index(project, real_result.issues, catalog)
        )
    finally:
        conn.close()


# ── (b) membership probe (fqid_outside_steward_catalog backing) ────────────


def test_index_admits_known_and_rejects_unknown(catalog):
    project = ProjectData.model_validate(_steward_project(_CLEAN_SOURCES))
    result = validate_semantic(project, catalog, caller="steward")
    index = build_catalog_index(project, result.issues, catalog)
    assert index.admits("scb/lisa/kon", "Kon")
    assert index.admits("scb/rams/syss", "Syss")
    # In the universe but NOT in this steward's catalog → not admitted.
    assert not index.admits("scb/rams/nosuchbinding", "Whatever")
    # #206: the right FQID at a column the steward does not hold → not admitted,
    # but `held_columns` still names the concept (the representation-level case).
    assert not index.admits("scb/lisa/kon", "KonDetailed")
    assert index.held_columns("scb/lisa/kon") == frozenset({"Kon"})
    assert index.held_columns("scb/rams/nosuchbinding") == frozenset()


def test_catalog_sizes_de_dupes_binding_columns():
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset(
                {
                    ("scb/lisa/kon", "Kon"),
                    ("scb/lisa/kon", "KonDetaljerad"),
                }
            ),
            "sos/patient/_default": frozenset({("sos/patient/diagnos", None)}),
        },
        period_range_by_register={
            "scb/lisa": ("2018", "2018"),
            "sos/patient": ("2020", "2020"),
        },
        drift_warnings=(),
    )

    assert index.catalog_sizes() == CatalogSizes(providers=2, registers=2, variables=2)


def test_held_variant_coords_excludes_drift_emptied_slot():
    # gap 4: a declared-but-empty variant slot (drift dropped all its bindings → it
    # maps to frozenset()) must NOT count as a held coord — the variants endpoint
    # lists only variants the steward actually holds data under.
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset({("scb/lisa/kon", "Kon")}),
            # A drift-emptied slot under the SAME register: declared, admits nothing.
            "scb/lisa/drifted": frozenset(),
        },
        period_range_by_register={"scb/lisa": ("2018", "2018")},
        drift_warnings=(),
    )
    assert index.held_variant_coords_for_register("scb/lisa") == frozenset(
        {"scb/lisa/individer-15plus"}
    )


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

    index = build_catalog_index(project, result.issues, catalog)
    # The resolvable binding survives; the ghost is dropped.
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    assert len(index.drift_warnings) == 1


def test_build_index_drops_only_warning_level_issues():
    """build_catalog_index drops a binding (+ surfaces drift) ONLY for a
    warning-level issue — the steward-downgraded resolution failures.
    An info `binding_state_drifts_within_period` (the binding RESOLVED, it just
    spans a transition) and a non-downgraded error `binding_value_set_version_
    ambiguous` (a researcher-author-time concern) must NOT drop the binding nor
    appear as drift. The drop decision is a pure walk over `issues`; the KEPT
    bindings must resolve against the catalog (#206 column resolution), so the
    fixture seeds `alder` / `civst` alongside the warned-on ghost `aaa` (a
    dropped binding is never resolved — it may be absent from the DB)."""
    from _slugged_db import add_state, add_variable, build_slugged_db

    conn = build_slugged_db()
    for var_id, name, slug, column in [
        (88, "Ålder", "alder", "Alder"),
        (89, "Civilstånd", "civst", "Civst"),
    ]:
        add_variable(conn, register_id=1, var_id=var_id, name=name, slug=slug)
        add_state(
            conn,
            register_id=1,
            variable_slug=slug,
            register_variant_id=10,
            valid_from="2018-01-01",
            valid_to="9999-12-31",
            delivery_column_name=column,
        )
    conn.commit()

    sources = [
        {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [
                {"variable": "scb/lisa/aaa", "type": "categorical"},  # warning → drop
                {"variable": "scb/lisa/alder", "type": "categorical"},  # info → keep
                {"variable": "scb/lisa/civst", "type": "categorical"},  # error → keep
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
    try:
        index = build_catalog_index(project, issues, Catalog(conn))
    finally:
        conn.close()
    admitted = {f for f, _ in index.bindings_by_variant["scb/lisa/individer-15plus"]}
    assert "scb/lisa/aaa" not in admitted  # warning dropped it
    assert "scb/lisa/alder" in admitted  # info must NOT drop
    assert "scb/lisa/civst" in admitted  # non-downgraded error must NOT drop
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
    index = build_catalog_index(project, result.issues, catalog)
    # The variant didn't resolve → the source's bindings are all unauthorable.
    assert index.bindings_by_variant.get("scb/lisa/ghostvariant") == frozenset()


# ── (d) the global deployment has no index ─────────────────────────────────


def test_extra_key_in_binding_raises_stewardcatalogerror(catalog, tmp_path):
    """A steward catalog with an unrecognized key in a binding PASSES
    validate_structural but reg_schema's `extra="forbid"` model rejects it — it
    must surface as a clear StewardCatalogError (fail fast), NOT an opaque
    pydantic.ValidationError out of the FastAPI lifespan (which would crash boot
    with a traceback instead of an actionable message)."""
    stewards = tmp_path / "stewards"
    bad_sources = [
        {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [
                {
                    "variable": "scb/lisa/kon",
                    "type": "categorical",
                    "typo_field_unknown": "oops",
                }
            ],
        }
    ]
    _write_steward(stewards, "ifau", bad_sources)
    steward = load_steward("ifau", root=stewards)
    with pytest.raises(StewardCatalogError):
        load_catalog_index(steward, catalog, root=stewards)


def test_steward_catalog_with_unresolved_semantic_error_fails_fast(tmp_path):
    """A steward catalog with a NON-downgraded semantic error (a bare
    binding_value_set_version_ambiguous) is genuinely invalid: after the three
    reg_meta-drift downgrades, result.ok is still False, so load_catalog_index fails
    fast with StewardCatalogError rather than booting a catalog-with-errors as if
    valid (which would admit the broken binding + never surface it)."""
    from _slugged_db import add_state, add_value_set, build_slugged_db

    conn = build_slugged_db()
    # Two co-delivered (overlapping) states for kon under the variant with DISTINCT
    # value sets → a bare binding is ambiguous (error, NOT one of the drift
    # downgrades). Ambiguity keys on value_set_id, so the two must differ.
    add_value_set(conn, value_set_id=701, codes=[("1", "Man"), ("2", "Kvinna")])
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    conn.execute(
        "UPDATE variable_state SET value_set_version_label = 'sun2020', "
        "value_set_id = 701 "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        value_set_version_label="sun2000",
        value_set_id=702,
    )
    conn.commit()
    stewards = tmp_path / "stewards"
    ambiguous = [
        {
            "name": "lisa",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
        }
    ]
    _write_steward(stewards, "ifau", ambiguous)
    steward = load_steward("ifau", root=stewards)
    try:
        with pytest.raises(StewardCatalogError, match="ambiguous"):
            load_catalog_index(steward, Catalog(conn), root=stewards)
    finally:
        conn.close()


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
    assert body["steward"]["catalog_period_span"] == {"from": 2018, "to": 2019}
    assert body["catalog_drift_warnings"] == []


def test_boot_survives_catalog_drift(catalog_db, _filtered_steward_dir):
    """⚠️ Boot-availability: a steward catalog referencing an FQID reg_meta
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
            {("scb/lisa/kon", "Kon")}
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
    body = resp.json()
    assert body["steward"]["catalog_period_span"] is None
    assert body["catalog_drift_warnings"] == []
