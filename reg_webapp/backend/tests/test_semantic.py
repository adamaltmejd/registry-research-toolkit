"""§6.8.3 semantic validator against the slugged ``catalog_db`` fixture.

Covers: a clean spec → no issues; an unresolvable ``register_variant`` →
``fqid_unresolved``; an unresolvable binding ``variable`` → ``fqid_unresolved``;
an out-of-validity period → ``period_outside_state_validity``; a missing
``value_set`` → ``value_set_missing``; and the researcher-vs-steward caller
level split (error vs warning) for the three downgraded codes.

The fixture DB resolves ``scb/lisa/individer-15plus`` (variant) with binding
``scb/lisa/kon`` (state ``2018-01-01..9999-12-31``, value set) and the
classification ``class/sun2020``.
"""

from __future__ import annotations

import pytest
import reg_meta.db
from reg_meta.catalog import Catalog
from reg_schema.project_data import ProjectData
from reg_webapp.semantic import validate_semantic


@pytest.fixture
def catalog(catalog_db):
    conn = reg_meta.db.open_db(catalog_db, check_schema=False)
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def _project(sources: list[dict]) -> ProjectData:
    """Build a structurally valid ProjectData around the given sources."""
    return ProjectData.model_validate(
        {
            "schema_version": "2.0.0",
            "steward": "ifau",
            "reg_meta_version": "5.1.0",
            "name": "test",
            "sources": sources,
        }
    )


_CLEAN_SOURCE = {
    "name": "lisa-2018",
    "register_variant": "scb/lisa/individer-15plus",
    "period": 2018,
    "bindings": [
        {
            "variable": "scb/lisa/kon",
            "type": "categorical",
            "value_set": "class/sun2020",
        },
    ],
}


def test_clean_spec_has_no_issues(catalog):
    result = validate_semantic(_project([_CLEAN_SOURCE]), catalog, caller="researcher")
    assert result.ok
    assert result.issues == ()


def test_unresolvable_register_variant_is_fqid_unresolved(catalog):
    source = {**_CLEAN_SOURCE, "register_variant": "scb/lisa/nosuchvariant"}
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    codes = {(i.code, i.level, i.path) for i in result.issues}
    assert (
        "fqid_unresolved",
        "error",
        "/sources/0/register_variant",
    ) in codes
    assert not result.ok


def test_unresolvable_register_prefix_is_fqid_unresolved(catalog):
    # A register the DB doesn't know → list_variants empty → fqid_unresolved.
    source = {
        "name": "s",
        "register_variant": "scb/nosuchregister/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/nosuchregister/x", "type": "categorical"}],
    }
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    rv_issue = next(i for i in result.issues if i.path == "/sources/0/register_variant")
    assert rv_issue.code == "fqid_unresolved"


def test_unresolvable_binding_variable_is_fqid_unresolved(catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/lisa/nosuchvar", "type": "categorical"}],
    }
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    issue = next(i for i in result.issues if i.path == "/sources/0/bindings/0/variable")
    assert issue.code == "fqid_unresolved"
    assert issue.level == "error"


def test_period_outside_state_validity(catalog):
    # kon's only state is 2018-01-01..9999-12-31; 2015 precedes it.
    source = {**_CLEAN_SOURCE, "period": 2015}
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    issue = next(i for i in result.issues if i.code == "period_outside_state_validity")
    assert issue.level == "error"
    assert issue.path == "/sources/0/bindings/0/variable"


def test_value_set_missing(catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "value_set": "class/nosuchclass",
            }
        ],
    }
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    issue = next(i for i in result.issues if i.code == "value_set_missing")
    assert issue.level == "error"
    assert issue.path == "/sources/0/bindings/0/value_set"


@pytest.mark.parametrize(
    ("source_patch", "code"),
    [
        ({"register_variant": "scb/lisa/nosuchvariant"}, "fqid_unresolved"),
        ({"period": 2015}, "period_outside_state_validity"),
    ],
)
def test_steward_caller_downgrades_error_to_warning(catalog, source_patch, code):
    """§6.8.3 caller context: the three reg_meta-backed codes are blocking errors
    for the researcher path but downgrade to warnings on the steward-catalog load
    path (so a deployment boots through reg_meta drift)."""
    source = {**_CLEAN_SOURCE, **source_patch}
    project = _project([source])

    researcher = validate_semantic(project, catalog, caller="researcher")
    steward = validate_semantic(project, catalog, caller="steward")

    r_issue = next(i for i in researcher.issues if i.code == code)
    s_issue = next(i for i in steward.issues if i.code == code)
    assert r_issue.level == "error"
    assert s_issue.level == "warning"
    # The researcher path blocks; the steward path stays ok=True (the binding
    # drops from the index instead — that's catalog_index.py's job).
    assert not researcher.ok
    assert steward.ok


def test_value_set_missing_downgrades_for_steward(catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "value_set": "class/nosuchclass",
            }
        ],
    }
    result = validate_semantic(_project([source]), catalog, caller="steward")
    issue = next(i for i in result.issues if i.code == "value_set_missing")
    assert issue.level == "warning"
    assert result.ok


def test_sos_provider_resolves_clean(catalog):
    # Smoke that an unrelated valid-but-unused source doesn't false-positive: a
    # second clean source alongside the first stays issue-free.
    result = validate_semantic(
        _project([_CLEAN_SOURCE, {**_CLEAN_SOURCE, "name": "lisa-2018-b"}]),
        catalog,
        caller="researcher",
    )
    assert result.ok


# ── §6.8.3 fold: co-delivered value-set versions ───────────────────────────
# A bare binding matching >1 state because several value-set versions are
# co-delivered in the bound period is `binding_value_set_version_ambiguous`
# (error); pinning `@<version>` narrows to one and passes. These need a 2-version
# fixture, so they build their own in-memory DB rather than the shared catalog_db.


@pytest.fixture
def multiversion_catalog():
    """A DB where `scb/lisa/kon` has TWO states co-delivered in 2018 under the
    same variant, differing only by value_set_version_label (a §5.7 fold)."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    # The default build seeded one kon state ('' label). Stamp it sun2020 and add
    # a second co-delivered sun2000 state in the same variant + window.
    conn.execute(
        "UPDATE variable_state SET value_set_version_label = 'sun2020' "
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
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_bare_binding_with_codelivered_versions_is_ambiguous(multiversion_catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }
    result = validate_semantic(
        _project([source]), multiversion_catalog, caller="researcher"
    )
    issue = next(
        i for i in result.issues if i.code == "binding_value_set_version_ambiguous"
    )
    assert issue.level == "error"
    assert "sun2000" in issue.message and "sun2020" in issue.message
    assert not result.ok


def test_pinned_version_narrows_and_passes(multiversion_catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        # Pin one of the co-delivered versions with the FQID @<version> suffix.
        "bindings": [
            {"variable": "scb/lisa/kon@sun2020", "type": "categorical"},
        ],
    }
    result = validate_semantic(
        _project([source]), multiversion_catalog, caller="researcher"
    )
    assert result.ok
    assert result.issues == ()


# ── §6.8.3: a version TRANSITION (sequential, non-overlapping) is drift, NOT a
# co-delivery ambiguity. resolve_at returns every state whose validity intersects
# the period, so a range / `_default` period crossing a re-version matches several
# SEQUENTIAL states; their distinct version labels must NOT trip the (blocking)
# ambiguity error — only OVERLAPPING (co-delivered) versions do.


@pytest.fixture
def transition_catalog():
    """A DB where `scb/lisa/kon` has two SEQUENTIAL (non-overlapping) states under
    the same variant: sun2000 valid 2010-2015, then sun2020 valid 2016-9999 — a
    version TRANSITION, not co-delivery."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    # Re-window the seeded kon state to the LATER era; add the earlier era as a
    # second, non-overlapping state under the same variant.
    conn.execute(
        "UPDATE variable_state SET value_set_version_label = 'sun2020', "
        "valid_from = '2016-01-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2010-01-01",
        valid_to="2015-12-31",
        value_set_version_label="sun2000",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_range_crossing_version_transition_is_drift_not_ambiguous(transition_catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2014, "to": 2018},  # spans the 2015→2016 re-version
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }
    result = validate_semantic(
        _project([source]), transition_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes  # NOT co-delivered
    assert "binding_state_drifts_within_period" in codes
    assert result.ok  # drift is info-only, non-blocking


def test_default_period_over_version_history_is_not_ambiguous(transition_catalog):
    # `_default` returns the full (sequential) history; distinct labels across
    # non-overlapping states must NOT trip the ambiguity error.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": "_default",
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }
    result = validate_semantic(
        _project([source]), transition_catalog, caller="researcher"
    )
    assert "binding_value_set_version_ambiguous" not in {i.code for i in result.issues}
    assert result.ok
