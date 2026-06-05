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
from reg_webapp.catalog_index import build_catalog_index
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
    same variant with DISTINCT value sets (different `value_set_id`) — the genuine
    co-delivery ambiguity (a `(variable, variant, period)` resolving to two
    different code-lists)."""
    from _slugged_db import add_state, add_value_set, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    add_value_set(conn, value_set_id=701, codes=[("1", "Man"), ("2", "Kvinna")])
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    # The seeded kon state spans 2018-01-01..9999-12-31; stamp it value set 701 +
    # label sun2020, then add a second co-delivered state with a DIFFERENT value
    # set 702 in the same variant + window.
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


@pytest.fixture
def same_value_set_catalog():
    """`scb/lisa/kon` has TWO states co-delivered in 2018 under the same variant
    that share ONE `value_set_id` but carry different free-text version labels —
    the same values under two names. This is NOT ambiguity (the re-key on
    `value_set_id` must NOT false-positive on it — the ~71% phantom case)."""
    from _slugged_db import add_state, add_value_set, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    add_value_set(conn, value_set_id=701, codes=[("1", "Man"), ("2", "Kvinna")])
    conn.execute(
        "UPDATE variable_state SET value_set_version_label = 'LKF 2003', "
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
        value_set_version_label="LKF 2004",  # different label, SAME value set
        value_set_id=701,
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_same_value_set_two_labels_is_not_ambiguous(same_value_set_catalog):
    # Two co-delivered states sharing one value_set_id are the same values under
    # two names — keying ambiguity on the label would false-positive here.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }
    result = validate_semantic(
        _project([source]), same_value_set_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes


@pytest.fixture
def multi_representation_catalog():
    """`scb/lisa/kon` carries TWO co-existing DELIVERY COLUMNS at 2018 — parallel
    REPRESENTATIONS of one concept (the SSYK 3/5-digit / age-bracket shape). A
    binding must pick one via `representation`."""
    from _slugged_db import add_state, add_value_set, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    add_value_set(conn, value_set_id=701, codes=[("1", "Man"), ("2", "Kvinna")])
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    conn.execute(
        "UPDATE variable_state SET value_set_id = 701, delivery_column_name = 'kon', "
        "value_set_version_label = 'grov' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="kon_detalj",  # a SECOND co-existing column
        value_set_version_label="detalj",  # distinct label (the §5.1 index keys on it)
        value_set_id=702,
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def _repr_source(representation=None):
    binding = {"variable": "scb/lisa/kon", "type": "categorical"}
    if representation is not None:
        binding["representation"] = representation
    return {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [binding],
    }


def test_multi_representation_without_representation_is_ambiguous(
    multi_representation_catalog,
):
    result = validate_semantic(
        _project([_repr_source()]), multi_representation_catalog, caller="researcher"
    )
    issue = next(
        i for i in result.issues if i.code == "binding_value_set_version_ambiguous"
    )
    assert issue.level == "error"
    # Both co-existing columns named; `'kon'` quoted distinguishes it from the
    # `kon_detalj` substring.
    assert "'kon'" in issue.message and "kon_detalj" in issue.message
    assert not result.ok


def test_representation_picks_one_column(multi_representation_catalog):
    result = validate_semantic(
        _project([_repr_source("kon_detalj")]),
        multi_representation_catalog,
        caller="researcher",
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_representation_unknown" not in codes


def test_unknown_representation_is_flagged(multi_representation_catalog):
    result = validate_semantic(
        _project([_repr_source("nope")]),
        multi_representation_catalog,
        caller="researcher",
    )
    issue = next(i for i in result.issues if i.code == "binding_representation_unknown")
    assert issue.level == "error"
    assert "nope" in issue.message


def test_unknown_representation_downgrades_for_steward(multi_representation_catalog):
    # A steward committed a representation a newer reg_meta build no longer
    # delivers as a column → drift: downgraded to warning (the binding drops from
    # the index) instead of crashing boot (§6.8.3 boot-availability invariant).
    result = validate_semantic(
        _project([_repr_source("nope")]),
        multi_representation_catalog,
        caller="steward",
    )
    issue = next(i for i in result.issues if i.code == "binding_representation_unknown")
    assert issue.level == "warning"
    assert result.ok


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


@pytest.fixture
def column_rename_catalog():
    """`scb/lisa/kon` delivered under DISTINCT columns in NON-overlapping windows:
    `KonOld` 2010-2015, renamed `KonNew` 2016-9999. A rename, not parallel
    co-existence — a range crossing it must be drift, not representation-ambiguity."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'KonNew', "
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
        delivery_column_name="KonOld",
        value_set_version_label="old",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_range_crossing_column_rename_is_drift_not_ambiguous(column_rename_catalog):
    # DISTINCT columns in NON-overlapping windows (a rename) must NOT demand a
    # `representation` — only co-EXISTING (overlapping) columns do.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2014, "to": 2018},  # spans the 2015→2016 rename
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }
    result = validate_semantic(
        _project([source]), column_rename_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_state_drifts_within_period" in codes
    assert result.ok


@pytest.fixture
def uneven_representation_catalog():
    """Two co-existing columns with UNEVEN spans: `kon` 2010-9999 and `kon_detalj`
    only 2018-9999. Picking the shorter column over a range that predates it
    under-covers the requested period."""
    from _slugged_db import add_state, add_value_set, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2010-01-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="kon_detalj",
        value_set_version_label="detalj",
        value_set_id=702,
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_representation_under_covering_range_is_drift(uneven_representation_catalog):
    # Range 2010-2020 picking `kon_detalj` (only 2018+) leaves 2010-2017 uncovered
    # vs the `kon` column — an info coverage note, not a blocking error.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2010, "to": 2020},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon_detalj",
            }
        ],
    }
    result = validate_semantic(
        _project([source]), uneven_representation_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_state_drifts_within_period" in codes
    assert result.ok


def test_representation_under_covering_default_is_drift(uneven_representation_catalog):
    # `_default` returns the full history (kon 2010+), so picking kon_detalj (2018+)
    # under-covers it too — the coverage check must treat `_default` like a range.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": "_default",
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon_detalj",
            }
        ],
    }
    result = validate_semantic(
        _project([source]), uneven_representation_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_state_drifts_within_period" in codes
    assert result.ok


# ── §6.8.3 (#207): an explicit range PARTIALLY covered by the concept's states.
# `resolve_at` returns the states INTERSECTING the requested `[from, to]`; if their
# union leaves a gap NO column delivers, the binding silently drops that sub-range.
# `range_period_partially_covered` (info) surfaces it. This is the WHOLE-CONCEPT
# under-coverage case — distinct from #204's `binding_state_drifts_within_period`,
# which is the CHOSEN representation under-covering vs a sibling column that DOES
# deliver the gap. Zero coverage stays `period_outside_state_validity`.


def _kon_source(period) -> dict:
    return {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": period,
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }


def test_range_fully_covered_has_no_partial_finding(catalog):
    # kon spans 2018-01-01..9999-12-31; a range fully inside it has no gap.
    result = validate_semantic(
        _project([_kon_source({"from": 2019, "to": 2021})]),
        catalog,
        caller="researcher",
    )
    codes = {i.code for i in result.issues}
    assert "range_period_partially_covered" not in codes
    assert result.ok


def test_range_partially_covered_is_flagged(catalog):
    # kon first delivered 2018; a 2010-2020 binding leaves 2010-2017 uncovered.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2020})]),
        catalog,
        caller="researcher",
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert issue.level == "info"
    assert issue.path == "/sources/0/bindings/0/variable"
    # The reported gap is the leading uncovered span, inclusive ISO bounds.
    assert "2010-01-01..2017-12-31" in issue.message
    # Info is non-blocking: the covered sub-range still extracts.
    assert result.ok


def test_zero_coverage_is_only_period_outside_state_validity(catalog):
    # A range entirely BEFORE kon's first state is zero coverage, not partial —
    # `resolve_at` returns no states, so only the existing code fires.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2015})]),
        catalog,
        caller="researcher",
    )
    codes = {i.code for i in result.issues}
    assert "period_outside_state_validity" in codes
    assert "range_period_partially_covered" not in codes


def test_point_period_has_no_partial_finding(catalog):
    # A point period is a single instant — no requested span to under-cover.
    result = validate_semantic(
        _project([_kon_source(2018)]), catalog, caller="researcher"
    )
    assert "range_period_partially_covered" not in {i.code for i in result.issues}


def test_default_period_has_no_partial_finding(catalog):
    # `_default` means "the full history" — there is no author-requested window to
    # compare against, so the whole-concept partial-coverage check must not fire.
    result = validate_semantic(
        _project([_kon_source("_default")]), catalog, caller="researcher"
    )
    assert "range_period_partially_covered" not in {i.code for i in result.issues}


def test_partial_coverage_does_not_drop_binding_from_steward_index(catalog):
    # The finding is `info` for both callers, so it must NOT drop the binding from
    # the steward catalog index — a partially-covered binding is still usable for the
    # covered sub-range. (A `warning` here would wrongly drop it; catalog_index.py
    # keys its DROP on warning level.)
    project = _project([_kon_source({"from": 2010, "to": 2020})])
    result = validate_semantic(project, catalog, caller="steward")
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert issue.level == "info"
    assert result.ok

    index = build_catalog_index(project, result.issues)
    assert index.admits("scb/lisa/kon")


@pytest.fixture
def internal_gap_catalog():
    """`scb/lisa/kon` delivered in TWO non-adjacent windows under one variant:
    2010-2012, then 2016-9999 — an INTERNAL gap (2013-2015 has no state at all).
    The seeded state is re-windowed to the later era; the earlier era is added as a
    second state, leaving the 2013-2015 hole."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_from = '2016-01-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2010-01-01",
        valid_to="2012-12-31",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_internal_gap_in_range_is_flagged(internal_gap_catalog):
    # Range 2010-2018 over a concept with a 2013-2015 hole → exactly that gap.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2018})]),
        internal_gap_catalog,
        caller="researcher",
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert "2013-01-01..2015-12-31" in issue.message
    assert result.ok


def test_day_adjacent_windows_leave_no_gap(internal_gap_catalog):
    # A range covering only the populated tail (2016+) is fully covered. Guards the
    # adjacency math: string `valid_from > valid_to` would false-positive a gap.
    result = validate_semantic(
        _project([_kon_source({"from": 2016, "to": 2018})]),
        internal_gap_catalog,
        caller="researcher",
    )
    assert "range_period_partially_covered" not in {i.code for i in result.issues}


@pytest.mark.parametrize("bad_endpoint", ["2019-02-29", "2018-02-30", "2021-04-31"])
def test_calendar_invalid_range_endpoint_is_graceful_invalid_period(
    catalog, bad_endpoint
):
    # The period grammar's day bound is syntactic — Feb 30 passes structural
    # validation — but the gap math feeds endpoints to `date.fromisoformat`, which
    # raises. The check must catch it and emit a graceful `invalid_period`, NOT let
    # a ValueError escape `validate_semantic` (which routes to an uncaught 500).
    source = _kon_source({"from": bad_endpoint, "to": 2020})
    # The call must NOT raise.
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    issue = next(i for i in result.issues if i.code == "invalid_period")
    assert issue.level == "error"
    assert issue.path == "/sources/0/bindings/0/variable"
    assert bad_endpoint in issue.message
    assert not result.ok
    # The nonsense range short-circuits: no phantom coverage finding is emitted.
    codes = {i.code for i in result.issues}
    assert "range_period_partially_covered" not in codes
    assert "period_outside_state_validity" not in codes


def test_calendar_invalid_endpoint_blocks_for_steward_too(catalog):
    # `invalid_period` is an author-side spec error, NOT reg_meta drift, so it is
    # not steward-downgraded: a malformed committed catalog must fail boot loudly.
    source = _kon_source({"from": "2019-02-29", "to": 2020})
    result = validate_semantic(_project([source]), catalog, caller="steward")
    issue = next(i for i in result.issues if i.code == "invalid_period")
    assert issue.level == "error"
    assert not result.ok


@pytest.mark.parametrize(
    "good_endpoint",
    [
        "2019-02",  # YYYY-02 in a NON-leap year: expands to 2019-02-29 hi (an
        # over-counted, non-real day) but a real month — must NOT be flagged.
        "2020-02",  # leap-year Feb, for symmetry
        "2019-12",  # plain month token
        "2019-Q1",  # quarter (hi = 2019-03-31)
        "2019-H1",  # half (hi = 2019-06-30)
        "HT2019",  # autumn term
        "VT2019",  # spring term
        "2019-02-28",  # a real Feb day token
    ],
)
def test_valid_period_token_endpoint_is_accepted(catalog, good_endpoint):
    # Regression for the `hi`-bound over-counting false positive: only a genuinely
    # impossible AUTHOR DAY (a YYYY-MM-DD token) is invalid; month/quarter/half/term
    # tokens whose synthetic upper bound happens to be a non-real day are fine. The
    # endpoint pairs with 2020 so the range resolves; we only assert it is not
    # rejected as an invalid period (coverage findings are orthogonal here).
    source = _kon_source({"from": good_endpoint, "to": 2020})
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    assert "invalid_period" not in {i.code for i in result.issues}
