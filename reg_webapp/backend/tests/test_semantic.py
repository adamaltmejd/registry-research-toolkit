"""Semantic validator against the slugged ``catalog_db`` fixture.

See DESIGN.md → Semantic validation (semantic.py). Covers: a clean spec → no
issues; an unresolvable ``register_variant`` →
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
from reg_webapp.semantic import period_display, validate_semantic


@pytest.fixture
def catalog(catalog_db):
    conn = reg_meta.db.open_db(catalog_db, check_schema=False)
    try:
        yield Catalog(conn)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (2018, "2018"),
        ("HT2020", "HT2020"),
        ("_default", "_default"),
    ],
)
def test_period_display_scalar(period, expected):
    assert period_display(period) == expected


def test_period_display_range_is_wire_form_not_repr():
    from reg_schema.project_data import PeriodRange  # noqa: PLC0415

    pr = PeriodRange.model_validate({"from": 2015, "to": 2020})
    rendered = period_display(pr)
    assert rendered == "2015..2020"
    assert "PeriodRange" not in rendered and "from_=" not in rendered


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
    """Caller context: the three reg_meta-backed codes are blocking errors
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


# ── Fold: co-delivered value-set versions ──────────────────────────────────
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
        value_set_version_label="detalj",  # distinct label (the index keys on it)
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
    # the index) instead of crashing boot (boot-availability invariant).
    result = validate_semantic(
        _project([_repr_source("nope")]),
        multi_representation_catalog,
        caller="steward",
    )
    issue = next(i for i in result.issues if i.code == "binding_representation_unknown")
    assert issue.level == "warning"
    assert result.ok


# ── A version TRANSITION (sequential, non-overlapping) is drift, NOT a
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
    # MESSAGE HYGIENE: the range period renders in wire form (`2014..2018`), never
    # a dataclass repr — the text travels through the API to CLI + SPA consumers.
    drift = next(
        i for i in result.issues if i.code == "binding_state_drifts_within_period"
    )
    assert "2014..2018" in drift.message, drift.message
    assert "PeriodRange" not in drift.message and "from_=" not in drift.message


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


@pytest.fixture
def representation_internal_gap_catalog():
    """The PINNED column `kon` is delivered in TWO disjoint windows (2010-2012 and
    2018-2020); a SIBLING column `kon_detalj` fills the middle (2013-2017). All
    three windows are non-overlapping, so no column co-exists with another (no
    `binding_value_set_version_ambiguous`). Over a 2010..2020 range the OUTER
    bounds of the pinned column (min_from 2010, max_to 2020) equal those of the
    full state set, so the old outer-bounds check stays silent — yet the pinned
    column's 2013-2017 extract is empty because only the sibling delivers it. This
    is the #342 internal-gap case the gap-based check must catch."""
    from _slugged_db import add_state, add_value_set, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    # Re-window the seeded `kon` state to the FIRST pinned-column era.
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2010-01-01', valid_to = '2012-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    # Second pinned-column era, leaving a 2013-2017 hole in `kon`.
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="2020-12-31",
        delivery_column_name="kon",
    )
    # Sibling column filling the gap between the two pinned eras.
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2013-01-01",
        valid_to="2017-12-31",
        delivery_column_name="kon_detalj",
        value_set_version_label="detalj",
        value_set_id=702,
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_representation_internal_gap_in_range_is_drift(
    representation_internal_gap_catalog,
):
    # #342: the pinned `kon` column is gapped 2013-2017 (a sibling fills it). Outer
    # bounds match the full set, so this is the case that is SILENT on `main`; the
    # gap-based check must flag the internal under-coverage as info (non-blocking).
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2010, "to": 2020},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(
        _project([source]), representation_internal_gap_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_state_drifts_within_period" in codes
    assert "binding_value_set_version_ambiguous" not in codes
    assert result.ok


def test_representation_full_coverage_range_is_no_drift(
    representation_internal_gap_catalog,
):
    # Control: pinning the SIBLING column over the exact window it fully covers
    # leaves no gap vs the full state set → no drift info.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2013, "to": 2017},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon_detalj",
            }
        ],
    }
    result = validate_semantic(
        _project([source]), representation_internal_gap_catalog, caller="researcher"
    )
    codes = {i.code for i in result.issues}
    assert "binding_state_drifts_within_period" not in codes
    assert result.ok


# ── #207: an explicit range PARTIALLY covered by the concept's states.
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

    index = build_catalog_index(project, result.issues, catalog)
    assert index.admits("scb/lisa/kon", "Kon")


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


@pytest.mark.parametrize(
    "good_endpoint",
    [
        "2019-02",  # YYYY-02 in a NON-leap year: synthesized hi = 2019-02-29 (an
        # over-counted, non-real day) — the crash case. Must snap to 2019-02-28.
        "2020-02",  # leap-year Feb, for symmetry (synthesized hi IS a real day)
        "2019-12",  # plain month token
        "2019-Q1",  # quarter (hi = 2019-03-31)
        "2019-H1",  # half (hi = 2019-06-30)
        "HT2019",  # autumn term
        "VT2019",  # spring term
        "2019-02-28",  # a real Feb day token
    ],
)
def test_valid_period_token_to_endpoint_is_accepted(catalog, good_endpoint):
    # Regression for the synthesized-`hi` over-count CRASH (#239 follow-up): the
    # token is the `to` endpoint, so its synthesized UPPER bound is what the gap
    # math runs real `date` arithmetic on. A non-leap `2019-02` expands to a
    # 2019-02-29 hi that `date.fromisoformat` rejects — `_requested_range_bounds`
    # snaps it to 2019-02-28 so the call must NOT raise. `from: 2019` is ≤ every
    # endpoint and intersects kon's 2018-01-01..9999-12-31 state, so the range is
    # FULLY covered: no `invalid_period`, no spurious phantom Feb-29 gap, usable.
    source = _kon_source({"from": 2019, "to": good_endpoint})
    result = validate_semantic(_project([source]), catalog, caller="researcher")
    codes = {i.code for i in result.issues}
    assert "invalid_period" not in codes, codes
    assert "range_period_partially_covered" not in codes, codes
    assert "period_outside_state_validity" not in codes, codes
    assert result.ok, result.issues


def test_non_leap_feb_to_endpoint_gap_is_snapped_not_phantom(catalog):
    # The snapped synthesized hi must produce CORRECT gap math, not a phantom
    # Feb-29 span. kon starts 2018-01-01; a `{2017, "2019-02"}` range has exactly
    # ONE real gap — the leading 2017 (uncovered before kon's first state) — and
    # the covered tail ends at the snapped 2019-02-28, never an impossible -02-29.
    result = validate_semantic(
        _project([_kon_source({"from": 2017, "to": "2019-02"})]),
        catalog,
        caller="researcher",
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert "2017-01-01..2017-12-31" in issue.message, issue.message
    # No spurious Feb-29 phantom gap leaked in.
    assert "2019-02-29" not in issue.message, issue.message
    assert result.ok


# ── #227: fqid_outside_steward_catalog (steward catalog filter) ─────────────
# Given the loaded steward `CatalogIndex`, the researcher path flags a RESOLVED
# FQID outside the steward's filtered subset as a non-blocking warning. The
# steward-load path and the `global` deployment (index=None) never emit it. The
# fixture DB resolves both `scb/lisa/kon` and `scb/rams/syss`; an index built from
# a kon-only steward project admits the former but not the latter.

_RAMS_SOURCE = {
    "name": "rams",
    "register_variant": "scb/rams/standard",
    "period": 2019,
    "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
}


@pytest.fixture
def kon_only_index(catalog):
    """A steward `CatalogIndex` admitting ONLY `scb/lisa/kon` (built from a
    one-source steward project). `scb/rams/syss` resolves reg_meta-wide but is NOT
    admitted by this index."""
    project = _project([_CLEAN_SOURCE])
    result = validate_semantic(project, catalog, caller="steward")
    assert result.ok
    index = build_catalog_index(project, result.issues, catalog)
    assert index.admits("scb/lisa/kon", "Kon")
    assert not index.admits("scb/rams/syss", "Syss")
    return index


def test_resolvable_unadmitted_fqid_is_outside_steward_catalog(catalog, kon_only_index):
    result = validate_semantic(
        _project([_RAMS_SOURCE]), catalog, caller="researcher", index=kon_only_index
    )
    outside = [i for i in result.issues if i.code == "fqid_outside_steward_catalog"]
    assert len(outside) == 1
    assert outside[0].level == "warning"
    assert outside[0].path == "/sources/0/bindings/0/variable"
    assert "scb/rams/syss" in outside[0].message
    # Non-blocking: the FQID resolves, it is merely outside this deployment.
    assert result.ok


@pytest.fixture
def two_lisa_var_catalog():
    """`scb/lisa` carries `kon` PLUS a second resolvable variable `alder`, both
    under `individer-15plus` with a 2018+ state. The shared `catalog_db` has only
    ONE variable per register, so landing an unadmitted warning at binding index 1
    AFTER an admitted binding 0 (both in one source, both period-clean) needs a
    second resolvable variable under kon's variant — built here."""
    from _slugged_db import add_state, add_variable, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    add_variable(conn, register_id=1, var_id=88, name="Ålder", slug="alder")
    add_state(
        conn,
        register_id=1,
        variable_slug="alder",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="Alder",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_outside_steward_catalog_warns_per_unadmitted_binding(two_lisa_var_catalog):
    # Multiplicity: one warning PER unadmitted binding, each at its own
    # `/sources/<i>/bindings/<j>/variable` path. The index admits only `kon`, so the
    # admitted `kon` (source 0 binding 0) stays silent while the unadmitted `alder`
    # warns at both its source-0/binding-1 and source-1/binding-0 positions.
    steward = _project(
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            }
        ]
    )
    sresult = validate_semantic(steward, two_lisa_var_catalog, caller="steward")
    assert sresult.ok
    index = build_catalog_index(steward, sresult.issues, two_lisa_var_catalog)
    assert index.admits("scb/lisa/kon", "Kon")
    assert not index.admits("scb/lisa/alder", "Alder")

    researcher = _project(
        [
            {
                "name": "s0",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [
                    {"variable": "scb/lisa/kon", "type": "categorical"},
                    {"variable": "scb/lisa/alder", "type": "numeric"},
                ],
            },
            {
                "name": "s1",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/alder", "type": "numeric"}],
            },
        ]
    )
    result = validate_semantic(
        researcher, two_lisa_var_catalog, caller="researcher", index=index
    )
    outside = [i for i in result.issues if i.code == "fqid_outside_steward_catalog"]
    assert len(outside) == 2
    assert {i.path for i in outside} == {
        "/sources/0/bindings/1/variable",
        "/sources/1/bindings/0/variable",
    }
    assert all(i.level == "warning" for i in outside)
    # The admitted `kon` (source 0 binding 0) did NOT warn; nothing else is wrong.
    assert result.ok


def test_outside_steward_catalog_coexists_with_period_check(catalog):
    # Coexistence: the admission check runs AFTER `_check_binding_period` (it
    # needs the resolved columns, #206) but its FQID-level arm fires even when
    # the period resolution failed, so a binding that is BOTH
    # unadmitted AND period-invalid emits BOTH codes. (The fixture's `syss` covers
    # all history and can't be made period-invalid, so the period-bounded binding
    # here is `kon` (state 2018+); the index therefore admits `syss`, leaving `kon`
    # unadmitted.)
    steward = _project([_RAMS_SOURCE])
    sresult = validate_semantic(steward, catalog, caller="steward")
    assert sresult.ok
    index = build_catalog_index(steward, sresult.issues, catalog)
    assert index.admits("scb/rams/syss", "Syss")
    assert not index.admits("scb/lisa/kon", "Kon")

    # kon's only state is 2018-01-01..9999-12-31; period 2015 is outside it.
    result = validate_semantic(
        _project([{**_CLEAN_SOURCE, "period": 2015}]),
        catalog,
        caller="researcher",
        index=index,
    )
    by_code = {i.code: i for i in result.issues}
    assert "fqid_outside_steward_catalog" in by_code
    assert by_code["fqid_outside_steward_catalog"].level == "warning"
    assert "period_outside_state_validity" in by_code
    # On the RESEARCHER path `period_outside_state_validity` is an ERROR (only the
    # steward caller downgrades it), so the result is NOT ok despite the warning.
    assert by_code["period_outside_state_validity"].level == "error"
    assert result.ok is False


def test_admitted_fqid_has_no_outside_steward_catalog(catalog, kon_only_index):
    result = validate_semantic(
        _project([_CLEAN_SOURCE]), catalog, caller="researcher", index=kon_only_index
    )
    assert "fqid_outside_steward_catalog" not in {i.code for i in result.issues}
    assert result.ok


def test_no_index_never_emits_outside_steward_catalog(catalog):
    # `index` defaults to None (the `global` deployment): the filter never fires,
    # even for an FQID outside any steward's catalog.
    result = validate_semantic(_project([_RAMS_SOURCE]), catalog, caller="researcher")
    assert "fqid_outside_steward_catalog" not in {i.code for i in result.issues}
    assert result.ok


def test_unresolvable_fqid_not_also_outside_catalog(catalog, kon_only_index):
    # An unresolvable FQID gets `fqid_unresolved` and returns BEFORE the admission
    # check — it must NOT also be flagged `fqid_outside_steward_catalog`.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/lisa/nosuchvar", "type": "categorical"}],
    }
    result = validate_semantic(
        _project([source]), catalog, caller="researcher", index=kon_only_index
    )
    codes = {i.code for i in result.issues}
    assert "fqid_unresolved" in codes
    assert "fqid_outside_steward_catalog" not in codes


# ── #206: representation_outside_steward_catalog (column-based admission) ───
# Admission keys on (FQID, RESOLVED delivery column). A steward holding only one
# representation of a multi-column concept admits exactly that column; a
# researcher binding the sibling column gets the DISTINCT
# `representation_outside_steward_catalog` (warning) whose message enumerates
# what the steward DOES hold. Matching is on the resolved column, never the raw
# `representation` string, so a steward-authored-`None`-before-drift catalog and
# a researcher who must now pin still compare equal.


@pytest.fixture
def two_repr_catalog():
    """`scb/lisa/kon` delivered under TWO CO-EXISTING columns at 2018+ — the
    seeded `Kon` plus a parallel `KonDetailed` (same window, distinct column) —
    a multi-representation concept whose bindings must pin `representation`."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="KonDetailed",
        # Distinct label only to satisfy the state uniqueness key (same
        # variable/variant/valid_from); value_set_id stays None on both states,
        # so the co-delivered-value-set backstop is NOT in play here.
        value_set_version_label="detailed",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def _kon_repr_source(representation: str | None) -> dict:
    binding: dict = {"variable": "scb/lisa/kon", "type": "categorical"}
    if representation is not None:
        binding["representation"] = representation
    return {
        "name": "lisa-2018",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [binding],
    }


@pytest.fixture
def kon_basic_only_index(two_repr_catalog):
    """A steward holding `kon` at the `Kon` column ONLY (its catalog pins
    `representation: "Kon"` — required, the concept is multi-representation)."""
    steward = _project([_kon_repr_source("Kon")])
    sresult = validate_semantic(steward, two_repr_catalog, caller="steward")
    assert sresult.ok and sresult.issues == ()
    index = build_catalog_index(steward, sresult.issues, two_repr_catalog)
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    return index


def test_sibling_representation_outside_steward_catalog(
    two_repr_catalog, kon_basic_only_index
):
    # The researcher pins the OTHER column: the FQID is in the catalog, its
    # resolved column is not → the distinct representation-level warning, whose
    # message names the missing column AND enumerates the steward's holdings.
    result = validate_semantic(
        _project([_kon_repr_source("KonDetailed")]),
        two_repr_catalog,
        caller="researcher",
        index=kon_basic_only_index,
    )
    by_code = {i.code: i for i in result.issues}
    assert "fqid_outside_steward_catalog" not in by_code
    issue = by_code["representation_outside_steward_catalog"]
    assert issue.level == "warning"
    assert issue.path == "/sources/0/bindings/0/variable"
    assert "'KonDetailed'" in issue.message
    assert "'Kon'" in issue.message
    # Non-blocking: the column is real reg_meta-wide, merely not supplied here.
    assert result.ok


def test_matching_representation_is_admitted(two_repr_catalog, kon_basic_only_index):
    result = validate_semantic(
        _project([_kon_repr_source("Kon")]),
        two_repr_catalog,
        caller="researcher",
        index=kon_basic_only_index,
    )
    codes = {i.code for i in result.issues}
    assert "representation_outside_steward_catalog" not in codes
    assert "fqid_outside_steward_catalog" not in codes
    assert result.ok


def test_ambiguous_binding_skips_representation_admission(
    two_repr_catalog, kon_basic_only_index
):
    # No `representation` on a multi-column concept → the binding is ambiguous
    # (`binding_value_set_version_ambiguous` error); WHICH column the author means
    # is unknowable, so the column-level admission check stays silent rather than
    # piling a speculative warning on top.
    result = validate_semantic(
        _project([_kon_repr_source(None)]),
        two_repr_catalog,
        caller="researcher",
        index=kon_basic_only_index,
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" in codes
    assert "representation_outside_steward_catalog" not in codes
    assert "fqid_outside_steward_catalog" not in codes


def test_steward_none_vs_researcher_pin_compare_equal_on_resolved_column(
    catalog, kon_only_index
):
    # Drift-handling rationale (#206): the steward authored `representation: None`
    # back when `kon` had ONE column; reg_meta later grows a sibling and the
    # researcher must pin. Both sides RESOLVE to the same `Kon` column, so the
    # pinned researcher binding is admitted — raw-string matching (None vs "Kon")
    # would falsely reject. (Single-column fixture: pinning is legal, not required.)
    source = {
        **_CLEAN_SOURCE,
        "bindings": [{**_CLEAN_SOURCE["bindings"][0], "representation": "Kon"}],
    }
    result = validate_semantic(
        _project([source]), catalog, caller="researcher", index=kon_only_index
    )
    codes = {i.code for i in result.issues}
    assert "representation_outside_steward_catalog" not in codes
    assert "fqid_outside_steward_catalog" not in codes
    assert result.ok


@pytest.fixture
def renamed_column_catalog():
    """`scb/lisa/kon` SEQUENTIALLY renamed: column `Kon` through 2019-12-31, then
    `KonNy` from 2020 (non-overlapping windows — a rename, NOT co-existing
    representations, so no `representation` pin is required on either side)."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_to = '2019-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2020-01-01",
        valid_to="9999-12-31",
        delivery_column_name="KonNy",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_resolved_column_mismatch_across_sequential_rename(renamed_column_catalog):
    # Neither side pins a `representation` (legal — one column per instant), yet
    # admission still catches the mismatch because BOTH sides resolve to columns:
    # the steward's 2018 catalog holds `Kon`; the researcher's 2020 binding
    # resolves to the renamed `KonNy`. Raw-string matching (None vs None) would
    # falsely admit it.
    steward = _project([_kon_repr_source(None)])  # period 2018 → column `Kon`
    sresult = validate_semantic(steward, renamed_column_catalog, caller="steward")
    assert sresult.ok and sresult.issues == ()
    index = build_catalog_index(steward, sresult.issues, renamed_column_catalog)
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )

    researcher = _project([{**_kon_repr_source(None), "period": 2020}])
    result = validate_semantic(
        researcher, renamed_column_catalog, caller="researcher", index=index
    )
    by_code = {i.code: i for i in result.issues}
    issue = by_code["representation_outside_steward_catalog"]
    assert issue.level == "warning"
    assert "'KonNy'" in issue.message
    assert "'Kon'" in issue.message
    assert "fqid_outside_steward_catalog" not in by_code
    assert result.ok


# ── #307: the period LIST form (interrupted series) ──────────────────────────
# Structural validation guarantees the list is non-empty, sorted, and disjoint
# before this layer runs; semantic resolution is PER SEGMENT
# (`period_outside_state_validity` / `range_period_partially_covered` name the
# segment) while the representation/ambiguity/drift checks run on the
# `state_id`-deduped union of every segment's states.


def test_list_period_clean_resolves_without_issues(catalog):
    # Both segments inside kon's single 2018+ state → no issues at all (the
    # state intersecting both segments counts ONCE — no phantom drift info).
    result = validate_semantic(
        _project([_kon_source([2018, {"from": 2019, "to": 2020}])]),
        catalog,
        caller="researcher",
    )
    assert result.issues == ()
    assert result.ok


def test_list_period_uncovered_segment_errors_per_segment(catalog):
    # kon's state starts 2018: the 2010..2012 segment has NO covering state →
    # one error naming that segment; the covered 2018 segment contributes none.
    result = validate_semantic(
        _project([_kon_source([{"from": 2010, "to": 2012}, 2018])]),
        catalog,
        caller="researcher",
    )
    outside = [i for i in result.issues if i.code == "period_outside_state_validity"]
    assert len(outside) == 1
    assert "2010..2012" in outside[0].message
    assert "segment of 2010..2012,2018" in outside[0].message
    assert not result.ok


def test_list_period_two_uncovered_segments_error_each(catalog):
    # Every uncovered segment gets its own error (per-segment feedback).
    result = validate_semantic(
        _project([_kon_source([2015, 2016, 2018])]),
        catalog,
        caller="researcher",
    )
    outside = [i for i in result.issues if i.code == "period_outside_state_validity"]
    assert len(outside) == 2
    assert any("2015" in i.message for i in outside)
    assert any("2016" in i.message for i in outside)


def test_list_period_partial_coverage_names_the_segment(catalog):
    # kon starts 2018: the 2017..2019 segment is PARTIALLY covered (2017 gap);
    # the 2021..2022 segment is fully covered. One info, naming the under-
    # covered segment, with the whole-series context appended.
    result = validate_semantic(
        _project(
            [_kon_source([{"from": 2017, "to": 2019}, {"from": 2021, "to": 2022}])]
        ),
        catalog,
        caller="researcher",
    )
    partial = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(partial) == 1
    assert "2017..2019" in partial[0].message
    assert "2017-01-01..2017-12-31" in partial[0].message
    assert "segment of 2017..2019,2021..2022" in partial[0].message
    assert result.ok


def test_list_period_segments_on_distinct_states_report_drift(internal_gap_catalog):
    # The gap fixture delivers kon in TWO windows (2010-2012, 2016+). A list
    # period with one segment in each window unions TWO distinct states →
    # the sequential-drift info fires (and the union is what admission sees).
    result = validate_semantic(
        _project(
            [_kon_source([{"from": 2010, "to": 2012}, {"from": 2016, "to": 2018}])]
        ),
        internal_gap_catalog,
        caller="researcher",
    )
    drift = [i for i in result.issues if i.code == "binding_state_drifts_within_period"]
    assert len(drift) == 1
    assert "spans 2 states" in drift[0].message
    assert result.ok


def test_list_period_steward_index_resolves_columns(catalog):
    # A steward catalog may itself use the list form: the kept binding's
    # columns resolve per segment and union into the (FQID, column) pairs.
    project = _project([_kon_source([2018, {"from": 2019, "to": 2020}])])
    result = validate_semantic(project, catalog, caller="steward")
    assert result.ok and result.issues == ()
    index = build_catalog_index(project, result.issues, catalog)
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    # The best-effort register span hints off the FIRST (lowest) segment.
    assert index.period_range_by_register["scb/lisa"] == ("2018", "2018")


@pytest.fixture
def gap_overlap_catalog():
    """Two DISTINCT columns whose validity windows overlap only BETWEEN the
    requested segments of an interrupted series: `Kon` 2005-2014 and `KonNy`
    2012-2025 (mutual overlap 2012-2014). A researcher binding segments
    [2010, 2020] touches one column per segment — never both at one requested
    instant — while the scalar range 2010..2020 includes the overlap window."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_from = '2005-01-01', "
        "valid_to = '2014-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2012-01-01",
        valid_to="2025-12-31",
        delivery_column_name="KonNy",
        value_set_version_label="ny",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_list_period_no_false_ambiguity_across_segment_gap(gap_overlap_catalog):
    # Codex P2 (#334): the two columns' windows overlap only in 2012-2014 —
    # BETWEEN the requested segments — so no requested instant extracts both.
    # The per-segment probe must NOT raise the blocking ambiguity error; the
    # series resolves to one column per segment (a drift info, like a rename).
    result = validate_semantic(
        _project([_kon_source([2010, 2020])]),
        gap_overlap_catalog,
        caller="researcher",
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_state_drifts_within_period" in codes
    assert result.ok


def test_scalar_range_through_the_overlap_is_still_ambiguous(gap_overlap_catalog):
    # Control: the scalar range 2010..2020 INCLUDES the 2012-2014 overlap
    # window, so the same catalog genuinely is ambiguous there — the
    # per-segment probe must not have weakened the scalar behavior.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2020})]),
        gap_overlap_catalog,
        caller="researcher",
    )
    by_code = {i.code: i for i in result.issues}
    assert "binding_value_set_version_ambiguous" in by_code
    assert not result.ok


@pytest.fixture
def middle_segment_sibling_catalog():
    """The pinned column `Kon` delivers 2009-2011 and 2019-2025 (two states);
    only the sibling `KonAlt` delivers 2014-2016. Segments [2010, 2015, 2020]
    have matching OUTER bounds for both the pin and the union — the middle
    segment's hole is invisible to the outer-bounds drift check."""
    from _slugged_db import add_state, build_slugged_db  # noqa: PLC0415

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_from = '2009-01-01', "
        "valid_to = '2011-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2019-01-01",
        valid_to="2025-12-31",
        delivery_column_name="Kon",
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2014-01-01",
        valid_to="2016-12-31",
        delivery_column_name="KonAlt",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_pinned_representation_missing_middle_segment_is_flagged(
    middle_segment_sibling_catalog,
):
    # Codex P2 (#334): the pin exists at the outer segments, so the
    # outer-bounds comparison is silent — but the 2015 extract would be
    # silently empty for the pinned column. The per-segment presence check
    # surfaces it as the same drift info, naming the segment.
    source = {
        **_kon_source([2010, 2015, 2020]),
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "Kon",
            }
        ],
    }
    result = validate_semantic(
        _project([source]), middle_segment_sibling_catalog, caller="researcher"
    )
    drift = [
        i
        for i in result.issues
        if i.code == "binding_state_drifts_within_period"
        and "no state at period 2015" in i.message
    ]
    assert len(drift) == 1
    assert "segment of 2010,2015,2020" in drift[0].message
    # Non-blocking (info): the covered segments still extract.
    assert result.ok
