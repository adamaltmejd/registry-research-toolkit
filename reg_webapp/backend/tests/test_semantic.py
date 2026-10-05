"""Semantic validator against the slugged ``catalog_db`` fixture.

See DESIGN.md → Semantic validation (semantic.py). Covers: a clean spec → no
issues; an unresolvable ``register_variant`` →
``fqid_unresolved``; an unresolvable binding ``variable`` → ``fqid_unresolved``;
an out-of-validity period → ``period_outside_state_validity``; a missing
``value_set`` → ``value_set_missing``. Compiled steward admission is exercised
through the source-built HTTP validation corpus.

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
            "schema_version": "3.0.0",
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
    result = validate_semantic(_project([_CLEAN_SOURCE]), catalog)
    assert result.ok
    assert result.issues == ()


def test_unresolvable_register_variant_is_fqid_unresolved(catalog):
    source = {**_CLEAN_SOURCE, "register_variant": "scb/lisa/nosuchvariant"}
    result = validate_semantic(_project([source]), catalog)
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
    result = validate_semantic(_project([source]), catalog)
    rv_issue = next(i for i in result.issues if i.path == "/sources/0/register_variant")
    assert rv_issue.code == "fqid_unresolved"


def test_unresolvable_binding_variable_is_fqid_unresolved(catalog):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/lisa/nosuchvar", "type": "categorical"}],
    }
    result = validate_semantic(_project([source]), catalog)
    issue = next(i for i in result.issues if i.path == "/sources/0/bindings/0/variable")
    assert issue.code == "fqid_unresolved"
    assert issue.level == "error"


def test_period_outside_state_validity(catalog):
    # kon's only state is 2018-01-01..9999-12-31; 2015 precedes it.
    source = {**_CLEAN_SOURCE, "period": 2015}
    result = validate_semantic(_project([source]), catalog)
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
    result = validate_semantic(_project([source]), catalog)
    issue = next(i for i in result.issues if i.code == "value_set_missing")
    assert issue.level == "error"
    assert issue.path == "/sources/0/bindings/0/value_set"


def test_variable_replaced_hint_after_effective_year(catalog):
    source = {**_CLEAN_SOURCE, "period": 2020}
    result = validate_semantic(_project([source]), catalog)
    issue = next(i for i in result.issues if i.code == "variable_replaced")
    assert issue.level == "info"
    assert issue.path == "/sources/0/bindings/0/variable"
    assert issue.successor_fqid == "scb/rams/syss"
    assert "effective 2019" in issue.message
    assert result.ok


def test_variable_replaced_hint_skips_period_before_effective_year(catalog):
    result = validate_semantic(_project([_CLEAN_SOURCE]), catalog)
    assert {i.code for i in result.issues} == set()
    assert result.ok


def test_deprecated_traversal_hint_for_deprecated_variable():
    from _slugged_db import build_slugged_db

    conn = build_slugged_db()
    conn.execute("UPDATE variable SET deprecated = 1 WHERE slug = 'kon'")
    conn.commit()
    try:
        result = validate_semantic(_project([_CLEAN_SOURCE]), Catalog(conn))
    finally:
        conn.close()
    issue = next(i for i in result.issues if i.code == "deprecated_traversal")
    assert issue.level == "info"
    assert issue.path == "/sources/0/bindings/0/variable"
    assert issue.successor_fqid is None
    assert result.ok


def test_sos_provider_resolves_clean(catalog):
    # Smoke that an unrelated valid-but-unused source doesn't false-positive: a
    # second clean source alongside the first stays issue-free.
    result = validate_semantic(
        _project([_CLEAN_SOURCE, {**_CLEAN_SOURCE, "name": "lisa-2018-b"}]),
        catalog,
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
    from _slugged_db import add_state, add_value_set, build_slugged_db

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
        delivery_column_name="Kon",  # the SAME column — co-delivery, not parallel
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
    result = validate_semantic(_project([source]), multiversion_catalog)
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
    from _slugged_db import add_state, add_value_set, build_slugged_db

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
        delivery_column_name="Kon",  # the SAME column, as in a real re-label
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
    result = validate_semantic(_project([source]), same_value_set_catalog)
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes


@pytest.fixture
def multi_representation_catalog():
    """`scb/lisa/kon` carries TWO co-existing DELIVERY COLUMNS at 2018 — parallel
    REPRESENTATIONS of one concept (the SSYK 3/5-digit / age-bracket shape). A
    binding must pick one via `representation`."""
    from _slugged_db import add_state, add_value_set, build_slugged_db

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
    result = validate_semantic(_project([_repr_source()]), multi_representation_catalog)
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
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_representation_unknown" not in codes


def test_unknown_representation_is_flagged(multi_representation_catalog):
    result = validate_semantic(
        _project([_repr_source("nope")]),
        multi_representation_catalog,
    )
    issue = next(i for i in result.issues if i.code == "binding_representation_unknown")
    assert issue.level == "error"
    assert "nope" in issue.message


# ── A version TRANSITION (sequential, non-overlapping) is drift, NOT a
# co-delivery ambiguity. resolve_at returns every state whose validity intersects
# the period, so a range period crossing a re-version matches several SEQUENTIAL
# states; their distinct version labels must NOT trip the (blocking) ambiguity
# error — only OVERLAPPING (co-delivered) versions do.


@pytest.fixture
def transition_catalog():
    """A DB where `scb/lisa/kon` has two SEQUENTIAL (non-overlapping) states under
    the same variant: sun2000 valid 2010-2015, then sun2020 valid 2016-9999 — a
    version TRANSITION, not co-delivery."""
    from _slugged_db import add_state, build_slugged_db

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
        delivery_column_name="Kon",  # a re-version keeps the column
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
    result = validate_semantic(_project([source]), transition_catalog)
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


@pytest.fixture
def column_rename_catalog():
    """`scb/lisa/kon` delivered under DISTINCT columns in NON-overlapping windows:
    `KonOld` 2010-2015, renamed `KonNew` 2016-9999. A rename, not parallel
    co-existence — a range crossing it must be drift, not representation-ambiguity."""
    from _slugged_db import add_state, build_slugged_db

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
    result = validate_semantic(_project([source]), column_rename_catalog)
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_state_drifts_within_period" in codes
    assert result.ok


@pytest.fixture
def uneven_representation_catalog():
    """Two co-existing columns with UNEVEN spans: `kon` 2010-9999 and `kon_detalj`
    only 2018-9999. Picking the shorter column over a range that predates it
    under-covers the requested period."""
    from _slugged_db import add_state, add_value_set, build_slugged_db

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


def test_representation_under_covering_range_is_clipped(uneven_representation_catalog):
    # Range 2010-2020 picking `kon_detalj` (only 2018+) is the §12 availability
    # clip: the pin is requested where it is available, so validation reports the
    # window the order will carry — informationally, never as an error.
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
    result = validate_semantic(_project([source]), uneven_representation_catalog)
    clip = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert clip.level == "info"
    assert "ordered for 2018..2020" in clip.message, clip.message
    assert "binding_value_set_version_ambiguous" not in {i.code for i in result.issues}
    assert result.ok


@pytest.fixture
def representation_internal_gap_catalog():
    """The PINNED column `kon` is delivered in TWO disjoint windows (2010-2012 and
    2018-9999); a SIBLING column `kon_detalj` fills the middle (2013-2017). All
    three windows are non-overlapping, so no column co-exists with another (no
    `binding_value_set_version_ambiguous`). Over a 2010..2020 range the pinned
    column has the same outer coverage as the full state set once clamped to the
    requested range, so the old outer-bounds check stays silent — yet the pinned
    column's 2013-2017 extract is empty because only the sibling delivers it. This
    is the #342/#465 internal-gap case the gap-based check must catch."""
    from _slugged_db import add_state, add_value_set, build_slugged_db

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
        valid_to="9999-12-31",
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


def test_representation_internal_gap_in_range_is_clipped(
    representation_internal_gap_catalog,
):
    # #342: the pinned `kon` column is gapped 2013-2017 (a sibling fills it), so
    # the clip is INTERNAL — outer bounds alone would miss it. The ordered period
    # names both eras, which is exactly what the order manifest carries.
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
    result = validate_semantic(_project([source]), representation_internal_gap_catalog)
    clip = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert clip.level == "info"
    assert "ordered for 2010..2012,2018..2020" in clip.message, clip.message
    assert "binding_value_set_version_ambiguous" not in {i.code for i in result.issues}
    assert result.ok


def test_representation_internal_gap_in_list_range_segment_is_clipped(
    representation_internal_gap_catalog,
):
    # #465: a list segment can be a range, and the clip spans the whole request
    # rather than being reported per segment — one clip for one request, which is
    # the point of the shared pass.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": [{"from": 2010, "to": 2020}, 2022],
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), representation_internal_gap_catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "requested period 2010..2020,2022" in clip[0].message, clip[0].message
    assert "ordered for 2010..2012,2018..2020,2022" in clip[0].message, clip[0].message
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
    result = validate_semantic(_project([source]), representation_internal_gap_catalog)
    codes = {i.code for i in result.issues}
    assert "binding_state_drifts_within_period" not in codes
    assert result.ok


def test_representation_full_coverage_to_open_end_is_no_drift(
    uneven_representation_catalog,
):
    # Pinning the long-lived column covers the requested range inside an
    # open-ended (`9999-12-31`) state. This guards both overflow and a spurious
    # one-day trailing gap at the open-end sentinel.
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
    result = validate_semantic(_project([source]), uneven_representation_catalog)
    assert result.issues == ()
    assert result.ok


@pytest.fixture
def list_year_segment_gap_catalog():
    """The pinned column `kon` starts midway through the 2020 list segment while
    sibling `kon_detalj` fills the leading half. The pin is present in the
    segment, so the per-segment presence loop is not enough."""
    from _slugged_db import add_state, build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2020-07-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2020-01-01",
        valid_to="2020-06-30",
        delivery_column_name="kon_detalj",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_representation_gap_in_list_year_segment_is_clipped(
    list_year_segment_gap_catalog,
):
    # A year member of a list is an INTERVAL, not an instant: pinning `kon`
    # (delivered from July 2020) clips away Jan-Jun 2020, and the ordered period
    # says so to the day rather than rounding the year in.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": [2020, 2021],
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), list_year_segment_gap_catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "ordered for 2020-07-01..2021" in clip[0].message, clip[0].message
    assert result.ok


@pytest.fixture
def february_catalog():
    """A valid February-only delivery under-covers a requested calendar year."""
    from _slugged_db import build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2019-02-01', valid_to = '2019-02-28' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_february_representation_has_partial_annual_coverage(
    february_catalog,
):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2019, "to": 2019},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), february_catalog)
    # February delivery does not establish coverage of the rest of the year.
    assert [(i.code, i.level) for i in result.issues] == [
        ("range_period_partially_covered", "info")
    ]
    assert result.ok


def test_representation_merged_family_uses_expanded_windows(catalog):
    # #319/#465: lonfink is one annual state expanded into Jan/Feb/Mars windows
    # sharing the same state_id. A range spanning them must compare the expanded
    # windows, not collapse them by state_id before checking a pinned
    # representation (collapsed, the annual state would look fully covering).
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": "2018-01", "to": "2018-03"},
        "bindings": [
            {
                "variable": "scb/lisa/lonfink",
                "type": "numeric",
                "representation": "LonFinkMars",
            }
        ],
    }
    result = validate_semantic(_project([source]), catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    # The request renders through the shared grammar, so it canonicalizes to
    # the coarsest exact token (`2018-01..2018-03` IS `2018-Q1`) — the same
    # spelling the order manifest carries.
    assert "requested period 2018-Q1" in clip[0].message, clip[0].message
    assert "ordered for 2018-03" in clip[0].message, clip[0].message
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
    )
    codes = {i.code for i in result.issues}
    assert "range_period_partially_covered" not in codes
    assert result.ok


def test_range_partially_covered_is_flagged(catalog):
    # kon first delivered 2018; a 2010-2020 binding is clipped to 2018-2020.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2020})]),
        catalog,
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert issue.level == "info"
    assert issue.path == "/sources/0/bindings/0/variable"
    # The report names the period the ORDER will carry, in the manifest's own
    # spelling — not a gap the researcher has to subtract themselves.
    assert "requested period 2010..2020" in issue.message, issue.message
    assert "ordered for 2018..2020" in issue.message, issue.message
    # Info is non-blocking: the available sub-range still orders.
    assert result.ok


def test_zero_coverage_is_only_period_outside_state_validity(catalog):
    # A range entirely BEFORE kon's first state is zero coverage, not partial —
    # `resolve_at` returns no states, so only the existing code fires.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2015})]),
        catalog,
    )
    codes = {i.code for i in result.issues}
    assert "period_outside_state_validity" in codes
    assert "range_period_partially_covered" not in codes


def test_point_period_has_no_partial_finding(catalog):
    # A point period is a single instant — no requested span to under-cover.
    result = validate_semantic(_project([_kon_source(2018)]), catalog)
    assert "range_period_partially_covered" not in {i.code for i in result.issues}


@pytest.fixture
def internal_gap_catalog():
    """`scb/lisa/kon` delivered in TWO non-adjacent windows under one variant:
    2010-2012, then 2016-9999 — an INTERNAL gap (2013-2015 has no state at all).
    The seeded state is re-windowed to the later era; the earlier era is added as a
    second state, leaving the 2013-2015 hole."""
    from _slugged_db import add_state, build_slugged_db

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
        delivery_column_name="Kon",  # the same column, delivered in two eras
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_internal_gap_in_range_is_flagged(internal_gap_catalog):
    # Range 2010-2018 over a concept with a 2013-2015 hole → the ordered period
    # is the disjoint pair, so the hole is named by what it interrupts.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2018})]),
        internal_gap_catalog,
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert "ordered for 2010..2012,2016..2018" in issue.message, issue.message
    assert result.ok


def test_day_adjacent_windows_leave_no_gap(internal_gap_catalog):
    # A range covering only the populated tail (2016+) is fully covered. Guards the
    # adjacency math: string `valid_from > valid_to` would false-positive a gap.
    result = validate_semantic(
        _project([_kon_source({"from": 2016, "to": 2018})]),
        internal_gap_catalog,
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
    result = validate_semantic(_project([source]), catalog)
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
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert "ordered for 2018..2019-02-28" in issue.message, issue.message
    # No impossible Feb-29 leaked into either endpoint.
    assert "2019-02-29" not in issue.message, issue.message
    assert result.ok


# ── #307: the period LIST form (interrupted series) ──────────────────────────
# Structural validation guarantees the list is non-empty, sorted, and disjoint
# before this layer runs. The shared pass then treats the list as ONE request
# with holes, not a series of independent ones: it reports one
# `range_period_partially_covered` for the whole request rather than one per
# segment. The holes stay real, though — the request is NOT its enclosing range:
# a window overlapping only inside a HOLE is never co-existence, and segments are
# resolved on their own terms so one cannot suppress another's canonical
# fallback.


def test_list_period_clean_resolves_with_replacement_hint(catalog):
    # Both segments inside kon's single 2018+ state: the state intersecting both
    # segments counts ONCE (no phantom drift info). The fixture's kon→syss
    # succession is effective for the second segment, so the only issue is the
    # non-blocking replacement hint.
    result = validate_semantic(
        _project([_kon_source([2018, {"from": 2019, "to": 2020}])]),
        catalog,
    )
    assert [i.code for i in result.issues] == ["variable_replaced"]
    assert result.ok


def test_list_period_uncovered_segment_is_clipped_not_an_error(catalog):
    # Y-45: kon's state starts 2018, so the 2010..2012 segment simply is not
    # available — under §12 intersection semantics the request is CLIPPED to
    # where the binding exists and the order carries 2018. A segment the
    # materializer drops must not be an error the SPA blocks the order on.
    result = validate_semantic(
        _project([_kon_source([{"from": 2010, "to": 2012}, 2018])]),
        catalog,
    )
    assert "period_outside_state_validity" not in {i.code for i in result.issues}
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "requested period 2010..2012,2018" in clip[0].message, clip[0].message
    assert "ordered for 2018" in clip[0].message, clip[0].message
    assert result.ok


def test_list_period_reports_one_clip_for_the_whole_request(catalog):
    # Several unavailable segments are ONE clip stating the whole ordered
    # period, not one finding per segment: the researcher fixes the period
    # against what the order will actually carry.
    result = validate_semantic(
        _project([_kon_source([2015, 2016, 2018])]),
        catalog,
    )
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    # 2015 and 2016 are day-adjacent, so the request is the merged
    # `2015..2016,2018` — two windows, not three.
    assert "requested period 2015..2016,2018" in clip[0].message, clip[0].message
    assert "ordered for 2018" in clip[0].message, clip[0].message
    assert result.ok


def test_list_period_partial_coverage_names_the_ordered_period(catalog):
    # kon starts 2018: the 2017..2019 segment is partly available and the
    # 2021..2022 segment fully. One info, naming the disjoint window that
    # survives the clip.
    result = validate_semantic(
        _project(
            [_kon_source([{"from": 2017, "to": 2019}, {"from": 2021, "to": 2022}])]
        ),
        catalog,
    )
    partial = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(partial) == 1
    assert "requested period 2017..2019,2021..2022" in partial[0].message
    assert "ordered for 2018..2019,2021..2022" in partial[0].message
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
    )
    drift = [i for i in result.issues if i.code == "binding_state_drifts_within_period"]
    assert len(drift) == 1
    assert "spans 2 states" in drift[0].message
    assert result.ok


@pytest.fixture
def gap_overlap_catalog():
    """Two DISTINCT columns whose validity windows overlap only BETWEEN the
    requested segments of an interrupted series: `Kon` 2005-2014 and `KonNy`
    2012-2025 (mutual overlap 2012-2014). A researcher binding segments
    [2010, 2020] touches one column per segment — never both at one requested
    instant — while the scalar range 2010..2020 includes the overlap window."""
    from _slugged_db import add_state, build_slugged_db

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
    from _slugged_db import add_state, build_slugged_db

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


def test_pinned_representation_missing_middle_segment_is_clipped(
    middle_segment_sibling_catalog,
):
    # The pin exists at the outer segments only, so the middle segment's 2015
    # extract would be empty for that column. The ordered period drops it, which
    # is both the report and what the manifest will say.
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
    result = validate_semantic(_project([source]), middle_segment_sibling_catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "requested period 2010,2015,2020" in clip[0].message, clip[0].message
    assert "ordered for 2010,2020" in clip[0].message, clip[0].message
    # Non-blocking (info): the available segments still order.
    assert result.ok


# ── Validation cost: required states, not code cardinality ──────────────────
#
# The consumer is `POST /api/project/validate` for an ordinary geography
# binding: one variable delivered under several variants with a state per year,
# every state pointing at the SAME large code list. Validation reads identity
# and state metadata only, so no code membership may be loaded for it and no
# state outside the requested period may be hydrated. (`_states_in_bounds`
# still SELECTs the variable's metadata history and filters the bounds in
# Python — that read is unchanged here; what this test pins is that neither
# unrequested history nor a shared code list adds per-row query work.)

_GEOGRAPHY_SHAPES = ((3, 2), (3, 2000), (40, 2000))

_GEOGRAPHY_SOURCE = {
    "name": "rtb-2000",
    "register_variant": "scb/rtb/personer",
    "period": 2000,
    "bindings": [
        {
            "variable": "scb/rtb/forsamling",
            "type": "categorical",
            "value_set": "class/sun2020",
        }
    ],
}


def _geography_conn(n_states: int, n_codes: int):
    """`scb/rtb/forsamling` delivered under two variants with `n_states` yearly
    states each, all sharing ONE value set of `n_codes` codes."""
    from _slugged_db import (
        add_register,
        add_state,
        add_value_set,
        add_variable,
        add_variant,
        build_slugged_db,
    )

    conn = build_slugged_db()
    add_register(conn, register_id=2, slug="rtb", name="RTB")
    add_variable(conn, register_id=2, var_id=99, name="Församling", slug="forsamling")
    add_value_set(
        conn,
        value_set_id=7,
        codes=[(f"{i:05d}", f"Församling {i}") for i in range(n_codes)],
    )
    for register_variant_id, slug in ((20, "personer"), (21, "hushall")):
        add_variant(
            conn,
            register_variant_id=register_variant_id,
            register_id=2,
            slug=slug,
            name=slug.title(),
        )
        for year in range(2000, 2000 + n_states):
            add_state(
                conn,
                register_id=2,
                variable_slug="forsamling",
                register_variant_id=register_variant_id,
                valid_from=f"{year}-01-01",
                valid_to=f"{year}-12-31",
                delivery_column_name="Forsamling",
                value_set_id=7,
            )
    conn.commit()
    return conn


def test_geography_binding_validates_without_loading_code_lists():
    # One statement count for every shape: growing the shared code list or the
    # state history the requested period does NOT need must not add query work.
    counts: dict[tuple[int, int], int] = {}
    for shape in _GEOGRAPHY_SHAPES:
        conn = _geography_conn(*shape)
        catalog = Catalog(conn)  # constructed before tracing: boot, not validation
        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        try:
            result = validate_semantic(_project([_GEOGRAPHY_SOURCE]), catalog)
        finally:
            conn.set_trace_callback(None)
            conn.close()
        assert result.issues == ()
        assert not [
            sql
            for sql in statements
            if "value_set_member" in sql
            or "value_code" in sql
            or "classification_conformance_code" in sql
        ], f"{shape} loaded code lists"
        counts[shape] = len(statements)
    assert len(set(counts.values())) == 1, counts
