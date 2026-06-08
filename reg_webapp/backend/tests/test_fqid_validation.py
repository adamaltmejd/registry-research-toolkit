"""FQID route-segment validation — the path-traversal allow-list.

See DESIGN.md → FQID path guard (catalog_fqid.py). Two layers:

1. Unit tests on ``validate_fqid_path`` directly (the chokepoint module): every
   traversal / malformed payload raises ``FqidPathError`` and every legal FQID
   passes. A binding leaf is a bare slug — the ``@version`` pin is retired, so any
   ``@`` is a non-slug character that is rejected here. The unit layer is where raw
   ``..`` payloads belong — an HTTP client normalizes ``scb/../etc`` to ``etc``
   before it reaches the server, so the raw-dotdot case can only be exercised
   against the function.
2. The SECURITY GATE through the live app: percent-encoded probes (which
   survive client normalization and arrive URL-decoded at the handler) return
   422 AND execute **zero SQL** (asserted via a sqlite3 trace hook counting
   statements), proving the guard runs before any Catalog query. Plus the
   retired ``@version`` form, now rejected at the gate.
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.catalog_fqid import FqidPathError, validate_fqid_path

# Raw payloads that must be REJECTED by the chokepoint (unit layer). Includes the
# raw `..` forms an HTTP client would collapse — only reachable here.
_REJECT_PATHS = [
    "scb/../etc/passwd",  # classic traversal
    "scb/..",
    "../etc",
    "scb/lisa/..",
    "scb/lisa/.",
    "scb/lisa/%2e%2e",  # not decoded at the function layer → literal `%`
    "scb/lisa/kon%00.json",  # literal `%` (and would be NUL post-decode)
    "scb/lisa/kon\x00",  # embedded NUL
    "scb/lisa/kon\n",  # trailing newline — the `$`-vs-`\Z` slug-regex hole, now closed
    "scb/lisa/kon\r",  # trailing carriage return
    "scb\n/lisa/kon",  # newline in the provider segment
    "scb//lisa",  # empty middle segment
    "/scb",  # leading slash → empty first segment
    "scb/",  # trailing slash → empty last segment
    "scb/lisa\\kon",  # backslash
    "scb/Lisa",  # uppercase (slug grammar)
    "scb/li sa",  # space
    # `@` is a non-slug character everywhere now that the @version pin is retired:
    # a binding leaf is a bare slug, so any `@` (including the once-legal pin form)
    # is rejected by the per-segment grammar.
    "scb/lisa/naringsgren@sni2007",  # the retired canonical pin form
    "scb/lisa/kon@@x",  # double @
    "scb/li@sa/kon",  # @ in a non-leaf segment
    "scb@x/lisa/kon",  # @ in provider segment
    "scb/lisa@x",  # @ on a 2-seg path
    "scb/lisa/naringsgren@bad/slug",  # @ then a slash → 4 segments
    # `class` is admitted ONLY as the leading classification prefix; in any other
    # slot it's a reserved token the guard now rejects (was previously deferred to
    # `parse`/`Fqid` downstream — which 500'd on the variants route).
    "scb/class/kon",  # class as register slot
    "scb/lisa/class",  # class as variable (leaf) slot
]
# NOTE: a 5-seg all-valid-slug path like `scb/lisa/kon/extra/more` is NOT a
# per-segment grammar violation — the chokepoint admits it (every segment is a
# slug); `reg_meta.fqid.parse` rejects the arity downstream (→ 422 at the app
# layer, covered by test_too_many_segments_returns_422).

# Legal FQIDs that must PASS the chokepoint (bare FQIDs — no @version pin).
_ACCEPT_PATHS = [
    ("scb", "scb"),
    ("scb/lisa", "scb/lisa"),
    ("scb/lisa/kon", "scb/lisa/kon"),
    ("class", "class"),
    ("class/sun2020", "class/sun2020"),
]


@pytest.mark.parametrize("raw", _REJECT_PATHS)
def test_validate_fqid_path_rejects(raw: str):
    with pytest.raises(FqidPathError):
        validate_fqid_path(raw)


@pytest.mark.parametrize(("raw", "expect_fqid"), _ACCEPT_PATHS)
def test_validate_fqid_path_accepts(raw: str, expect_fqid: str):
    result = validate_fqid_path(raw)
    assert result.fqid == expect_fqid


# ── SECURITY GATE: 422 + zero SQL through the live app ──────────────────────

# Probes that survive HTTP-client URL normalization and reach the handler
# decoded. The raw `..` forms collapse at the client, so the app layer uses the
# percent-encoded variants (which decode to the traversal segment server-side).
_APP_REJECT_PROBES = [
    "scb/lisa/%2e%2e",  # → `..` after Starlette decode
    "scb%2f..%2fetc",  # encoded slashes → `scb/../etc` (`..` segment)
    "scb/lisa/kon%00.json",  # embedded NUL
    "scb/lisa/kon@@x",  # double @ on the leaf
    "scb/lisa/naringsgren@bad/slug",  # @ then slash
    "scb/Lisa",  # uppercase
]


class _StatementCounter:
    """Wraps ``sqlite3.connect`` while active to count BOTH connection opens
    (``opens``) and SQL statements executed (``count``, via
    ``set_trace_callback``). A 422 path is a true "no DB hit" only if it runs zero
    SQL AND opens zero connections — the guard (a sub-dependency) rejects
    before the per-request connection ever opens."""

    def __init__(self) -> None:
        self.count = 0
        self.opens = 0
        self._orig = sqlite3.connect

    def __enter__(self) -> _StatementCounter:
        counter = self

        def traced(*args, **kwargs):  # noqa: ANN002, ANN003 — passthrough shim
            counter.opens += 1
            conn = counter._orig(*args, **kwargs)
            conn.set_trace_callback(lambda _stmt: counter._bump())
            return conn

        sqlite3.connect = traced
        return self

    def __exit__(self, *exc) -> None:  # noqa: ANN002
        sqlite3.connect = self._orig

    def _bump(self) -> None:
        self.count += 1

    def reset(self) -> None:
        self.count = 0
        self.opens = 0


@pytest.mark.parametrize("probe", _APP_REJECT_PROBES)
def test_path_traversal_returns_422_with_zero_sql(catalog_db, probe: str):
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        # Boot (the lifespan's open_db schema check) runs SQL — reset AFTER it so
        # we measure only the request.
        counter.reset()
        resp = client.get(f"/api/catalog/{probe}")
    assert resp.status_code == 422, f"{probe!r} should be 422, got {resp.status_code}"
    assert counter.count == 0, (
        f"{probe!r} executed {counter.count} SQL statement(s); the guard must "
        f"run before any Catalog query (zero SQL)"
    )
    # "no DB hit": the guard is a sub-dependency, so it 422s BEFORE the
    # per-request connection opens — a rejected path opens zero connections too.
    assert counter.opens == 0, (
        f"{probe!r} opened {counter.opens} connection(s); the guard must run "
        f"before the per-request connection opens (no DB hit on rejection)"
    )


@pytest.mark.parametrize(
    "probe",
    [
        "scb/lisa/naringsgren@sni2007",  # the retired canonical pin form
        "scb/lisa/naringsgren@bad/slug",
        "scb/lisa/kon@@x",
    ],
)
def test_at_version_rejected_by_gate(catalog_db, probe: str):
    """The `@version` pin is retired — a binding leaf is a bare slug, so any `@`
    (including the once-canonical `scb/lisa/naringsgren@sni2007`) fails the gate
    with 422 and ZERO SQL (the value set is resolved from the variant/period, never
    pinned on the FQID)."""
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(f"/api/catalog/{probe}?period=2020")
    assert resp.status_code == 422
    assert counter.count == 0


@pytest.mark.parametrize("path", ["_default", "scb/lisa/_default", "class/_default"])
def test_default_variant_literal_rejected_by_guard(catalog_db, path: str):
    """`_default` (the variant coordinate) is NOT a catalog path segment, so the
    guard rejects it in any position → 422 with no DB hit (variants are a
    register sub-resource, never a `/api/catalog/{fqid}` segment)."""
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(f"/api/catalog/{path}")
    assert resp.status_code == 422
    assert counter.opens == 0  # guard-rejected → no connection opened


@pytest.mark.parametrize("path", ["scb/class/kon", "scb/lisa/class"])
def test_class_literal_in_illegal_slot_guard_rejects(catalog_db, path: str):
    """`class` is admitted ONLY as the leading classification-prefix segment; in a
    register / variable slot the guard now rejects it → 422 with no connection
    opened (previously deferred to `parse`/`Fqid`, which 500'd on the variants
    route's direct `Fqid.register_fqid('class', …)`)."""
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(f"/api/catalog/{path}")
    assert resp.status_code == 422
    assert counter.opens == 0  # guard-rejected → no connection opened


# ── A5.2a-ii GATE: the FQID path guard on ALL 7 suffixed/sub-resource routes
# The "path-traversal payloads against EVERY {fqid:path} route" requirement.
# A percent-encoded traversal probe in the FQID part of each suffixed route must
# 422 with zero SQL + zero opens — the `_validated_fqid` (and the variants
# segment guard) is a sub-dependency that runs before the per-request open.

_KON = "scb/lisa/kon"
# The 6 binding-suffix routes (FQID before the literal suffix) + the variants
# sub-resource (FQID is the 2-seg register prefix before the literal `variants`).
_SUFFIXED_ROUTE_TEMPLATES = [
    "/api/catalog/{fqid}/states",
    "/api/catalog/{fqid}/predecessors",
    "/api/catalog/{fqid}/successors",
    "/api/catalog/{fqid}/related",
    "/api/catalog/{fqid}/lineage",
    "/api/catalog/{fqid}/lineage_warnings",
]


@pytest.mark.parametrize("template", _SUFFIXED_ROUTE_TEMPLATES)
@pytest.mark.parametrize(
    "probe", ["scb/lisa/%2e%2e", "scb%2f..%2fetc", "scb/lisa/kon%00"]
)
def test_suffixed_route_traversal_422_zero_sql(catalog_db, template: str, probe: str):
    url = template.format(fqid=probe)
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(url)
    assert resp.status_code == 422, f"{url!r} → {resp.status_code}"
    assert counter.count == 0, f"{url!r} executed {counter.count} SQL statement(s)"
    assert counter.opens == 0, f"{url!r} opened {counter.opens} connection(s)"


@pytest.mark.parametrize(
    "url",
    [
        "/api/catalog/scb/%2e%2e/variants",  # traversal in the register segment
        "/api/catalog/%2e%2e/lisa/variants",  # traversal in the provider segment
        "/api/catalog/scb/_default/variants",  # `_default` not a path segment
        "/api/catalog/scb/Lisa/variants",  # uppercase (slug grammar)
    ],
)
def test_variants_route_traversal_422_zero_sql(catalog_db, url: str):
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(url)
    assert resp.status_code == 422, f"{url!r} → {resp.status_code}"
    assert counter.count == 0
    assert counter.opens == 0


# ── GATE: the ?period / ?variant / ?value_set_version query allow-lists ──────
# The "?period= canonicalization" requirement: malformed period / variant
# values (SQLi probes, traversal, embedded NULs, percent-encoded slashes) return
# 422 with zero SQL executed AND zero connections opened — the parser is a
# pre-open dependency, so a rejection never touches the DB.

# The named probes (a SQLi string, a traversal, an embedded NUL, an encoded
# slash) plus a couple of grammar misses.
_BAD_PERIODS = [
    "2020'; DROP TABLE--",
    "../../etc/passwd",
    "2020%00",  # percent-encoded NUL (Starlette decodes to a real NUL)
    "2020%2f..%2f",  # percent-encoded slashes
    "2020-13",  # month out of range
    "2018..badtoken",  # bad range endpoint
    "2020\n",  # trailing newline — the `$`-vs-`\Z` period-regex hole, now closed
    "HT2020\n",
    "2020-Q3\n",
]
_BAD_VARIANTS = ["Std", "../etc", "x%00", "x'; DROP--", "in valid"]
# [A5.3b] ?value_set_version is a FREE-TEXT label (matched by a Python `==` in
# resolve_at, NOT SQL), so the gate rejects only control chars / over-length —
# NOT slug-shape (real labels carry spaces/commas/case). A non-matching value like
# "../etc" or "Sni2007" is now ACCEPTED (it simply narrows to no state); the bad
# set is control/NUL chars + an over-cap string.
_BAD_VSV = ["x\x00", "a\tb", "a\nb", "x" * 201]


@pytest.mark.parametrize("period", _BAD_PERIODS)
def test_bad_period_query_422_zero_sql(catalog_db, period: str):
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(f"/api/catalog/{_KON}", params={"period": period})
    assert resp.status_code == 422, f"{period!r} → {resp.status_code}"
    assert counter.count == 0, f"{period!r} executed {counter.count} SQL statement(s)"
    assert counter.opens == 0, f"{period!r} opened {counter.opens} connection(s)"


@pytest.mark.parametrize("variant", _BAD_VARIANTS)
def test_bad_variant_query_422_zero_sql(catalog_db, variant: str):
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(
            f"/api/catalog/{_KON}", params={"period": "2020", "variant": variant}
        )
    assert resp.status_code == 422, f"{variant!r} → {resp.status_code}"
    assert counter.count == 0
    assert counter.opens == 0


@pytest.mark.parametrize("vsv", _BAD_VSV)
def test_bad_value_set_version_query_422_zero_sql(catalog_db, vsv: str):
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(
            f"/api/catalog/{_KON}",
            params={"period": "2020", "value_set_version": vsv},
        )
    assert resp.status_code == 422, f"{vsv!r} → {resp.status_code}"
    assert counter.count == 0
    assert counter.opens == 0
