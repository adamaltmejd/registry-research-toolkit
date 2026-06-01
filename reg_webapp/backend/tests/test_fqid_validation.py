"""§16 FQID route-segment validation — the path-traversal allow-list.

Two layers:

1. Unit tests on ``validate_fqid_path`` directly (the chokepoint module): every
   traversal / malformed payload raises ``FqidPathError`` and every legal FQID
   (incl. the ``@version`` carve-out) passes. The unit layer is where raw ``..``
   payloads belong — an HTTP client normalizes ``scb/../etc`` to ``etc`` before
   it reaches the server, so the raw-dotdot case can only be exercised against
   the function.
2. The §16 SECURITY GATE through the live app: percent-encoded probes (which
   survive client normalization and arrive URL-decoded at the handler) return
   422 AND execute **zero SQL** (asserted via a sqlite3 trace hook counting
   statements), proving the guard runs before any Catalog query. Plus the
   positive ``@version`` carve-out and its two negative cousins.
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
    "scb/lisa/kon@@x",  # double @
    "scb/li@sa/kon",  # @ in a non-leaf segment
    "scb@x/lisa/kon",  # @ in provider segment
    "scb/lisa@x",  # @ on a 2-seg path (not a binding leaf)
    "scb/lisa/naringsgren@bad/slug",  # @ then a slash → 4 segments
]
# NOTE: a 5-seg all-valid-slug path like `scb/lisa/kon/extra/more` is NOT a
# per-segment grammar violation — the chokepoint admits it (every segment is a
# slug); `reg_meta.fqid.parse` rejects the arity downstream (→ 422 at the app
# layer, covered by test_too_many_segments_returns_422).

# Legal FQIDs that must PASS the chokepoint.
_ACCEPT_PATHS = [
    ("scb", "scb", None),
    ("scb/lisa", "scb/lisa", None),
    ("scb/lisa/kon", "scb/lisa/kon", None),
    ("class", "class", None),
    ("class/sun2020", "class/sun2020", None),
    # The @version carve-out: the bare FQID is stripped, the version stripped out.
    ("scb/lisa/naringsgren@sni2007", "scb/lisa/naringsgren", "sni2007"),
]


@pytest.mark.parametrize("raw", _REJECT_PATHS)
def test_validate_fqid_path_rejects(raw: str):
    with pytest.raises(FqidPathError):
        validate_fqid_path(raw)


@pytest.mark.parametrize(("raw", "expect_fqid", "expect_version"), _ACCEPT_PATHS)
def test_validate_fqid_path_accepts(
    raw: str, expect_fqid: str, expect_version: str | None
):
    result = validate_fqid_path(raw)
    assert result.fqid == expect_fqid
    assert result.value_set_version == expect_version


# ── §16 SECURITY GATE: 422 + zero SQL through the live app ──────────────────

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
    SQL AND opens zero connections — the §16 guard (a sub-dependency) rejects
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
        f"{probe!r} executed {counter.count} SQL statement(s); the §16 guard must "
        f"run before any Catalog query (zero SQL)"
    )
    # §16 "no DB hit": the guard is a sub-dependency, so it 422s BEFORE the
    # per-request connection opens — a rejected path opens zero connections too.
    assert counter.opens == 0, (
        f"{probe!r} opened {counter.opens} connection(s); the §16 guard must run "
        f"before the per-request connection opens (no DB hit on rejection)"
    )


def test_at_version_positive_carveout_passes_gate(catalog_db):
    """The canonical pinned-binding URL `scb/lisa/naringsgren@sni2007` passes the
    §16 gate (the `@version` pin is legal, not traversal). It then 404s because
    that binding isn't in the fixture — but it reached resolution (SQL ran),
    proving the GATE admitted it rather than 422'ing it."""
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get("/api/catalog/scb/lisa/naringsgren@sni2007")
    # Not 422: the gate admitted the @version form. 404 because the binding is
    # absent from the fixture (resolution ran → SQL executed).
    assert resp.status_code == 404, (
        f"expected 404 (admitted then not found), got {resp.status_code}"
    )
    assert counter.count > 0, (
        "the admitted @version path must reach resolution (SQL runs)"
    )


@pytest.mark.parametrize("probe", ["scb/lisa/naringsgren@bad/slug", "scb/lisa/kon@@x"])
def test_at_version_negative_cousins_fail_gate(catalog_db, probe: str):
    """The two illegitimate `@` forms fail the gate with 422 and zero SQL."""
    with _StatementCounter() as counter, TestClient(create_app()) as client:
        counter.reset()
        resp = client.get(f"/api/catalog/{probe}")
    assert resp.status_code == 422
    assert counter.count == 0


@pytest.mark.parametrize("path", ["_default", "scb/lisa/_default", "class/_default"])
def test_reserved_literal_in_illegal_position_returns_422(catalog_db, path: str):
    """The guard ADMITS the `class`/`_default` reserved literals in any segment
    (§5.2), then `parse` rejects an illegal placement → 422 (not 404/500). Locks
    the admit-then-parse-rejects contract against a future `parse` change."""
    with TestClient(create_app()) as client:
        resp = client.get(f"/api/catalog/{path}")
    assert resp.status_code == 422
