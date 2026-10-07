"""Reserved-slug drift guard (#228): ``reg_meta.fqid``'s reserved slugs mirror the
catalog sub-resource routes in the committed ``openapi.json``.

See reg_meta/DESIGN.md → FQID grammar. The reserved-slug sets exist ONLY to stop
a slug from shadowing one of these catalog routes, so the two MUST stay in
lockstep. The routes are read from the committed OpenAPI snapshot, which
``test_openapi_snapshot.py`` pins to the live app, so a new catalog sub-resource
route forgotten from the reserved set fails here. Set equality gives both
directions at once: every route's tail IS reserved, and every reserved token HAS
a route. Route ORDER (suffixed routes before the greedy catch-all) is pinned by
each suffixed route answering with its own shape over HTTP.
"""

from __future__ import annotations

import json
from pathlib import Path

from reg_meta.fqid import (
    RESERVED_GROUP_SLUG,
    RESERVED_HTTP_SUFFIX_SLUGS,
    RESERVED_VARIANTS_SLUG,
)

_OPENAPI = Path(__file__).resolve().parents[1] / "openapi.json"
_CATALOG_PATHS = [
    path
    for path in json.loads(_OPENAPI.read_text(encoding="utf-8"))["paths"]
    if path.startswith("/api/catalog")
]


def test_binding_suffix_routes_are_the_reserved_suffix_slugs():
    # The `{fqid}/<suffix>` binding routes; the bare `/api/catalog/{fqid}`
    # catch-all has no `{fqid}/`, so it is excluded.
    suffix_tails = {
        path.rsplit("{fqid}/", 1)[1] for path in _CATALOG_PATHS if "{fqid}/" in path
    }
    assert suffix_tails == RESERVED_HTTP_SUFFIX_SLUGS


def test_register_sub_resource_tail_is_the_reserved_variants_slug():
    # Only a fixed-shape route ending in a LITERAL tail reserves that tail. The
    # group routes ride a `{key}` (their `/graph` tail is the binding suffix
    # already reserved above), and the `/api/catalog` root is no sub-resource.
    variants_tails = {
        tail
        for path in _CATALOG_PATHS
        if "{fqid}" not in path and "{key}" not in path and path != "/api/catalog"
        for tail in [path.rsplit("/", 1)[1]]
        if "{" not in tail
    }
    assert variants_tails == {RESERVED_VARIANTS_SLUG}


def test_group_prefix_is_the_reserved_group_slug():
    # The `/catalog/group/{provider}/...` subject route puts a LITERAL token
    # where a provider slug would sit, so a provider named `group` would have its
    # binding-suffix URL captured by it: the reservation is on the PROVIDER slot.
    # `catalog` (the shared route prefix) is not a reservable token.
    provider_prefixes = {
        segs[i - 1]
        for path in _CATALOG_PATHS
        for segs in [path.split("/")]
        for i, seg in enumerate(segs)
        if seg == "{provider}" and i > 0 and "{" not in segs[i - 1]
    } - {"catalog"}
    assert provider_prefixes == {RESERVED_GROUP_SLUG}
