"""`GET /api/search` result rows against the slugged ``catalog_db`` fixture (#350).

Covers a non-terminal classification edition's ``terminal_fqid``, a
register-local code's null ``code_system`` and concept-group folding (#322).
Leaf hits, code owners, code-aware surfacing and diacritic folding are pinned by
``conformance/cases/http_search``.
"""

from __future__ import annotations

from backend_test_support import search_group as _group

# ── leaf hits + navigable FQIDs ──────────────────────────────────────────────


def test_lone_old_edition_leaf_carries_terminal_fqid(client):
    # `SUN1996` matches only the sun1996 short_name — a lone, NON-terminal edition
    # of the sun1996 → sun2000 → sun2020 succession chain (#571), and one NOT in any
    # concept group, so it stays a leaf rather than folding. The reg_meta fold
    # annotates it with the terminal edition's fqid; the route must surface it on
    # `ClassificationSearchResult` so the SPA can link "current edition".
    g = _group(
        client.get("/api/search", params={"q": "SUN1996"}).json(), "classifications"
    )
    leaves = [r for r in g["results"] if r["type"] == "classification"]
    hit = next(r for r in leaves if r["fqid"] == "class/sun1996")
    assert hit["terminal_fqid"] == "class/sun2020"


# ── value/code groups (#352) ─────────────────────────────────────────────────


def test_register_local_code_has_null_code_system(client):
    # A code with NO owning classification (the kvinna_only value, seeded as a
    # register-local value with no classification owner) has code_system == null.
    g = _group(
        client.get("/api/search", params={"q": "Kvinna"}).json(),
        "register_value_sets",
    )
    hit = next(
        r for r in g["results"] if r["label"] == "Kvinna" and not r["classifications"]
    )
    assert hit["code_system"] is None


# ── concept-group folding (#322) ─────────────────────────────────────────────


def test_variable_concept_group_folds(client):
    # inkjan + inkfeb both named "Inkomst" fold into the `ink` group row.
    g = _group(client.get("/api/search", params={"q": "Inkomst"}).json(), "variables")
    groups = [r for r in g["results"] if r["type"] == "group"]
    assert groups, "expected a folded concept-group row in the variables group"
    grp = next(r for r in groups if r["group_key"] == "ink")
    assert grp["kind"] == "variable"
    assert grp["member_count"] == 2
    assert {m["fqid"] for m in grp["members"]} == {"scb/rams/inkjan", "scb/rams/inkfeb"}


def test_classification_group_folds_without_duplicate_leaves(client):
    # The terminal sun2020 carries the name "Svensk utbildningsnomenklatur", which
    # is also the `sun` group label → a hit on it folds into ONE group row AND its
    # member leaves are SUBSUMED (not emitted standalone too — the #350 review bug:
    # classification leaves were duplicated as both leaf and folded member). The
    # `sun` group's members are the terminal dimensions sun2020 + niva-test (#608 /
    # #516 umbrella shape); the superseded sun2000/sun1996 share the name but are
    # NOT members, so they stay standalone leaves — and never collide with members.
    g = _group(
        client.get("/api/search", params={"q": "utbildningsnomenklatur"}).json(),
        "classifications",
    )
    groups = [r for r in g["results"] if r["type"] == "group"]
    grp = next(r for r in groups if r["kind"] == "classification")
    member_fqids = {m["fqid"] for m in grp["members"]}
    assert {"class/sun2020", "class/niva-test"} <= member_fqids
    leaf_fqids = {r["fqid"] for r in g["results"] if r["type"] == "classification"}
    # No member appears as a standalone leaf alongside its folded group row.
    assert not (member_fqids & leaf_fqids)
