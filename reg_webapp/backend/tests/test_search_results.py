"""`GET /api/search` result rows against the slugged ``catalog_db`` fixture (#350).

Covers register / variable / classification leaf hits and their navigable
FQIDs, value and code groups (#352), code-aware classification surfacing,
concept-group folding (#322) and diacritic parity (å→a) with the SPA filter.
"""

from __future__ import annotations

from backend_test_support import search_group as _group

# ── leaf hits + navigable FQIDs ──────────────────────────────────────────────


def test_register_hit_carries_fqid(client):
    g = _group(client.get("/api/search", params={"q": "LISA"}).json(), "registers")
    fqids = [r["fqid"] for r in g["results"]]
    assert "scb/lisa" in fqids
    assert all(r["type"] == "register" for r in g["results"])


def test_variable_hit_carries_binding_fqid(client):
    g = _group(client.get("/api/search", params={"q": "Kön"}).json(), "variables")
    hit = next(r for r in g["results"] if r["type"] == "variable")
    assert hit["fqid"] == "scb/lisa/kon"
    # The owning register name rides under the wire key `register`.
    assert hit["register"] == "LISA"


def test_classification_leaf_hit(client):
    # `SUN2020` matches only the sun2020 short_name → a single leaf (no fold).
    g = _group(
        client.get("/api/search", params={"q": "SUN2020"}).json(), "classifications"
    )
    leaves = [r for r in g["results"] if r["type"] == "classification"]
    hit = next(r for r in leaves if r["fqid"] == "class/sun2020")
    # A lone member keeps its family hint (symmetric with variable leaves).
    assert hit["concept_group"] == "sun"
    assert hit["concept_group_label"]
    # The terminal edition itself carries no `terminal_fqid` (it IS current).
    assert hit["terminal_fqid"] is None


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


def test_value_groups_always_present(client):
    # Present even when nothing matches (keep all groups in the envelope).
    body = client.get("/api/search", params={"q": "zzqq"}).json()
    for name in ("classification_codes", "register_value_sets"):
        g = _group(body, name)
        assert not g["has_more"]
        assert g["results"] == []


def test_code_label_hit_carries_owning_variable(client):
    # "Man" is a value label on the kon binding's value set (seeded in conftest)
    # → a code hit annotated with its owning variable.
    g = _group(
        client.get("/api/search", params={"q": "Man"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["type"] == "code"
    assert hit["code"] == "1"
    # The owning variable carries the binding FQID + register context.
    owner = next(v for v in hit["variables"] if v["fqid"] == "scb/lisa/kon")
    assert owner["register"] == "LISA"
    assert hit["variable_count"] >= 1


def test_code_hit_carries_owning_classification(client):
    # The "Man" code is also linked to the sun2020 classification (seeded in
    # conftest) → the hit carries a non-empty classification owner + count.
    g = _group(
        client.get("/api/search", params={"q": "Man"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["classification_count"] >= 1
    owner = next(c for c in hit["classifications"] if c["fqid"] == "class/sun2020")
    assert owner["short_name"] == "SUN2020"


def test_code_shaped_query_well_formed(client):
    # A code-shaped query (digit + len>=3) drives the value_code.code exact/prefix
    # path. The fixture has no "0180" code, so this asserts the group stays
    # well-formed (no 500); the code-match resolution itself is covered by the
    # reg_meta query-layer unit test.
    body = client.get("/api/search", params={"q": "0180"}).json()
    assert isinstance(_group(body, "classification_codes")["results"], list)
    assert isinstance(_group(body, "register_value_sets")["results"], list)


def test_code_hit_carries_code_system(client):
    # The "Man" code is owned by the sun2020 classification (short_name SUN2020),
    # so its inferred `code_system` is that short_name (#393 item 3).
    g = _group(
        client.get("/api/search", params={"q": "Man"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["code_system"] == "SUN2020"


def test_c12_code_hit_uses_icd_code_system(client):
    g = _group(
        client.get("/api/search", params={"q": "C12"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Malign tumör i tungbas")
    assert hit["code_system"] == "ICD-10-SE"
    assert [c["fqid"] for c in hit["classifications"]] == ["class/icd-10-se"]


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


# ── code-aware classification surfacing (#393 item 5) ────────────────────────


def test_code_shaped_query_surfaces_owning_classification(client):
    # 'C12' is a code-shaped query (digit + len>=3) owned by ICD-10-SE
    # (seeded in conftest), matching no classification NAME. The classifications
    # group must surface ICD-10-SE via code-containment, navigable.
    g = _group(client.get("/api/search", params={"q": "C12"}).json(), "classifications")
    fqids = [r["fqid"] for r in g["results"] if r["type"] == "classification"]
    assert "class/icd-10-se" in fqids
    assert len(g["results"]) >= 1


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


# ── diacritic parity with the SPA filter (å→a) ───────────────────────────────


def test_diacritic_folding_matches_spa(client):
    # unicode61 folds both index + query side, so "kon" (no umlaut) finds "Kön" —
    # the same fold the SPA's foldText applies client-side.
    folded = _group(client.get("/api/search", params={"q": "kon"}).json(), "variables")
    exact = _group(client.get("/api/search", params={"q": "Kön"}).json(), "variables")
    assert any(r.get("fqid") == "scb/lisa/kon" for r in folded["results"])
    assert any(r.get("fqid") == "scb/lisa/kon" for r in exact["results"])
