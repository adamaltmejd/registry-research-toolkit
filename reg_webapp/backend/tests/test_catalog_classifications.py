"""`/api/catalog/class` classification root and leaf browse against ``catalog_db``.

Covers the classification root's terminal-edition listing and family folding,
and the classification leaf's edition chain, value-set codes and
cross-references.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient
from reg_webapp.app import create_app


def test_classification_root_drops_superseded_and_folds_dimension_group(client):
    """#608: the classification root surfaces only TERMINAL editions as children
    (a row whose `superseded_by` is truthy — a successor exists — is dropped) and
    carries the #516 umbrella `sun` group over its members. The fixture seeds
    sun1996 → sun2000 → sun2020 with `supersedes_id` projected from those edges
    (so the superseded-by filter is genuinely exercised, not a no-op on NULLs);
    the `sun` group members are the terminal sun2020 + the non-succession
    `niva-test` aggregate, which therefore stay in `children` and fold. The
    umbrella is AXIS-LESS (`axes == []`)."""
    body = client.get("/api/catalog/class").json()
    assert len(body["groups"]) == 1
    group = body["groups"][0]
    assert group["key"] == "sun"
    assert group["axes"] == []
    assert {m["fqid"] for m in group["members"]} == {
        "class/sun2020",
        "class/niva-test",
    }
    child_fqids = {c["fqid"] for c in body["children"]}
    # Terminal editions + non-succession aggregates survive and fold under the group.
    assert "class/sun2020" in child_fqids
    assert "class/niva-test" in child_fqids
    # The superseded editions are dropped from children (the regression lock — this
    # fails on the pre-#608 code, which surfaced every classification as a child).
    assert "class/sun1996" not in child_fqids
    assert "class/sun2000" not in child_fqids


def test_classification_root_replaces_future_successor_family(catalog_db):
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "UPDATE classification SET valid_from = 1997 WHERE slug = 'icd-10-se'"
        )
        conn.execute(
            "INSERT INTO classification "
            "(short_name, name, slug, valid_from) "
            "VALUES ('ICD-11-SE', 'ICD-11-SE', 'icd-11-se', 2027)"
        )
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('icd-10-se', 'icd-11-se', 2027, 'curated:test')"
        )

    with TestClient(create_app()) as local_client:
        root = local_client.get("/api/catalog/class").json()
        family = local_client.get("/api/catalog/group/class/icd").json()

    families = {item["key"]: item for item in root["families"]}
    assert families["icd"] == {
        "kind": "classification-family",
        "key": "icd",
        "label": "ICD",
        "editions": family["editions"],
    }
    child_fqids = {c["fqid"] for c in root["children"]}
    assert "class/icd-10-se" not in child_fqids
    assert "class/icd-11-se" not in child_fqids
    assert [edition["slug"] for edition in family["editions"]] == [
        "icd-10-se",
        "icd-11-se",
    ]
    assert [edition["is_current"] for edition in family["editions"]] == [True, False]


def test_classification_leaf_embeds_full_edition_chain(client):
    # #571: the classification leaf embeds the FULL succession timeline (oldest
    # first, terminal last) so the browse panel renders every edition synchronously.
    # The fixture seeds sun1996 → sun2000 → sun2020 (live terminal) — all LIVE rows,
    # matching the build validator's invariant (succession edges resolve to live
    # classification slugs; validate.py, the classification_replaced_by check).
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    chain = resp.json()["edition_chain"]
    assert [e["slug"] for e in chain] == ["sun1996", "sun2000", "sun2020"]
    by_slug = {e["slug"]: e for e in chain}
    # Every edition is a live row → each carries a fqid (no dead-edition shape).
    assert all(e["fqid"] == f"class/{e['slug']}" for e in chain)
    assert by_slug["sun1996"]["name"] == "Svensk utbildningsnomenklatur"
    assert by_slug["sun1996"]["effective_year"] == 2000
    # Live terminal == the queried edition: is_current AND is_self.
    assert by_slug["sun2020"]["fqid"] == "class/sun2020"
    assert by_slug["sun2020"]["is_current"] is True
    assert by_slug["sun2020"]["is_self"] is True
    assert by_slug["sun2020"]["effective_year"] is None
    assert by_slug["sun2000"]["is_current"] is False
    assert by_slug["sun2000"]["is_self"] is False


def test_classification_leaf_embeds_value_set_codes(client):
    # #609: the classification leaf embeds the RESOLVED edition's value-set codes
    # (code-ordered) so the SPA's code viewer renders synchronously. The fixture
    # links sun2020 to a canonical "Man" code (is_valid=1).
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    codes = resp.json()["codes"]
    by_label = {c["label"]: c for c in codes}
    assert "Man" in by_label
    assert all(code["is_valid"] is True for code in codes)
    assert by_label["Man"]["is_valid"] is True
    # Code-ordered (the SQL ORDER BY vc.code, vc.label).
    assert [c["code"] for c in codes] == sorted(c["code"] for c in codes)


def test_classification_leaf_embeds_dimension_cross_reference(client):
    # #609: the leaf embeds the curated umbrella group(s) it belongs to (the niva ↔
    # aggregate granularity cross-reference). The fixture's `group:sun` umbrella
    # (AXIS-LESS) has sun2020 + the niva-test aggregate as members.
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    dimensions = resp.json()["dimensions"]
    assert [g["key"] for g in dimensions] == ["sun"]
    assert dimensions[0]["axes"] == []
    member_fqids = {m["fqid"] for m in dimensions[0]["members"]}
    assert {"class/sun2020", "class/niva-test"} <= member_fqids


def test_classification_leaf_embeds_family_cross_reference(catalog_db):
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "UPDATE classification SET valid_from = 1997 WHERE slug = 'icd-10-se'"
        )
        conn.execute(
            "INSERT INTO classification "
            "(short_name, name, slug, valid_from) "
            "VALUES ('ICD-11-SE', 'ICD-11-SE', 'icd-11-se', 2027)"
        )
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('icd-10-se', 'icd-11-se', 2027, 'curated:test')"
        )

    with TestClient(create_app()) as local_client:
        resp = local_client.get("/api/catalog/class/icd-11-se")

    assert resp.status_code == 200
    family = resp.json()["family"]
    assert family["kind"] == "classification-family"
    assert family["key"] == "icd"
    assert family["label"] == "ICD"
    assert [edition["slug"] for edition in family["editions"]] == [
        "icd-10-se",
        "icd-11-se",
    ]


def test_classification_leaf_without_codes_or_dimensions_is_empty(client):
    # A classification in no umbrella group and with no codes carries empty lists
    # (the SPA omits both sections). sun1996 is a superseded edition with neither.
    resp = client.get("/api/catalog/class/sun1996")
    assert resp.status_code == 200
    body = resp.json()
    assert body["codes"] == []
    assert body["dimensions"] == []
    assert body["family"] is None


def test_classification_split_root_edition_chain_fans_out(client):
    # #605 / #579: browsing the SPLIT root embeds ALL downstream branches in
    # edition_chain (the forward closure), not just the deterministic-first one.
    # The fixture seeds sni-root1996 → {sni-grp2000, sni-ink2000, sni-niv2000},
    # each → its 2020 tip. The closure is DFS in ORDER BY successor_slug (grp < ink
    # < niv), each branch's 2000→2020 subtree before the next.
    resp = client.get("/api/catalog/class/sni-root1996")
    assert resp.status_code == 200
    chain = resp.json()["edition_chain"]
    assert [e["slug"] for e in chain] == [
        "sni-root1996",
        "sni-grp2000",
        "sni-grp2020",
        "sni-ink2000",
        "sni-ink2020",
        "sni-niv2000",
        "sni-niv2020",
    ]
    # All three 2020 branch tips are current → MULTIPLE is_current editions.
    currents = {e["slug"] for e in chain if e["is_current"]}
    assert currents == {"sni-grp2020", "sni-ink2020", "sni-niv2020"}
    assert sum(e["is_current"] for e in chain) == 3
    # The split root is the queried (self) edition; its year is its det-first edge's.
    self_editions = [e["slug"] for e in chain if e["is_self"]]
    assert self_editions == ["sni-root1996"]
    by_slug = {e["slug"]: e for e in chain}
    assert by_slug["sni-root1996"]["effective_year"] == 2000


def test_classification_split_branch_leaf_scopes_to_own_path(client):
    # #605: querying a LEAF of the split (sni-niv2020) returns ONLY its own path back
    # to the root — the inriktning/grupp sibling branches are NOT included.
    resp = client.get("/api/catalog/class/sni-niv2020")
    assert resp.status_code == 200
    chain = resp.json()["edition_chain"]
    assert [e["slug"] for e in chain] == [
        "sni-root1996",
        "sni-niv2000",
        "sni-niv2020",
    ]
    assert [e["slug"] for e in chain if e["is_current"]] == ["sni-niv2020"]
    assert [e["slug"] for e in chain if e["is_self"]] == ["sni-niv2020"]
