"""CLI `resolve` and split-sibling isolation across `get` commands."""

from __future__ import annotations

import io

from cli_test_support import run_json as _run_json

# ---------------------------------------------------------------------------
# Split-sibling isolation (A2.2 split → A2.6 query)
# ---------------------------------------------------------------------------


class TestSplitSiblingIsolation:
    """A2.2 puts several variables under one `(register_id, provider_key)` —
    split siblings share a `var_id` but have distinct `variable_id`s, slugs,
    names, and their OWN `variable_state` + value sets. Once a `get_*` command
    has matched ONE sibling (by its unique name/alias), it must select states by
    that row's `variable_id`, not by the shared `provider_key`. Filtering on
    `provider_key` leaks the OTHER sibling's states/codes — these tests fail on
    that bug (they see 2 states / both value sets where 1 sibling has 1 each).
    """

    @staticmethod
    def _split_db():
        from _slugged_db import (
            add_state,
            add_value_set,
            add_variable,
            build_slugged_db,
        )

        # Default fixture: register 1 (lisa), variant 10, variable var_id 44
        # name "Kön" slug "kon". Replace it with two explicit siblings so the
        # scenario is unambiguous: drop the default variable layer, add A + B.
        conn = build_slugged_db(variable=None)
        # Sibling A — provider_key 44, distinct slug + name + own column/codes.
        add_variable(
            conn, register_id=1, var_id=44, name="SSYK 3-pos", slug="ssyk-3pos"
        )
        add_value_set(conn, value_set_id=1, codes=[("A1", "A-one"), ("A2", "A-two")])
        add_state(
            conn,
            register_id=1,
            variable_slug="ssyk-3pos",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Ssyk3",
            value_set_id=1,
        )
        # Sibling B — same provider_key 44, its own slug/name/column/codes.
        add_variable(
            conn, register_id=1, var_id=44, name="SSYK 5-pos", slug="ssyk-5pos"
        )
        add_value_set(conn, value_set_id=2, codes=[("B1", "B-one"), ("B2", "B-two")])
        add_state(
            conn,
            register_id=1,
            variable_slug="ssyk-5pos",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Ssyk5",
            value_set_id=2,
        )
        conn.commit()
        return conn

    def test_varinfo_returns_only_matched_sibling_states(self):
        from reg_meta.queries import get_varinfo

        conn = self._split_db()
        # Match sibling A by its unique name → exactly one variable, and its
        # instances must be ONLY A's single state (column Ssyk3), not B's.
        result = get_varinfo(conn, "SSYK 3-pos", register="lisa")
        assert len(result) == 1
        a = result[0]
        assert a["name"] == "SSYK 3-pos"
        cols = sorted(c for inst in a["instances"] for c in inst["aliases"])
        assert cols == ["Ssyk3"]
        assert len(a["instances"]) == 1

        # And sibling B in isolation sees only Ssyk5.
        b = get_varinfo(conn, "SSYK 5-pos", register="lisa")[0]
        b_cols = sorted(c for inst in b["instances"] for c in inst["aliases"])
        assert b_cols == ["Ssyk5"]

    def test_varinfo_resolves_swedish_delivery_column(self):
        """The alias-column fallback in get_varinfo folds the queried column
        against the stored delivery column via py_lower. A variable delivered as
        "Ägare" must resolve when queried by "ägare" (lowercase Swedish Ä). ASCII
        LOWER() leaves Ä/ä distinct, so this returns not-found there (refs #853)."""
        from _slugged_db import build_slugged_db
        from reg_meta.queries import get_varinfo

        conn = build_slugged_db(delivery_column_name="Ägare", variable_slug="agare")
        result = get_varinfo(conn, "ägare")
        assert len(result) == 1
        assert result[0]["name"] == "Kön"

    def test_values_returns_only_matched_sibling_codes(self):
        from reg_meta.queries import get_values_by_variable

        conn = self._split_db()
        # Match sibling A by name → only A's value set (A1/A2), never B's.
        result = get_values_by_variable(conn, "SSYK 3-pos", register="lisa")
        codes = {v["code"] for inst in result["instances"] for v in inst["values"]}
        assert codes == {"A1", "A2"}

        b = get_values_by_variable(conn, "SSYK 5-pos", register="lisa")
        b_codes = {v["code"] for inst in b["instances"] for v in inst["values"]}
        assert b_codes == {"B1", "B2"}

    def test_classifications_isolate_per_sibling(self):
        """A2.7 resolves the A2.6 limitation: `classifications_for_variable`
        follows exact state_classification links keyed by variable_id,
        so each split sibling returns ONLY its own classification. Pre-A2.7 (off
        `variable_instance`, keyed by the shared `var_id`) both siblings would
        return BOTH classifications."""
        from reg_meta.queries import classifications_for_variable

        conn = self._split_db()
        # Two distinct classifications; tag sibling A's state with one, B's with
        # the other. (`build_slugged_db` already seeded one classification, so
        # don't hard-code ids — capture the new rows' lastrowid.)
        cls_a = conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SSYK2012', 'Std för yrkesklassificering', 'ssyk2012')"
        ).lastrowid
        cls_b = conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SSYK96', 'Std för yrkesklassificering 96', 'ssyk96')"
        ).lastrowid
        a_vid, b_vid = (
            conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            for slug in ("ssyk-3pos", "ssyk-5pos")
        )
        conn.execute(
            "INSERT INTO state_classification (classification_id, state_id) SELECT ?, state_id FROM variable_state WHERE variable_id = ?",
            (cls_a, a_vid),
        )
        conn.execute(
            "INSERT INTO state_classification (classification_id, state_id) SELECT ?, state_id FROM variable_state WHERE variable_id = ?",
            (cls_b, b_vid),
        )
        conn.commit()

        a_cls = classifications_for_variable(conn, a_vid)
        assert [c["short_name"] for c in a_cls] == ["SSYK2012"]
        b_cls = classifications_for_variable(conn, b_vid)
        assert [c["short_name"] for c in b_cls] == ["SSYK96"]

    def test_values_by_numeric_var_id_attributes_split_siblings(self):
        """A2.7 (Codex P2 #149): a numeric var_id that maps to >1 split sibling
        (same provider_key 44, distinct variable_id/slug) no longer merges them
        anonymously — every instance carries its owning variable's slug, and the
        codes stay sibling-exact (no cross-contamination)."""
        from reg_meta.queries import get_values_by_variable

        conn = self._split_db()
        result = get_values_by_variable(conn, "44", register="lisa")
        by_slug = {
            inst["variable_slug"]: {v["code"] for v in inst["values"]}
            for inst in result["instances"]
        }
        assert by_slug == {"ssyk-3pos": {"A1", "A2"}, "ssyk-5pos": {"B1", "B2"}}


# ---------------------------------------------------------------------------
# Resolve
# ---------------------------------------------------------------------------


def _split_sibling_db():
    """Slugged DB whose register 1 holds TWO variables under provider_key 44,
    both named `Kön` and both delivering the column `Kon` — the A2.2 split
    geometry (siblings share a `provider_key`; only the slug differs)."""
    from _slugged_db import add_variable, build_slugged_db

    conn = build_slugged_db()
    add_variable(conn, register_id=1, var_id=44, name="Kön", slug="kon-2")
    conn.execute(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) "
        "SELECT variable_id, 10, 'Kon' FROM variable WHERE slug = 'kon-2'"
    )
    conn.commit()
    return conn


class TestResolve:
    def test_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "resolve", "--columns", "Kon", "--register", "TESTREG"]
        )
        assert code == 0
        col = data["data"]["columns"][0]
        assert col["status"] == "matched"
        assert all(m["register_id"] == "1" for m in col["matches"])
        # Nothing is split in the fixture register, so the match stays unique.
        assert len(col["matches"]) == 1

    def test_require_match_fails(self, db_path: str):
        _data, code = _run_json(
            ["--db", db_path, "resolve", "--columns", "ZZZNOPE", "--require-match"]
        )
        assert code == 17

    def test_malformed_stdin_json_is_a_usage_error(self, db_path: str, monkeypatch):
        monkeypatch.setattr("sys.stdin", io.StringIO('["Kon",'))
        data, code = _run_json(["--db", db_path, "resolve"])
        assert code == 2
        assert data["error"]["code"] == "usage_error"
        assert "--columns" in data["error"]["remediation"]

    def test_alias_anomaly(self, db_path: str):
        """Both TestCol and TestKolumn should resolve to var 100."""
        data1, _ = _run_json(
            [
                "--db",
                db_path,
                "resolve",
                "--columns",
                "TestCol",
                "--register",
                "TESTREG",
            ]
        )
        data2, _ = _run_json(
            [
                "--db",
                db_path,
                "resolve",
                "--columns",
                "TestKolumn",
                "--register",
                "TESTREG",
            ]
        )
        assert data1["data"]["columns"][0]["matches"][0]["var_id"] == 100
        assert data2["data"]["columns"][0]["matches"][0]["var_id"] == 100

    def test_swedish_uppercase_column_folds(self):
        """#853 regression: a delivery column stored with an uppercase Swedish
        letter (`Ägare`) must resolve case-insensitively. SQLite `LOWER()` is
        ASCII-only (`LOWER('Ägare')` == `'Ägare'`), so the column-side fold must
        go through the Unicode-aware `py_lower` UDF — otherwise the Python-lowered
        query value `'ägare'` never matches and the column reports `no_match`."""
        from _slugged_db import build_slugged_db
        from reg_meta.queries import resolve

        conn = build_slugged_db(delivery_column_name="Ägare", variable_slug="agare")
        results = resolve(conn, ["ägare"])
        assert results[0]["status"] == "matched"
        assert results[0]["matches"][0]["matched_column"] == "Ägare"

    def test_match_carries_canonical_fqid(self, db_path: str):
        """The canonical binding FQID is the identity a caller navigates by —
        register_id/var_id/name do not distinguish split siblings."""
        data, _ = _run_json(["--db", db_path, "resolve", "--columns", "Kon"])
        match = data["data"]["columns"][0]["matches"][0]
        assert match["fqid"] == "scb/testreg/kon"

    def test_split_siblings_both_survive(self):
        """Y-47 regression: an A2.2 split leaves sibling variables that SHARE
        (register_id, provider_key) and name. Grouping the alias lookup on that
        pair returned ONE match for two real variables, so a caller needing a
        unique variable could not see the ambiguity. Grouping on the canonical
        `variable_id` keeps both, distinguished by their FQIDs.

        Fails on the pre-Y-47 query: one match, `matches[0]["fqid"]` a single
        sibling."""
        from reg_meta.queries import resolve

        conn = _split_sibling_db()
        for scope in (None, "LISA"):
            results = resolve(conn, ["Kon"], register=scope)
            matches = results[0]["matches"]
            assert results[0]["status"] == "matched"
            # Both siblings, in stable variable_id order; every other displayed
            # field is identical, so only the FQID tells them apart.
            assert [m["fqid"] for m in matches] == ["scb/lisa/kon", "scb/lisa/kon-2"]
            assert {m["var_id"] for m in matches} == {44}

    def test_repeated_alias_does_not_duplicate_variable(self):
        """One variable delivering the same column under several alias rows
        (per variant, or a case spelling) is still ONE match, carrying one
        deterministic representative spelling.

        Y-102: that spelling is the STATE's own where a state names the column —
        `Kon` here, though `KON` is the lower of the two alias rows by byte order
        — which is the spelling the catalog's own readers list the column under
        (`catalog.representative_columns`), so `resolve` and a browse row name one
        column the same way."""
        from _slugged_db import build_slugged_db
        from reg_meta.queries import resolve

        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "SELECT variable_id, 10, 'KON' FROM variable WHERE slug = 'kon'"
        )
        conn.commit()
        matches = resolve(conn, ["kon"])[0]["matches"]
        assert [m["fqid"] for m in matches] == ["scb/lisa/kon"]
        assert matches[0]["matched_column"] == "Kon"

    def test_representative_spelling_falls_back_to_lowest_alias(self):
        """Y-102, the other arm of the rule: a column carried only by
        `variable_alias` — a co-delivered spelling no `variable_state` names — has
        no state spelling to prefer, so the lowest by byte order represents it, as
        it does in `Catalog.register_variable_deliveries`."""
        from _slugged_db import build_slugged_db
        from reg_meta.queries import resolve

        conn = build_slugged_db()
        conn.executemany(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "SELECT variable_id, 10, ? FROM variable WHERE slug = 'kon'",
            [("Konkod",), ("KONKOD",)],
        )
        conn.commit()
        matches = resolve(conn, ["konkod"])[0]["matches"]
        assert [m["matched_column"] for m in matches] == ["KONKOD"]
