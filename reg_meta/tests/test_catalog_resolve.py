"""Catalog.resolve(): related documents, binding metadata, split siblings and
parsed FQID objects."""

from __future__ import annotations

import sqlite3

import catalog_test_support
import pytest
from _slugged_db import (
    add_state,
    build_slugged_db,
)
from reg_meta.catalog import (
    Catalog,
    ResolvedRegister,
    ResolvedVariable,
)
from reg_meta.doc_db import DOC_SCHEMA_VERSION
from reg_meta.fqid import Fqid, FqidError
from reg_meta_build.doc_db import DOC_DDL

# The shared fixture, bound by assignment: an imported name used only as a
# test parameter reads as an unused import redefined (ruff F401/F811).
slugged_conn = catalog_test_support.slugged_conn


def _related_doc_conn(register: str = "lisa") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DOC_DDL)
    conn.execute(
        "INSERT INTO doc_meta (key, value) VALUES ('schema_version', ?)",
        (DOC_SCHEMA_VERSION,),
    )
    conn.execute(
        "INSERT INTO related_document ("
        "register, title, filename, source_url, license, fetched, "
        "sha256, byte_size, content"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            register,
            "LISA manual",
            "lisa_manual.pdf",
            "https://example.test/lisa_manual.pdf",
            "CC BY 4.0",
            "2026-06-29",
            "c" * 64,
            7,
            b"manual",
        ),
    )
    conn.commit()
    return conn


class TestResolveRegister:
    def test_resolves_with_related_documents(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn, doc_conn=_related_doc_conn()).resolve("scb/lisa")
        assert isinstance(r, ResolvedRegister)
        assert [doc.filename for doc in r.related_documents] == ["lisa_manual.pdf"]


class TestResolveBinding:
    """A2.5 (see DESIGN.md → Catalog API surface): `resolve()` returns the longitudinal `ResolvedVariable` —
    the variable's shared metadata + its `variable_state` history, no per-edition
    cvid. The interim `ResolvedVariableBinding` and the `editions()` path that
    returned it were removed in A2.6."""

    def test_resolves_register_related_documents(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn, doc_conn=_related_doc_conn()).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert [doc.filename for doc in r.related_documents] == ["lisa_manual.pdf"]

    def test_variant_label_is_none_for_a_null_named_variant(self) -> None:
        # A NULL-named variant → variant_label None (the consumer falls back to the
        # slug for display). The slug stays present as the add coordinate.
        conn = build_slugged_db(variant=(None, "snoskotrar", 10))
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.states[0].variant == "snoskotrar"
        assert r.states[0].variant_label is None

    def test_operational_definition_flows_through_resolve(self) -> None:
        # #892/#932: the per-(split-)variable distinguishing text is a first-class
        # column surfaced on `ResolvedVariable`. Default fixture leaves it NULL.
        conn = build_slugged_db()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.operational_definition is None
        conn.execute(
            "UPDATE variable SET operational_definition = ? WHERE slug = 'kon'",
            ("Avser personens registrerade kön vid årets slut.",),
        )
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert (
            r.operational_definition
            == "Avser personens registrerade kön vid årets slut."
        )


class TestStoredVariableSlug:
    """A2.1.5 (see reg_meta_build/DESIGN.md → Slug curation): the resolver reads the stored `variable.slug`, not a slug
    derived from `delivery_column_name` at query time. A2.5: resolution is now
    longitudinal (`ResolvedVariable`, period-independent) — split siblings
    resolve to distinct `variable_id`s, and point selection moved to
    `resolve_at`."""

    def test_split_siblings_resolve_to_distinct_variables(self) -> None:
        # A2.2 split → A2.5 longitudinal: two sibling variables share provider_key
        # '44' (a split puts several variables under one source key; see reg_meta_build/DESIGN.md → Build-time triage (SCB)) but have
        # distinct slugs + distinct `variable_id`s and own DISJOINT delivery
        # columns. Each resolves to its OWN `ResolvedVariable` with its own state
        # — no shared cvid fan-out (the interim hazard the A2.2 flip removed).
        conn = build_slugged_db(delivery_column_name="Ssyk3", variable_slug="ssyk-3pos")
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '44', 'SSYK 5-pos', 'ssyk-5pos')"
        )
        # The Ssyk5 sibling's own state under the same variant. Target by slug:
        # provider_key '44' is shared across the split siblings, so var_id can't
        # disambiguate — the register-unique slug can.
        add_state(
            conn,
            register_id=1,
            variable_slug="ssyk-5pos",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Ssyk5",
        )
        conn.commit()
        r3 = Catalog(conn).resolve("scb/lisa/ssyk-3pos")
        r5 = Catalog(conn).resolve("scb/lisa/ssyk-5pos")
        assert isinstance(r3, ResolvedVariable)
        assert isinstance(r5, ResolvedVariable)
        # Distinct variables (distinct variable_id), each with its own column.
        assert r3.variable_id != r5.variable_id
        assert [s.delivery_column_name for s in r3.states] == ["Ssyk3"]
        assert [s.delivery_column_name for s in r5.states] == ["Ssyk5"]


class TestResolveFqidObject:
    def test_rejects_incomplete_fqid_object(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # A hand-constructed Fqid with the wrong fields for its kind round-
        # trips to a different kind on emit-then-parse; the resolver must
        # fail fast with FqidError instead of TypeError inside a resolver.
        from reg_meta.fqid import FqidKind

        # Claims to be a binding but carries no `variable`, so emit-then-parse
        # yields a 2-segment REGISTER kind — a mismatch the resolver rejects.
        bad = Fqid(
            kind=FqidKind.VARIABLE_BINDING,
            provider="scb",
            register="lisa",
            variable=None,
        )
        with pytest.raises(FqidError, match="incomplete"):
            Catalog(slugged_conn).resolve(bad)
