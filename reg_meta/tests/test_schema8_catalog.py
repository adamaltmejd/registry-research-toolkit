"""Book-local coding evidence and lossless schema-8 catalog boundaries."""

from __future__ import annotations

import json

from _slugged_db import add_state, add_value_set, build_slugged_db
from reg_meta.catalog import Catalog, ResolvedVariable
from reg_meta.cli_common import write_json


def _coded_db():
    conn = build_slugged_db()
    conn.execute("DELETE FROM variable_state")
    add_value_set(
        conn,
        value_set_id=3,
        codes=[("1", "Source label"), ("9", "Other"), ("99", "Missing")],
    )
    book = conn.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]
    state = add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="2018-12-31",
        delivery_column_name="Kon",
        value_set_id=3,
        classification_id=book,
    )
    conn.execute(
        "INSERT INTO classification_conformance VALUES (?, ?, 'extended', 3, 1, 2, ?)",
        (state, book, 1 / 3),
    )
    conn.execute(
        "INSERT INTO classification_code (classification_id, code_id) SELECT ?, code_id FROM value_code WHERE code = '1'",
        (book,),
    )
    for code, kind, meaning in (
        ("9", "nonstandard", None),
        ("99", "sentinel", "missing"),
    ):
        conn.execute(
            "INSERT INTO classification_conformance_code SELECT ?, ?, code_id, ?, ?, '[]' FROM value_code WHERE code = ?",
            (state, book, kind, meaning, code),
        )
    return conn, state, book


def test_multiple_books_keep_separate_evidence_without_duplicate_states():
    conn, state, _ = _coded_db()
    second = conn.execute(
        "INSERT INTO classification (slug, short_name, name) VALUES ('other', 'Other', 'Other book') RETURNING id"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO state_classification (state_id, classification_id) VALUES (?, ?)",
        (state, second),
    )
    cat = Catalog(conn)
    [resolved] = cat.states("scb/lisa/kon")
    assert [b.slug for b in resolved.classifications] == ["other", "sun2020"]
    assert resolved.classifications[0].conformance is None
    verdict = resolved.classifications[1].conformance
    assert verdict is not None
    assert (verdict.nonstandard_code_count, verdict.sentinel_code_count) == (1, 1)
    assert [(m.code, m.member_kind) for m in verdict.nonconforming_codes] == [
        ("9", "nonstandard"),
        ("99", "sentinel"),
    ]
    assert [
        (m.code, m.label)
        for m in cat.state_canonical_codes(state, 3, classification_slug="sun2020")
    ] == [("1", "Source label")]
    assert [
        m.code
        for m in cat.state_nonstandard_codes(state, 3, classification_slug="sun2020")
    ] == ["9"]
    [sentinel] = cat.state_sentinel_codes(state, 3, classification_slug="sun2020")
    assert sentinel.sentinel_meaning == "missing"
    assert cat.state_sentinel_codes(state, 3, classification_slug="other") is None
    assert cat.state_sentinel_codes(state, 4, classification_slug="sun2020") is None


def test_per_column_book_evidence_requires_exact_original_alias_window():
    conn, state, book = _coded_db()
    variable = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'kon'"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO variable_alias_window (variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, coding_metadata, value_set_id, value_set_version_label) VALUES (?, 10, 'Kon', '2017-01-01', '2019-12-31', 'per_column', 3, 'wave')",
        (variable,),
    )
    certificate = {
        "valid_from": "2017-01-01",
        "valid_to": "2019-12-31",
        "delivery_column_name": "Kon",
        "classification_sha256": "a" * 64,
        "source_fingerprints": ["b" * 64],
        "members": [["99", "Missing"]],
        "provenance": "reviewed",
    }
    evidence = {
        "declared_classification": "sun2020",
        "status": "extended",
        "checked_codes": ["1", "9", "99"],
        "nonconforming_members": [["9", "Other"]],
        "sentinel_members": [["99", "Missing"]],
        "scoped_sentinels": [certificate],
    }
    conn.execute(
        "INSERT INTO alias_window_classification VALUES (?, 10, 'Kon', '2017-01-01', ?, 'reviewed', ?)",
        (variable, book, json.dumps(evidence)),
    )
    cat = Catalog(conn)
    [resolved] = cat.states("scb/lisa/kon")
    assert (resolved.valid_from, resolved.coding_window_from) == (
        "2018-01-01",
        "2017-01-01",
    )
    [member] = cat.state_sentinel_codes(
        state,
        3,
        classification_slug="sun2020",
        delivery_column_name="Kon",
        alias_window_from=resolved.coding_window_from,
    )
    assert member.sentinel_meaning is None
    assert member.scoped_sentinels[0].source_fingerprints == ("b" * 64,)
    assert (
        cat.state_sentinel_codes(
            state,
            3,
            classification_slug="sun2020",
            delivery_column_name="Kon",
            alias_window_from="2018-01-01",
        )
        is None
    )
    assert (
        cat.state_sentinel_codes(
            state,
            3,
            classification_slug="sun2020",
            delivery_column_name="Sibling",
            alias_window_from="2017-01-01",
        )
        is None
    )


def test_storage_identifiers_are_lossless_json_strings_in_models_and_cli(capsys):
    conn, state, _ = _coded_db()
    large = 2**62 + 123
    conn.execute(
        "UPDATE variable_state SET state_id = ? WHERE state_id = ?", (large, state)
    )
    resolved = Catalog(conn).resolve("scb/lisa/kon")
    assert isinstance(resolved, ResolvedVariable)
    assert resolved.states[0].state_id == large
    assert resolved.model_dump(mode="json")["states"][0]["state_id"] == str(large)
    write_json(
        {"state_id": large, "register_id": large, "count": large, "year": 2018}, None
    )
    assert json.loads(capsys.readouterr().out) == {
        "state_id": str(large),
        "register_id": str(large),
        "count": large,
        "year": 2018,
    }


def test_alias_classification_counts_only_overlapping_states():
    from reg_meta.queries import classifications_for_variable

    conn, _, book = _coded_db()
    conn.execute("DELETE FROM classification_conformance_code")
    conn.execute("DELETE FROM classification_conformance")
    conn.execute("DELETE FROM state_classification")
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2020-01-01",
        valid_to="2020-12-31",
        delivery_column_name="Kon",
        value_set_id=3,
    )
    variable = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'kon'"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO variable_alias_window (variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, coding_metadata, value_set_id) VALUES (?, 10, 'Kon', '2018-01-01', '2018-12-31', 'per_column', 3)",
        (variable,),
    )
    conn.execute(
        "INSERT INTO alias_window_classification VALUES (?, 10, 'Kon', '2018-01-01', ?, NULL, NULL)",
        (variable, book),
    )
    [row] = classifications_for_variable(conn, variable)
    assert row["instance_count"] == 1


def test_graph_keeps_full_metadata_without_hydrating_codes(monkeypatch):
    from reg_meta.graph import _graph_states

    conn, _, _ = _coded_db()
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2019-01-01",
        valid_to="2019-12-31",
        delivery_column_name="KonNew",
        value_set_id=3,
    )
    cat = Catalog(conn)
    expected = _graph_states(tuple(cat.states("scb/lisa/kon")))

    def unexpected_codes(*args, **kwargs):
        raise AssertionError("graph must not hydrate value-set membership")

    monkeypatch.setattr(cat, "_value_set_codes", unexpected_codes)
    graph = cat.graph_for_fqid("scb/lisa/kon")
    [node] = graph.nodes
    assert node.states == expected
    assert len(node.states) == 2


def test_warning_scope_filters_preserve_default_and_exact_variable_evidence():
    import pytest
    from reg_meta.catalog import DataWarning
    from reg_meta.source_evidence import canonical_sha256

    conn, _, _ = _coded_db()
    variable = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'kon'"
    ).fetchone()[0]
    for variable_id, fqid in ((None, None), (variable, "scb/lisa/kon")):
        payload = {
            name: field.default
            for name, field in DataWarning.model_fields.items()
            if not field.is_required()
        }
        payload.update(
            register_fqid="scb/lisa",
            variable_fqid=fqid,
            code="source-limitation",
            severity="warning",
            summary="Retained uncertainty",
            detail="Exact source fact unavailable",
            diagnostic_detail_sha256="a" * 64,
            source_subject="source",
        )
        payload["warning_id"] = canonical_sha256(payload)
        conn.execute(
            "INSERT INTO data_warning (warning_id, register_id, variable_id, warning_json) VALUES (?, 1, ?, ?)",
            (payload["warning_id"], variable_id, json.dumps(payload)),
        )
    cat = Catalog(conn)
    both = cat.data_warnings("scb/lisa/kon")
    assert len(both) == 2
    assert cat.data_warnings("scb/lisa/kon", include_unassigned=False) == tuple(
        w for w in both if w.variable_fqid is not None
    )
    assert cat.data_warnings("scb/lisa", unassigned_only=True) == tuple(
        w for w in both if w.variable_fqid is None
    )
    with pytest.raises(ValueError, match="unassigned_only requires"):
        cat.data_warnings("scb/lisa", unassigned_only=True, include_unassigned=False)
    # A changed stored payload must cross validation again, even with the same ID.
    original = both[0]
    conn.execute(
        "UPDATE data_warning SET warning_json = json_set(warning_json, '$.summary', 'Changed without updating identity') WHERE warning_id = ?",
        (original.warning_id,),
    )
    with pytest.raises(ValueError, match="identity does not match"):
        cat.data_warnings("scb/lisa")


def test_variable_lookup_preserves_all_matching_register_candidates():
    from _slugged_db import add_variable

    conn = build_slugged_db()
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name, slug) VALUES (99, 1, 'Duplicate coordinate', 'lisa')"
    )
    add_variable(
        conn,
        register_id=99,
        var_id=900,
        slug="second-register-only",
        name="Only in second register",
    )
    row = Catalog(conn)._lookup_variable("scb", "lisa", "second-register-only")
    assert row is not None
    assert row["register_id"] == 99
    assert (
        Catalog(conn)._lookup_variable("scb", "missing", "second-register-only") is None
    )
