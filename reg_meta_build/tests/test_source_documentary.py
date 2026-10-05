"""Literal documentary bindings retain exact evidence and fail closed on drift."""

from __future__ import annotations

import json
from contextlib import closing
from types import SimpleNamespace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta.documentary import (
    DocumentaryRelationship,
    DocumentaryVariableReference,
    SourceCodeCrosswalkDeclaration,
    SourceCodeOperand,
    SourceDerivationClause,
    SourceDerivationDeclaration,
)
from reg_meta.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceRevision,
    canonical_sha256,
)
from reg_meta_build.curation_tree import DocumentaryBindingEntry
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata
from reg_meta_build.source_coordinates import native_variable_key
from reg_meta_build.source_documentary import compile_documentary_bindings
from reg_meta_build.source_records import (
    SourceEvidenceRow,
    SourceEvidenceTable,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row


def _setup(kind="derivation"):
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    records = []
    header = REGISTERINFORMATION_HEADER.split("|")
    for native in (1, 2):
        values = _var_row(
            colname=str(native),
            var_id=native,
            cvid=100 + native,
            varname=str(native),
            year="2000",
        ).split("|")
        records.append(
            clean_scb_row(
                header,
                native,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                revision,
            ).record
        )
    locator = RecordLocator(
        semantic_record_key=("table:literal",),
        physical_file="rows.csv",
        physical_table="literal",
        physical_record="row:2",
        physical_cells=("literal!A2",),
    )
    common = {
        "revision": revision,
        "locator": locator,
        "delivered_cells": (
            DeliveredCell(
                name="literal", present=True, raw_value="-", interpreted_value="-"
            ),
        ),
        "member_name": value_field("1"),
        "supplied_period": value_field("1973/74-1990 och framåt"),
        "description": None,
    }
    if kind == "derivation":
        declaration = SourceDerivationDeclaration(
            **common,
            clauses=(
                SourceDerivationClause(
                    name="Variabler", content=value_field("2 + GRVB")
                ),
                SourceDerivationClause(
                    name="Algoritm",
                    content=value_field("this is not an executable formula ** /"),
                ),
            ),
        )
        anchors = [
            {
                "clause_index": 0,
                "token": "2",
                "variable": "scb/example/operand",
                "originals_sha256": canonical_sha256(
                    [records[1].model_dump(mode="json")]
                ),
            }
        ]
        unresolved = [
            {
                "clause_index": 0,
                "token": "GRVB",
                "reason": "No exact native endpoint supplied.",
            }
        ]
    else:
        declaration = SourceCodeCrosswalkDeclaration(
            **common,
            section_period=None,
            section_locator=None,
            operands=(
                SourceCodeOperand(role="input", name="old code", code=value_field("-")),
                SourceCodeOperand(
                    role="output", name="new code", code=value_field("1")
                ),
            ),
        )
        anchors = []
        unresolved = [
            {
                "operand_index": 0,
                "token": "old code",
                "reason": "Historical namespace is literal only.",
            }
        ]
    table = SourceEvidenceTable(
        source="fixture",
        source_revision_id=revision.revision_id,
        name="literal",
        rows=(
            SourceEvidenceRow(
                locator=locator, role="declaration", cells=declaration.delivered_cells
            ),
        ),
    )
    entry = DocumentaryBindingEntry(
        source="fixture",
        table="literal",
        row="row:2",
        member="1",
        owner="scb/example/owner",
        payload_sha256=canonical_sha256(declaration.model_dump(mode="json")),
        table_sha256=canonical_sha256(table.model_dump(mode="json")),
        owner_originals_sha256=canonical_sha256([records[0].model_dump(mode="json")]),
        anchors=anchors,
        unresolved=unresolved,
        evidence="exact source evidence",
        noted="2026-09-30",
    )
    register = SimpleNamespace(
        documentary=SimpleNamespace(binding=[entry], retained=[]),
        source_file="registers/scb/example.toml",
    )
    tree = SimpleNamespace(registers=[register])
    keys = [native_variable_key(r) for r in records]
    assert all(keys)
    names = {
        "scb/example/owner": {("fixture", keys[0])},
        "scb/example/operand": {("fixture", keys[1])},
    }
    return tree, declaration, table, tuple(records), names


def _prepared(declarations, tables, records):
    def families(source, select_family):
        for record in records:
            key = native_variable_key(record)
            assert key is not None
            if select_family(key):
                yield key, (record,)

    return SimpleNamespace(
        iter_evidence=lambda: iter(
            SimpleNamespace(kind="reference", declaration=d) for d in declarations
        ),
        records=SimpleNamespace(
            iter_tables=lambda source: iter(tables), iter_native_families=families
        ),
    )


def _compile(
    setup,
    *,
    declarations=None,
    tables=None,
    records=None,
    names=None,
    sources=("fixture",),
):
    from reg_meta_build.prepared_catalog import ReferenceEvidence

    tree, d, t, rr, nn = setup
    prepared = _prepared(
        (d,) if declarations is None else declarations,
        (t,) if tables is None else tables,
        rr if records is None else records,
    )
    prepared.iter_evidence = lambda: iter(
        ReferenceEvidence(revision_id=x.revision.revision_id, declaration=x)
        for x in ((d,) if declarations is None else declarations)
    )
    return compile_documentary_bindings(
        tree,
        prepared,
        tuple(SimpleNamespace(source=s) for s in sources),
        nn if names is None else names,
    )


@pytest.mark.parametrize("kind", ["derivation", "code_crosswalk"])
def test_literal_binding_persists_without_availability_extension(tmp_path, kind):
    setup = _setup(kind)
    relations, issues = _compile(setup)
    assert not issues and len(relations) == 1
    relation = relations[0]
    assert type(relation).model_validate_json(relation.model_dump_json()) == relation
    assert relation.model_dump(mode="json")["relationship_id"] == str(
        relation.relationship_id
    )
    assert relation.binding_status == "owner_bound_literal"
    assert relation.declaration.model_dump(mode="json") == setup[1].model_dump(
        mode="json"
    )
    variables = tuple(
        ResolvedVariable(
            register=ResolvedRegister(provider="scb", slug="example", name="Example"),
            slug=slug,
            provider_key=slug,
            name=slug,
            definition=None,
            description=None,
            operational_definition=None,
            measurement_unit=None,
            is_sensitive=False,
            is_identifier=False,
            states=(
                ResolvedState(
                    variant=ResolvedVariant(slug="people", name="People"),
                    valid_from="2000-01-01",
                    valid_to="2000-12-31",
                    delivery_column_name=slug,
                    data_type="text",
                    data_length="1",
                    operational_definition=None,
                    provenance="source",
                ),
            ),
        )
        for slug in ("owner", "operand")
    )
    write_resolved_catalog(
        variables,
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        metadata=ResolvedMetadata(documentary_relationships=relations),
    )
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        rows = conn.execute(
            "SELECT owner_variable_id, declaration_json FROM source_relationship"
        ).fetchall()
        assert tuple(
            type(relations[0].declaration).model_validate_json(row["declaration_json"])
            for row in rows
        ) == tuple(relation.declaration for relation in relations)
        assert {row["owner_variable_id"] for row in rows} == {
            conn.execute(
                "SELECT variable_id FROM variable WHERE slug='owner'"
            ).fetchone()[0]
        }
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable_state WHERE valid_from <= '1973-12-31' AND valid_to >= '1973-01-01'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT valid_from FROM variable_state JOIN variable USING (variable_id) WHERE slug='operand'"
            ).fetchone()[0]
            == "2000-01-01"
        )
        with pytest.raises(ValidationError):
            type(relations[0]).model_validate_json("{}")


@pytest.mark.parametrize("wire_id", ["01", "+1", "1.0", " 1", "-1", "0", True, 1.5])
def test_documentary_json_rejects_invalid_storage_identifiers(wire_id):
    (relation,), issues = _compile(_setup())
    assert not issues
    payload = relation.model_dump(mode="json")
    payload["relationship_id"] = wire_id
    with pytest.raises(ValidationError):
        type(relation).model_validate_json(json.dumps(payload))
    payload = relation.model_dump()
    payload["relationship_id"] = str(relation.relationship_id)
    with pytest.raises(ValidationError):
        type(relation).model_validate(payload)


@pytest.mark.parametrize(
    "change", ["payload", "peer", "endpoint", "ownership", "missing", "duplicate"]
)
def test_complete_payload_peer_endpoint_and_ownership_guards_fail_closed(change):
    setup = _setup()
    _tree, d, t, records, _names = setup
    kwargs = {}
    if change == "payload":
        kwargs["declarations"] = (
            d.model_copy(update={"member_name": value_field("other")}),
        )
    elif change == "peer":
        kwargs["tables"] = (t.model_copy(update={"rows": (*t.rows, t.rows[0])}),)
    elif change == "endpoint":
        kwargs["records"] = (
            records[0],
            records[1].model_copy(update={"context": ("new context",)}),
        )
    elif change == "ownership":
        kwargs["names"] = {}
    elif change == "missing":
        kwargs["records"] = (records[0],)
    else:
        kwargs["declarations"] = (d, d)
    relations, issues = _compile(setup, **kwargs)
    assert (
        relations == ()
        and len(issues) == 1
        and issues[0].code == "stale_curation_entry"
    )


def test_unselected_source_is_not_bound_and_existing_deferral_remains_possible():
    assert _compile(_setup(), sources=("other",)) == ((), ())


def test_literal_coordinate_and_json_contract_reject_guessed_or_duplicate_tokens():
    setup = _setup()
    relations, _ = _compile(setup)
    r = relations[0]
    for token, index in (("weeks", 0), ("2", 4)):
        with pytest.raises(ValidationError):
            DocumentaryRelationship.model_validate_json(
                r.model_copy(
                    update={
                        "variables": (
                            DocumentaryVariableReference(
                                clause_index=index,
                                token=token,
                                variable="scb/example/operand",
                            ),
                        )
                    }
                ).model_dump_json()
            )
    with pytest.raises(ValidationError):
        DocumentaryRelationship.model_validate_json(
            r.model_copy(
                update={"unresolved": (r.unresolved[0], r.unresolved[0])}
            ).model_dump_json()
        )


def test_owner_name_and_negative_native_guards_remain_checked_with_matching_payloads():
    from reg_meta_build.curation_tree import DocumentaryEndpointGuard

    setup = _setup()
    tree, declaration, _table, _records, _names = setup
    entry = tree.registers[0].documentary.binding[0]
    tree.registers[0].documentary.binding = [
        entry.model_copy(update={"owner": "scb/example/operand"})
    ]
    relations, issues = _compile(setup)
    assert not relations and "no longer has owner" in issues[0].detail
    changed = declaration.model_copy(update={"member_name": value_field("other")})
    tree.registers[0].documentary.binding = [
        entry.model_copy(
            update={"payload_sha256": canonical_sha256(changed.model_dump(mode="json"))}
        )
    ]
    relations, issues = _compile(setup, declarations=(changed,))
    assert not relations and "supplied owner name" in issues[0].detail
    tree.registers[0].documentary.binding = [
        entry.model_copy(
            update={
                "negative_native_guards": [
                    DocumentaryEndpointGuard(
                        native="2", originals_sha256=canonical_sha256([])
                    )
                ]
            }
        )
    ]
    relations, issues = _compile(setup)
    assert not relations and "negative native endpoint" in issues[0].detail
    tree.registers[0].documentary.binding = [entry, entry]
    relations, issues = _compile(setup)
    assert not relations and len(issues) == 2


def _retained_setup():
    from reg_meta_build.curation_tree import DocumentaryRetainedEntry

    tree, declaration, table, records, names = _setup("code_crosswalk")
    declaration = declaration.model_copy(update={"member_name": None})
    tree.registers[0].register_info = SimpleNamespace(provider="scb", slug="example")
    tree.registers[0].documentary.binding = []
    tree.registers[0].documentary.retained = [
        DocumentaryRetainedEntry(
            source="fixture",
            table="literal",
            row="row:2",
            payload_sha256=canonical_sha256(declaration.model_dump(mode="json")),
            table_sha256=canonical_sha256(table.model_dump(mode="json")),
            reason="Neither code namespace has an established variable endpoint.",
            evidence="Complete literal table reviewed.",
            noted="2026-10-02",
        )
    ]
    return tree, declaration, table, records, names


@pytest.mark.parametrize(
    "change", ["payload", "missing", "duplicate", "removed_peer", "duplicate_peer"]
)
def test_retained_declaration_requires_complete_exact_physical_evidence(change):
    setup = _retained_setup()
    _, declaration, table, _, _ = setup
    kwargs = {}
    if change == "payload":
        kwargs["declarations"] = (
            declaration.model_copy(update={"description": value_field("changed")}),
        )
    elif change == "missing":
        kwargs["declarations"] = ()
    elif change == "duplicate":
        kwargs["declarations"] = (declaration, declaration)
    elif change == "removed_peer":
        kwargs["tables"] = (table.model_copy(update={"rows": ()}),)
    else:
        kwargs["tables"] = (
            table.model_copy(update={"rows": (*table.rows, table.rows[0])}),
        )
    relations, issues = _compile(setup, **kwargs)
    assert not relations
    assert [issue.code for issue in issues] == ["stale_curation_entry"]


@pytest.mark.parametrize(
    "change", ["wrong_register", "wrong_source", "duplicate_entry"]
)
def test_retained_literal_requires_one_admitted_source_register_disposition(change):
    setup = _retained_setup()
    tree, _, _, _, names = setup
    if change == "wrong_register":
        tree.registers[0].register_info.slug = "other"
    elif change == "wrong_source":
        names = {
            fqid: {("other", key) for _, key in families}
            for fqid, families in names.items()
        }
    else:
        entry = tree.registers[0].documentary.retained[0]
        tree.registers[0].documentary.retained = [entry, entry]
    relations, issues = _compile(setup, names=names)
    assert not relations
    assert issues and all(issue.code == "stale_curation_entry" for issue in issues)


def test_retained_literal_writer_preserves_raw_evidence_without_endpoints(tmp_path):
    from reg_meta_build.resolved_metadata import RetainedDocumentaryRelationship

    setup = _retained_setup()
    relations, issues = _compile(setup)
    assert not issues
    assert len(relations) == 1 and isinstance(
        relations[0], RetainedDocumentaryRelationship
    )
    relation = relations[0]
    assert relation.declaration == setup[1]
    with pytest.raises(ValidationError):
        RetainedDocumentaryRelationship.model_validate_json(
            relation.model_dump_json()[:-1] + ',"owner":"scb/example/owner"}'
        )
    with pytest.raises(ValidationError):
        DocumentaryRelationship(
            relationship_id=1,
            owner="scb/example/owner",
            declaration=setup[1],
            provenance="Must not attach an owner to a declaration without a member.",
        )
    from reg_meta_build.catalog_dependencies import resolve_metadata_dependencies

    register = ResolvedRegister(provider="scb", slug="example", name="Example")
    resolved = resolve_metadata_dependencies(
        ResolvedMetadata(documentary_relationships=relations),
        (),
        registers=(register,),
        variants=(),
        classifications=(),
        withheld={},
    )
    assert not resolved.diagnostics
    assert resolved.metadata.documentary_relationships == relations
    out = tmp_path / "retained.db"
    write_resolved_catalog(
        (),
        out,
        manifest=synthetic_manifest(),
        diagnostic=True,
        parent_registers=(register,),
        metadata=ResolvedMetadata(documentary_relationships=relations),
    )
    with closing(open_built_db(out)) as conn:
        row = conn.execute("SELECT * FROM source_relationship").fetchone()
        assert row["owner_variable_id"] is None
        assert row["binding_status"] == "retained_unattached"
        assert (
            SourceCodeCrosswalkDeclaration.model_validate_json(row["declaration_json"])
            == setup[1]
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM source_relationship_variable"
            ).fetchone()[0]
            == 0
        )
        assert conn.execute("pragma integrity_check").fetchone()[0] == "ok"
        assert not conn.execute("pragma foreign_key_check").fetchall()
    import sqlite3

    with sqlite3.connect(out) as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE source_relationship SET binding_status='owner_bound_literal'"
        )
