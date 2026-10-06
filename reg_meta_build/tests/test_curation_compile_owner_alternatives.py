"""Column-owner alternatives, partial partitions, uncoded text roles and global event routing compile from source coordinates."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from _curation_compile_support import (
    errata_fixture as _errata_fixture,
    errata_record as _errata_record,
    make_prepared as _prepared,
    make_scope as _scope,
    make_tree as _tree,
    partition_scope as _partition_scope,
    scb_partition_records as _scb_partition_records,
    scb_partition_tree as _scb_partition_tree,
    scope_with_coding_names as _scope_with_coding_names,
)
from reg_meta_build.curation_compile import (
    compile_coding_register,
    compile_curation,
    compile_deferred_partitions,
    compile_partitions,
    convert_column_partitions,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
)
from reg_meta_build.prepared_sources import (
    PreparedPartitionRecord,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_coordinates import (
    native_variable_key,
)
from reg_meta_build.source_curation import (
    capture_expectations,
    evaluate_cases,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
)


def _column_owner_alternatives_fixture(tmp_path, *, digest_only=False):
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256
    from reg_meta_build.source_records import CodeSetReference, SourceFields

    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n',
    )
    rows = tuple(
        _errata_record(column="ANSWER", year="2020", member=member).model_copy(
            update={
                "fields": _errata_record(
                    column="ANSWER", year="2020", member=member
                ).fields.model_copy(
                    update={"operational_definition": value_field(text)}
                ),
                "code_set_references": (
                    CodeSetReference(
                        reference_id="source-list",
                        content_sha256="a" * 64,
                        physical_locator="source-list.csv",
                    ),
                ),
            }
        )
        for member, text in ((20, "Derived amount"), (21, "Sum of components"))
    )
    owner = IdentityColumnOwnerEntry(
        variable="1.5",
        variant="1.2",
        column="ANSWER",
        owner="1.5.amount",
        ref="Same physical amount, retain both source operations",
        source_editions=["2020"],
        expected_records=None
        if digest_only
        else list(
            capture_expectations(
                rows, fields=tuple(SourceFields.model_fields), parents=True, coding=True
            )
        ),
        expected_evidence_sha256=acknowledgement_evidence_sha256(rows),
    )
    tree = load_curation_tree(root)
    register = next(r for r in tree.registers if r.register_info.native_id == "1")
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={
                    "identity": register.identity.model_copy(
                        update={"column_owner": [owner]}
                    )
                }
            ),
        ),
    )
    scope = _partition_scope(rows)

    def compile_rows(records, *, projected=False, deferred=False):
        partition_records = (
            tuple(
                PreparedPartitionRecord(
                    source=r.source,
                    subject=r.subject,
                    parent_facts=r.parent_facts,
                    edition_scope=r.edition_scope,
                    edition_period_scope=r.edition_period_scope,
                    locators=r.locators,
                    fields=r.fields.model_copy(update={"operational_definition": None}),
                )
                for r in records
            )
            if projected
            else records
        )
        reader = SimpleNamespace(
            iter_partition_families=lambda *args, **kwargs: iter(
                ((native_variable_key(rows[0]), partition_records),)
            ),
            iter_native_families=lambda *args, **kwargs: iter(
                ((native_variable_key(rows[0]), records),)
            ),
        )
        compiler = compile_deferred_partitions if deferred else compile_partitions
        return compiler(tree, SimpleNamespace(records=reader), (scope,))

    compiled = compile_rows(rows)
    assert not compiled[-1]
    return rows, owner, compiled[0][scope.source, None], compile_rows


@pytest.mark.parametrize(
    "change", ["operation", "remove", "add", "duplicate", "parent", "scope", "coding"]
)
@pytest.mark.parametrize("digest_only", [False, True])
def test_column_owner_alternatives_refuse_original_drift(tmp_path, change, digest_only):
    rows, _, cases, compile_rows = _column_owner_alternatives_fixture(
        tmp_path, digest_only=digest_only
    )
    first = rows[0]
    if change == "operation":
        changed = (
            first.model_copy(
                update={
                    "fields": first.fields.model_copy(
                        update={
                            "operational_definition": value_field("Changed formula")
                        }
                    )
                }
            ),
            rows[1],
        )
    elif change == "remove":
        changed = rows[1:]
    elif change == "add":
        changed = (*rows, _errata_record(column="ANSWER", year="2020", member=22))
    elif change == "duplicate":
        changed = (
            *rows,
            first.model_copy(update={"record_id": first.record_id + "-copy"}),
        )
    elif change == "parent":
        parent = first.parent_facts[0]
        changed = (
            first.model_copy(
                update={
                    "parent_facts": (
                        parent.model_copy(
                            update={
                                "fields": parent.fields.model_copy(
                                    update={"name": value_field("Changed parent name")}
                                )
                            }
                        ),
                        *first.parent_facts[1:],
                    )
                }
            ),
            rows[1],
        )
    elif change == "scope":
        changed = (
            first.model_copy(
                update={
                    "edition_scope": TemporalScope(
                        kind="intervals",
                        intervals=(ScopeInterval(start="2019", end="2019"),),
                    )
                }
            ),
            rows[1],
        )
    else:
        changed = (
            first.model_copy(
                update={
                    "code_set_references": (
                        first.code_set_references[0].model_copy(
                            update={"content_sha256": "b" * 64}
                        ),
                    )
                }
            ),
            rows[1],
        )
    refreshed = compile_rows(changed)
    assert any(d.code == "stale_curation_entry" for d in refreshed[-1])
    assert compile_rows(changed, projected=True) == refreshed
    assert not any(compile_rows(changed, projected=True, deferred=True)[0].values())
    assert apply_occurrence_cases(changed, cases).diagnostics


@pytest.mark.parametrize(
    "update",
    [
        {"source_editions": []},
        {"expected_evidence_sha256": "invalid"},
        {
            "expected_fields": [
                {"name": "column_name", "status": "value", "value": "ANSWER"}
            ]
        },
    ],
)
def test_digest_column_owner_requires_finite_exclusive_guards(tmp_path, update):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    _, owner, _, _ = _column_owner_alternatives_fixture(tmp_path, digest_only=True)
    with pytest.raises(ValidationError):
        IdentityColumnOwnerEntry.model_validate(owner.model_dump(mode="json") | update)


def test_column_owner_alternatives_require_complete_exclusive_guards(tmp_path):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    _, owner, _, _ = _column_owner_alternatives_fixture(tmp_path)
    supplied = owner.model_dump(mode="json")
    for update in (
        {"expected_evidence_sha256": None},
        {"source_editions": []},
        {
            "expected_fields": [
                {"name": "column_name", "status": "value", "value": "ANSWER"}
            ]
        },
        {
            "expected_records": [
                owner.expected_records[0].model_dump(mode="json")
                | {"alternatives": [{"fields": []}]}
            ]
        },
    ):
        with pytest.raises(ValidationError):
            IdentityColumnOwnerEntry.model_validate(supplied | update)


def test_partial_partition_finding_excludes_exact_scoped_owned_originals():
    records = _scb_partition_records(("ID", "ID", "ID"), variants=(2, 3, 2))
    supported = "1.5.supported"
    converted = convert_column_partitions(
        records,
        source_id="1.5",
        split_ids=(supported,),
        declared_columns={"ID": None},
        declaration_reference="Retain the contradictory delivery only as unresolved.",
        scoped_owners={(record_ref(records[0]), "ID"): supported},
    )
    assert len(converted.diagnostics) == 1
    assert converted.diagnostics[0].code == "unassigned_original_columns"
    assert converted.diagnostics[0].refs == tuple(
        sorted((record_ref(records[1]), record_ref(records[2])), key=str)
    )
    assert converted.case is not None
    applied = apply_occurrence_cases(records, (converted.case,))
    native = native_variable_key(records[0])
    assert applied.occurrences[0].variable_key != native
    assert [o.variable_key for o in applied.occurrences[1:]] == [native, native]
    assert tuple(o.source_records[0] for o in applied.occurrences) == records
    assert evaluate_cases((converted.case,), records[:-1])[0].status == "stale"


@pytest.mark.parametrize("stored_role", ["label", "free_text"])
def test_guarded_uncoded_text_role_preserves_attached_book_and_refuses_drift(
    tmp_path, stored_role
):
    from reg_meta_build.curation_tree import CodingUncodedEntry
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    original = _errata_record(column="ANSWER", year="2020", member=20)
    originals = (original,)
    tree, _, scope = _errata_fixture(tmp_path, originals, "")
    scope = _scope_with_coding_names(scope, original)
    occurrence = source_occurrence(original)
    column = occurrence.column_key
    assert column is not None
    claims = (
        CodeListClaim(
            "attached-book",
            original.edition_scope,
            (
                CodeMembershipClaim(
                    "1", "Description", TemporalScope(kind="year_independent")
                ),
            ),
            version_label="Reference classification",
        ),
    )
    register = next(r for r in tree.registers if r.register_info.slug == "sample")
    entry = CodingUncodedEntry(
        variable="1.5",
        variant="people",
        column="ANSWER",
        periods=[["2020-01-01", "2020-12-31"]],
        stored_role=stored_role,
        expected_evidence_sha256=acknowledgement_evidence_sha256(
            originals, tuple(coding_source_sha256(q) for q in claims)
        ),
        reason="The reviewed source documents a stored text component.",
        source="Exact original source and reference-book review",
        data_warning="The attached numeric book is reference evidence, not the text response domain.",
    )
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"uncoded": [entry]})}
    )

    def compile_rows(rows, source_claims):
        return compile_coding_register(
            register,
            scope,
            originals=rows,
            columns={column: rows},
            column_scopes={column: frozenset((original.edition_scope,))},
            coding={column: source_claims},
        )

    cases, issues = compile_rows(originals, claims)
    assert not issues and len(cases) == 1
    applied = apply_coding_choices(originals, cases, coding={column: claims})
    assert not applied.diagnostics
    assert applied.coding[column].claims == claims
    assert all(segment.code_set is None for segment in applied.coding[column].segments)
    for rows, source_claims in (
        (
            (
                original.model_copy(
                    update={
                        "fields": original.fields.model_copy(
                            update={
                                "description": value_field("Changed source meaning")
                            }
                        )
                    }
                ),
            ),
            claims,
        ),
        ((), claims),
        (
            originals,
            (
                replace(
                    claims[0],
                    members=(
                        CodeMembershipClaim(
                            "1", "Changed label", TemporalScope(kind="year_independent")
                        ),
                    ),
                ),
            ),
        ),
    ):
        assert compile_rows(rows, source_claims)[0] == ()
        assert apply_coding_choices(
            rows, cases, coding={column: source_claims}
        ).diagnostics
    with pytest.raises(ValueError, match="complete source coding guards"):
        CodingUncodedEntry.model_validate(
            {**entry.model_dump(), "expected_evidence_sha256": None}
        )
    ordinary = CodingUncodedEntry.model_validate(
        {**entry.model_dump(), "stored_role": None, "expected_evidence_sha256": None}
    )
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"uncoded": [ordinary]})}
    )
    assert compile_rows(originals, claims)[0] == ()


@pytest.mark.parametrize("guarded", [True, False])
@pytest.mark.parametrize(
    "code",
    ["unresolved_source_event_endpoint", "unresolved_lineage_ambiguous_source_variant"],
)
def test_global_event_acknowledgement_is_routed_out_of_local_scope(
    tmp_path, guarded, code
):
    root = tmp_path / "curation"
    _tree(root)
    source_ref = json.dumps(
        {"source": "scb-timeseries", "semantic_record_key": ["event", "exact"]}
    )
    fragment = (
        f'\n[[acknowledge]]\ncode = "{code}"\n'
        'subject = "exact"\nreason = "Source endpoint is absent"\n'
        'evidence = "Complete accepted evidence reviewed"\n'
        f"refs = [{json.dumps(source_ref)}]\n"
    )
    if guarded:
        fragment += (
            f'expected_evidence_sha256 = "{"0" * 64}"\n'
            f'expected_diagnostic_sha256 = "{"1" * 64}"\n'
        )
    with (root / "registers/scb/sample.toml").open("a") as handle:
        handle.write(fragment)
    tree = load_curation_tree(root)
    if not guarded:
        with pytest.raises(ValueError, match="full evidence and diagnostic guards"):
            compile_curation(tree, _prepared(), (_scope(),), subset=True)
        return
    compiled = compile_curation(tree, _prepared(), (_scope(),), subset=True)
    assert (
        len(
            compiled.event_acknowledgements
            if code == "unresolved_source_event_endpoint"
            else compiled.lineage_acknowledgements
        )
        == 1
    )
    assert not any(
        case.decision.kind == "acknowledge"
        for cases in compiled.cases.values()
        for case in cases
    )
