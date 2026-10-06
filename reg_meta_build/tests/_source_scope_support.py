"""Shared synthetic source records, naming and guard builders and the complete-scope resolve entry for the source-scope tests."""

from __future__ import annotations

from collections import defaultdict

from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row as _var_row
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.source_coordinates import (
    native_parent_key,
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CurationCase,
    PeerGuard,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    SourceRecord,
    value_field,
)
from reg_meta_build.source_scope import resolve_source_scope
from reg_meta_build.source_support import SourceSupportBindings
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

REVISION = SourceRevision.create(
    dataset="scope-fixture",
    publisher="SCB",
    purpose="Scope composition",
    upstream_revision="1",
    artifact_path="records.csv",
    artifact_size=1,
    artifact_sha256="a" * 64,
)


def record(
    member=1,
    variable=5,
    variant=2,
    column="VALUE",
    year="2020",
    register_id=1,
    edition_name=None,
    edition_id=None,
    data_length="1",
):
    header = REGISTERINFORMATION_HEADER.split("|")
    values = _var_row(
        cvid=member,
        var_id=variable,
        colname=column,
        register=("TEST", register_id, variant),
        versionname=edition_name,
        regver_id=int(year) if edition_id is None else edition_id,
        data_length=data_length,
        year=year,
    ).split("|")
    result = clean_scb_row(
        header,
        member,
        {
            name: (True, value, value)
            for name, value in zip(header, values, strict=True)
        },
        REVISION,
    ).record
    return result.model_copy(
        update={
            "fields": result.fields.model_copy(
                update={
                    "identifier": value_field(False),
                    "sensitivity": value_field(False),
                }
            )
        }
    )


def names(records):
    grouped = defaultdict(list)
    for item in records:
        grouped["variable", native_variable_key(item)].append(item)
        for parent in item.parent_facts:
            if parent.kind in {"register", "variant"}:
                kind = "register" if parent.kind == "register" else "register_variant"
                grouped[kind, native_parent_key(item.source, "scb", parent)].append(
                    item
                )
    result = []
    for (kind, key), members in grouped.items():
        first = members[0]
        member_key = str(key[-1])
        register_key = source_register_key(first)
        assert register_key is not None
        register_id = str(register_key[-1])
        result.append(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind=kind,
                    provider="scb",
                    source_key=key,
                    register_key=source_register_key(first)
                    if kind != "register"
                    else None,
                ),
                naming=SlugEntry(
                    kind=kind,
                    provider="scb",
                    source_id=register_id
                    if kind == "register"
                    else f"{register_id}.{member_key}",
                    slug={
                        "register": "example",
                        "register_variant": f"people-{member_key}",
                        "variable": f"value-{member_key}",
                    }[kind],
                ),
                contributors=(),
            )
        )
    return tuple(result)


def guard(item: SourceRecord):
    return PeerGuard(
        guard_id="exact-original-variable",
        source=item.source,
        coordinates=(
            ("register", item.subject.register_name),
            ("variable", item.subject.variable),
        ),
        expected_members=(record_ref(item),),
    )


def resolve(
    records,
    *,
    cases=(),
    naming=None,
    naming_ambiguities=(),
    provider_keys=None,
    derive_native_provider_keys=False,
    on_diagnostic=None,
    value_sessions=(),
    diagnostic=False,
    classifications=None,
    label_rules=None,
    classification_overrides=None,
    source_diagnostics=(),
    coding_registers=(),
    coding_scope=None,
):
    support = SourceSupportBindings((), ())
    for item in records:
        support.observe(item)
    support.seal()
    return resolve_source_scope(
        records,
        cases=cases,
        naming=names(records) if naming is None else naming,
        naming_ambiguities=naming_ambiguities,
        provider_keys={
            key: str(r.subject.variable.native_id)
            for r in records
            if (key := native_variable_key(r)) is not None
        }
        if provider_keys is None
        else provider_keys,
        derive_native_provider_keys=derive_native_provider_keys,
        value_sessions=value_sessions,
        support=support,
        source_diagnostics=source_diagnostics,
        classifications=classifications or {},
        classification_references={},
        label_rules=label_rules or {},
        classification_overrides=classification_overrides or {},
        on_diagnostic=on_diagnostic,
        diagnostic=diagnostic,
        coding_registers=coding_registers,
        coding_scope=coding_scope,
    )


def acknowledge(issue, item):
    register_key = source_register_key(item)
    assert register_key is not None
    return CurationCase(
        case_id="acknowledged",
        targets=(),
        decision=AcknowledgeDecision(
            code=issue.code,
            subject=issue.subject,
            refs=issue.refs,
            fields=issue.fields,
            valid_from=issue.valid_from,
            valid_to=issue.valid_to,
            register_key=register_key,
            reason="Accepted while the source stays unresolved.",
            evidence="Fixture diagnostic ledger.",
        ),
    )
