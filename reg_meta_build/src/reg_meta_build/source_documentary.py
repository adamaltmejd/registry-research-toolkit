"""Compile checked owner-bound literal declarations without interpreting source prose."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import TYPE_CHECKING

from reg_meta.documentary import (
    DocumentaryRelationship,
    DocumentaryVariableReference,
    SourceCodeCrosswalkDeclaration,
    SourceDerivationDeclaration,
)
from reg_meta.source_evidence import canonical_sha256

from reg_meta_build.id import mint
from reg_meta_build.prepared_catalog import ReferenceEvidence
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef

if TYPE_CHECKING:
    from reg_meta_build.curation_tree import CurationTree
    from reg_meta_build.pipeline import CompiledScope
    from reg_meta_build.prepared_catalog import PreparedCatalogSources
    from reg_meta_build.source_records import SourceRecord


def compile_documentary_bindings(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    variable_families: dict[str, set[tuple[str, tuple[str | int, ...]]]],
) -> tuple[tuple[DocumentaryRelationship, ...], tuple[ResolutionDiagnostic, ...]]:
    entries = [
        (register, index, entry)
        for register in tree.registers
        for index, entry in enumerate(register.documentary.binding, 1)
    ]
    if not entries:
        return (), ()
    selected_sources = {s.source for s in scopes}
    declarations = defaultdict(list)
    for evidence in prepared.iter_evidence():
        if isinstance(evidence, ReferenceEvidence) and isinstance(
            evidence.declaration,
            SourceCodeCrosswalkDeclaration | SourceDerivationDeclaration,
        ):
            d = evidence.declaration
            declarations[
                d.revision.dataset, d.locator.physical_table, d.locator.physical_record
            ].append(d)
    wanted_tables = {(e.source, e.table) for _, _, e in entries}
    tables = defaultdict(list)
    for source in {s for s, _ in wanted_tables} & selected_sources:
        for table in prepared.records.iter_tables(source=source):
            if (source, table.name) in wanted_tables:
                tables[source, table.name].append(table)
    needed = defaultdict(set)
    for _, _, e in entries:
        needed[e.source].update(
            (
                e.member,
                *(a.token for a in e.anchors),
                *(g.native for g in e.negative_native_guards),
            )
        )
    families: dict[
        tuple[str, str], list[tuple[tuple[str | int, ...], tuple[SourceRecord, ...]]]
    ] = defaultdict(list)
    for source, natives in needed.items():
        if source in selected_sources:
            for key, records in prepared.records.iter_native_families(
                source, select_family=lambda k, natives=natives: str(k[-1]) in natives
            ):
                families[source, str(key[-1])].append((key, records))
    result, issues = [], []
    counts = Counter((e.source, e.table, e.row) for _, _, e in entries)
    for register, index, entry in entries:
        case_id = f"{register.source_file}#/documentary/binding/{index}"
        if entry.source not in selected_sources:
            # Existing pipeline evidence disposition reports the out-of-slice deferral.
            continue
        matches = declarations[entry.source, entry.table, entry.row]
        table_matches = tables[entry.source, entry.table]
        errors = []
        if (
            len(matches) != 1
            or canonical_sha256(matches[0].model_dump(mode="json"))
            != entry.payload_sha256
        ):
            errors.append("exact declaration payload is missing, ambiguous or changed")
        if (
            len(table_matches) != 1
            or canonical_sha256(table_matches[0].model_dump(mode="json"))
            != entry.table_sha256
        ):
            errors.append(
                "complete physical table peers are missing, ambiguous or changed"
            )
        if len(matches) == 1:
            member = matches[0].member_name
            if (
                member is None
                or member.status != "value"
                or member.value != entry.member
            ):
                errors.append("supplied owner name disagrees with the authored member")
        endpoints = [
            (entry.member, entry.owner, entry.owner_originals_sha256),
            *((a.token, a.variable, a.originals_sha256) for a in entry.anchors),
        ]
        for native, fqid, expected in sorted(set(endpoints)):
            found = families[entry.source, native]
            if len(found) != 1:
                errors.append(
                    f"endpoint {native!r} has {len(found)} native families; expected one"
                )
                continue
            key, records = found[0]
            if (
                canonical_sha256([r.model_dump(mode="json") for r in records])
                != expected
            ):
                errors.append(f"complete endpoint originals changed for {native!r}")
            if variable_families.get(fqid) != {(entry.source, key)}:
                errors.append(
                    f"exact native endpoint {native!r} no longer has owner {fqid!r}"
                )
        for guard in entry.negative_native_guards:
            records = [
                r
                for _, members in families[entry.source, guard.native]
                for r in members
            ]
            if (
                canonical_sha256([r.model_dump(mode="json") for r in records])
                != guard.originals_sha256
            ):
                errors.append(f"negative native endpoint {guard.native!r} changed")
        coordinate = entry.source, entry.table, entry.row
        if counts[coordinate] != 1:
            errors.append(
                "multiple accepted bindings select the same literal declaration"
            )
        refs = tuple(
            SourceRecordRef(
                source=entry.source, semantic_record_key=d.locator.semantic_record_key
            )
            for d in matches
        )
        if errors:
            issues.append(
                ResolutionDiagnostic(
                    code="stale_curation_entry",
                    severity="error",
                    subject=case_id,
                    detail="; ".join(errors),
                    refs=refs,
                    withheld_output=("catalog_relationship",),
                )
            )
            continue
        declaration = matches[0]
        result.append(
            DocumentaryRelationship(
                relationship_id=mint(
                    "source_relationship",
                    entry.source,
                    entry.table,
                    entry.row,
                    entry.owner,
                ),
                owner=entry.owner,
                declaration=declaration,
                variables=tuple(
                    DocumentaryVariableReference(
                        **a.model_dump(exclude={"originals_sha256"})
                    )
                    for a in entry.anchors
                ),
                unresolved=tuple(entry.unresolved),
                provenance=f"{case_id}: {entry.evidence}",
            )
        )
    return tuple(result), tuple(issues)
