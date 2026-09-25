"""Compile tracked curation against prepared sources and build the catalog."""

from __future__ import annotations

import gzip
import json
import time
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from reg_meta_build.catalog_dependencies import (
    DEFERRED_REFERENCE,
    CoverageObligation,
    check_delivery_coverage,
    resolve_classification_successions,
    resolve_metadata_dependencies,
    resolve_month_groups,
    resolve_panel_dependencies,
    resolve_variable_edge_groups,
    resolve_variable_successions,
)
from reg_meta_build.catalog_lineage import resolve_catalog_lineage
from reg_meta_build.concept_groups import CodeLabelPair  # noqa: TC001
from reg_meta_build.curation_compile import (
    CompiledCodebook,
    compile_curation,
    compile_native_naming,
    compile_partitions,
    finalize_classification_bindings,
    source_event_id,
    tree_sha256,
    validate_sentinels,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.db import _emit_timing, _paths_overlap
from reg_meta_build.input_snapshot import _git, input_bundle_repository
from reg_meta_build.prepared_catalog import (
    ReferenceEvidence,
    open_prepared_catalog_sources,
)
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata
from reg_meta_build.source_classifications import resolve_canonical_codes
from reg_meta_build.source_coordinates import NativeKey  # noqa: TC001
from reg_meta_build.source_curation import (
    CurationCase,
    ResolutionDiagnostic,
    SourceRecordRef,
)
from reg_meta_build.source_event_resolution import SourceEventBindings
from reg_meta_build.source_naming import (  # noqa: TC001
    NamingAmbiguity,
    NamingDeclaration,
)
from reg_meta_build.source_reference_records import (
    SourceColumnTypeDeclaration,
    SourceEventDeclaration,
    SourceJoinKeyDeclaration,
)
from reg_meta_build.source_reference_resolution import (
    resolve_export_metadata,
    resolve_identifier_metadata,
)
from reg_meta_build.source_scope import (
    declared_dependency_keys,
    declared_register_fqids,
    resolve_source_scope,
)
from reg_meta_build.source_support import SourceSupportBindings
from reg_meta_build.source_value_bindings import open_value_bindings
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


class CompletedArtifactError(Exception):
    """Report finalization failed after the catalog reached its destination."""

    def __init__(
        self, cause: Exception, result: dict[str, object], report_dir: Path
    ) -> None:
        super().__init__(str(cause))
        self.result = dict(result)
        self.report_dir = report_dir


@contextmanager
def _retain_completed_artifact(
    result: dict[str, object], report_dir: Path
) -> Iterator[None]:
    try:
        yield
    except Exception as exc:
        if result.get("database"):
            raise CompletedArtifactError(exc, result, report_dir) from exc
        raise


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CompiledScope(_Model):
    """In-memory decisions for one complete prepared source scope."""

    source: str
    register_key: NativeKey | None
    cases: tuple[CurationCase, ...] = ()
    naming: tuple[NamingDeclaration, ...] = ()
    naming_ambiguities: tuple[NamingAmbiguity, ...] = ()
    provider_keys: tuple[tuple[NativeKey, str | None], ...] = ()
    variants: tuple[tuple[NativeKey, ResolvedVariant], ...] = ()


class CompiledGlobals(_Model):
    """Strict contract for the declarations compiled from tracked curation."""

    classifications: tuple[CompiledCodebook, ...] = ()
    classification_successions: tuple[ResolvedClassificationSuccession, ...] = ()
    metadata: ResolvedMetadata = ResolvedMetadata()
    code_label_pairs: tuple[CodeLabelPair, ...] = ()
    identifier_sources: tuple[str, ...] = ()
    event_sources: tuple[tuple[str, str], ...] = ()
    lineage_defaults: tuple[tuple[str, str], ...] = ()


def _unique_pairs[K, V](items: tuple[tuple[K, V], ...], description: str) -> dict[K, V]:
    result = dict(items)
    if len(result) != len(items):
        raise ValueError(f"duplicate {description}")
    return result


def _selected_scopes(
    keys: Iterable[tuple[str, NativeKey | None]], specs: tuple[str, ...]
) -> set[tuple[str, NativeKey | None]]:
    """The scopes `--registers` names. A register scope answers to its native
    register coordinate (the SCB register id) and to SOURCE:ID, a whole-source
    scope to its source; a name matching scopes in several sources is refused."""
    names = defaultdict(set)
    for source, register in keys:
        if register is None:
            names[source].add((source, register))
        else:
            names[str(register[-1])].add((source, register))
            names[f"{source}:{register[-1]}"].add((source, register))
    if unknown := set(specs) - names.keys():
        raise ValueError(f"--registers names no selected scope: {sorted(unknown)}")
    if ambiguous := sorted(
        spec for spec in set(specs) if len({source for source, _ in names[spec]}) > 1
    ):
        raise ValueError(
            f"--registers names scopes in several sources, qualify as SOURCE:ID: {ambiguous}"
        )
    return {key for spec in specs for key in names[spec]}


def build_catalog(
    prepared_path: Path,
    input_commit: str,
    input_manifest_sha256: str,
    output: Path,
    report_dir: Path,
    *,
    diagnostic: bool = False,
    registers: tuple[str, ...] = (),
    curation_dir: Path | None = None,
    dump_decisions: Path | None = None,
) -> dict[str, object]:
    """Build the compiled curation; failures never replace an active catalog.

    A diagnostic completion retains strict errors and returns publication_ready
    false. Malformed prepared inputs or declarations raise normally.
    The single event stream accounts for source records, cases and diagnostics;
    it references prepared evidence instead of copying every source projection.
    `registers` forms only the named scopes, with shared inputs still complete,
    skips curation touching none of them and defers references into registers
    that exist but were not selected. Its output is never publishable and its
    corpus volume guards do not apply.
    """
    if len(input_commit) != 40 or any(
        ch not in "0123456789abcdef" for ch in input_commit
    ):
        raise ValueError("--input-commit must be a lowercase 40-character SHA")
    if len(input_manifest_sha256) != 64 or any(
        ch not in "0123456789abcdef" for ch in input_manifest_sha256
    ):
        raise ValueError("--input-manifest-sha256 must be a lowercase SHA-256")
    started = time.perf_counter()
    publishable = not diagnostic and not registers
    prepared_path = prepared_path.resolve()
    output, report_dir = output.resolve(), report_dir.resolve()
    if curation_dir is None:
        from reg_meta_build._curation import repo_curation_dir

        curation_dir = repo_curation_dir()
        if curation_dir is None:
            raise ValueError("--curation-dir is required outside a checkout")
    curation_dir = curation_dir.resolve()
    if not curation_dir.is_dir():
        raise ValueError(f"curation directory does not exist: {curation_dir}")
    dump_decisions = dump_decisions.resolve() if dump_decisions is not None else None
    output_paths = {output, Path(str(output) + ".prev")}
    protected_dirs = (prepared_path, curation_dir)
    if dump_decisions is not None and (
        dump_decisions.exists()
        or any(
            dump_decisions.is_relative_to(path) or path.is_relative_to(dump_decisions)
            for path in (*protected_dirs, report_dir, output, *output_paths)
        )
    ):
        raise ValueError(
            "--dump-decisions must be a new directory separate from inputs and outputs"
        )
    if (
        any(path.is_relative_to(curation_dir) for path in output_paths)
        or report_dir.is_relative_to(curation_dir)
        or any(path.is_relative_to(prepared_path) for path in output_paths)
        or report_dir.is_relative_to(prepared_path)
        or output.is_relative_to(report_dir)
        or (not publishable and output.exists())
        or (output.exists() and not output.is_file())
    ):
        raise ValueError(
            "build outputs must be separate from prepared inputs and each other"
        )
    prepared = open_prepared_catalog_sources(
        prepared_path,
        input_commit=input_commit,
        expected_sha256=input_manifest_sha256,
    )
    scope_keys = set()
    for entry in prepared.manifest.inputs:
        if entry.record_usage != "occurrence" or entry.revision is None:
            continue
        source = entry.revision.dataset
        if entry.role == "scb_records":
            scope_keys.update(
                (source, register)
                for register, _, _ in prepared.records.register_coordinates(source)
            )
        else:
            scope_keys.add((source, None))
    whole_sources = {source for source, register in scope_keys if register is None}
    visit = _selected_scopes(scope_keys, registers) if registers else set(scope_keys)
    occurrence_sources = {
        entry.revision.dataset
        for entry in prepared.manifest.inputs
        if entry.record_usage == "occurrence" and entry.revision is not None
    }
    if registers and not {source for source, _ in scope_keys} <= occurrence_sources:
        raise ValueError(
            "curation scopes name a source outside the prepared occurrence-source selection"
        )
    tree = load_curation_tree(curation_dir)
    label_rules = {
        label: entry.classification.slug
        for entry in tree.classifications
        for label in entry.binding.value_set_labels
    }
    classification_overrides = {
        bound.variable: (
            entry.classification.slug,
            f"classifications/{entry.classification.short_name}.toml#/binding/variable/{index}",
        )
        for entry in tree.classifications
        for index, bound in enumerate(entry.binding.variable, 1)
    }
    scope_seeds = tuple(
        CompiledScope(source=source, register_key=register)
        for source, register in sorted(visit, key=repr)
    )
    compiled = compile_curation(
        tree,
        prepared,
        scope_seeds,
        subset=bool(registers),
    )
    if "_classifications" in compiled.report:
        valid_overrides = set(compiled.report["_classifications"]["entries_matched"])
        classification_overrides = {
            fqid: binding
            for fqid, binding in classification_overrides.items()
            if binding[1] in valid_overrides
        }
    selected = CompiledGlobals.model_validate(compiled.fields, strict=True)
    scopes = {
        key: CompiledScope.model_validate_json(
            json.dumps(
                {
                    "source": key[0],
                    "register_key": key[1],
                    "cases": [
                        item.model_dump(mode="json")
                        for item in compiled.cases.get(key, ())
                    ],
                    "naming": [
                        item.model_dump(mode="json")
                        for item in (compiled.naming or {}).get(key, ())
                    ],
                    "naming_ambiguities": [
                        item.model_dump(mode="json")
                        for item in (compiled.naming_ambiguities or {}).get(key, ())
                    ],
                    "provider_keys": (compiled.provider_keys or {}).get(key, ()),
                    "variants": [
                        (native_key, variant.model_dump(mode="json"))
                        for native_key, variant in (compiled.variants or {}).get(
                            key, ()
                        )
                    ],
                }
            )
        )
        for key in visit
    }
    unselected_scopes: dict[tuple[str, NativeKey | None], CompiledScope] = {}
    if registers:
        outside = tuple(
            CompiledScope(source=source, register_key=register)
            for source, register in sorted(scope_keys - visit, key=repr)
        )
        outside_naming, _, _, _, _ = compile_native_naming(
            tree, prepared, outside, subset=True
        )
        outside_with_names = tuple(
            scope.model_copy(
                update={
                    "naming": outside_naming.get((scope.source, scope.register_key), ())
                }
            )
            for scope in outside
        )
        _, split_names, _, ambiguities, split_bases, _ = compile_partitions(
            tree, prepared, outside_with_names
        )
        for scope in outside_with_names:
            key = scope.source, scope.register_key
            names = tuple(
                name
                for name in scope.naming
                if name.target.source_key not in split_bases.get(key, set())
            ) + split_names.get(key, ())
            unselected_scopes[key] = CompiledScope.model_validate_json(
                scope.model_copy(
                    update={
                        "naming": names,
                        "naming_ambiguities": ambiguities.get(key, ()),
                    }
                ).model_dump_json()
            )
    coding_registers = {
        f"{register.register_info.provider}/{register.register_info.slug}": register
        for register in tree.registers
        if (
            register.coding.choice
            or register.coding.uncoded
            or register.coding.omit
            or register.coding.extend
        )
    }
    coding_ids = {}
    for name, register in coding_registers.items():
        ids = tuple(
            f"{register.source_file}#/coding.{kind}/{index}/period/{period_index}"
            for kind, entries in (
                ("choice", register.coding.choice),
                ("uncoded", register.coding.uncoded),
                ("omit", register.coding.omit),
                ("extend", register.coding.extend),
            )
            for index, entry in enumerate(entries, 1)
            for period_index, _ in enumerate(entry.periods, 1)
        )
        coding_ids[name] = ids
        compiled.report.setdefault(
            name,
            {
                key: []
                for key in (
                    "entries_read",
                    "entries_matched",
                    "stale",
                    "over_broad",
                    "not_evaluated_in_subset",
                )
            },
        )["entries_read"].extend(ids)
    matched_labels: set[str] = set()
    duplicate_overrides: set[str] = set()
    curation_hash = tree_sha256(curation_dir)
    if dump_decisions is not None:
        dump_decisions.mkdir()
        (dump_decisions / "global.json").write_text(
            json.dumps(
                compiled.fields,
                default=lambda item: (
                    item.model_dump(mode="json")
                    if hasattr(item, "model_dump")
                    else item.__dict__
                ),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        (dump_decisions / "compile-report.json").write_text(
            json.dumps(
                compiled.report,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
    # The accepted preparation's immutable commit dates the catalog vintage;
    # rebuild wall time would make identical selected inputs produce new bytes.
    import_date = (
        datetime.fromtimestamp(
            int(
                _git(
                    input_bundle_repository(prepared_path),
                    "show",
                    "-s",
                    "--format=%ct",
                    input_commit,
                )
            ),
            UTC,
        )
        .isoformat()
        .replace("+00:00", "Z")
    )
    if _paths_overlap(
        output_paths,
        {prepared_path / "manifest.json"}
        | {prepared_path / item.path for item in prepared.manifest.files},
    ) or any(path.exists() and not path.is_file() for path in output_paths):
        raise ValueError("catalog and backup paths must not alias selected inputs")
    _emit_timing("pipeline: open selected inputs", started)
    phase_started = time.perf_counter()
    entries = tuple(
        e for e in prepared.manifest.inputs if e.record_usage == "occurrence"
    )
    sources = {e.revision.dataset for e in entries if e.revision is not None}
    if registers:
        if not {s for s, _ in scope_keys} <= sources:
            raise ValueError(
                "curation scopes name a source outside the prepared occurrence-source selection"
            )
    elif {s for s, _ in scope_keys} != sources:
        raise ValueError(
            "curation scopes do not cover the complete prepared occurrence-source selection"
        )
    support_sources = {
        e.revision.dataset
        for e in prepared.manifest.inputs
        if e.record_usage == "field_support" and e.revision is not None
    }
    if (
        len(set(selected.identifier_sources)) != len(selected.identifier_sources)
        or not set(selected.identifier_sources) <= support_sources
    ):
        raise ValueError(
            "identifier metadata must select distinct prepared support sources"
        )
    event_sources = _unique_pairs(selected.event_sources, "event source")
    prepared_sources = {
        e.revision.dataset for e in prepared.manifest.inputs if e.revision is not None
    }
    if (
        not event_sources.keys() <= prepared_sources
        or not set(event_sources.values()) <= sources
    ):
        raise ValueError(
            "event bindings must name selected reference and occurrence sources"
        )
    if any(
        getattr(selected.metadata, name)
        for name in (
            "identifiers",
            "source_columns",
            "source_join_keys",
            "timeseries_events",
        )
    ):
        raise ValueError(
            "literal reference metadata must be resolved from prepared sources"
        )
    report_dir.mkdir(parents=True, exist_ok=False)
    counts: Counter[str] = Counter()
    acknowledged: Counter[str] = Counter()
    seen_scopes, seen_cases, seen_coding = set(), set(), set()
    coverage: list[CoverageObligation] = []
    parents, variant_registers, variables, withheld, evidence = {}, {}, {}, {}, {}
    books = {}
    sibling_pairs, slice_keys = set(), set()
    build_result: dict[str, object] = {}
    with _retain_completed_artifact(build_result, report_dir), ExitStack() as stack:
        events = stack.enter_context(
            gzip.open(report_dir / "events.jsonl.gz", "wt", encoding="utf-8")
        )

        def event(kind: str, value: dict[str, object]) -> None:
            events.write(
                json.dumps({"kind": kind, **value}, ensure_ascii=False, sort_keys=True)
                + "\n"
            )

        def issue(value: ResolutionDiagnostic) -> None:
            counts[value.severity] += 1
            if value.code == DEFERRED_REFERENCE:
                counts["deferred_references"] += 1
            event("issue", value.model_dump(mode="json"))

        for compile_issue in compiled.diagnostics:
            issue(compile_issue)

        try:
            for entry in prepared.manifest.inputs:
                event("input", entry.model_dump(mode="json"))
            declarations = []
            for index, item in enumerate(prepared.iter_evidence()):
                disposition = "source_context"
                if isinstance(item, ReferenceEvidence):
                    declaration = item.declaration
                    if isinstance(
                        declaration,
                        SourceColumnTypeDeclaration
                        | SourceEventDeclaration
                        | SourceJoinKeyDeclaration,
                    ):
                        declarations.append(declaration)
                        disposition = "literal_metadata"
                    else:
                        disposition = "unbound_relationship"
                        issue(
                            ResolutionDiagnostic(
                                code="unbound_source_relationship",
                                severity="error",
                                subject=repr(declaration.locator.semantic_record_key),
                                detail="A literal source relationship has no accepted binding to catalog endpoints; its evidence is retained without inventing that binding.",
                                refs=(
                                    SourceRecordRef(
                                        source=declaration.revision.dataset,
                                        semantic_record_key=declaration.locator.semantic_record_key,
                                    ),
                                ),
                                withheld_output=("catalog_relationship",),
                            )
                        )
                event(
                    "prepared_evidence",
                    {
                        "index": index,
                        "revision_id": item.revision_id,
                        "evidence_kind": item.kind,
                        "disposition": disposition,
                    },
                )
                counts["prepared_evidence"] += 1
            event_bindings = SourceEventBindings(
                (
                    d
                    for d in declarations
                    if isinstance(d, SourceEventDeclaration)
                    and d.revision.dataset in event_sources
                ),
                event_sources,
            )
            exports = resolve_export_metadata(declarations)
            identifiers = resolve_identifier_metadata(
                record
                for source in selected.identifier_sources
                for record in prepared.records.iter_records(source=source)
            )
            for value in (*exports.diagnostics, *identifiers.diagnostics):
                issue(value)
            source_metadata = exports.metadata.model_copy(
                update={"identifiers": identifiers.metadata.identifiers}
            )
            value_sources = {
                v.manifest.revision.dataset: v for v in prepared.value_sources
            }
            for declaration in selected.classifications:
                values = value_sources[declaration.source]
                with values.session() as session:
                    members = {}
                    physical = 0
                    for association in session.lookup_descriptor(
                        declaration.descriptor
                    ):
                        value = session.value(association.value_key)
                        members[value.payload_key] = value
                        physical += 1
                    resolved = resolve_canonical_codes(
                        members.values(),
                        source=declaration.source,
                        subject=str(declaration.metadata.get("slug")),
                    )
                for value in resolved.diagnostics:
                    issue(value)
                if not resolved.codes:
                    raise ValueError(
                        "empty canonical codebook dependency is not yet supported"
                    )
                metadata = dict(declaration.metadata)
                book = ResolvedClassification.model_validate(
                    {
                        **metadata,
                        "codes": resolved.codes,
                        "sentinel_codes": validate_sentinels(
                            metadata.pop("sentinel_codes", None),
                            subject=str(declaration.metadata.get("slug")),
                        ),
                    }
                )
                if book.slug in books:
                    raise ValueError("duplicate classification identity")
                books[book.slug] = book
                event(
                    "codebook",
                    {
                        "source": declaration.source,
                        "descriptor": declaration.descriptor,
                        "associations": physical,
                        "classification": book.slug,
                    },
                )
            references = _unique_pairs(
                tuple((b.short_name, b.slug) for b in books.values()),
                "classification reference",
            )
            support = SourceSupportBindings(
                prepared.manifest.support_joins,
                (
                    r
                    for source in sorted(
                        {j.source for j in prepared.manifest.support_joins}
                    )
                    for r in prepared.records.iter_records(source=source)
                ),
            )
            for target in prepared.records.iter_support_targets(
                prepared.manifest.support_joins
            ):
                support.observe_target(target)
            support.seal()
            for value in support.diagnostics:
                issue(value)
            sessions = stack.enter_context(open_value_bindings(prepared.value_sources))
            value_roles = {
                entry.revision.dataset: entry.role
                for entry in prepared.manifest.inputs
                if entry.revision is not None
            }
            for session in sessions:
                manifest = session.session.source.manifest
                canonical = value_roles[session.source] == "code_list"
                if session.join is None and not canonical:
                    raise ValueError(
                        f"value source lacks an implemented join contract: {session.source}"
                    )
                event(
                    "value_source",
                    {
                        "source": session.source,
                        "disposition": "canonical_evidence"
                        if canonical
                        else "occurrence_coding",
                        "associations": manifest.association_count,
                        "validity_rows": manifest.validity_count,
                    },
                )
                counts["value_associations"] += manifest.association_count
                if canonical:
                    continue
                for problem in session.source_issues():
                    # An unbindable list has no target occurrence to visit later.
                    # Preserve its exact lookup tokens separately from field issues.
                    event("value_source_issue", asdict(problem))
                    issue(
                        ResolutionDiagnostic(
                            code=problem.code,
                            severity="error",
                            subject=session.source,
                            detail=f"Source code-list evidence cannot identify its target: {problem!r}",
                            fields=("coding",),
                            withheld_output=("unbound_value_membership",),
                        )
                    )
            _emit_timing("pipeline: reference metadata and support", phase_started)
            phase_started = time.perf_counter()
            # A scoped build reads register coordinates, never records, to count
            # its selected scopes and to recognize the registers it leaves out.
            coordinates = (
                {
                    source: prepared.records.register_coordinates(source)
                    for source in {s for s, _ in scope_keys if (s, None) not in visit}
                }
                if registers
                else {}
            )
            expected = 0
            for entry in entries:
                assert entry.revision is not None
                source = entry.revision.dataset
                keys = {register for scope, register in visit if scope == source}
                if not keys:
                    continue
                if registers and source not in whole_sources:
                    expected += sum(
                        count
                        for register, _, count in coordinates[source]
                        if register in keys
                    )
                else:
                    expected += entry.counts.records
                slices = (
                    ((None, tuple(prepared.records.iter_records(source=source))),)
                    if source in whole_sources
                    else prepared.records.iter_register_slices(
                        source, keys if registers else None
                    )
                )
                for register, originals in slices:
                    scope_started = time.perf_counter()
                    scope_key = source, register
                    scope = scopes[scope_key]
                    scope_coding = tuple(
                        coding_registers[name]
                        for name in sorted(
                            set(declared_register_fqids(scope.naming).values())
                            & coding_registers.keys()
                        )
                    )

                    def record_coding_compilation(
                        register, new_cases, new_diagnostics, scope_key=scope_key
                    ) -> None:
                        name = f"{register.register_info.provider}/{register.register_info.slug}"
                        if name in seen_coding:
                            raise ValueError(f"coding register compiled twice: {name}")
                        seen_coding.add(name)
                        statuses = compiled.report[name]
                        statuses["entries_matched"].extend(
                            case.case_id for case in new_cases
                        )
                        for diagnostic_issue in new_diagnostics:
                            statuses[
                                "over_broad"
                                if diagnostic_issue.code == "overbroad_curation_entry"
                                else "stale"
                            ].append(diagnostic_issue.case_id)
                        merged_scope = scopes[scope_key].model_copy(
                            update={
                                "cases": (*scopes[scope_key].cases, *new_cases),
                            }
                        )
                        scopes[scope_key] = CompiledScope.model_validate_json(
                            merged_scope.model_dump_json()
                        )
                        compiled.cases[scope_key] = tuple(
                            sorted(
                                (*compiled.cases.get(scope_key, ()), *new_cases),
                                key=lambda case: case.case_id,
                            )
                        )

                    resolution_started = time.perf_counter()
                    result = resolve_source_scope(
                        originals,
                        cases=scope.cases,
                        naming=scope.naming,
                        naming_ambiguities=scope.naming_ambiguities,
                        provider_keys=_unique_pairs(
                            scope.provider_keys, "provider key"
                        ),
                        derive_native_provider_keys=True,
                        value_sessions=sessions,
                        support=support,
                        classifications=books,
                        classification_references=references,
                        label_rules=label_rules,
                        classification_overrides=classification_overrides,
                        matched_labels=matched_labels,
                        duplicate_overrides=duplicate_overrides,
                        declared_variants=_unique_pairs(
                            scope.variants, "declared variant"
                        ),
                        on_diagnostic=issue,
                        diagnostic=diagnostic,
                        coding_scope=scope,
                        coding_registers=scope_coding,
                        on_coding_compiled=record_coding_compilation,
                    )
                    _emit_timing(f"pipeline: resolve {scope_key!r}", resolution_started)
                    seen_scopes.add(scope_key)
                    acknowledged.update(result.acknowledged)
                    coverage.extend(result.coverage)
                    sibling_pairs.update(result.siblings.pairs)
                    for pair in result.siblings.decisions:
                        event(
                            "sibling_pair",
                            {
                                "family": pair.family,
                                "a": pair.a,
                                "b": pair.b,
                                "columns": pair.columns,
                                "disposition": pair.kind,
                                "refs": [r.model_dump(mode="json") for r in pair.refs],
                            },
                        )
                    if registers:
                        slice_keys.update(
                            declared_dependency_keys(
                                scope.naming, scope.naming_ambiguities
                            )
                        )
                    for declaration in scope.naming:
                        if declaration.target.kind == "register_variant":
                            key = declaration.target.source_key
                            register_key = declaration.target.register_key
                            if (
                                key in variant_registers
                                and variant_registers[key] != register_key
                            ):
                                raise ValueError("conflicting global variant parent")
                            variant_registers[key] = register_key
                    for evaluation in result.evaluations:
                        if evaluation.case_id in seen_cases:
                            raise ValueError("curation case evaluated more than once")
                        seen_cases.add(evaluation.case_id)
                        event(
                            "case",
                            evaluation.model_dump(mode="json", exclude={"decision"}),
                        )
                    for kind, mapping in (
                        ("register", result.parents.registers),
                        ("variant", result.parents.variants),
                        ("edition", result.parents.editions),
                    ):
                        for key, value in mapping.items():
                            token = kind, key
                            if token in parents and parents[token] != value:
                                raise ValueError("conflicting global parent definition")
                            parents[token] = value
                    for key, value in result.variables.items():
                        if key in variables:
                            raise ValueError("repeated variable family across scopes")
                        variables[key] = value
                    for key, causes in result.withheld_dependencies.items():
                        if key in withheld:
                            raise ValueError(
                                "repeated withheld dependency across scopes"
                            )
                        withheld[key] = causes
                    uses = {}
                    for occurrence in result.corrections.occurrences:
                        variable = result.variables.get(occurrence.variable_key)
                        fqid = (
                            f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug}"
                            if variable is not None
                            else None
                        )
                        disposition = {
                            "variable": fqid,
                            "use": occurrence.use,
                            "withheld_fields": occurrence.withheld_fields,
                            "cases": sorted(
                                {c.case_id for c in occurrence.corrections}
                            ),
                        }
                        for record in occurrence.source_records:
                            uses.setdefault(record.record_id, []).append(disposition)
                        if not occurrence.source_records:
                            event(
                                "added_occurrence",
                                {"key": occurrence.occurrence_key, **disposition},
                            )
                        if fqid is not None:
                            evidence.setdefault(fqid, set()).update(
                                (r.source, r.locators[0].semantic_record_key)
                                for r in occurrence.evidence
                            )
                    if set(uses) != {r.record_id for r in originals} or len(
                        uses
                    ) != len(originals):
                        raise ValueError(
                            "physical source occurrence accounting is incomplete"
                        )
                    event_bindings.observe_scope(originals, result, uses)
                    for record_id, dispositions in uses.items():
                        event(
                            "source_occurrence",
                            {"record_id": record_id, "dispositions": dispositions},
                        )
                    counts["physical_occurrences"] += len(originals)
                    counts["scopes"] += 1
                    event(
                        "scope_complete",
                        {
                            "source": source,
                            "register": register,
                            "records": len(originals),
                        },
                    )
                    _emit_timing(f"pipeline: scope {scope_key!r}", scope_started)
            _emit_timing("pipeline: all source scopes", phase_started)
            phase_started = time.perf_counter()
            if seen_scopes != visit:
                raise ValueError("declared source scopes were not visited")
            if counts["physical_occurrences"] != expected:
                raise ValueError(
                    f"{'selected' if registers else 'full'} source occurrence count differs from preparation"
                )
            selected_coding_names = {
                name
                for key in visit
                for name in declared_register_fqids(scopes[key].naming).values()
            }
            for name, ids in coding_ids.items():
                if name in seen_coding:
                    continue
                if registers and name not in selected_coding_names:
                    compiled.report[name]["not_evaluated_in_subset"].extend(ids)
                    continue
                compiled.report[name]["stale"].extend(ids)
                for case_id in ids:
                    issue(
                        ResolutionDiagnostic(
                            code="stale_curation_entry",
                            severity="error",
                            case_id=case_id,
                            subject=name,
                            detail=f"{case_id} matches no selected source scope",
                            withheld_output=(case_id,),
                        )
                    )
            for binding_issue in finalize_classification_bindings(
                compiled,
                tree,
                matched_labels=matched_labels,
                duplicate_overrides=duplicate_overrides,
                subset=bool(registers),
            ):
                issue(binding_issue)
            if dump_decisions is not None:
                for index, key in enumerate(sorted(visit, key=repr)):
                    (dump_decisions / f"scope-{index:05d}.json").write_text(
                        json.dumps(
                            {
                                "source": key[0],
                                "register_key": key[1],
                                "cases": [
                                    case.model_dump(mode="json")
                                    for case in compiled.cases.get(key, ())
                                ],
                                "naming": [
                                    item.model_dump(mode="json")
                                    for item in (compiled.naming or {}).get(key, ())
                                ],
                                "provider_keys": (compiled.provider_keys or {}).get(
                                    key, ()
                                ),
                                "variants": [
                                    (native_key, variant.model_dump(mode="json"))
                                    for native_key, variant in (
                                        compiled.variants or {}
                                    ).get(key, ())
                                ],
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                (dump_decisions / "compile-report.json").write_text(
                    json.dumps(
                        compiled.report,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n",
                    encoding="utf-8",
                )
            # A scoped build skips curation whose every register reference lies
            # outside the selected registers. Elsewhere it defers only a
            # reference to what exists but was not selected: compiled naming in
            # the unselected scopes declares those registers, variants and variables,
            # and the prepared store holds their observed names and native IDs.
            # Anything else stays the complete build's error.
            slice_registers = (
                {key[1] for key in slice_keys if key[0] == "register"}
                if registers
                else None
            )
            unselected: set[tuple[str, ...]] = set()
            unselected_names: dict[str, set[str]] = {}
            if registers:
                unvisited = scope_keys - visit
                names = defaultdict(set)
                for source, rows in coordinates.items():
                    for register, coordinate, _ in rows:
                        key = source, None if source in whole_sources else register
                        if key in unvisited and coordinate.name:
                            names[register].add(coordinate.name)
                for key in sorted(unvisited, key=repr):
                    scope = unselected_scopes[key]
                    declared = (
                        declared_dependency_keys(scope.naming, scope.naming_ambiguities)
                        - slice_keys
                    )
                    unselected |= declared
                    for register, fqid in declared_register_fqids(scope.naming).items():
                        if ("register", fqid) in declared:
                            unselected_names.setdefault(fqid, set()).update(
                                names[register]
                            )
                for source in sorted(set(event_sources.values())):
                    outside = {r for s, r in unvisited if s == source}
                    if outside:
                        event_bindings.observe_unselected(
                            source,
                            prepared.records.iter_native_ids(
                                source, None if source in whole_sources else outside
                            ),
                        )
            refs = {
                fqid: tuple(
                    SourceRecordRef(source=s, semantic_record_key=k)
                    for s, k in sorted(items)
                )
                for fqid, items in evidence.items()
            }
            register_parents = {
                key: value
                for (kind, key), value in parents.items()
                if kind == "register"
            }
            variants = tuple(
                (register_parents[variant_registers[key]], value)
                for (kind, key), value in parents.items()
                if kind == "variant"
            )
            editions = tuple(
                value for (kind, key), value in parents.items() if kind == "edition"
            )
            panel = resolve_panel_dependencies(
                tuple(v for v in variables.values() if v is not None),
                registers=tuple(register_parents.values()),
                variants=variants,
                editions=editions,
                withheld=withheld,
            )
            for value in panel.diagnostics:
                issue(value)
            edges = resolve_variable_edge_groups(
                selected.code_label_pairs,
                panel.variables,
                foldable_sibling_pairs=tuple(sorted(sibling_pairs)),
                curated_groups=selected.metadata.variable_groups,
                evidence=refs,
                withheld=withheld,
                unselected=unselected,
                slice_registers=slice_registers,
            )
            months = resolve_month_groups(
                panel.variables,
                edge_groups=edges.groups,
                curated_groups=selected.metadata.variable_groups,
                evidence=refs,
            )
            for value in (*edges.diagnostics, *months.diagnostics):
                issue(value)
            metadata = selected.metadata.model_copy(
                update={
                    **{
                        name: getattr(source_metadata, name)
                        for name in (
                            "identifiers",
                            "source_columns",
                            "source_join_keys",
                            "timeseries_events",
                        )
                    },
                    "variable_groups": (
                        *selected.metadata.variable_groups,
                        *edges.groups,
                        *months.groups,
                    ),
                }
            )
            source_events = event_bindings.resolve(metadata)
            if event_bindings.skipped_events:
                dropped = compiled.report["_subset"]["dropped"]
                dropped.extend(
                    source_event_id(ref) for ref in event_bindings.skipped_events
                )
                dropped.sort()
                if dump_decisions is not None:
                    (dump_decisions / "compile-report.json").write_text(
                        json.dumps(
                            compiled.report,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n",
                        encoding="utf-8",
                    )
            for value in source_events.diagnostics:
                issue(value)
            resolved_metadata = resolve_metadata_dependencies(
                source_events.metadata,
                panel.variables,
                registers=panel.registers,
                variants=panel.variants,
                classifications=tuple(books.values()),
                withheld=withheld,
                unselected=unselected,
                slice_registers=slice_registers,
            )
            for value in resolved_metadata.diagnostics:
                issue(value)
            lineage = resolve_catalog_lineage(
                panel.variables,
                registers=panel.registers,
                variants=panel.variants,
                defaults=_unique_pairs(selected.lineage_defaults, "lineage default"),
                metadata=resolved_metadata.metadata,
                evidence=refs,
                withheld=withheld,
                unselected=unselected,
                unselected_names=unselected_names,
                slice_registers=slice_registers,
            )
            for value in lineage.diagnostics:
                issue(value)
            if registers:
                counts["skipped_curation"] = sum(
                    part.skipped
                    for part in (edges, source_events, resolved_metadata, lineage)
                )
            successions = resolve_classification_successions(
                tuple(books.values()), selected.classification_successions
            )
            final_metadata = resolve_variable_successions(
                lineage.metadata, lineage.variables, successions
            )
            # Mandatory in both modes, whatever else the ledger holds: losing
            # supported delivery is our bug, and no curation error may stand in
            # for the source outcome that never came. Strict mode raises before
            # anything is placed. Diagnostic mode records each unexplained
            # change as an error diagnostic and completes with a nonpublishable
            # database, so one family's defect no longer hides the rest of the
            # cycle.
            for value in check_delivery_coverage(
                lineage.variables, coverage, withheld=withheld, diagnostic=diagnostic
            ):
                issue(value)
            _emit_timing("pipeline: catalog dependencies", phase_started)
            build_result.update(
                {
                    "status": "blocked" if counts["error"] else "ready",
                    "publication_ready": publishable and not counts["error"],
                    "counts": dict(counts),
                    "acknowledged": dict(sorted(acknowledged.items())),
                    "variables": len(panel.variables),
                    "states": sum(len(v.states) for v in panel.variables),
                    "database": None,
                    "curation_tree_sha256": curation_hash,
                }
            )
            if registers:
                build_result.update(
                    registers=sorted(set(registers)),
                    corpus_validation="not_applicable",
                )
            if diagnostic or not counts["error"]:
                phase_started = time.perf_counter()
                write_resolved_catalog(
                    lineage.variables,
                    output,
                    diagnostic=diagnostic,
                    scoped=bool(registers),
                    corpus=publishable,
                    manifest={
                        "import_date": import_date,
                        "prepared_commit": input_commit,
                        "prepared_manifest_sha256": input_manifest_sha256,
                        CURATION_TREE_SHA256_KEY: curation_hash,
                    },
                    parent_registers=panel.registers,
                    parent_variants=panel.variants,
                    editions=panel.editions,
                    classifications=tuple(books.values()),
                    classification_successions=successions,
                    metadata=final_metadata,
                )
                build_result.update(
                    status="diagnostic_complete" if diagnostic else "complete",
                    database=str(output),
                )
                _emit_timing("pipeline: database materialization", phase_started)
                if diagnostic and not registers:
                    # Structural validation already passed before placement. Keep
                    # the unchanged corpus safeguards visible on partial output.
                    validation = validate_built_db(output, corpus=True)
                    corpus_report: dict[str, object] = {
                        "passed": validation.passed,
                        "failures": validation.failures,
                    }
                    build_result["corpus_validation"] = corpus_report
                    event("corpus_validation", corpus_report)
        except Exception as exc:
            if build_result.get("database"):
                raise
            (report_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "engineering_failure",
                        "error": str(exc),
                        "counts": dict(counts),
                    },
                    indent=2,
                )
                + "\n"
            )
            raise
    with _retain_completed_artifact(build_result, report_dir):
        (report_dir / "summary.json").write_text(
            json.dumps(build_result, indent=2) + "\n"
        )
        _emit_timing("pipeline: total", started)
    return build_result
