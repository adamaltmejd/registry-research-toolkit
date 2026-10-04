"""Compile tracked curation against prepared sources and build the catalog."""

from __future__ import annotations

import gzip
import json
import time
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from io import TextIOWrapper
from pathlib import Path
from typing import TYPE_CHECKING, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError
from reg_meta.source_evidence import canonical_sha256

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
    CompiledCuration,
    _swecov_columns,
    coding_entry_windows,
    compile_curation,
    compile_deferred_naming,
    finalize_classification_bindings,
    source_event_id,
    tree_sha256,
    validate_sentinels,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.data_warnings import acknowledged_data_warnings, scope_data_warnings
from reg_meta_build.db import SCHEMA_VERSION, _emit_timing, _paths_overlap
from reg_meta_build.input_snapshot import _git, input_bundle_repository
from reg_meta_build.prepared_catalog import (
    ReferenceEvidence,
    open_prepared_catalog_sources,
)
from reg_meta_build.prepared_values import _DESCRIPTOR
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import (
    ResolvedMetadata,
    RetainedDocumentaryRelationship,
)
from reg_meta_build.source_classifications import resolve_canonical_codes
from reg_meta_build.source_coordinates import (
    NativeKey,
    source_register_key,
)
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CurationCase,
    GuardValidationContext,
    ResolutionDiagnostic,
    SourceRecordRef,
)
from reg_meta_build.source_effects import record_ref
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
from reg_meta_build.source_support import (
    SourceSupportBindings,
    diagnostic_register_contexts,
    scope_support_diagnostics,
)
from reg_meta_build.source_value_bindings import open_value_bindings
from reg_meta_build.sources.swecov_column_types import (
    SWECOV_COLUMN_TYPES_PATH,
    index_steward_column_storage,
)
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from pydantic_core import InitErrorDetails

    from reg_meta_build.curation_tree import CurationTree
    from reg_meta_build.source_value_bindings import (
        ValueBindingIssue,
        ValueBindingSession,
    )


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
    """In-memory declarations for one prepared source scope."""

    source: str
    register_key: NativeKey | None
    cases: tuple[CurationCase, ...] = ()
    source_diagnostics: tuple[tuple[NativeKey, ResolutionDiagnostic], ...] = ()
    naming: tuple[NamingDeclaration, ...] = ()
    naming_ambiguities: tuple[NamingAmbiguity, ...] = ()
    provider_keys: tuple[tuple[NativeKey, str | None], ...] = ()
    variants: tuple[tuple[NativeKey, ResolvedVariant], ...] = ()


_COMPILED_SCOPE_JSON = TypeAdapter(
    object, config=ConfigDict(ser_json_inf_nan="constants")
)
# Only variadic tuples can be validated entry by entry. Collection constraints
# belong to the final normal scope validation, not the single-entry adapters.
_COMPILED_SCOPE_COLLECTIONS = {
    name: TypeAdapter(field.annotation, config=ConfigDict(strict=True))
    for name, field in CompiledScope.model_fields.items()
    if get_origin(field.annotation) is tuple
    and get_args(field.annotation)[-1:] == (Ellipsis,)
}


class CompiledGlobals(_Model):
    """Strict contract for the declarations compiled from tracked curation."""

    classifications: tuple[CompiledCodebook, ...] = ()
    classification_successions: tuple[ResolvedClassificationSuccession, ...] = ()
    metadata: ResolvedMetadata = ResolvedMetadata()
    code_label_pairs: tuple[CodeLabelPair, ...] = ()
    identifier_sources: tuple[str, ...] = ()
    event_sources: tuple[tuple[str, str], ...] = ()
    lineage_defaults: tuple[tuple[str, str], ...] = ()


# Catalog dependency checks, including those within selected registers, are not
# reached by local checking. No deferred-reference proof or final skipped_curation
# accounting follows from a completed local check.
LOCAL_CHECKS_NOT_RUN = (
    "panel_dependencies",
    "variable_edge_groups",
    "month_groups",
    "source_events",
    "metadata_dependencies",
    "catalog_lineage",
    "successions",
    "delivery_coverage",
    "sqlite_structural_validation",
    "corpus_validation",
)


def _validate_compiled_scope(values: dict[str, object]) -> CompiledScope:
    """Validate complete JSON entries without retaining a whole-scope wire buffer."""
    context = GuardValidationContext()
    # simplify: CompiledScope has no cross-field validators. Revisit header parsing
    # if it gains one; final normal validation checks the assembled scope below.
    header = CompiledScope.model_validate_json(
        _COMPILED_SCOPE_JSON.dump_json(
            {
                name: value
                for name, value in values.items()
                if name not in _COMPILED_SCOPE_COLLECTIONS
            },
            warnings="error",
        ),
        context=context,
    )
    parsed = header.model_dump(mode="python")
    for name, adapter in _COMPILED_SCOPE_COLLECTIONS.items():
        supplied = values.get(name, ())
        # Real compiled collections are tuples. Other containers still cross the
        # complete JSON contract so, for example, an empty dict cannot disappear.
        chunks = (
            ((index, (item,)) for index, item in enumerate(supplied))
            if type(supplied) is tuple
            else ((0, supplied),)
        )
        items = []
        for offset, chunk in chunks:
            try:
                items.extend(
                    adapter.validate_json(
                        _COMPILED_SCOPE_JSON.dump_json(chunk, warnings="error"),
                        strict=True,
                        context=context,
                    )
                )
            except ValidationError as exc:
                errors = exc.errors(include_url=False)
                for error in errors:
                    loc = error["loc"]
                    error["loc"] = (
                        (name, offset + loc[0], *loc[1:])
                        if loc and isinstance(loc[0], int)
                        else (name, *loc)
                    )
                raise ValidationError.from_exception_data(
                    CompiledScope.__name__, cast("list[InitErrorDetails]", errors)
                ) from exc
        parsed[name] = tuple(items)
    # Every supplied value has crossed strict JSON validation; normal final model
    # validation still checks the assembled scope, including any scope validators.
    return CompiledScope.model_validate(parsed, context=context)


def _compiled_scope(
    key: tuple[str, NativeKey | None], compiled: CompiledCuration
) -> CompiledScope:
    """Validate one compiled scope through its serialized JSON contract."""
    return _validate_compiled_scope(
        {
            "source": key[0],
            "register_key": key[1],
            "cases": compiled.cases.get(key, ()),
            "source_diagnostics": (compiled.source_diagnostics or {}).get(key, ()),
            "naming": (compiled.naming or {}).get(key, ()),
            "naming_ambiguities": (compiled.naming_ambiguities or {}).get(key, ()),
            "provider_keys": (compiled.provider_keys or {}).get(key, ()),
            "variants": (compiled.variants or {}).get(key, ()),
        }
    )


def _unique_pairs[K, V](items: tuple[tuple[K, V], ...], description: str) -> dict[K, V]:
    result = dict(items)
    if len(result) != len(items):
        raise ValueError(f"duplicate {description}")
    return result


def _classification_references(
    tree: CurationTree,
) -> tuple[dict[str, str], dict[str, tuple[str, ...]]]:
    references = _unique_pairs(
        tuple(
            (name, entry.classification.slug)
            for entry in tree.classifications
            for name in (entry.classification.short_name, *entry.classification.aliases)
        ),
        "classification reference",
    )
    families = _unique_pairs(
        tuple(
            (alias, family.members)
            for family in tree.classification_families.family
            for alias in family.aliases
        ),
        "classification family alias",
    )
    if set(references) & set(families):
        raise ValueError("duplicate classification reference and family alias")
    return references, families


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


def _value_source_issue(
    session: ValueBindingSession, problem: ValueBindingIssue
) -> tuple[ResolutionDiagnostic, dict[str, object]]:
    """An unbound descriptor is evidence, never an inferred variable reference."""
    revision = session.session.source.manifest.revision.revision_id
    descriptor = next(
        (d for d in session.descriptors if d.payload_key == problem.descriptor_key),
        None,
    )
    payload = (
        _DESCRIPTOR.dump_python(descriptor, mode="json", warnings="error")
        if descriptor is not None
        else None
    )
    physical = (
        sum(1 for _ in session.session.lookup_descriptor(descriptor.payload_key))
        if descriptor is not None
        else problem.occurrence_count
    )
    evidence = {
        **asdict(problem),
        "revision_id": revision,
        "descriptor": payload,
        "physical_associations": physical,
    }
    return ResolutionDiagnostic(
        code=problem.code,
        severity="error",
        subject=repr(
            (
                session.source,
                revision,
                problem.descriptor_key,
                canonical_sha256(payload),
                physical,
                problem.raw_member_tokens,
            )
        ),
        detail=f"Source code-list evidence cannot identify its target: {problem!r}",
        fields=("coding",),
        withheld_output=("unbound_value_membership",),
    ), evidence


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
    return _run_pipeline(
        prepared_path,
        input_commit,
        input_manifest_sha256,
        output,
        report_dir,
        diagnostic=diagnostic,
        registers=registers,
        curation_dir=curation_dir,
        dump_decisions=dump_decisions,
    )


def check_curation(
    prepared_path: Path,
    input_commit: str,
    input_manifest_sha256: str,
    report_dir: Path,
    *,
    registers: tuple[str, ...],
    curation_dir: Path | None = None,
    dump_decisions: Path | None = None,
) -> dict[str, object]:
    """Check complete selected scopes before catalog assembly, without a database."""
    if not registers or not all(registers):
        raise ValueError("--registers requires nonempty comma-separated scope names")
    return _run_pipeline(
        prepared_path,
        input_commit,
        input_manifest_sha256,
        None,
        report_dir,
        diagnostic=True,
        registers=registers,
        curation_dir=curation_dir,
        dump_decisions=dump_decisions,
    )


def _run_pipeline(
    prepared_path: Path,
    input_commit: str,
    input_manifest_sha256: str,
    output: Path | None,
    report_dir: Path,
    *,
    diagnostic: bool,
    registers: tuple[str, ...],
    curation_dir: Path | None,
    dump_decisions: Path | None,
) -> dict[str, object]:
    check = output is None
    if len(input_commit) != 40 or any(
        ch not in "0123456789abcdef" for ch in input_commit
    ):
        raise ValueError("--input-commit must be a lowercase 40-character SHA")
    if len(input_manifest_sha256) != 64 or any(
        ch not in "0123456789abcdef" for ch in input_manifest_sha256
    ):
        raise ValueError("--input-manifest-sha256 must be a lowercase SHA-256")
    from .artifact_identity import builder_commit, generation_id

    started = time.perf_counter()
    publishable = not diagnostic and not registers
    prepared_path = prepared_path.resolve()
    output = output.resolve() if output is not None else None
    report_dir = report_dir.resolve()
    if curation_dir is None:
        from reg_meta_build._curation import repo_curation_dir

        curation_dir = repo_curation_dir()
        if curation_dir is None:
            raise ValueError("--curation-dir is required outside a checkout")
    curation_dir = curation_dir.resolve()
    if not curation_dir.is_dir():
        raise ValueError(f"curation directory does not exist: {curation_dir}")
    slug_dir = (curation_dir.parent / "fqid_slugs").resolve()
    dump_decisions = dump_decisions.resolve() if dump_decisions is not None else None
    output_paths = {output, Path(str(output) + ".prev")} if output else set()
    protected_dirs = (prepared_path, curation_dir, slug_dir)
    if dump_decisions is not None and (
        dump_decisions.exists()
        or any(
            dump_decisions.is_relative_to(path) or path.is_relative_to(dump_decisions)
            for path in (*protected_dirs, report_dir, *output_paths)
        )
    ):
        raise ValueError(
            "--dump-decisions must be a new directory separate from inputs and outputs"
        )
    if (
        any(
            path.is_relative_to(directory) or directory.is_relative_to(path)
            for path in (*output_paths, report_dir)
            for directory in protected_dirs
        )
        or (
            output is not None
            and slug_dir.is_dir()
            and slug_dir.is_relative_to(output.parent)
        )
        or (output is not None and output.is_relative_to(report_dir))
        or (output is not None and not publishable and output.exists())
        or (output is not None and output.exists() and not output.is_file())
    ):
        raise ValueError(
            "build outputs must be separate from build inputs and each other"
        )
    revision = None
    if publishable and output is not None:
        output_directories = {"--db": output.parent, "--report-dir": report_dir}
        if dump_decisions is not None:
            output_directories["--dump-decisions"] = dump_decisions
        revision = builder_commit(output_directories=output_directories)
    input_started = time.perf_counter()
    prepared = open_prepared_catalog_sources(
        prepared_path,
        input_commit=input_commit,
        expected_sha256=input_manifest_sha256,
    )
    occurrence_sources = {
        entry.revision.dataset
        for entry in prepared.manifest.inputs
        if entry.record_usage == "occurrence" and entry.revision is not None
    }
    scope_keys = set()
    for source in sorted(occurrence_sources):
        source_registers = {
            register for register, _, _ in prepared.records.register_coordinates(source)
        }
        if None in source_registers and len(source_registers) > 1:
            raise ValueError(
                f"a source cannot select both whole-source and register scopes: {source}"
            )
        scope_keys.update((source, register) for register in source_registers)
    whole_sources = {source for source, register in scope_keys if register is None}
    visit = _selected_scopes(scope_keys, registers) if registers else set(scope_keys)
    if registers and not {source for source, _ in scope_keys} <= occurrence_sources:
        raise ValueError(
            "curation scopes name a source outside the prepared occurrence-source selection"
        )
    _emit_timing("pipeline: load prepared sources and scope indexes", input_started)
    curation_started = time.perf_counter()
    tree = load_curation_tree(curation_dir)
    references, family_references = _classification_references(tree)
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
    storage_columns = _swecov_columns(prepared)
    compiled = compile_curation(
        tree,
        prepared,
        scope_seeds,
        subset=bool(registers),
        storage_columns=storage_columns,
    )
    _emit_timing("pipeline: compile selected curation", curation_started)
    contract_started = time.perf_counter()
    if "_classifications" in compiled.report:
        valid_overrides = set(compiled.report["_classifications"]["entries_matched"])
        classification_overrides = {
            fqid: binding
            for fqid, binding in classification_overrides.items()
            if binding[1] in valid_overrides
        }
    global_json = json.dumps(
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
    selected = CompiledGlobals.model_validate_json(global_json)
    scopes: dict[tuple[str, NativeKey | None], CompiledScope] = {}
    for key in visit:
        scopes[key] = _compiled_scope(key, compiled)
        if dump_decisions is None:
            # Release each raw graph as its validated replacement becomes available.
            for mapping in (
                compiled.cases,
                compiled.source_diagnostics,
                compiled.naming,
                compiled.naming_ambiguities,
                compiled.provider_keys,
                compiled.variants,
            ):
                if mapping is not None:
                    mapping.pop(key, None)
    selected_coding_names = {
        name
        for scope in scopes.values()
        for name in declared_register_fqids(scope.naming).values()
    }
    if dump_decisions is None:
        # Validated scopes own the cases now; keep the duplicate only for dumps.
        compiled.cases.clear()
    _emit_timing("pipeline: validate compiled global contracts", contract_started)
    unselected_scopes: dict[tuple[str, NativeKey | None], CompiledScope] = {}
    if registers and not check:
        deferred_started = time.perf_counter()
        outside_keys = scope_keys - visit
        outside = tuple(
            CompiledScope(source=source, register_key=register)
            for source, register in sorted(outside_keys, key=repr)
        )
        outside_naming, outside_ambiguities = compile_deferred_naming(
            tree, prepared, outside, storage_columns
        )
        unselected_scopes = {
            key: CompiledScope(
                source=key[0],
                register_key=key[1],
                naming=outside_naming.get(key, ()),
                naming_ambiguities=outside_ambiguities.get(key, ()),
            )
            for key in sorted(outside_keys, key=repr)
        }
        _emit_timing("pipeline: compile deferred naming", deferred_started)
    prepared.records.clear_decoded_records()
    coding_registers = {
        f"{register.register_info.provider}/{register.register_info.slug}": register
        for register in tree.registers
        if (
            register.coding.warning
            or register.coding.choice
            or register.coding.uncoded
            or register.coding.omit
            or register.coding.extend
            or register.coding.documented
            or register.coding.sentinel
            or register.coding.support
        )
    }
    coding_ids = {}
    for name, register in coding_registers.items():
        ids = tuple(
            f"{register.source_file}#/coding.{kind}/{index}/period/{period_index}"
            for kind, entries in (
                ("warning", register.coding.warning),
                ("choice", register.coding.choice),
                ("uncoded", register.coding.uncoded),
                ("omit", register.coding.omit),
                ("extend", register.coding.extend),
                ("documented", register.coding.documented),
                ("sentinel", register.coding.sentinel),
                ("support", register.coding.support),
            )
            for index, entry in enumerate(entries, 1)
            for period_index, _ in enumerate(coding_entry_windows(entry), 1)
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
            global_json + "\n",
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
    # Global contracts are validated and any requested dump is written now.
    compiled.fields.clear()
    del global_json
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
    selected_occurrence_sources = {source for source, _ in visit}
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
    data_warnings = {}
    lineage_originals = {
        ref: []
        for case in compiled.lineage_acknowledgements
        if isinstance(case.decision, AcknowledgeDecision)
        for ref in case.decision.refs
    }
    lineage_registers = {}
    sibling_pairs, slice_keys = set(), set()
    build_result: dict[str, object] = {}
    with _retain_completed_artifact(build_result, report_dir), ExitStack() as stack:
        raw_events = stack.enter_context((report_dir / "events.jsonl.gz").open("xb"))
        compressed_events = stack.enter_context(
            gzip.GzipFile(filename="", mode="wb", fileobj=raw_events, mtime=0)
        )
        events = stack.enter_context(TextIOWrapper(compressed_events, encoding="utf-8"))

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
            retained_relationship_issues: dict[str, list[ResolutionDiagnostic]] = (
                defaultdict(list)
            )
            documentary = {
                (
                    r.declaration.revision.dataset,
                    r.declaration.locator.physical_table,
                    r.declaration.locator.physical_record,
                ): r
                for r in selected.metadata.documentary_relationships
            }
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
                        if not (
                            isinstance(declaration, SourceColumnTypeDeclaration)
                            and declaration.revision.artifact_path
                            == SWECOV_COLUMN_TYPES_PATH
                        ):
                            declarations.append(declaration)
                            disposition = "literal_metadata"
                    else:
                        outside_slice = (
                            bool(registers)
                            and declaration.revision.dataset in sources
                            and declaration.revision.dataset
                            not in selected_occurrence_sources
                        )
                        relationship = documentary.get(
                            (
                                declaration.revision.dataset,
                                declaration.locator.physical_table,
                                declaration.locator.physical_record,
                            )
                        )
                        if relationship is not None and not outside_slice:
                            if isinstance(
                                relationship, RetainedDocumentaryRelationship
                            ):
                                disposition = "retained_unattached"
                                retained_issue = ResolutionDiagnostic(
                                    code="retained_unattached_source_relationship",
                                    severity="warning",
                                    subject=f"{relationship.register_ref}:{declaration.locator.physical_table}:{declaration.locator.physical_record}",
                                    detail=relationship.reason,
                                    refs=(
                                        SourceRecordRef(
                                            source=declaration.revision.dataset,
                                            semantic_record_key=declaration.locator.semantic_record_key,
                                        ),
                                    ),
                                    withheld_output=("catalog_relationship",),
                                    case_id=relationship.provenance.split(": ", 1)[0],
                                )
                                retained_relationship_issues[
                                    relationship.register_ref
                                ].append(retained_issue)
                                issue(retained_issue)
                            else:
                                disposition = "owner_bound_literal"
                                for unresolved in relationship.unresolved:
                                    coordinate = (
                                        f"clause:{unresolved.clause_index}"
                                        if unresolved.clause_index is not None
                                        else f"operand:{unresolved.operand_index}"
                                    )
                                    issue(
                                        ResolutionDiagnostic(
                                            code="unresolved_documentary_operand",
                                            severity="warning",
                                            subject=f"{relationship.owner}:{declaration.locator.physical_table}:{declaration.locator.physical_record}:{coordinate}",
                                            detail=f"{unresolved.token!r}: {unresolved.reason} The owner-bound literal is retained without claiming executable or fully bound semantics.",
                                            refs=(
                                                SourceRecordRef(
                                                    source=declaration.revision.dataset,
                                                    semantic_record_key=declaration.locator.semantic_record_key,
                                                ),
                                            ),
                                            withheld_output=("fully_bound_operands",),
                                        )
                                    )
                        else:
                            disposition = (
                                "out_of_slice_relationship"
                                if outside_slice
                                else "unbound_relationship"
                            )
                            issue(
                                ResolutionDiagnostic(
                                    code=DEFERRED_REFERENCE
                                    if outside_slice
                                    else "unbound_source_relationship",
                                    severity="warning" if outside_slice else "error",
                                    subject=repr(
                                        declaration.locator.semantic_record_key
                                    ),
                                    detail=(
                                        "The literal relationship belongs to an unselected prepared occurrence source; endpoint resolution is deferred and its original evidence is retained."
                                        if outside_slice
                                        else "A literal source relationship has no accepted binding to catalog endpoints; its evidence is retained without inventing that binding."
                                    ),
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
            storage_by_register = {
                f"{register.register_info.provider}/{register.register_info.slug}": index_steward_column_storage(
                    register.register_info.steward_table_prefixes, storage_columns
                )
                for register in tree.registers
                if register.register_info.steward_table_prefixes
            }
            event_bindings = SourceEventBindings(
                (
                    d
                    for d in declarations
                    if isinstance(d, SourceEventDeclaration)
                    and d.revision.dataset in event_sources
                ),
                event_sources,
                compiled.event_acknowledgements,
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
                event("support_source_issue", value.model_dump(mode="json"))
            sessions = stack.enter_context(open_value_bindings(prepared.value_sources))
            diagnostic_coordinates = {
                source: prepared.records.register_coordinates(source)
                for source in {
                    *(
                        source
                        for join in prepared.manifest.support_joins
                        for source in join.target_sources
                    ),
                    *(
                        source
                        for session in sessions
                        if session.join is not None
                        for source in session.join.record_sources
                    ),
                }
            }
            source_issues, unscoped_issues = scope_support_diagnostics(
                support, diagnostic_coordinates, frozenset(visit) if registers else None
            )
            pending_source_issues = defaultdict(list)

            def route_source_issue(
                context: tuple[str, NativeKey | None], value: ResolutionDiagnostic
            ) -> None:
                source, register = context
                destination = context if context in visit else (source, None)
                if register is None or destination not in visit:
                    issue(value)
                else:
                    pending_source_issues[destination].append((register, value))

            for context, values in source_issues.items():
                for value in values:
                    route_source_issue(context, value)
            for value in unscoped_issues:
                issue(value)
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
                assert session.join is not None
                for problem in session.source_issues():
                    # An unbindable list has no target occurrence to visit later.
                    # Preserve its exact lookup tokens separately from field issues.
                    value, source_evidence = _value_source_issue(session, problem)
                    event("value_source_issue", source_evidence)
                    contexts = diagnostic_register_contexts(
                        session.join.record_sources, diagnostic_coordinates
                    )
                    context = next(iter(contexts)) if len(contexts) == 1 else None
                    if context is None or context[1] is None:
                        issue(value)
                    elif (
                        registers
                        and context not in visit
                        and (context[0], None) not in visit
                    ):
                        issue(
                            value.model_copy(
                                update={
                                    "code": DEFERRED_REFERENCE,
                                    "severity": "warning",
                                    "detail": f"Outside the selected register slice: {problem.code}. {value.detail}",
                                }
                            )
                        )
                    else:
                        route_source_issue(context, value)
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
                    scope = scopes.pop(scope_key)
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
                        _validate_compiled_scope(
                            {
                                "source": scope_key[0],
                                "register_key": scope_key[1],
                                "cases": new_cases,
                            }
                        )
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
                        if dump_decisions is not None:
                            compiled.cases[scope_key] = tuple(
                                sorted(
                                    (*compiled.cases.get(scope_key, ()), *new_cases),
                                    key=lambda case: case.case_id,
                                )
                            )

                    resolution_started = time.perf_counter()
                    scope_issues: list[ResolutionDiagnostic] = []

                    def record_scope_issue(
                        value: ResolutionDiagnostic,
                        *,
                        collected: list[ResolutionDiagnostic] = scope_issues,
                    ) -> None:
                        collected.append(value)
                        issue(value)

                    result = resolve_source_scope(
                        originals,
                        source_diagnostics=(
                            *scope.source_diagnostics,
                            *pending_source_issues.pop(scope_key, ()),
                        ),
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
                        classification_family_references=family_references,
                        label_rules=label_rules,
                        classification_overrides=classification_overrides,
                        matched_labels=matched_labels,
                        duplicate_overrides=duplicate_overrides,
                        declared_variants=_unique_pairs(
                            scope.variants, "declared variant"
                        ),
                        on_diagnostic=record_scope_issue,
                        diagnostic=diagnostic,
                        coding_scope=scope,
                        coding_registers=scope_coding,
                        storage_by_register=storage_by_register,
                        on_coding_compiled=record_coding_compilation,
                    )
                    _emit_timing(f"pipeline: resolve {scope_key!r}", resolution_started)
                    seen_scopes.add(scope_key)
                    for warning in scope_data_warnings(
                        result,
                        diagnostics=(
                            *scope_issues,
                            *(
                                d
                                for r in result.parents.registers.values()
                                for d in retained_relationship_issues.get(
                                    f"{r.provider}/{r.slug}", ()
                                )
                            ),
                        ),
                    ):
                        data_warnings[warning.warning_id] = warning
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
                    if lineage_originals:
                        for original in originals:
                            ref = record_ref(original)
                            if ref in lineage_originals:
                                lineage_originals[ref].append(original)
                                owner = source_register_key(original)
                                if owner in result.parents.registers:
                                    lineage_registers[owner] = result.parents.registers[
                                        owner
                                    ]
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
                    prepared.records.clear_decoded_records()
                    del result, originals, uses, scope
            _emit_timing("pipeline: all source scopes", phase_started)
            for remaining in pending_source_issues.values():
                for _, value in remaining:
                    issue(value)
            phase_started = time.perf_counter()
            if seen_scopes != visit:
                raise ValueError("declared source scopes were not visited")
            if counts["physical_occurrences"] != expected:
                raise ValueError(
                    f"{'selected' if registers else 'full'} source occurrence count differs from preparation"
                )
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
            if check:
                build_result.update(
                    status="curation_check_complete",
                    passed=not counts["error"],
                    publication_ready=False,
                    database=None,
                    registers=sorted(set(registers)),
                    prepared_commit=input_commit,
                    prepared_manifest_sha256=input_manifest_sha256,
                    curation_tree_sha256=curation_hash,
                    counts=dict(counts),
                    acknowledged=dict(sorted(acknowledged.items())),
                    checks_not_run=list(LOCAL_CHECKS_NOT_RUN),
                )
            else:
                assert output is not None
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
                            declared_dependency_keys(
                                scope.naming, scope.naming_ambiguities
                            )
                            - slice_keys
                        )
                        unselected |= declared
                        for register, fqid in declared_register_fqids(
                            scope.naming
                        ).items():
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
                    if value.acknowledged_by is not None:
                        acknowledged[value.code] += 1
                    issue(value)
                for warning in acknowledged_data_warnings(
                    source_events.diagnostics,
                    compiled.event_acknowledgements,
                    event_bindings.guarded_registers,
                ):
                    data_warnings[warning.warning_id] = warning
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
                    defaults=_unique_pairs(
                        selected.lineage_defaults, "lineage default"
                    ),
                    metadata=resolved_metadata.metadata,
                    evidence=refs,
                    withheld=withheld,
                    unselected=unselected,
                    unselected_names=unselected_names,
                    source_labels={
                        f"{register.register_info.provider}/{register.register_info.slug}": register.register_info.source_labels
                        for register in tree.registers
                        if register.register_info.source_labels
                    },
                    slice_registers=slice_registers,
                    acknowledgements=compiled.lineage_acknowledgements,
                    acknowledgement_originals={
                        ref: tuple(rows) for ref, rows in lineage_originals.items()
                    },
                )
                for value in lineage.diagnostics:
                    if value.acknowledged_by is not None:
                        acknowledged[value.code] += 1
                    issue(value)
                for warning in acknowledged_data_warnings(
                    lineage.diagnostics,
                    compiled.lineage_acknowledgements,
                    lineage_registers,
                ):
                    data_warnings[warning.warning_id] = warning
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
                    lineage.variables,
                    coverage,
                    withheld=withheld,
                    diagnostic=diagnostic,
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
                    identity = {}
                    if publishable:
                        assert revision is not None
                        if builder_commit() != revision:
                            raise ValueError(
                                "Builder revision changed during compilation"
                            )
                        identity = {
                            "builder_commit": revision,
                            "generation_id": generation_id(
                                {
                                    "schema_version": SCHEMA_VERSION,
                                    "builder_commit": revision,
                                    "catalog_artifact_kind": "catalog",
                                    "prepared_commit": input_commit,
                                    "prepared_manifest_sha256": input_manifest_sha256,
                                    "curation_tree_sha256": curation_hash,
                                }
                            ),
                        }
                    write_resolved_catalog(
                        lineage.variables,
                        output,
                        diagnostic=diagnostic,
                        scoped=bool(registers),
                        corpus=publishable,
                        manifest={
                            **identity,
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
                        data_warnings=tuple(
                            data_warnings[k] for k in sorted(data_warnings)
                        ),
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
