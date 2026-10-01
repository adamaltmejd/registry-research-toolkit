"""Compose common resolution over a complete source scope, before materialization.

A scope contains every original peer needed by its decisions and every competing
alias owner in its registers. Catalog naming never gates source-level decisions.
The caller opens prepared value sessions once and seals support bindings against
the complete selected corpus before invoking this resolver.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.catalog_dependencies import variable_dependency_keys
from reg_meta_build.catalog_resolution import ParentResolution, resolve_parents
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_annotations import apply_alias_cases
from reg_meta_build.source_classification_bindings import apply_classification_cases
from reg_meta_build.source_coding import coding_source_sha256
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CheckedIdentityChange,
    ClassificationDecision,
    CodingDecision,
    DeliveryMetadataDecision,
    OccurrenceCorrectionDecision,
    RepresentationDecision,
    ResolutionDiagnostic,
    SourceEvidence,
    acknowledgement_evidence_sha256,
    evaluate_cases,
)
from reg_meta_build.source_effects import (
    OccurrenceCorrections,
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import reconcile_source_fields
from reg_meta_build.source_naming import check_naming_target, native_provider_keys
from reg_meta_build.source_representations import resolve_representation_cases
from reg_meta_build.source_siblings import SiblingResolution, resolve_sibling_pairs
from reg_meta_build.source_value_bindings import (
    bind_copied_coding,
    bind_occurrence_code_lists,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from reg_meta_build.catalog_dependencies import CoverageObligation, DependencyKey
    from reg_meta_build.curation_tree import RegisterCuration
    from reg_meta_build.pipeline import CompiledScope
    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedVariable,
        ResolvedVariant,
    )
    from reg_meta_build.source_coding import CodeListClaim
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import (
        CaseEvaluation,
        CurationCase,
        SourceRecordRef,
    )
    from reg_meta_build.source_naming import NamingAmbiguity, NamingDeclaration
    from reg_meta_build.source_occurrences import EffectiveOccurrence
    from reg_meta_build.source_records import SourceRecord
    from reg_meta_build.source_support import SourceSupportBindings
    from reg_meta_build.source_value_bindings import (
        ValueBindingResult,
        ValueBindingSession,
    )
    from reg_meta_build.sources.swecov_column_types import StewardColumnStorage


@dataclass(frozen=True)
class ScopeResolution:
    parents: ParentResolution
    corrections: OccurrenceCorrections
    variables: dict[NativeKey, ResolvedVariable | None]
    evaluations: tuple[CaseEvaluation, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]
    error_count: int
    warning_count: int
    withheld_dependencies: dict[DependencyKey, tuple[ResolutionDiagnostic, ...]]
    siblings: SiblingResolution
    coverage: tuple[CoverageObligation, ...]
    acknowledged: dict[str, int]


def declared_register_fqids(
    naming: Iterable[NamingDeclaration],
) -> dict[NativeKey, str]:
    """Each named register's catalog FQID by its native register key."""
    return {
        declaration.target.source_key: (
            f"{declaration.naming.provider}/{declaration.naming.slug}"
        )
        for declaration in naming
        if declaration.target.kind == "register" and declaration.naming.slug is not None
    }


def declared_dependency_keys(
    naming: tuple[NamingDeclaration, ...],
    naming_ambiguities: tuple[NamingAmbiguity, ...],
) -> set[DependencyKey]:
    """The registers, variants and variables a scope's naming declares, keyed as
    formation would make them available. An ambiguous name is a declared variable
    whose ownership is unresolved, keyed as the complete build withholds it."""
    registers = declared_register_fqids(naming)
    keys: set[DependencyKey] = {("register", fqid) for fqid in registers.values()}
    for declaration in naming:
        register = registers.get(declaration.target.register_key)
        slug = declaration.naming.slug
        if register is None or slug is None:
            continue
        if declaration.target.kind == "register_variant":
            keys.add(("variant", register, slug))
        elif declaration.target.kind == "variable":
            keys.add(("variable", f"{register}/{slug}"))
    for ambiguity in naming_ambiguities:
        register = registers.get(ambiguity.family.register_key)
        if register is not None:
            keys.update(("variable", f"{register}/{n.slug}") for n in ambiguity.names)
    return keys


def resolve_source_scope(
    originals: tuple[SourceRecord, ...],
    *,
    cases: tuple[CurationCase, ...],
    naming: tuple[NamingDeclaration, ...],
    naming_ambiguities: tuple[NamingAmbiguity, ...] = (),
    provider_keys: Mapping[NativeKey, str | None],
    derive_native_provider_keys: bool = False,
    value_sessions: tuple[ValueBindingSession, ...],
    support: SourceSupportBindings,
    classifications: Mapping[str, ResolvedClassification],
    classification_references: Mapping[str, str],
    classification_family_references: Mapping[str, tuple[str, ...]] = {},
    label_rules: Mapping[str, str] = {},
    classification_overrides: Mapping[str, tuple[str, str]] = {},
    matched_labels: set[str] | None = None,
    duplicate_overrides: set[str] | None = None,
    declared_variants: Mapping[NativeKey, ResolvedVariant] | None = None,
    on_binding: Callable[[NativeKey, ValueBindingResult], None] | None = None,
    on_diagnostic: Callable[[ResolutionDiagnostic], None] | None = None,
    source_diagnostics: tuple[tuple[NativeKey, ResolutionDiagnostic], ...] = (),
    diagnostic: bool = False,
    coding_scope: CompiledScope | None = None,
    coding_registers: tuple[RegisterCuration, ...] = (),
    storage_by_register: Mapping[str, Mapping[str, StewardColumnStorage]] | None = None,
    on_coding_compiled: Callable[
        [RegisterCuration, tuple[CurationCase, ...], tuple[ResolutionDiagnostic, ...]],
        None,
    ]
    | None = None,
) -> ScopeResolution:
    """Resolve one complete scope without IO policy.

    Two strict/diagnostic forks: the stale-partition withholding below, and
    formation's per-column state overlaps, which a diagnostic build withholds.

    Missing mappings and unsupported decisions are implementation failures, with
    one diagnostic exception: an unmapped key that is the unsplit base of a
    non-applied partition case's exact split set, with every split key mapped,
    is treated as an explicit None provider key (unresolved catalog identity), so
    a stale partition decision withholds its family instead of aborting the
    build. Strict mode still fails fast.

    An explicit None provider key records an unresolved catalog identity; coding and
    other source decisions still run. Checked naming and parent facts determine
    which dependencies may be materialized. No source key is parsed as a catalog
    identity, and a withheld variant does not discard safe sibling states.
    A diagnostic sink streams the full ledger instead of retaining it in memory;
    severity counts are always returned, including when the sink is used.
    Exact evidenced dependency omissions are returned independently of that sink.
    Missing references are never converted to omissions merely because absent.
    Supported delivery the formed variables still owe the catalog is carried out
    unchanged, for the boundary guard that runs before the database is written.
    An acknowledgement turns the one error it names into a counted warning once
    the whole scope has been resolved; the output that error withheld stays withheld.
    """
    if len({c.case_id for c in cases}) != len(cases):
        raise ValueError("source scope case IDs must be unique")
    evidence = SourceEvidence(originals)
    diagnostics = []
    counts = {"error": 0, "warning": 0}
    acknowledgements = tuple(c for c in cases if c.decision.kind == "acknowledge")
    guarded_refs = {
        ref
        for case in acknowledgements
        if isinstance(case.decision, AcknowledgeDecision)
        and case.decision.expected_evidence_sha256 is not None
        for ref in case.decision.refs
    }
    guarded_originals: dict[SourceRecordRef, list[SourceRecord]] = defaultdict(list)
    if guarded_refs:
        for original in originals:
            ref = record_ref(original)
            if ref in guarded_refs:
                guarded_originals[ref].append(original)
        support_sources = {ref.source for ref in guarded_refs} & support.joins.keys()
        if support_sources:
            # External support diagnostics name actual support originals, not the
            # delivery rows. Pin their full facts without joining their flags.
            for item in support.accounting:
                if item.record.source in support_sources:
                    ref = record_ref(item.record)
                    if ref in guarded_refs:
                        guarded_originals[ref].append(item.record)
    guarded_coding: dict[SourceRecordRef, list[str]] = defaultdict(list)
    original_registers = (
        {record_ref(item): source_register_key(item) for item in originals}
        if acknowledgements
        else {}
    )
    # Errors an acknowledgement names, settled once the whole scope is resolved.
    held: dict[
        tuple[
            str,
            str,
            tuple[SourceRecordRef, ...],
            tuple[str, ...],
            str | None,
            str | None,
        ],
        tuple[CurationCase, AcknowledgeDecision, list[ResolutionDiagnostic]],
    ] = {}
    for case in acknowledgements:
        decision = case.decision
        assert isinstance(decision, AcknowledgeDecision)
        issue_key = (
            decision.code,
            decision.subject,
            decision.refs,
            decision.fields,
            decision.valid_from,
            decision.valid_to,
        )
        if issue_key in held:
            raise ValueError(f"one issue is acknowledged twice: {case.case_id}")
        held[issue_key] = case, decision, []

    def record(issue: ResolutionDiagnostic) -> None:
        counts[issue.severity] += 1
        if on_diagnostic is None:
            diagnostics.append(issue)
        else:
            on_diagnostic(issue)

    def emit(
        issue: ResolutionDiagnostic, *, source_register: NativeKey | None = None
    ) -> None:
        if held and issue.severity == "error":
            match = held.get(
                (
                    issue.code,
                    issue.subject,
                    issue.refs,
                    issue.fields,
                    issue.valid_from,
                    issue.valid_to,
                )
            )
            if match is not None and (
                source_register == match[1].register_key
                if source_register is not None
                else bool(issue.refs)
                and all(
                    original_registers.get(ref) == match[1].register_key
                    for ref in issue.refs
                )
            ):
                match[2].append(issue)
                return
        record(issue)

    observed_registers = (
        {
            source_register_key(item)
            for item in originals
            if item.subject.register_name.status == "value"
        }
        if source_diagnostics
        else set()
    )
    for register, initial_issue in source_diagnostics:
        if register not in observed_registers:
            raise ValueError(
                "source diagnostic lacks a positively observed scope register"
            )
        emit(initial_issue, source_register=register)

    names = {}
    withheld_naming = set()
    for declaration in naming:
        target = declaration.target
        token = target.kind, target.source_key
        if token in names and names[token].naming != declaration.naming:
            raise ValueError(f"conflicting catalog naming declarations: {token!r}")
        names.setdefault(token, declaration)
        issues = check_naming_target(target, evidence)
        for issue in issues:
            emit(issue)
        if issues:
            withheld_naming.add(target.source_key)
    if derive_native_provider_keys:
        provider_keys = {
            **native_provider_keys(evidence.native_variable_anchors, names.values()),
            **provider_keys,
        }
    occurrence_cases = tuple(
        c for c in cases if c.decision.kind == "correct_occurrences"
    )
    copied_coding = bind_copied_coding(evidence, occurrence_cases, value_sessions)
    corrected = apply_occurrence_cases(evidence, occurrence_cases, coding=copied_coding)
    enumerated_columns = {
        entry.column
        for register in coding_registers
        for entry in register.coding.documented
        if entry.source_authority is not None
        and entry.source_authority.enumeration is not None
    }
    enumerated_bindings = defaultdict(list) if enumerated_columns else None
    coding_evidence = SourceEvidence(
        originals,
        effective_occurrences=corrected.occurrences,
        value_bindings=enumerated_bindings,
    )
    for issue in corrected.diagnostics:
        emit(issue)
    evaluations = [a.evaluation for a in corrected.accounting]
    # Acknowledgements pin no source members, so they are always applicable;
    # a stale or over-broad one is reported when it is settled below.
    evaluations.extend(evaluate_cases(acknowledgements, evidence))
    parents = resolve_parents(
        corrected.occurrences,
        tuple(names.values()),
        withheld_naming=frozenset(withheld_naming),
        rebinds={
            record_ref(source): occurrence.variant_key
            for occurrence in corrected.occurrences
            if occurrence.variant_key is not None
            for source in occurrence.source_records
            if occurrence.variant_key != native_variant_key(source)
            and occurrence.edition_key is not None
        },
    )
    for issue in parents.diagnostics:
        emit(issue)
    register_fqids = declared_register_fqids(names.values())
    variable_fqids = Counter(
        f"{register_fqids[declaration.target.register_key]}/{declaration.naming.slug}"
        for declaration in names.values()
        if declaration.target.kind == "variable"
        and declaration.naming.slug is not None
        and declaration.target.register_key in register_fqids
    )
    withheld = defaultdict(list)
    parent_causes = defaultdict(list)
    for issue in parents.diagnostics:
        if issue.code in {"unknown_parent_name", "withheld_parent_naming"}:
            parent_causes[issue.subject].append(issue)
    for key, fqid in register_fqids.items():
        if key not in parents.registers and (causes := parent_causes.get(repr(key))):
            withheld["register", fqid].extend(causes)
    for (kind, key), declaration in names.items():
        if (
            kind != "register_variant"
            or key in parents.variants
            or declaration.naming.slug is None
        ):
            continue
        fqid = register_fqids.get(declaration.target.register_key)
        if fqid is not None:
            causes = parent_causes.get(repr(key)) or withheld.get(
                ("register", fqid), ()
            )
            if causes:
                withheld["variant", fqid, declaration.naming.slug].extend(causes)
    variants: dict[NativeKey, ResolvedVariant | None] = {
        key: parents.variants.get(key)
        for kind, key in names
        if kind == "register_variant"
    }
    for key, variant in (declared_variants or {}).items():
        declaration = names.get(("register_variant", key))
        if declaration is None or declaration.naming.slug != variant.slug:
            raise ValueError("declared variant lacks matching checked naming")
        if key in parents.variants and parents.variants[key] != variant:
            raise ValueError("declared variant conflicts with resolved source facts")
        if (
            key not in withheld_naming
            and declaration.target.register_key in parents.registers
        ):
            variants[key] = variant
            parents.variants[key] = variant

    groups: dict[NativeKey, list[EffectiveOccurrence]] = defaultdict(list)
    column_owners = {}
    original_columns: dict[NativeKey, set[NativeKey]] = defaultdict(set)
    for occurrence in corrected.occurrences:
        if occurrence.use != "catalog" or occurrence.variable_key is None:
            continue
        groups[occurrence.variable_key].append(occurrence)
        if occurrence.column_key is not None:
            if (
                len(occurrence.variable_key) >= 2
                and occurrence.variable_key[-2] == "accepted-partition"
            ):
                for source_record in occurrence.source_records:
                    if (original := native_column_key(source_record)) is not None:
                        original_columns[original].add(occurrence.column_key)
            owner = column_owners.setdefault(
                occurrence.column_key, occurrence.variable_key
            )
            if owner != occurrence.variable_key:
                raise ValueError("one exact column key has multiple variable owners")
    claims_by_variable: dict[NativeKey, dict[NativeKey, list[CodeListClaim]]] = {}
    column_records: dict[NativeKey, dict[SourceRecordRef, SourceRecord]] = defaultdict(
        dict
    )

    def bind_claims(
        key: NativeKey, items: tuple[EffectiveOccurrence, ...]
    ) -> dict[NativeKey, list[CodeListClaim]]:
        claims: dict[NativeKey, list[CodeListClaim]] = defaultdict(list)
        for occurrence in sorted(
            items,
            key=lambda o: (
                repr(
                    tuple(
                        (r.source, r.subject.member.native_id)
                        for r in (o.source_records or o.coding_records)
                    )
                ),
                repr(
                    o.edition_period_scope
                    if o.edition_period_scope.kind != "not_applicable"
                    else o.edition_scope
                ),
            ),
        ):
            column = occurrence.column_key
            if column is None:
                continue
            if coding_registers:
                for source_record in occurrence.evidence:
                    column_records[column][record_ref(source_record)] = source_record
            bound = bind_occurrence_code_lists(
                occurrence, value_sessions, support=support
            )
            claims[column].extend(bound.claims)
            if guarded_refs:
                tokens = tuple(coding_source_sha256(claim) for claim in bound.claims)
                for original in occurrence.source_records:
                    ref = record_ref(original)
                    if ref in guarded_refs:
                        guarded_coding[ref].extend(tokens)
            if (
                enumerated_bindings is not None
                and occurrence.fields.column_name is not None
                and occurrence.fields.column_name.value in enumerated_columns
            ):
                binding_scope = (
                    occurrence.edition_period_scope
                    if occurrence.edition_period_scope.kind != "not_applicable"
                    else occurrence.edition_scope
                )
                enumerated_bindings[column].extend(
                    (binding_scope, binding) for binding in bound.bindings
                )
            if on_binding is not None:
                on_binding(key, bound)
            for binding in bound.bindings:
                if binding.item_validity_set_aside:
                    emit(
                        ResolutionDiagnostic(
                            code="item_validity_set_aside",
                            severity="warning",
                            subject=repr(column),
                            detail=(
                                f"Descriptor {binding.descriptor_key!r} keeps edition"
                                " members despite item validity at associations "
                                f"{tuple(a.locator for a in binding.item_validity_set_aside)!r}"
                            ),
                            refs=tuple(
                                dict.fromkeys(
                                    record_ref(r) for r in occurrence.evidence
                                )
                            ),
                            fields=("coding",),
                        )
                    )
            for issue in bound.issues:
                emit(
                    ResolutionDiagnostic(
                        code=issue.code,
                        severity="error",
                        subject=repr(column),
                        detail=f"Prepared coding evidence cannot be bound: {issue!r}",
                        refs=tuple(
                            dict.fromkeys(record_ref(r) for r in occurrence.evidence)
                        ),
                        fields=("coding",),
                        withheld_output=("state.value_set",),
                    )
                )
        return claims

    if coding_registers:
        # Coding decisions need every effective column and original claim in the
        # register before any variable starts applying choices. Other scopes keep
        # the bounded one-variable-at-a-time binding path.
        for key, items in sorted(groups.items(), key=lambda item: repr(item[0])):
            claims_by_variable[key] = bind_claims(key, tuple(items))
    if coding_registers and coding_scope is None:
        raise ValueError("coding register compilation requires its scope declarations")
    compiled_cases = []
    coding_columns = {
        column: tuple(records[ref] for ref in sorted(records, key=repr))
        for column, records in column_records.items()
    }
    original_coding = {
        column: tuple(values)
        for claims in claims_by_variable.values()
        for column, values in claims.items()
    }
    for register in coding_registers:
        assert coding_scope is not None
        new_cases, new_diagnostics = compile_coding_register(
            register,
            coding_scope,
            originals=originals,
            columns=coding_columns,
            column_scopes=coding_evidence.effective_scopes or {},
            coding=original_coding,
            classifications=classifications,
            value_bindings=enumerated_bindings,
        )
        compiled_cases.extend(new_cases)
        for issue in new_diagnostics:
            emit(issue)
        if on_coding_compiled is not None:
            on_coding_compiled(register, new_cases, new_diagnostics)
    cases = (*cases, *compiled_cases)
    if len({c.case_id for c in cases}) != len(cases):
        raise ValueError("source scope case IDs must be unique")
    late = defaultdict(list)
    aliases = []
    for case in cases:
        kind = case.decision.kind
        if kind in {"correct_occurrences", "acknowledge"}:
            continue
        if kind in {"search_alias", "alias_window"}:
            aliases.append(case)
            continue
        if isinstance(case.decision, (CodingDecision, ClassificationDecision)):
            column_key = case.decision.column_key
            if column_key not in column_owners:
                replacements = original_columns.get(column_key, set())
                if len(replacements) == 1:
                    column_key = next(iter(replacements))
                    case = case.model_copy(
                        update={
                            "decision": case.decision.model_copy(
                                update={"column_key": column_key}
                            )
                        }
                    )
            key = column_owners.get(column_key)
        else:
            assert isinstance(
                case.decision, (RepresentationDecision, DeliveryMetadataDecision)
            )
            key = case.decision.variable_key
        if key not in groups:
            raise ValueError(
                f"case has an unconverted effective identity: {case.case_id}"
            )
        late[key].append(case)
    variables = {}
    coverage: list[CoverageObligation] = []
    stale_splits: list[tuple[NativeKey, ...]] = []
    if diagnostic:
        for entry in corrected.accounting:
            if entry.disposition == "applied":
                continue
            decision = entry.case.decision
            if not isinstance(decision, OccurrenceCorrectionDecision):
                continue
            splits = tuple(
                tuple(effect.variable_key)
                for effect in decision.effects
                if isinstance(effect, CheckedIdentityChange)
            )
            if splits:
                stale_splits.append(splits)
    for key, items in sorted(groups.items(), key=lambda item: repr(item[0])):
        occurrences = tuple(items)
        refs = tuple(
            sorted({record_ref(r) for o in occurrences for r in o.evidence}, key=repr)
        )
        if key not in provider_keys:
            if not (
                diagnostic
                and any(
                    all(
                        len(split) > len(key) and split[: len(key)] == key
                        for split in splits
                    )
                    and all(provider_keys.get(split) is not None for split in splits)
                    for splits in stale_splits
                )
            ):
                raise ValueError(f"missing explicit provider key mapping: {key!r}")
            provider_key = None
        else:
            provider_key = provider_keys[key]
        claims = (
            claims_by_variable[key]
            if coding_registers
            else bind_claims(key, occurrences)
        )
        selected = tuple(late[key])
        chosen = apply_coding_choices(
            coding_evidence,
            tuple(c for c in selected if c.decision.kind == "coding"),
            coding={column: tuple(values) for column, values in claims.items()},
        )
        declaration = names.get(("variable", key))
        register_fqid = (
            register_fqids.get(declaration.target.register_key) if declaration else None
        )
        fqid = (
            f"{register_fqid}/{declaration.naming.slug}"
            if register_fqid and declaration and declaration.naming.slug
            else None
        )
        classified = apply_classification_cases(
            coding_evidence,
            tuple(c for c in selected if c.decision.kind == "classification"),
            coding=chosen.coding,
            classifications=classifications,
            occurrences=occurrences,
            references=classification_references,
            family_references=classification_family_references,
            label_rules=label_rules,
            override=(
                classification_overrides.get(fqid)
                if fqid is not None and variable_fqids[fqid] == 1
                else None
            ),
            matched_labels=matched_labels,
            duplicate_overrides=duplicate_overrides,
        )
        representation = resolve_representation_cases(
            coding_evidence,
            tuple(
                c
                for c in selected
                if c.decision.kind in {"representations", "delivery_metadata"}
            ),
            coding=classified.coding,
        )
        for result in (chosen, classified, representation):
            evaluations.extend(result.evaluations)
            for issue in result.diagnostics:
                emit(issue)
        if provider_key is not None and (
            declaration is None or declaration.naming.slug is None
        ):
            raise ValueError(f"provider key has no converted catalog naming: {key!r}")
        register = (
            parents.registers.get(declaration.target.register_key)
            if declaration
            else None
        )
        variables[key] = None
        if provider_key is None or key in withheld_naming or register is None:
            if (
                provider_key is None
                and key not in withheld_naming
                and all(
                    occurrence.fields.column_name is not None
                    and occurrence.fields.column_name.status == "negative"
                    for occurrence in occurrences
                )
            ):
                issue = ResolutionDiagnostic(
                    code="omitted_columnless_occurrence",
                    severity="warning",
                    subject=repr(key),
                    detail="The source states the member has no physical column; "
                    "the occurrence is omitted on purpose.",
                    refs=refs,
                    fields=("column_name",),
                    withheld_output=("occurrence",),
                )
            else:
                issue = ResolutionDiagnostic(
                    code="unresolved_catalog_identity"
                    if provider_key is None or key in withheld_naming
                    else "withheld_register_dependency",
                    severity="error",
                    subject=repr(key),
                    detail="Catalog formation is withheld; source-level decisions were still evaluated against the complete original scope.",
                    refs=refs,
                    fields=("identity",),
                    withheld_output=("variable",),
                )
            emit(issue)
            if fqid is not None:
                withheld["variable", fqid].append(issue)
            for column, coding in classified.coding.items():
                for issue in coding.issues:
                    emit(
                        ResolutionDiagnostic(
                            code=issue.code,
                            severity="error",
                            subject=repr(column),
                            detail="Source coding remains unresolved even though catalog identity is withheld: "
                            + ", ".join(issue.claim_ids),
                            refs=refs,
                            fields=("coding",),
                            valid_from=issue.valid_from,
                            valid_to=issue.valid_to,
                            withheld_output=(issue.withheld,),
                        )
                    )
            continue
        assert declaration is not None and declaration.naming.slug is not None
        fields = {
            match.record.record_id: match.fields
            for occurrence in occurrences
            for record in occurrence.source_records
            for match in support.bind(record)
        }
        flags, _conflicts = reconcile_source_fields(
            occurrences, support=tuple(fields.values())
        )
        formed = form_native_variable(
            occurrences,
            register=register,
            variants=variants,
            slug=declaration.naming.slug,
            provider_key=provider_key,
            flags=flags,
            coding=classified.coding,
            representations=representation.cases,
            diagnostic=diagnostic,
            storage=(storage_by_register or {}).get(
                f"{register.provider}/{register.slug}"
            ),
        )
        variables[key] = (
            formed.variable.model_copy(
                update={"deprecated": declaration.naming.deprecated}
            )
            if formed.variable is not None
            else None
        )
        coverage.extend(formed.coverage)
        for issue in formed.diagnostics:
            emit(issue)
        assert fqid is not None
        if formed.variable is None:
            causes = tuple(
                issue for issue in formed.diagnostics if fqid in issue.withheld_output
            )
            if not causes:
                raise ValueError(
                    "variable formation omitted output without a terminal source outcome"
                )
            withheld["variable", fqid].extend(causes)
        else:
            for variant, causes in formed.withheld_variant_states.items():
                withheld["variant_states", fqid, variant].extend(causes)
            for (variant, column), causes in formed.withheld_representations.items():
                withheld["representation", fqid, column].extend(causes)
                withheld["succession_representation", fqid, column.lower()].extend(
                    causes
                )
                withheld[
                    "succession_representation", fqid, column.lower(), variant
                ].extend(causes)
    annotated = apply_alias_cases(
        evidence, tuple(aliases), variables=variables, variants=variants
    )
    evaluations.extend(annotated.evaluations)
    for issue in annotated.diagnostics:
        emit(issue)
    available = {
        key
        for variable in annotated.variables.values()
        if variable is not None
        for key in variable_dependency_keys(variable)
    }
    available.update(("register", register_fqids[key]) for key in parents.registers)
    for key, variant in parents.variants.items():
        register_key = names["register_variant", key].target.register_key
        assert register_key is not None
        available.add(("variant", register_fqids[register_key], variant.slug))
    for key, causes in _attribute_unresolved_names(
        naming_ambiguities,
        originals=originals,
        evidence=evidence,
        groups=groups,
        occurrences=corrected.occurrences,
        variables=annotated.variables,
        unresolved_keys=frozenset(
            k for k, provider_key in provider_keys.items() if provider_key is None
        ),
        register_fqids=register_fqids,
        variant_slugs={
            key: name.naming.slug
            for (kind, key), name in names.items()
            if kind == "register_variant" and name.naming.slug is not None
        },
        available=available,
    ).items():
        withheld[key].extend(causes)
    # One attributed issue can explain several exact dependency keys.
    for issue in dict.fromkeys(
        cause
        for causes in withheld.values()
        for cause in causes
        if cause.code == "ambiguous_named_identity"
    ):
        emit(issue)
    if {e.case_id for e in evaluations} != {c.case_id for c in cases} or len(
        evaluations
    ) != len(cases):
        raise ValueError(
            "source scope did not evaluate every selected case exactly once"
        )
    sibling_identities = {}
    for occurrence in corrected.occurrences:
        key = occurrence.variable_key
        if key is None or key in sibling_identities:
            continue
        declaration = names.get(("variable", key))
        sibling_identities[key] = (
            f"{register_fqids[declaration.target.register_key]}/{declaration.naming.slug}"
            if declaration is not None
            and declaration.naming.slug is not None
            and declaration.target.register_key in register_fqids
            and key not in withheld_naming
            else None
        )
    siblings = resolve_sibling_pairs(
        corrected.occurrences, identities=sibling_identities
    )
    for issue in siblings.diagnostics:
        emit(issue)
    acknowledged: dict[ResolutionDiagnostic, ResolutionDiagnostic] = {}
    for case, decision, matched in held.values():
        evidence_matches = (
            decision.expected_evidence_sha256 is None
            or decision.expected_evidence_sha256
            == acknowledgement_evidence_sha256(
                (record for ref in decision.refs for record in guarded_originals[ref]),
                (token for ref in decision.refs for token in guarded_coding[ref]),
            )
        )
        if len(matched) == 1 and evidence_matches:
            issue = matched[0]
            warning = issue.model_copy(
                update={"severity": "warning", "acknowledged_by": case.case_id}
            )
            acknowledged[issue] = warning
            record(warning)
            continue
        for issue in matched:
            record(issue)
        record(
            ResolutionDiagnostic(
                code="overbroad_curation_entry"
                if len(matched) > 1 and evidence_matches
                else "stale_curation_entry",
                severity="error",
                case_id=case.case_id,
                subject=decision.subject,
                detail=(
                    f"The acknowledgement of {decision.code!r} has changed original or coding evidence."
                    if not evidence_matches
                    else f"The acknowledgement of {decision.code!r} matches {len(matched)} issues; it must name exactly one."
                ),
                refs=decision.refs,
                fields=decision.fields,
                valid_from=decision.valid_from,
                valid_to=decision.valid_to,
            )
        )
    # Dependents of acknowledged output inherit the warning, not the error.
    withheld_dependencies = {
        key: tuple(dict.fromkeys(acknowledged.get(cause, cause) for cause in causes))
        for key, causes in withheld.items()
        if key not in available
    }
    return ScopeResolution(
        parents,
        corrected,
        annotated.variables,
        tuple(sorted(evaluations, key=lambda e: e.case_id)),
        tuple(diagnostics),
        counts["error"],
        counts["warning"],
        withheld_dependencies,
        siblings,
        tuple(coverage),
        dict(Counter(issue.code for issue in acknowledged)),
    )


def _attribute_unresolved_names(
    ambiguities: tuple[NamingAmbiguity, ...],
    *,
    originals: tuple[SourceRecord, ...],
    evidence: SourceEvidence,
    groups: Mapping[NativeKey, list[EffectiveOccurrence]],
    occurrences: tuple[EffectiveOccurrence, ...],
    variables: Mapping[NativeKey, ResolvedVariable | None],
    unresolved_keys: frozenset[NativeKey],
    register_fqids: Mapping[NativeKey, str],
    variant_slugs: Mapping[NativeKey, str],
    available: set[DependencyKey],
) -> dict[DependencyKey, tuple[ResolutionDiagnostic, ...]]:
    """Attribute actual unresolved source outcomes, never an absent-reference fallback.

    The diagnostic bridge itself must still match its complete pinned family.
    Stale/broken bridge inputs require conversion repair, not a curation waiver.
    A partially supported name keeps its variable and every supported dependency;
    only exact missing variant/column references in the unresolved rows are named.
    """
    wanted = {a.family.source_key for a in ambiguities}
    if len(wanted) != len(ambiguities):
        raise ValueError("duplicate ambiguous naming family")
    if not wanted:
        return {}
    original_families: dict[NativeKey, list[SourceRecord]] = defaultdict(list)
    for record in originals:
        if (key := native_variable_key(record)) in wanted:
            assert key is not None
            original_families[key].append(record)
    unresolved = {
        key: list(groups[key])
        for key in wanted & unresolved_keys
        if key in variables and variables[key] is None
    }
    for occurrence in occurrences:
        if occurrence.use != "catalog" or "identity" not in occurrence.withheld_fields:
            continue
        for key in {native_variable_key(r) for r in occurrence.source_records} & wanted:
            assert key is not None
            unresolved.setdefault(key, []).append(occurrence)
    withheld: dict[DependencyKey, list[ResolutionDiagnostic]] = defaultdict(list)
    for ambiguity in ambiguities:
        family = ambiguity.family
        key = family.source_key
        records = original_families[key]
        if (
            not records
            or {record_ref(r) for r in records} != {e.ref for e in family.expectations}
            or any(source_register_key(r) != family.register_key for r in records)
            or check_naming_target(family, evidence)
        ):
            raise ValueError(
                f"ambiguous naming bridge is stale or belongs to another family: {key!r}"
            )
        if not unresolved.get(key):
            raise ValueError(
                f"ambiguous naming lacks an unresolved native identity: {key!r}"
            )
        assert family.register_key is not None
        register = register_fqids.get(family.register_key)
        if register is None:
            raise ValueError(f"ambiguous naming lacks a converted register: {key!r}")
        for name in ambiguity.names:
            fqid = f"{register}/{name.slug}"
            candidates = {
                column
                for source_id, column in ambiguity.candidate_columns
                if source_id == name.source_id
            }
            pending = tuple(
                occurrence
                for occurrence in unresolved[key]
                if any(
                    native_variable_key(record) == key
                    and record.fields.column_name is not None
                    and record.fields.column_name.status == "value"
                    and record.fields.column_name.value in candidates
                    for record in occurrence.source_records
                )
            )
            if not pending:
                continue
            affected: dict[DependencyKey, set[SourceRecordRef]] = defaultdict(set)
            for occurrence in pending:
                refs = {record_ref(r) for r in occurrence.evidence}
                if ("variable", fqid) not in available:
                    affected["variable", fqid].update(refs)
                    continue
                variant = (
                    variant_slugs.get(occurrence.variant_key)
                    if occurrence.variant_key is not None
                    else None
                )
                if variant is None:
                    continue
                affected["variant_states", fqid, variant].update(refs)
                column = occurrence.fields.column_name
                if (
                    column is not None
                    and column.status == "value"
                    and isinstance(column.value, str)
                    and column.value
                ):
                    text = column.value
                    for dependency in (
                        ("representation", fqid, text),
                        ("succession_representation", fqid, text.lower()),
                        ("succession_representation", fqid, text.lower(), variant),
                    ):
                        affected[dependency].update(refs)
            for dependency, refs in affected.items():
                if dependency in available:
                    continue
                issue = ResolutionDiagnostic(
                    code="ambiguous_named_identity",
                    severity="error",
                    subject=fqid,
                    detail=f"Accepted name {name.source_id!r} cannot establish ownership within original family {key!r}: {ambiguity.reason} Naming evidence: "
                    + ", ".join(
                        e.entry_id
                        for e in ambiguity.entries
                        if e.entry.source_id == name.source_id
                    ),
                    refs=tuple(sorted(refs, key=repr)),
                    fields=("identity", "column_name"),
                    withheld_output=(repr(dependency),),
                )
                withheld[dependency].append(issue)
    return {key: tuple(causes) for key, causes in withheld.items()}
