"""Persist user-facing source limitations after diagnostic acknowledgement settles."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from hashlib import sha256
from typing import TYPE_CHECKING, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)
from reg_core_py import intersect, merge, parse_fqid

from reg_meta_build.source_curation import CodingDecision, SourceWarningDecision
from reg_meta_build.source_intervals import occurrence_bounds

from .inventory import _edition_intervals, _segment_bounds
from .slug_grammar import validate_slug
from .source_evidence import SourceRecordRef, canonical_sha256

if TYPE_CHECKING:
    from collections.abc import Mapping

    from reg_meta_build.inventory import InventoryTable
    from reg_meta_build.resolved_catalog import ResolvedRegister
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CurationCase, ResolutionDiagnostic
    from reg_meta_build.source_scope import ScopeResolution


class _CatalogModel(BaseModel):
    """Frozen Pydantic base for the catalog return surface (#681): the
    `Resolved*` / `*Summary` / `*Ref` / edition / coverage / group / tag models
    the webapp consumes as response models (collapsing its 1:1 wrappers in a
    follow-up). Frozen preserves the immutability the prior `@dataclass(frozen=True)`
    gave; FQID fields are canonical FQID strings checked with `parse_fqid`.
    Constructed by KEYWORD (Pydantic takes no positional args).

    `populate_by_name` + `extra="forbid"` are hoisted here so the
    `register`-aliasing register-bearing models don't each repeat them, and so a
    typo'd kwarg fails loudly (restoring the prior `@dataclass`'s fail-fast).
    `serialize_by_alias` makes a direct `model_dump()` / `model_dump_json()` emit
    the public wire key (`register`, not the internal `register_name` attr the
    `BaseModel.register`-method clash forced; #681). Harmless on the alias-free
    models."""

    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="forbid",
        serialize_by_alias=True,
    )


class DataWarning(_CatalogModel):
    """Retained source limitation or interpretation assumption, not an editorial notice."""

    warning_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    register_fqid: str
    variable_fqid: str | None = None
    variant: str | None = None
    delivery_column_name: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    code: str
    severity: Literal["warning", "error"]
    summary: str
    detail: str
    diagnostic_detail_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    fields: tuple[str, ...] = ()
    refs: tuple[SourceRecordRef, ...] = ()
    withheld_output: tuple[str, ...] = ()
    acknowledged_by: str | None = None
    case_id: str | None = None

    @model_validator(mode="after")
    def _scope_and_identity(self) -> Self:
        if parse_fqid(self.register_fqid).kind != "register":
            raise ValueError("data warning requires an actual register coordinate")
        if self.variable_fqid is not None and (
            parse_fqid(self.variable_fqid).kind != "variable"
            or self.variable_fqid.rsplit("/", 1)[0] != self.register_fqid
        ):
            raise ValueError("data warning variable must belong to its register")
        if self.variant is not None:
            validate_slug(self.variant, "variant", allow_default=True)
            if self.variable_fqid is None:
                raise ValueError("state-scoped warning requires a written variable")
        if self.delivery_column_name is not None and (
            self.variant is None or not self.delivery_column_name.strip()
        ):
            raise ValueError(
                "a warning column requires an exact variant and nonempty literal"
            )
        for bound in (self.valid_from, self.valid_to):
            if bound is not None and date.fromisoformat(bound).isoformat() != bound:
                raise ValueError("warning bounds must be canonical ISO dates")
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_from > self.valid_to
        ):
            raise ValueError("data warning bounds are reversed")
        if not all(v.strip() for v in (self.code, self.summary, self.detail)):
            raise ValueError("data warning content must be nonempty")
        if self.warning_id != warning_identity(
            self.model_dump(mode="json", exclude={"warning_id"})
        ):
            raise ValueError(
                "data warning identity does not match its complete content"
            )
        return self


def warning_identity(payload: Mapping[str, object]) -> str:
    """The id of a warning's content, its curation pointers aside (#1606).

    `acknowledged_by` and `case_id` locate an entry by its array position, so
    deleting or reordering an unrelated entry would move them. Warnings that
    differ only in those pointers are one warning; see `keep_warning`.
    """
    return canonical_sha256({**payload, "acknowledged_by": None, "case_id": None})


def keep_warning(warnings: dict[str, DataWarning], warning: DataWarning) -> None:
    """Add a warning, merging one that differs only in its pointers.

    The merged warning keeps the first pointers, so input order never decides
    which one the catalog shows. A present pointer comes before a missing one, so
    an acknowledged twin is never dropped for an unacknowledged one.
    """
    kept = warnings.get(warning.warning_id)
    if kept is None or _pointer_order(warning) < _pointer_order(kept):
        warnings[warning.warning_id] = warning


def _pointer_order(
    warning: DataWarning,
) -> tuple[tuple[bool, str, int], tuple[bool, str, int]]:
    return _pointer_key(warning.acknowledged_by), _pointer_key(warning.case_id)


def _pointer_key(pointer: str | None) -> tuple[bool, str, int]:
    # `file#/kind/N` orders by file and kind, then N as a number, so `/9` precedes
    # `/10`; a missing pointer sorts last.
    if pointer is None:
        return True, "", 0
    head, _, position = pointer.rpartition("/")
    return (False, head, int(position)) if position.isdigit() else (False, pointer, -1)


# Editorial projection notices do not describe a problem fetching or using data.
QUALITY_CODES = frozenset(
    {
        "missing_coding_period",
        "nonconforming_classification_codes",
        "sentinel_classification_codes",
        "unsupported_classification_scope",
        "unresolved_classification_reference",
        "unresolved_catalog_identity",
        "unresolved_column_representation",
        "ambiguous_named_identity",
        "conflicting_code_memberships",
        "conflicting_classification_labels",
        "unsupported_coding_scope",
        "item_validity_set_aside",
        "supported_erroneous_coding_association",
        "curated_state_omission",
        "omitted_columnless_occurrence",
        "retained_unattached_source_relationship",
    }
)


# Build-side bookkeeping (maintainer decision, #1296 item 4): the build report keeps
# these, the catalog does not. Every other code is user-facing.
BUILD_ONLY_CODES = frozenset(
    {
        "item_validity_set_aside",
        "source_identity_assumption",
        "omitted_columnless_occurrence",
        "unresolved_catalog_identity",
        "unresolved_native_identity",
        "ambiguous_named_identity",
        "unknown_support_key",
        "retained_unattached_source_relationship",
        "unresolved_source_event_endpoint",
        "supported_erroneous_coding_association",
        "conflicting_occurrence_facts",
        "conflicting_variable_fact",
        "source_metadata_conflict",
        "unresolved_lineage_ambiguous_source_variant",
        # The value-set view lists these codes itself (#1708).
        "sentinel_classification_codes",
        "nonconforming_classification_codes",
        # About a derived sibling group, not the source data (#1708).
        "ambiguous_sibling_shape",
        # Support metadata that binds to no variable, like `unknown_support_key`.
        "ambiguous_support_target",
    }
)

# Diagnostics a researcher reads as one limitation (#1708), by their public code.
# The build keeps the original code: acknowledgements name it.
PUBLIC_CODES = {"no_supported_states": "unsupported_occurrence"}

WARNING_SUMMARIES = {
    "retained_unattached_source_relationship": "A source code crosswalk has no established variable link",
    "unresolved_source_event_endpoint": "A source succession refers to an unavailable catalog endpoint",
    "unknown_support_key": "Some source metadata cannot be linked to a delivered column",
    "unknown_code_membership": "Source codes are retained without established response labels",
    "unresolved_list_reference": "The source response dictionary is unavailable",
    "missing_coding_period": "Response codes are unavailable for this period",
    "nonconforming_classification_codes": "The source includes codes outside its declared classification",
    "sentinel_classification_codes": "The source uses special codes outside the official classification",
    "unsupported_classification_scope": "The classification edition could not be established",
    "unresolved_classification_reference": "The named classification dictionary is unavailable",
    "unresolved_catalog_identity": "The source variable identity could not be established",
    "unresolved_column_representation": "The delivered column interpretation could not be established",
    "ambiguous_named_identity": "The source assigns conflicting meanings to a delivered column",
    "conflicting_code_memberships": "The source supplies conflicting response codes or meanings",
    "conflicting_classification_labels": "The source supplies conflicting classification labels",
    "unsupported_coding_scope": "The response domain applicability could not be established",
    "item_validity_set_aside": "An explicit source link overrides unreliable code validity dates",
    "supported_erroneous_coding_association": "A conflicting source coding association was retained but not applied",
    "curated_state_omission": "An unsupported delivery interpretation has been withheld",
    "omitted_columnless_occurrence": "Some source records cannot be linked to a delivered column",
    "unsupported_occurrence": "A source delivery has no established column or period and is withheld",
    "unresolved_flag": "The variable is withheld because its identifier or sensitivity status is unknown",
    "unsupported_representation_coding": "The source lists disagree about the stored response codes",
    "conflicting_coding_labels": "The source response lists carry different titles",
    "unassigned_original_columns": "Some delivered source columns have no established catalog variable",
    "unresolved_member_list": "The source response list has no enumerable codes",
    "unknown_code_validity": "The validity periods of the source response codes are unknown",
    "unknown_data_type": "The stored data type is not documented",
}


WARNING_DETAILS = {
    "unknown_support_key": "The source metadata is retained, but its link to a physical delivered column has not been established.",
    "unknown_code_membership": "The original source codes are retained; their response labels could not be established.",
    "unresolved_list_reference": "The supplied inputs refer to a response dictionary that is unavailable. No substitute dictionary has been assumed.",
    "missing_coding_period": "The supplied evidence does not establish the stored response codes for this period. Other documented periods retain their known domains.",
    "nonconforming_classification_codes": "The source response domain includes non-standard codes. Source codes remain available; only positively matched dictionary members count as verified classification codes.",
    "sentinel_classification_codes": "The source retains special response codes outside the official classification. They must not be interpreted as official classification members.",
    "unsupported_classification_scope": "The supplied evidence does not establish an applicable classification edition for this source window.",
    "unresolved_classification_reference": "The named classification dictionary cannot be verified from the supplied inputs. Original source domains and declarations are retained.",
    "unresolved_catalog_identity": "The source facts are retained, but the evidence does not establish a unique catalog variable identity.",
    "unresolved_column_representation": "The source facts are retained, but the meaning of this physical delivery column could not be established.",
    "ambiguous_named_identity": "The source assigns different meanings to the same delivered column. No single interpretation has been selected without evidence.",
    "conflicting_code_memberships": "The supplied source lists disagree about response codes or their meanings. Ambiguous coding remains unavailable unless an exact reviewed domain establishes it.",
    "conflicting_classification_labels": "The source supplies conflicting labels for classification codes. Original declarations remain available without an unsupported label correction.",
    "unsupported_coding_scope": "The supplied evidence does not establish when this response domain applies. An unsupported period has not been inferred.",
    "item_validity_set_aside": "Explicit source associations determine the applied response domain despite conflicting global code validity dates. The original dates are retained.",
    "supported_erroneous_coding_association": "An exact reviewed source coding association is retained as evidence but is not applied to the response domain.",
    "curated_state_omission": "An exact reviewed delivery interpretation lacks sufficient support and has been withheld. The original source records remain available.",
    "omitted_columnless_occurrence": "The original source records have no usable physical column identity and are retained without attachment to a delivered column.",
    "unsupported_occurrence": "The source records do not establish a delivered column, period or availability for this delivery. The delivery is withheld, and so is the variable when no other delivery remains; no period has been inferred.",
    "unresolved_flag": "The source does not establish whether this variable identifies a person, or whether it is sensitive; the warning's fields name which. The catalog cannot represent an unknown status, so the variable and the output that depends on it are withheld.",
    "unsupported_representation_coding": "Overlapping source response lists for this column disagree about the stored codes or are incomplete. No single list has been chosen as the stored coding.",
    "conflicting_coding_labels": "The source response lists agree on codes and labels but carry different list titles or version names. The response domain is kept; it has no common title.",
    "unassigned_original_columns": "The reviewed column ownership covers only part of the source variable's original columns. The remaining columns are retained without a catalog variable identity.",
    "unresolved_member_list": "The source refers to a response list for this column but its members cannot be enumerated. No finite response domain has been assumed.",
    "unknown_code_validity": "The source response codes are retained, but the periods in which each code is valid are not documented.",
    "unknown_data_type": "The source does not state how the column is stored. Its documented values do not establish the type, so none has been assumed.",
    "source_availability_limitation": "The source documents how this delivery relates to its editions or periods only in part. The summary states what has been assumed.",
    "response_domain_assumption": "The applied response codes or their meanings rest on a reviewed assumption the source does not fully document. The summary states the assumption.",
    "assumed_storage_type": "The stored data type or value meaning rests on a reviewed assumption the source does not fully document. The summary states the assumption.",
    "reference_period_assumption": "The reference period of this delivery rests on a reviewed assumption the source does not state directly. The summary states the assumption.",
}


def _public_text(code: str, curated: str | None = None) -> tuple[str, str]:
    """The published summary and detail of a user-facing code, never curator
    rationale. An assumption warning's `curated` summary replaces the code's."""
    summary = curated or WARNING_SUMMARIES.get(code)
    detail = WARNING_DETAILS.get(code)
    if summary is None or detail is None:
        raise ValueError(
            f"data warning code {code!r} has no published summary and detail: add "
            "it to WARNING_SUMMARIES and WARNING_DETAILS, or to BUILD_ONLY_CODES"
        )
    return summary, detail


def diagnostic_data_warning(
    issue: ResolutionDiagnostic,
    *,
    register: tuple[str, str],
    owner: tuple[str, str, str] | None = None,
    variant: str | None = None,
    column: str | None = None,
    reason: str | None = None,
) -> DataWarning:
    """Project a settled diagnostic at a positively established catalog coordinate.

    A catalog warning publishes its code's own text; the reviewer's `reason` is
    rationale and reaches only a build-only warning, which stays in the report."""
    code = PUBLIC_CODES.get(issue.code, issue.code)
    if code in BUILD_ONLY_CODES:
        summary = WARNING_SUMMARIES.get(
            code, "The source has an acknowledged data limitation"
        )
        detail = (
            issue.detail
            if code == "retained_unattached_source_relationship"
            else reason
            or WARNING_DETAILS.get(
                code,
                "An exact reviewed source limitation is acknowledged; the original diagnostic remains in the build report.",
            )
        )
    else:
        summary, detail = _public_text(code)
    payload = {
        "register_fqid": str(parse_fqid("/".join(register))),
        "variable_fqid": str(parse_fqid("/".join(owner))) if owner else None,
        "variant": variant,
        "delivery_column_name": column,
        "valid_from": issue.valid_from,
        "valid_to": issue.valid_to,
        "code": code,
        "severity": issue.severity,
        "summary": summary,
        "detail": detail,
        "diagnostic_detail_sha256": sha256(issue.detail.encode("utf-8")).hexdigest(),
        "fields": list(issue.fields),
        "refs": [r.model_dump(mode="json") for r in issue.refs],
        "withheld_output": list(issue.withheld_output),
        "acknowledged_by": issue.acknowledged_by,
        "case_id": issue.case_id,
    }
    return DataWarning.model_validate_json(
        json.dumps({"warning_id": warning_identity(payload), **payload})
    )


# The code of an authored steward mapping warning (#1684); user-facing.
STEWARD_DELIVERY_LIMITATION = "steward_delivery_limitation"


def mapping_data_warnings(
    tables: tuple[InventoryTable, ...],
) -> tuple[DataWarning, ...]:
    """The steward's authored mapping warnings (#1684) at each mapping's
    canonical cell, over the period each serves (clipped to its window).

    `tables` is the canonical inventory (`canonical_inventory`), so the column is
    the representation readers attribute by. One cell's warnings with the same
    texts coalesce where they overlap or abut: an annual table per year, each
    warned, is one warning over the years, as its licensed clone is one state
    (`holdings_compile._licensed_clones`). The curated mapping is itself the
    witness, so a warning cites no source records. A year-independent table's
    warning carries no bounds."""
    # simplify: a warning is keyed by the logical cell, so it reaches every
    # partition shard (and projection) serving that cell. Add a holding key to
    # `data_warning` when one shard needs a caveat its siblings must not show.
    dated: dict[tuple[str, str, str, str, str], list[tuple[str, str]]] = defaultdict(
        list
    )
    independent: set[tuple[str, str, str, str, str]] = set()
    for table in tables:
        edition = _edition_intervals(table)
        for column in table.columns:
            for mapping in column.mappings:
                warning = mapping.data_warning
                if warning is None:
                    continue
                key = (
                    mapping.variable,
                    mapping.register_variant,
                    mapping.representation,
                    warning.summary,
                    warning.detail,
                )
                if table.period_scope == "year_independent":
                    independent.add(key)
                    continue
                window = (
                    _segment_bounds(warning.window)
                    if warning.window is not None
                    else None
                )
                dated[key] += [
                    clipped
                    for segment in mapping.served(edition)
                    if (
                        clipped := segment
                        if window is None
                        else intersect(segment, window)
                    )
                ]
    rows = [(key, (None, None)) for key in independent] + [
        (key, bounds) for key, spans in dated.items() for bounds in merge(spans)
    ]
    warnings: dict[str, DataWarning] = {}
    for (variable, variant, column, summary, detail), (lo, hi) in rows:
        payload = {
            "register_fqid": variable.rsplit("/", 1)[0],
            "variable_fqid": variable,
            "variant": variant.rsplit("/", 1)[1],
            "delivery_column_name": column,
            "valid_from": lo,
            "valid_to": hi,
            "code": STEWARD_DELIVERY_LIMITATION,
            "severity": "warning",
            "summary": summary,
            "detail": detail,
            "diagnostic_detail_sha256": sha256(detail.encode("utf-8")).hexdigest(),
            "fields": [],
            "refs": [],
            "withheld_output": [],
            "acknowledged_by": None,
            "case_id": None,
        }
        keep_warning(
            warnings,
            DataWarning.model_validate_json(
                json.dumps({"warning_id": warning_identity(payload), **payload})
            ),
        )
    return tuple(warnings[k] for k in sorted(warnings))


def acknowledged_data_warnings(
    diagnostics: tuple[ResolutionDiagnostic, ...],
    cases: tuple[CurationCase, ...],
    registers: Mapping[NativeKey, ResolvedRegister],
) -> tuple[DataWarning, ...]:
    """Project guarded global limitations at their observed owner registers."""
    reviewed = {case.case_id: case.decision for case in cases}
    warnings = {}
    for issue in diagnostics:
        if issue.acknowledged_by is None:
            continue
        decision = reviewed[issue.acknowledged_by]
        assert decision.kind == "acknowledge"
        register = registers[decision.register_key]
        warning = diagnostic_data_warning(
            issue,
            register=(register.provider, register.slug),
            reason=decision.reason,
        )
        keep_warning(warnings, warning)
    return tuple(warnings[k] for k in sorted(warnings))


def scope_data_warnings(
    result: ScopeResolution,
    *,
    diagnostics: tuple[ResolutionDiagnostic, ...] | None = None,
) -> tuple[DataWarning, ...]:
    """Attach only positively witnessed ownership; ambiguous evidence stays register-wide."""
    owners = defaultdict(set)
    delivery_witnesses = defaultdict(set)
    # A curated addition delivers no source record. Its evidence only supports
    # the edition it joins, and an accepted edition's support is the whole
    # variant slice, so it claims an issue only when the issue cites all of that
    # support, as the addition's own issues do (#1628). Grouped by support set
    # (one accepted edition's additions share it) under its least ref, which any
    # such issue cites.
    additions = defaultdict(lambda: defaultdict(lambda: (set(), set())))
    for occurrence in result.corrections.occurrences:
        variable = result.variables.get(occurrence.variable_key)
        if variable is None:
            continue
        coordinate = (
            variable.register_ref.provider,
            variable.register_ref.slug,
            variable.slug,
        )
        variant = result.parents.variants.get(occurrence.variant_key)
        column = occurrence.fields.column_name
        witnesses = (
            {
                (variant.slug, column.value, lo, hi)
                for lo, hi in occurrence_bounds(occurrence) or ()
            }
            if variant is not None and column is not None and column.status == "value"
            else set()
        )
        refs = {
            (record.source, locator.semantic_record_key)
            for record in occurrence.evidence
            for locator in record.locators
        }
        if not occurrence.source_records:
            if refs:
                coordinates, witnessed = additions[min(refs)][frozenset(refs)]
                coordinates.add(coordinate)
                witnessed |= witnesses
            continue
        for ref in refs:
            owners[ref].add(coordinate)
            delivery_witnesses[ref] |= witnesses
    registers = {(r.provider, r.slug) for r in result.parents.registers.values()}
    registers.update(
        (v.register_ref.provider, v.register_ref.slug)
        for v in result.variables.values()
        if v is not None
    )
    reviewed_reasons = {
        evaluation.case_id: evaluation.decision.reason
        for evaluation in result.evaluations
        if evaluation.status == "applicable"
        and evaluation.decision is not None
        and evaluation.decision.kind == "acknowledge"
    }
    warnings = {}
    for issue in result.diagnostics if diagnostics is None else diagnostics:
        if issue.acknowledged_by is None and issue.code not in QUALITY_CODES:
            continue
        refs = {(r.source, r.semantic_record_key) for r in issue.refs}
        claimed = [
            (support, *claim)
            for ref in refs
            if ref in additions
            for support, claim in additions[ref].items()
            if support <= refs
        ]
        covered = set().union(*(support for support, _, _ in claimed))
        candidates = set().union(
            *(owners[r] for r in refs), *(coordinates for _, coordinates, _ in claimed)
        )
        owner = (
            next(iter(candidates))
            if len(candidates) == 1 and all(owners[r] or r in covered for r in refs)
            else None
        )
        register = (
            owner[:2]
            if owner
            else next(iter(registers))
            if len(registers) == 1
            else None
        )
        if register is None:
            # No actual resolved register means there is no safe catalog coordinate.
            continue
        variant = column = None
        if owner and issue.valid_from is not None and issue.valid_to is not None:
            variable = next(
                v
                for v in result.variables.values()
                if v is not None
                and (v.register_ref.provider, v.register_ref.slug, v.slug) == owner
            )
            witnesses = {
                (s.variant.slug, s.delivery_column_name)
                for s in variable.states
                if s.valid_from is not None
                and s.valid_to is not None
                and s.valid_from <= issue.valid_to
                and s.valid_to >= issue.valid_from
            }
            issue_lo = date.fromisoformat(issue.valid_from).toordinal()
            issue_hi = date.fromisoformat(issue.valid_to).toordinal()
            observed = {
                (v, c)
                for v, c, lo, hi in set().union(
                    *(delivery_witnesses[r] for r in refs),
                    *(witnessed for _, _, witnessed in claimed),
                )
                if lo <= issue_lo and hi >= issue_hi
            }
            if len(witnesses) == 1 and witnesses == observed:
                variant, column = next(iter(witnesses))
        warning = diagnostic_data_warning(
            issue,
            register=register,
            owner=owner,
            variant=variant,
            column=column,
            reason=reviewed_reasons.get(issue.acknowledged_by),
        )
        keep_warning(warnings, warning)
    # Equal assumption warnings of one cell over all its occurrences and
    # states: each span with the refs of the occurrence it came from (#1708).
    assumption_payloads = {}
    assumption_spans = defaultdict(list)
    annotated = {
        evaluation.case_id: evaluation.decision
        for evaluation in result.evaluations
        if evaluation.status == "applicable"
        and evaluation.decision is not None
        and getattr(evaluation.decision, "data_warning", None) is not None
    }
    coding_annotations = defaultdict(list)
    for case_id, decision in annotated.items():
        if isinstance(decision, (CodingDecision, SourceWarningDecision)):
            coding_annotations[decision.column_key].append((case_id, decision))
    for occurrence in result.corrections.occurrences:
        variable = result.variables.get(occurrence.variable_key)
        if variable is None:
            continue
        column = occurrence.fields.column_name
        native_variant = result.parents.variants.get(occurrence.variant_key)
        if column is None or column.status != "value" or native_variant is None:
            continue
        bounds = occurrence_bounds(occurrence)
        if not bounds:
            continue
        applicable = []
        member_refs = {
            (record.source, locator.semantic_record_key)
            for record in occurrence.evidence
            for locator in record.locators
        }
        for correction in occurrence.corrections:
            decision = annotated.get(correction.case_id)
            if decision is None or decision.kind != "correct_occurrences":
                continue
            refs = {
                (ref.source, ref.semantic_record_key)
                for ref in decision.data_warning_refs
            }
            if refs and not member_refs.intersection(refs):
                continue
            applicable.append(
                (
                    correction.case_id,
                    decision.data_warning,
                    decision.reason + "\n" + decision.provenance,
                    decision.data_warning_fields,
                    bounds,
                )
            )
        for case_id, decision in coding_annotations.get(occurrence.column_key, ()):
            lower = date.fromisoformat(decision.valid_from).toordinal()
            upper = date.fromisoformat(decision.valid_to).toordinal()
            exact = tuple(
                (max(lo, lower), min(hi, upper))
                for lo, hi in bounds
                if max(lo, lower) <= min(hi, upper)
            )
            applicable.append(
                (
                    case_id,
                    decision.data_warning,
                    decision.reason + "\n" + decision.provenance,
                    decision.fields
                    if decision.kind == "source_warning"
                    else ("coding",),
                    exact,
                )
            )
        states = tuple(
            state
            for state in variable.states
            if state.variant.slug == native_variant.slug
            and state.delivery_column_name == column.value
        )
        for case_id, summary, rationale, fields, exact in applicable:
            code = (
                "reference_period_assumption"
                if "reference_period" in fields
                else "source_metadata_conflict"
                if annotated[case_id].kind == "source_warning"
                else "assumed_storage_type"
                if "data_type" in fields
                else "source_identity_assumption"
                if "identity" in fields
                else "source_availability_limitation"
                if "availability" in fields
                else "response_domain_assumption"
            )
            # The rationale stays with a build-only warning, in the report.
            summary, detail = (
                (summary, rationale)
                if code in BUILD_ONLY_CODES
                else _public_text(code, summary)
            )
            payload = {
                "register_fqid": str(
                    parse_fqid(
                        f"{variable.register_ref.provider}/{variable.register_ref.slug}"
                    )
                ),
                "variable_fqid": str(
                    parse_fqid(
                        f"{variable.register_ref.provider}/"
                        f"{variable.register_ref.slug}/{variable.slug}"
                    )
                ),
                "variant": native_variant.slug,
                "delivery_column_name": column.value,
                "code": code,
                "severity": "warning",
                "summary": summary,
                "detail": detail,
                "diagnostic_detail_sha256": sha256(detail.encode("utf-8")).hexdigest(),
                "fields": list(fields),
                "withheld_output": [],
                "acknowledged_by": None,
            }
            token = canonical_sha256(payload)
            assumption_payloads[token] = payload
            refs = frozenset(
                (record.source, locator.semantic_record_key)
                for record in occurrence.source_records
                for locator in record.locators
            )
            for state in states:
                if state.valid_from is None or state.valid_to is None:
                    continue
                for lo, hi in exact:
                    lo = max(lo, date.fromisoformat(state.valid_from).toordinal())
                    hi = min(hi, date.fromisoformat(state.valid_to).toordinal())
                    if lo <= hi:
                        assumption_spans[token].append(
                            (
                                date.fromordinal(lo).isoformat(),
                                date.fromordinal(hi).isoformat(),
                                case_id,
                                refs,
                            )
                        )
    for token, spans in sorted(assumption_spans.items()):
        # One warning per contiguous window; a gap between windows stays a gap.
        for lo, hi in merge([(f, t) for f, t, _, _ in spans]):
            within = [s for s in spans if s[0] <= hi and s[1] >= lo]
            payload = {
                **assumption_payloads[token],
                "valid_from": lo,
                "valid_to": hi,
                "refs": [
                    {"source": ref[0], "semantic_record_key": list(ref[1])}
                    for ref in sorted(set().union(*(s[3] for s in within)))
                ],
                "case_id": min((s[2] for s in within), key=_pointer_key),
            }
            warning = DataWarning.model_validate_json(
                json.dumps({"warning_id": warning_identity(payload), **payload})
            )
            keep_warning(warnings, warning)
    return tuple(warnings[k] for k in sorted(warnings))
