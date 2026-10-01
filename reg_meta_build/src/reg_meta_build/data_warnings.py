"""Persist user-facing source limitations after diagnostic acknowledgement settles."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from hashlib import sha256
from typing import TYPE_CHECKING

from reg_meta.catalog import DataWarning
from reg_meta.fqid import Fqid
from reg_meta.source_evidence import canonical_sha256

from reg_meta_build.source_intervals import occurrence_bounds

if TYPE_CHECKING:
    from reg_meta_build.source_curation import ResolutionDiagnostic
    from reg_meta_build.source_scope import ScopeResolution

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
        "missing_data_type",
        "unresolved_data_type",
        "omitted_columnless_occurrence",
    }
)


WARNING_SUMMARIES = {
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
    "missing_data_type": "The stored data type is unavailable",
    "unresolved_data_type": "The stored data type could not be established",
    "omitted_columnless_occurrence": "Some source records cannot be linked to a delivered column",
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
    "missing_data_type": "The supplied evidence does not establish the stored data type for this delivery.",
    "unresolved_data_type": "The source data type could not be resolved safely. No unsupported storage type has been selected.",
    "omitted_columnless_occurrence": "The original source records have no usable physical column identity and are retained without attachment to a delivered column.",
}


def scope_data_warnings(
    result: ScopeResolution,
    *,
    diagnostics: tuple[ResolutionDiagnostic, ...] | None = None,
) -> tuple[DataWarning, ...]:
    """Attach only positively witnessed ownership; ambiguous evidence stays register-wide."""
    owners = defaultdict(set)
    delivery_witnesses = defaultdict(set)
    for occurrence in result.corrections.occurrences:
        variable = result.variables.get(occurrence.variable_key)
        if variable is None:
            continue
        coordinate = (
            variable.register_ref.provider,
            variable.register_ref.slug,
            variable.slug,
        )
        for record in occurrence.evidence:
            for locator in record.locators:
                ref = (record.source, locator.semantic_record_key)
                owners[ref].add(coordinate)
                variant = result.parents.variants.get(occurrence.variant_key)
                column = occurrence.fields.column_name
                if (
                    variant is not None
                    and column is not None
                    and column.status == "value"
                ):
                    for lo, hi in occurrence_bounds(occurrence) or ():
                        delivery_witnesses[ref].add(
                            (variant.slug, column.value, lo, hi)
                        )
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
        candidates = set().union(*(owners[r] for r in refs)) if refs else set()
        owner = (
            next(iter(candidates))
            if len(candidates) == 1 and all(owners[r] for r in refs)
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
                for r in refs
                for v, c, lo, hi in delivery_witnesses[r]
                if lo <= issue_lo and hi >= issue_hi
            }
            if len(witnesses) == 1 and witnesses == observed:
                variant, column = next(iter(witnesses))
        payload = {
            "register_fqid": str(Fqid.register_fqid(*register)),
            "variable_fqid": str(Fqid.binding_fqid(*owner)) if owner else None,
            "variant": variant,
            "delivery_column_name": column,
            "valid_from": issue.valid_from,
            "valid_to": issue.valid_to,
            "code": issue.code,
            "severity": issue.severity,
            "summary": WARNING_SUMMARIES.get(
                issue.code, "The source has an acknowledged data limitation"
            ),
            "detail": reviewed_reasons.get(issue.acknowledged_by)
            or WARNING_DETAILS.get(
                issue.code,
                "An exact reviewed source limitation is acknowledged; the original diagnostic remains in the build report.",
            ),
            "diagnostic_detail_sha256": sha256(
                issue.detail.encode("utf-8")
            ).hexdigest(),
            "source_subject": issue.subject,
            "fields": list(issue.fields),
            "refs": [r.model_dump(mode="json") for r in issue.refs],
            "withheld_output": list(issue.withheld_output),
            "acknowledged_by": issue.acknowledged_by,
            "case_id": issue.case_id,
        }
        warning = DataWarning.model_validate_json(
            json.dumps({"warning_id": canonical_sha256(payload), **payload})
        )
        warnings[warning.warning_id] = warning
    assumption_payloads = {}
    assumption_refs = defaultdict(dict)
    annotated = {
        evaluation.case_id: evaluation.decision
        for evaluation in result.evaluations
        if evaluation.status == "applicable"
        and evaluation.decision is not None
        and getattr(evaluation.decision, "data_warning", None) is not None
    }
    coding_annotations = defaultdict(list)
    for case_id, decision in annotated.items():
        if decision.kind == "coding":
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
                    ("coding",),
                    exact,
                )
            )
        states = tuple(
            state
            for state in variable.states
            if state.variant.slug == native_variant.slug
            and state.delivery_column_name == column.value
        )
        for case_id, summary, detail, fields, exact in applicable:
            for state in states:
                if state.valid_from is None or state.valid_to is None:
                    continue
                for lo, hi in exact:
                    lo = max(lo, date.fromisoformat(state.valid_from).toordinal())
                    hi = min(hi, date.fromisoformat(state.valid_to).toordinal())
                    if lo > hi:
                        continue
                    payload = {
                        "register_fqid": str(
                            Fqid.register_fqid(
                                variable.register_ref.provider,
                                variable.register_ref.slug,
                            )
                        ),
                        "variable_fqid": str(
                            Fqid.binding_fqid(
                                variable.register_ref.provider,
                                variable.register_ref.slug,
                                variable.slug,
                            )
                        ),
                        "variant": state.variant.slug,
                        "delivery_column_name": state.delivery_column_name,
                        "valid_from": date.fromordinal(lo).isoformat(),
                        "valid_to": date.fromordinal(hi).isoformat(),
                        "code": "assumed_storage_type"
                        if "data_type" in fields
                        else "source_identity_assumption"
                        if "identity" in fields
                        else "response_domain_assumption",
                        "severity": "warning",
                        "summary": summary,
                        "detail": detail,
                        "diagnostic_detail_sha256": sha256(
                            detail.encode("utf-8")
                        ).hexdigest(),
                        "source_subject": case_id,
                        "fields": list(fields),
                        "refs": [],
                        "withheld_output": [],
                        "acknowledged_by": None,
                        "case_id": case_id,
                    }
                    token = canonical_sha256(payload)
                    assumption_payloads[token] = payload
                    for record in occurrence.source_records:
                        for locator in record.locators:
                            ref = (record.source, locator.semantic_record_key)
                            assumption_refs[token][ref] = {
                                "source": ref[0],
                                "semantic_record_key": list(ref[1]),
                            }
    for token, payload in sorted(assumption_payloads.items()):
        payload["refs"] = [
            assumption_refs[token][ref] for ref in sorted(assumption_refs[token])
        ]
        warning = DataWarning.model_validate_json(
            json.dumps({"warning_id": canonical_sha256(payload), **payload})
        )
        warnings[warning.warning_id] = warning
    return tuple(warnings[k] for k in sorted(warnings))
