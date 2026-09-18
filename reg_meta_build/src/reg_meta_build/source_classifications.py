"""Resolve an explicitly selected canonical codebook without choosing labels.

The common caller supplies its accepted classification identity and the exact
prepared list membership. Similar codes or labels never establish that binding.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.resolved_catalog import (
    ResolvedClassificationCode,
    ResolvedConformance,
)
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Set as AbstractSet

    from reg_meta_build.resolved_catalog import ResolvedCodeSet
    from reg_meta_build.source_values import SourceValue


@dataclass(frozen=True)
class CanonicalCodeResolution:
    codes: tuple[ResolvedClassificationCode, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]
    source_payloads: int


@dataclass(frozen=True)
class ClassificationConformance:
    conformance: ResolvedConformance
    diagnostics: tuple[ResolutionDiagnostic, ...]


def resolve_classification_conformance(
    code_set: ResolvedCodeSet,
    *,
    classification: str,
    canonical_codes: AbstractSet[str],
    subject: str,
    refs: tuple[SourceRecordRef, ...],
    valid_from: str,
    valid_to: str,
    sentinel_codes: Mapping[str, str] | None = None,
) -> ClassificationConformance:
    """Check an already-declared binding against curated sentinel meanings.

    Literal code strings establish membership; labels remain the source's labels.
    A mismatching code withholds the catalog binding, not the source code list or
    the original declaration. Its exact members remain in conformance evidence.
    A mismatching code on the declared classification's curated `sentinel_codes`
    list (exact code string, with its curated meaning) keeps the catalog binding:
    it stays a variable-local member of the state's value set — never a
    `classification_code` row — and is reported once per state as a warning.
    No match fraction or globally special code token can acknowledge a
    discrepancy: sentinels are per-classification curation, never a global
    waiver. Canonical code sets are indexed once by the caller, not rebuilt
    per state.
    """
    if not canonical_codes:
        raise ValueError("classification conformance requires a nonempty codebook")
    if not refs:
        raise ValueError("classification conformance requires original source refs")
    sentinels = dict(sentinel_codes) if sentinel_codes else {}
    checked = tuple(sorted({code for code, _ in code_set.members}))
    sentinel_members = tuple(pair for pair in code_set.members if pair[0] in sentinels)
    nonconforming = tuple(
        pair
        for pair in code_set.members
        if pair[0] not in canonical_codes and pair[0] not in sentinels
    )
    conformance = ResolvedConformance(
        declared_classification=classification,
        status="severed" if nonconforming else "kept",
        checked_codes=checked,
        nonconforming_members=nonconforming,
        sentinel_members=sentinel_members,
    )
    diagnostics: list[ResolutionDiagnostic] = []
    if nonconforming:
        diagnostics.append(
            ResolutionDiagnostic(
                code="nonconforming_classification_codes",
                severity="error",
                subject=subject,
                detail=f"Declared classification {classification!r} does not contain "
                f"codes {sorted({code for code, _ in nonconforming})!r}. "
                "The original code list and declared binding remain as evidence; "
                "the catalog classification link is withheld pending a bounded decision.",
                refs=refs,
                fields=("coding", "classification"),
                valid_from=valid_from,
                valid_to=valid_to,
                withheld_output=("state.classification",),
            )
        )
    if sentinel_members:
        listed = "; ".join(
            f"{code!r}: {sentinels[code]}"
            for code in sorted({code for code, _ in sentinel_members})
        )
        diagnostics.append(
            ResolutionDiagnostic(
                code="sentinel_classification_codes",
                severity="warning",
                subject=subject,
                detail=f"Declared classification {classification!r} contains curated "
                f"sentinel codes ({listed}). The binding is kept and the codes "
                "stay variable-local members of the state's value set.",
                refs=refs,
                fields=("coding", "classification"),
                valid_from=valid_from,
                valid_to=valid_to,
            )
        )
    return ClassificationConformance(conformance, tuple(diagnostics))


def resolve_canonical_codes(
    values: Iterable[SourceValue], *, source: str, subject: str
) -> CanonicalCodeResolution:
    """Retain agreed codes; withhold only incomplete or contradictory members.

    Duplicate dictionary payloads and physical rows do not choose a label. The
    prepared source retains those occurrences; the caller accounts for list
    associations separately from the distinct payload count returned here.
    """
    payloads = {}
    by_code: dict[str | None, list[SourceValue]] = defaultdict(list)
    for value in values:
        previous = payloads.get(value.payload_key)
        if previous is not None:
            if previous != value:
                raise ValueError(
                    "one canonical source payload key has different content"
                )
            continue
        payloads[value.payload_key] = value
        by_code[value.code].append(value)
    codes = []
    diagnostics = []
    for code, members in sorted(
        by_code.items(), key=lambda item: (item[0] is not None, item[0] or "")
    ):
        labels = {value.label for value in members}
        if not code or None in labels or len(labels) != 1:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="conflicting_classification_labels"
                    if len(labels) > 1
                    else "unknown_classification_member",
                    severity="error",
                    subject=subject,
                    detail=f"Canonical code {code!r} has labels {sorted(labels, key=repr)!r}; no member was selected.",
                    refs=tuple(
                        sorted(
                            {
                                SourceRecordRef(
                                    source=source,
                                    semantic_record_key=locator.semantic_record_key,
                                )
                                for value in members
                                for locator in value.locators
                            },
                            key=repr,
                        )
                    ),
                    fields=("code", "label"),
                    withheld_output=("classification.code",),
                )
            )
            continue
        label = next(iter(labels))
        assert label is not None
        codes.append(
            ResolvedClassificationCode(
                code=code,
                label=label,
                # Existing catalog level means ASCII digit count, not an inferred tree.
                level=len(code) if code.isascii() and code.isdigit() else None,
            )
        )
    if not codes:
        diagnostics.append(
            ResolutionDiagnostic(
                code="empty_canonical_classification",
                severity="error",
                subject=subject,
                detail="The selected source codebook supplies no unambiguous canonical members.",
                fields=("coding",),
                withheld_output=("classification", "classification_bindings"),
            )
        )
    return CanonicalCodeResolution(tuple(codes), tuple(diagnostics), len(payloads))
