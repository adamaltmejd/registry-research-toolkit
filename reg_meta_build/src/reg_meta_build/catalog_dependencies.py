"""Resolve catalog dependencies against explicit source-resolution outcomes.

Missing conversion or broken references remain fatal. Only a dependency already
withheld with source evidence can withhold dependent metadata. The same result is
used for strict publication and diagnostic materialization. The same rule guards
delivery: supported source coverage that no explicit outcome withholds must still
be represented by the variables about to be written.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING, Literal

from reg_meta_build._components import DisjointSet
from reg_meta_build._resolved_common import _ResolvedWindow, remaining_windows
from reg_meta_build.concept_groups import (
    _MONTH_LABELS,
    classification_succession_edges,
    month_group_candidates,
)
from reg_meta_build.relations import (
    _REPLACED_BY_NOTE_VINTAGE_LIFT,
    variable_vintage_succession_edges,
)
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedCodeSet,
    ResolvedEdition,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    _prepare_classification_succession,
    validate_resolved_variables,
)
from reg_meta_build.resolved_metadata import (
    ResolvedGroupAxis,
    ResolvedGroupFacet,
    ResolvedGroupVariable,
    ResolvedMetadata,
    ResolvedStateRef,
    ResolvedSuccession,
    ResolvedVariableGroup,
    validate_metadata_structure,
)
from reg_meta_build.source_curation import ResolutionDiagnostic

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Iterator, Mapping

    from reg_meta_build.concept_groups import CodeLabelPair
    from reg_meta_build.source_curation import SourceRecordRef


type DependencyKey = tuple[str, ...]

# A register-scoped build's one warning for a reference whose other end is
# declared only by unselected scopes. Only the complete build can resolve it.
DEFERRED_REFERENCE = "deferred_out_of_slice_reference"


def resolve_classification_successions(
    classifications: tuple[ResolvedClassification, ...],
    declared: tuple[ResolvedClassificationSuccession, ...] = (),
) -> tuple[ResolvedClassificationSuccession, ...]:
    """Combine existing automatic edition chains and explicit accepted edges.

    Check the whole graph before materialization. An explicit edge cannot hide a
    duplicate, missing endpoint or cycle by being checked in a separate pass.
    """
    if len({c.slug for c in classifications}) != len(classifications):
        raise ValueError("duplicate classification identity in succession resolution")
    derived = tuple(
        ResolvedClassificationSuccession(
            predecessor=a,
            successor=b,
            effective_year=year,
            note="derived:vintage_chain",
        )
        for a, b, year in classification_succession_edges(
            (c.slug, c.name) for c in classifications
        )
    )
    combined = tuple(
        sorted((*derived, *declared), key=lambda e: (e.predecessor, e.successor))
    )
    _prepare_classification_succession(classifications, combined)
    return combined


def resolve_variable_successions(
    metadata: ResolvedMetadata,
    variables: tuple[ResolvedVariable, ...],
    classifications: tuple[ResolvedClassificationSuccession, ...],
) -> ResolvedMetadata:
    """Add existing classification-derived variable links before writing the DB.

    Explicit source/curated edges retain their richer attribution. Validate the
    combined graph so a derived edge cannot close a cycle through those edges.
    """
    edges = variable_vintage_succession_edges(
        (
            (
                f"{v.register_ref.provider}/{v.register_ref.slug}",
                v.name,
                v.slug,
                link.classification,
            )
            for v in variables
            for state in v.states
            for link in state.classification_links
            if v.name is not None
        ),
        ((e.predecessor, e.successor, e.effective_year) for e in classifications),
    )
    existing = {(e.predecessor, e.successor) for e in metadata.successions}
    combined = metadata.model_copy(
        update={
            "successions": metadata.successions
            + tuple(
                ResolvedSuccession(
                    predecessor=a,
                    successor=b,
                    effective_year=year,
                    note=_REPLACED_BY_NOTE_VINTAGE_LIFT,
                )
                for a, b, year in edges
                if (a, b) not in existing
            )
        }
    )
    validate_metadata_structure(combined)
    return combined


def _variable_fqid(variable: ResolvedVariable) -> str:
    register = variable.register_ref
    return f"{register.provider}/{register.slug}/{variable.slug}"


def _state_dependency_key(
    fqid: str, variant: str, state: ResolvedState | ResolvedStateRef
) -> DependencyKey:
    if state.period_scope == "year_independent":
        return (
            "independent_state",
            fqid,
            variant,
            state.delivery_column_name,
            state.value_set_version_label,
        )
    assert state.valid_from is not None and state.valid_to is not None
    return (
        "state",
        fqid,
        variant,
        state.valid_from,
        state.valid_to,
        state.delivery_column_name,
        state.value_set_version_label,
    )


def variable_dependency_keys(variable: ResolvedVariable) -> set[DependencyKey]:
    """Exact materialized references, shared by resolution and dependency checks."""
    fqid = _variable_fqid(variable)
    keys: set[DependencyKey] = {("variable", fqid)}
    for item in (*variable.states, *variable.aliases):
        column = item.delivery_column_name
        keys.update(
            (
                ("representation", fqid, column),
                ("succession_representation", fqid, column.lower()),
                ("succession_representation", fqid, column.lower(), item.variant.slug),
            )
        )
    for item in variable.states:
        keys.add(("variant_states", fqid, item.variant.slug))
        keys.add(_state_dependency_key(fqid, item.variant.slug, item))
    return keys


@dataclass(frozen=True)
class CoverageObligation:
    """One effective positive source claim the final catalog must still deliver.

    Already reduced by the explicit outcomes that withhold part of the claim, so
    an obligation left here has no accepted excuse for going missing. The claimed
    delivery facts travel with the window: exact type/length claims and the member
    correction attributions the written state must still contain. Each fact claim
    is tri-state: ('value', text) the written state must equal, ('negative',
    None) asserting the source leaves the fact absent so the written state must
    be None, or None for no claim, which is never compared.
    """

    fqid: str
    variant: str
    column: str
    valid_from: str | None
    valid_to: str | None
    refs: tuple[SourceRecordRef, ...]
    data_type_claim: tuple[str, str | None] | None = None
    data_length_claim: tuple[str, str | None] | None = None
    attributions: tuple[str, ...] = ()
    coding_claim: tuple[ResolvedCodeSet | None, str | None] | None = None
    column_text_claim: tuple[str | None, str | None] | None = None
    name_claim: tuple[str, str | None] | None = None
    description_claim: tuple[str, str | None] | None = None
    definition_claim: tuple[str, str | None] | None = None
    measurement_unit_claim: tuple[str, str | None] | None = None
    period_scope: Literal["intervals", "year_independent"] = "intervals"

    def __post_init__(self) -> None:
        if self.period_scope == "year_independent":
            if self.valid_from is not None or self.valid_to is not None:
                raise ValueError("independent delivery obligation cannot carry dates")
        elif self.period_scope == "intervals":
            if self.valid_from is None or self.valid_to is None:
                raise ValueError("dated delivery obligation requires both bounds")
            _ResolvedWindow(valid_from=self.valid_from, valid_to=self.valid_to)
        else:
            raise ValueError("unsupported delivery obligation scope")


def _alias_overlap(
    variables: Iterable[ResolvedVariable], obligation: CoverageObligation
) -> list[tuple[str, str]]:
    """The obligation window slices an alias window on its coordinate delivers."""
    cover = []
    if obligation.period_scope == "year_independent":
        return cover
    assert obligation.valid_from is not None and obligation.valid_to is not None
    for variable in variables:
        for alias in variable.aliases:
            if (
                alias.variant.slug != obligation.variant
                or alias.delivery_column_name != obligation.column
            ):
                continue
            for window in alias.windows:
                lower = max(window.valid_from, obligation.valid_from)
                upper = min(window.valid_to, obligation.valid_to)
                if lower <= upper:
                    cover.append((lower, upper))
    return list(dict.fromkeys(cover))


def _ambiguous_backing_slices(
    clipped: list[tuple[str, str]],
) -> list[tuple[str, str, int]]:
    """Maximal slices where more than one backing window overlaps, with the count."""
    cuts = sorted(
        {
            point
            for start, end in clipped
            for point in (
                date.fromisoformat(start).toordinal(),
                date.fromisoformat(end).toordinal() + 1,
            )
        }
    )
    runs: list[tuple[int, int, int]] = []
    for low, high in pairwise(cuts):
        count = sum(
            1
            for start, end in clipped
            if date.fromisoformat(start).toordinal() <= low
            and date.fromisoformat(end).toordinal() >= high - 1
        )
        if count < 2:
            continue
        if runs and runs[-1][1] + 1 == low and runs[-1][2] == count:
            runs[-1] = (runs[-1][0], high - 1, count)
        else:
            runs.append((low, high - 1, count))
    return [
        (date.fromordinal(first).isoformat(), date.fromordinal(last).isoformat(), n)
        for first, last, n in runs
    ]


def check_delivery_coverage(
    variables: Iterable[ResolvedVariable],
    obligations: Iterable[CoverageObligation],
    *,
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
    diagnostic: bool = False,
) -> tuple[ResolutionDiagnostic, ...]:
    """Refuse silent loss of supported delivery coverage before the catalog is placed.

    Strict mode raises on the first unexplained kind found. Diagnostic mode
    returns one error diagnostic per obligation and kind instead, so the build
    can report every problem and continue.

    Delivery is established by a final state or a declared representation window
    on the same resolved variable, variant and physical column; a search alias
    without windows, another column or another variant delivers nothing here.
    Periods are never widened: only the claimed window has to be represented.
    Every overlapping final state on the same coordinate, and every shared state
    behind an alias window for that coordinate, must also keep each claimed fact
    and contain every claimed attribution as an exact provenance element. Each
    fact travels as tri-state: a value claim the written state must equal, a
    negative claim the written state must leave absent, or no claim, which is
    never compared. A conflicting representation fact clears the claim to none.
    A variable or one of its variants that the ledger withholds outright, with
    source evidence, owes nothing at that exact coordinate; the claim stays a
    curation blocker whichever stage recorded it, and a sibling stays checked.
    """
    delivered: dict[tuple[str, str, str], list[tuple[str, str]]] = defaultdict(list)
    by_fqid: dict[str, list[ResolvedVariable]] = defaultdict(list)
    for variable in variables:
        fqid = _variable_fqid(variable)
        by_fqid[fqid].append(variable)
        for state in variable.states:
            if state.period_scope == "year_independent":
                continue
            assert state.valid_from is not None and state.valid_to is not None
            delivered[fqid, state.variant.slug, state.delivery_column_name].append(
                (state.valid_from, state.valid_to)
            )
        for alias in variable.aliases:
            delivered[fqid, alias.variant.slug, alias.delivery_column_name].extend(
                (window.valid_from, window.valid_to) for window in alias.windows
            )
    losses: list[str] = []
    fact_changes: list[str] = []
    notes: list[tuple[CoverageObligation, list[str], list[str]]] = []
    for obligation in obligations:
        if any(
            key in withheld
            for key in (
                ("variable", obligation.fqid),
                ("variant_states", obligation.fqid, obligation.variant),
            )
        ):
            continue
        ob_losses: list[str] = []
        ob_facts: list[str] = []
        key = (obligation.fqid, obligation.variant, obligation.column)
        scope_label = (
            "year_independent"
            if obligation.period_scope == "year_independent"
            else f"{obligation.valid_from}..{obligation.valid_to}"
        )
        refs = ", ".join(
            "/".join((ref.source, *ref.semantic_record_key)) for ref in obligation.refs
        )
        if obligation.period_scope == "year_independent":
            if not any(
                state.period_scope == "year_independent"
                and state.variant.slug == obligation.variant
                and state.delivery_column_name == obligation.column
                for variable in by_fqid.get(obligation.fqid, ())
                for state in variable.states
            ):
                ob_losses.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} year_independent delivery claimed by {refs} is missing"
                )
        else:
            assert obligation.valid_from is not None and obligation.valid_to is not None
            for start, end in remaining_windows(
                delivered.get(key, ()), obligation.valid_from, obligation.valid_to
            ):
                ob_losses.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} {start}..{end} claimed by {refs}"
                )
        claimed_type = obligation.data_type_claim
        claimed_length = obligation.data_length_claim
        claimed_attributions = obligation.attributions
        check_type = claimed_type is not None
        expected_type: str | None = None
        type_claim_label = "None"
        if claimed_type is not None:
            if claimed_type[0] == "value":
                expected_type = claimed_type[1]
                type_claim_label = repr(claimed_type[1])
            else:
                type_claim_label = "None (negative source claim)"
        check_length = claimed_length is not None
        expected_length: str | None = None
        length_claim_label = "None"
        if claimed_length is not None:
            if claimed_length[0] == "value":
                expected_length = claimed_length[1]
                length_claim_label = repr(claimed_length[1])
            else:
                length_claim_label = "None (negative source claim)"
        alias_cover = _alias_overlap(by_fqid.get(obligation.fqid, ()), obligation)
        if alias_cover:
            backing: dict[tuple[str, str, str, str], ResolvedState] = {}
            for variable in by_fqid.get(obligation.fqid, ()):
                for state in variable.states:
                    if (
                        state.variant.slug != obligation.variant
                        or state.period_scope != "intervals"
                    ):
                        continue
                    assert state.valid_from is not None and state.valid_to is not None
                    if any(
                        state.valid_from <= end and state.valid_to >= start
                        for start, end in alias_cover
                    ):
                        backing[
                            state.variant.slug,
                            state.delivery_column_name,
                            state.valid_from,
                            state.valid_to,
                        ] = state
            for start, end in alias_cover:
                clipped = sorted(
                    (max(state.valid_from, start), min(state.valid_to, end))
                    for state in backing.values()
                    if state.valid_from is not None
                    and state.valid_to is not None
                    and state.valid_from <= end
                    and state.valid_to >= start
                )
                for gap_start, gap_end in remaining_windows(clipped, start, end):
                    ob_facts.append(
                        f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                        f"{scope_label} claimed by {refs}: "
                        f"no written state carries the claimed facts for {gap_start}..{gap_end}"
                    )
                for first, last, count in _ambiguous_backing_slices(clipped):
                    ob_facts.append(
                        f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                        f"{scope_label} claimed by {refs}: "
                        f"alias backing is ambiguous: {count} states of variant "
                        f"{obligation.variant} overlap {first}..{last}"
                    )
        if (
            not check_type
            and not check_length
            and not claimed_attributions
            and obligation.coding_claim is None
            and obligation.column_text_claim is None
            and obligation.name_claim is None
            and obligation.description_claim is None
            and obligation.definition_claim is None
            and obligation.measurement_unit_claim is None
            and not alias_cover
        ):
            losses.extend(ob_losses)
            notes.append((obligation, ob_facts, ob_losses))
            continue
        candidates: dict[tuple[str, str, str | None, str | None], ResolvedState] = {}
        for variable in by_fqid.get(obligation.fqid, ()):
            for state in variable.states:
                if (
                    state.variant.slug != obligation.variant
                    or state.period_scope != obligation.period_scope
                ):
                    continue
                if state.period_scope == "year_independent":
                    if state.delivery_column_name == obligation.column:
                        candidates[
                            (state.variant.slug, state.delivery_column_name, None, None)
                        ] = state
                    continue
                assert state.valid_from is not None and state.valid_to is not None
                assert (
                    obligation.valid_from is not None
                    and obligation.valid_to is not None
                )
                direct = state.delivery_column_name == obligation.column
                behind_alias = False
                if not direct:
                    for alias in variable.aliases:
                        if (
                            alias.variant.slug != obligation.variant
                            or alias.delivery_column_name != obligation.column
                        ):
                            continue
                        for window in alias.windows:
                            if (
                                window.valid_from <= obligation.valid_to
                                and window.valid_to >= obligation.valid_from
                                and window.valid_from <= state.valid_to
                                and window.valid_to >= state.valid_from
                            ):
                                lower = max(window.valid_from, obligation.valid_from)
                                upper = min(window.valid_to, obligation.valid_to)
                                if (
                                    state.valid_from <= upper
                                    and state.valid_to >= lower
                                ):
                                    behind_alias = True
                                    break
                        if behind_alias:
                            break
                if not (direct or behind_alias):
                    continue
                if not (
                    state.valid_from <= obligation.valid_to
                    and state.valid_to >= obligation.valid_from
                ):
                    continue
                metadata_windows = [
                    window.model_copy(
                        update={
                            "valid_from": max(state.valid_from, window.valid_from),
                            "valid_to": min(state.valid_to, window.valid_to),
                        }
                    )
                    for alias in variable.aliases
                    if alias.variant.slug == obligation.variant
                    and alias.delivery_column_name == obligation.column
                    for window in alias.windows
                    if (
                        window.column_metadata == "per_column"
                        or window.coding_metadata == "per_column"
                    )
                    and state.valid_from <= window.valid_to
                    and window.valid_from <= state.valid_to
                    and window.valid_from <= obligation.valid_to
                    and window.valid_to >= obligation.valid_from
                ]
                for window in metadata_windows:
                    if window.coding_metadata == "per_column" and (
                        window.value_set is None
                        or not window.value_set.members
                        or state.value_set is not None
                        or state.value_set_version_label
                        or state.classification_links
                    ):
                        ob_facts.append(
                            f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                            f"{scope_label} claimed by {refs}: invalid per-column coding override or classified backing"
                        )
                projected = [
                    state.model_copy(
                        update={
                            "delivery_column_name": obligation.column,
                            "valid_from": window.valid_from,
                            "valid_to": window.valid_to,
                            **(
                                {
                                    "name": window.name,
                                    "description": window.description,
                                    "definition": window.definition,
                                    "measurement_unit": window.measurement_unit,
                                    "data_type": window.data_type,
                                    "data_length": window.data_length,
                                    "operational_definition": window.operational_definition,
                                    "source_register_text": window.source_register_text,
                                }
                                if window.column_metadata == "per_column"
                                else {}
                            ),
                            **(
                                {
                                    "value_set": window.value_set,
                                    "value_set_version_label": window.value_set_version_label,
                                }
                                if window.coding_metadata == "per_column"
                                else {}
                            ),
                        }
                    )
                    for window in metadata_windows
                ]
                if metadata_windows:
                    if direct:
                        projected.extend(
                            state.model_copy(
                                update={"valid_from": start, "valid_to": end}
                            )
                            for start, end in remaining_windows(
                                ((w.valid_from, w.valid_to) for w in metadata_windows),
                                max(state.valid_from, obligation.valid_from),
                                min(state.valid_to, obligation.valid_to),
                            )
                        )
                else:
                    projected.append(state)
                for candidate in projected:
                    token = (
                        candidate.variant.slug,
                        candidate.delivery_column_name,
                        candidate.valid_from,
                        candidate.valid_to,
                    )
                    candidates.setdefault(token, candidate)
        for state in candidates.values():
            for field in ("name", "description"):
                claim = getattr(obligation, field + "_claim")
                if claim is not None and getattr(state, field) != claim[1]:
                    ob_facts.append(
                        f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                        f"{scope_label} claimed by {refs}: literal delivery {field} changed"
                    )
            if (
                obligation.measurement_unit_claim is not None
                and state.measurement_unit != obligation.measurement_unit_claim[1]
            ):
                ob_facts.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                    f"{scope_label} claimed by {refs}: literal delivery unit changed"
                )
            if (
                obligation.definition_claim is not None
                and state.definition != obligation.definition_claim[1]
            ):
                ob_facts.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} {scope_label} claimed by {refs}: literal definition changed"
                )
            if (
                obligation.coding_claim is not None
                and (state.value_set, state.value_set_version_label)
                != obligation.coding_claim
            ):
                ob_facts.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                    f"{scope_label} claimed by {refs}: claimed coding={obligation.coding_claim!r} "
                    f"written coding={(state.value_set, state.value_set_version_label)!r}"
                )
            if (
                obligation.column_text_claim is not None
                and (state.operational_definition, state.source_register_text)
                != obligation.column_text_claim
            ):
                ob_facts.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                    f"{scope_label} claimed by {refs}: column operation/source attribution changed"
                )
            if check_type and state.data_type != expected_type:
                ob_facts.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                    f"{scope_label} claimed by {refs}: "
                    f"claimed data_type={type_claim_label} written {state.data_type!r}"
                )
            if check_length and state.data_length != expected_length:
                ob_facts.append(
                    f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                    f"{scope_label} claimed by {refs}: "
                    f"claimed data_length={length_claim_label} written {state.data_length!r}"
                )
            if claimed_attributions:
                elements = (
                    set(state.provenance.split("\n\n")) if state.provenance else set()
                )
                missing = [
                    item for item in claimed_attributions if item not in elements
                ]
                if missing:
                    ob_facts.append(
                        f"{obligation.fqid} {obligation.variant}/{obligation.column} "
                        f"{scope_label} claimed by {refs}: "
                        f"claimed attributions={tuple(missing)!r} written provenance={state.provenance!r}"
                    )
        losses.extend(ob_losses)
        fact_changes.extend(ob_facts)
        notes.append((obligation, ob_facts, ob_losses))
    if diagnostic:
        found: list[ResolutionDiagnostic] = []
        for obligation, ob_facts, ob_losses in notes:
            if not (ob_facts or ob_losses):
                continue
            subject = f"{obligation.fqid} {obligation.variant}/{obligation.column} " + (
                "year_independent"
                if obligation.period_scope == "year_independent"
                else f"{obligation.valid_from}..{obligation.valid_to}"
            )
            if ob_facts:
                found.append(
                    ResolutionDiagnostic(
                        code="unexplained_delivery_fact_change",
                        severity="error",
                        subject=subject,
                        detail=(
                            "supported delivery facts changed without an explicit "
                            f"source outcome ({len(ob_facts)} fact(s)): "
                            + "; ".join(ob_facts)
                        ),
                        refs=obligation.refs,
                        withheld_output=(obligation.fqid,),
                    )
                )
            if ob_losses:
                found.append(
                    ResolutionDiagnostic(
                        code="unexplained_delivery_coverage_loss",
                        severity="error",
                        subject=subject,
                        detail=(
                            "supported delivery coverage was lost without an explicit "
                            f"source outcome ({len(ob_losses)} window(s)): "
                            + "; ".join(ob_losses)
                        ),
                        refs=obligation.refs,
                        withheld_output=(obligation.fqid,),
                    )
                )
        return tuple(found)
    if fact_changes:
        raise ValueError(
            "supported delivery facts changed without an explicit source outcome "
            f"({len(fact_changes)} fact(s)): " + "; ".join(fact_changes[:10])
        )
    if losses:
        raise ValueError(
            "supported delivery coverage was lost without an explicit source outcome "
            f"({len(losses)} window(s)): " + "; ".join(losses[:10])
        )
    return ()


@dataclass(frozen=True)
class MissingCatalogDependency:
    key: DependencyKey
    output: str


class CatalogDependencyError(ValueError):
    """All unexplained missing references; no resolved result may be written."""

    def __init__(self, missing: tuple[MissingCatalogDependency, ...]) -> None:
        self.missing = missing
        super().__init__(
            f"{len(missing)} unexplained missing catalog dependencies: "
            + "; ".join(f"{m.key!r} for {m.output}" for m in missing[:10])
        )


# Shared inputs: keys of these kinds lie in no register.
_SHARED_KINDS = frozenset({"classification", "source_column"})


def _dependency_register(key: DependencyKey) -> str | None:
    """The register FQID a key lives in; shared inputs belong to no register."""
    kind, fqid = key[0], key[1]
    if kind in _SHARED_KINDS:
        return None
    return fqid if kind in {"register", "variant"} else fqid.rsplit("/", 1)[0]


def _declared_entities(key: DependencyKey) -> tuple[DependencyKey, ...]:
    """The declared registers, variants and variables a key names. A representation
    or state is proven only this far; shared inputs name none."""
    kind = key[0]
    if kind in {"register", "variant", "variable"}:
        return (key,)
    if kind in _SHARED_KINDS:
        return ()
    variable = ("variable", key[1])
    if kind in {"state", "variant_states"}:
        variant = key[2]
    else:  # a representation, optionally of one variant
        variant = key[3] if len(key) == 4 else None
    if variant is None:
        return (variable,)
    return variable, ("variant", key[1].rsplit("/", 1)[0], variant)


class CatalogDependencies:
    """Exact available references and evidenced omissions, never a missing fallback.

    A key starts with its kind, followed by its complete catalog coordinates.
    Callers build the withheld map from resolution accounting, not the baseline DB.
    Descendant lookup may inherit a known withheld parent supplied by the caller.
    A register-scoped build names its `slice_registers` and the `unselected`
    registers, variants and variables that other scope files declare but were not
    formed. A curation entry whose every register reference lies outside the
    slice is skipped. In any other entry a missing key naming only unselected
    entities is deferred to the complete build as a warning; any other missing
    key, such as one no scope declares, stays missing, exactly as in the complete
    build.
    """

    def __init__(
        self,
        available: set[DependencyKey],
        withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
        unselected: Collection[DependencyKey] = (),
        slice_registers: Collection[str] | None = None,
    ) -> None:
        if overlap := available & withheld.keys():
            raise ValueError(
                f"catalog dependencies both present and withheld: {sorted(overlap)}"
            )
        for key, causes in withheld.items():
            if not causes or any(not cause.refs for cause in causes):
                raise ValueError(f"withheld dependency lacks source evidence: {key!r}")
        self.available = available
        self.withheld = withheld
        self.unselected = unselected
        self.slice_registers = slice_registers
        self.diagnostics: list[ResolutionDiagnostic] = []
        self.missing: list[MissingCatalogDependency] = []
        self.skipped = 0
        self._registers: set[str] = set()

    def check(self) -> None:
        if self.missing:
            raise CatalogDependencyError(tuple(self.missing))

    @contextmanager
    def entry(self) -> Iterator[int]:
        """Evaluate one curation entry. In a register-scoped build, one whose every
        register reference lies outside the slice is skipped: what its references
        resolved to is discarded, it emits nothing and counts as skipped. One with
        no register reference is evaluated as in the complete build."""
        start, missing = len(self.diagnostics), len(self.missing)
        self._registers = set()
        yield start
        if (
            self.slice_registers is not None
            and self._registers
            and self._registers.isdisjoint(self.slice_registers)
        ):
            del self.diagnostics[start:]
            del self.missing[missing:]
            self.skipped += 1

    def require(
        self,
        key: DependencyKey,
        *,
        output: str,
        parents: tuple[DependencyKey, ...] = (),
    ) -> bool:
        if (
            self.slice_registers is not None
            and (register := _dependency_register(key)) is not None
        ):
            self._registers.add(register)
        if key in self.available:
            return True
        causes = next(
            (self.withheld[k] for k in (key, *parents) if k in self.withheld),
            None,
        )
        if (
            causes is None
            and self.unselected
            and (declared := _declared_entities(key))
            and all(entity in self.unselected for entity in declared)
        ):
            self.diagnostics.append(
                ResolutionDiagnostic(
                    code=DEFERRED_REFERENCE,
                    severity="warning",
                    subject=output,
                    detail=f"Dependency {key!r} names {declared!r}, declared only by unselected scopes; the complete build resolves it.",
                    withheld_output=(output,),
                )
            )
            return False
        if causes is None:
            self.missing.append(MissingCatalogDependency(key, output))
            return False
        for cause in causes:
            self.diagnostics.append(
                cause.model_copy(
                    update={
                        "code": "withheld_catalog_dependency",
                        "subject": output,
                        "detail": f"Dependency {key!r} is withheld: {cause.code}: {cause.detail}",
                        "withheld_output": (output,),
                    }
                )
            )
        return False


@dataclass(frozen=True)
class PanelResolution:
    variables: tuple[ResolvedVariable, ...]
    registers: tuple[ResolvedRegister, ...]
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...]
    editions: tuple[ResolvedEdition, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


@dataclass(frozen=True)
class MetadataResolution:
    metadata: ResolvedMetadata
    diagnostics: tuple[ResolutionDiagnostic, ...]
    skipped: int


@dataclass(frozen=True)
class GroupEdgeDisposition:
    source: Literal["code_label", "same_definition"]
    a: str
    b: str
    status: Literal["grouped", "claimed_by_curated", "withheld"]
    group_key: str | None = None


@dataclass(frozen=True)
class GroupEdgeResolution:
    groups: tuple[ResolvedVariableGroup, ...]
    dispositions: tuple[GroupEdgeDisposition, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]
    skipped: int


@dataclass(frozen=True)
class MonthGroupResolution:
    groups: tuple[ResolvedVariableGroup, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def resolve_month_groups(
    variables: tuple[ResolvedVariable, ...],
    *,
    edge_groups: tuple[ResolvedVariableGroup, ...],
    curated_groups: tuple[ResolvedVariableGroup, ...],
    evidence: Mapping[str, tuple[SourceRecordRef, ...]],
) -> MonthGroupResolution:
    """Derive existing month families before writing, retaining collision reports.

    Already grouped edge members do not participate. Curated keys are reserved;
    conflicting membership in a differently named curated group is still fatal,
    as in the original rule. Missing variables are accounted for by source
    resolution, not invented here to meet the three-month threshold.
    """
    by_fqid = {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": v
        for v in variables
    }
    if len(by_fqid) != len(variables):
        raise ValueError("duplicate variable identity in month grouping")
    claimed = {m.variable for g in edge_groups for m in g.members}
    if missing := claimed - by_fqid.keys():
        raise ValueError(
            f"resolved edge groups have missing variables: {sorted(missing)}"
        )
    existing = (*edge_groups, *curated_groups)
    validate_metadata_structure(ResolvedMetadata(variable_groups=existing))
    candidates = month_group_candidates(
        (
            (fqid.rsplit("/", 1)[0], v.slug, v.name)
            for fqid, v in by_fqid.items()
            if fqid not in claimed
        ),
        reserved_keys=frozenset((g.register_ref, g.key) for g in existing),
    )
    groups, diagnostics = [], []
    for candidate in candidates:
        fqids = tuple(f"{candidate.register}/{slug}" for _, slug in candidate.members)
        if any(not evidence.get(fqid) for fqid in fqids):
            raise ValueError(f"month-group candidate lacks source evidence: {fqids!r}")
        if candidate.issue is not None:
            output = f"variable_group:{candidate.register}/{candidate.key}"
            diagnostics.append(
                ResolutionDiagnostic(
                    code="month_group_" + candidate.issue,
                    severity="warning",
                    subject=output,
                    detail=(
                        "Distinct qualifying month stems collapse to the same group key."
                        if candidate.issue == "stem_collision"
                        else "The month stem collides with an existing or declared group key."
                    ),
                    refs=tuple(
                        sorted({r for fqid in fqids for r in evidence[fqid]}, key=repr)
                    ),
                    fields=("group",),
                    withheld_output=(output,),
                )
            )
            continue
        assert candidate.label is not None
        groups.append(
            ResolvedVariableGroup(
                register=candidate.register,
                key=candidate.key,
                label=candidate.label,
                source="token",
                axes=(ResolvedGroupAxis(axis="month", ordinal=0, label="månad"),),
                members=tuple(
                    ResolvedGroupVariable(
                        variable=f"{candidate.register}/{slug}",
                        facets=(
                            ResolvedGroupFacet(
                                axis="month",
                                value=f"{month:02d}",
                                label=_MONTH_LABELS[month],
                            ),
                        ),
                    )
                    for month, slug in candidate.members
                ),
            )
        )
    validate_metadata_structure(ResolvedMetadata(variable_groups=(*existing, *groups)))
    return MonthGroupResolution(tuple(groups), tuple(diagnostics))


def resolve_variable_edge_groups(
    pairs: tuple[CodeLabelPair, ...],
    variables: tuple[ResolvedVariable, ...],
    *,
    foldable_sibling_pairs: tuple[tuple[str, str], ...] = (),
    curated_groups: tuple[ResolvedVariableGroup, ...],
    evidence: Mapping[str, tuple[SourceRecordRef, ...]],
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
    unselected: Collection[DependencyKey] = (),
    slice_registers: Collection[str] | None = None,
) -> GroupEdgeResolution:
    """Resolve existing code/label and checked same-definition group edges.

    Code/label pairs retain their coding and shared-variant guards. The caller
    supplies already checked foldable sibling pairs, never slug patterns. A common
    native definition alone does not bypass the existing code/label and type-shape
    exclusions; those belong to the source-to-pair derivation.
    Both edge kinds share one component pass; deriving them separately could put
    a variable in two groups. Exclude curated members before connecting components.
    Invalid/unavailable edges retain source evidence; unexplained refs stay fatal.
    """
    by_fqid = {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": v
        for v in variables
    }
    if len(by_fqid) != len(variables):
        raise ValueError("duplicate variable identity in group-edge resolution")
    declared = tuple(
        (
            f"{p.code_provider}/{p.code_register}/{p.code_variable}",
            f"{p.label_provider}/{p.label_register}/{p.label_variable}",
        )
        for p in pairs
    )
    if len(set(declared)) != len(declared) or any(a == b for a, b in declared):
        raise ValueError("code-label declarations must be unique non-self pairs")
    siblings = tuple(tuple(sorted(pair)) for pair in foldable_sibling_pairs)
    if len(set(siblings)) != len(siblings) or any(a == b for a, b in siblings):
        raise ValueError("same-definition declarations must be unique non-self pairs")
    dependencies = CatalogDependencies(
        {("variable", fqid) for fqid in by_fqid}, withheld, unselected, slice_registers
    )
    claimed = {m.variable for g in curated_groups for m in g.members}
    components: DisjointSet[str] = DisjointSet()
    dispositions = []
    edges: tuple[tuple[Literal["code_label", "same_definition"], str, str], ...] = (
        *(("code_label", a, b) for a, b in declared),
        *(("same_definition", a, b) for a, b in siblings),
    )
    for source, code, label in edges:
        output = f"{source}_pair:{code}:{label}"
        with dependencies.entry():
            available = [
                dependencies.require(
                    ("variable", fqid),
                    output=output,
                    parents=(("register", fqid.rsplit("/", 1)[0]),),
                )
                for fqid in (code, label)
            ]
        if not all(available):
            dispositions.append(GroupEdgeDisposition(source, code, label, "withheld"))
            continue
        if any(not evidence.get(fqid) for fqid in (code, label)):
            raise ValueError(f"group-edge declaration lacks source evidence: {output}")
        coded, named = by_fqid[code], by_fqid[label]
        problems = []
        if source == "code_label":
            if not any(s.value_set is not None for s in coded.states):
                problems.append("the code endpoint has no supported value set")
            if any(s.value_set is not None for s in named.states):
                problems.append("the label endpoint owns a value set")
            if not (
                {s.variant.slug for s in coded.states}
                & {s.variant.slug for s in named.states}
            ):
                problems.append("the endpoints have no shared register variant")
        if code.rsplit("/", 1)[0] != label.rsplit("/", 1)[0]:
            problems.append("the endpoints belong to different registers")
        if problems:
            dependencies.diagnostics.append(
                ResolutionDiagnostic(
                    code=f"unresolved_{source}_pair",
                    severity="error",
                    subject=output,
                    detail="; ".join(problems),
                    refs=tuple(dict.fromkeys((*evidence[code], *evidence[label]))),
                    fields=("coding", "identity"),
                    withheld_output=(output,),
                )
            )
            dispositions.append(GroupEdgeDisposition(source, code, label, "withheld"))
        elif code in claimed or label in claimed:
            dispositions.append(
                GroupEdgeDisposition(source, code, label, "claimed_by_curated")
            )
        else:
            components.add(code)
            components.add(label)
            components.union(code, label)
            dispositions.append(GroupEdgeDisposition(source, code, label, "grouped"))
    dependencies.check()
    groups = []
    membership = {}
    reserved = {(g.register_ref, g.key) for g in curated_groups}
    for members in sorted(sorted(c) for c in components.components().values()):
        register, key = members[0].rsplit("/", 1)
        if (register, key) in reserved:
            raise ValueError(
                f"derived edge-group key conflicts with curated group: {register}/{key}"
            )
        groups.append(
            ResolvedVariableGroup(
                register=register,
                key=key,
                label=by_fqid[members[0]].name or key,
                source="edge",
                members=tuple(ResolvedGroupVariable(variable=m) for m in members),
            )
        )
        membership.update(dict.fromkeys(members, key))
    return GroupEdgeResolution(
        tuple(groups),
        tuple(
            GroupEdgeDisposition(d.source, d.a, d.b, d.status, membership[d.a])
            if d.status == "grouped"
            else d
            for d in dispositions
        ),
        tuple(dependencies.diagnostics),
        dependencies.skipped,
    )


def resolve_metadata_dependencies(
    metadata: ResolvedMetadata,
    variables: tuple[ResolvedVariable, ...],
    *,
    registers: tuple[ResolvedRegister, ...],
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...],
    classifications: tuple[ResolvedClassification, ...],
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
    unselected: Collection[DependencyKey] = (),
    slice_registers: Collection[str] | None = None,
) -> MetadataResolution:
    """Withhold only declared metadata depending on evidenced unresolved facts.

    Tags and sufficiently populated groups retain their independent members.
    Binary relations are indivisible. Missing references without an explicit
    source-resolution cause are fatal even if another endpoint is withheld.
    """
    metadata = ResolvedMetadata.model_validate(metadata)
    validate_metadata_structure(metadata)
    available: set[DependencyKey] = {
        ("register", f"{r.provider}/{r.slug}") for r in registers
    }
    available.update(("variant", f"{r.provider}/{r.slug}", v.slug) for r, v in variants)
    available.update(("classification", c.slug) for c in classifications)
    available.update(
        ("source_column", c.table_name, c.column_name) for c in metadata.source_columns
    )
    for variable in variables:
        available.update(variable_dependency_keys(variable))
    dependencies = CatalogDependencies(available, withheld, unselected, slice_registers)

    def entity(fqid: str, output: str) -> bool:
        kind = "register" if fqid.count("/") == 1 else "variable"
        parents = () if kind == "register" else (("register", fqid.rsplit("/", 1)[0]),)
        return dependencies.require((kind, fqid), output=output, parents=parents)

    def representation(
        fqid: str,
        column: str,
        output: str,
        *,
        variant: str | None = None,
        literal: bool = False,
    ) -> bool:
        key = (
            "representation" if literal else "succession_representation",
            fqid,
            column if literal else column.lower(),
        )
        if variant is not None:
            key = (*key, variant)
        return dependencies.require(
            key,
            output=output,
            parents=(("variable", fqid), ("register", fqid.rsplit("/", 1)[0])),
        )

    def state(ref: ResolvedStateRef, output: str) -> bool:
        return dependencies.require(
            _state_dependency_key(ref.variable, ref.variant, ref),
            output=output,
            parents=(
                ("variant_states", ref.variable, ref.variant),
                ("variable", ref.variable),
                ("register", ref.variable.rsplit("/", 1)[0]),
            ),
        )

    def withheld_group(output: str, start: int) -> None:
        causes = dependencies.diagnostics[start:]
        dependencies.diagnostics.append(
            ResolutionDiagnostic(
                code="withheld_catalog_dependency",
                severity="error"
                if any(c.severity == "error" for c in causes)
                else "warning",
                subject=output,
                detail="The group lacks its parent or at least two supported members.",
                refs=tuple(
                    dict.fromkeys(ref for issue in causes for ref in issue.refs)
                ),
                withheld_output=(output,),
            )
        )

    groups = []
    for group in metadata.variable_groups:
        output = f"variable_group:{group.register_ref}/{group.key}"
        with dependencies.entry() as start:
            parent = entity(group.register_ref, output)
            members = []
            for member in group.members:
                member_output = f"{output}:member:{member.variable}:{member.delivery_column_name or ''}"
                supported = (
                    entity(member.variable, member_output)
                    if member.delivery_column_name is None
                    else representation(
                        member.variable,
                        member.delivery_column_name,
                        member_output,
                        literal=True,
                    )
                )
                if supported:
                    members.append(member)
            if parent and len(members) >= 2:
                groups.append(group.model_copy(update={"members": tuple(members)}))
            else:
                withheld_group(output, start)
    classification_groups = []
    for group in metadata.classification_groups:
        output = f"classification_group:{group.key}"
        start = len(dependencies.diagnostics)
        members = tuple(
            member
            for member in group.members
            if dependencies.require(
                ("classification", member.classification),
                output=f"{output}:member:{member.classification}",
            )
        )
        if len(members) >= 2:
            classification_groups.append(group.model_copy(update={"members": members}))
        else:
            withheld_group(output, start)
    tags = []
    for tag in metadata.tags:
        skipped_before = dependencies.skipped
        with dependencies.entry():
            members = tuple(
                member
                for member in tag.members
                if entity(member.target, f"tag:{tag.slug}:member:{member.target}")
            )
        if dependencies.skipped > skipped_before:
            # Every member lies outside the slice: omit the tag entirely
            # rather than materializing it with an empty member list.
            continue
        tags.append(tag.model_copy(update={"members": members}))

    def selected[T](
        items: tuple[T, ...], field: str, references: Callable[[T, str], list[bool]]
    ) -> tuple[T, ...]:
        result = []
        for index, item in enumerate(items):
            with dependencies.entry():
                checks = references(item, f"{field}:{index}")
            if all(checks):
                result.append(item)
        return tuple(result)

    historical = {h.target: h for h in metadata.historical_predecessors}
    for target, declaration in historical.items():
        if (declaration.kind, target) in available:
            raise ValueError(
                f"obsolete historical predecessor declaration now names a live entity: {target}"
            )
        if (declaration.kind, target) in withheld:
            raise ValueError(
                f"historical predecessor is a withheld selected entity: {target}"
            )
    successions = selected(
        metadata.successions,
        "successions",
        lambda e, o: [
            True if e.predecessor in historical else entity(e.predecessor, o),
            entity(e.successor, o),
        ],
    )
    updates = {
        "variable_groups": tuple(groups),
        "classification_groups": tuple(classification_groups),
        "tags": tuple(tags),
        "variable_same_as": selected(
            metadata.variable_same_as,
            "variable_same_as",
            lambda e, o: [entity(e.a, o), entity(e.b, o)],
        ),
        "classification_same_as": selected(
            metadata.classification_same_as,
            "classification_same_as",
            lambda e, o: [
                dependencies.require(("classification", end.classification), output=o)
                for end in (e.a, e.b)
            ],
        ),
        "successions": successions,
        "historical_predecessors": tuple(
            h
            for h in metadata.historical_predecessors
            if any(e.predecessor == h.target for e in successions)
        ),
        "variant_successions": selected(
            metadata.variant_successions,
            "variant_successions",
            lambda e, o: [
                dependencies.require(
                    ("variant", end.register_ref, end.variant),
                    output=o,
                    parents=(("register", end.register_ref),),
                )
                for end in (e.predecessor, e.successor)
            ],
        ),
        "representation_successions": selected(
            metadata.representation_successions,
            "representation_successions",
            lambda e, o: [
                representation(
                    end.variable, end.delivery_column_name, o, variant=e.variant
                )
                for end in (e.predecessor, e.successor)
            ],
        ),
        "classification_derivations": selected(
            metadata.classification_derivations,
            "classification_derivations",
            lambda e, o: [
                dependencies.require(("classification", end), output=o)
                for end in (e.derived, e.source)
            ],
        ),
        "state_lineage": selected(
            metadata.state_lineage,
            "state_lineage",
            lambda e, o: [state(e.consumer, o), state(e.source, o)],
        ),
        "lineage_warnings": selected(
            metadata.lineage_warnings,
            "lineage_warnings",
            lambda e, o: [state(e.consumer, o)],
        ),
        "documentary_relationships": selected(
            metadata.documentary_relationships,
            "documentary_relationships",
            lambda e, o: [
                entity(fqid, o)
                for fqid in (e.owner, *(a.variable for a in e.variables))
            ],
        ),
        "source_join_keys": selected(
            metadata.source_join_keys,
            "source_join_keys",
            lambda e, o: [
                dependencies.require(
                    ("source_column", e.table_name, e.column_name), output=o
                )
            ],
        ),
    }
    dependencies.check()
    return MetadataResolution(
        metadata.model_copy(update=updates),
        tuple(dependencies.diagnostics),
        dependencies.skipped,
    )


def resolve_panel_dependencies(
    variables: tuple[ResolvedVariable, ...],
    *,
    registers: tuple[ResolvedRegister, ...] = (),
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...] = (),
    editions: tuple[ResolvedEdition, ...] = (),
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
) -> PanelResolution:
    """Keep independent parent facts when an exact panel axis is unsupported.

    A composite key is one assertion: losing a member withholds the complete key,
    not a shorter invented key. The other axis, grain, names and prose survive.
    States, aliases, editions and standalone parents share the rewritten variant.
    """
    # All variables may have been withheld for source errors. Independent parent
    # facts still resolve; the writer enforces the strict publication floor.
    variables, by_register, by_variant = validate_resolved_variables(
        variables, allow_empty=True
    )

    def register_parent(register: ResolvedRegister) -> None:
        key = register.provider, register.slug
        if key in by_register and by_register[key] != register:
            raise ValueError(f"inconsistent resolved parent register: {key!r}")
        by_register[key] = register

    def variant_parent(register: ResolvedRegister, variant: ResolvedVariant) -> None:
        register_parent(register)
        key = register.provider, register.slug, variant.slug
        if key in by_variant and by_variant[key] != variant:
            raise ValueError(f"inconsistent resolved parent variant: {key!r}")
        by_variant[key] = variant

    for register in registers:
        register_parent(register)
    for register, variant in variants:
        variant_parent(register, variant)
    for edition in editions:
        variant_parent(edition.register_ref, edition.variant)

    available = {
        key for variable in variables for key in variable_dependency_keys(variable)
    }
    dependencies = CatalogDependencies(available, withheld)
    for key, variant in by_variant.items():
        register = "/".join(key[:2])
        updates = {}
        for field in ("panel_entity_key", "panel_time_key"):
            value = getattr(variant, field)
            if value is None or (field == "panel_time_key" and value == "period"):
                continue
            members = value if isinstance(value, tuple) else (value,)
            if not members:
                raise ValueError("panel keys cannot be empty tuples")
            supported = [
                dependencies.require(
                    ("variant_states", f"{register}/{slug}", variant.slug),
                    parents=(("variable", f"{register}/{slug}"),),
                    output=f"{register}/{variant.slug}:{field}",
                )
                for slug in members
            ]
            if not all(supported):
                updates[field] = None
        if updates:
            by_variant[key] = variant.model_copy(update=updates)

    dependencies.check()

    def resolved_variant(
        register: ResolvedRegister, variant: ResolvedVariant
    ) -> ResolvedVariant:
        return by_variant[register.provider, register.slug, variant.slug]

    return PanelResolution(
        variables=tuple(
            variable.model_copy(
                update={
                    "states": tuple(
                        s.model_copy(
                            update={
                                "variant": resolved_variant(
                                    variable.register_ref, s.variant
                                )
                            }
                        )
                        for s in variable.states
                    ),
                    "aliases": tuple(
                        a.model_copy(
                            update={
                                "variant": resolved_variant(
                                    variable.register_ref, a.variant
                                )
                            }
                        )
                        for a in variable.aliases
                    ),
                }
            )
            for variable in variables
        ),
        registers=tuple(by_register[key] for key in sorted(by_register)),
        variants=tuple(
            (by_register[key[:2]], by_variant[key]) for key in sorted(by_variant)
        ),
        editions=tuple(
            e.model_copy(
                update={"variant": resolved_variant(e.register_ref, e.variant)}
            )
            for e in editions
        ),
        diagnostics=tuple(dependencies.diagnostics),
    )
