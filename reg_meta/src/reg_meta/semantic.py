"""Project semantic validation — the reg_meta-backed layer of `/validate`.

See DESIGN.md → Project semantic validation (`semantic.py`). The third
validation layer: the supported-version decision and `reg_schema`'s structural
layer run first; this one resolves every FQID in a *structurally valid*
``project_data.json`` against a reg_meta ``Catalog``. It lives in reg_meta —
NOT ``reg_schema`` — because ``reg_schema`` is reg_meta-free by design (the
shared validation surface stays importable without pulling reg_meta); semantic
rules need the DB. It is shared domain code beside the order materializer
(reg_meta/DESIGN.md → "Project semantic validation"): the FastAPI
``POST /api/project/validate`` and the ``reg-meta validate`` CLI are thin
adapters over ``validate_project`` and emit its canonical bytes
(``validation_json``).

It emits the same frozen ``reg_schema.ValidationIssue`` shape the other layers
do — composition is tuple concatenation, no merge semantics.
``validate_semantic`` takes a ``Catalog`` (never opens a connection);
``validate_project`` opens one through its caller's ``connect`` only after the
DB-free layers pass, so a rejected body costs no DB hit.

Validation resolves reference semantics and then probes the artifact's compiled
mappings at the source's exact variant. The two steward membership warning codes
remain nonblocking; validation is never a claim of physical order readiness.

Inputs are the ``reg_schema`` Pydantic models (``ProjectData`` / ``Source`` /
``Binding``), which ``validate_project`` constructs only AFTER
``validate_structural`` passes — so this layer assumes well-formed FQIDs /
period grammar and resolves them, rather than re-checking shape. In particular,
calendar-day validity of the AUTHOR-supplied period endpoints (rejecting an
impossible author day like ``2019-02-29``) is a STRUCTURAL guarantee (see
reg_schema/DESIGN.md → Structural rules and issue codes), so this layer does
not pre-check it.

**AVAILABILITY IS NOT DECIDED HERE.** The source period is expanded
(``order.requested_intervals``) and each binding resolved
(``order.resolve_binding``) by the SHARED pass the order materializer runs, and
this layer only translates those facts into issues. That is what makes the two
agree: under the materializer's intersection semantics (reg_meta/DESIGN.md →
"Order materializer and manifest") a binding is requested wherever it is
available inside the source window, so availability narrower than the request
is an INFO clip here and a clipped order there, while only a binding available
nowhere in the request blocks both. The period grammar, the
interval algebra and the synthesized-month-end snap all live behind that pass;
the arithmetic left on this side reads the intervals that pass already clipped,
through those same shared helpers. Steps 3+4 of the materializer (the steward's
physical topology and its coverage gate) do NOT run here: a clean validation is
a resolvable project, never a proof of physical order readiness.
"""

from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, Any, Literal

from pydantic import ValidationError
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural
from reg_schema.validation import ValidationIssue, ValidationResult

from .catalog import Catalog
from .db import get_manifest
from .errors import RegMetaError
from .fqid import FqidError, parse

# `inventory` owns the interval grammar an edition, a project period and an
# availability clip all speak, so this layer renders (`_render`) and intersects
# (`_overlap`) through it rather than keeping a second speller or overlap rule.
from .inventory import _overlap, _render

# The SHARED availability/slicing pass (the materializer's steps 1+2) and the
# supported-version decision — see the module docstring.
from .order import requested_intervals, resolve_binding, schema_version_issue

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable
    from contextlib import AbstractContextManager

    from reg_schema.project_data import Binding, Source

    from .catalog import VariableIdentity
    from .order import StateWindow

type Interval = tuple[str, str]


def validate_project(
    raw: dict[str, Any],
    connect: Callable[[], AbstractContextManager[sqlite3.Connection]],
) -> ValidationResult:
    """The §6.8.0 composition over a raw ``project_data.json``: supported
    version → structural → model build → semantic. The adapters' ONE door.

    A FAILING project is a result, never a raise: this is a diagnostic, and
    every issue of every layer that ran comes back concatenated. The
    supported-version issue (``order.schema_version_issue``, the decision the
    order door gates on too) returns ALONE — the layers under it read the
    document as the current contract, which is the claim it just rejected. The
    structural layer's failure skips the semantic layer (it assumes a
    structurally valid spec).

    ``connect`` opens the selected artifact for this call only, and only once
    the DB-free layers have passed, so a rejected body costs no DB hit. The
    adapter owns how (the webapp's per-request thread-confined open, the CLI's
    catalog selection); this function closes it by leaving the ``with``."""
    unsupported = schema_version_issue(raw)
    if unsupported is not None:
        return ValidationResult(issues=(unsupported,))
    structural = validate_structural(raw)
    if not structural.ok:
        return structural
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        return ValidationResult(issues=(*structural.issues, _model_issue(exc)))
    with connect() as conn:
        semantic = validate_semantic(project, Catalog(conn, scope="reference"))
    return ValidationResult(issues=structural.issues + semantic.issues)


def validation_json(result: ValidationResult) -> str:
    """The canonical serialization both adapters emit VERBATIM: ``ok`` plus
    every issue in layer order, sorted keys, UTF-8, trailing newline — the
    ``OrderManifest.to_json`` conventions, so the FastAPI body and the CLI's
    stdout are byte-identical. An absent ``successor_fqid`` is an explicit
    ``null``: the wire shape the SPA's codegen'd type reads."""
    return (
        json.dumps(
            {
                "ok": result.ok,
                "issues": [dataclasses.asdict(issue) for issue in result.issues],
            },
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    )


def _model_issue(exc: ValidationError) -> ValidationIssue:
    """A residual ``ProjectData.model_validate`` failure as an error issue.

    THIN DEFENSIVE catch. ``validate_structural`` owns the structural problems
    (missing / mistyped / unexpected keys — incl. ``unexpected_field`` on every
    closed project object), and the model is built only once structural passed,
    so this is a constraint structural did NOT replicate — effectively
    unreachable under today's models, and surfaced as an issue (code
    ``invalid_field``), never a crash. The path points at the first offending
    field."""
    errors = exc.errors()
    loc = errors[0]["loc"] if errors else ()
    # RFC 6901: "" points at the whole document; "/" would mean a property keyed
    # by the empty string (unresolvable). A model-level error has an empty loc.
    path = "/" + "/".join(str(p) for p in loc) if loc else ""
    return ValidationIssue(
        level="error",
        code="invalid_field",
        path=path,
        message=(
            "project_data failed model construction (a constraint the "
            f"structural layer did not catch?): {exc}"
        ),
    )


def validate_semantic(
    project: ProjectData,
    catalog: Catalog,
) -> ValidationResult:
    """Validate reference semantics and compiled source-variant membership.

    The caller owns the reference Catalog and connection lifetime. Structural
    validation runs before this boundary."""
    steward = (
        get_manifest(catalog.holdings.conn).get("catalog_artifact_kind") == "steward"
    )
    issues: list[ValidationIssue] = []
    for s_idx, source in enumerate(project.sources):
        _check_source(source, s_idx, catalog, steward, issues)
    return ValidationResult(issues=tuple(issues))


def _issue(
    code: str,
    level: Literal["error", "warning", "info"],
    path: str,
    message: str,
    *,
    successor_fqid: str | None = None,
) -> ValidationIssue:
    """Positional shorthand for a ``ValidationIssue``, code first — the order the
    rules below read in."""
    return ValidationIssue(
        level=level,
        code=code,
        path=path,
        message=message,
        successor_fqid=successor_fqid,
    )


def _check_source(
    source: Source,
    s_idx: int,
    catalog: Catalog,
    steward: bool,
    issues: list[ValidationIssue],
) -> None:
    base = f"/sources/{s_idx}"

    # The source's register_variant must resolve to a known variant. The
    # variant coordinate is `<provider>/<register>/<variant>` — NOT an FQID kind
    # (see reg_meta/DESIGN.md → FQID grammar), so it can't go through
    # `Catalog.resolve`. We resolve the
    # provider/register prefix to a known register and the variant slug to a
    # `register_variant` row via `list_variants` (the variant browse axis). A
    # missing register OR variant is `fqid_unresolved`.
    variant_ok = _check_register_variant(source.register_variant, base, catalog, issues)

    # The requested window, expanded ONCE per source through the shared grammar
    # (an inventory edition and a project period expand identically). Structural
    # validation already guaranteed the period grammar, so the raise is a
    # fail-closed backstop — spelled with the materializer's own code, since a
    # period neither can expand is the same fact on both paths.
    try:
        requested = requested_intervals(source.period)
    except (TypeError, ValueError) as exc:
        issues.append(
            _issue(
                "period_not_orderable",
                "error",
                f"{base}/period",
                f"source {source.name!r} has no orderable period: {exc}",
            )
        )
        return

    for b_idx, binding in enumerate(source.bindings):
        _check_binding(
            binding,
            source,
            base,
            b_idx,
            variant_ok,
            requested,
            catalog,
            steward,
            issues,
        )


def _check_register_variant(
    register_variant: str,
    base: str,
    catalog: Catalog,
    issues: list[ValidationIssue],
) -> bool:
    """Resolve a `<provider>/<register>/<variant>` coordinate. Returns True when
    the (provider, register, variant) all resolve — the binding checks reuse this
    to decide whether a `period_outside_state_validity` probe is meaningful (a
    period probe against an unresolved variant is noise)."""
    path = f"{base}/register_variant"
    # Structural validation guarantees the 3-part shape; defensively guard a
    # malformed coordinate as unresolved rather than raising.
    parts = register_variant.split("/")
    if len(parts) != 3:
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                path,
                f"register_variant {register_variant!r} is not a 3-part coordinate",
            )
        )
        return False
    provider, register, variant = parts
    variant_slugs = {v.slug for v in catalog.list_variants(provider, register)}
    if not variant_slugs:
        # Empty means the (provider, register) names no register OR the register
        # has no variants — either way the coordinate doesn't resolve.
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                path,
                f"register_variant {register_variant!r} resolves to no register "
                "or no variants in reg_meta",
            )
        )
        return False
    if variant not in variant_slugs:
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                path,
                f"variant {variant!r} is not a known variant of "
                f"{provider}/{register} (known: {sorted(variant_slugs)})",
            )
        )
        return False
    return True


def _period_end_year(requested: tuple[Interval, ...]) -> int:
    """Latest requested year, for replacement-hint gating."""
    return int(requested[-1][1][:4])


def _replacement_applies(
    effective_year: int | None, requested: tuple[Interval, ...]
) -> bool:
    """Whether a succession edge is effective by the requested period."""
    if effective_year is None:
        # An undated succession is not tied to any requested year, so it never
        # qualifies against a concrete period.
        return False
    return effective_year <= _period_end_year(requested)


def _has_codelivered_versions(states: tuple[StateWindow, ...]) -> bool:
    """CO-DELIVERY: are ≥2 of the binding's kept states, with DISTINCT VALUE SETS
    (different ``value_set_id`` — not merely a different free-text version
    label), available at the SAME REQUESTED instant? That is the genuine
    ambiguity: the same coordinate yields two different code-lists. Two states
    that share a ``value_set_id`` but carry different version labels are the SAME
    values under two names — NOT ambiguity (keying on the label would
    false-positive on ~71% of co-deliveries). Sequential states from a transition
    are drift (info), not co-delivery.

    Overlap is tested on ``StateWindow.intervals`` — each state's validity
    already clipped to the request by the shared pass — so two windows that meet
    only inside a HOLE of a disjoint period never co-deliver: no requested
    instant extracts both. O(n²) over the few states a binding resolves to.

    Note: post-curation the reg_meta build enforces one value set per
    ``(variable, variant, period)`` (the build's co-delivery curation + ``validate``
    invariant), so this should not fire in practice — it is a defensive backstop."""
    for i, a in enumerate(states):
        for b in states[i + 1 :]:
            if a.state.value_set_id != b.state.value_set_id and _overlap(
                a.intervals, b.intervals
            ):
                return True
    return False


def _check_binding(
    binding: Binding,
    source: Source,
    base: str,
    b_idx: int,
    variant_ok: bool,
    requested: tuple[Interval, ...],
    catalog: Catalog,
    steward: bool,
    issues: list[ValidationIssue],
) -> None:
    bbase = f"{base}/bindings/{b_idx}"
    var_path = f"{bbase}/variable"

    # The binding FQID must resolve to a known variable (following
    # `same_as` curated links — `Catalog.variable_identity` does that
    # internally, the same way `resolve` does, without hydrating the variable's
    # historical states and their code lists: this layer reads identity and
    # succession only). The binding FQID is a bare 3-segment variable (the
    # `@version` pin is retired — the value set is determined by the resolved
    # `(variable, variant, period)`).
    try:
        parsed = parse(binding.variable)
    except FqidError:
        # Structurally valid input shouldn't reach here; treat as unresolved.
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                var_path,
                f"binding variable {binding.variable!r} is not a parseable FQID",
            )
        )
        return
    try:
        identity = catalog.variable_identity(parsed)
    except RegMetaError:
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                var_path,
                f"column {binding.variable!r} resolves to no variable in reg_meta",
            )
        )
        # The variable doesn't resolve, so the PERIOD probe is meaningless — skip
        # it. The value_set (an independent `class/<slug>` FQID) can still be broken
        # on its own, so validate it before returning.
        _check_value_set(binding, bbase, catalog, issues)
        return

    _check_binding_hints(binding, var_path, identity, requested, issues)

    resolved_columns = _check_binding_period(
        binding, source, var_path, variant_ok, requested, catalog, issues
    )
    if steward and variant_ok:
        _check_steward_admission(
            binding.variable,
            source.register_variant,
            var_path,
            resolved_columns,
            catalog,
            issues,
        )
    _check_value_set(binding, bbase, catalog, issues)


def _check_binding_hints(
    binding: Binding,
    var_path: str,
    identity: VariableIdentity,
    requested: tuple[Interval, ...],
    issues: list[ValidationIssue],
) -> None:
    """Non-blocking semantic hints that require resolved variable metadata."""
    if identity.deprecated:
        issues.append(
            _issue(
                "deprecated_traversal",
                "info",
                var_path,
                f"column {binding.variable!r} resolves to a deprecated catalog "
                "variable; prefer a current successor when one is available",
            )
        )

    for successor in identity.replaced_by:
        if not _replacement_applies(successor.effective_year, requested):
            continue
        successor_fqid = str(successor.fqid) if successor.fqid is not None else None
        effective = (
            f" effective {successor.effective_year}"
            if successor.effective_year is not None
            else ""
        )
        target = successor_fqid or (
            f"{successor.provider}/{successor.register_name}/{successor.variable}"
        )
        issues.append(
            _issue(
                "variable_replaced",
                "info",
                var_path,
                f"column {binding.variable!r} has replacement {target!r}{effective} "
                f"by requested period {_render(requested)}",
                successor_fqid=successor_fqid,
            )
        )


# `resolve_binding`'s blocking codes, translated to the validation code the SPA
# already labels for that condition. A code with no entry keeps its own spelling
# (`representation_unresolved` is registered under the materializer's name), so a
# future finding surfaces truthfully instead of raising.
_VALIDATION_CODE = {
    "variable_unresolved": "fqid_unresolved",
    "binding_unavailable": "period_outside_state_validity",
    "representation_unknown": "binding_representation_unknown",
    "representation_ambiguous": "binding_value_set_version_ambiguous",
}


def _check_binding_period(
    binding: Binding,
    source: Source,
    var_path: str,
    variant_ok: bool,
    requested: tuple[Interval, ...],
    catalog: Catalog,
    issues: list[ValidationIssue],
) -> frozenset[str] | None:
    """Translate the SHARED availability/slicing resolution
    (``order.resolve_binding``) into issues. Nothing about windows is decided
    here — the materializer\'s own pass decides, and this reports.

    Under intersection semantics the binding is requested wherever it is
    available inside the source window, so:

    - availability NARROWER than the request → ``range_period_partially_covered``
      (info, naming the period that WILL be ordered). It is the same fact for an
      explicit range, a point period and a segment of a #307 list — one code, one
      answer, and never an error, because the materializer orders the clipped
      window rather than refusing it;
    - availability EMPTY everywhere in the request → ``period_outside_state_validity``
      (error), the binding as authored extracts nothing;
    - a pinned ``representation`` that is no delivery column of the concept
      anywhere in the request → ``binding_representation_unknown`` (error);
    - ≥2 delivery columns valid at the SAME requested instant with no pin →
      ``binding_value_set_version_ambiguous`` (error): the extract would pull more
      than one column and a manifest never guesses. Distinct columns in
      NON-overlapping windows are a sequential rename, which fans out into slices
      rather than blocking;
    - a state with no delivery column at all → ``representation_unresolved``
      (error), the code the order blocks with.

    Two checks read the resolution rather than the request: ≥2 distinct value
    sets co-delivered on one column → ``binding_value_set_version_ambiguous``
    (error, a defensive backstop the reg_meta build\'s curation should make
    unreachable), and a request spanning several sequential states →
    ``binding_state_drifts_within_period`` (info; the resolver returns the
    per-state subsets at extract time).

    Returns the binding\'s RESOLVED delivery columns for the steward admission
    check (#206), or ``None`` when they are indeterminate — the variant did not
    resolve, or the binding carries a blocking finding, each of which already
    has its own issue, so admission stays silent rather than piling on."""
    # An unresolved variant already produced an `fqid_unresolved`; a period probe
    # against it would be derivative noise. Skip it.
    if not variant_ok:
        return None

    resolution = resolve_binding(catalog, source, binding, requested)

    if resolution.clip is not None:
        issues.append(
            _issue(
                "range_period_partially_covered",
                "info",
                var_path,
                f"column {binding.variable!r} is available for only part of "
                f"requested period {resolution.clip.requested_period} at "
                f"{source.register_variant}; it is ordered for "
                f"{resolution.clip.ordered_period}",
            )
        )
    if resolution.finding is not None:
        code = resolution.finding.code
        issues.append(
            _issue(
                _VALIDATION_CODE.get(code, code),
                "error",
                var_path,
                resolution.finding.message,
            )
        )
        return None

    # Backstop: distinct value sets on ONE column at the same requested instant —
    # a reg_meta build co-delivery the curation missed (the build `validate`
    # invariant should make this unreachable against a clean catalog).
    if _has_codelivered_versions(resolution.states):
        labels: dict[int | None, str] = {}
        for window in resolution.states:
            labels.setdefault(
                window.state.value_set_id, window.state.value_set_version_label
            )
        issues.append(
            _issue(
                "binding_value_set_version_ambiguous",
                "error",
                var_path,
                f"column {binding.variable!r} resolves to several co-delivered "
                f"value sets {sorted(labels.values())} on one column at "
                f"{source.register_variant} period {resolution.requested_period} "
                "— this reg_meta build needs co-delivery curation",
            )
        )
        # The value-set ambiguity doesn\'t blur WHICH column(s) the binding
        # denotes, so the resolved columns are still good for admission.
        return resolution.columns

    # Drift (info): a period crossing a state transition resolves to several
    # SEQUENTIAL states (non-overlapping windows), possibly differing on version
    # label (a re-version) or shape — informational; the resolver returns the
    # per-state subsets at extract time.
    if len(resolution.states) > 1:
        issues.append(
            _issue(
                "binding_state_drifts_within_period",
                "info",
                var_path,
                f"column {binding.variable!r} spans {len(resolution.states)} "
                "states across a transition within period "
                f"{resolution.requested_period}",
            )
        )

    # >1 distinct column here only via a sequential rename across the period
    # (co-existing columns errored out above); the steward must hold each one
    # the extract would touch.
    return resolution.columns


def _format_columns(columns: Iterable[str | None]) -> str:
    """Render a set of delivery-column tokens for an issue message. ``None`` (a
    state genuinely carrying no ``delivery_column_name``) renders as a readable
    placeholder rather than a Python ``None``."""
    return ", ".join(
        repr(c) if c is not None else "(unnamed column)"
        for c in sorted(columns, key=lambda c: (c is None, c or ""))
    )


def _check_steward_admission(
    variable: str,
    variant_coord: str,
    var_path: str,
    resolved_columns: frozenset[str] | None,
    catalog: Catalog,
    issues: list[ValidationIssue],
) -> None:
    """Warn on missing authored binding/variant or canonical representation mappings.

    Indeterminate semantic columns skip representation admission. Physical periods
    are checked by materialize_order; no cross-variant possession is inferred."""
    ids = catalog.holdings.binding_ids(variable, variant_coord)
    held = catalog.holdings.columns(*ids) if ids is not None else frozenset()
    if not held:
        issues.append(
            _issue(
                "fqid_outside_steward_catalog",
                "warning",
                var_path,
                f"column {variable!r} resolves in reg_meta but is outside this "
                f"deployment's steward catalog under {variant_coord} — the "
                "steward does not supply it there",
            )
        )
        return
    if resolved_columns is None:
        return
    assert ids is not None
    missing = frozenset(
        column
        for column in resolved_columns
        if catalog.canonical_delivery_column(*ids, column) not in held
    )
    if missing:
        issues.append(
            _issue(
                "representation_outside_steward_catalog",
                "warning",
                var_path,
                f"column {variable!r} resolves to representation "
                f"{_format_columns(missing)}, which this steward does not supply "
                f"under {variant_coord} — available there as "
                f"{_format_columns(held)} only",
            )
        )


def _check_value_set(
    binding: Binding,
    bbase: str,
    catalog: Catalog,
    issues: list[ValidationIssue],
) -> None:
    """A binding's `value_set` (a `class/<slug>` FQID) must resolve to a
    known classification."""
    if binding.value_set is None:
        return
    vs_path = f"{bbase}/value_set"
    try:
        parsed = parse(binding.value_set)
    except FqidError:
        issues.append(
            _issue(
                "value_set_missing",
                "error",
                vs_path,
                f"value_set {binding.value_set!r} is not a parseable FQID",
            )
        )
        return
    try:
        catalog.resolve(parsed)
    except RegMetaError:
        issues.append(
            _issue(
                "value_set_missing",
                "error",
                vs_path,
                f"value_set {binding.value_set!r} resolves to no classification "
                "in reg_meta",
            )
        )
