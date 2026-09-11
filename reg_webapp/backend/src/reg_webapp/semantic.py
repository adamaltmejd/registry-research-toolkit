"""Semantic validation — reg_meta-backed.

See DESIGN.md → Semantic validation (semantic.py). The third validation layer:
structural (``reg_schema``) validation runs first;
this one resolves every
FQID in a *structurally valid* ``project_data.json`` against a live reg_meta
``Catalog``. It lives in the webapp — NOT ``reg_schema`` — because ``reg_schema``
is reg_meta-free by design (the shared validation surface stays importable
without pulling reg_meta); semantic rules need the DB, so they belong where the
DB is (the webapp backend, and any local tool that has loaded reg_meta).

It emits the same frozen ``reg_schema.ValidationIssue`` shape the other layers
do — composition is tuple concatenation, no merge semantics. It takes a
``Catalog`` (never opens a connection): A5.2b-ii's ``POST /api/project/validate``
calls it per-request with an in-handler connection.

There is ONE caller: the researcher path (``POST /api/project/validate``), where
an unresolved FQID is a blocking ``error``. It runs the COLUMN-based steward
admission check (#206) when an ``index`` (the loaded ``CatalogIndex``) is
supplied, probing the source's OWN ``register_variant``: a steward's inventory
states whole ``(register_variant, variable, representation)`` coordinates, so
holding a concept under one variant admits nothing under another. A RESOLVED
FQID the steward holds no column of under that variant emits
``fqid_outside_steward_catalog``, and one whose RESOLVED delivery column it does
not hold there emits ``representation_outside_steward_catalog`` — both
non-blocking ``warning``s (the column is real reg_meta-wide but this filtered
deployment does not supply it). The ``global`` deployment's ``index`` is ``None``
(no filter), so it never emits the codes.

Inputs are the ``reg_schema`` Pydantic models (``ProjectData`` / ``Source`` /
``Binding``), which the webapp constructs only AFTER ``validate_structural``
passes — so this layer assumes well-formed FQIDs / period grammar and resolves
them, rather than re-checking shape. In particular, calendar-day validity of the
AUTHOR-supplied period endpoints (rejecting an impossible author day like
``2019-02-29``) is a STRUCTURAL guarantee (see reg_schema/DESIGN.md → Structural
rules and issue codes) — every caller runs structural
first and short-circuits before semantic — so this layer no longer pre-checks it.

**AVAILABILITY IS NOT DECIDED HERE.** The source period is expanded
(``order.requested_intervals``) and each binding resolved
(``order.resolve_binding``) by the SHARED reg_meta pass the order materializer
runs, and this layer only translates those facts into issues. That is what makes
the two agree: under REFACTOR_SPEC.md §12 intersection semantics a binding is
requested wherever it is available inside the source window, so availability
narrower than the request is an INFO clip here and a clipped order there, while
only a binding available nowhere in the request blocks both. The period grammar,
the interval algebra and the synthesized-month-end snap all live behind that
pass; the arithmetic left on this side reads the intervals that pass already
clipped, through those same shared helpers. Steps 3+4 of the
materializer (the steward's physical topology and its coverage gate) do NOT run
here: a clean validation is a resolvable project, never a proof of physical
order readiness.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from reg_meta.errors import RegMetaError
from reg_meta.fqid import FqidError, parse

# The SHARED availability/slicing pass (REFACTOR_SPEC.md §12 steps 1+2) the order
# materializer runs — see the module docstring. `reg_meta.inventory` owns the
# interval grammar an edition, a project period and an availability clip all
# speak, so this layer renders (`_render`) and intersects (`_overlap`) through it
# rather than keeping a second speller or a second overlap rule.
from reg_meta.inventory import _overlap, _render
from reg_meta.order import requested_intervals, resolve_binding
from reg_schema.validation import ValidationIssue, ValidationResult

if TYPE_CHECKING:
    from collections.abc import Iterable

    from reg_meta.catalog import Catalog, VariableIdentity
    from reg_meta.order import StateWindow
    from reg_schema.project_data import Binding, ProjectData, Source

    from reg_webapp.catalog_index import CatalogIndex, Interval


def validate_semantic(
    project: ProjectData,
    catalog: Catalog,
    *,
    index: CatalogIndex | None = None,
) -> ValidationResult:
    """Run semantic rules over ``project`` against ``catalog``.

    Walks ``sources[*].register_variant``, each ``sources[*].bindings[*]``
    (``variable`` + ``value_set``), resolving against the live ``Catalog``.
    Returns a ``ValidationResult`` carrying the issues (see module docstring).
    Never opens a connection — the caller owns the ``Catalog``'s lifetime.

    ``index`` is the loaded steward ``CatalogIndex``: when supplied, a RESOLVED
    binding outside it — under the source's OWN ``register_variant`` — yields
    ``fqid_outside_steward_catalog`` (no column of the concept held there) or
    ``representation_outside_steward_catalog`` (concept held, but not the
    binding's resolved column) — both warnings. ``None`` (the ``global``
    deployment) never emits them.
    """
    issues: list[ValidationIssue] = []
    for s_idx, source in enumerate(project.sources):
        _check_source(source, s_idx, catalog, index, issues)
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
    index: CatalogIndex | None,
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
            binding, source, base, b_idx, variant_ok, requested, catalog, index, issues
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
    index: CatalogIndex | None,
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
    # STEWARD CATALOG FILTER (#227, column-based per #206). The FQID resolves
    # reg_meta-wide (we are past the resolve-success path, so an unresolved FQID
    # already got `fqid_unresolved` and returned — no double-report here), but a
    # FILTERED steward deployment supplies only a subset of that universe. Runs
    # AFTER the period check because admission compares RESOLVED delivery columns
    # (the binding's `resolved_columns`), which only the period resolution knows.
    # `index=None` (the `global` deployment) never emits either code, and an
    # UNRESOLVED variant skips the probe entirely: holdings are keyed BY variant,
    # so asking about one reg_meta itself does not know would answer "the steward
    # doesn't supply it" on top of the `fqid_unresolved` that variant already
    # earned — the same derivative noise the period probe skips for.
    # Admission keys on the source's variant coordinate and the LITERAL binding
    # FQID (a curated same_as sibling names a DIFFERENT column, so under
    # column-holdings semantics warning on it is correct, not a keying artifact).
    if index is not None and variant_ok:
        _check_steward_admission(
            binding.variable,
            source.register_variant,
            var_path,
            resolved_columns,
            index,
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

    Under §12 intersection semantics the binding is requested wherever it is
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

    # The clip is reported BEFORE any blocking finding, exactly as the
    # materializer records it before its ambiguity gate returns: a binding that
    # is both clipped and ambiguous surfaces both, so the researcher sees the
    # window the finding is stated against.
    #
    # `info`, not `warning`: the binding RESOLVED and is usable — the available
    # window orders fine. The steward index keys its binding-DROP on `warning`
    # level (catalog_index.py), so an `info` correctly keeps a clipped binding in
    # the index.
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
    index: CatalogIndex,
    issues: list[ValidationIssue],
) -> None:
    """Column-based steward admission (#206), scoped to the source's variant.

    An inventory mapping states a whole ``(register_variant, variable,
    representation)`` coordinate (§12), so the probe consults ``variant_coord``'s
    holdings and NOT the cross-variant union: a steward that maps `kon` only
    under `individer-15plus` does not supply it to a project sourcing
    `individer-16plus`, and admitting it would let an order through for a column
    the steward cannot deliver.

    Two distinct findings, both non-blocking ``warning``s (the "what would my
    project look like under steward X?" feature relies on them enumerating, not
    blocking):

    - ``fqid_outside_steward_catalog`` — the steward holds NO column of this
      concept under this variant.
    - ``representation_outside_steward_catalog`` — it holds the concept there,
      but not the column this binding resolves to; the message enumerates what
      the steward DOES hold ("SSYK at 1-digit only" is the actionable form of
      "not available").

    ``resolved_columns=None`` means the binding's own column is indeterminate
    (period/representation/ambiguity issues already reported) — only the
    FQID-level check can run; the column-level check stays silent."""
    held = index.held_columns_for_variant(variable, variant_coord)
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
    missing = resolved_columns - held
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
