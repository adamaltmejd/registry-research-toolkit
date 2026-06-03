"""Semantic (§6.8.3) validation — reg_meta-backed.

The third validation layer (§6.8.0): structural (§6.8.1, ``reg_schema``) and
namespaced-block (§6.8.2, owning packages) run first; this one resolves every
FQID in a *structurally valid* ``project_data.json`` against a live reg_meta
``Catalog``. It lives in the webapp — NOT ``reg_schema`` — because ``reg_schema``
is reg_meta-free by design (§6.8.1, MONA-amalgamatable); semantic rules need the
DB, so they belong where the DB is (the webapp backend, and any local tool that
has loaded reg_meta — REFACTOR_SPEC §6.8.3).

It emits the same frozen ``reg_schema.ValidationIssue`` shape the other layers
do (§6.8.0) — composition is tuple concatenation, no merge semantics. It takes a
``Catalog`` (never opens a connection): A5.2b-ii's ``POST /api/project/validate``
calls it per-request with an in-handler connection; the steward catalog load
(``stewards.py``) calls it at boot with the boot connection.

**Caller context (§6.8.3 "researcher-project vs steward-catalog").** The
``caller`` flag drives the level mapping. For the *researcher* path
(``POST /api/project/validate``) unresolved FQIDs are blocking ``error``s. For
the *steward-catalog load* path (FastAPI startup, §9.1) ``fqid_unresolved``,
``value_set_missing``, and ``period_outside_state_validity`` are downgraded
``error`` → ``warning`` so the deployment doesn't fail to start when reg_meta
evolves out from under a steward's committed catalog; the affected bindings are
then dropped from the in-memory index (``catalog_index.py``), but ``ok`` stays
True, so the caller must inspect the warnings, not just ``ok`` (§6.8.0).

Inputs are the ``reg_schema`` Pydantic models (``ProjectData`` / ``Source`` /
``Binding``), which the webapp constructs only AFTER ``validate_structural``
passes — so this layer assumes well-formed FQIDs / period grammar and resolves
them, rather than re-checking shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from reg_meta.errors import RegMetaError
from reg_meta.fqid import FqidError, parse
from reg_schema.validation import ValidationIssue, ValidationResult

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog, Period
    from reg_meta.fqid import Fqid
    from reg_schema.project_data import Binding, PeriodRange, ProjectData, Source

Caller = Literal["researcher", "steward"]

# §6.8.3 caller-context: these three codes downgrade error → warning on the
# steward-catalog load path so a deployment boots through reg_meta drift (the
# affected bindings drop from the in-memory index instead of crashing startup).
# The researcher path keeps them as errors. The split is a level *mapping* by
# caller, NOT a different rule set (§6.8.3 "same codes, different level mapping").
_STEWARD_DOWNGRADED: frozenset[str] = frozenset(
    {"fqid_unresolved", "value_set_missing", "period_outside_state_validity"}
)


def validate_semantic(
    project: ProjectData,
    catalog: Catalog,
    *,
    caller: Caller,
) -> ValidationResult:
    """Run §6.8.3 semantic rules over ``project`` against ``catalog``.

    Walks ``sources[*].register_variant``, each ``sources[*].bindings[*]``
    (``variable`` + ``value_set``), resolving against the live ``Catalog``.
    Returns a ``ValidationResult`` whose ``issues`` carry the §6.8.3 codes at the
    level dictated by ``caller`` (see module docstring). Never opens a connection
    — the caller owns the ``Catalog``'s lifetime.
    """
    issues: list[ValidationIssue] = []
    for s_idx, source in enumerate(project.sources):
        _check_source(source, s_idx, catalog, caller, issues)
    return ValidationResult(issues=tuple(issues))


def _issue(
    code: str,
    base_level: Literal["error", "warning", "info"],
    caller: Caller,
    path: str,
    message: str,
) -> ValidationIssue:
    """Build an issue, applying the §6.8.3 steward error→warning downgrade.

    Only the three ``_STEWARD_DOWNGRADED`` codes flip, and only on the steward
    path; ``info`` issues and the researcher path pass through unchanged."""
    level = base_level
    if caller == "steward" and base_level == "error" and code in _STEWARD_DOWNGRADED:
        level = "warning"
    return ValidationIssue(level=level, code=code, path=path, message=message)


def _check_source(
    source: Source,
    s_idx: int,
    catalog: Catalog,
    caller: Caller,
    issues: list[ValidationIssue],
) -> None:
    base = f"/sources/{s_idx}"

    # §6.8.3: the source's register_variant must resolve to a known variant. The
    # variant coordinate is `<provider>/<register>/<variant>` — NOT an FQID kind
    # (§5.2), so it can't go through `Catalog.resolve`. We resolve the
    # provider/register prefix to a known register and the variant slug to a
    # `register_variant` row via `list_variants` (the variant browse axis). A
    # missing register OR variant is `fqid_unresolved`.
    variant_ok = _check_register_variant(
        source.register_variant, base, catalog, caller, issues
    )

    for b_idx, binding in enumerate(source.bindings):
        _check_binding(
            binding, source, base, b_idx, variant_ok, catalog, caller, issues
        )


def _check_register_variant(
    register_variant: str,
    base: str,
    catalog: Catalog,
    caller: Caller,
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
                caller,
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
                caller,
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
                caller,
                path,
                f"variant {variant!r} is not a known variant of "
                f"{provider}/{register} (known: {sorted(variant_slugs)})",
            )
        )
        return False
    return True


def period_for_resolve(period: int | str | PeriodRange) -> Period:
    """Convert a `Source.period` (Pydantic) into the polymorphic `Period`
    `Catalog.resolve_at` expects (`int | str | dict`). A `PeriodRange` becomes a
    `{"from", "to"}` dict; int / str pass through (the `_default` sentinel rides
    through as a plain str — `resolve_at` treats it as no-period-filter)."""
    if isinstance(period, (int, str)):
        return period
    # PeriodRange: `from_` is the Python-safe alias of the wire key `from`.
    return {"from": period.from_, "to": period.to}


def _has_codelivered_versions(states) -> bool:  # noqa: ANN001 — reg_meta VariableState
    """§6.8.3 CO-DELIVERY: are ≥2 states with DISTINCT VALUE SETS (different
    ``value_set_id`` — not merely a different free-text version label) valid at the
    SAME instant (overlapping validity windows)? That is the genuine ambiguity: the
    same coordinate yields two different code-lists. Two states that share a
    ``value_set_id`` but carry different version labels are the SAME values under
    two names — NOT ambiguity (keying on the label would false-positive on ~71% of
    co-deliveries). Sequential states from a transition (non-overlapping windows)
    are drift (info), not co-delivery. O(n²) over the few states resolve_at returns;
    ``valid_from`` / ``valid_to`` are ISO ``YYYY-MM-DD`` strings (chronological).

    Note: post-curation the reg_meta build enforces one value set per
    ``(variable, variant, period)`` (the build's co-delivery curation + ``validate``
    invariant), so this should not fire in practice — it is a defensive backstop."""
    for i, a in enumerate(states):
        for b in states[i + 1 :]:
            if (
                a.value_set_id != b.value_set_id
                and a.valid_from <= b.valid_to
                and b.valid_from <= a.valid_to
            ):
                return True
    return False


def _check_binding(
    binding: Binding,
    source: Source,
    base: str,
    b_idx: int,
    variant_ok: bool,
    catalog: Catalog,
    caller: Caller,
    issues: list[ValidationIssue],
) -> None:
    bbase = f"{base}/bindings/{b_idx}"
    var_path = f"{bbase}/variable"

    # §6.8.3: the binding FQID must resolve to a known variable (following
    # `same_as` curated links — `Catalog.resolve` does that internally). The
    # binding FQID is a bare 3-segment variable (the `@version` pin is retired —
    # the value set is determined by the resolved `(variable, variant, period)`).
    try:
        parsed = parse(binding.variable)
    except FqidError:
        # Structurally valid input shouldn't reach here; treat as unresolved.
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                caller,
                var_path,
                f"binding variable {binding.variable!r} is not a parseable FQID",
            )
        )
        return
    try:
        catalog.resolve(parsed)
    except RegMetaError:
        issues.append(
            _issue(
                "fqid_unresolved",
                "error",
                caller,
                var_path,
                f"binding variable {binding.variable!r} resolves to no variable "
                "in reg_meta",
            )
        )
        # The variable doesn't resolve, so the PERIOD probe is meaningless — skip
        # it. The value_set (an independent `class/<slug>` FQID) can still be broken
        # on its own, so validate it before returning.
        _check_value_set(binding, bbase, catalog, caller, issues)
        return

    _check_binding_period(
        binding,
        source,
        bbase,
        var_path,
        variant_ok,
        parsed,
        catalog,
        caller,
        issues,
    )
    _check_value_set(binding, bbase, catalog, caller, issues)


def _check_binding_period(
    binding: Binding,
    source: Source,
    bbase: str,
    var_path: str,
    variant_ok: bool,
    parsed: Fqid,
    catalog: Catalog,
    caller: Caller,
    issues: list[ValidationIssue],
) -> None:
    """§6.8.3: the binding must resolve to a `variable_state` at the source's
    variant AND period. No covering state → `period_outside_state_validity`. A
    range period crossing a state transition → `binding_state_drifts_within_period`
    (info). The binding resolving to ≥2 DISTINCT value sets co-delivered in that
    period → `binding_value_set_version_ambiguous` (error). That last case is a
    defensive backstop: the reg_meta build enforces one value set per
    `(variable, variant, period)` (its co-delivery curation + `validate`
    invariant), so a clean catalog never trips it — there is no author-side pin."""
    # An unresolved variant already produced an `fqid_unresolved`; a period probe
    # against it would be derivative noise. Skip it.
    if not variant_ok:
        return
    variant = source.register_variant.split("/")[2]
    period = period_for_resolve(source.period)
    try:
        states = catalog.resolve_at(parsed, period, variant=variant)
    except RegMetaError:
        # resolve_at only raises when the binding FQID doesn't resolve — already
        # handled above — or on a malformed period (structurally pre-validated).
        # Either way nothing actionable to add here.
        return

    if not states:
        issues.append(
            _issue(
                "period_outside_state_validity",
                "error",
                caller,
                var_path,
                f"binding {binding.variable!r} has no state covering "
                f"{source.register_variant} at period {source.period!r}",
            )
        )
        return

    # §6.8.3 fold: CO-DELIVERY — ≥2 states with DISTINCT value sets (different
    # `value_set_id`) whose validity windows OVERLAP (valid at the same instant).
    # That breaks the `(variable, variant, period) → one value set` invariant, so
    # it is an error. `resolve_at` returns every state whose validity INTERSECTS the
    # period, so for a range / `_default` period the matched states may instead be
    # SEQUENTIAL across a transition (non-overlapping) — that's drift (info) below,
    # NOT ambiguity. There is no author-side pin: the fix is reg_meta build-time
    # curation (this should not fire against a curated catalog).
    if _has_codelivered_versions(states):
        # Distinct value sets at the same instant, labelled for legibility.
        labels: dict[int | None, str] = {}
        for s in states:
            labels.setdefault(s.value_set_id, s.value_set_version_label)
        issues.append(
            _issue(
                "binding_value_set_version_ambiguous",
                "error",
                caller,
                var_path,
                f"binding {binding.variable!r} resolves to several co-delivered "
                f"value sets {sorted(labels.values())} at {source.register_variant} "
                f"period {source.period!r}; a (variable, variant, period) must map "
                f"to one value set — this reg_meta build needs co-delivery curation",
            )
        )
        return

    # §6.8.3 drift (info): a range / `_default` period crossing a state transition
    # resolves to several SEQUENTIAL states (non-overlapping windows), possibly
    # differing on version label (a re-version) or shape — informational; the
    # resolver returns the per-state subsets at extract time. The co-delivered
    # (overlapping) version case was already the error above.
    if len(states) > 1:
        issues.append(
            _issue(
                "binding_state_drifts_within_period",
                "info",
                caller,
                var_path,
                f"binding {binding.variable!r} spans {len(states)} states across a "
                f"transition within period {source.period!r}",
            )
        )


def _check_value_set(
    binding: Binding,
    bbase: str,
    catalog: Catalog,
    caller: Caller,
    issues: list[ValidationIssue],
) -> None:
    """§6.8.3: a binding's `value_set` (a `class/<slug>` FQID) must resolve to a
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
                caller,
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
                caller,
                vs_path,
                f"value_set {binding.value_set!r} resolves to no classification "
                "in reg_meta",
            )
        )
