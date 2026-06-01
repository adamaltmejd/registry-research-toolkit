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


def _period_for_resolve(period: int | str | PeriodRange) -> Period:
    """Convert a `Source.period` (Pydantic) into the polymorphic `Period`
    `Catalog.resolve_at` expects (`int | str | dict`). A `PeriodRange` becomes a
    `{"from", "to"}` dict; int / str pass through (the `_default` sentinel rides
    through as a plain str — `resolve_at` treats it as no-period-filter)."""
    if isinstance(period, (int, str)):
        return period
    # PeriodRange: `from_` is the Python-safe alias of the wire key `from`.
    return {"from": period.from_, "to": period.to}


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
    # `same_as` curated links — `Catalog.resolve` does that internally). Split off
    # any `@<version>` pin first: `parse` rejects `@` (it's not an identity
    # segment, §5.2) — the pin is a value-set-version narrowing carried into the
    # period probe, not part of the variable's identity. Parsed once here and
    # threaded into the period probe (no re-parse).
    bare_fqid, pinned_version = _parse_binding_variable(binding.variable)
    try:
        parsed = parse(bare_fqid)
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
        # The variable doesn't resolve, so the period/value-set probes below are
        # meaningless — stop here (one issue per broken binding).
        _check_value_set(binding, bbase, catalog, caller, issues)
        return

    _check_binding_period(
        binding,
        source,
        bbase,
        var_path,
        variant_ok,
        parsed,
        pinned_version,
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
    pinned_version: str | None,
    catalog: Catalog,
    caller: Caller,
    issues: list[ValidationIssue],
) -> None:
    """§6.8.3: the binding must resolve to a `variable_state` at the source's
    variant AND period. No covering state → `period_outside_state_validity`. A
    range period crossing a state transition → `binding_state_drifts_within_period`
    (info). Multiple states matching a BARE binding because several value-set
    versions are co-delivered in that period (a §5.7 fold) →
    `binding_value_set_version_ambiguous` (error) — the author must pin the
    version with the FQID's `@<version>` suffix."""
    # An unresolved variant already produced an `fqid_unresolved`; a period probe
    # against it would be derivative noise. Skip it.
    if not variant_ok:
        return
    variant = source.register_variant.split("/")[2]
    period = _period_for_resolve(source.period)
    # `pinned_version` (the FQID's `@<version>` suffix, parsed once in
    # `_check_binding`) narrows a pinned binding to one state (§6.8.3), so it never
    # trips the ambiguity check below; `parsed` is the same bare FQID, reused.
    try:
        states = catalog.resolve_at(
            parsed,
            period,
            variant=variant,
            value_set_version=pinned_version,
        )
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

    # §6.8.3 fold: >1 state for a BARE (unpinned) binding means several value-set
    # versions are co-delivered in the bound period — the build must not silently
    # pick a coding, so this is an error directing the author to pin `@<version>`.
    # A pinned binding already narrowed to one state (value_set_version above).
    if pinned_version is None:
        versions = sorted({s.value_set_version_label for s in states})
        if len(versions) > 1:
            issues.append(
                _issue(
                    "binding_value_set_version_ambiguous",
                    "error",
                    caller,
                    var_path,
                    f"binding {binding.variable!r} matches multiple co-delivered "
                    f"value-set versions {versions} at {source.register_variant} "
                    f"period {source.period!r}; pin one with the FQID '@<version>' "
                    "suffix",
                )
            )
            return

    # §6.8.3 drift: a range period that crosses a state transition resolves to
    # several states differing on shape — informational (the resolver returns the
    # per-state subsets at extract time). Only meaningful for >1 state that is NOT
    # the version-ambiguity case above (those share the period but differ on
    # version, not transition).
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


def _parse_binding_variable(variable_fqid: str) -> tuple[str, str | None]:
    """Split a binding-leaf FQID into ``(bare_fqid, value_set_version)``.

    The leaf may carry a ``@<version>`` value-set-version pin. ``parse`` rejects
    ``@`` (the pin is a delivery detail, not part of the variable's identity,
    §5.2), so we strip it for resolution and return it separately to narrow the
    ``resolve_at`` probe. The structural layer guarantees at most one ``@`` on the
    leaf (§6.8.1). The single owner of this split — ``catalog_index`` imports it
    for the bare index key."""
    provider_register, _, leaf = variable_fqid.rpartition("/")
    bare_leaf, _, version = leaf.partition("@")
    bare = f"{provider_register}/{bare_leaf}" if provider_register else bare_leaf
    return bare, version or None
