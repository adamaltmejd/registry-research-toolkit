"""Semantic validation — reg_meta-backed.

See DESIGN.md → Semantic validation (semantic.py). The third validation layer:
structural (``reg_schema``) and namespaced-block (owning packages) run first;
this one resolves every
FQID in a *structurally valid* ``project_data.json`` against a live reg_meta
``Catalog``. It lives in the webapp — NOT ``reg_schema`` — because ``reg_schema``
is reg_meta-free by design (MONA-amalgamatable); semantic rules need the
DB, so they belong where the DB is (the webapp backend, and any local tool that
has loaded reg_meta).

It emits the same frozen ``reg_schema.ValidationIssue`` shape the other layers
do — composition is tuple concatenation, no merge semantics. It takes a
``Catalog`` (never opens a connection): A5.2b-ii's ``POST /api/project/validate``
calls it per-request with an in-handler connection; the steward catalog load
(``stewards.py``) calls it at boot with the boot connection.

**Caller context (researcher-project vs steward-catalog).** The
``caller`` flag drives the level mapping. For the *researcher* path
(``POST /api/project/validate``) unresolved FQIDs are blocking ``error``s. For
the *steward-catalog load* path (FastAPI startup) ``fqid_unresolved``,
``value_set_missing``, ``period_outside_state_validity``, and
``binding_representation_unknown`` are downgraded ``error`` → ``warning`` so the
deployment doesn't fail to start when reg_meta evolves out from under a steward's
committed catalog; the affected bindings are then dropped from the in-memory
index (``catalog_index.py``), but ``ok`` stays True, so the caller must inspect
the warnings, not just ``ok``.

The *researcher* path additionally emits ``fqid_outside_steward_catalog`` (a
non-blocking ``warning``) when an ``index`` (the loaded ``CatalogIndex``) is
supplied and a RESOLVED FQID falls outside it — the column is real reg_meta-wide
but this filtered deployment does not supply it. The steward-catalog load path
passes NO ``index`` (it BUILDS the index from its own validated project
afterward), and the ``global`` deployment's ``index`` is ``None`` (no filter), so
neither ever emits the code.

Inputs are the ``reg_schema`` Pydantic models (``ProjectData`` / ``Source`` /
``Binding``), which the webapp constructs only AFTER ``validate_structural``
passes — so this layer assumes well-formed FQIDs / period grammar and resolves
them, rather than re-checking shape. In particular, calendar-day validity of the
AUTHOR-supplied period endpoints (rejecting an impossible author day like
``2019-02-29``) is a STRUCTURAL guarantee (see reg_schema/DESIGN.md → Structural
rules and issue codes) — every caller runs structural
first and short-circuits before semantic — so this layer no longer pre-checks it.
What structural does NOT guarantee is the SYNTHESIZED upper bound: a non-leap
``YYYY-02`` month token expands to an over-counted ``-02-29`` ``hi`` (intentional
in reg_meta for lexical interval overlap), so before the gap math does real
``date`` arithmetic this layer snaps that bound to the real month-end
(``_snap_to_real_month_end``).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

from reg_meta.errors import RegMetaError
from reg_meta.fqid import FqidError, parse, period_token_to_bounds
from reg_schema.validation import ValidationIssue, ValidationResult

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog, Period
    from reg_meta.fqid import Fqid
    from reg_schema.project_data import Binding, PeriodRange, ProjectData, Source

    from reg_webapp.catalog_index import CatalogIndex

Caller = Literal["researcher", "steward"]

# Caller-context: these codes downgrade error → warning on the
# steward-catalog load path so a deployment boots through reg_meta drift (the
# affected bindings drop from the in-memory index instead of crashing startup).
# The researcher path keeps them as errors. The split is a level *mapping* by
# caller, NOT a different rule set (same codes, different level mapping).
# `binding_representation_unknown` belongs here for the same reason as
# `period_outside_state_validity`: a steward pinned a representation that a newer
# reg_meta build no longer delivers as a column — drift, not an author error.
# `binding_value_set_version_ambiguous` deliberately stays strict (an author-time
# choice, not drift).
_STEWARD_DOWNGRADED: frozenset[str] = frozenset(
    {
        "fqid_unresolved",
        "value_set_missing",
        "period_outside_state_validity",
        "binding_representation_unknown",
    }
)


def validate_semantic(
    project: ProjectData,
    catalog: Catalog,
    *,
    caller: Caller,
    index: CatalogIndex | None = None,
) -> ValidationResult:
    """Run semantic rules over ``project`` against ``catalog``.

    Walks ``sources[*].register_variant``, each ``sources[*].bindings[*]``
    (``variable`` + ``value_set``), resolving against the live ``Catalog``.
    Returns a ``ValidationResult`` whose ``issues`` carry the codes at the
    level dictated by ``caller`` (see module docstring). Never opens a connection
    — the caller owns the ``Catalog``'s lifetime.

    ``index`` is the loaded steward ``CatalogIndex`` (researcher path only): when
    supplied, a RESOLVED FQID outside it yields ``fqid_outside_steward_catalog``
    (warning). ``None`` (the steward-load path and the ``global`` deployment) never
    emits it.
    """
    issues: list[ValidationIssue] = []
    for s_idx, source in enumerate(project.sources):
        _check_source(source, s_idx, catalog, caller, index, issues)
    return ValidationResult(issues=tuple(issues))


def _issue(
    code: str,
    base_level: Literal["error", "warning", "info"],
    caller: Caller,
    path: str,
    message: str,
) -> ValidationIssue:
    """Build an issue, applying the steward error→warning downgrade.

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
    variant_ok = _check_register_variant(
        source.register_variant, base, catalog, caller, issues
    )

    for b_idx, binding in enumerate(source.bindings):
        _check_binding(
            binding, source, base, b_idx, variant_ok, catalog, caller, index, issues
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


def period_display(period: int | str | PeriodRange) -> str:
    """Human/wire string for a ``Source.period`` — for ISSUE MESSAGES and the
    order manifest, never a Python ``repr``.

    A ``PeriodRange`` renders as ``"<from>..<to>"`` (literal ``..``, matching the
    ``?period=`` range form a researcher already sees in the URL); int / str
    (incl. the ``"_default"`` sentinel) → ``str()``. We deliberately use the wire
    grammar rather than the ``repr`` so messages that travel through the API to
    CLI consumers and the SPA findings panel read as ``2015..2020``, not
    ``PeriodRange(from_=2015, to=2020)``."""
    if isinstance(period, (int, str)):
        return str(period)
    # PeriodRange: `from_` is the Python-safe alias of the wire key `from`.
    return f"{period.from_}..{period.to}"


def period_for_resolve(period: int | str | PeriodRange) -> Period:
    """Convert a `Source.period` (Pydantic) into the polymorphic `Period`
    `Catalog.resolve_at` expects (`int | str | dict`). A `PeriodRange` becomes a
    `{"from", "to"}` dict; int / str pass through (the `_default` sentinel rides
    through as a plain str — `resolve_at` treats it as no-period-filter)."""
    if isinstance(period, (int, str)):
        return period
    # PeriodRange: `from_` is the Python-safe alias of the wire key `from`.
    return {"from": period.from_, "to": period.to}


def _endpoint_bounds(endpoint: int | str) -> tuple[str, str]:
    """ISO `(lo, hi)` for one `PeriodRange` endpoint — a bare year (int) or a
    period token (str). Mirrors reg_meta's own range-endpoint expansion (the int
    arm is the documented year case; tokens defer to `period_token_to_bounds`,
    the single source of truth for the period grammar)."""
    if isinstance(endpoint, int):
        return f"{endpoint:04d}-01-01", f"{endpoint:04d}-12-31"
    return period_token_to_bounds(endpoint)


def _snap_to_real_month_end(iso: str) -> str:
    """Snap a synthesized upper bound to a REAL calendar date.

    `period_token_to_bounds` over-counts February's synthesized upper bound to
    day 29 (`_MONTH_LAST_DAY["02"]`) regardless of leap year — intentional and
    harmless for reg_meta's LEXICAL ISO-string interval overlap, but this layer
    does real `date` arithmetic (`_range_coverage_gaps`), where a non-leap
    `2019-02-29` raises `ValueError`. The only token whose synthesized `hi` is a
    non-real date is a non-leap `YYYY-02` month token, so only `YYYY-02-29` in a
    non-leap year can reach the fallback; snapping it to `-02-28` is also MORE
    correct (a window through "Feb 2019" really ends Feb 28, and it avoids a
    spurious 1-day phantom gap). Author-supplied `YYYY-MM-DD` days are already
    calendar-valid (structural guarantee), so they pass the try arm."""
    try:
        date.fromisoformat(iso)
    except ValueError:
        return iso[:8] + "28"
    return iso


def _requested_range_bounds(period: PeriodRange) -> tuple[str, str]:
    """The inclusive ISO `[lo, hi]` the author asked for with an explicit range:
    `lo` from the `from` endpoint, `hi` from the `to` endpoint. Structural
    validation already guaranteed `from <= to` and that each AUTHOR-supplied
    endpoint is a real calendar date (structural guarantee). The SYNTHESIZED upper bound may
    still over-count a non-leap February to day 29 (intentional in reg_meta for
    lexical interval overlap), so `hi` is snapped to the real month-end before the
    gap math does real `date` arithmetic on it."""
    lo, _ = _endpoint_bounds(period.from_)
    _, hi = _endpoint_bounds(period.to)
    return lo, _snap_to_real_month_end(hi)


def _has_codelivered_versions(states) -> bool:  # noqa: ANN001 — reg_meta VariableState
    """CO-DELIVERY: are ≥2 states with DISTINCT VALUE SETS (different
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


def _coexisting_columns(states) -> list[str]:  # noqa: ANN001 — reg_meta VariableState
    """REPRESENTATION: the distinct delivery columns that are valid at the
    SAME instant (overlapping windows) — genuine parallel representations the
    binding must choose between (SSYK 3/4/5-digit, age brackets). Distinct columns
    in NON-overlapping windows are a SEQUENTIAL rename across the period (column A
    until 2014, renamed B from 2015) — that is drift, not co-existence, so it must
    NOT demand a `representation`. Mirrors `_has_codelivered_versions` but keys on
    `delivery_column_name` rather than `value_set_id`. O(n²) over the few states
    `resolve_at` returns; ISO `YYYY-MM-DD` strings compare chronologically."""
    cols: set[str] = set()
    for i, a in enumerate(states):
        for b in states[i + 1 :]:
            if (
                a.delivery_column_name
                and b.delivery_column_name
                and a.delivery_column_name != b.delivery_column_name
                and a.valid_from <= b.valid_to
                and b.valid_from <= a.valid_to
            ):
                cols.add(a.delivery_column_name)
                cols.add(b.delivery_column_name)
    return sorted(cols)


def _range_coverage_gaps(
    states,  # noqa: ANN001 — reg_meta VariableState
    lo: str,
    hi: str,
) -> list[tuple[str, str]]:
    """The sub-intervals of the requested ISO range ``[lo, hi]`` that NO state
    covers. Empty when the union of the states' validity windows spans the whole
    range — i.e. the concept is fully delivered across the bound period.

    Each state's validity is clamped to ``[lo, hi]`` and the clamped intervals
    are merged; any uncovered span (including a leading gap before the first
    state, a trailing gap after the last, or an internal gap between two
    non-adjacent states) is returned as an inclusive ``(gap_lo, gap_hi)`` pair.
    Day-adjacent windows (e.g. ``..2013-12-31`` then ``2014-01-01..``) leave no
    gap. Dates are real calendar ISO ``YYYY-MM-DD``; ``9999-12-31`` is the
    open-ended sentinel but, clamped to ``hi``, never overflows ``date`` math."""
    one_day = timedelta(days=1)
    windows = sorted(
        (max(s.valid_from, lo), min(s.valid_to, hi))
        for s in states
        if s.valid_from <= hi and s.valid_to >= lo
    )
    gaps: list[tuple[str, str]] = []
    cursor = date.fromisoformat(lo)
    for w_lo, w_hi in windows:
        start = date.fromisoformat(w_lo)
        if start > cursor:
            gaps.append((cursor.isoformat(), (start - one_day).isoformat()))
        cursor = max(cursor, date.fromisoformat(w_hi) + one_day)
    end = date.fromisoformat(hi)
    if cursor <= end:
        gaps.append((cursor.isoformat(), hi))
    return gaps


def _check_binding(
    binding: Binding,
    source: Source,
    base: str,
    b_idx: int,
    variant_ok: bool,
    catalog: Catalog,
    caller: Caller,
    index: CatalogIndex | None,
    issues: list[ValidationIssue],
) -> None:
    bbase = f"{base}/bindings/{b_idx}"
    var_path = f"{bbase}/variable"

    # The binding FQID must resolve to a known variable (following
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

    # STEWARD CATALOG FILTER (#227). The FQID resolves reg_meta-wide (we are past
    # the resolve-success path, so an unresolved FQID already got `fqid_unresolved`
    # and returned — no double-report here), but a FILTERED steward deployment
    # supplies only a subset of that universe. A resolved FQID outside the loaded
    # `index` is a non-blocking `warning`: the column is real but unavailable in
    # THIS deployment — also the deliberate "what would my project look like under
    # steward X?" feature. `index=None` (the steward-load path AND the `global`
    # deployment) never emits it. Admission is keyed on the LITERAL binding FQID and
    # is variant-agnostic per the shipped `admits` probe; precise per-variant /
    # value_set / same_as-aware keying is deferred to #206 — so this deliberately
    # does NOT widen to value_sets / register_variants.
    if index is not None and not index.admits(binding.variable):
        issues.append(
            _issue(
                "fqid_outside_steward_catalog",
                "warning",
                caller,
                var_path,
                f"binding {binding.variable!r} resolves in reg_meta but is outside "
                "this deployment's steward catalog — the steward does not supply it",
            )
        )

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
    """The binding must resolve to a `variable_state` at the source's
    variant AND period. Endpoints are already real calendar dates here (structural
    guarantee — see module docstring). No covering state →
    `period_outside_state_validity`. An explicit range only PARTIALLY covered by
    the concept's states (a gap NO column delivers) → `range_period_partially_covered`
    (info). A range period crossing a state transition →
    `binding_state_drifts_within_period` (info). The binding resolving to ≥2 DISTINCT
    value sets co-delivered in that period → `binding_value_set_version_ambiguous`
    (error). That last case is a defensive backstop: the reg_meta build enforces one
    value set per `(variable, variant, period)` (its co-delivery curation + `validate`
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
                f"{source.register_variant} at period "
                f"{period_display(source.period)}",
            )
        )
        return

    # PARTIAL RANGE COVERAGE (the whole-concept-under-coverage case). The
    # author named an explicit `[from, to]`; `resolve_at` returned the states that
    # INTERSECT it, but their union may leave a sub-range NO column delivers (e.g.
    # SSYK first delivered 2014 under a `from:2010,to:2020` binding → 2010–2013 has
    # no data at all). Zero coverage is already `period_outside_state_validity`
    # above; this fires only on a PROPER gap inside the requested range. Scoped to
    # an explicit `PeriodRange`: a point/token period is a single instant (no
    # requested span to under-cover) and `_default` means "the full history",
    # which has no author-requested window to compare against. This is distinct
    # from #204's `binding_state_drifts_within_period`, which is about the CHOSEN
    # representation under-covering vs a SIBLING column that DOES deliver the gap.
    #
    # `info`, not `warning`: the binding RESOLVED and is usable (the covered
    # sub-range extracts fine) — the gap is a caveat, like the sibling drift codes.
    # The steward index keys its binding-DROP on `warning` level (catalog_index.py),
    # so an `info` correctly keeps a partially-covered binding in the index.
    if not isinstance(source.period, (int, str)):
        # Author endpoints are calendar-valid (structural guarantee);
        # `_requested_range_bounds` additionally snaps the synthesized upper bound
        # (a non-leap `YYYY-02` over-counts to `-02-29`) to the real month-end, so
        # the real `date` arithmetic in `_range_coverage_gaps` can't raise.
        lo, hi = _requested_range_bounds(source.period)
        gaps = _range_coverage_gaps(states, lo, hi)
        if gaps:
            spans = ", ".join(f"{g_lo}..{g_hi}" for g_lo, g_hi in gaps)
            issues.append(
                _issue(
                    "range_period_partially_covered",
                    "info",
                    caller,
                    var_path,
                    f"binding {binding.variable!r} covers only part of requested "
                    f"range {period_display(source.period)} at "
                    f"{source.register_variant}; no state delivers {spans}",
                )
            )

    # REPRESENTATION. A FQID names one concept; the reg_meta build enforces
    # one value set per `(variable, variant, period, delivery_column)`, but a
    # concept may carry several co-existing delivery columns — parallel
    # representations (SSYK 3/4/5-digit, age brackets). `binding.representation`
    # selects one by its `delivery_column_name`.
    if binding.representation is not None:
        matched = [
            s for s in states if s.delivery_column_name == binding.representation
        ]
        if not matched:
            avail = sorted(
                {s.delivery_column_name for s in states if s.delivery_column_name}
            )
            issues.append(
                _issue(
                    "binding_representation_unknown",
                    "error",
                    caller,
                    var_path,
                    f"binding {binding.variable!r} representation "
                    f"{binding.representation!r} is not a delivery column at "
                    f"{source.register_variant} period "
                    f"{period_display(source.period)} (available: {avail})",
                )
            )
            return
        # The chosen column may not span the whole MULTI-period: resolve_at returns
        # only intersecting states, so narrowing to `matched` can silently drop a
        # sub-range the column doesn't cover (e.g. SSYK5 from 2014 under a 2010–2020
        # binding → 2010–2013 lost). Surface it as info. A point int period is a
        # single instant (no gap); `_default` returns the FULL state history, so it
        # CAN under-cover just like a PeriodRange and must be checked too.
        is_multi_period = (
            not isinstance(source.period, (int, str)) or source.period == "_default"
        )
        if is_multi_period and (
            min(s.valid_from for s in matched) > min(s.valid_from for s in states)
            or max(s.valid_to for s in matched) < max(s.valid_to for s in states)
        ):
            issues.append(
                _issue(
                    "binding_state_drifts_within_period",
                    "info",
                    caller,
                    var_path,
                    f"binding {binding.variable!r} representation "
                    f"{binding.representation!r} covers only part of period "
                    f"{period_display(source.period)} at {source.register_variant}; "
                    f"the rest of the range has no state for that column",
                )
            )
        states = matched

    # ≥2 delivery columns CO-EXISTING (overlapping windows) and no `representation`
    # chosen → the binding is AMBIGUOUS: it would extract more than one column. The
    # author must pick one (the SPA offers a chooser); this is where the retired
    # `@version` pin's job now lives, keyed on the delivery column. Distinct columns
    # in NON-overlapping windows (a sequential rename) are NOT ambiguous — they fall
    # through to the drift-info case below. (When `representation` is set, `states`
    # is one column, so `coexisting` is empty and this never fires.)
    coexisting = _coexisting_columns(states)
    if len(coexisting) > 1:
        issues.append(
            _issue(
                "binding_value_set_version_ambiguous",
                "error",
                caller,
                var_path,
                f"binding {binding.variable!r} resolves to {len(coexisting)} "
                f"co-existing representations {coexisting} at "
                f"{source.register_variant} period {period_display(source.period)}; "
                f"set `representation` to one of them",
            )
        )
        return

    # Backstop: distinct value sets on ONE column at the same instant — a reg_meta
    # build co-delivery the curation missed (the build `validate` invariant should
    # make this unreachable against a clean catalog).
    if _has_codelivered_versions(states):
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
                f"value sets {sorted(labels.values())} on one column at "
                f"{source.register_variant} period {period_display(source.period)} — "
                f"this reg_meta build needs co-delivery curation",
            )
        )
        return

    # Drift (info): a range / `_default` period crossing a state transition
    # resolves to several SEQUENTIAL states (non-overlapping windows) on ONE column,
    # possibly differing on version label (a re-version) or shape — informational;
    # the resolver returns the per-state subsets at extract time.
    if len(states) > 1:
        issues.append(
            _issue(
                "binding_state_drifts_within_period",
                "info",
                caller,
                var_path,
                f"binding {binding.variable!r} spans {len(states)} states across a "
                f"transition within period {period_display(source.period)}",
            )
        )


def _check_value_set(
    binding: Binding,
    bbase: str,
    catalog: Catalog,
    caller: Caller,
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
