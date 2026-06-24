"""In-memory steward catalog index.

See DESIGN.md → Steward layering and the in-memory catalog index (stewards.py +
catalog_index.py). Built once at FastAPI startup from a steward's validated
``steward.project_data.json`` and held on ``app.state`` for the lifetime
of the process. It is the filter that scopes a steward deployment to a subset of
reg_meta's universe: the validate endpoint and the variable-list authoring
endpoints (A5.2b-ii) consult it, and ``fqid_outside_steward_catalog`` (see
DESIGN.md → Semantic validation (semantic.py)) fires when a researcher's
project references an FQID not in it.

Two maps (derived directly from the steward project's ``sources[]``):

- ``bindings_by_variant`` — ``register_variant coordinate`` (the 3-part
  ``<provider>/<register>/<variant>`` string) → ``frozenset`` of
  ``(binding FQID, resolved delivery column)`` pairs admitted under it.
  Admission is COLUMN-based (#206): a steward catalog is a statement of
  *holdings*, and holdings are physical delivery columns, not concepts —
  concept-level (bare-FQID) admission cannot express "this steward has SSYK,
  but only at the 1-digit level". The column side of each pair is the
  RESOLVED ``delivery_column_name`` of the steward binding's states (its
  ``representation`` when pinned; every column its states deliver otherwise —
  a sequential rename inside the steward's period contributes one pair per
  column), never the raw ``representation`` string: resolving both sides at
  their own validation time compares equal across steward-authored-before-drift
  (a steward ``None`` vs. a researcher who must now pin). ``None`` is NOT a
  wildcard — it resolves to the unique column it denoted at authoring (and a
  state genuinely carrying no ``delivery_column_name`` keeps ``None`` as its
  column token, matching a researcher resolution of the same state).
- ``period_range_by_register`` — ``register FQID`` (2-segment
  ``<provider>/<register>``) → the ``(lo, hi)`` period span the steward declares
  for that register, as the raw period tokens seen on its sources. Best-effort
  span for UI hinting, NOT a validity gate (the semantic validator's per-binding
  ``period_outside_state_validity`` is the gate).

The index also derives steward-filtered ``/api/stats`` counts. Variables de-dupe
by binding FQID (not delivery column), while registers come from valid steward
sources plus any kept binding's parent register.

This is an INTERNAL dataclass — never a response body — so it is a plain stdlib
frozen ``@dataclass``, not Pydantic (see DESIGN.md → Pydantic boundary: only
response models are Pydantic; reg_webapp internals are dataclasses). The ``global`` deployment
(``has_catalog_filter=False``) has NO index (``None``); the catalog endpoints
pass through to reg_meta's full universe.

A binding that the semantic validator (steward-caller mode) could not resolve
(``fqid_unresolved`` / ``period_outside_state_validity`` / ``value_set_missing``,
all downgraded to ``warning`` at boot) is DROPPED from the index — it
can't be authored against until the steward updates the catalog. ``drift_warnings``
carries those downgraded warnings so ``/api/context`` can surface a "catalog
drift" banner.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.catalog import CatalogSizes
from reg_meta.fqid import parse

from reg_webapp.semantic import period_for_resolve, period_segments

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog
    from reg_schema.project_data import Binding, ProjectData, Source
    from reg_schema.validation import ValidationIssue


@dataclass(frozen=True)
class DriftWarning:
    """A single boot-time catalog-drift warning (steward downgrade).

    A thin, JSON-serializable projection of the steward-mode ``ValidationIssue``
    that caused a binding to drop from the index — surfaced on ``/api/context``
    so the SPA can show which FQIDs the steward's committed catalog references but
    reg_meta no longer admits. Mirrors ``ValidationIssue`` minus ``level`` (these
    are all warnings by construction)."""

    code: str
    path: str
    message: str


@dataclass(frozen=True)
class CatalogIndex:
    """The in-memory steward catalog filter. Internal — never serialized."""

    bindings_by_variant: dict[str, frozenset[tuple[str, str | None]]]
    period_range_by_register: dict[str, tuple[str, str]]
    drift_warnings: tuple[DriftWarning, ...]

    def admits(self, fqid: str, column: str | None) -> bool:
        """True iff the ``(fqid, resolved delivery column)`` pair is admitted by
        ANY variant in the index (#206: admission is column-based — see module
        docstring). ``fqid`` is the bare 3-segment binding FQID (no ``@version``
        pin to normalize away — that grammar is retired); ``column`` is the
        RESOLVED ``delivery_column_name`` on the caller's side, never the raw
        ``representation`` string. Backs ``fqid_outside_steward_catalog`` /
        ``representation_outside_steward_catalog`` (semantic.py)."""
        return any(
            (fqid, column) in bindings for bindings in self.bindings_by_variant.values()
        )

    def held_columns(self, fqid: str) -> frozenset[str | None]:
        """The delivery columns this steward holds for ``fqid``, across all
        variants. Empty ⇔ the FQID is not in the catalog at all (the
        ``fqid_outside_steward_catalog`` case); non-empty without containing the
        researcher's resolved column is the ``representation_outside_steward_
        catalog`` case, and this set is what its message enumerates ("available
        from this steward as … only")."""
        return frozenset(
            column
            for bindings in self.bindings_by_variant.values()
            for f, column in bindings
            if f == fqid
        )

    def catalog_sizes(self) -> CatalogSizes:
        """Headline catalog counts for a filtered steward deployment.

        The index is column-based, so a single binding FQID can appear more than
        once (different variants or resolved delivery columns). The landing-page
        variable count is browse-grain, not column-grain, so it de-dupes by FQID.
        ``period_range_by_register`` contributes valid source registers even if
        every binding under one drift-dropped from the authorable set."""
        variable_fqids = {
            fqid
            for bindings in self.bindings_by_variant.values()
            for fqid, _column in bindings
        }
        register_fqids = set(self.period_range_by_register)
        register_fqids.update("/".join(fqid.split("/")[:2]) for fqid in variable_fqids)
        provider_slugs = {fqid.split("/", 1)[0] for fqid in register_fqids}
        return CatalogSizes(
            providers=len(provider_slugs),
            registers=len(register_fqids),
            variables=len(variable_fqids),
        )


def _dropped_binding_paths(warnings: tuple[ValidationIssue, ...]) -> set[str]:
    """JSON-pointer prefixes of bindings the steward-mode validator could not
    resolve. A binding warning has path ``/sources/<i>/bindings/<j>/variable`` (or
    ``…/value_set``); we key on the ``…/bindings/<j>`` prefix so a binding is
    dropped whether its variable OR its value_set failed to resolve.

    An UNRESOLVED ``register_variant`` (path ``/sources/<i>/register_variant``)
    drops the whole source: every binding under it is unauthorable. We record the
    source prefix so all its bindings drop."""
    dropped: set[str] = set()
    for issue in warnings:
        path = issue.path
        marker = "/bindings/"
        if marker in path:
            # …/sources/<i>/bindings/<j>/<field> → …/sources/<i>/bindings/<j>
            head, _, tail = path.partition(marker)
            j = tail.split("/", 1)[0]
            dropped.add(f"{head}{marker}{j}")
        elif path.endswith("/register_variant"):
            # The whole source is unauthorable; record its prefix.
            dropped.add(path.rsplit("/register_variant", 1)[0])
    return dropped


def _is_dropped(source_base: str, binding_base: str, dropped: set[str]) -> bool:
    """A binding drops if its own ``/bindings/<j>`` prefix OR its parent source
    prefix is in ``dropped`` (an unresolved register_variant drops every binding
    under the source)."""
    return binding_base in dropped or source_base in dropped


def build_catalog_index(
    project: ProjectData,
    issues: tuple[ValidationIssue, ...],
    catalog: Catalog,
) -> CatalogIndex:
    """Build the index from a validated steward ``project`` plus the
    steward-mode semantic ``issues`` it produced, resolving each kept binding's
    delivery columns against ``catalog`` (#206: the index stores
    ``(FQID, resolved column)`` pairs, so building needs the same live
    ``Catalog`` the validation ran against — the boot connection).

    ``project`` has already passed ``validate_structural`` (well-formed FQIDs /
    period grammar) and ``validate_semantic`` in steward-caller mode. We walk its
    ``sources``, skipping any binding (or whole source) flagged as drift, and
    accumulate the two maps.

    Only ``warning``-level issues mark a DROP (and populate ``drift_warnings``):
    those are exactly the steward-downgraded resolution failures
    (``fqid_unresolved`` / ``value_set_missing`` / ``period_outside_state_validity``
    / ``binding_representation_unknown``) — the FQID / value-set / period / pinned
    representation this catalog references that reg_meta no longer admits. An
    ``info`` ``binding_state_drifts_within_period`` (the binding RESOLVED, it just
    spans a transition or its representation under-covers the range) and a
    non-downgraded ``error`` ``binding_value_set_version_ambiguous`` (kept an error
    — a researcher-author-time concern, not catalog drift) must NOT drop
    the binding; the steward filter keeps it and the researcher path enforces it."""
    warnings = tuple(i for i in issues if i.level == "warning")
    dropped = _dropped_binding_paths(warnings)
    bindings_by_variant: dict[str, set[tuple[str, str | None]]] = {}
    periods_by_register: dict[str, list[str]] = {}

    for s_idx, source in enumerate(project.sources):
        source_base = f"/sources/{s_idx}"
        variant_coord = source.register_variant
        # Always declare the variant slot (a dropped-variant source maps to an
        # empty set — declared, but admits nothing).
        variant_bindings = bindings_by_variant.setdefault(variant_coord, set())
        if source_base in dropped:
            # Whole source dropped (unresolved register_variant): not authorable, so
            # it must NOT contribute to the register's period span either.
            continue
        register_fqid = "/".join(variant_coord.split("/")[:2])
        periods_by_register.setdefault(register_fqid, []).append(_period_token(source))
        for b_idx, binding in enumerate(source.bindings):
            binding_base = f"{source_base}/bindings/{b_idx}"
            if _is_dropped(source_base, binding_base, dropped):
                continue
            for column in _resolved_columns(binding, source, catalog):
                variant_bindings.add((binding.variable, column))

    return CatalogIndex(
        bindings_by_variant={k: frozenset(v) for k, v in bindings_by_variant.items()},
        period_range_by_register={
            reg: (min(tokens), max(tokens))
            for reg, tokens in periods_by_register.items()
        },
        drift_warnings=tuple(
            DriftWarning(code=w.code, path=w.path, message=w.message) for w in warnings
        ),
    )


def _period_token(source) -> str:  # noqa: ANN001 — reg_schema Source (TYPE_CHECKING-only import)
    """A comparable string token for a source's period, for the best-effort
    register span. A bare int/str period is its own token; a range collapses to
    its endpoints' span via ``from``. ISO/period tokens sort chronologically as
    strings only loosely (mixed grammars don't), so this is a UI hint, never a
    gate — see the module docstring."""
    period = source.period
    if isinstance(period, tuple):
        # #307 list form: structurally sorted ascending, so the first segment
        # carries the lower-bound token.
        period = period[0]
    if isinstance(period, (int, str)):
        return str(period)
    # PeriodRange: use the `from_` endpoint as the lower-bound token.
    return str(period.from_)


def _resolved_columns(
    binding: Binding, source: Source, catalog: Catalog
) -> frozenset[str | None]:
    """The delivery columns a KEPT steward binding holds: the distinct
    ``delivery_column_name``s of its states at the source's (variant, period),
    narrowed to ``binding.representation`` when pinned. Mirrors the researcher
    side's resolution in ``semantic._check_binding_period`` so the two sides
    compare on the same token (#206).

    A kept binding MUST resolve here: the steward-mode validator just ran
    against the SAME ``catalog`` (one boot connection), every resolution
    failure was downgraded to a warning that dropped the binding before this
    is called (a #307 LIST period warns per uncovered segment, so kept means
    EVERY segment resolves), and ``load_catalog_index`` fails fast on residual
    errors. An empty resolution therefore means the index build and the
    validation disagree — a bug, not drift — so fail fast rather than silently
    admitting nothing."""
    variant = source.register_variant.split("/")[2]
    columns: set[str | None] = set()
    for segment in period_segments(source.period):
        states = catalog.resolve_at(
            parse(binding.variable), period_for_resolve(segment), variant=variant
        )
        if binding.representation is not None:
            states = [
                s for s in states if s.delivery_column_name == binding.representation
            ]
        if not states:
            raise ValueError(
                f"steward binding {binding.variable!r} passed steward-mode "
                f"validation but resolved to no states at index build "
                f"({source.register_variant}, period segment {segment!r} of "
                f"{source.period!r}, representation {binding.representation!r})"
            )
        columns.update(s.delivery_column_name for s in states)
    return frozenset(columns)
