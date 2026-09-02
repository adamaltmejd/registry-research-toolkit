"""In-memory steward catalog index.

See DESIGN.md → Steward layering and the in-memory catalog index (stewards.py +
catalog_index.py). Built once at FastAPI startup from the steward's committed
``inventory.toml`` (``reg_meta.inventory``) and held on ``app.state`` for the
lifetime of the process. It is the filter that scopes a steward deployment to a
subset of reg_meta's universe: the validate endpoint and the variable-list
authoring endpoints (A5.2b-ii) consult it, and ``fqid_outside_steward_catalog``
(see DESIGN.md → Semantic validation (semantic.py)) fires when a researcher's
project references an FQID not in it.

The delivery inventory is the steward's SINGLE source of truth (REFACTOR_SPEC.md
§12): a `(register_variant, variable, representation)` coordinate is admitted iff
some inventory mapping states it. An inventory column with NO mapping admits
nothing — it stays in the physical coverage denominator without becoming
authorable or orderable.

Three maps (derived directly from the inventory's ``tables → columns →
mappings``):

- ``bindings_by_variant`` — ``register_variant coordinate`` (the 3-part
  ``<provider>/<register>/<variant>`` string) → ``frozenset`` of
  ``(binding FQID, resolved delivery column)`` pairs admitted under it.
  Admission is COLUMN-based (#206): a steward inventory is a statement of
  *holdings*, and holdings are physical delivery columns, not concepts —
  concept-level (bare-FQID) admission cannot express "this steward has SSYK,
  but only at the 1-digit level". The column side of each pair is the
  RESOLVED ``delivery_column_name``: a mapping's ``representation`` is exactly
  that canonical token (NOT the physical ``column.name``, which is the
  steward's own literal spelling), so an explicit representation is admitted
  verbatim with no DB access. A mapping's ``representation`` is ``None`` only
  when it means "the concept's SINGLE representation" (§12) — never a wildcard —
  so that case is resolved against the catalog over the table's edition bounds,
  which is what the researcher side compares against.
- ``periods_by_coordinate`` — the full §12 coordinate
  ``(register_variant, binding FQID, resolved delivery column)`` → the
  ascending, non-overlapping union of the EDITION BOUNDS of every table
  stating it. This is the coordinate's ADMITTED PERIOD: an inventory mapping
  states not just *what* the steward holds but *when*, and that "when" is
  per coordinate, not per register — one variant can be delivered 1990-2010 and
  its successor 2011-, in the same register. Abutting editions collapse (a
  column in a yearly table since 1990 is ONE interval, not thirty), disjoint
  ones do not: a coordinate delivered in 2018 and again in 2020 has a real 2019
  hole, and flattening that to an outer span is exactly the loss this map
  exists to prevent.
- ``period_range_by_register`` — ``register FQID`` (2-segment
  ``<provider>/<register>``) → the outer inclusive ISO ``(lo, hi)`` of its
  coordinates' intervals. The coarse, gap-free PROJECTION of
  ``periods_by_coordinate``, for UI hinting only — NOT a validity gate (the
  semantic validator's per-binding ``period_outside_state_validity`` is the
  gate).

The index also derives steward-filtered ``/api/stats`` counts. Variables de-dupe
by binding FQID (not delivery column), while registers come from the inventory's
period spans plus any admitted binding's parent register.

This is an INTERNAL dataclass — never a response body — so it is a plain stdlib
frozen ``@dataclass``, not Pydantic (see DESIGN.md → Pydantic boundary: only
response models are Pydantic; reg_webapp internals are dataclasses). The
``global`` deployment has NO inventory and therefore NO index (``None``); the
catalog endpoints pass through to reg_meta's full universe.

A ``representation = None`` mapping the catalog cannot resolve is DROPPED from
the index — it can't be authored against until the steward regenerates the
inventory against the current reg_meta. ``drift_warnings`` carries those misses
so ``/api/context`` can surface a "catalog drift" banner.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from reg_meta.catalog import CatalogSizes
from reg_meta.errors import RegMetaError
from reg_meta.fqid import snap_to_real_month_end
from reg_meta.inventory import edition_bounds

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog
    from reg_meta.inventory import ColumnMapping, DeliveryInventory

# One admitted §12 coordinate — `(register_variant, binding FQID, resolved
# delivery column)`. The `representation` slot holds the RESOLVED column for the
# same reason `bindings_by_variant` does (see module docstring), so the two maps
# are keyed alike.
Coordinate = tuple[str, str, str | None]

# An inclusive ISO `(lo, hi)` date interval, `reg_meta.inventory`'s currency.
Interval = tuple[str, str]


@dataclass(frozen=True)
class DriftWarning:
    """A single boot-time catalog-drift warning.

    A thin, JSON-serializable projection of the inventory mapping that could not
    be resolved and therefore dropped from the index — surfaced on
    ``/api/context`` so the SPA can show which coordinates the steward's
    committed inventory states but reg_meta no longer admits. Mirrors
    ``ValidationIssue`` minus ``level`` (these are all warnings by
    construction)."""

    code: str
    path: str
    message: str


@dataclass(frozen=True)
class CatalogIndex:
    """The in-memory steward catalog filter. Internal — never serialized.

    Built ONCE at boot and never mutated, so the derived projections (the held
    FQID / provider sets, the flattened admission pairs, the per-FQID held-column
    map) are ``functools.cached_property``: each rescans ``bindings_by_variant``
    only on first access, then the result is memoized. A register page probes
    ``admits`` once per concept-group member — hundreds of full ~5.7k-pair scans
    on the un-memoized class — so the flattened ``_admitted_pairs`` /
    ``_held_columns_by_fqid`` indexes turn those into O(1) lookups. ``cached_property``
    coexists with ``@dataclass(frozen=True)``: the cached value is written into
    ``__dict__`` (this class has NO ``__slots__``), bypassing the frozen
    ``__setattr__``; the generated ``__hash__`` / ``__eq__`` read the declared
    fields only, never the cached attributes, so neither is perturbed."""

    bindings_by_variant: dict[str, frozenset[tuple[str, str | None]]]
    periods_by_coordinate: dict[Coordinate, tuple[Interval, ...]]
    period_range_by_register: dict[str, tuple[str, str]]
    drift_warnings: tuple[DriftWarning, ...]

    @cached_property
    def catalog_period_span(self) -> tuple[int, int] | None:
        """Steward-wide year span for UI bounds — the outer years of
        ``period_range_by_register``.

        Every bound comes from a table EDITION, which is always one explicit
        finite period (never ``"_default"`` — ``reg_meta.inventory`` rejects
        that), so each bound's leading four characters are its year. ``None``
        only when the steward admits nothing, so there is no span to hint with.
        """
        years = [
            int(bound[:4])
            for bounds in self.period_range_by_register.values()
            for bound in bounds
        ]
        if not years:
            return None
        return (min(years), max(years))

    @cached_property
    def _admitted_pairs(self) -> frozenset[tuple[str, str | None]]:
        """Every admitted ``(fqid, resolved delivery column)`` pair, flattened
        across variants ONCE. Backs the O(1) ``admits`` membership probe (the hot
        per-member path on a register page)."""
        return frozenset(
            pair for bindings in self.bindings_by_variant.values() for pair in bindings
        )

    @cached_property
    def _held_columns_by_fqid(self) -> dict[str, frozenset[str | None]]:
        """The held delivery columns grouped by binding FQID, built ONCE. Backs the
        O(1) ``held_columns`` lookup."""
        by_fqid: dict[str, set[str | None]] = {}
        for fqid, column in self._admitted_pairs:
            by_fqid.setdefault(fqid, set()).add(column)
        return {fqid: frozenset(cols) for fqid, cols in by_fqid.items()}

    @cached_property
    def _held_columns_by_variant(self) -> dict[tuple[str, str], frozenset[str | None]]:
        """The held delivery columns grouped by ``(variant coordinate, FQID)``,
        built ONCE. Backs the O(1) ``held_columns_for_variant`` lookup."""
        by_key: dict[tuple[str, str], set[str | None]] = {}
        for variant_coord, bindings in self.bindings_by_variant.items():
            for fqid, column in bindings:
                by_key.setdefault((variant_coord, fqid), set()).add(column)
        return {key: frozenset(cols) for key, cols in by_key.items()}

    @cached_property
    def _variant_coords_by_register(self) -> dict[str, frozenset[str]]:
        """The non-empty variant coordinates grouped by their 2-segment register
        FQID, built ONCE. A variant slot admitting nothing is EXCLUDED, mirroring
        ``held_variant_coords_for_register``'s contract."""
        by_register: dict[str, set[str]] = {}
        for coord, bindings in self.bindings_by_variant.items():
            if bindings:
                register_fqid = "/".join(coord.split("/")[:2])
                by_register.setdefault(register_fqid, set()).add(coord)
        return {reg: frozenset(coords) for reg, coords in by_register.items()}

    def admits(self, fqid: str, column: str | None) -> bool:
        """True iff the ``(fqid, resolved delivery column)`` pair is admitted by
        ANY variant in the index (#206: admission is column-based — see module
        docstring). ``fqid`` is the bare 3-segment binding FQID (no ``@version``
        pin to normalize away — that grammar is retired); ``column`` is the
        RESOLVED ``delivery_column_name`` on the caller's side.

        DISCOVERY grain, deliberately variant-blind: the browse and search
        surfaces (#859) list what the steward holds *anywhere* and carry their
        own variant axis (``held_variant_coords_for_register``). A project's
        source names ONE variant, so its admission goes through the
        variant-scoped ``held_columns_for_variant`` instead."""
        return (fqid, column) in self._admitted_pairs

    def held_columns(self, fqid: str) -> frozenset[str | None]:
        """The delivery columns this steward holds for ``fqid`` across ALL
        variants — the discovery-grain union backing the browse/search column
        narrowing, with the same variant-blindness rationale as ``admits``."""
        return self._held_columns_by_fqid.get(fqid, frozenset())

    def held_columns_for_variant(
        self, fqid: str, variant_coord: str
    ) -> frozenset[str | None]:
        """The delivery columns this steward holds for ``fqid`` UNDER
        ``variant_coord`` — the admission-grain probe.

        A coordinate is admitted iff some inventory mapping states it, and a
        mapping states a ``register_variant`` (§12), so this consults that exact
        variant's holdings and never the cross-variant union: a steward whose
        inventory maps ``kon`` only under ``individer-15plus`` does NOT supply it
        to a project sourcing ``individer-16plus``, and admitting it would ship
        an order for a column the steward cannot deliver.

        Empty ⇔ the steward holds no column of the concept under this variant
        (the ``fqid_outside_steward_catalog`` case); non-empty without the
        researcher's resolved column is the ``representation_outside_steward_
        catalog`` case, and this set is what its message enumerates ("available
        there as … only")."""
        return self._held_columns_by_variant.get((variant_coord, fqid), frozenset())

    @cached_property
    def admitted_variable_fqids(self) -> frozenset[str]:
        """The bare binding FQIDs the steward holds, across all variants — the
        ``fqid`` side of every ``(fqid, column)`` pair. Browse-grain (column
        de-duped): the discovery surfaces (#859 browse + search) narrow their
        variable rows against this set. Column-grain admission for a known FQID
        is the separate ``admits`` / ``held_columns`` probe."""
        return frozenset(fqid for fqid, _column in self._admitted_pairs)

    @cached_property
    def held_register_fqids(self) -> frozenset[str]:
        """The 2-segment register FQIDs the steward holds: the registers in
        ``period_range_by_register`` UNIONED with the parent register of every
        admitted binding. Mirrors ``catalog_sizes``'s register derivation EXACTLY
        (keep the two consistent)."""
        registers = set(self.period_range_by_register)
        registers.update(
            "/".join(fqid.split("/")[:2]) for fqid in self.admitted_variable_fqids
        )
        return frozenset(registers)

    @cached_property
    def held_provider_slugs(self) -> frozenset[str]:
        """The provider slugs the steward holds — the first segment of each held
        register FQID. Backs the browse-root provider filter (#859)."""
        return frozenset(fqid.split("/", 1)[0] for fqid in self.held_register_fqids)

    def admits_register(self, register_fqid: str) -> bool:
        """True iff the steward holds the 2-segment ``register_fqid``."""
        return register_fqid in self.held_register_fqids

    def admits_provider(self, provider_slug: str) -> bool:
        """True iff the steward holds any register under ``provider_slug``."""
        return provider_slug in self.held_provider_slugs

    def held_variant_coords_for_register(self, register_fqid: str) -> frozenset[str]:
        """The variant coordinates (``provider/register/variant``) under
        ``register_fqid`` that admit ≥1 binding. A variant slot admitting nothing
        is EXCLUDED, so the variants endpoint (#859) lists only variants the
        steward actually holds data under."""
        return self._variant_coords_by_register.get(register_fqid, frozenset())

    def catalog_sizes(self) -> CatalogSizes:
        """Headline catalog counts for a filtered steward deployment.

        The index is column-based, so a single binding FQID can appear more than
        once (different variants or resolved delivery columns). The landing-page
        variable count is browse-grain, not column-grain, so it de-dupes by FQID.
        """
        return CatalogSizes(
            providers=len(self.held_provider_slugs),
            registers=len(self.held_register_fqids),
            variables=len(self.admitted_variable_fqids),
        )


def build_catalog_index(inventory: DeliveryInventory, catalog: Catalog) -> CatalogIndex:
    """Build the index from the steward's loaded delivery ``inventory``.

    Walks ``tables → columns → mappings``. Each mapping states one admitted
    ``(register_variant, variable, representation)`` coordinate, and the table it
    sits in states the EDITION over which the steward holds it — so admission is
    edition-aware by construction. A column with no mappings states nothing:
    it is inventoried for coverage but admits nothing (§12).

    ``catalog`` is needed ONLY to resolve a ``representation = None`` mapping
    (see ``_resolved_columns``); an explicit representation IS the resolved
    ``delivery_column_name``, so an all-explicit inventory (the committed swecov
    one) builds with zero DB access.
    """
    bindings_by_variant: dict[str, set[tuple[str, str | None]]] = {}
    intervals_by_coordinate: dict[Coordinate, list[Interval]] = {}
    drift: list[DriftWarning] = []

    for table in inventory.tables:
        # Validated at load (`InventoryTable._check_finite_edition`), so this
        # cannot raise here.
        bounds = edition_bounds(table.edition)
        for column in table.columns:
            for mapping in column.mappings:
                columns: frozenset[str | None] | None
                if mapping.representation is not None:
                    columns = frozenset({mapping.representation})
                else:
                    columns = _resolved_columns(mapping, bounds, catalog)
                if not columns:
                    drift.append(
                        _drift_warning(
                            table.id, column.name, mapping, unresolved=columns is None
                        )
                    )
                    continue
                variant_coord = mapping.register_variant
                fqid = str(mapping.variable)
                bindings_by_variant.setdefault(variant_coord, set()).update(
                    (fqid, resolved) for resolved in columns
                )
                for resolved in columns:
                    intervals_by_coordinate.setdefault(
                        (variant_coord, fqid, resolved), []
                    ).extend(bounds)

    periods_by_coordinate = {
        coordinate: _merged(intervals)
        for coordinate, intervals in intervals_by_coordinate.items()
    }
    return CatalogIndex(
        bindings_by_variant={k: frozenset(v) for k, v in bindings_by_variant.items()},
        periods_by_coordinate=periods_by_coordinate,
        period_range_by_register=_register_spans(periods_by_coordinate),
        drift_warnings=tuple(drift),
    )


def _merged(intervals: list[Interval]) -> tuple[Interval, ...]:
    """The ascending, non-overlapping union of inclusive ISO intervals.

    Intervals that overlap OR abut collapse; a gap of even one day keeps them
    apart. The abutment test needs real `date` arithmetic, so the upper bound is
    snapped first: `edition_bounds` inherits the period grammar's over-counted
    non-leap `YYYY-02-29` (intentional there — the resolver's overlap test is
    lexical). Only the comparison is snapped; the interval is STORED as
    `edition_bounds` produced it, so a stored bound and a project period still
    expand through the one grammar.
    """
    merged: list[Interval] = []
    for lo, hi in sorted(intervals):
        previous = merged[-1] if merged else None
        if previous is not None and lo <= _day_after(previous[1]):
            merged[-1] = (previous[0], max(previous[1], hi))
        else:
            merged.append((lo, hi))
    return tuple(merged)


def _day_after(iso: str) -> str:
    return (
        date.fromisoformat(snap_to_real_month_end(iso)) + timedelta(days=1)
    ).isoformat()


def _register_spans(
    periods_by_coordinate: dict[Coordinate, tuple[Interval, ...]],
) -> dict[str, tuple[str, str]]:
    """Collapse the per-coordinate intervals to one outer ISO span per 2-segment
    register FQID — ``period_range_by_register``. Lossy BY DESIGN (it is the UI
    hint, not the gate); ``periods_by_coordinate`` keeps the exact intervals."""
    spans: dict[str, tuple[str, str]] = {}
    for (variant_coord, _fqid, _column), intervals in periods_by_coordinate.items():
        register_fqid = "/".join(variant_coord.split("/")[:2])
        lo, hi = intervals[0][0], intervals[-1][1]
        span = spans.get(register_fqid)
        spans[register_fqid] = (
            (lo, hi) if span is None else (min(span[0], lo), max(span[1], hi))
        )
    return spans


def _resolved_columns(
    mapping: ColumnMapping,
    bounds: tuple[tuple[str, str], ...],
    catalog: Catalog,
) -> frozenset[str | None] | None:
    """The delivery columns a ``representation = None`` mapping denotes.

    ``None`` means "the concept's SINGLE representation" (§12), never a wildcard,
    so it must be resolved to the column(s) reg_meta actually delivers for that
    ``(variable, variant)`` over the table's edition — that is the token the
    researcher side resolves to and compares against (#206). A state genuinely
    carrying no ``delivery_column_name`` keeps ``None`` as its column token,
    matching a researcher resolution of the same state.

    Returns ``None`` when the FQID itself no longer resolves, and an EMPTY set
    when it resolves but no state covers the edition — the two drift codes the
    caller distinguishes.
    """
    variant = mapping.register_variant.split("/")[2]
    columns: set[str | None] = set()
    for lo, hi in bounds:
        try:
            states = catalog.resolve_at(
                mapping.variable, {"from": lo, "to": hi}, variant=variant
            )
        except RegMetaError:
            return None
        columns.update(state.delivery_column_name for state in states)
    return frozenset(columns)


def _drift_warning(
    table_id: str,
    column_name: str,
    mapping: ColumnMapping,
    *,
    unresolved: bool,
) -> DriftWarning:
    """The boot-time warning for an unresolvable mapping, located in the
    inventory's own author-facing spelling (`reg_meta.inventory._location`).
    ``unresolved`` distinguishes "the FQID resolves to nothing" from "it resolves
    but no state covers the edition"."""
    return DriftWarning(
        code="fqid_unresolved" if unresolved else "period_outside_state_validity",
        path=f"table[{table_id!r}].column[{column_name!r}]",
        message=(
            f"inventory maps {mapping.register_variant} {mapping.variable!s} "
            + (
                "but it resolves to no variable in reg_meta"
                if unresolved
                else "but reg_meta delivers no state for it over this table's edition"
            )
            + " — the coordinate is dropped from this deployment's catalog until "
            "the inventory is regenerated"
        ),
    )
