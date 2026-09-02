"""Inventory ↔ catalog-DB consistency gate (REFACTOR_SPEC.md §12).

`inventory.py` validates an authored inventory's STRUCTURE with no DB open.
This module is §12's other half — the "standing build/CI gate, not a one-off
check" that every mapping's `(register_variant, variable FQID, representation)`
resolves against the catalog DB the deployment actually serves. Pre-v1 slug
churn is legal, so a catalog release that renames a slug strands inventory
mappings silently: the coordinate stays well-formed TOML and simply stops naming
anything.

`check_inventory` is the single implementation, with two consumers: the
named-steward webapp boot (`reg_webapp.stewards.check_delivery_inventory`),
which refuses to serve an inventory its own DB cannot resolve, and the
maintainer's pytest over the committed inventory, run whenever a real flavored
DB is at hand.

Resolution is read the way the ORDER PATH reads it (`order._materialize_binding`
→ `Catalog.resolve_at`), never re-derived: a mapping this gate rejects is one no
order could ever fill, and a gate stricter than the materializer would fail a
deployment over holdings it can actually serve. The bulk lookups below are the
same exact-slug rules `Catalog._resolve_variant_id` / `_lookup_variable` apply,
batched — an inventory carries tens of thousands of mappings and this runs at
every boot, so per-mapping `Catalog` calls are not affordable; the curated
`variable_same_as` fallback (rare by construction) does go through `Catalog`
itself.

Findings are grouped by the COORDINATE that failed, not by the mapping
occurrence: one renamed variant slug is one finding over N mappings, which is
the unit a maintainer actually repairs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

from .catalog import Catalog
from .errors import RegMetaError

# `_location` renders one physical `table[...].column[...]` locator in the
# author-facing spelling the inventory validator's own messages use, so both
# halves of the §12 gate point at a file position the same way.
from .inventory import _location

if TYPE_CHECKING:
    import sqlite3

    from .catalog import VariableState
    from .inventory import DeliveryInventory

# How many physical locations one finding names. A renamed provider slug strands
# thousands of mappings at once; the maintainer repairs the COORDINATE, so a
# handful of examples locate the damage while `mapping_count` keeps the scale
# honest.
_MAX_LOCATIONS = 5

# How many findings `unresolved_message` renders before summarizing the rest. A
# boot traceback is not a report.
_MAX_RENDERED = 20


# The four grains a coordinate can strand at, coarse to fine.
# `binding_unavailable` borrows the ORDER PATH's own name for it
# (`order._materialize_binding`): the variant and the variable each exist, but
# the catalog carries no state pairing them, so no request could ever fill that
# mapping.
_FindingCode = Literal[
    "variant_unresolved",
    "variable_unresolved",
    "binding_unavailable",
    "representation_unresolved",
]


class InventoryFinding(BaseModel):
    """One inventory coordinate the catalog DB cannot resolve.

    `coordinate` is the failing coordinate itself — the repair unit, and the
    deterministic sort key within a code. `mapping_count` is how many mappings
    it strands; `locations` names the first `_MAX_LOCATIONS` of them.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    code: _FindingCode
    coordinate: str
    message: str
    mapping_count: int
    locations: tuple[str, ...]


class _Uses:
    """Where one coordinate is used: a capped sample of physical locations plus
    the true total."""

    __slots__ = ("count", "locations")

    def __init__(self) -> None:
        self.count = 0
        self.locations: list[str] = []

    def record(self, location: str) -> None:
        """Count every mapping, but sample each physical location once — one
        column carrying several mappings of the same variable (a combined table
        serving several variants) would otherwise spend the whole sample on one
        place. Mappings of a column are consecutive, so comparing against the
        last recorded location dedupes fully."""
        self.count += 1
        if len(self.locations) < _MAX_LOCATIONS and location != (
            self.locations[-1] if self.locations else None
        ):
            self.locations.append(location)


# One `(register_variant, variable FQID)` binding of the inventory, and the
# `(…, representation)` cell that narrows it. EVERY mapping states a pair; only a
# mapping that pins a representation states a cell.
_Pair = tuple[str, str]
_Cell = tuple[str, str, str]
# The same binding at the DB's own grain: `(variable_id, register_variant_id)`.
_PairIds = tuple[int, int]


def check_inventory(
    inventory: DeliveryInventory, conn: sqlite3.Connection
) -> tuple[InventoryFinding, ...]:
    """Every coordinate of `inventory` that does not resolve against `conn`.

    Empty when the inventory is consistent with the catalog the deployment
    serves — the passing baseline. Deterministic: findings come grouped by code
    (variant, then variable, then binding, then representation) and sorted by
    coordinate within each group, so two runs over the same inputs produce the
    same report.

    A mapping that omits `representation` skips the representation check ONLY:
    which canonical representation an unqualified binding resolves to is
    request-dependent (§12's single-representation arm is decided across the
    requested period, and `order._materialize_binding`'s `unqualified_ok` owns
    it), but that the binding is delivered at its declared variant at all is
    not — every mapping is checked at that grain.
    """
    variants: dict[str, _Uses] = {}
    variables: dict[str, _Uses] = {}
    pairs: dict[_Pair, _Uses] = {}
    cells: dict[_Cell, _Uses] = {}
    for table in inventory.tables:
        for column in table.columns:
            location = _location(table.id, column.name)
            for mapping in column.mappings:
                variable = str(mapping.variable)
                pair = (mapping.register_variant, variable)
                variants.setdefault(mapping.register_variant, _Uses()).record(location)
                variables.setdefault(variable, _Uses()).record(location)
                pairs.setdefault(pair, _Uses()).record(location)
                if mapping.representation is not None:
                    cells.setdefault((*pair, mapping.representation), _Uses()).record(
                        location
                    )

    variant_ids = _variant_ids(conn, set(variants))
    variable_ids = _variable_ids(conn, set(variables))
    catalog = Catalog(conn)
    findings: list[InventoryFinding] = []

    for coordinate in sorted(variants):
        if coordinate not in variant_ids:
            findings.append(
                _finding(
                    "variant_unresolved",
                    coordinate,
                    f"register_variant {coordinate!r} names no register variant "
                    "in the catalog",
                    variants[coordinate],
                )
            )

    # A direct slug miss can still resolve through a curated `variable_same_as`
    # edge — the identity resolution the order path takes — so ask `Catalog`
    # before calling a variable stranded. The call is one indexed lookup plus a
    # cached same_as short-circuit on the common (genuinely absent) path.
    aliased: set[str] = set()
    for variable in sorted(variables):
        if variable in variable_ids:
            continue
        if _resolves(catalog, variable):
            aliased.add(variable)
            continue
        findings.append(
            _finding(
                "variable_unresolved",
                variable,
                f"variable {variable!r} does not resolve against the catalog",
                variables[variable],
            )
        )

    # The PAIRING, checked for every mapping — an existing variable and an
    # existing variant can still be a binding the catalog never delivers, when
    # the variable's states all live under other variants. The order path calls
    # that `binding_unavailable`; here it holds at every period, so no request
    # could ever fill the mapping.
    aliased_states: dict[_Pair, list[VariableState]] = {}
    pair_probes: dict[_Pair, tuple[int, int]] = {}
    for pair in pairs:
        coordinate, variable = pair
        register_variant_id = variant_ids.get(coordinate)
        if register_variant_id is None:
            continue  # already reported at the variant grain; don't cascade
        if variable in aliased:
            # A same_as target sits under ANOTHER register, so its states carry
            # that register's variants, not this coordinate's. Rather than
            # re-derive where an aliased variable's states live, ask the order
            # path itself, once per pair — `resolve_at` over the whole history
            # (`"_default"`, no period filter) with the same variant narrowing
            # the materializer applies. Rare: same_as is curator-authored and
            # tiny.
            aliased_states[pair] = _aliased_states(catalog, variable, coordinate)
            continue
        variable_id = variable_ids.get(variable)
        if variable_id is None:
            continue  # already reported at the variable grain
        pair_probes[pair] = (variable_id, register_variant_id)

    delivered = _delivered(conn, set(pair_probes.values()))
    unavailable = sorted(
        [pair for pair, key in pair_probes.items() if key not in delivered]
        + [pair for pair, states in aliased_states.items() if not states]
    )
    for pair in unavailable:
        coordinate, variable = pair
        findings.append(
            _finding(
                "binding_unavailable",
                " ".join(pair),
                f"the catalog resolves no state for variable {variable} at "
                f"{coordinate}, so it never delivers this binding there",
                pairs[pair],
            )
        )

    stranded = set(unavailable)
    unresolved: list[_Cell] = []
    for cell in cells:
        coordinate, variable, representation = cell
        pair = (coordinate, variable)
        if pair in stranded:
            continue  # already reported at the binding grain; don't cascade
        key = pair_probes.get(pair)
        if key is not None:
            if representation not in delivered[key]:
                unresolved.append(cell)
        elif (states := aliased_states.get(pair)) is not None and not any(
            state.delivery_column_name == representation for state in states
        ):
            unresolved.append(cell)
    for cell in sorted(unresolved):
        coordinate, variable, representation = cell
        findings.append(
            _finding(
                "representation_unresolved",
                " ".join(cell),
                f"representation {representation!r} is not a delivery column of "
                f"{variable} at {coordinate}",
                cells[cell],
            )
        )
    return tuple(findings)


def unresolved_message(findings: tuple[InventoryFinding, ...]) -> str:
    """Every unresolved coordinate as ONE human-readable line.

    The boot failure and the maintainer's pytest both render findings here, so
    both name the same damage in the same words (`order.blocked_message`'s
    posture). Deliberately one line — it is read inside an exception message —
    and capped at `_MAX_RENDERED` coordinates with an explicit remainder, never
    a silent truncation.
    """
    rendered = "; ".join(
        f"{finding.code}: {finding.message} [{finding.mapping_count} mapping(s), "
        f"e.g. {', '.join(finding.locations)}]"
        for finding in findings[:_MAX_RENDERED]
    )
    if len(findings) > _MAX_RENDERED:
        rendered += (
            f"; … and {len(findings) - _MAX_RENDERED} more unresolved coordinate(s)"
        )
    # Coordinates, not mappings: one mapping strands under as many coordinates as
    # it names, so summing `mapping_count` across grains would overcount the
    # inventory's own mappings. Scale stays per finding, where it is exact.
    return f"{len(findings)} unresolved inventory coordinate(s): {rendered}"


def _finding(
    code: _FindingCode,
    coordinate: str,
    message: str,
    uses: _Uses,
) -> InventoryFinding:
    return InventoryFinding(
        code=code,
        coordinate=coordinate,
        message=message,
        mapping_count=uses.count,
        locations=tuple(uses.locations),
    )


def _variant_ids(conn: sqlite3.Connection, wanted: set[str]) -> dict[str, int]:
    """`<provider>/<register>/<variant>` → `register_variant_id`, for the
    coordinates `wanted` names.

    Exact-slug matching, like `Catalog._resolve_variant_id` — there is no
    aliasing at the variant grain. Filtered while streaming, so the working set
    is the inventory's (hundreds) and not the catalog's."""
    return {
        coordinate: register_variant_id
        for provider, register, variant, register_variant_id in conn.execute(
            "SELECT p.slug, r.slug, rv.slug, rv.register_variant_id "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE rv.slug IS NOT NULL AND r.slug IS NOT NULL"
        )
        if (coordinate := f"{provider}/{register}/{variant}") in wanted
    }


def _variable_ids(conn: sqlite3.Connection, wanted: set[str]) -> dict[str, int]:
    """Binding FQID → `variable_id`, for the FQIDs `wanted` names. The
    register-unique stored slug is the natural key (`Catalog._lookup_variable`);
    a NULL-slug variable is not FQID-addressable, so it cannot answer a
    mapping."""
    return {
        fqid: variable_id
        for provider, register, variable, variable_id in conn.execute(
            "SELECT p.slug, r.slug, v.slug, v.variable_id "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL"
        )
        if (fqid := f"{provider}/{register}/{variable}") in wanted
    }


def _delivered(
    conn: sqlite3.Connection, pairs: set[_PairIds]
) -> dict[_PairIds, frozenset[str]]:
    """For each pair of `pairs` the catalog carries a state for, the delivery
    column names the resolver would produce over the whole history.

    A pair ABSENT from the result is one no `variable_state` row pairs — the
    binding is unreachable. A pair mapped to an EMPTY set is reachable but
    delivers no column (its states all carry a NULL `delivery_column_name`);
    that is still a reachable binding, because whether an unqualified binding is
    orderable across a given period is the representation grain, which only the
    order path can decide.

    `variable_alias_window` is read the way its ONLY reader reads it
    (`Catalog._expand_state_windows`), never as a flat union: a window row is
    not an independently delivered representation. It expands a state only when
    it is CONTAINED in that state's validity AND that state's own delivery
    column participates in the contained set; otherwise the state stands on its
    own column and its windows deliver nothing. Unioning the two tables would
    bless an orphaned, non-contained or non-participating window as deliverable
    and let a deployment boot on a mapping `resolve_at` cannot fill — the exact
    false pass this gate exists to prevent.

    Two streaming scans, filtered against the inventory's own pairs, so the
    working set stays the inventory's and not the catalog's."""
    states: dict[_PairIds, list[tuple[str, str, str | None]]] = {}
    for variable_id, register_variant_id, valid_from, valid_to, column in conn.execute(
        "SELECT variable_id, register_variant_id, valid_from, valid_to, "
        "delivery_column_name FROM variable_state"
    ):
        pair = (variable_id, register_variant_id)
        if pair in pairs:
            states.setdefault(pair, []).append((valid_from, valid_to, column))
    if not states:
        return {}
    windows: dict[_PairIds, list[tuple[str, str, str]]] = {}
    for variable_id, register_variant_id, column, valid_from, valid_to in conn.execute(
        "SELECT variable_id, register_variant_id, delivery_column_name, "
        "valid_from, valid_to FROM variable_alias_window"
    ):
        pair = (variable_id, register_variant_id)
        if pair in states:
            windows.setdefault(pair, []).append((column, valid_from, valid_to))
    return {
        pair: _expanded_columns(state_rows, windows.get(pair, []))
        for pair, state_rows in states.items()
    }


def _expanded_columns(
    states: list[tuple[str, str, str | None]],
    windows: list[tuple[str, str, str]],
) -> frozenset[str]:
    """`Catalog._expand_state_windows`'s delivery columns for one pair.

    The gate reads the WHOLE history (`"_default"`, no period filter), so the
    resolver's window ∩ requested-bounds test is trivially true and is not
    mirrored — every contained window is in range. What is mirrored is the
    containment and participation pair of conditions, which is what decides
    whether a state expands into its windows or stands on its own column."""
    columns: set[str] = set()
    for valid_from, valid_to, column in states:
        contained = [w for w in windows if valid_from <= w[1] and w[2] <= valid_to]
        if (
            contained
            and column is not None
            and any(w[0].lower() == column.lower() for w in contained)
        ):
            # The state's own column participates, so the state EXPANDS: the
            # contained windows REPLACE it, the base column returning as the
            # window that matched it — in that window's own spelling.
            columns.update(w[0] for w in contained)
        elif column is not None:
            # No window contained by this state, or none carrying its column:
            # the state stands on its own and its windows deliver nothing.
            columns.add(column)
    return frozenset(columns)


def _aliased_states(
    catalog: Catalog, fqid: str, coordinate: str
) -> list[VariableState]:
    """The states an aliased binding resolves to at `coordinate`, over the whole
    history — `resolve_at`'s own variant narrowing, which is what the order path
    applies.

    A resolution failure counts as NO states, never an abort. A same_as edge is
    cross-register by construction, so the target's shape is not the
    coordinate's, and this gate owes its two consumers a COMPLETE grouped report
    — one raised coordinate must not cost the boot failure and the maintainer's
    pytest every other finding in the run. The pair is then reported as
    `binding_unavailable`, which is what it is however the resolution failed,
    and boot still refuses to serve it."""
    try:
        return catalog.resolve_at(fqid, "_default", variant=coordinate.split("/")[2])
    except RegMetaError:
        return []


def _resolves(catalog: Catalog, fqid: str) -> bool:
    """Does this binding FQID resolve at all — directly, or through a curated
    `variable_same_as` edge? The inventory's FQIDs are validated bindings, so
    only the not-found `RegMetaError` is possible here."""
    try:
        catalog.resolve(fqid)
    except RegMetaError:
        return False
    return True
