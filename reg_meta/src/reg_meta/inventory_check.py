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

    from .inventory import DeliveryInventory

# How many physical locations one finding names. A renamed provider slug strands
# thousands of mappings at once; the maintainer repairs the COORDINATE, so a
# handful of examples locate the damage while `mapping_count` keeps the scale
# honest.
_MAX_LOCATIONS = 5

# How many findings `unresolved_message` renders before summarizing the rest. A
# boot traceback is not a report.
_MAX_RENDERED = 20


class InventoryFinding(BaseModel):
    """One inventory coordinate the catalog DB cannot resolve.

    `coordinate` is the failing coordinate itself — the repair unit, and the
    deterministic sort key within a code. `mapping_count` is how many mappings
    it strands; `locations` names the first `_MAX_LOCATIONS` of them.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    code: Literal[
        "variant_unresolved", "variable_unresolved", "representation_unresolved"
    ]
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


# One `(register_variant, variable FQID, representation)` cell of the inventory.
_Cell = tuple[str, str, str]


def check_inventory(
    inventory: DeliveryInventory, conn: sqlite3.Connection
) -> tuple[InventoryFinding, ...]:
    """Every coordinate of `inventory` that does not resolve against `conn`.

    Empty when the inventory is consistent with the catalog the deployment
    serves — the passing baseline. Deterministic: findings come grouped by code
    (variant, then variable, then representation) and sorted by coordinate
    within each group, so two runs over the same inputs produce the same report.

    A mapping that omits `representation` gets no representation check: §12's
    single-representation arm is request-dependent (the binding must resolve to
    ONE canonical representation across the requested period), which only the
    order pass can decide — `order._materialize_binding`'s `unqualified_ok`
    owns it.
    """
    variants: dict[str, _Uses] = {}
    variables: dict[str, _Uses] = {}
    cells: dict[_Cell, _Uses] = {}
    for table in inventory.tables:
        for column in table.columns:
            location = _location(table.id, column.name)
            for mapping in column.mappings:
                variable = str(mapping.variable)
                variants.setdefault(mapping.register_variant, _Uses()).record(location)
                variables.setdefault(variable, _Uses()).record(location)
                if mapping.representation is not None:
                    cell = (mapping.register_variant, variable, mapping.representation)
                    cells.setdefault(cell, _Uses()).record(location)

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

    unresolved: list[_Cell] = []
    probes: list[tuple[_Cell, tuple[int, int, str]]] = []
    for cell in cells:
        coordinate, variable, representation = cell
        register_variant_id = variant_ids.get(coordinate)
        if register_variant_id is None:
            continue  # already reported at the variant grain; don't cascade
        if variable in aliased:
            # A same_as target sits under ANOTHER register, so its states carry
            # that register's variants, not this coordinate's. Rather than
            # re-derive where an aliased variable's representations live, ask
            # the order path itself. Rare: same_as is curator-authored and tiny.
            if not _aliased_representation_resolves(
                catalog, variable, coordinate, representation
            ):
                unresolved.append(cell)
            continue
        variable_id = variable_ids.get(variable)
        if variable_id is None:
            continue  # already reported at the variable grain
        probes.append((cell, (variable_id, register_variant_id, representation)))

    resolvable = _resolvable_representations(conn, {key for _, key in probes})
    unresolved.extend(cell for cell, key in probes if key not in resolvable)
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
    code: Literal[
        "variant_unresolved", "variable_unresolved", "representation_unresolved"
    ],
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


def _resolvable_representations(
    conn: sqlite3.Connection, wanted: set[tuple[int, int, str]]
) -> set[tuple[int, int, str]]:
    """Which `(variable_id, register_variant_id, delivery_column_name)` triples
    of `wanted` the catalog actually offers.

    The resolver's representation universe is `variable_state`'s denormalized
    latest alias UNION the `variable_alias_window` rows
    `Catalog._expand_state_windows` expands a state into — a monthly-family
    (#319) or multi-alias (#945) column exists ONLY in the window table, so
    reading `variable_state` alone would strand a mapping the order path
    resolves. Filtered while streaming, like the lookups above."""
    if not wanted:
        return set()
    return {
        key
        for row in conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name "
            "FROM variable_state WHERE delivery_column_name IS NOT NULL "
            "UNION ALL "
            "SELECT variable_id, register_variant_id, delivery_column_name "
            "FROM variable_alias_window"
        )
        if (key := (row[0], row[1], row[2])) in wanted
    }


def _resolves(catalog: Catalog, fqid: str) -> bool:
    """Does this binding FQID resolve at all — directly, or through a curated
    `variable_same_as` edge? The inventory's FQIDs are validated bindings, so
    only the not-found `RegMetaError` is possible here."""
    try:
        catalog.resolve(fqid)
    except RegMetaError:
        return False
    return True


def _aliased_representation_resolves(
    catalog: Catalog, fqid: str, coordinate: str, representation: str
) -> bool:
    """Is `representation` a delivery column of an aliased `fqid` at
    `coordinate`? `resolve_at` over the whole history (`"_default"`, no period
    filter) is exactly what the order path calls, variant narrowing included."""
    states = catalog.resolve_at(fqid, "_default", variant=coordinate.split("/")[2])
    return any(state.delivery_column_name == representation for state in states)
