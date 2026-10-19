"""Compile physical holdings facts against the derived `resolver_column` universe."""

from __future__ import annotations

import sqlite3
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._resolved_common import _storage_id
from .data_warnings import DataWarning, keep_warning, mapping_data_warnings
from .db import catalog_coordinate_ids, stored_data_warnings, write_data_warnings
from .errors import EXIT_CONFIG, RegMetaError
from .holdings_accounting import (
    INVENTORY_ROLE_PATH,
    HoldingsAccounting,
    account_holdings,
)
from .inventory import (
    ColumnMapping,
    DeliveryInventory,
    InventoryColumn,
    _edition_intervals,
    _location,
    _merge,
    _render,
    _segment_bounds,
    edition_bounds,
    load_inventory,
    validate_inventory_placements,
)
from .inventory_coverage import (
    _fold,
    _load_windows,
    uncovered_days,
    warning_gap_problem,
)
from .source_evidence import canonical_json
from .steward_spec import scope_locator

if TYPE_CHECKING:
    from pathlib import Path

    from .steward_spec import StewardSpec


class HoldingsFoldError(ValueError):
    """Located rejected mappings, retained locally for separate source review."""

    def __init__(self, rejections: tuple[str, ...]) -> None:
        self.rejections = rejections
        super().__init__(
            f"Holdings fold gate rejected {len(rejections)} mappings:\n"
            + "\n".join(rejections)
        )


@dataclass(frozen=True)
class CompiledHoldings:
    tables: int
    columns: int
    mappings: int
    accounting: HoldingsAccounting


def canonical_inventory(
    conn: sqlite3.Connection,
    inventory: DeliveryInventory,
    *,
    coordinate_ids: tuple[dict[str, int], dict[str, int]] | None = None,
) -> DeliveryInventory:
    """Canonicalize against the resolver's whole-history delivery universe."""
    variables, variants = coordinate_ids or catalog_coordinate_ids(conn)
    universes: dict[tuple[int, int], dict[str, str]] = {}
    rejections = []
    tables = []
    for table in inventory.tables:
        columns = []
        for column in table.columns:
            mappings = []
            seen = set()
            for index, mapping in enumerate(column.mappings):
                locator = f"policy/inventory.toml:table[{table.id!r}].column[{column.name!r}].mapping[{index}]"
                variable_id = variables.get(mapping.variable)
                variant_id = variants.get(mapping.register_variant)
                if variable_id is None or variant_id is None:
                    rejections.append(
                        f"{locator}: unresolved coordinate {mapping.register_variant} {mapping.variable}"
                    )
                    continue
                key = variable_id, variant_id
                if key not in universes:
                    universes[key] = dict(
                        conn.execute(
                            "SELECT delivery_column_lower, delivery_column_name "
                            "FROM resolver_column "
                            "WHERE variable_id = ? AND register_variant_id = ?",
                            key,
                        )
                    )
                canonical = universes[key].get(mapping.representation.lower())
                if canonical is None:
                    rejections.append(
                        f"{locator}: {mapping.register_variant} {mapping.variable} representation {mapping.representation!r} is not a resolver-emitted delivery column"
                    )
                    continue
                triple = variant_id, variable_id, canonical
                if triple in seen:
                    rejections.append(
                        f"{locator}: duplicate canonical triple {mapping.register_variant} {mapping.variable} {canonical!r}"
                    )
                    continue
                seen.add(triple)
                mappings.append(
                    mapping.model_copy(update={"representation": canonical})
                )
            columns.append(column.model_copy(update={"mappings": tuple(mappings)}))
        tables.append(table.model_copy(update={"columns": tuple(columns)}))
    if rejections:
        raise HoldingsFoldError(tuple(rejections))
    canonical = inventory.model_copy(update={"tables": tuple(tables)})
    validate_inventory_placements(canonical.tables)
    return canonical


COVERAGE_WARNING_INVALID = "inventory_coverage_warning_invalid"

# The state-keyed rows a clone carries over, each copied under the clone's id.
_STATE_CHILDREN = (
    "state_classification",
    "classification_conformance",
    "classification_conformance_code",
)


def _licensed_clones(
    conn: sqlite3.Connection,
    canonical: DeliveryInventory,
    *,
    variables: dict[str, int],
    variants: dict[str, int],
) -> list[tuple[sqlite3.Row, int, str, str]]:
    """The catalog states a mapping warning licenses (#1684), as `(source state
    row, clone id, valid_from, valid_to)`.

    A warned mapping whose served period the catalog's windows for its cell
    (`inventory_coverage._load_windows`: states and alias windows, folded by
    column) leave partly uncovered holds those days only on the warning's word,
    so the warning must state exactly those days (U). U licenses a copy of the
    cell's state in force at the end of the served period: the extract is in the
    coding current at extraction. Where U reaches that end, it copies the cell's
    nearest state after U, else the nearest before it. Two such states (code
    versions in force together) are refused as ambiguous. Licences of one cell,
    code version and source state coalesce where they overlap or abut
    (`_coalesce`), so the flavor gets one state per run. Each coalesced clone is
    then held to the catalog's no-overlap rule (`_clone_overlaps`) against the
    catalog's states and the other clones. An unwarned mapping is never refused here:
    coverage is not a holdings validation gate (`inventory_coverage`), and its
    uncovered years stay a `coverage_gap` at order time."""
    # simplify: U is one leading or trailing interval, and a warned mapping with U
    # carries no second, unrelated caveat inside its coverage. Split the warning
    # and the clone per interval if a delivery is ever uncovered at both ends.
    warned = [
        (table, column, mapping)
        for table in canonical.tables
        if table.period_scope == "intervals"
        for column in table.columns
        for mapping in column.mappings
        if mapping.data_warning is not None
    ]
    if not warned:
        return []
    pairs = {
        (variables[mapping.variable], variants[mapping.register_variant])
        for _, _, mapping in warned
    }
    windows = _load_windows(conn, pairs)
    problems: list[str] = []
    # Each warned mapping's licence: its uncovered days U and the one state of its
    # cell they copy, keyed by (pair, code version, source state).
    licences: dict[tuple[tuple[int, int], str, int], list[_Licence]] = defaultdict(list)
    for table, column, mapping in warned:
        assert mapping.data_warning is not None
        pair = (variables[mapping.variable], variants[mapping.register_variant])
        served = mapping.served(_edition_intervals(table))
        gaps = uncovered_days(served, windows.for_mapping(pair, mapping.representation))
        if not gaps:
            continue
        where = (
            f"policy/inventory.toml:{_location(table.id, column.name)}: "
            f"{mapping.register_variant} {mapping.variable} representation "
            f"{mapping.representation!r}"
        )
        window = mapping.data_warning.window
        stated = (_segment_bounds(window),) if window is not None else served
        if problem := warning_gap_problem(served, gaps, stated):
            problems.append(f"{where}: {problem}")
            continue
        (lo, hi), end = gaps[0], served[-1][1]
        sources = _clone_sources(
            _cell_states(conn, pair), mapping.representation, lo, hi, end
        )
        if not sources:
            problems.append(
                f"{where}: no catalog state of the cell to clone over {_render(gaps)}"
                " (its coverage is alias windows only)"
            )
            continue
        if len(sources) > 1:
            labels = ", ".join(repr(s["value_set_version_label"]) for s in sources)
            problems.append(
                f"{where}: {len(sources)} catalog states of the cell (code versions "
                f"{labels}) could each be cloned over {_render(gaps)}; a warning "
                "licenses one coding, so the clone source is ambiguous"
            )
            continue
        (source,) = sources
        key = (pair, source["value_set_version_label"], source["state_id"])
        licences[key].append(_Licence(where, mapping, source, lo, hi))
    clones = [
        clone
        for (pair, _label, _source), group in sorted(licences.items())
        for clone in _coalesce(pair, group)
    ]
    # Each pair of clones is compared once, by the earlier of the two.
    for index, clone in enumerate(clones):
        problems += _clone_overlaps(conn, clone, clones[index + 1 :])
    if problems:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code=COVERAGE_WARNING_INVALID,
            error_class="configuration",
            message="A mapping warning does not license the held days the "
            "catalog leaves uncovered:\n" + "\n".join(f"  {line}" for line in problems),
            remediation="Set the mapping's data_warning window to the uncovered "
            "days, map the column whose state documents them, or extend the "
            "catalog's coverage; see reg_meta_build/DESIGN.md → Inventory TOML.",
        )
    return [
        (clone.source, clone.clone_id, clone.lo, clone.hi)
        for clone in sorted(clones, key=lambda clone: clone.clone_id)
    ]


@dataclass(frozen=True)
class _Licence:
    """One warned mapping's uncovered days `lo..hi` and the state they copy."""

    where: str
    mapping: ColumnMapping
    source: sqlite3.Row
    lo: str
    hi: str


@dataclass(frozen=True)
class _Clone:
    """One cloned state over `lo..hi`: the union of the overlapping or abutting
    licences of one cell, code version and source state."""

    pair: tuple[int, int]
    source: sqlite3.Row
    clone_id: int
    lo: str
    hi: str
    licences: tuple[_Licence, ...]


def _coalesce(pair: tuple[int, int], group: list[_Licence]) -> list[_Clone]:
    """One clone per run of overlapping or abutting licences (`_merge`): an
    annual table per year, each warned, licenses one state over the years, and
    two shards licensing one gap are one clone. The id is the catalog state id
    the clone's coordinates mint."""
    clones = []
    for lo, hi in _merge([(licence.lo, licence.hi) for licence in group]):
        members = tuple(
            licence for licence in group if lo <= licence.lo and licence.hi <= hi
        )
        mapping, source = members[0].mapping, members[0].source
        provider, register, slug = mapping.variable.split("/")
        clone_id = _storage_id(
            provider,
            "state",
            register,
            slug,
            mapping.register_variant.rsplit("/", 1)[1],
            lo,
            source["value_set_version_label"],
        )
        clones.append(_Clone(pair, source, clone_id, lo, hi, members))
    return clones


def _clone_overlaps(
    conn: sqlite3.Connection, clone: _Clone, later: list[_Clone]
) -> list[str]:
    """Why `clone` breaks the rule `ResolvedVariable` applies at catalog formation
    ("overlapping states in variant"): states of one variant and code version
    never overlap, under any column. Held against the catalog's states (a
    warning cannot license years another column already documents) and against
    the `later` clones (two codings for one day; licences of one source state
    are already one clone, so these copy different states). Checked before the
    INSERT, whose unique index would otherwise only catch a same-start clash."""
    label = clone.source["value_set_version_label"]
    where = clone.licences[0].where
    span = _render(((clone.lo, clone.hi),))
    problems: list[str] = []
    for state in _cell_states(conn, clone.pair):
        if (
            state["value_set_version_label"] == label
            and state["valid_from"] <= clone.hi
            and clone.lo <= state["valid_to"]
        ):
            problems.append(
                f"{where}: the clone over {span} would overlap the catalog state of "
                f"column {state['delivery_column_name']!r} over "
                f"{_render(((state['valid_from'], state['valid_to']),))}; a warning "
                "cannot license years another column of the cell already "
                "documents: map that column's representation instead"
            )
            break
    for other in later:
        if (
            other.pair == clone.pair
            and other.source["value_set_version_label"] == label
            and other.lo <= clone.hi
            and clone.lo <= other.hi
        ):
            problems.append(
                f"{where}: the clone over {span} (copying column "
                f"{clone.source['delivery_column_name']!r}) overlaps the clone over "
                f"{_render(((other.lo, other.hi),))} (copying column "
                f"{other.source['delivery_column_name']!r}) licensed at "
                f"{other.licences[0].where}; one day of a cell takes one coding: "
                "narrow one warning or map one column"
            )
    return problems


def _cell_states(conn: sqlite3.Connection, pair: tuple[int, int]) -> list[sqlite3.Row]:
    """The dated catalog states of a (variable, variant) pair, every column."""
    return conn.execute(
        "SELECT * FROM variable_state WHERE variable_id = ? "
        "AND register_variant_id = ? AND period_scope = 'intervals' "
        "ORDER BY valid_from, value_set_version_label",
        pair,
    ).fetchall()


def _clone_sources(
    states: list[sqlite3.Row],
    representation: str,
    lo: str,
    hi: str,
    end: str,
) -> list[sqlite3.Row]:
    """The cell's states a clone over `lo..hi` may copy: those of its column in
    force on `end`, the served period's last day, unless the gap reaches it; then
    those starting first after the gap, else those ending last before it. More
    than one (code versions in force together) is ambiguous for the caller."""
    column = [
        s
        for s in states
        if _fold(s["delivery_column_name"] or "") == _fold(representation)
    ]
    if hi < end:
        return [s for s in column if s["valid_from"] <= end <= s["valid_to"]]
    after = [s for s in column if s["valid_from"] > hi]
    if after:
        first = min(s["valid_from"] for s in after)
        return [s for s in after if s["valid_from"] == first]
    before = [s for s in column if s["valid_to"] < lo]
    if before:
        last = max(s["valid_to"] for s in before)
        return [s for s in before if s["valid_to"] == last]
    return []


def _write_clones(
    conn: sqlite3.Connection,
    clones: list[tuple[sqlite3.Row, int, str, str]],
    *,
    steward: str,
) -> None:
    """Insert each licensed clone and its state-keyed child rows. The clone's
    provenance names the steward: the catalog documents no state there."""
    for source, clone_id, lo, hi in clones:
        row = {
            **dict(source),
            "state_id": clone_id,
            "valid_from": lo,
            "valid_to": hi,
            "pooled": 0,
            "provenance": f"steward:{steward}",
        }
        conn.execute(
            f"INSERT INTO variable_state ({', '.join(row)}) "
            f"VALUES ({', '.join('?' * len(row))})",
            tuple(row.values()),
        )
        for table in _STATE_CHILDREN:
            columns = [
                name
                for _, name, *_ in conn.execute(f"PRAGMA table_info({table})")
                if name != "state_id"
            ]
            conn.execute(
                f"INSERT INTO {table} (state_id, {', '.join(columns)}) "
                f"SELECT ?, {', '.join(columns)} FROM {table} WHERE state_id = ?",
                (clone_id, source["state_id"]),
            )


def _write_mapping_warnings(
    conn: sqlite3.Connection, canonical: DeliveryInventory
) -> None:
    """Add the steward's mapping warnings (#1684) to the base's, in the flavor only.

    `write_data_warnings` writes a whole, empty table pair, so the stored warnings
    and the steward's are merged and rewritten. Base rows keep their content; the
    flavored `validate_built_db` re-derives every row's identity."""
    steward = mapping_data_warnings(canonical.tables)
    if not steward:
        return
    merged: dict[str, DataWarning] = {}
    for warning in (*stored_data_warnings(conn), *steward):
        keep_warning(merged, warning)
    # `data_warning` references its text rows, so it is cleared first.
    conn.execute("DELETE FROM data_warning")
    conn.execute("DELETE FROM data_warning_text")
    write_data_warnings(conn, tuple(merged[k] for k in sorted(merged)))


def compile_holdings(
    conn: sqlite3.Connection,
    root: Path,
    *,
    steward: str,
    spec: StewardSpec,
    accounting: HoldingsAccounting | None = None,
) -> CompiledHoldings:
    """Compile facts, reusing accounting only from the same immutable input snapshot."""
    conn.row_factory = sqlite3.Row
    inventory_path = root / INVENTORY_ROLE_PATH
    inventory = load_inventory(inventory_path)
    if inventory.steward != steward:
        raise ValueError("Accepted inventory steward does not match selected steward")
    accounting = accounting or account_holdings(
        root, inventory, spec=spec, steward=steward
    )
    variables, variants = catalog_coordinate_ids(conn)
    canonical = canonical_inventory(
        conn, inventory, coordinate_ids=(variables, variants)
    )
    clones = _licensed_clones(conn, canonical, variables=variables, variants=variants)
    raw = tomllib.loads(inventory_path.read_text(encoding="utf-8"))
    authored = {table["id"]: (index, table) for index, table in enumerate(raw["table"])}
    logical = {table.id: table for table in canonical.tables}
    unknown = {
        entry.table: (index, entry) for index, entry in enumerate(accounting.unknown)
    }
    column_id = mappings_count = 0
    for table_id, physical_id in enumerate(sorted(set(logical) | set(unknown)), 1):
        if physical_id in logical:
            table = logical[physical_id]
            index, original = authored[physical_id]
            edition = original["edition"]
            scope, partition, reason = table.period_scope, table.partition, None
            source_ref = f"policy/inventory.toml:table[{index}]"
            periods = edition_bounds(table.edition) if scope == "intervals" else ()
            columns = table.columns
        else:
            index, entry = unknown[physical_id]
            edition, scope, partition, reason = (
                entry.edition,
                "unknown",
                entry.partition,
                entry.reason,
            )
            source_ref = f"{scope_locator(steward)}:retain_unknown[{index}]"
            periods = ()
            columns = tuple(
                InventoryColumn(name=name)
                for name in sorted(accounting.columns[physical_id])
            )
        conn.execute(
            "INSERT INTO holding_table(table_id, physical_id, scope, edition_json, partition, retain_unknown_reason, source_ref) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                table_id,
                physical_id,
                scope,
                canonical_json(edition) if edition is not None else None,
                partition,
                reason,
                source_ref,
            ),
        )
        conn.executemany(
            "INSERT INTO holding_period(table_id, lo, hi) VALUES (?, ?, ?)",
            ((table_id, lo, hi) for lo, hi in periods),
        )
        literal_columns = (
            {column.name: column for column in inventory.tables[index].columns}
            if physical_id in logical
            else {}
        )
        for column in sorted(columns, key=lambda c: c.name):
            column_id += 1
            conn.execute(
                "INSERT INTO holding_column(column_id, table_id, name, unmapped_reason) VALUES (?, ?, ?, ?)",
                (column_id, table_id, column.name, column.unmapped_reason),
            )
            literals = (
                literal_columns[column.name].mappings if physical_id in logical else ()
            )
            for literal, mapping in zip(literals, column.mappings, strict=True):
                member = mapping.member
                lo, hi = (member and member.bounds()) or (None, None)
                window_lo, window_hi = (
                    edition_bounds(mapping.window)[0]
                    if mapping.window is not None
                    else (None, None)
                )
                conn.execute(
                    "INSERT INTO holding_mapping(column_id, variant_id, variable_id, representation_literal, representation_canonical, member_position, member_lo, member_hi, window_lo, window_hi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        column_id,
                        variants[mapping.register_variant],
                        variables[mapping.variable],
                        literal.representation,
                        mapping.representation,
                        member and member.position,
                        lo,
                        hi,
                        window_lo,
                        window_hi,
                    ),
                )
                mappings_count += 1
    # A second pass: tables insert in physical_id order, so a projection may sort
    # before the parent its foreign key names. One statement sets both columns,
    # which the schema requires together.
    conn.executemany(
        "UPDATE holding_table SET projection_of = ?, projection = ? WHERE physical_id = ?",
        (
            (table.projection.of, table.projection.token, table.id)
            for table in canonical.tables
            if table.projection is not None
        ),
    )
    _write_mapping_warnings(conn, canonical)
    if clones:
        from .derive import derive

        _write_clones(conn, clones, steward=steward)
        # The derived tables read the states, so the clones need a full derive.
        # simplify: extend-db then derives twice; narrow this to the
        # state-dependent tables if a flavored build's time ever matters.
        # Above derive's parallel threshold its workers read committed rows, so
        # commit first, as extend-db does before its own derive.
        conn.commit()
        derive(conn)
    return CompiledHoldings(
        len(logical) + len(unknown), column_id, mappings_count, accounting
    )
