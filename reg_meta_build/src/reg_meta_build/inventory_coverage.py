"""Steward holdings vs. catalog windows (Y-115) — the extend-db coverage gate.

`extend-db` builds a steward-flavored DB and validates it, but nothing in that
suite compared the steward's OWN holdings statement — `reg_webapp/stewards/
<steward>/inventory.toml`, the §12 delivery inventory — against the windows the
catalog carries. Neither runtime gate does either: `catalog_index` trusts an
explicit `representation` over the table's whole edition, and
`reg_meta.inventory_check` only asks whether the coordinate EXISTS. So a column
the steward holds in an edition reg_meta has no `variable_state` /
`variable_alias_window` for passes every gate and fails the RESEARCHER instead —
`order.resolve_binding` refuses it (`binding_unavailable` →
`period_outside_state_validity`) or clips it silently.

This module is that comparison, with two consumers that must not disagree about
the rule: the flavored validation gate (`validate._check_inventory_window_coverage`)
and the steward generator's `build_catalog.py errata` worklist. A miss renders in
the `[[version]]` / `[[delivered]]` grammar of `reg_meta_build/scb_errata.toml`,
because that is where the repair goes: SCB omitted a row from its own export, so
the correction is made at SCB's grain (see `scb_errata.py`).

Reading rules, each deliberate:

* Coverage is judged PER PHYSICAL COLUMN of a table: covered iff ANY of its
  mappings covers the edition. One physical column serving several variants stays
  orderable as long as one of them delivers it.
* A mapping's basis is the table's whole `edition_bounds` — the same claim
  `catalog_index` admits an explicit `representation` over — so this gate refuses
  exactly the claim the runtime would have trusted.
* `variable_state` and `variable_alias_window` are read as a FLAT UNION keyed by
  the folded delivery column, and an edition is covered when the MERGED union
  contains it (a column whose edition straddles two abutting states is delivered,
  not missing). Looser than `inventory_check._expanded_columns`, which mirrors
  `Catalog._expand_state_windows`' containment/participation rule: the question
  here is whether the catalog claims the column in that edition AT ALL, and a
  stricter read would demand errata for windows the resolver does deliver.
* Columns fold with `str.lower()` — `py_lower`'s rule, which
  `reg_webapp.catalog_index._fold_column` matches a held column against a catalog
  row with. NEVER SQL `lower()` (ASCII-only, so `Kön` would not fold) and never
  `_curation.fold_column` (NFKD + ASCII-drop, which folds DISTINCT columns
  together).
* A coordinate whose slugs do not resolve at all is SKIPPED and counted, never
  failed: that is `reg_meta.inventory_check`'s finding, repaired by regenerating
  the inventory (release step 11). A gate that failed on it would go red on
  ordinary pre-v1 slug churn instead of on a real holdings contradiction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

# `_merge` / `_render` are `reg_meta.inventory`'s interval primitives: the ONE
# grammar an edition, a project period and an availability window expand and
# render through (`order.py`, `catalog.py` and `reg_webapp.catalog_index` reach
# into them the same way). A held range must read here exactly as it does in the
# inventory's own errors. `_variant_ids` / `_variable_ids` are `inventory_check`'s
# batched exact-slug resolvers, so this gate and the §12 boot gate agree about
# which coordinates exist.
from reg_meta.inventory import _merge, _render, edition_bounds
from reg_meta.inventory_check import _variable_ids, _variant_ids

# The package's shared TOML basic-string leaf, so a worklist entry quotes a
# column exactly as every other candidate emitter in reg_meta_build does.
from reg_meta_build.fqid_slugs import _toml_str

if TYPE_CHECKING:
    import sqlite3

    from reg_meta.inventory import DeliveryInventory, InventoryColumn

# An inclusive ISO `(lo, hi)` date interval — `reg_meta.inventory`'s currency.
_Interval = tuple[str, str]
# One binding at the DB's own grain: `(variable_id, register_variant_id)`.
_PairIds = tuple[int, int]
# One inventory coordinate as the mappings spell it: `(variant coord, FQID)`.
_Coord = tuple[str, str]


@dataclass(frozen=True)
class CoverageMiss:
    """One `(register, variant, column)` the steward holds in editions the
    catalog has no window for — the grain `scb_errata.toml`'s `[[delivered]]`
    repairs, so a group is one curation decision.

    `column` is the CANONICAL delivery column (the mapping's `representation`,
    SCB's own spelling), falling back to the held spelling for a mapping that
    pins none; `editions` are the uncovered edition intervals and `undocumented`
    the subset no `variable_state` on the variant covers at all (those need a
    `[[version]]` as well — SCB never documented the edition). `windows` is what
    the catalog does carry for the column here.
    """

    register: str
    variant: str
    column: str
    editions: tuple[_Interval, ...]
    undocumented: tuple[_Interval, ...]
    windows: tuple[_Interval, ...]

    @property
    def coordinate(self) -> str:
        return f"{self.register}/{self.variant}"


@dataclass(frozen=True)
class CoverageReport:
    """`misses` plus the denominators, both at the gate's own grain of one
    held column × edition claim: `pairs` judged, `missed_pairs` of them
    uncovered, and `unresolved` mappings skipped because their coordinate
    names nothing in the catalog."""

    misses: tuple[CoverageMiss, ...]
    pairs: int
    missed_pairs: int
    unresolved: int


def coverage_misses(
    conn: sqlite3.Connection, inventory: DeliveryInventory
) -> CoverageReport:
    """Every held column × edition the catalog in `conn` has no window for.

    Deterministic: misses come sorted by `(register, variant, column)`, each
    carrying its editions ascending, so two runs over the same inputs produce the
    same report (and the same worklist file).
    """
    coords: set[_Coord] = {
        (mapping.register_variant, str(mapping.variable))
        for table in inventory.tables
        for column in table.columns
        for mapping in column.mappings
    }
    variant_ids = _variant_ids(conn, {variant for variant, _ in coords})
    variable_ids = _variable_ids(conn, {variable for _, variable in coords})
    pair_ids: dict[_Coord, _PairIds] = {
        coord: (variable_ids[coord[1]], variant_ids[coord[0]])
        for coord in coords
        if coord[0] in variant_ids and coord[1] in variable_ids
    }
    windows = _load_windows(conn, set(pair_ids.values()), set(variant_ids.values()))

    accumulated: dict[tuple[str, str, str], _Accumulator] = {}
    pairs = 0
    missed_pairs = 0
    unresolved = 0
    for table in inventory.tables:
        bounds = edition_bounds(table.edition)
        for column in table.columns:
            placed = _placements(column, pair_ids, windows)
            unresolved += len(column.mappings) - len(placed)
            # An unmapped column admits nothing (§12): it stays out of the
            # denominator because it is never ordered, so it cannot contradict the
            # catalog.
            if not placed:
                continue
            for edition in bounds:
                pairs += 1
                if any(_covers(edition, place.windows) for place in placed):
                    continue
                missed_pairs += 1
                for place in placed:
                    accumulated.setdefault(place.key, _Accumulator()).record(
                        edition,
                        place.windows,
                        documented=_covers(edition, place.documented),
                    )
    return CoverageReport(
        misses=tuple(accumulated[key].finish(*key) for key in sorted(accumulated)),
        pairs=pairs,
        missed_pairs=missed_pairs,
        unresolved=unresolved,
    )


def miss_line(miss: CoverageMiss) -> str:
    """One report line for `miss`: the held editions merged into ranges, what the
    catalog carries instead, and the `scb_errata.toml` entry that repairs it."""
    undocumented = (
        f", no documented register version over {_ranges(miss.undocumented)} "
        f"(a [[version]] each)"
        if miss.undocumented
        else ""
    )
    inline = ", ".join(f"{key} = {value}" for key, value in _delivered_fields(miss))
    return (
        f"{miss.coordinate} {miss.column}: {_held_vs_windows(miss)}"
        f"{undocumented} — [[delivered]] {inline}"
    )


def version_candidates(report: CoverageReport) -> tuple[tuple[str, str, str], ...]:
    """The `[[version]]` entries `report` needs: one `(register, variant, period
    token)` per undocumented edition, deduped and ordered.

    Shared with `build_catalog.py errata`'s summary, so the count it prints to the
    maintainer is the count the worklist it just wrote carries."""
    return tuple(
        sorted(
            {
                (miss.register, miss.variant, token)
                for miss in report.misses
                for token in _tokens(miss.undocumented)
            }
        )
    )


def errata_worklist(report: CoverageReport) -> str:
    """`report` as a candidate `scb_errata.toml`, split into the `[[version]]`
    entries the undocumented editions need and the `[[delivered]]` entries every
    miss needs.

    A candidate, NOT a drop-in: `evidence` and `noted` are what make an errata
    entry an upstream-error record rather than a window override, and only the
    maintainer can write them, so they ride along commented out. The versions are
    the editions' period tokens — SCB's `Registerversionnamn` for an annual
    register, a token to check against the export for anything else.
    """
    versions = version_candidates(report)
    lines = [
        "# GENERATED by input_data/swecov/build_catalog.py errata — candidate",
        "# reg_meta_build/scb_errata.toml entries for the columns the steward holds",
        "# in editions the flavored catalog has no state or alias window for.",
        "#",
        "# CURATE, don't paste wholesale: every entry needs the `evidence` that",
        "# establishes SCB delivered the row, and the `noted` date. A miss whose",
        "# column SCB never documents anywhere is a variable_grafts.toml graft or a",
        "# canonical_attach entry instead — not errata.",
        "#",
        f"# {report.missed_pairs} held column × edition pair(s) in "
        f"{len(report.misses)} group(s), out of {report.pairs} judged.",
        "",
        f"# ── version-missing: {len(versions)} held edition(s) SCB documents no "
        "register version for; mint them first ──",
    ]
    for register, variant, name in versions:
        lines.append("")
        lines.append(
            _stanza(
                "version",
                (
                    ("register", _toml_str(register)),
                    ("variant", _toml_str(variant)),
                    ("name", _toml_str(name)),
                ),
            )
        )
    lines += [
        "",
        f"# ── column-missing: the omitted column rows themselves, "
        f"{len(report.misses)} (register, variant, column) group(s) ──",
    ]
    for miss in report.misses:
        lines += [
            "",
            f"# {_held_vs_windows(miss)}",
            _stanza("delivered", _delivered_fields(miss)),
        ]
    return "\n".join(lines) + "\n"


# ── the catalog side ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Windows:
    """The catalog's merged delivery windows at the three grains the gate asks
    about: one column of one binding, any column of one binding (the
    single-representation arm), and any variable of one variant (which register
    versions SCB documented at all)."""

    by_column: dict[tuple[_PairIds, str], tuple[_Interval, ...]]
    by_pair: dict[_PairIds, tuple[_Interval, ...]]
    by_variant: dict[int, tuple[_Interval, ...]]

    def for_mapping(
        self, pair: _PairIds, representation: str | None
    ) -> tuple[_Interval, ...]:
        """The windows a mapping's coverage is judged against. An explicit
        `representation` names ONE canonical column; `None` means "the concept's
        single representation" (§12), which any of the binding's columns can
        answer, so it is judged against the binding as a whole."""
        if representation is None:
            return self.by_pair.get(pair, ())
        return self.by_column.get((pair, _fold(representation)), ())


def _load_windows(
    conn: sqlite3.Connection, pairs: set[_PairIds], variants: set[int]
) -> _Windows:
    """Two streaming scans, filtered against the inventory's own pairs and
    variants, so the working set stays the inventory's and not the catalog's (the
    same posture `inventory_check._delivered` takes).

    The per-variant grain reads `variable_state` only: a state IS the coalescer's
    window for a documented register version, while an alias window is a
    co-delivery inside one, so states alone answer "did SCB document this
    edition".
    """
    by_column: dict[tuple[_PairIds, str], list[_Interval]] = {}
    by_pair: dict[_PairIds, list[_Interval]] = {}
    by_variant: dict[int, list[_Interval]] = {}
    for variable_id, variant_id, valid_from, valid_to, column in conn.execute(
        "SELECT variable_id, register_variant_id, valid_from, valid_to, "
        "delivery_column_name FROM variable_state"
    ):
        if variant_id in variants:
            by_variant.setdefault(variant_id, []).append((valid_from, valid_to))
        pair = (variable_id, variant_id)
        if pair in pairs:
            by_pair.setdefault(pair, []).append((valid_from, valid_to))
            if column is not None:
                by_column.setdefault((pair, _fold(column)), []).append(
                    (valid_from, valid_to)
                )
    for variable_id, variant_id, column, valid_from, valid_to in conn.execute(
        "SELECT variable_id, register_variant_id, delivery_column_name, "
        "valid_from, valid_to FROM variable_alias_window"
    ):
        pair = (variable_id, variant_id)
        if pair in pairs:
            by_pair.setdefault(pair, []).append((valid_from, valid_to))
            by_column.setdefault((pair, _fold(column)), []).append(
                (valid_from, valid_to)
            )
    return _Windows(
        by_column={key: _merge(rows) for key, rows in by_column.items()},
        by_pair={key: _merge(rows) for key, rows in by_pair.items()},
        by_variant={key: _merge(rows) for key, rows in by_variant.items()},
    )


@dataclass(frozen=True)
class _Placement:
    """One resolved mapping of one physical column, with everything that does not
    depend on the edition already derived: the errata group the mapping lands in,
    the windows its coverage is judged against, and the windows of the variant's
    documented register versions (the `[[version]]` question)."""

    key: tuple[str, str, str]
    windows: tuple[_Interval, ...]
    documented: tuple[_Interval, ...]


def _placements(
    column: InventoryColumn, pair_ids: dict[_Coord, _PairIds], windows: _Windows
) -> list[_Placement]:
    """`column`'s mappings that name a coordinate this catalog has, each resolved
    ONCE: the edition loop then only compares."""
    placed = []
    for mapping in column.mappings:
        pair = pair_ids.get((mapping.register_variant, str(mapping.variable)))
        if pair is None:
            continue
        register, _, variant = mapping.register_variant.rpartition("/")
        placed.append(
            _Placement(
                key=(register, variant, mapping.representation or column.name),
                windows=windows.for_mapping(pair, mapping.representation),
                documented=windows.by_variant.get(pair[1], ()),
            )
        )
    return placed


@dataclass
class _Accumulator:
    """One `(register, variant, column)` group under construction."""

    editions: set[_Interval] = field(default_factory=set)
    undocumented: set[_Interval] = field(default_factory=set)
    windows: set[_Interval] = field(default_factory=set)

    def record(
        self, edition: _Interval, windows: tuple[_Interval, ...], *, documented: bool
    ) -> None:
        self.editions.add(edition)
        self.windows.update(windows)
        if not documented:
            self.undocumented.add(edition)

    def finish(self, register: str, variant: str, column: str) -> CoverageMiss:
        return CoverageMiss(
            register=register,
            variant=variant,
            column=column,
            editions=tuple(sorted(self.editions)),
            undocumented=tuple(sorted(self.undocumented)),
            # Merged: the group folds every table stating the column, so the
            # coordinate's windows are one continuous history, not a per-table list.
            windows=_merge(list(self.windows)),
        )


def _fold(column: str) -> str:
    """A delivery column's case-folded identity — `reg_webapp.catalog_index.
    _fold_column`'s rule (`py_lower`), the fold every reader that matches a HELD
    column against a catalog row goes through. Python's, never SQL's."""
    return column.lower()


def _covers(edition: _Interval, windows: tuple[_Interval, ...]) -> bool:
    """Is `edition` wholly inside `windows`? `windows` is merged, so abutting
    coverage is one interval and single-interval containment IS union coverage."""
    lo, hi = edition
    return any(window[0] <= lo and hi <= window[1] for window in windows)


# ── rendering ────────────────────────────────────────────────────────────────


def _ranges(intervals: tuple[_Interval, ...]) -> str:
    """Intervals merged into the inventory's own period rendering (`2010..2012`,
    `2019-Q3`) — abutting editions read as the run they are."""
    return _render(_merge(list(intervals)))


def _tokens(intervals: tuple[_Interval, ...]) -> tuple[str, ...]:
    """One period token per interval, UNMERGED: an errata `versions` list names
    each edition separately, whatever run they form."""
    return tuple(_render((interval,)) for interval in intervals)


def _held_vs_windows(miss: CoverageMiss) -> str:
    """The contradiction in one clause: what the steward holds, what the catalog
    carries. The gate's failure line and the worklist's comment say it the same
    way because they describe the same miss."""
    return (
        f"held {_ranges(miss.editions)}, catalog windows "
        f"{_ranges(miss.windows) if miss.windows else 'none'}"
    )


def _delivered_fields(miss: CoverageMiss) -> tuple[tuple[str, str], ...]:
    versions = ", ".join(_toml_str(token) for token in _tokens(miss.editions))
    return (
        ("register", _toml_str(miss.register)),
        ("variant", _toml_str(miss.variant)),
        ("column", _toml_str(miss.column)),
        ("versions", f"[{versions}]"),
    )


def _stanza(header: str, fields: tuple[tuple[str, str], ...]) -> str:
    """The entry as pasteable TOML, with the curator's own two keys stubbed."""
    body = "".join(f"{key} = {value}\n" for key, value in fields)
    return (
        f"[[{header}]]\n{body}"
        '# evidence = "why we know SCB delivered this"\n'
        '# noted = "YYYY-MM-DD"'
    )
