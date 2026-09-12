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
and the steward generator's `build_catalog.py errata` worklist. A miss on the `scb`
provider renders as VALID, COMPLETE `[[version]]` / `[[delivered]]` stanzas of
`reg_meta_build/scb_errata.toml` — pasteable as they stand but for `evidence` and
`noted`, which ride as TODO placeholders — because that is where the repair goes:
SCB omitted a row from its own export, so the correction is made at SCB's grain
(see `scb_errata.py`).

A miss on ANY OTHER provider is never rendered in that grammar. `scb_errata.toml`
corrects SCB's own export, and its loader refuses an entry on another provider
outright (`scb_errata._PROVIDER`), so a stanza naming `fk/...` would aim the
maintainer at a file that cannot hold it — which is exactly what the 2026-09-12
flavored run did with its 26 `fk` and 11 `riksarkivet` groups. Those windows are
curated per provider instead: `input_data/<Provider>/<slug>.toml`'s `valid_from`
for a thin curated agency, the Socialstyrelsen export for `sos`, the inventory
`extend-db` overlaid for a steward-minted flavor provider. Such a miss renders as
ONE line naming the held editions, the catalog's windows and that surface.

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
* Columns fold with NFC normalization and then Python `str.lower()`, on BOTH
  sides — the held spelling out of the inventory TOML and `delivery_column_name`
  out of the DB. That is the webapp's rule (`catalog_index._fold_column`,
  `py_lower`), extended with the normalization a cross-file comparison needs:
  `Ä` reaches us composed (U+00C4) from one file and decomposed (`A` + U+0308)
  from the other, and those are the SAME column. NEVER SQL `lower()` (ASCII-only,
  so `Kön` would not fold) and never `_curation.fold_column` (NFKD + ASCII-drop,
  which folds DISTINCT columns together).
* An omitted row names the `Registerversionnamn` the catalog carries VERBATIM
  (`register_version`, matched to a held edition through `edition_claims` — the
  coalescer's own parse of an SCB edition name), because that is the coordinate
  `scb_errata.toml` is authored in. A held edition no documented version covers
  needs a `[[version]]` minted first, named with its period token; the
  `[[delivered]]` then lists that token beside the real names.
* A coordinate whose slugs do not resolve at all is SKIPPED and counted, never
  failed: that is `reg_meta.inventory_check`'s finding, repaired by regenerating
  the inventory (release step 11). A gate that failed on it would go red on
  ordinary pre-v1 slug churn instead of on a real holdings contradiction.
"""

from __future__ import annotations

import functools
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

# `_merge` / `_render` / `_intersect` are `reg_meta.inventory`'s interval
# primitives: the ONE grammar an edition, a project period and an availability
# window expand and render through (`order.py`, `catalog.py` and
# `reg_webapp.catalog_index` reach into them the same way). A held range must read
# here exactly as it does in the inventory's own errors. `_variant_ids` /
# `_variable_ids` are `inventory_check`'s batched exact-slug resolvers, so this
# gate and the §12 boot gate agree about which coordinates exist.
from reg_meta.inventory import _intersect, _merge, _render, edition_bounds
from reg_meta.inventory_check import _variable_ids, _variant_ids

# `_CURATED_PROVIDERS` is the build's own `(provider slug, input_data subdir)`
# registry — the one `sources/curated.py` reads — so the surface this gate names
# for a non-SCB miss is the file the build really takes that provider's windows
# from. `edition_claims` is the coalescer's own parse of a `Registerversionnamn`
# into the years it claims, so a version covers a held edition here exactly as it
# delivers one in the build. The package's shared `_toml_str` quotes every string
# leaf, so a stanza reads like every other candidate emitter's. `_PROVIDER` is the
# ONE provider `scb_errata.toml` accepts, imported rather than restated so the
# partition here cannot drift from the loader's own refusal.
from reg_meta_build.db import _CURATED_PROVIDERS
from reg_meta_build.edition_bounds import edition_claims
from reg_meta_build.fqid_slugs import _toml_str
from reg_meta_build.scb_errata import _PROVIDER

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Sequence

    from reg_meta.inventory import DeliveryInventory, InventoryColumn

# An inclusive ISO `(lo, hi)` date interval — `reg_meta.inventory`'s currency.
_Interval = tuple[str, str]
# One binding at the DB's own grain: `(variable_id, register_variant_id)`.
_PairIds = tuple[int, int]
# One inventory coordinate as the mappings spell it: `(variant coord, FQID)`.
_Coord = tuple[str, str]

# The curator's own two fields. Emitted as real TOML values, not comments, so a
# stanza is COMPLETE the moment it is pasted — every key `load_scb_errata`
# requires is present. `noted` is the one placeholder the loader refuses (it
# demands a canonical `YYYY-MM-DD`), which is the point: an undated entry, and so
# an uncurated one, cannot reach a build.
_TODO_EVIDENCE = "TODO: the evidence that SCB delivered this row"
_TODO_NOTED = "TODO: YYYY-MM-DD"


@dataclass(frozen=True)
class CoverageMiss:
    """One `(register, variant, column)` the steward holds in editions the
    catalog has no window for — on the `scb` provider, the grain
    `scb_errata.toml`'s `[[delivered]]` repairs, so a group is one curation
    decision.

    `register` is the 2-segment `provider/register` FQID and `variant` the variant
    slug, the pair `[[delivered]]` is keyed on. `column` is the CANONICAL delivery
    column (the mapping's `representation`, SCB's own spelling), falling back to
    the held spelling for a mapping that pins none. `versions` are the
    `Registerversionnamn` the omitted rows must name, and `mint` the subset of
    those the catalog does not know at all (each needs its own `[[version]]`).
    `editions` are the uncovered edition intervals and `windows` what the catalog
    does carry for the column here.
    """

    register: str
    variant: str
    column: str
    editions: tuple[_Interval, ...]
    versions: tuple[str, ...]
    mint: tuple[str, ...]
    windows: tuple[_Interval, ...]

    @property
    def coordinate(self) -> str:
        return f"{self.register}/{self.variant}"

    @property
    def provider(self) -> str:
        """The FQID's provider segment — what decides where the repair goes."""
        return self.register.partition("/")[0]

    @property
    def errata(self) -> bool:
        """Can `scb_errata.toml` repair this miss? Only on the `scb` provider:
        that file records what SCB omitted from SCB's OWN export, and its loader
        refuses any other provider, so rendering a stanza for one would aim the
        maintainer at a file that cannot accept it. Every other provider's windows
        are curated elsewhere (`_curated_surface`).
        """
        return self.provider == _PROVIDER


@dataclass(frozen=True)
class CoverageReport:
    """`misses` plus the denominators, both at the gate's own grain of one held
    column × edition claim: `pairs` judged, `missed_pairs` of them uncovered, and
    `unresolved` mappings skipped because their coordinate names nothing in the
    catalog."""

    misses: tuple[CoverageMiss, ...]
    pairs: int
    missed_pairs: int
    unresolved: int

    @property
    def errata_misses(self) -> tuple[CoverageMiss, ...]:
        """The misses `scb_errata.toml` can repair — the only ones that render as
        stanzas (`CoverageMiss.errata`)."""
        return tuple(miss for miss in self.misses if miss.errata)

    @property
    def curated_misses(self) -> tuple[CoverageMiss, ...]:
        """The misses repaired on their own provider's curated window surface. The
        two partitions are stated HERE so the gate, the worklist and the generator's
        counts cannot disagree about which miss is which."""
        return tuple(miss for miss in self.misses if not miss.errata)


def coverage_misses(
    conn: sqlite3.Connection, inventory: DeliveryInventory
) -> CoverageReport:
    """Every held column × edition the catalog in `conn` has no window for.

    Deterministic: misses come sorted by `(register, variant, column)`, each
    carrying its editions ascending and its version names sorted, so two runs over
    the same inputs produce the same report (and the same worklist file).
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
    windows = _load_windows(conn, set(pair_ids.values()))
    # Read on the FIRST miss only, and only over the variants a placement can
    # reach: a GREEN gate — the steady state this gate drives towards — never
    # touches `register_version` at all, because a documented version matters only
    # once a held edition is missing from it.
    versions = functools.cache(
        lambda: _load_versions(conn, {variant for _, variant in pair_ids.values()})
    )

    accumulated: defaultdict[tuple[str, str, str], _Accumulator] = defaultdict(
        _Accumulator
    )
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
                    accumulated[place.key].record(
                        edition,
                        place.windows,
                        versions().naming(place.variant_id, edition),
                    )
    return CoverageReport(
        misses=tuple(accumulated[key].finish(*key) for key in sorted(accumulated)),
        pairs=pairs,
        missed_pairs=missed_pairs,
        unresolved=unresolved,
    )


def miss_line(miss: CoverageMiss) -> str:
    """One report line locating `miss` in the grammar of ITS repair.

    A `scb` coordinate names the held editions merged into ranges, what the catalog
    carries instead, and the versions the omitted rows must list; the pasteable
    repair is then the stanza block (`errata_stanzas`), not this line. A coordinate
    on any other provider names the curated surface that carries its window
    instead, because no stanza can repair it — the line IS the whole finding.
    """
    if not miss.errata:
        return (
            f"{miss.coordinate} {miss.column}: {_held_vs_windows(miss)} — not "
            f"errata (provider `{miss.provider}`, not `{_PROVIDER}`); the window is "
            f"curated in {_curated_surface(miss.provider)}"
        )
    mint = (
        f" ({', '.join(miss.mint)} not documented at all — a [[version]] each)"
        if miss.mint
        else ""
    )
    return (
        f"{miss.coordinate} {miss.column}: {_held_vs_windows(miss)}, omitted from "
        f"version(s) {', '.join(miss.versions)}{mint}"
    )


def version_candidates(
    misses: Sequence[CoverageMiss],
) -> tuple[tuple[str, str, str], ...]:
    """The `[[version]]` entries the `scb` misses in `misses` need: one `(register,
    variant, Registerversionnamn)` per version the catalog does not know at all,
    deduped and ordered. Two columns omitted from the same undocumented edition need
    the version minted ONCE — `load_scb_errata` refuses a duplicate `(variant,
    name)`. A miss on another provider contributes nothing: it is not errata.
    """
    return tuple(
        sorted(
            {
                (miss.register, miss.variant, name)
                for miss in misses
                if miss.errata
                for name in miss.mint
            }
        )
    )


def errata_stanzas(misses: Sequence[CoverageMiss]) -> str:
    """The `scb` misses in `misses` as a pasteable `scb_errata.toml` fragment: every
    `[[version]]` the undocumented editions need first (the `[[delivered]]` rows
    hang on them), then one `[[delivered]]` per group.

    Valid, complete TOML — it parses, and `load_scb_errata` accepts its shape but
    for the placeholder `noted`. A candidate all the same, NOT a drop-in: only the
    maintainer can write the `evidence` that makes an entry an upstream-error
    record rather than a window override, and the date it was found.

    A miss on another provider is dropped HERE rather than filtered by the caller:
    the one renderer of this grammar is the one place that can guarantee no stanza
    ever names a provider the loader refuses (`miss_line` reports those instead).
    """
    errata = [miss for miss in misses if miss.errata]
    stanzas = _version_stanzas(errata) + [_delivered_stanza(miss) for miss in errata]
    return "\n\n".join(stanzas) + "\n" if stanzas else ""


def errata_worklist(report: CoverageReport) -> str:
    """`report` as a candidate `scb_errata.toml` file in three sections a maintainer
    works down: the `[[version]]` entries the undocumented editions need, the
    `[[delivered]]` entries the `scb` misses need — and then, as COMMENTS because no
    stanza of this file can carry them, the misses on every other provider with the
    curated surface each one's window is widened on.
    """
    errata = report.errata_misses
    curated = report.curated_misses
    versions = _version_stanzas(errata)
    lines = [
        "# GENERATED by input_data/swecov/build_catalog.py errata — candidate",
        "# reg_meta_build/scb_errata.toml entries for the columns the steward holds",
        "# in editions the flavored catalog has no state or alias window for.",
        "#",
        "# CURATE, don't paste wholesale: `evidence` and `noted` ride as TODO",
        "# placeholders, and the loader refuses a placeholder `noted`, so nothing",
        "# here reaches a build until a maintainer has established that SCB really",
        "# delivered the row and dated the finding. A miss whose column SCB never",
        "# documents anywhere is a variable_grafts.toml graft or a canonical_attach",
        "# entry instead — not errata.",
        "#",
        "# Only the `scb` sections are stanzas. This file corrects SCB's own export,",
        "# and its loader refuses an entry on another provider, so every non-SCB miss",
        "# rides in the third section as a comment naming the surface its window is",
        "# curated on — nothing to paste here, a window to widen there.",
        "#",
        f"# {report.missed_pairs} held column × edition pair(s) in "
        f"{len(report.misses)} group(s), out of {report.pairs} judged.",
        "",
        f"# ── version-missing: {len(versions)} held `scb` edition(s) the catalog "
        "documents no register version for; mint them first ──",
    ]
    for stanza in versions:
        lines += ["", stanza]
    lines += [
        "",
        f"# ── column-missing: the omitted column rows themselves, "
        f"{len(errata)} `scb` (register, variant, column) group(s) ──",
    ]
    for miss in errata:
        lines += ["", f"# {_held_vs_windows(miss)}", _delivered_stanza(miss)]
    lines += [
        "",
        f"# ── curated windows: {len(curated)} group(s) on a provider other than "
        "`scb`, which this file cannot correct — widen the window on the surface "
        "named on each line ──",
    ]
    for miss in curated:
        lines += ["", f"# {miss_line(miss)}"]
    return "\n".join(lines) + "\n"


# ── the catalog side ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Windows:
    """The catalog's merged delivery windows at the two grains the gate asks
    about: one column of one binding, and any column of one binding (the
    single-representation arm)."""

    by_column: dict[tuple[_PairIds, str], tuple[_Interval, ...]]
    by_pair: dict[_PairIds, tuple[_Interval, ...]]

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


def _load_windows(conn: sqlite3.Connection, pairs: set[_PairIds]) -> _Windows:
    """One streaming scan per delivery table, filtered against the inventory's own
    pairs, so the working set stays the inventory's and not the catalog's (the same
    posture `inventory_check._delivered` takes). The two tables are read by ONE
    loop body because this gate wants their flat union: a delivery is a delivery
    whichever table states it.

    `delivery_column_name` folds HERE, once, on the way in: the DB's spelling and
    the inventory's meet only under `_fold`, and folding at the boundary keeps
    every comparison downstream fold against fold.
    """
    by_column: dict[tuple[_PairIds, str], list[_Interval]] = {}
    by_pair: dict[_PairIds, list[_Interval]] = {}
    for delivery_table in ("variable_state", "variable_alias_window"):
        for variable_id, variant_id, column, valid_from, valid_to in conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name, "
            f"valid_from, valid_to FROM {delivery_table}"
        ):
            pair = (variable_id, variant_id)
            if pair not in pairs:
                continue
            by_pair.setdefault(pair, []).append((valid_from, valid_to))
            # NULL on `variable_state` only (`variable_alias_window` is a column
            # row by definition): a state with no delivery column still delivers
            # the BINDING, which is the `by_pair` grain, just not a named column.
            if column is not None:
                by_column.setdefault((pair, _fold(column)), []).append(
                    (valid_from, valid_to)
                )
    return _Windows(
        by_column={key: _merge(rows) for key, rows in by_column.items()},
        by_pair={key: _merge(rows) for key, rows in by_pair.items()},
    )


@dataclass(frozen=True)
class _Versions:
    """Each variant's documented `Registerversionnamn` with the period it claims,
    name-sorted. A version IS the coordinate an errata entry is authored in, so
    this is what an omitted row names — never a date, never a synthesized token
    when the catalog has SCB's own spelling."""

    by_variant: dict[int, tuple[tuple[str, tuple[_Interval, ...]], ...]]

    def naming(self, variant_id: int, edition: _Interval) -> tuple[str, ...] | None:
        """The version names an omitted row for `edition` must list, or `None` when
        no documented version covers it.

        Every version OVERLAPPING the edition contributes its name, and the
        edition counts as documented only when their merged claims contain it: a
        held `2010..2012` edition is three annual versions, and one that runs off
        the end of the documented history is not documented at all.
        """
        overlapping = [
            (name, claims)
            for name, claims in self.by_variant.get(variant_id, ())
            if any(_intersect(edition, claim) for claim in claims)
        ]
        documented = _merge([claim for _, claims in overlapping for claim in claims])
        if not _covers(edition, documented):
            return None
        return tuple(name for name, _ in overlapping)


def _load_versions(conn: sqlite3.Connection, variants: set[int]) -> _Versions:
    """One scan of `register_version`, filtered to the variants the inventory can
    actually reach (a coordinate whose variable did not resolve is never asked).

    A name with no parseable year claims nothing (`edition_claims` is empty) and
    is dropped: it can neither document a held edition nor be the token a
    `[[version]]` mints.
    """
    claimed: dict[int, dict[str, list[_Interval]]] = {}
    for variant_id, name in conn.execute(
        "SELECT register_variant_id, registerversionnamn FROM register_version"
    ):
        if variant_id not in variants or not name:
            continue
        claims = [(lo, hi) for _, lo, hi in edition_claims(name)]
        if claims:
            claimed.setdefault(variant_id, {}).setdefault(name, []).extend(claims)
    return _Versions(
        by_variant={
            variant_id: tuple(
                sorted((name, _merge(rows)) for name, rows in names.items())
            )
            for variant_id, names in claimed.items()
        }
    )


@dataclass(frozen=True)
class _Placement:
    """One resolved mapping of one physical column, with everything that does not
    depend on the edition already derived: the errata group the mapping lands in,
    the windows its coverage is judged against, and the variant whose documented
    versions an omitted row would name."""

    key: tuple[str, str, str]
    windows: tuple[_Interval, ...]
    variant_id: int


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
                variant_id=pair[1],
            )
        )
    return placed


@dataclass
class _Accumulator:
    """One `(register, variant, column)` group under construction."""

    editions: set[_Interval] = field(default_factory=set)
    versions: set[str] = field(default_factory=set)
    mint: set[str] = field(default_factory=set)
    windows: set[_Interval] = field(default_factory=set)

    def record(
        self,
        edition: _Interval,
        windows: tuple[_Interval, ...],
        names: tuple[str, ...] | None,
    ) -> None:
        self.editions.add(edition)
        self.windows.update(windows)
        if names is None:
            # Nothing documented covers this edition: the repair mints the version
            # under its own period token, and the omitted row names that token.
            token = _render((edition,))
            self.mint.add(token)
            self.versions.add(token)
        else:
            self.versions.update(names)

    def finish(self, register: str, variant: str, column: str) -> CoverageMiss:
        return CoverageMiss(
            register=register,
            variant=variant,
            column=column,
            editions=tuple(sorted(self.editions)),
            versions=tuple(sorted(self.versions)),
            mint=tuple(sorted(self.mint)),
            # Merged: the group folds every table stating the column, so the
            # coordinate's windows are one continuous history, not a per-table list.
            windows=_merge(list(self.windows)),
        )


def _fold(column: str) -> str:
    """A delivery column's identity for matching: NFC, then Python `str.lower()`.

    `reg_webapp.catalog_index._fold_column`'s rule (`py_lower`) is the lowercase
    half — the fold every reader that matches a HELD column against a catalog row
    goes through, Python's and never SQL's (SQLite `LOWER()` is ASCII-only, so
    `Kön` would not fold). NFC is the half a CROSS-FILE comparison needs: the held
    spelling comes out of the steward's inventory TOML and the catalog's out of
    the DB, and `Ä` is one codepoint in one and two in the other without changing
    which column SCB delivered. Not `_curation.fold_column` (NFKD + ASCII-drop),
    which folds DISTINCT columns onto each other.
    """
    return unicodedata.normalize("NFC", column).lower()


def _covers(edition: _Interval, windows: tuple[_Interval, ...]) -> bool:
    """Is `edition` wholly inside `windows`? `windows` is merged, so abutting
    coverage is one interval and single-interval containment IS union coverage."""
    lo, hi = edition
    return any(window[0] <= lo and hi <= window[1] for window in windows)


# ── the repair surface ───────────────────────────────────────────────────────

# Where each non-SCB provider's delivery windows are curated — the file a miss on
# that provider is repaired in, since `scb_errata.toml` refuses it. Built from the
# build's own `(slug, input_data subdir)` registry, so a renamed subdir moves this
# pointer with it instead of leaving it aimed at a path that no longer exists.
# `sos` is the one seeded provider in neither registry: it has a machine export
# (untracked workbooks) rather than a curated TOML, so its window comes from the
# delivery itself.
_CURATED_SURFACE: dict[str, str] = {
    slug: f"reg_meta_build/input_data/{subdir}/{slug}.toml (`valid_from` / "
    "`valid_to` on the register or the variant)"
    for slug, subdir in _CURATED_PROVIDERS
} | {
    "sos": "reg_meta_build/input_data/Socialstyrelsen/ (the SOS delivery "
    "workbooks — SOS states its own coverage, there is no curated TOML)"
}


def _curated_surface(provider: str) -> str:
    """The surface carrying `provider`'s windows, named as the repair for a miss
    `scb_errata.toml` cannot take.

    A provider the global build does not seed at all is a STEWARD-MINTED flavor
    provider (`extend-db`'s `providers[]`): nothing in this repo states its windows,
    so the repair is in the inventory the steward's generator produced.
    """
    return _CURATED_SURFACE.get(
        provider,
        "the steward inventory extend-db overlays (this flavor provider's own "
        "`states[].valid_from` / `valid_to`)",
    )


# ── rendering ────────────────────────────────────────────────────────────────


def _ranges(intervals: tuple[_Interval, ...]) -> str:
    """Intervals merged into the inventory's own period rendering (`2010..2012`,
    `2019-Q3`) — abutting editions read as the run they are. A miss's `windows` are
    merged already (`_Accumulator.finish`) and render through `_render` directly."""
    return _render(_merge(list(intervals)))


def _held_vs_windows(miss: CoverageMiss) -> str:
    """The contradiction in one clause: what the steward holds, what the catalog
    carries. The gate's failure line and the worklist's comment say it the same
    way because they describe the same miss."""
    return (
        f"held {_ranges(miss.editions)}, catalog windows "
        f"{_render(miss.windows) if miss.windows else 'none'}"
    )


def _version_stanzas(misses: Sequence[CoverageMiss]) -> list[str]:
    return [
        _stanza(
            "version",
            (
                ("register", _toml_str(register)),
                ("variant", _toml_str(variant)),
                ("name", _toml_str(name)),
            ),
        )
        for register, variant, name in version_candidates(misses)
    ]


def _delivered_stanza(miss: CoverageMiss) -> str:
    versions = ", ".join(_toml_str(name) for name in miss.versions)
    return _stanza(
        "delivered",
        (
            ("register", _toml_str(miss.register)),
            ("variant", _toml_str(miss.variant)),
            ("column", _toml_str(miss.column)),
            ("versions", f"[{versions}]"),
        ),
    )


def _stanza(header: str, fields: tuple[tuple[str, str], ...]) -> str:
    """One entry as valid, complete TOML — every key `load_scb_errata` requires,
    with the curator's own two as TODO placeholders."""
    body = "".join(f"{key} = {value}\n" for key, value in fields)
    return (
        f"[[{header}]]\n{body}"
        f"evidence = {_toml_str(_TODO_EVIDENCE)}\n"
        f"noted = {_toml_str(_TODO_NOTED)}"
    )
