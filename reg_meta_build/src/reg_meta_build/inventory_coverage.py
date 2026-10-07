"""Steward column-window diagnostics for the curator's Y-115 worklist.

The steward generator's `build_catalog.py errata` command compares the authored
inventory against catalog windows. This diagnostic is not a compiled holdings
validation gate: a physical table's record span is not each column's availability.
Compilation checks resolver-emitted columns over their whole history; query-time
resolution and ordering must separately require applicable column windows.

A miss on the `scb` provider renders as a complete `[[errata.delivered]]` stanza
only when the variable came from SCB's export. A variable whose `source_label`
is `scb-errata` came from an `[[errata.column]]` entry and has no documented row
for the delivered loader to clone. Those misses render comments directing the
curator back to the target-variant `[[errata.column]]` entry and mapping.
Independently justified `[[errata.version]]` candidates remain pasteable with
`evidence` and `noted` TODO placeholders.

A miss on ANY OTHER provider is never rendered in that grammar. `curation/registers/scb/<slug>.toml`
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
* Only a table whose edition is ONE period token is assessed. An integer is
  normalized to its annual token by the inventory loader, and sub-annual / LA
  tokens retain their exact bounds. A range or list is a multi-period table: its
  dates describe the records in the file, not every column's availability, so it
  is counted and reported as not assessed and contributes no inferred correction.
* An assessed mapping's basis is the table's full single-period `edition_bounds`.
  A miss describes the curator's coverage worklist; it does not alter the compiled
  physical holdings or determine whether a later researcher request is covered.
* `variable_state` and `variable_alias_window` are read as a FLAT UNION keyed by
  the folded delivery column, and an edition is covered when the MERGED union
  contains it (a column whose edition straddles two abutting states is delivered,
  not missing). Looser than `Catalog.delivery_columns`' resolver-based
  containment/participation rule: the question here is whether the catalog
  claims the column in that edition AT ALL, and a
  stricter read would demand errata for windows the resolver does deliver.
* Columns fold with NFC normalization and then Python `str.lower()`, on BOTH
  sides — the held spelling out of the inventory TOML and `delivery_column_name`
  out of the DB. This extends the catalog's `py_lower` rule with the
  normalization a cross-file comparison needs:
  `Ä` reaches us composed (U+00C4) from one file and decomposed (`A` + U+0308)
  from the other, and those are the SAME column. NEVER SQL `lower()` (ASCII-only,
  so `Kön` would not fold) and never `_curation.fold_column` (NFKD + ASCII-drop,
  which folds DISTINCT columns together).
* An omitted row names the `Registerversionnamn` the catalog carries VERBATIM
  (`register_version`, matched to a held edition through `edition_claims` — the
  coalescer's own parse of an SCB edition name), because that is the coordinate
  `curation/registers/scb/<slug>.toml` is authored in. A held edition no documented version covers
  needs a `[[errata.version]]` minted first, named with its period token; the
  `[[errata.delivered]]` then lists that token beside the real names. At this suggestion
  boundary only, an undocumented `LA2020` is spelled in SCB's native form
  `2020/2021`, whose edition claims have the same school-year bounds.
* A coordinate whose slugs do not resolve at all is SKIPPED and counted, never
  failed: compilation owns unresolved-coordinate failures, repaired in the
  builder inputs before publication. A gate that failed on it would go red on
  ordinary pre-v1 slug churn instead of on a real holdings contradiction.
"""

from __future__ import annotations

import functools
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

# Reuse the inventory's interval grammar at the build-time diagnostic boundary.
from reg_meta.inventory import _intersect, _merge, _render, edition_bounds

# `_CURATED_PROVIDERS` is the build's own `(provider slug, input_data subdir)`
# registry — the one `sources/curated.py` reads — so the surface this gate names
# for a non-SCB miss is the file the build really takes that provider's windows
# from. `edition_claims` is the coalescer's own parse of a `Registerversionnamn`
# into the years it claims, so a version covers a held edition here exactly as it
# delivers one in the build. The package's shared `_toml_str` quotes every string
# leaf, so a stanza reads like every other candidate emitter's. `_PROVIDER` is the
# ONE provider `curation/registers/scb/<slug>.toml` accepts, imported rather than restated so the
# partition here cannot drift from the loader's own refusal.
from reg_meta_build.db import _CURATED_PROVIDERS, catalog_coordinate_ids
from reg_meta_build.edition_bounds import edition_claims
from reg_meta_build.fqid_slugs import _toml_str
from reg_meta_build.scb_errata import _PROVIDER, ERRATA_COLUMN_SOURCE_LABEL

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
# An `[[errata.column]]`'s evidence is the other half of the same fact: what says the
# column exists, plus the export saying nothing about it. The first clause is the
# only part that differs by `source`.
_TODO_COLUMN_EVIDENCE = (
    "TODO: {attests}, and that SCB's export carries no row for it on any version "
    "of this variant"
)
_COLUMN_ATTESTATION = {
    "scb-docs": "the SCB doc page documenting this column",
    "steward-holdings": "the delivery list holding this column",
}


@dataclass(frozen=True)
class CoverageMiss:
    """One `(register, variant, column)` the steward holds in editions the
    catalog has no window for, grouped as one curation decision.

    `register` is the 2-segment `provider/register` FQID and `variant` the variant
    slug, the pair `[[errata.delivered]]` is keyed on. `column` is the CANONICAL delivery
    column (the mapping's `representation`, SCB's own spelling), falling back to
    the held spelling for a mapping that pins none. `versions` are the
    `Registerversionnamn` the omitted rows must name, and `mint` the subset of
    those the catalog does not know at all (each needs its own `[[errata.version]]`).
    `editions` are the uncovered edition intervals and `windows` what the catalog
    does carry for the column here. `errata_column` records that at least one
    grouped mapping resolves to a variable minted by a `[[errata.column]]` entry; such a
    group must never become a `[[errata.delivered]]` candidate because no source row
    exists to clone.
    """

    register: str
    variant: str
    column: str
    editions: tuple[_Interval, ...]
    versions: tuple[str, ...]
    mint: tuple[str, ...]
    windows: tuple[_Interval, ...]
    errata_column: bool
    period_scope: Literal["intervals", "year_independent"] = "intervals"

    @property
    def coordinate(self) -> str:
        return f"{self.register}/{self.variant}"

    @property
    def provider(self) -> str:
        """The FQID's provider segment — what decides where the repair goes."""
        return self.register.partition("/")[0]

    @property
    def scb(self) -> bool:
        """Is this miss on the one provider the SCB register files accept?"""
        return self.provider == _PROVIDER

    @property
    def delivered_candidate(self) -> bool:
        """Can the delivered loader clone a real SCB row for this variable?"""
        return self.scb and not self.errata_column and self.period_scope == "intervals"


@dataclass(frozen=True)
class CoverageReport:
    """`misses` plus the denominators, both at the gate's own grain of one held
    column × single delivered period: `pairs` assessed, `missed_pairs` of them
    uncovered, `unresolved` mappings skipped because their coordinate names
    nothing in the catalog, and `skipped_tables` range/list editions excluded
    because a multi-period record span is not column-availability evidence."""

    misses: tuple[CoverageMiss, ...]
    pairs: int
    missed_pairs: int
    unresolved: int
    skipped_tables: int

    @property
    def scb_misses(self) -> tuple[CoverageMiss, ...]:
        """Every miss on the provider the SCB register files accept."""
        return tuple(miss for miss in self.misses if miss.scb)

    @property
    def delivered_misses(self) -> tuple[CoverageMiss, ...]:
        """SCB misses with a real source row `[[errata.delivered]]` can clone."""
        return tuple(miss for miss in self.misses if miss.delivered_candidate)

    @property
    def errata_column_misses(self) -> tuple[CoverageMiss, ...]:
        """SCB misses on variables minted from `[[errata.column]]` entries."""
        return tuple(
            miss
            for miss in self.misses
            if miss.period_scope == "intervals" and miss.scb and miss.errata_column
        )

    @property
    def curated_misses(self) -> tuple[CoverageMiss, ...]:
        """The misses repaired on their own provider's curated window surface."""
        return tuple(
            miss
            for miss in self.misses
            if miss.period_scope == "intervals" and not miss.scb
        )


def skipped_tables_line(count: int) -> str:
    """One shared explanation for the gate and both worklist reports."""
    return (
        f"{count:,} multi-period table(s) not assessed — range/list editions "
        "describe table records, not each column's availability"
    )


def coverage_misses(
    conn: sqlite3.Connection, inventory: DeliveryInventory
) -> CoverageReport:
    """Every held column × edition the catalog in `conn` has no window for.

    Deterministic: misses come sorted by `(register, variant, column)`, each
    carrying its editions ascending and its version names sorted, so two runs over
    the same inputs produce the same report (and the same worklist file).
    """
    assessed_tables = tuple(
        table for table in inventory.tables if isinstance(table.edition, str)
    )
    coords: set[_Coord] = {
        (mapping.register_variant, str(mapping.variable))
        for table in assessed_tables
        for column in table.columns
        for mapping in column.mappings
    }
    variable_ids, variant_ids = catalog_coordinate_ids(
        conn,
        variable_fqids={variable for _, variable in coords},
        variant_coords={variant for variant, _ in coords},
    )
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
    errata_variable_ids = functools.cache(
        lambda: {
            variable_id
            for (variable_id,) in conn.execute(
                "SELECT variable_id FROM variable WHERE source_label = ?",
                (ERRATA_COLUMN_SOURCE_LABEL,),
            )
        }
    )

    accumulated: defaultdict[tuple[str, str, str], _Accumulator] = defaultdict(
        _Accumulator
    )
    independent_misses: dict[tuple[str, str, str], CoverageMiss] = {}
    pairs = 0
    missed_pairs = 0
    unresolved = 0
    for table in assessed_tables:
        if table.period_scope == "year_independent":
            for column in table.columns:
                placed = _placements(column, pair_ids, windows)
                unresolved += len(column.mappings) - len(placed)
                if not placed:
                    continue
                pairs += 1
                if any(
                    ((place.variable_id, place.variant_id), _fold(place.key[2]))
                    in windows.independent_columns
                    for place in placed
                ):
                    continue
                missed_pairs += 1
                for place in placed:
                    register, variant, canonical_column = place.key
                    independent_misses[place.key] = CoverageMiss(
                        register,
                        variant,
                        canonical_column,
                        (),
                        (),
                        (),
                        (),
                        place.variable_id in errata_variable_ids(),
                        period_scope="year_independent",
                    )
            continue
        bounds = edition_bounds(table.edition)
        for column in table.columns:
            placed = _placements(column, pair_ids, windows)
            unresolved += len(column.mappings) - len(placed)
            # An unmapped column admits nothing: it stays out of the
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
                        place.variable_id in errata_variable_ids(),
                    )
    return CoverageReport(
        misses=tuple(
            sorted(
                (
                    *(accumulated[key].finish(*key) for key in sorted(accumulated)),
                    *independent_misses.values(),
                ),
                key=lambda m: (m.register, m.variant, m.column, m.period_scope),
            )
        ),
        pairs=pairs,
        missed_pairs=missed_pairs,
        unresolved=unresolved,
        skipped_tables=len(inventory.tables) - len(assessed_tables),
    )


def miss_line(miss: CoverageMiss) -> str:
    """One report line locating `miss` in the grammar of ITS repair.

    An SCB-export coordinate names the held editions, catalog windows and versions
    the omitted rows must list; its pasteable repair is then `errata_stanzas`. An
    errata-created variable instead names the authoring surfaces to inspect. A
    coordinate on any other provider names the curated surface carrying its window.
    """
    if miss.period_scope == "year_independent":
        return f"{miss.coordinate} {miss.column}: held independent physical column has no exact independent delivery state; inspect its checked source/owner mapping without inventing dates or register versions"
    if not miss.scb:
        return (
            f"{miss.coordinate} {miss.column}: {_held_vs_windows(miss)} — not "
            f"errata (provider `{miss.provider}`, not `{_PROVIDER}`); the window is "
            f"curated in {_curated_surface(miss.provider)}"
        )
    if miss.errata_column:
        return (
            f"{miss.coordinate} {miss.column}: {_held_vs_windows(miss)} — "
            "`source_label = 'scb-errata'`; inspect the matching target-variant "
            "[[errata.column]] entry and inventory mapping; a new version covers the "
            "column only if that entry uses `all_versions = true`"
        )
    mint = (
        f" ({', '.join(miss.mint)} not documented at all — a [[errata.version]] each)"
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
    """The `[[errata.version]]` entries the `scb` misses in `misses` need: one `(register,
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
                if miss.scb
                for name in miss.mint
            }
        )
    )


def errata_stanzas(misses: Sequence[CoverageMiss]) -> str:
    """The loadable candidates grouped by register file:
    every independently missing `[[errata.version]]`, then one `[[errata.delivered]]` for each
    SCB-export variable with a source row the loader can clone.

    Valid, complete TOML — it parses, and `load_scb_errata` accepts its shape but
    for the placeholder `noted`. A candidate all the same, NOT a drop-in: only the
    maintainer can write the `evidence` that makes an entry an upstream-error
    record rather than a window override, and the date it was found.

    Another provider contributes no stanza, and an errata-created variable never
    contributes a `[[errata.delivered]]`: its `[[errata.column]]` entry has no real row to clone.
    The one renderer of this grammar therefore guarantees that every emitted entry
    is one the corresponding loader can consume (`miss_line` reports the rest).
    """
    scb = [miss for miss in misses if miss.scb]
    delivered = [miss for miss in scb if miss.delivered_candidate]
    candidates: dict[str, list[str]] = {}
    for register, stanza in _version_stanzas(scb):
        candidates.setdefault(register, []).append(stanza)
    for miss in delivered:
        candidates.setdefault(miss.register, []).append(_delivered_stanza(miss))
    blocks = [
        "\n\n".join(
            (f"# curation/registers/scb/{register.split('/', 1)[1]}.toml", *rows)
        )
        for register, rows in sorted(candidates.items())
    ]
    return "\n\n".join(blocks) + "\n" if blocks else ""


def column_stanza(variant: str, column: str, label: str, source: str) -> str:
    """One `[[errata.column]]` candidate: a column SCB's export documents on NO version
    of the variant, so there is no row to re-add and the entry mints the variable.

    Rendered HERE, beside the `[[errata.version]]` / `[[errata.delivered]]` stanzas, though its
    producer is the SWECOV generator rather than this gate — one renderer of the
    errata grammar is the one place that can keep a candidate loadable when the
    grammar gains a key. `label` is the only description either evidence source
    gives, and becomes both `name` and `definition`.

    No `data_type`: a delivery list spells its own types (`varchar`), not the four
    the loader accepts, and the key is optional precisely because a holdings list
    carries none — emitting a guess would publish it as a fact. `all_versions =
    true`: neither source dates the column, and an `scb-docs` entry's validity
    years are on its doc page, for the curator to narrow to a `versions` list.
    """
    return _stanza(
        "errata.column",
        (
            ("variant", _toml_str(variant)),
            ("column", _toml_str(column)),
            ("name", _toml_str(label)),
            ("definition", _toml_str(label)),
            ("all_versions", "true"),
            ("source", _toml_str(source)),
        ),
        evidence=_TODO_COLUMN_EVIDENCE.format(attests=_COLUMN_ATTESTATION[source]),
    )


def errata_worklist(report: CoverageReport) -> str:
    """`report` as candidate register-file tables for a maintainer.

    Loadable `[[errata.version]]` and `[[errata.delivered]]` candidates come first.
    SCB misses on `[[errata.column]]`-minted variables and all non-SCB misses are comments because
    neither can be repaired by cloning a documented SCB row.
    """
    scb = report.scb_misses
    delivered = report.delivered_misses
    errata_columns = report.errata_column_misses
    curated = report.curated_misses
    versions_by_register: dict[str, list[str]] = {}
    for register, stanza in _version_stanzas(scb):
        versions_by_register.setdefault(register, []).append(stanza)
    delivered_by_register: dict[str, list[CoverageMiss]] = {}
    for miss in delivered:
        delivered_by_register.setdefault(miss.register, []).append(miss)
    version_count = sum(len(rows) for rows in versions_by_register.values())
    lines = [
        "# GENERATED by input_data/swecov/build_catalog.py errata — candidate",
        "# reg_meta_build/curation/registers/scb/<slug>.toml entries for the columns the steward holds",
        "# in editions the flavored catalog has no state or alias window for.",
        "#",
        "# CURATE, don't paste wholesale: `evidence` and `noted` ride as TODO",
        "# placeholders, and the loader refuses a placeholder `noted`, so nothing",
        "# here reaches a build until a maintainer has established that SCB really",
        "# delivered the row and dated the finding. A variable already minted by a",
        "# [[errata.column]] entry has no real SCB row for [[errata.delivered]] to clone; its miss",
        "# is reported below for inspection, never emitted as a delivered stanza.",
        "#",
        "# Only loadable SCB correction candidates below are stanzas. This file",
        "# corrects SCB's own export and its loader refuses another provider, so every",
        "# non-SCB miss is a comment naming the surface its window is curated on.",
        "#",
        f"# {report.missed_pairs} held column × edition pair(s) in "
        f"{len(report.misses)} group(s), out of {report.pairs} assessed.",
        f"# {skipped_tables_line(report.skipped_tables)}.",
        "",
        f"# ── version-missing: {version_count} held `scb` edition(s) the catalog "
        "documents no register version for; mint them when evidenced ──",
        "# A version candidate covers an errata-created column only when its matching",
        "# target-variant [[errata.column]] entry uses `all_versions = true`.",
    ]
    lines += [
        "",
        f"# ── column-missing: the omitted column rows themselves, "
        f"{len(delivered)} [[errata.delivered]] candidate(s) ──",
    ]
    for register in sorted(set(versions_by_register) | set(delivered_by_register)):
        lines += ["", f"# curation/registers/scb/{register.split('/', 1)[1]}.toml"]
        for stanza in versions_by_register.get(register, ()):
            lines += ["", stanza]
        for miss in delivered_by_register.get(register, ()):
            lines += ["", f"# {_held_vs_windows(miss)}", _delivered_stanza(miss)]
    lines += [
        "",
        f"# ── errata-created columns: {len(errata_columns)} group(s); no "
        "[[errata.delivered]] candidate ──",
        "# Inspect each matching target-variant [[errata.column]] entry and inventory",
        "# mapping. The DB does not reveal `all_versions` versus explicit `versions`.",
        "# Add a held edition to explicit `versions` only when that entry exists,",
        "# omits it, and evidence supports the addition. A missing entry or a",
        "# routing/materialization mismatch needs its own correction, not widening.",
    ]
    for miss in errata_columns:
        lines += ["", f"# {miss.coordinate} {miss.column}: {_held_vs_windows(miss)}"]
    lines += [
        "",
        f"# ── curated windows: {len(curated)} group(s) on a provider other than "
        "`scb`, which this file cannot correct — widen the window on the surface "
        "named on each line ──",
    ]
    for miss in curated:
        lines += ["", f"# {miss_line(miss)}"]
    for miss in report.misses:
        if miss.period_scope == "year_independent":
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
    independent_columns: frozenset[tuple[_PairIds, str]] = frozenset()

    def for_mapping(
        self, pair: _PairIds, representation: str | None
    ) -> tuple[_Interval, ...]:
        """The windows a mapping's coverage is judged against. An explicit
        `representation` names ONE canonical column; `None` means "the concept's
        single representation", which any of the binding's columns can
        answer, so it is judged against the binding as a whole."""
        if representation is None:
            return self.by_pair.get(pair, ())
        return self.by_column.get((pair, _fold(representation)), ())


def _load_windows(conn: sqlite3.Connection, pairs: set[_PairIds]) -> _Windows:
    """One streaming scan per delivery table, filtered against the inventory's own
    pairs, so the working set stays the inventory's and not the catalog's. The
    two tables are read by ONE loop body because this gate wants their flat union:
    a delivery is a delivery
    whichever table states it.

    `delivery_column_name` folds HERE, once, on the way in: the DB's spelling and
    the inventory's meet only under `_fold`, and folding at the boundary keeps
    every comparison downstream fold against fold.
    """
    by_column: dict[tuple[_PairIds, str], list[_Interval]] = {}
    by_pair: dict[_PairIds, list[_Interval]] = {}
    independent_columns = set()
    for delivery_table in ("variable_state", "variable_alias_window"):
        scope = "period_scope" if delivery_table == "variable_state" else "'intervals'"
        for (
            variable_id,
            variant_id,
            column,
            valid_from,
            valid_to,
            period_scope,
        ) in conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name, "
            f"valid_from, valid_to, {scope} FROM {delivery_table}"
        ):
            pair = (variable_id, variant_id)
            if pair not in pairs:
                continue
            if period_scope == "year_independent":
                if column is not None:
                    independent_columns.add((pair, _fold(column)))
                continue
            by_pair.setdefault(pair, []).append((valid_from, valid_to))
            if column is not None:
                by_column.setdefault((pair, _fold(column)), []).append(
                    (valid_from, valid_to)
                )
    return _Windows(
        by_column={key: _merge(rows) for key, rows in by_column.items()},
        by_pair={key: _merge(rows) for key, rows in by_pair.items()},
        independent_columns=frozenset(independent_columns),
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
        edition counts as documented only when their merged claims contain the
        full delivered period. One that runs off the end of the documented
        history is not documented at all.
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
    `[[errata.version]]` mints.
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
    the windows its coverage is judged against, the variable whose provenance
    determines whether a source row exists, and the variant whose documented
    versions an omitted row would name."""

    key: tuple[str, str, str]
    windows: tuple[_Interval, ...]
    variable_id: int
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
                variable_id=pair[0],
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
    errata_column: bool = False

    def record(
        self,
        edition: _Interval,
        windows: tuple[_Interval, ...],
        names: tuple[str, ...] | None,
        errata_column: bool,
    ) -> None:
        self.editions.add(edition)
        self.windows.update(windows)
        self.errata_column |= errata_column
        if names is None:
            # Nothing documented covers this edition: the repair mints the version
            # under its SCB-native name, and the omitted row names that version.
            token = _suggested_version_name(edition)
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
            errata_column=self.errata_column,
        )


def _suggested_version_name(edition: _Interval) -> str:
    """Render a held period in the SCB free-text grammar at the suggestion edge.

    The authoring token for a school year is `LA2020`, but SCB's existing edition
    reader understands that same interval as `2020/2021`. Keep the broader SCB
    grammar unchanged; only generated names need this translation.
    """
    token = _render((edition,))
    if token.startswith("LA"):
        year = int(token[2:])
        return f"{year}/{year + 1}"
    return token


def _fold(column: str) -> str:
    """A delivery column's identity for matching: NFC, then Python `str.lower()`.

    The catalog's `py_lower` rule is the lowercase half, used when readers
    match a held column against a catalog row. Use Python's and never SQL's
    lowercase operation (SQLite `LOWER()` is ASCII-only, so
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
# that provider is repaired in, since `curation/registers/scb/<slug>.toml` refuses it. Built from the
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
    `curation/registers/scb/<slug>.toml` cannot take.

    A provider the global build does not seed at all is a STEWARD-MINTED flavor
    provider (`extend-db` provider TOMLs): its window is authored in that provider's
    steward curation file.
    """
    return _CURATED_SURFACE.get(
        provider,
        "the curated-provider TOML extend-db overlays (this flavor provider's own "
        "`state.valid_from` / `valid_to`)",
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


def _version_stanzas(misses: Sequence[CoverageMiss]) -> list[tuple[str, str]]:
    return [
        (
            register,
            _stanza(
                "errata.version",
                (
                    ("variant", _toml_str(variant)),
                    ("name", _toml_str(name)),
                ),
            ),
        )
        for register, variant, name in version_candidates(misses)
    ]


def _delivered_stanza(miss: CoverageMiss) -> str:
    versions = ", ".join(_toml_str(name) for name in miss.versions)
    return _stanza(
        "errata.delivered",
        (
            ("variant", _toml_str(miss.variant)),
            ("column", _toml_str(miss.column)),
            ("versions", f"[{versions}]"),
        ),
    )


def _stanza(
    header: str,
    fields: tuple[tuple[str, str], ...],
    evidence: str = _TODO_EVIDENCE,
) -> str:
    """One entry as valid, complete TOML — every key `load_scb_errata` requires,
    with the curator's own two as TODO placeholders."""
    body = "".join(f"{key} = {value}\n" for key, value in fields)
    return (
        f"[[{header}]]\n{body}"
        f"evidence = {_toml_str(evidence)}\n"
        f"noted = {_toml_str(_TODO_NOTED)}"
    )
