"""Read-only succession-candidate curation worklist diagnostic.

A variable re-minted under a new var_id appears as two catalog variables without a
succession edge. This diagnostic proposes checked ``replaced_by`` curation for
adjacent delivery eras; it does not assign identity or continuity automatically.
`variable_same_as.py` proposes interchangeability, not succession.

Same "diagnostics make worklists, curated TOML lands them" pattern as
`classifications.dump_classification_residue`: it reads a BUILT DB, NEVER mutates,
and materializes NOTHING. The emitted TOML is in the exact `[[edge]]` grammar
`relations.py` accepts, so a CONFIRMED candidate copies across into
`curation/relations.toml` verbatim (the same text-only boundary
`variable_same_as.render_candidates_toml` has). There is still no loader for the
file itself — the maintainer curates.

A CANDIDATE PAIR is two `variable_state` rows in ONE register whose windows are
disjoint and ADJACENT (`_adjacent`: the later `valid_from` is the day, or the year,
after the earlier `valid_to`) carrying the SAME delivery column under two var_ids
(the re-minted variable, `ForvErs`), in ANY variant coordinate: a re-minted variable
may move variants, and whether it did is the curator's call. Emitted at VARIABLE
grain, like the curated "Summa annan inkomst" block.

Two gates drop a pair: it is already joined by a `variable_replaced_by` /
`representation_replaced_by` edge in EITHER direction (the curation is done), or the
two variables were CO-DELIVERED (overlapping states in one `register_variant`: they
shipped together, so neither succeeds the other).

The same run reports two more kinds, also as worklist data only (#1907):

- A CONTRADICTED EDGE is a curated succession edge (a relations.toml edge, `note`
  `curated:…`; the builder's derived edges are not the curator's to fix) with an
  `effective_year` that the built catalog's deliveries disagree with: in the edge's
  scope (its variant, or every variant when unscoped; its two columns at
  representation grain) the predecessor still delivers in or after the effective
  year, or the successor delivers before it. It prevents a page that tells the
  reader a column was renamed in a year when both columns are listed (the 2016
  `CfarNr → CfarNr_LISA` edge after the 2026-10-01 errata delivered `CfarNr_LISA`
  for 1990–2015). A variable-grain edge is allowed a dual-delivery transition: the
  predecessor delivering in Y and Y+1 at most, the successor in Y-2 and Y-1 at most
  (`_DUAL_DELIVERY_YEARS`). An edge without an `effective_year` makes no year claim
  and is not checked.
- A FOLD CANDIDATE is two variables of one register whose headers differ only by an
  exact provider suffix (`PROVIDER_HEADER_SUFFIXES`: `CfarNr` / `CfarNr_LISA`). The
  succession rule needs the SAME column, so it never pairs them, and an edge is the
  wrong shape: one quantity under two headers is one variable (the `AstNr` /
  `AstNr_LISA` partition). A pair is dropped when the two headers were co-delivered
  as the provider exported them (`HushallsID` / `HushallsID_IoT`, 2020–2023), which
  is two quantities. Two overlaps do not count as co-delivery. An overlap where a
  state is errata-attested (documentation, not the provider's export: added by an
  `[[errata.delivered]]` entry, or minted by an `[[errata.column]]` entry with
  source `scb-docs`) is the curator's evidence to settle, and is reported; a
  correction of an exported state (`errata:field` and the other correction tables)
  is still the provider's export, and so is a `steward-holdings` mint (a steward's
  physical holdings). An overlap of one handover year, the last year of one header
  and the first of the other (`FamTyp` / `Famtyp_BarnReg` in 2014, the shape the
  curated `FamStall` fold has), is a transition. A curated edge does not drop a fold candidate: the edge is the
  shape a fold replaces.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

from reg_meta_build._curation import fold_column, strip_provider_suffix
from reg_meta_build.curation_tree import ErrataCuration
from reg_meta_build.fqid_slugs import _toml_comment, _toml_str
from reg_meta_build.scb_errata import _SOURCES as _COLUMN_SOURCES
from reg_meta_build.source_files import _progress

if TYPE_CHECKING:
    import sqlite3


@dataclass(frozen=True)
class Era:
    """One side of a candidate pair: the delivering `variable_state` plus the
    identity a curator reads. `fqid` is the provider/register/variable slug triple
    (the FQID the emitted edge carries), `variant` the delivering register_variant
    slug, `column` the era's `delivery_column_name`, and `[valid_from, valid_to]`
    the era window. `errata` marks a state documentation attests
    (`_errata_attested`), not the provider's own export."""

    variable_id: int
    fqid: str
    name: str | None
    provider_key: str
    variant: str
    column: str
    valid_from: str
    valid_to: str
    errata: bool = False


@dataclass(frozen=True)
class SuccessionCandidate:
    """One candidate succession: `predecessor` superseded by `successor`, evidenced
    by the EARLIEST adjacent era pair found for the two variables."""

    register_fqid: str
    predecessor: Era
    successor: Era

    @property
    def effective_year(self) -> int:
        """The year the successor era opens — the edge's `effective_year`."""
        return int(self.successor.valid_from[:4])


# One variant's delivery years: `(variant slug, first year, last year)`.
YearSpan = tuple[str, int, int]


@dataclass(frozen=True)
class EdgeContradiction:
    """A curated succession edge whose `effective_year` the deliveries disagree
    with. `variant` is the edge's scope ('' = unscoped); the columns are None on a
    variable-grain edge. `predecessor_after` lists the years, per variant, the
    predecessor delivers in or after `effective_year`; `successor_before` the
    years the successor delivers before it. At least one is non-empty."""

    predecessor: str
    successor: str
    predecessor_column: str | None
    successor_column: str | None
    variant: str
    effective_year: int
    predecessor_after: tuple[YearSpan, ...]
    successor_before: tuple[YearSpan, ...]


@dataclass(frozen=True)
class FoldSide:
    """One variable of a fold candidate: its FQID, name and native key, the header
    spellings it delivers the paired column under, and its delivery years of that
    column per variant."""

    fqid: str
    name: str | None
    provider_key: str
    columns: tuple[str, ...]
    spans: tuple[YearSpan, ...]


@dataclass(frozen=True)
class FoldCandidate:
    """A bare header (`base`) and its provider-suffixed twin (`suffixed`) under
    two variables of one register. `errata_overlaps` are the years, per variant,
    the two overlap only through an errata-attested state; `handover_years` a
    variant's single shared year that ends one header and starts the other.
    `edged` is True when a curated succession edge already joins the two."""

    register_fqid: str
    suffix: str
    base: FoldSide
    suffixed: FoldSide
    errata_overlaps: tuple[YearSpan, ...]
    handover_years: tuple[tuple[str, int], ...]
    edged: bool

    @property
    def native_variable(self) -> str | None:
        """The SCB var_id both sides were split from, when they share one: the
        partition the AstNr shape folds them in. None for two native ids."""
        base = self.base.provider_key.split(".", 1)[0]
        same = base == self.suffixed.provider_key.split(".", 1)[0]
        return base if same and base.isdigit() else None


@dataclass(frozen=True)
class SuccessionResult:
    """The diagnostic's output: the candidates plus the headline counts. Read-only
    — nothing is materialized. `candidates` is sorted for a deterministic emit;
    `per_register_counts` is `{register_fqid: count}` (a register with no candidate
    is absent). `contradictions` and `folds` are the two other kinds, also
    sorted."""

    candidates: tuple[SuccessionCandidate, ...]
    total: int
    per_register_counts: dict[str, int]
    contradictions: tuple[EdgeContradiction, ...] = ()
    folds: tuple[FoldCandidate, ...] = ()


def _adjacent(earlier_to: str, later_from: str) -> bool:
    """True when the era starting `later_from` directly SUCCEEDS the era ending
    `earlier_to`: disjoint (`later_from > earlier_to`) and adjacent — the later
    start is the DAY after the earlier end (2021-12-31 → 2022-01-01), or falls in
    the YEAR after it (SCB delivery windows are year-grained, and a clipped era can
    end mid-year: 2015-06-30 → 2016-01-01 is still the next delivery year).

    Bounds are the full-date `YYYY-MM-DD` storage contract, so the lexical
    comparison is chronological and the year prefix is a fixed slice. The day check
    only runs inside one year, so the `9999-12-31` open-ended sentinel can never
    overflow `date` arithmetic (a same-year successor to `YYYY-12-31` cannot exist).
    """
    if later_from <= earlier_to:
        return False
    year_delta = int(later_from[:4]) - int(earlier_to[:4])
    if year_delta > 1:
        return False
    if year_delta == 1:
        return True
    return date.fromisoformat(later_from) == date.fromisoformat(earlier_to) + timedelta(
        days=1
    )


def _delivered_eras(conn: sqlite3.Connection) -> list[tuple[int, Era]]:
    """Every `variable_state` that can be a candidate side, as `(register_id, Era)`.

    A side needs a delivery column (a pair shares one) and needs its
    provider/register/variable/variant slugs (the emitted FQIDs and the evidence
    comments' variant must resolve against the built DB). It needs a window too: a
    year-independent state has no era to compare. The ORDER BY fixes the row
    order, so the groups built below never inherit SQLite's."""
    cursor = conn.execute(
        """
        SELECT
            v.register_id,
            p.slug AS provider_slug,
            r.slug AS register_slug,
            v.variable_id,
            v.slug AS variable_slug,
            v.name,
            v.provider_key,
            rv.slug AS variant_slug,
            s.delivery_column_name,
            s.valid_from,
            s.valid_to,
            s.provenance
        FROM variable_state s
        JOIN variable v ON v.variable_id = s.variable_id
        JOIN register r ON r.register_id = v.register_id
        JOIN provider p ON p.provider_id = r.provider_id
        JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id
        WHERE s.delivery_column_name IS NOT NULL
          AND s.valid_from IS NOT NULL
          AND s.valid_to IS NOT NULL
          AND v.slug IS NOT NULL
          AND r.slug IS NOT NULL
          AND rv.slug IS NOT NULL
        ORDER BY v.register_id, v.slug, s.valid_from, s.valid_to, s.state_id
        """
    )
    return [
        (
            register_id,
            Era(
                variable_id=variable_id,
                fqid=f"{p_slug}/{r_slug}/{v_slug}",
                name=name,
                provider_key=provider_key,
                variant=variant_slug,
                column=column,
                valid_from=valid_from,
                valid_to=valid_to,
                errata=_errata_attested(provenance),
            ),
        )
        for (
            register_id,
            p_slug,
            r_slug,
            variable_id,
            v_slug,
            name,
            provider_key,
            variant_slug,
            column,
            valid_from,
            valid_to,
            provenance,
        ) in cursor
    ]


# The `[[errata.column]]` sources that are documentation, not a delivery: SCB's
# own documentation. Every other source (`steward-holdings`, a steward's physical
# holdings) counts as exported (operator ruling, PR #1915).
_DOCUMENTATION_COLUMN_SOURCES = frozenset({"scb-docs"})

# The `errata:<class>` provenance heads that count as exported: a correction
# table's own name (`curation_compile._curated_head`: `errata:field`,
# `errata:flags`, ...) on a state the provider exported, and a non-documentation
# `[[errata.column]]` source (`errata:steward-holdings`). Every other errata head
# is documentation: an `[[errata.delivered]]` entry's `upstream` class, which is
# free text (`omitted-column-in-version` by default), or `errata:scb-docs`.
_EXPORTED_ERRATA_CLASSES = (frozenset(ErrataCuration.model_fields) - {"delivered"}) | (
    _COLUMN_SOURCES - _DOCUMENTATION_COLUMN_SOURCES
)


def _errata_attested(provenance: str | None) -> bool:
    """True when documentation attests the state, not the provider's export: an
    `[[errata.delivered]]` entry added it, or an `[[errata.column]]` entry with
    source `scb-docs` minted it. A correction of an exported state (`errata:field`
    relabelling its header, a flag or a storage type) does not count, and neither
    does a `steward-holdings` mint, which a steward physically holds. Any
    provenance line can carry the head: a pooled state lists one per decision
    (`db.py` → `variable_state.provenance`)."""
    return provenance is not None and any(
        line.startswith("errata:")
        and line.removeprefix("errata:") not in _EXPORTED_ERRATA_CLASSES
        for line in provenance.splitlines()
    )


def _variable_windows(
    conn: sqlite3.Connection, variable_ids: set[int]
) -> dict[int, list[tuple[int, str, str]]]:
    """For the named variables, ALL their `(register_variant_id, valid_from,
    valid_to)` state windows — the corpus the co-delivery gate reads. Deliberately
    unfiltered by delivery column (a column-less state still proves the two
    variables shipped together), unlike `_delivered_eras`. The scan walks the whole
    table but KEEPS only the variables that survived pairing — a few hundred rows
    instead of the corpus's few hundred thousand."""
    windows: dict[int, list[tuple[int, str, str]]] = {}
    for variable_id, variant_id, valid_from, valid_to in conn.execute(
        "SELECT variable_id, register_variant_id, valid_from, valid_to "
        "FROM variable_state"
    ):
        if variable_id in variable_ids:
            windows.setdefault(variable_id, []).append(
                (variant_id, valid_from, valid_to)
            )
    return windows


def _codelivered(
    windows: dict[int, list[tuple[int, str, str]]], a_id: int, b_id: int
) -> bool:
    """True when the two variables were CO-DELIVERED: each has a state in the SAME
    `register_variant` with OVERLAPPING `[valid_from, valid_to]` — a parallel
    representation, not a succession.

    This is a window-overlap PROXY for the build's edition-exact co-delivery
    (`variable_state` ships no `regver_id`). The proxy over-includes a boundary
    pair, which here is LOSSY (a dropped candidate): a genuine succession whose
    predecessor lags one projected state into the successor's window is missing
    from the worklist until the exact edition join is recomputable."""
    return any(
        a_variant == b_variant and a_from <= b_to and b_from <= a_to
        for a_variant, a_from, a_to in windows.get(a_id, ())
        for b_variant, b_from, b_to in windows.get(b_id, ())
    )


# A curated succession edge's `note` provenance head (`curated:slug_toml`): the
# edges a curator wrote in relations.toml, as opposed to the builder's own
# (`derived:classification_vintage_lift`, `auto:timeseries_event`), which no
# relations.toml edit can fix or delete.
_CURATED_EDGE = "note LIKE 'curated:%'"


def _edged_variable_pairs(
    conn: sqlite3.Connection, *, curated_only: bool = False
) -> set[frozenset[str]]:
    """The unordered variable-FQID pairs already joined by a succession edge, from
    BOTH `variable_replaced_by` and `representation_replaced_by` (the two grains
    this diagnostic proposes). Direction-blind on purpose: an existing edge either
    way means the pair is settled, so it is not a candidate. The representation
    table is collapsed to its variable endpoints — a curated column rename between
    two variables settles the pair whatever columns it names. `curated_only`
    keeps the relations.toml edges alone."""
    # Both tables carry the same predecessor_*/successor_* endpoint columns, so one
    # statement serves both (the table names are literals, not input).
    sql = (
        "SELECT predecessor_provider || '/' || predecessor_register || '/' "
        "         || predecessor_variable, "
        "       successor_provider || '/' || successor_register || '/' "
        "         || successor_variable "
        "FROM {}" + (f" WHERE {_CURATED_EDGE}" if curated_only else "")
    )
    return {
        frozenset(row)
        for table in ("variable_replaced_by", "representation_replaced_by")
        for row in conn.execute(sql.format(table))
    }


def _merge_spans(spans: list[YearSpan]) -> tuple[YearSpan, ...]:
    """Collapse year spans to one `(variant, first, last)` per variant: the
    evidence a curator reads, not a gap-exact coverage."""
    merged: dict[str, tuple[int, int]] = {}
    for variant, lo, hi in spans:
        if variant in merged:
            old_lo, old_hi = merged[variant]
            lo, hi = min(lo, old_lo), max(hi, old_hi)
        merged[variant] = (lo, hi)
    return tuple((variant, lo, hi) for variant, (lo, hi) in sorted(merged.items()))


def _years(era: Era) -> tuple[int, int]:
    return int(era.valid_from[:4]), int(era.valid_to[:4])


# Maintainer ruling (PR #1915): a variable that succeeds another can be delivered
# beside it for a short transition (SUN2000 → SUN2020: both editions ship while
# users switch). At variable grain an edge is not contradicted when the
# predecessor's deliveries at or after the effective year Y all fall in
# Y..Y+DUAL-1 and the successor's deliveries before Y all fall in Y-DUAL..Y-1. A
# representation-grain edge names one column renamed to another, so it gets no
# such allowance.
_DUAL_DELIVERY_YEARS = 2


def _dual_delivery(after: list[YearSpan], before: list[YearSpan], year: int) -> bool:
    return all(hi < year + _DUAL_DELIVERY_YEARS for _, _, hi in after) and all(
        lo >= year - _DUAL_DELIVERY_YEARS for _, lo, _ in before
    )


def _contradicted_edges(
    conn: sqlite3.Connection, eras: list[Era]
) -> list[EdgeContradiction]:
    """Every curated succession edge with an `effective_year` that its endpoints'
    deliveries contradict, in its scope (module docstring). Only relations.toml
    edges (`_CURATED_EDGE`) are read: the fix the worklist names is an edit there.
    Errata-attested states count: an errata delivery is what the catalog
    publishes, so it is what the edge must agree with (#1907). A variable-grain
    dual-delivery transition (`_DUAL_DELIVERY_YEARS`) is not a contradiction."""
    by_fqid: dict[str, list[Era]] = {}
    for era in eras:
        by_fqid.setdefault(era.fqid, []).append(era)

    def scoped(fqid: str, column: str | None, variant: str) -> list[Era]:
        key = None if column is None else fold_column(column)
        return [
            era
            for era in by_fqid.get(fqid, ())
            if (key is None or fold_column(era.column) == key)
            and (not variant or era.variant == variant)
        ]

    edges = [
        (pred, succ, None, None, "", year)
        for pred, succ, year in conn.execute(
            "SELECT predecessor_provider || '/' || predecessor_register || '/' "
            "         || predecessor_variable, "
            "       successor_provider || '/' || successor_register || '/' "
            "         || successor_variable, effective_year "
            "FROM variable_replaced_by "
            f"WHERE effective_year IS NOT NULL AND {_CURATED_EDGE}"
        )
    ] + list(
        conn.execute(
            "SELECT predecessor_provider || '/' || predecessor_register || '/' "
            "         || predecessor_variable, "
            "       successor_provider || '/' || successor_register || '/' "
            "         || successor_variable, "
            "       predecessor_column, successor_column, variant, effective_year "
            "FROM representation_replaced_by "
            f"WHERE effective_year IS NOT NULL AND {_CURATED_EDGE}"
        )
    )

    # A header renamed and renamed back is two opposite edges (frida `borgnr` →
    # `PERSORGNR` in 2014, back in 2018). Each one's deliveries outside its own
    # era are the other edge's, so the years past the reverse edge are not
    # checked: a round trip is not a contradiction.
    years = {(p, s, pc, sc, v): y for p, s, pc, sc, v, y in edges}

    out: list[EdgeContradiction] = []
    for pred, succ, pred_col, succ_col, variant, year in edges:
        reverse = years.get((succ, pred, succ_col, pred_col, variant))
        until = reverse - 1 if reverse is not None and reverse > year else 9999
        since = reverse if reverse is not None and reverse < year else 0
        after = [
            (era.variant, max(lo, year), min(hi, until))
            for era in scoped(pred, pred_col, variant)
            for lo, hi in (_years(era),)
            if max(lo, year) <= min(hi, until)
        ]
        before = [
            (era.variant, max(lo, since), min(hi, year - 1))
            for era in scoped(succ, succ_col, variant)
            for lo, hi in (_years(era),)
            if max(lo, since) <= min(hi, year - 1)
        ]
        if pred_col is None and _dual_delivery(after, before, year):
            continue
        if after or before:
            out.append(
                EdgeContradiction(
                    predecessor=pred,
                    successor=succ,
                    predecessor_column=pred_col,
                    successor_column=succ_col,
                    variant=variant,
                    effective_year=year,
                    predecessor_after=_merge_spans(after),
                    successor_before=_merge_spans(before),
                )
            )
    out.sort(
        key=lambda c: (
            c.predecessor,
            c.successor,
            c.predecessor_column or "",
            c.successor_column or "",
            c.variant,
        )
    )
    return out


def _overlap(a: Era, b: Era) -> tuple[int, int] | None:
    """The years two eras of one variant share, or None."""
    (a_lo, a_hi), (b_lo, b_hi) = _years(a), _years(b)
    lo, hi = max(a_lo, b_lo), min(a_hi, b_hi)
    return (lo, hi) if lo <= hi else None


def _fold_gate(
    base: list[Era], suffixed: list[Era]
) -> tuple[list[YearSpan], list[tuple[str, int]]] | None:
    """None when the two headers were co-delivered as the provider exported them
    (two quantities, not a fold candidate). Otherwise the overlaps that do not
    count (module docstring): the errata-attested years and the handover years,
    per variant."""
    errata: list[YearSpan] = []
    handovers: list[tuple[str, int]] = []
    for variant in sorted({e.variant for e in base} & {e.variant for e in suffixed}):
        b = [e for e in base if e.variant == variant]
        s = [e for e in suffixed if e.variant == variant]
        exported: set[tuple[int, int]] = set()
        for x in b:
            for y in s:
                shared = _overlap(x, y)
                if shared is None:
                    continue
                if x.errata or y.errata:
                    errata.append((variant, *shared))
                else:
                    exported.add(shared)
        if not exported:
            continue
        if len(exported) > 1:
            return None
        ((lo, hi),) = exported
        b_years = [_years(e) for e in b if not e.errata]
        s_years = [_years(e) for e in s if not e.errata]
        ends_b = max(h for _, h in b_years) == lo and min(f for f, _ in s_years) == lo
        ends_s = max(h for _, h in s_years) == lo and min(f for f, _ in b_years) == lo
        if lo != hi or not (ends_b or ends_s):
            return None
        handovers.append((variant, lo))
    return errata, handovers


def _fold_side(eras: list[Era]) -> FoldSide:
    first = eras[0]
    return FoldSide(
        fqid=first.fqid,
        name=first.name,
        provider_key=first.provider_key,
        columns=tuple(sorted({e.column for e in eras})),
        spans=_merge_spans([(e.variant, *_years(e)) for e in eras]),
    )


def _fold_candidates(
    by_column: dict[tuple[int, str], list[Era]], edged: set[frozenset[str]]
) -> list[FoldCandidate]:
    """Pair each provider-suffixed header with its bare twin in the same register,
    across two variables, and keep the pairs `_fold_gate` admits."""
    out: list[FoldCandidate] = []
    for (register_id, column), members in by_column.items():
        stripped = strip_provider_suffix(column)
        if stripped is None or (register_id, stripped[0]) not in by_column:
            continue
        base_by_var: dict[int, list[Era]] = {}
        for era in by_column[(register_id, stripped[0])]:
            base_by_var.setdefault(era.variable_id, []).append(era)
        suff_by_var: dict[int, list[Era]] = {}
        for era in members:
            suff_by_var.setdefault(era.variable_id, []).append(era)
        for base_id, base in base_by_var.items():
            for suff_id, suffixed in suff_by_var.items():
                # One variable delivering both headers is already folded.
                if base_id == suff_id:
                    continue
                gate = _fold_gate(base, suffixed)
                if gate is None:
                    continue
                errata, handovers = gate
                base_side, suff_side = _fold_side(base), _fold_side(suffixed)
                spelling = suff_side.columns[0]
                out.append(
                    FoldCandidate(
                        register_fqid=base_side.fqid.rsplit("/", 1)[0],
                        suffix=spelling[len(spelling) - len(stripped[1]) :],
                        base=base_side,
                        suffixed=suff_side,
                        errata_overlaps=_merge_spans(errata),
                        handover_years=tuple(handovers),
                        edged=frozenset((base_side.fqid, suff_side.fqid)) in edged,
                    )
                )
    out.sort(key=lambda c: (c.register_fqid, c.base.fqid, c.suffixed.fqid))
    return out


def infer_succession_candidates(conn: sqlite3.Connection) -> SuccessionResult:
    """Emit the succession-candidate worklist from a BUILT DB (read-only — NEVER
    mutates).

    Pairs every delivered era against the eras that could directly succeed it
    (`_adjacent`) on the same column within its register, then drops a pair that is
    already edged (`_edged_variable_pairs`) or CO-DELIVERED (`_codelivered`).
    Several era pairs can evidence ONE succession, so pairs are grouped per the
    EMITTED EDGE's identity and emitted once, evidenced by the EARLIEST
    transition."""
    edged = _edged_variable_pairs(conn)
    delivered = _delivered_eras(conn)

    # Pairs form within a (register, column), keyed on `fold_column` — the corpus's
    # canonical column identity (the SCB rule-2 connectivity key), so case/diacritic
    # header twins are ONE column here exactly as they are to the build.
    by_column: dict[tuple[int, str], list[Era]] = {}
    for register_id, era in delivered:
        by_column.setdefault((register_id, fold_column(era.column)), []).append(era)

    # simplify: O(n^2) in the era count of ONE column inside ONE register. That is
    # a handful of rows per delivering variant on the real corpus; bucket a group by
    # successor start year if a register ever grows one past a few thousand eras.
    pairs: list[tuple[Era, Era]] = [
        (pred, succ)
        for members in by_column.values()
        for pred in members
        for succ in members
        # Same column, so the pair is a candidate only ACROSS var_ids: within one
        # var_id it is one variable's own era chain.
        if pred.provider_key != succ.provider_key
        and _adjacent(pred.valid_to, succ.valid_from)
        and frozenset((pred.fqid, succ.fqid)) not in edged
    ]

    # Only the pairs that got this far need the co-delivery corpus.
    windows = _variable_windows(
        conn, {era.variable_id for pred, succ in pairs for era in (pred, succ)}
    )

    # Group by the EMITTED EDGE's identity so one succession emits one `[[edge]]`:
    # `relations.py` keys a variable-grain edge on the FQIDs alone, so two variables
    # succeeding each other on SEVERAL shared columns are ONE candidate.
    groups: dict[tuple[str, str], list[tuple[Era, Era]]] = {}
    for pred, succ in pairs:
        if _codelivered(windows, pred.variable_id, succ.variable_id):
            continue
        groups.setdefault((pred.fqid, succ.fqid), []).append((pred, succ))

    candidates: list[SuccessionCandidate] = []
    for evidence in groups.values():
        # The earliest transition is the representative era pair (and its year the
        # effective_year). The tiebreak runs over the whole era pair so the pick is
        # decided by CONTENT, never by the order the pairs were appended.
        pred, succ = min(
            evidence,
            key=lambda e: (
                e[1].valid_from,
                e[0].valid_from,
                e[0].valid_to,
                e[1].valid_to,
                e[0].variant,
                e[1].variant,
                e[0].column,
                e[1].column,
            ),
        )
        candidates.append(
            SuccessionCandidate(
                # Both endpoints are in one register, so either FQID's leading
                # `provider/register` is the candidate's register.
                register_fqid=pred.fqid.rsplit("/", 1)[0],
                predecessor=pred,
                successor=succ,
            )
        )

    candidates.sort(
        key=lambda c: (c.register_fqid, c.predecessor.fqid, c.successor.fqid)
    )
    per_register_counts = dict(Counter(c.register_fqid for c in candidates))
    contradictions = _contradicted_edges(conn, [era for _, era in delivered])
    # The fold's "delete the edge" advice is for a relations.toml edge only.
    folds = _fold_candidates(by_column, _edged_variable_pairs(conn, curated_only=True))
    _progress(
        f"  {len(candidates):,} succession candidate(s) across "
        f"{len(per_register_counts):,} register(s); "
        f"{len(contradictions):,} contradicted edge(s); "
        f"{len(folds):,} fold candidate(s)"
    )
    return SuccessionResult(
        candidates=tuple(candidates),
        total=len(candidates),
        per_register_counts=per_register_counts,
        contradictions=tuple(contradictions),
        folds=tuple(folds),
    )


def _era_comment(label: str, era: Era) -> str:
    name = f" ({_toml_comment(era.name)})" if era.name else ""
    return (
        f"#   {label}: {_toml_comment(era.fqid)}{name} "
        f"var_id {_toml_comment(era.provider_key)}, "
        f"column {_toml_comment(era.column)}, "
        f"variant {_toml_comment(era.variant)}, "
        f"{era.valid_from}..{era.valid_to}"
    )


def render_succession_toml(result: SuccessionResult) -> str:
    """Render the candidates as `[[edge]] type = "replaced_by"` TOML — the EXACT
    shape `curation/relations.toml` accepts, so a confirmed candidate copies across
    verbatim (the same text-only boundary `variable_same_as.render_candidates_toml`
    has; nothing loads this file). Built by hand (not `tomli_w`) so the per-candidate
    evidence `#` comments survive; every interpolated string goes through the shared
    `_toml_str` / `_toml_comment` leaves.

    Grouped by register, deterministic within it. Each candidate emits a
    VARIABLE-grain edge (`from` / `to` / `effective_year`). No `note` is emitted —
    the transition reason is the curator's to write."""
    lines = [
        "# GENERATED succession candidate worklist — "
        "reg-meta-build succession-candidates.",
        "#",
        "# Two catalog variables that look like consecutive ERAS of one delivered",
        "# column: their delivery windows are DISJOINT and ADJACENT (the later era",
        "# starts the day, or the year, after the earlier one ends) and they were",
        "# NEVER co-delivered: the SAME delivery column under two SCB var_ids (a",
        "# re-minted variable), proposed as a VARIABLE-grain edge.",
        "# A pair already joined by a variable/representation replaced_by edge is",
        "# skipped.",
        "#",
        "# NOTHING here loads into a build. These are CANDIDATES, not confirmed",
        "# edges: review each against the register's documentation and copy only",
        "# the confirmed ones into reg_meta_build/curation/relations.toml, adding a",
        "# `note` with the transition reason.",
        "#",
        f"# {result.total} candidate(s) across "
        f"{len(result.per_register_counts)} register(s).",
    ]

    if not result.candidates:
        lines.append("")
        lines.append("# (no succession candidates)")

    current_register: str | None = None
    for c in result.candidates:
        if c.register_fqid != current_register:
            current_register = c.register_fqid
            lines.append("")
            lines.append(
                f"# === register {_toml_comment(c.register_fqid)} — "
                f"{result.per_register_counts[c.register_fqid]} candidate(s) ==="
            )
        lines.append("")
        lines.append(_era_comment("from", c.predecessor))
        lines.append(_era_comment("to  ", c.successor))
        lines.append("[[edge]]")
        lines.append('type = "replaced_by"')
        lines.append(f"from = {_toml_str(c.predecessor.fqid)}")
        lines.append(f"to = {_toml_str(c.successor.fqid)}")
        lines.append(f"effective_year = {c.effective_year}")

    lines.extend(_contradiction_lines(result.contradictions))
    lines.extend(_fold_lines(result.folds))
    return "\n".join(lines) + "\n"


def _spans_text(spans: tuple[YearSpan, ...]) -> str:
    return ", ".join(
        f"{_toml_comment(variant)} {lo}..{hi}" for variant, lo, hi in spans
    )


def _contradiction_lines(contradictions: tuple[EdgeContradiction, ...]) -> list[str]:
    """The contradicted curated edges, as comments only: each is an edge already in
    curation/relations.toml, so the fix is an edit there, never a copy."""
    lines = [
        "",
        f"# === contradicted curated edges — {len(contradictions)} edge(s) ===",
        "# A curated replaced_by edge whose effective_year the deliveries disagree",
        "# with, in the edge's scope (its variant, or every variant; its columns at",
        "# representation grain): the predecessor still delivers in or after the",
        "# year, or the successor delivers before it. A variable-grain edge may",
        "# overlap as a dual-delivery transition: the predecessor through year+1,",
        "# the successor from year-2. Derived edges are not listed. Fix or delete",
        "# the edge in curation/relations.toml; a fold may be the right shape",
        "# (below).",
    ]
    for c in contradictions:
        pred = c.predecessor + (
            f" [{c.predecessor_column}]" if c.predecessor_column else ""
        )
        succ = c.successor + (f" [{c.successor_column}]" if c.successor_column else "")
        scope = f"variant {c.variant}" if c.variant else "unscoped"
        lines.append("")
        lines.append(
            f"#   edge: {_toml_comment(pred)} -> {_toml_comment(succ)}, "
            f"{_toml_comment(scope)}, effective_year {c.effective_year}"
        )
        if c.predecessor_after:
            lines.append(
                "#     predecessor delivered from the effective year: "
                + _spans_text(c.predecessor_after)
            )
        if c.successor_before:
            lines.append(
                "#     successor delivered before the effective year: "
                + _spans_text(c.successor_before)
            )
    return lines


def _fold_side_comment(label: str, side: FoldSide) -> str:
    name = f" ({_toml_comment(side.name)})" if side.name else ""
    return (
        f"#   {label}: {_toml_comment(side.fqid)}{name} "
        f"var_id {_toml_comment(side.provider_key)}, "
        f"column {_toml_comment(' / '.join(side.columns))}: "
        f"{_spans_text(side.spans)}"
    )


def _fold_lines(folds: tuple[FoldCandidate, ...]) -> list[str]:
    """The provider-suffix fold candidates, as comments only: a fold is register
    curation (`[[identity.partition]]` plus `[[errata.field]]`), not a
    relations.toml edge, and its editions and field guards are the curator's to
    read from the source."""
    lines = [
        "",
        f"# === provider-suffix fold candidates — {len(folds)} pair(s) ===",
        "# A bare header and its provider-suffixed twin (CfarNr / CfarNr_LISA) under",
        "# two variables of one register, never co-delivered as the provider",
        "# exported them. If the two are one quantity, fold them the AstNr way in",
        "# curation/registers/<provider>/<register>.toml: map both headers to one",
        "# child in the variable's [[identity.partition]], and correct each edition",
        "# whose exported header is not the physical one with an [[errata.field]]",
        '# field = "column_name" (the AstNr / AstNr_LISA entries in lisa.toml).',
    ]
    for f in folds:
        lines.append("")
        lines.append(
            f"#   --- {_toml_comment(f.register_fqid)}: "
            f"{_toml_comment(' / '.join(f.base.columns))} + "
            f"{_toml_comment(f.suffix)}"
        )
        lines.append(_fold_side_comment("base    ", f.base))
        lines.append(_fold_side_comment("suffixed", f.suffixed))
        if f.errata_overlaps:
            lines.append(
                "#     overlap attested only by documentation ([[errata.delivered]] "
                "or a scb-docs [[errata.column]]; settle the evidence first): "
                + _spans_text(f.errata_overlaps)
            )
        if f.handover_years:
            lines.append(
                "#     one handover year delivers both headers: "
                + ", ".join(f"{_toml_comment(v)} {y}" for v, y in f.handover_years)
            )
        if f.edged:
            lines.append(
                "#     a curated replaced_by edge joins the two: delete it with the fold"
            )
        if f.native_variable is None:
            lines.append(
                "#     two native ids: no shared partition, the fold shape needs a ruling"
            )
        else:
            lines.append(
                f"#     one native variable ({f.native_variable}): fold the two "
                "partition children into one"
            )
    return lines
