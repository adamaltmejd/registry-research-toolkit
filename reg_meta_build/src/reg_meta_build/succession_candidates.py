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
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

from reg_meta_build._curation import fold_column
from reg_meta_build.fqid_slugs import _toml_comment, _toml_str
from reg_meta_build.source_files import _progress

if TYPE_CHECKING:
    import sqlite3


@dataclass(frozen=True)
class Era:
    """One side of a candidate pair: the delivering `variable_state` plus the
    identity a curator reads. `fqid` is the provider/register/variable slug triple
    (the FQID the emitted edge carries), `variant` the delivering register_variant
    slug, `column` the era's `delivery_column_name`, and `[valid_from, valid_to]`
    the era window."""

    variable_id: int
    fqid: str
    name: str | None
    provider_key: str
    variant: str
    column: str
    valid_from: str
    valid_to: str


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


@dataclass(frozen=True)
class SuccessionResult:
    """The diagnostic's output: the candidates plus the headline counts. Read-only
    — nothing is materialized. `candidates` is sorted for a deterministic emit;
    `per_register_counts` is `{register_fqid: count}` (a register with no candidate
    is absent)."""

    candidates: tuple[SuccessionCandidate, ...]
    total: int
    per_register_counts: dict[str, int]


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
    comments' variant must resolve against the built DB). The ORDER BY fixes the row order, so the groups
    built below never inherit SQLite's."""
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
            s.valid_to
        FROM variable_state s
        JOIN variable v ON v.variable_id = s.variable_id
        JOIN register r ON r.register_id = v.register_id
        JOIN provider p ON p.provider_id = r.provider_id
        JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id
        WHERE s.delivery_column_name IS NOT NULL
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
        ) in cursor
    ]


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


def _edged_variable_pairs(conn: sqlite3.Connection) -> set[frozenset[str]]:
    """The unordered variable-FQID pairs already joined by a succession edge, from
    BOTH `variable_replaced_by` and `representation_replaced_by` (the two grains
    this diagnostic proposes). Direction-blind on purpose: an existing edge either
    way means the pair is curated, so it is not a candidate. The representation
    table is collapsed to its variable endpoints — a curated column rename between
    two variables settles the pair whatever columns it names."""
    # Both tables carry the same predecessor_*/successor_* endpoint columns, so one
    # statement serves both (the table names are literals, not input).
    sql = (
        "SELECT predecessor_provider || '/' || predecessor_register || '/' "
        "         || predecessor_variable, "
        "       successor_provider || '/' || successor_register || '/' "
        "         || successor_variable "
        "FROM {}"
    )
    return {
        frozenset(row)
        for table in ("variable_replaced_by", "representation_replaced_by")
        for row in conn.execute(sql.format(table))
    }


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

    # Pairs form within a (register, column), keyed on `fold_column` — the corpus's
    # canonical column identity (the SCB rule-2 connectivity key), so case/diacritic
    # header twins are ONE column here exactly as they are to the build.
    by_column: dict[tuple[int, str], list[Era]] = {}
    for register_id, era in _delivered_eras(conn):
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
    _progress(
        f"  {len(candidates):,} succession candidate(s) across "
        f"{len(per_register_counts):,} register(s)"
    )
    return SuccessionResult(
        candidates=tuple(candidates),
        total=len(candidates),
        per_register_counts=per_register_counts,
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
        return "\n".join(lines) + "\n"

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

    return "\n".join(lines) + "\n"
