"""Read-only succession-candidate curation worklist diagnostic.

SCB variable identity is `(register_id, var_id)` and the build pools columns only
WITHIN one var_id (`sources/scb.py::_triage_groups`); a split container gives every
column its own variable (`_apply_split`). There is deliberately no automatic pooling
across var_ids and no label-change rule, so a NEVER-CO-DELIVERED rename lands as two
catalog variables with no edge between them: `ForvErs` under var 31395 (…2021) and
var 47670 (2022…) are two browse rows for one column, and inside a split container
`PeOrgNr` / `PeOrgNr_LISA` are the `-2` sibling pair. The designed fix is a CURATED
`replaced_by` edge in `curation/relations.toml`; what was missing is the diagnostic
that finds the candidates.

Neither existing diagnostic does it: `split_sibling_suspects.py` (#918) explicitly
SKIPS non-co-delivered pairs (its gate 1 — a pair that never overlapped is not a
mis-import), and `variable_same_as.py` proposes interchangeability, not succession.
This is the complement of #918: same corpus, opposite temporal gate.

Same "diagnostics make worklists, curated TOML lands them" pattern as
`split_sibling_suspects` / `classifications.dump_classification_residue`: it reads a
BUILT DB, NEVER mutates, and materializes NOTHING. Unlike #918's worklist the emitted
TOML is in the exact `[[edge]]` grammar `relations.py` accepts, so a CONFIRMED
candidate copies across into `curation/relations.toml` verbatim (the same text-only
boundary `variable_same_as.render_candidates_toml` has). There is still no loader for
the file itself — the maintainer curates.

A CANDIDATE PAIR is two `variable_state` rows in ONE register (any variant
coordinate) whose windows are disjoint and ADJACENT (`_adjacent`: the later
`valid_from` is the day, or the year, after the earlier `valid_to`), in one of two
shapes:

  - `cross_var_id` — the SAME delivery column under two var_ids (the re-minted
    variable, `ForvErs`). Emitted at VARIABLE grain, like the curated "Summa annan
    inkomst" block.
  - `split_rename` — two variables sharing ONE `provider_key` (split-container
    siblings) whose columns DIFFER (the never-co-delivered rename, `PeOrgNr` →
    `PeOrgNr_LISA`). Emitted at REPRESENTATION grain (`from_column` / `to_column`,
    #843, plus `variant` #846 when every era pair sits in one register_variant).

Two gates drop a pair: it is already joined by a `variable_replaced_by` /
`representation_replaced_by` edge in EITHER direction (the curation is done), or the
two variables were CO-DELIVERED (overlapping states in one `register_variant` — the
same shape #918 gates ON, and its territory, not this one's).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

from reg_meta_build._curation import fold_column
from reg_meta_build.db import _progress
from reg_meta_build.fqid_slugs import _toml_comment, _toml_str

if TYPE_CHECKING:
    import sqlite3

# The two candidate shapes. `cross_var_id` emits a variable-grain edge,
# `split_rename` a representation-grain one; both are `type = "replaced_by"`.
KINDS: tuple[str, ...] = ("cross_var_id", "split_rename")


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
    by the EARLIEST adjacent era pair found for the two variables. `kind` is
    `cross_var_id` (same column, two var_ids — variable grain) or `split_rename`
    (one var_id, two split siblings, differing columns — representation grain).
    `variant` is the register_variant slug scoping a representation edge; `''` on a
    variable-grain candidate and on a rename whose evidence spans several variants
    (a variable-level rename)."""

    register_fqid: str
    kind: str
    predecessor: Era
    successor: Era
    variant: str

    @property
    def effective_year(self) -> int:
        """The year the successor era opens — the edge's `effective_year`."""
        return int(self.successor.valid_from[:4])


@dataclass(frozen=True)
class SuccessionResult:
    """The diagnostic's output: the candidates plus the headline counts. Read-only
    — nothing is materialized. `candidates` is sorted for a deterministic emit;
    `per_register_counts` is `{register_fqid: count}` (a register with no candidate
    is absent) and `per_kind_counts` carries both `KINDS` (zero included)."""

    candidates: tuple[SuccessionCandidate, ...]
    total: int
    per_register_counts: dict[str, int]
    per_kind_counts: dict[str, int]


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

    A side needs a delivery column (both shapes compare columns) and needs its
    provider/register/variable/variant slugs (the emitted FQIDs and `variant` must
    resolve against the built DB). The ORDER BY fixes the row order, so the groups
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
    `register_variant` with OVERLAPPING `[valid_from, valid_to]`. The same gate
    `split_sibling_suspects._codelivered_pairs` applies (there to REQUIRE overlap,
    here to reject it) — a co-delivered pair is a parallel-representation question,
    #918's territory, not a succession.

    Both are the same window-overlap PROXY for the build's edition-exact
    co-delivery (`variable_state` ships no `regver_id`; see that helper's note).
    The proxy over-includes a boundary pair, which is conservative THERE (one extra
    reviewable row) but LOSSY here (a dropped candidate), so a genuine succession
    whose predecessor lags one projected state into the successor's window is
    missing from the worklist until the exact edition join is recomputable."""
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
    (`_adjacent`) within its register, in the two shapes the module docstring
    defines, then drops a pair that is already edged (`_edged_variable_pairs`) or
    CO-DELIVERED (`_codelivered`). Several era pairs can evidence ONE succession
    (the same two variables meeting in several variants), so pairs are grouped per
    `(kind, predecessor, successor, columns)` and emitted once, evidenced by the
    EARLIEST transition; a group whose era pairs do not all sit in one
    register_variant emits unscoped (`variant = ''`)."""
    edged = _edged_variable_pairs(conn)

    # The two pairing universes. `cross_var_id` pairs within a (register, column),
    # keyed on `fold_column` — the corpus's canonical column identity (the SCB
    # rule-2 connectivity key), so case/diacritic header twins are ONE column here
    # exactly as they are to the build. `split_rename` pairs within a split family,
    # the (register, provider_key) key `split_sibling_suspects` uses.
    by_column: dict[tuple[int, str], list[Era]] = {}
    by_family: dict[tuple[int, str], list[Era]] = {}
    for register_id, era in _delivered_eras(conn):
        by_column.setdefault((register_id, fold_column(era.column)), []).append(era)
        by_family.setdefault((register_id, era.provider_key), []).append(era)

    # simplify: O(n^2) in the era count of ONE group — one column's eras, or one
    # split family's, inside ONE register. That is a handful of rows per delivering
    # variant on the real corpus; bucket a group by successor start year if a
    # register ever grows one past a few thousand eras.
    pairs: list[tuple[str, Era, Era]] = []
    for kind, universe in (("cross_var_id", by_column), ("split_rename", by_family)):
        for members in universe.values():
            for pred in members:
                for succ in members:
                    if kind == "cross_var_id":
                        # Same column, so the pair is a candidate only ACROSS
                        # var_ids: within one var_id it is one variable's own era
                        # chain, or a #918 split whose siblings share a column.
                        shaped = pred.provider_key != succ.provider_key
                    else:
                        # One var_id: two DIFFERENT siblings whose columns differ (a
                        # single variable's own column rename needs no edge).
                        shaped = pred.variable_id != succ.variable_id and fold_column(
                            pred.column
                        ) != fold_column(succ.column)
                    if (
                        shaped
                        and _adjacent(pred.valid_to, succ.valid_from)
                        and frozenset((pred.fqid, succ.fqid)) not in edged
                    ):
                        pairs.append((kind, pred, succ))

    # Only the pairs that got this far need the co-delivery corpus.
    windows = _variable_windows(
        conn, {era.variable_id for _, pred, succ in pairs for era in (pred, succ)}
    )

    # Group by the EMITTED EDGE's identity so one succession emits one `[[edge]]`:
    # the columns are part of a representation edge's identity but not a
    # variable-grain one, so two variables succeeding each other on SEVERAL shared
    # columns are one cross_var_id candidate, not one per column.
    groups: dict[tuple[str, str, str, str, str], list[tuple[Era, Era]]] = {}
    for kind, pred, succ in pairs:
        if _codelivered(windows, pred.variable_id, succ.variable_id):
            continue
        columns = (
            ("", "")
            if kind == "cross_var_id"
            else (fold_column(pred.column), fold_column(succ.column))
        )
        groups.setdefault((kind, pred.fqid, succ.fqid, *columns), []).append(
            (pred, succ)
        )

    candidates: list[SuccessionCandidate] = []
    for (kind, *_), evidence in groups.items():
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
        # #846 scope — representation grain ONLY (the loader rejects `variant` on a
        # variable-grain edge), and only when EVERY era pair of this succession sits
        # inside one register_variant. Evidence spanning variants (or meeting across
        # them) is a variable-level rename, emitted unscoped.
        scopes = (
            {p.variant if p.variant == s.variant else "" for p, s in evidence}
            if kind == "split_rename"
            else {""}
        )
        candidates.append(
            SuccessionCandidate(
                # Both endpoints are in one register, so either FQID's leading
                # `provider/register` is the candidate's register.
                register_fqid=pred.fqid.rsplit("/", 1)[0],
                kind=kind,
                predecessor=pred,
                successor=succ,
                variant=next(iter(scopes)) if len(scopes) == 1 else "",
            )
        )

    candidates.sort(
        key=lambda c: (
            c.register_fqid,
            c.kind,
            c.predecessor.fqid,
            c.successor.fqid,
            c.predecessor.column,
            c.successor.column,
            c.variant,
        )
    )
    per_register_counts = dict(Counter(c.register_fqid for c in candidates))
    # Every kind is reported, zero included, so the summary's shape is stable.
    per_kind_counts = dict.fromkeys(KINDS, 0) | Counter(c.kind for c in candidates)
    _progress(
        f"  {len(candidates):,} succession candidate(s) across "
        f"{len(per_register_counts):,} register(s) ("
        + ", ".join(f"{per_kind_counts[kind]:,} {kind}" for kind in KINDS)
        + ")"
    )
    return SuccessionResult(
        candidates=tuple(candidates),
        total=len(candidates),
        per_register_counts=per_register_counts,
        per_kind_counts=per_kind_counts,
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

    Grouped by register, deterministic within it. A `cross_var_id` candidate emits a
    VARIABLE-grain edge (`from` / `to` / `effective_year`); a `split_rename` one
    emits a REPRESENTATION-grain edge, adding `from_column` / `to_column` and the
    `variant` scope when the evidence has one. No `note` is emitted — the transition
    reason is the curator's to write."""
    lines = [
        "# GENERATED succession candidate worklist — "
        "reg-meta-build succession-candidates.",
        "#",
        "# Two catalog variables that look like consecutive ERAS of one delivered",
        "# column: their delivery windows are DISJOINT and ADJACENT (the later era",
        "# starts the day, or the year, after the earlier one ends) and they were",
        "# NEVER co-delivered. Two shapes:",
        "#   cross_var_id — the SAME delivery column under two SCB var_ids (a",
        "#                  re-minted variable): a VARIABLE-grain edge.",
        "#   split_rename — two split-container siblings of ONE var_id whose",
        "#                  columns differ (a never-co-delivered rename): a",
        "#                  REPRESENTATION-grain edge (from_column / to_column,",
        "#                  plus `variant` when all the evidence sits in one",
        "#                  register_variant).",
        "# A pair already joined by a variable/representation replaced_by edge is",
        "# skipped, and so is a CO-DELIVERED pair (that is split-sibling-suspects'",
        "# territory — its gate is this one's mirror image).",
        "#",
        "# NOTHING here loads into a build. These are CANDIDATES, not confirmed",
        "# edges: review each against the register's documentation and copy only",
        "# the confirmed ones into reg_meta_build/curation/relations.toml, adding a",
        "# `note` with the transition reason.",
        "#",
        f"# {result.total} candidate(s) across "
        f"{len(result.per_register_counts)} register(s): "
        + ", ".join(f"{result.per_kind_counts[kind]} {kind}" for kind in KINDS)
        + ".",
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
        lines.append(f"# {c.kind}")
        lines.append(_era_comment("from", c.predecessor))
        lines.append(_era_comment("to  ", c.successor))
        lines.append("[[edge]]")
        lines.append('type = "replaced_by"')
        lines.append(f"from = {_toml_str(c.predecessor.fqid)}")
        lines.append(f"to = {_toml_str(c.successor.fqid)}")
        if c.kind == "split_rename":
            lines.append(f"from_column = {_toml_str(c.predecessor.column)}")
            lines.append(f"to_column = {_toml_str(c.successor.column)}")
            if c.variant:
                lines.append(f"variant = {_toml_str(c.variant)}")
        lines.append(f"effective_year = {c.effective_year}")

    return "\n".join(lines) + "\n"
