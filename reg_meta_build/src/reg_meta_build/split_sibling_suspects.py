"""Read-only split-sibling curation worklist diagnostic (#918).

#800 retired the researcher-facing `related` edge and its `import_bug_suspect`
relation kind; the fold-or-sever curation of those split-sibling pairs was
DEFERRED to this diagnostic. The signal is recomputed READ-ONLY from a BUILT DB —
the `variable_related_to` table is GONE, so the shape evidence is re-derived from
`variable` / `variable_state` directly (NEVER mutates; nothing is auto-applied).

This is the residue-style "diagnostics make worklists, curated TOML lands them"
pattern — the EXACT analog of `classifications.dump_classification_residue` /
`render_residue_toml`: it productizes a curation backlog into a comment-rich
worklist a maintainer reads, and materializes NOTHING. There is no loader for the
emitted TOML on purpose — the maintainer folds (`concept_groups.toml`) or severs
by hand; the `disposition` placeholder is the human's to fill.

A SPLIT family is the variables sharing one `(register_id, provider_key)` (the
A2.2 triage minted siblings under one source var_id). For each intra-family PAIR
the shape relation is classified off each variable's REPRESENTATIVE state (its
latest-era `variable_state`): a `type_flip` (one side numeric, the other text —
the primary SCB/SOS import-bug signal) or a `length_disagree` (an unclassifiable
"other" type on at least one side with a present-on-both differing `data_length`)
is a SUSPECT. This mirrors `sources/scb.py::_import_bug_suspect` (the build-time
fold gate) exactly, reusing the same `_data_type_class` classifier so the
diagnostic and the build can't diverge on what "numeric-vs-text" means.

Each suspect also records whether the two variables are ALREADY co-grouped (share
a `concept_group` via `concept_group_variable`): a co-grouped suspect already
folds into one browse row, so it is the resolved case; an un-grouped suspect is
the OPEN fold/sever question the worklist exists to surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import TYPE_CHECKING

from reg_meta_build._curation import _data_type_class
from reg_meta_build.db import _progress
from reg_meta_build.fqid_slugs import _toml_comment, _toml_str

if TYPE_CHECKING:
    import sqlite3


@dataclass(frozen=True)
class SiblingShape:
    """One split sibling's REPRESENTATIVE shape — the fields of its latest-era
    `variable_state` (latest `valid_to`, deterministic tiebreak) plus its FQID +
    name. `has_value_set` is `value_set_id IS NOT NULL` on that state (value-set
    presence, not the id). `delivery_column` is the representative era's delivery
    column (NULL when the latest state carried none). A variable with NO state at
    all still appears (LEFT JOIN) with every shape field None — it can still pair,
    but never classifies as a suspect (an all-None side is `other` with no
    length)."""

    variable_id: int
    fqid: str  # provider/register/variable slug triple
    name: str | None
    data_type: str | None
    data_length: str | None
    has_value_set: bool
    delivery_column: str | None


@dataclass(frozen=True)
class SplitSiblingSuspect:
    """One suspect intra-family PAIR: the two siblings' shapes, the `reason`
    (`type_flip` | `length_disagree`), and whether they are already co-grouped.
    `register_fqid` is the shared `provider/register` slug; `provider_key` is the
    siblings' shared source var_id. `a` / `b` are ordered by `variable_id` for a
    deterministic emit."""

    register_fqid: str
    provider_key: str
    a: SiblingShape
    b: SiblingShape
    reason: str  # "type_flip" | "length_disagree"
    co_grouped: bool


@dataclass(frozen=True)
class SplitSiblingResult:
    """The diagnostic's output: every suspect pair plus the headline counts.
    Read-only — nothing is materialized. `family_count` / `family_variable_count`
    are the split-family totals (the #805 sanity check: 2295 families / 8412
    variables on the real corpus); `total_pairs` is the suspect-pair count
    (== `len(suspects)`); `co_grouped_count` is how many already fold."""

    suspects: tuple[SplitSiblingSuspect, ...]
    total_pairs: int
    family_count: int
    family_variable_count: int
    co_grouped_count: int


def _split_relation_reason(a: SiblingShape, b: SiblingShape) -> str | None:
    """Classify a pair's shape relation, returning the suspect `reason` or None.

    Mirrors `sources/scb.py::_import_bug_suspect` (the build-time fold gate),
    reusing the shared `_data_type_class` classifier:

    - `type_flip` — one side numeric, the other text (the primary import-bug
      signal: one lumped delivery shipped as a number, the other as text);
    - `length_disagree` — at least one side's type is unclassifiable (`other`)
      AND both carry a `data_length` AND the lengths differ (the only remaining
      shape evidence when the numeric/text class can't be read).

    A SAME-class length difference is deliberately NOT a suspect: on genuinely
    distinct split siblings differing widths are normal and would fire on nearly
    every split, diluting the worklist. Returns None for a non-suspect pair."""
    class_a, class_b = _data_type_class(a.data_type), _data_type_class(b.data_type)
    if {class_a, class_b} == {"numeric", "text"}:
        return "type_flip"
    if (
        "other" in (class_a, class_b)
        and a.data_length
        and b.data_length
        and a.data_length != b.data_length
    ):
        return "length_disagree"
    return None


def _representative_shapes(conn: sqlite3.Connection) -> dict[int, SiblingShape]:
    """Per variable, the REPRESENTATIVE shape from its latest-era `variable_state`.

    "Latest era" = the state with the greatest `valid_to`, tie-broken by
    `valid_from` then `state_id` (both descending) so the pick is deterministic
    regardless of insert order. A variable with no state at all still gets an
    entry (LEFT JOIN) with all shape fields None. FQID is the provider/register/
    variable slug triple (a NULL slug renders as an empty segment — still
    informative on a partial build)."""
    rows = conn.execute(
        """
        SELECT
            v.variable_id,
            p.slug AS provider_slug,
            r.slug AS register_slug,
            v.slug AS variable_slug,
            v.name AS name,
            rep.data_type,
            rep.data_length,
            rep.value_set_id,
            rep.delivery_column_name
        FROM variable v
        JOIN register r ON r.register_id = v.register_id
        JOIN provider p ON p.provider_id = r.provider_id
        LEFT JOIN (
            -- One representative variable_state per variable: the latest era.
            -- ROW_NUMBER over (valid_to, valid_from, state_id) DESC picks it
            -- deterministically; rn = 1 is the representative.
            SELECT
                vs.variable_id,
                vs.data_type,
                vs.data_length,
                vs.value_set_id,
                vs.delivery_column_name,
                ROW_NUMBER() OVER (
                    PARTITION BY vs.variable_id
                    ORDER BY vs.valid_to DESC, vs.valid_from DESC, vs.state_id DESC
                ) AS rn
            FROM variable_state vs
        ) rep ON rep.variable_id = v.variable_id AND rep.rn = 1
        """
    ).fetchall()
    shapes: dict[int, SiblingShape] = {}
    for (
        variable_id,
        p_slug,
        r_slug,
        v_slug,
        name,
        data_type,
        data_length,
        value_set_id,
        delivery_column,
    ) in rows:
        fqid = f"{p_slug or ''}/{r_slug or ''}/{v_slug or ''}"
        shapes[variable_id] = SiblingShape(
            variable_id=variable_id,
            fqid=fqid,
            name=name,
            data_type=data_type,
            data_length=data_length,
            has_value_set=value_set_id is not None,
            delivery_column=delivery_column,
        )
    return shapes


def _co_grouped_pairs(conn: sqlite3.Connection) -> set[frozenset[int]]:
    """The unordered `{variable_id, variable_id}` pairs that share at least one
    `concept_group` (via `concept_group_variable`). A co-grouped suspect already
    folds into one browse row — the resolved case. Self-joins the membership
    table on `group_id` with `a < b` to emit each unordered pair once."""
    rows = conn.execute(
        """
        SELECT DISTINCT m1.variable_id, m2.variable_id
        FROM concept_group_variable m1
        JOIN concept_group_variable m2
          ON m1.group_id = m2.group_id
         AND m1.variable_id < m2.variable_id
        """
    ).fetchall()
    return {frozenset((a, b)) for a, b in rows}


def infer_split_sibling_suspects(conn: sqlite3.Connection) -> SplitSiblingResult:
    """Emit the #918 split-sibling SUSPECT worklist from a BUILT DB (read-only —
    NEVER mutates).

    A SPLIT family is the variables sharing one `(register_id, provider_key)` with
    `provider_key IS NOT NULL` and >= 2 members. For each intra-family PAIR the
    shape relation is classified off each variable's representative
    `variable_state` (`_representative_shapes`); a `type_flip` or `length_disagree`
    pair (`_split_relation_reason`, mirroring the build-time
    `_import_bug_suspect`) is a SUSPECT. Each suspect records whether the pair is
    already co-grouped (`_co_grouped_pairs`). Nothing is materialized."""
    shapes = _representative_shapes(conn)
    co_grouped = _co_grouped_pairs(conn)

    # Split families: (register_id, provider_key) with >= 2 members. provider_key
    # IS NOT NULL by the DDL (NOT NULL column), but the explicit guard documents
    # the family key and is harmless. Grouped in SQL so only multi-member families
    # come back; the variable_ids are gathered per family in Python.
    family_rows = conn.execute(
        """
        SELECT register_id, provider_key, variable_id
        FROM variable
        WHERE provider_key IS NOT NULL
          AND (register_id, provider_key) IN (
              SELECT register_id, provider_key
              FROM variable
              WHERE provider_key IS NOT NULL
              GROUP BY register_id, provider_key
              HAVING COUNT(*) > 1
          )
        ORDER BY register_id, provider_key, variable_id
        """
    ).fetchall()

    families: dict[tuple[int, str], list[int]] = {}
    for register_id, provider_key, variable_id in family_rows:
        families.setdefault((register_id, provider_key), []).append(variable_id)

    family_count = len(families)
    family_variable_count = sum(len(members) for members in families.values())

    suspects: list[SplitSiblingSuspect] = []
    for (_register_id, provider_key), members in families.items():
        # Members are already variable_id-ascending (ORDER BY above), so each
        # combination is (a, b) with a.variable_id < b.variable_id — a
        # deterministic, once-per-pair emit.
        for vid_a, vid_b in combinations(members, 2):
            a, b = shapes[vid_a], shapes[vid_b]
            reason = _split_relation_reason(a, b)
            if reason is None:
                continue
            # register_fqid is the shared provider/register slug (drop the
            # variable segment of either sibling's FQID — both share it).
            register_fqid = a.fqid.rsplit("/", 1)[0]
            suspects.append(
                SplitSiblingSuspect(
                    register_fqid=register_fqid,
                    provider_key=provider_key,
                    a=a,
                    b=b,
                    reason=reason,
                    co_grouped=frozenset((vid_a, vid_b)) in co_grouped,
                )
            )

    co_grouped_count = sum(1 for s in suspects if s.co_grouped)
    _progress(
        f"  {len(suspects):,} split-sibling suspect pair(s) across "
        f"{family_count:,} split families / {family_variable_count:,} variables "
        f"({co_grouped_count:,} already co-grouped)"
    )
    return SplitSiblingResult(
        suspects=tuple(suspects),
        total_pairs=len(suspects),
        family_count=family_count,
        family_variable_count=family_variable_count,
        co_grouped_count=co_grouped_count,
    )


def render_suspects_toml(result: SplitSiblingResult) -> str:
    """Render the suspect worklist as a comment-rich TOML a maintainer curates by
    hand — one `[[pair]]` per suspect, GROUPED BY family and sorted high-value-
    first (largest families first, then register / var_id). Built by hand (not
    `tomli_w`) so the per-pair evidence `#` comments survive; every interpolated
    string goes through the shared `_toml_str` / `_toml_comment` leaves for
    round-trip safety, the same as `render_residue_toml`.

    Each `[[pair]]` carries the shape evidence (both sides' FQID / name / delivery
    column / data_type / data_length / value-set presence, the suspect reason, and
    the co-group status) and a `disposition = ""` placeholder for the maintainer
    to set to `"fold"` or `"distinct"`. NOTHING loads this — it is a worklist for
    humans; the fold lands via `concept_groups.toml` and a sever is a no-op (the
    edge is already gone). Read-only worklist only."""
    # Group suspects by family (register_fqid, provider_key) so each family's
    # pairs emit together; sort families high-value-first (most pairs first), then
    # register / provider_key for stability.
    by_family: dict[tuple[str, str], list[SplitSiblingSuspect]] = {}
    for s in result.suspects:
        by_family.setdefault((s.register_fqid, s.provider_key), []).append(s)

    families_sorted = sorted(
        by_family.items(),
        # -len(pairs) → largest family first; then register / provider_key.
        key=lambda item: (-len(item[1]), item[0][0], item[0][1]),
    )

    lines = [
        "# GENERATED split-sibling curation worklist — "
        "reg-meta-build split-sibling-suspects.",
        "#",
        "# A SPLIT family is the variables the A2.2 triage minted under one source",
        "# var_id (shared (register, provider_key)). A SUSPECT pair disagrees on",
        "# physical SHAPE in a way that suggests one delivery was mis-imported — a",
        "# numeric-vs-text type_flip, or a length_disagree on an unclassifiable type.",
        "# #800 retired the `related` edge; this re-derives the signal READ-ONLY for",
        "# the deferred fold-or-sever curation.",
        "#",
        "# NOTHING here loads into a build. For each pair set disposition to:",
        '#   "fold"     — same concept, different representation: fold the siblings',
        "#                 into one concept_groups.toml group.",
        '#   "distinct" — genuinely different concepts: leave them severed (no-op;',
        "#                 the related edge is already gone).",
        "# co_grouped = true means the pair already folds into one browse row (the",
        "# resolved case); co_grouped = false is the OPEN fold/sever question.",
        "#",
        f"# {result.total_pairs} suspect pair(s) across {result.family_count} split "
        f"families / {result.family_variable_count} variables "
        f"({result.co_grouped_count} already co-grouped).",
    ]

    def _shape_comment(label: str, sh: SiblingShape) -> str:
        name = f" ({_toml_comment(sh.name)})" if sh.name else ""
        col = _toml_comment(sh.delivery_column) if sh.delivery_column else "—"
        return (
            f"#   {label}: {_toml_comment(sh.fqid)}{name} "
            f"(variable_id {sh.variable_id}); "
            f"data_type={_toml_comment(sh.data_type) if sh.data_type else '—'}, "
            f"data_length={_toml_comment(sh.data_length) if sh.data_length else '—'}, "
            f"value_set={'yes' if sh.has_value_set else 'no'}, "
            f"delivery_column={col}"
        )

    for (register_fqid, provider_key), pairs in families_sorted:
        lines.append("")
        lines.append(
            f"# === family {_toml_comment(register_fqid)} var_id "
            f"{_toml_comment(provider_key)} — {len(pairs)} suspect pair(s) ==="
        )
        # Within a family, sort pairs by the two variable_ids for stability.
        for s in sorted(pairs, key=lambda x: (x.a.variable_id, x.b.variable_id)):
            lines.append("")
            lines.append(f"# reason: {s.reason}; co_grouped: {s.co_grouped}")
            lines.append(_shape_comment("a", s.a))
            lines.append(_shape_comment("b", s.b))
            lines.append("[[pair]]")
            lines.append(f"register = {_toml_str(register_fqid)}")
            lines.append(f"provider_key = {_toml_str(provider_key)}")
            lines.append(f"variable_a = {_toml_str(s.a.fqid)}")
            lines.append(f"variable_b = {_toml_str(s.b.fqid)}")
            lines.append(f"reason = {_toml_str(s.reason)}")
            lines.append(f"co_grouped = {'true' if s.co_grouped else 'false'}")
            lines.append('disposition = ""  # "fold" | "distinct"')

    if not result.suspects:
        lines.append("")
        lines.append("# (no suspect pairs)")

    return "\n".join(lines) + "\n"
