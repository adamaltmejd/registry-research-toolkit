"""Curated monthly-column-family merges (#319) — the first consumer of the
interval-native resolver (#271).

A monthly family is 12 month-named delivery columns for one concept, shipped
inside ANNUAL editions (LISA `lonfink{jan..dec}`, `agi{1,2,3}lonfink{jan..dec}`).
Today each is 12 separate catalog variables; a researcher must know the
month-suffix convention. This merge folds the 12 columns into ONE variable that
keeps an ANNUAL `variable_state` per delivery year (the per-month dimension is a
representation/alias concern, NOT a coding boundary — see DESIGN.md → Consumers:
monthly column families), and records each month column's validity window in
`variable_alias_window` so `resolve_at("2024-03")` picks the `mar` column.

NOT the `[[column_merge]]` surface (`curation/scb/source_column_repairs.toml`):
that asserts era-RENAMES that never co-occur — the exact opposite of 12
deliberately-parallel columns.

Member resolution is by `delivery_column_name`, not slug: the merge runs BEFORE
`populate_variable_slugs`, so the month columns are identified by their delivery
column ending in a month token (the same `_MONTH_TOKENS` vocab the concept-group
month pass uses), folded the same way slugs are. A family that does not resolve to
a coherent member set FAILS the build (EXIT_CONFIG) — curation drift, fix it.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.fqid import derive_variable_slug, period_token_to_bounds

from ._curation import (
    curation_error,
    load_curation_entries,
    require_str,
    resolve_register_id,
)
from .concept_groups import _MONTH_TOKENS

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

# A monthly family must resolve to at least this many distinct months to be a
# coherent merge target (mirrors the concept-group month-fold guard
# `_MIN_MONTH_SIBLINGS`). Fewer means the stem didn't actually name a monthly
# family — a curation error, not a 1-or-2-column merge.
_MIN_FAMILY_MONTHS = 3


@dataclass(frozen=True)
class MonthlyFamily:
    """One `[[monthly_family]]` entry: the 12 month columns under
    `provider/register` whose delivery-column name is `family_stem` + a month
    token merge into one variable slugged `family_stem`, labelled `label`."""

    provider: str
    register: str
    family_stem: str
    label: str


def repo_family_merges_path() -> Path | None:
    """`reg_meta_build/family_merges.toml` from a repo checkout, or None (wheel
    installs don't ship curation — a maintainer artifact like the slug TOMLs and
    `concept_groups.toml`). Package root, NOT under `fqid_slugs/`."""
    candidate = Path(__file__).resolve().parent.parent.parent / "family_merges.toml"
    return candidate if candidate.is_file() else None


_require_str = functools.partial(
    require_str,
    code="family_merges_invalid",
    prefix="family_merges",
    file_name="family_merges.toml",
)


def load_family_merges(path: Path | None) -> tuple[MonthlyFamily, ...]:
    """Parse the monthly-family-merge TOML. Empty when no file (synthetic test
    builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only `[[monthly_family]]`
    top-level; `register` is a 2-segment `provider/register` FQID; `family_stem` /
    `label` non-empty strings; each (register, family_stem) unique. Member
    RESOLUTION (do 12 month columns with that stem exist?) happens at materialize
    time against the built DB, not here."""
    entries = load_curation_entries(
        path,
        entry_key="monthly_family",
        label="monthly-family-merge",
        prefix="family_merges",
        code_base="family_merges",
        file_name="family_merges.toml",
        entry_fields="register / family_stem / label",
    )
    out: list[MonthlyFamily] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register", "[[monthly_family]]")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "family_merges_invalid",
                f"family_merges register {register_fqid!r} must be a 2-segment "
                "`provider/register` FQID.",
                'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
            )
        family_stem = _require_str(entry, "family_stem", "[[monthly_family]]")
        label = _require_str(entry, "label", "[[monthly_family]]")
        scope_key = (parts[0], parts[1], family_stem)
        if scope_key in seen:
            raise curation_error(
                "family_merges_invalid",
                f"family_merges duplicate family_stem {family_stem!r} under "
                f"{register_fqid}.",
                "Each (register, family_stem) may appear once in "
                "reg_meta_build/family_merges.toml.",
            )
        seen.add(scope_key)
        out.append(
            MonthlyFamily(
                provider=parts[0],
                register=parts[1],
                family_stem=family_stem,
                label=label,
            )
        )
    return tuple(out)


@dataclass(frozen=True)
class _Member:
    """One resolved month column of a family: the owning variable_id, the month
    (1-12 from the column's token), and its delivery column name."""

    variable_id: int
    register_variant_id: int
    month: int
    delivery_column_name: str


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


def _stem_and_month(slug: str) -> tuple[str, int] | None:
    """Split a column-derived slug into (stem, month) if it ends in a month token,
    else None. Mirrors the concept-group month-fold detection."""
    for token, month in _MONTH_TOKENS.items():
        if slug.endswith(token) and len(slug) > len(token):
            return slug[: -len(token)], month
    return None


def _family_members(
    conn: sqlite3.Connection, register_id: int, family_stem: str
) -> list[_Member]:
    """Find the month columns of `family_stem` under `register_id`. A member is a
    variable whose representative delivery column (the latest, the same basis
    `populate_variable_slugs` slugs from) derives to `family_stem` + a month token.
    Identified by `delivery_column_name` because the merge runs BEFORE slugs
    exist. One column per (variable, variant) — a variable delivering the same
    stem-month across variants yields one member per variant."""
    rows = conn.execute(
        "SELECT DISTINCT vs.variable_id, vs.register_variant_id, "
        "vs.delivery_column_name "
        "FROM variable_state vs "
        "JOIN variable v ON vs.variable_id = v.variable_id "
        "WHERE v.register_id = ? AND vs.delivery_column_name IS NOT NULL",
        (register_id,),
    ).fetchall()
    members: list[_Member] = []
    for variable_id, register_variant_id, column in rows:
        slug = derive_variable_slug(column)
        if slug is None:
            continue
        split = _stem_and_month(slug)
        if split is None or split[0] != family_stem:
            continue
        members.append(_Member(variable_id, register_variant_id, split[1], column))
    return members


def _delivered_years(conn: sqlite3.Connection, variable_id: int) -> dict[int, set[int]]:
    """Per (register_variant_id) → set of delivery years a variable's annual
    states span (year(valid_from)..year(valid_to), inclusive). The window rows
    enumerate these years × the column's month."""
    out: dict[int, set[int]] = {}
    for rvid, vfrom, vto in conn.execute(
        "SELECT register_variant_id, valid_from, valid_to FROM variable_state "
        "WHERE variable_id = ?",
        (variable_id,),
    ):
        y_lo, y_hi = int(vfrom[:4]), int(vto[:4])
        # Clamp the open-ended sentinel (9999) to the opening year — an ongoing
        # monthly column gets a window for the year it started, not to year 9999.
        if y_hi >= 9999:
            y_hi = y_lo
        out.setdefault(rvid, set()).update(range(y_lo, y_hi + 1))
    return out


def _member_value_sets(
    conn: sqlite3.Connection, variable_id: int
) -> dict[tuple[int, int], set[int | None]]:
    """Per (register_variant_id, delivery year) → the set of `value_set_id`s the
    variable's annual states carry that year (NULL preserved as a member, so a
    NULL-vs-non-NULL divergence is detectable). Used to gate the merge: all member
    columns delivering a year must agree on the value domain (cadence policy:
    within a delivery, union semantics — a silent drop of a divergent sibling's
    value set would misrepresent the domain)."""
    out: dict[tuple[int, int], set[int | None]] = {}
    for rvid, vfrom, vto, vsid in conn.execute(
        "SELECT register_variant_id, valid_from, valid_to, value_set_id "
        "FROM variable_state WHERE variable_id = ?",
        (variable_id,),
    ):
        y_lo, y_hi = int(vfrom[:4]), int(vto[:4])
        if y_hi >= 9999:
            y_hi = y_lo
        for year in range(y_lo, y_hi + 1):
            out.setdefault((rvid, year), set()).add(vsid)
    return out


def _check_value_set_agreement(
    conn: sqlite3.Connection,
    family: MonthlyFamily,
    members: list[_Member],
    ctx: str,
) -> None:
    """LOUD-FAIL if member columns delivering the SAME (variant, year) disagree on
    `value_set_id` (#319). The merge keeps ONE annual claim per year (the
    survivor's) and drops the siblings' states — so a divergent sibling value set
    would be silently lost, misrepresenting the value domain. Per the cadence
    policy a delivery's columns share the domain; a real divergence means the stem
    matched non-parallel columns (or a genuinely mergeable family needs curation).
    NULL is a value: all-NULL agrees, mixed NULL/non-NULL or differing non-NULL
    ids diverge. The column→value-set provenance for the message comes from each
    member's own states (a member is one (variable, variant))."""
    # (variant, year) → {value_set_id, …} across every member.
    per_year: dict[tuple[int, int], set[int | None]] = {}
    # (variant, year) → {value_set_id: [columns]} for the actionable message.
    cols_by_vs: dict[tuple[int, int], dict[int | None, list[str]]] = {}
    vs_cache: dict[int, dict[tuple[int, int], set[int | None]]] = {}
    for m in members:
        if m.variable_id not in vs_cache:
            vs_cache[m.variable_id] = _member_value_sets(conn, m.variable_id)
        member_vs = vs_cache[m.variable_id]
        for (rvid, year), vsids in member_vs.items():
            if rvid != m.register_variant_id:
                continue
            key = (rvid, year)
            per_year.setdefault(key, set()).update(vsids)
            for vsid in vsids:
                cols_by_vs.setdefault(key, {}).setdefault(vsid, []).append(
                    m.delivery_column_name
                )
    for (rvid, year), vsids in sorted(per_year.items()):
        if len(vsids) > 1:
            detail = "; ".join(
                f"value_set {vsid}: {sorted(set(cols_by_vs[(rvid, year)][vsid]))}"
                for vsid in sorted(vsids, key=lambda v: (v is None, v))
            )
            raise curation_error(
                "family_merges_value_set_conflict",
                f"{ctx}: member columns disagree on value_set_id for variant "
                f"{rvid}, year {year} — {detail}. The merge keeps one annual claim "
                "per year, so a divergent sibling's value set would be dropped.",
                "Confirm the family stem only matches columns sharing a value "
                "domain, or curate the family (reg_meta_build/family_merges.toml). "
                "Union/curation of a genuinely mergeable divergence is a "
                "maintainer-build decision.",
            )


def materialize_family_merges(
    conn: sqlite3.Connection,
    families: tuple[MonthlyFamily, ...],
    *,
    providers: frozenset[str],
    fold_slug_hints: dict[int, str],
    progress: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Merge each curated monthly family into ONE variable (#319).

    Runs POST-triage (so `variable_state` / `variable_alias` exist) but BEFORE
    `populate_variable_slugs` (so the survivor is slugged as the family stem via
    `fold_slug_hints`, and the merged-away siblings never get a slug). Per family:

    1. Resolve the member month columns (`_family_members`) — LOUD-FAIL if fewer
       than `_MIN_FAMILY_MONTHS` distinct months resolve (the stem named no
       coherent monthly family).
    2. Pick the lex-min `variable_id` as the survivor; set its `name` to the
       family label and register `fold_slug_hints[survivor] = family_stem`.
    3. Emit `variable_alias_window` rows: one per (column, delivered year) →
       `YYYY-MM` window. Re-point every member's `variable_alias` to the survivor
       (so `get_datacolumns` still returns all 12 columns).
    4. Consolidate `variable_state` to the survivor's ANNUAL claims: delete the
       N-1 siblings' states + the siblings' `variable` rows (regenerate-not-
       migrate; the stored state stays one annual row/year — the per-month
       dimension is read-time from the window table). The survivor's own annual
       states are the claims; a sibling delivering a year the survivor does NOT is
       a coherence break (LOUD-FAIL) — monthly families are parallel by design.

    `providers` gates families to this build (mirrors the other curation passes).
    Returns `{"families": n, "columns": n, "windows": n}`."""
    counts = {"families": 0, "columns": 0, "windows": 0}
    for family in families:
        if family.provider not in providers:
            continue
        ctx = f"[[monthly_family]] {family.family_stem!r} ({family.provider}/{family.register})"
        register_id = resolve_register_id(conn, family.provider, family.register)
        if register_id is None:
            raise curation_error(
                "family_merges_unresolved",
                f"{ctx}: register {family.provider}/{family.register!r} does not "
                "resolve.",
                "Fix the `register` FQID in reg_meta_build/family_merges.toml.",
            )
        members = _family_members(conn, register_id, family.family_stem)
        distinct_months = {m.month for m in members}
        if len(distinct_months) < _MIN_FAMILY_MONTHS:
            raise curation_error(
                "family_merges_unresolved",
                f"{ctx}: resolved {len(distinct_months)} distinct month column(s) "
                f"(need >= {_MIN_FAMILY_MONTHS}). The stem named no coherent "
                "monthly family in this build.",
                "Fix `family_stem` / `register` in reg_meta_build/family_merges.toml "
                "(the stem is the column-name prefix before the month token).",
            )
        survivor = min(m.variable_id for m in members)
        # `_family_members` yields one member per (variable, variant), so a
        # variable spanning several variants repeats its variable_id — cache the
        # per-variable delivered-years lookup so it runs once per variable, not
        # once per member.
        delivered_years_cache: dict[int, dict[int, set[int]]] = {}

        def _years_for(variable_id: int) -> dict[int, set[int]]:
            if variable_id not in delivered_years_cache:
                delivered_years_cache[variable_id] = _delivered_years(conn, variable_id)
            return delivered_years_cache[variable_id]

        survivor_years = _years_for(survivor)
        _check_value_set_agreement(conn, family, members, ctx)

        conn.execute(
            "UPDATE variable SET name = ? WHERE variable_id = ?",
            (family.label, survivor),
        )
        fold_slug_hints[survivor] = family.family_stem

        # Window rows + alias re-point, per member column.
        for m in members:
            years = _years_for(m.variable_id).get(m.register_variant_id, set())
            for year in sorted(years):
                if year not in survivor_years.get(m.register_variant_id, set()):
                    raise curation_error(
                        "family_merges_unresolved",
                        f"{ctx}: column {m.delivery_column_name!r} delivers "
                        f"{year} in variant {m.register_variant_id} but the merged "
                        "annual claim does not — monthly families must be parallel.",
                        "Verify the family stem only matches truly parallel "
                        "month columns (reg_meta_build/family_merges.toml).",
                    )
                lo, hi = period_token_to_bounds(f"{year}-{m.month:02d}")
                conn.execute(
                    "INSERT OR IGNORE INTO variable_alias_window "
                    "(variable_id, register_variant_id, delivery_column_name, "
                    "valid_from, valid_to) VALUES (?, ?, ?, ?, ?)",
                    (survivor, m.register_variant_id, m.delivery_column_name, lo, hi),
                )
                counts["windows"] += 1
            counts["columns"] += 1

        # Re-point every member's alias columns to the survivor (so get_datacolumns
        # still surfaces all 12), then drop the siblings' states + rows.
        siblings = [m.variable_id for m in members if m.variable_id != survivor]
        if siblings:
            placeholders = ",".join("?" * len(siblings))
            conn.execute(
                f"UPDATE OR IGNORE variable_alias SET variable_id = ? "
                f"WHERE variable_id IN ({placeholders})",
                (survivor, *siblings),
            )
            # Re-point the SCB cvid scratch's stamped owning variable_id too: it
            # is dropped before ship, but the `code_variable_map` SCB top-up
            # (db.py, runs AFTER this merge) reads `variable_instance.variable_id`
            # to attribute codes — leaving the sibling id there would re-insert
            # code_variable_map rows referencing the deleted sibling (dangling FK).
            # No-op when SCB didn't run (the table is empty). Guarded on the
            # table's existence: the merge runs pre-drop in a real build, but a
            # direct call against a post-ship DB (where `variable_instance` is
            # gone) has nothing to re-point — skip rather than error.
            if _table_exists(conn, "variable_instance"):
                conn.execute(
                    f"UPDATE variable_instance SET variable_id = ? "
                    f"WHERE variable_id IN ({placeholders})",
                    (survivor, *siblings),
                )
            conn.execute(
                f"DELETE FROM variable_state WHERE variable_id IN ({placeholders})",
                siblings,
            )
            # Any alias rows that collided on the survivor's PK under OR IGNORE are
            # left behind on the sibling — clear them so the variable FK has no
            # orphan after the sibling row is deleted.
            conn.execute(
                f"DELETE FROM variable_alias WHERE variable_id IN ({placeholders})",
                siblings,
            )
            conn.execute(
                f"DELETE FROM variable WHERE variable_id IN ({placeholders})",
                siblings,
            )
        counts["families"] += 1

    if progress is not None:
        progress(
            f"  {counts['families']:,} monthly families merged "
            f"({counts['columns']:,} columns, {counts['windows']:,} alias windows)"
        )
    return counts
