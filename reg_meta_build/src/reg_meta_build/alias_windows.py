"""Build-side alias-window materialization.

``variable_alias`` is the shipped full delivery-column set, but it has no time
coordinate. For cvids where SCB lists several delivery columns on the same
concrete delivery instance, those columns are co-delivered representations of the
same state, not search-only aliases. This pass records state-window aliases in
``variable_alias_window`` so ``Catalog.states()`` / ``resolve_at()`` can expose
them through the existing representation picker contract.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.queries import extract_year

from .edition_bounds import edition_bounds

if TYPE_CHECKING:
    from collections.abc import Callable


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


def _version_bounds(version_name: str | None) -> tuple[str, str] | None:
    year = extract_year(version_name or "")
    return edition_bounds(version_name, year)


def _state_overlaps_bounds(
    valid_from: str, valid_to: str, bounds: tuple[str, str] | None
) -> bool:
    if bounds is None:
        return True
    lo, hi = bounds
    return valid_from <= hi and valid_to >= lo


def materialize_multi_alias_windows(
    conn: sqlite3.Connection,
    *,
    progress: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Emit ``variable_alias_window`` rows for multi-alias SCB cvids.

    Scope is intentionally narrow: only cvids with more than one DISTINCT
    ``variable_alias_build`` column get considered. Ordinary one-column aliases
    keep the 1:1 resolver path. For each affected cvid, the pass finds the single
    shipped ``variable_state`` that owns that cvid's edition and writes one
    window per co-delivered alias column using the state's own validity bounds.
    Mixed-shape cvids whose aliases map to several state windows stay as ordinary
    search/header aliases; they are not one-state representation families. The
    representative state column is always included, so adding alias windows never
    hides the base delivery column.
    """
    required = {
        "variable_alias_build",
        "variable_instance",
        "register_version",
        "variable_state",
        "variable_alias_window",
    }
    if not all(_table_exists(conn, table) for table in required):
        return {"cvids": 0, "windows": 0, "skipped": 0}

    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    multi_cvids = {
        int(row["cvid"])
        for row in cur.execute(
            "SELECT cvid FROM variable_alias_build "
            "GROUP BY cvid HAVING COUNT(DISTINCT delivery_column_name) > 1"
        )
    }
    if not multi_cvids:
        return {"cvids": 0, "windows": 0, "skipped": 0}

    cvid_list = sorted(multi_cvids)
    placeholders = ",".join("?" for _ in cvid_list)
    cvid_rows = cur.execute(
        "SELECT vi.cvid, vi.variable_id, vi.register_variant_id, "
        "vi.value_set_id, COALESCE(vi.value_set_version_label, '') AS label, "
        "rv.registerversionnamn "
        "FROM variable_instance vi "
        "JOIN register_version rv ON rv.regver_id = vi.regver_id "
        f"WHERE vi.cvid IN ({placeholders}) "
        "ORDER BY vi.cvid",
        cvid_list,
    ).fetchall()
    aliases_by_cvid: dict[int, tuple[str, ...]] = {
        int(row["cvid"]): tuple(
            str(alias_row["delivery_column_name"])
            for alias_row in cur.execute(
                "SELECT delivery_column_name FROM variable_alias_build "
                "WHERE cvid = ? ORDER BY delivery_column_name",
                (row["cvid"],),
            )
        )
        for row in cvid_rows
    }

    windows: set[tuple[int, int, str, str, str]] = set()
    unresolved: list[str] = []
    skipped = 0
    for row in cvid_rows:
        aliases = aliases_by_cvid[int(row["cvid"])]
        if row["variable_id"] is None:
            unresolved.append(
                f"cvid={row['cvid']} register_variant_id={row['register_variant_id']} "
                f"version={row['registerversionnamn']!r} has no owning variable_id"
            )
            continue
        bounds = _version_bounds(row["registerversionnamn"])
        strict_states = cur.execute(
            "SELECT state_id, valid_from, valid_to, delivery_column_name "
            "FROM variable_state "
            "WHERE variable_id = ? AND register_variant_id = ? "
            "AND value_set_id IS ? AND value_set_version_label = ? "
            "ORDER BY valid_from, valid_to, state_id",
            (
                row["variable_id"],
                row["register_variant_id"],
                row["value_set_id"],
                row["label"],
            ),
        ).fetchall()
        matches = [
            state
            for state in strict_states
            if _state_overlaps_bounds(state["valid_from"], state["valid_to"], bounds)
        ]
        if not matches:
            fallback_states = cur.execute(
                "SELECT state_id, valid_from, valid_to, delivery_column_name "
                "FROM variable_state "
                "WHERE variable_id = ? AND register_variant_id = ? "
                "ORDER BY valid_from, valid_to, state_id",
                (row["variable_id"], row["register_variant_id"]),
            ).fetchall()
            matches = [
                state
                for state in fallback_states
                if _state_overlaps_bounds(
                    state["valid_from"], state["valid_to"], bounds
                )
            ]
        if len(matches) > 1:
            alias_matches = [
                state for state in matches if state["delivery_column_name"] in aliases
            ]
            if alias_matches:
                matches = alias_matches
        if bounds is not None and len(matches) > 1:
            start_matches = [
                state for state in matches if state["valid_from"] == bounds[0]
            ]
            if start_matches:
                matches = start_matches
        unique_matches = {
            (state["state_id"], state["valid_from"], state["valid_to"])
            for state in matches
        }
        if len(unique_matches) != 1:
            skipped += 1
            continue
        match_state_id, valid_from, valid_to = next(iter(unique_matches))
        state_column = next(
            state["delivery_column_name"]
            for state in matches
            if state["state_id"] == match_state_id
            and state["valid_from"] == valid_from
            and state["valid_to"] == valid_to
        )
        columns = set(aliases)
        if state_column:
            columns.add(str(state_column))
        for column in columns:
            windows.add(
                (
                    int(row["variable_id"]),
                    int(row["register_variant_id"]),
                    column,
                    valid_from,
                    valid_to,
                )
            )

    if unresolved:
        sample = "\n".join(f"  {line}" for line in unresolved[:10])
        more = "" if len(unresolved) <= 10 else f"\n  ... {len(unresolved) - 10} more"
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="alias_window_unresolved_state",
            error_class="configuration",
            message=(
                "Multi-alias cvid(s) could not be mapped to exactly one "
                "variable_state window:\n"
                f"{sample}{more}"
            ),
            remediation=(
                "Check the coalesced variable_state identity for these cvids; "
                "alias windows must be anchored to one shipped state window."
            ),
        )

    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO variable_alias_window "
        "(variable_id, register_variant_id, delivery_column_name, valid_from, valid_to) "
        "VALUES (?, ?, ?, ?, ?)",
        sorted(windows),
    )
    inserted = conn.total_changes - before
    if progress is not None:
        skipped_note = f"; skipped {skipped:,} mixed-shape cvid(s)" if skipped else ""
        progress(
            f"  {inserted:,} alias representation windows from "
            f"{len(cvid_list) - skipped:,} multi-alias cvid(s){skipped_note}"
        )
    return {"cvids": len(cvid_list), "windows": inserted, "skipped": skipped}
