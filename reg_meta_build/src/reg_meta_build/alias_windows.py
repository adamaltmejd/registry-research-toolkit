"""Build-side alias-window materialization.

``variable_alias`` is the shipped full delivery-column set, but it has no time
coordinate. For cvids where SCB lists several delivery columns on the same
concrete delivery instance, those columns are co-delivered representations of the
same state, not search-only aliases. This pass records state-window aliases in
``variable_alias_window`` so ``Catalog.states()`` / ``resolve_at()`` can expose
them through the existing representation picker contract.
"""

from __future__ import annotations

import functools
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.errors import EXIT_CONFIG, RegMetaError

from ._curation import (
    curation_error,
    fold_column,
    load_curation_entries,
    require_evidence,
    require_fqid,
    require_str,
    resolve_register_id,
    resolve_variable_id,
)
from .scb_errata import _state_provenance, scoped_state_provenance
from .sources.scb import register_edition_claims

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

_FILE_NAME = "curation/alias_windows.toml"
_CODE = "alias_windows_invalid"
_FIELDS = frozenset(
    {"variable", "variant", "column", "source_editions", "evidence", "noted"}
)
_CORRECTION_CLASS = "omitted-column-in-version"

_require_str = functools.partial(
    require_str,
    code=_CODE,
    prefix="alias_windows",
    file_name=_FILE_NAME,
)
_require_evidence = functools.partial(
    require_evidence,
    code=_CODE,
    prefix="alias_windows",
    file_name=_FILE_NAME,
)


@dataclass(frozen=True)
class CuratedAliasWindow:
    """One existing alias made orderable in named source editions."""

    provider: str
    register: str
    variable: str
    variant: str
    column: str
    source_editions: tuple[str, ...]
    evidence: str

    @property
    def fqid(self) -> str:
        return f"{self.provider}/{self.register}/{self.variable}"


def _source_editions(entry: dict, context: str) -> tuple[str, ...]:
    raw = entry.get("source_editions")
    if (
        not isinstance(raw, list)
        or not raw
        or not all(isinstance(value, str) and value.strip() for value in raw)
    ):
        raise curation_error(
            _CODE,
            f"alias_windows {context} needs `source_editions` as a non-empty "
            f"list of source-edition names, got {raw!r}.",
            "Give exact `register_version.registerversionnamn` values, e.g. "
            '`source_editions = ["2018"]`.',
        )
    editions = tuple(value.strip() for value in raw)
    seen: set[str] = set()
    repeated: set[str] = set()
    for edition in editions:
        if edition in seen:
            repeated.add(edition)
        seen.add(edition)
    if repeated:
        raise curation_error(
            _CODE,
            f"alias_windows {context} repeats source edition(s) {sorted(repeated)}.",
            "Name each source edition once in an entry.",
        )
    return editions


def load_alias_windows(path: Path | None) -> tuple[CuratedAliasWindow, ...]:
    """Load exact-edition windows for aliases an identity already owns.

    The source grammar is deliberately SCB-only: source-edition bounds come
    from ``register_edition_claims``, the same reader that built SCB's states.
    Database-dependent ownership and edition checks run after variable slugs
    have been assigned in ``materialize_curated_alias_windows``.
    """
    entries = load_curation_entries(
        path,
        entry_key="alias",
        label="alias-window",
        prefix="alias_windows",
        code_base="alias_windows",
        file_name=_FILE_NAME,
        entry_fields=(
            "variable / variant / column / source_editions / evidence / noted"
        ),
    )
    out: list[CuratedAliasWindow] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for entry in entries:
        unknown = sorted(set(entry) - _FIELDS)
        if unknown:
            raise curation_error(
                _CODE,
                f"alias_windows [[alias]] entry has unknown key(s): {unknown}.",
                f"An [[alias]] entry takes only {sorted(_FIELDS)} — fix the typo "
                f"in reg_meta_build/{_FILE_NAME}.",
            )
        provider, register, variable = require_fqid(
            entry,
            "variable",
            code=_CODE,
            prefix="alias_windows",
            entry_table="[[alias]]",
            file_name=_FILE_NAME,
        )
        fqid = f"{provider}/{register}/{variable}"
        if provider != "scb":
            raise curation_error(
                "alias_windows_unknown_provider",
                f"alias_windows {fqid} names provider {provider!r}; exact "
                "source-edition alias windows currently support 'scb' only.",
                "Move the declaration to that provider's own source-edition "
                "curation, or fix the variable FQID.",
            )
        variant = _require_str(entry, "variant", f"[[alias]] {fqid}")
        column = _require_str(entry, "column", f"[[alias]] {fqid}/{variant}")
        context = f"[[alias]] {fqid}/{variant}/{column}"
        key = (provider, register, variable, variant, fold_column(column))
        if key in seen:
            raise curation_error(
                _CODE,
                f"alias_windows has duplicate declarations for {context}.",
                "Give one [[alias]] per (variable, variant, column), listing all "
                "of its exact source editions together.",
            )
        seen.add(key)
        out.append(
            CuratedAliasWindow(
                provider=provider,
                register=register,
                variable=variable,
                variant=variant,
                column=column,
                source_editions=_source_editions(entry, context),
                evidence=_require_evidence(entry, context),
            )
        )
    return tuple(out)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


def _version_bounds(
    register_id: int, version_name: str | None
) -> tuple[str, str] | None:
    """The inclusive ISO hull of everything an edition name claims.

    Goes through `register_edition_claims`, the same reading the coalescer built
    the states from — declared projection set included — so a multi-year version
    (a school year, a year range) matches the widened state it produced instead
    of only its first year, and a projection vintage still matches its own year.
    """
    claims = register_edition_claims(register_id, version_name)
    if not claims:
        return None
    return claims[0][1], claims[-1][2]


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
        "SELECT vi.cvid, vi.variable_id, vi.register_id, vi.register_variant_id, "
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
        bounds = _version_bounds(row["register_id"], row["registerversionnamn"])
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


def materialize_curated_alias_windows(
    conn: sqlite3.Connection,
    declarations: tuple[CuratedAliasWindow, ...],
    *,
    providers: frozenset[str],
    progress: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Add exact source-edition windows for aliases already on one identity.

    This pass runs after variable slug assignment, with SCB's source-edition
    metadata available. It never writes ``variable_alias`` or ``variable_state``:
    the declaration must resolve to one existing variable, variant, alias,
    source edition, source delivery instance, and containing state. Each edition
    contributes its own claim intervals from ``register_edition_claims``;
    neighboring years and gaps are never inferred.

    The curated window carries scoped correction provenance. The read side uses
    that marker to add it to the source-derived representation result, preserving
    the original state and any partial window family without writing or widening
    a synthetic base window.
    """
    active = tuple(d for d in declarations if d.provider in providers)
    if not active:
        return {"entries": 0, "windows": 0}

    required = {
        "provider",
        "register",
        "register_variant",
        "register_version",
        "variable",
        "variable_alias",
        "variable_alias_build",
        "variable_alias_window",
        "variable_instance",
        "variable_state",
    }
    missing = sorted(table for table in required if not _table_exists(conn, table))
    if missing:
        raise curation_error(
            "alias_windows_build_order",
            f"alias_windows materialization is missing required table(s): {missing}.",
            "Run alias-window curation after core graph and slug materialization, "
            "with source-edition metadata available.",
        )

    # Staged first: a later invalid declaration must not leave an earlier entry
    # partly applied when this function is exercised outside build_db's transaction.
    targets: dict[tuple[int, int, str, str, str], list[tuple[str, str]]] = {}

    for declaration in active:
        variable_id = resolve_variable_id(
            conn,
            declaration.provider,
            declaration.register,
            declaration.variable,
        )
        if variable_id is None:
            raise curation_error(
                "alias_windows_unknown_variable",
                f"alias_windows variable {declaration.fqid!r} does not resolve.",
                "Use the canonical variable FQID after slug assignment, or remove "
                "the stale declaration.",
            )
        register_id = resolve_register_id(
            conn, declaration.provider, declaration.register
        )
        assert register_id is not None  # the variable FQID resolved through it
        variant_rows = conn.execute(
            "SELECT register_variant_id FROM register_variant "
            "WHERE register_id = ? AND slug = ? ORDER BY register_variant_id",
            (register_id, declaration.variant),
        ).fetchall()
        if len(variant_rows) != 1:
            raise curation_error(
                "alias_windows_unknown_variant",
                f"alias_windows {declaration.fqid}/{declaration.variant} does not "
                "name exactly one variant of the variable's register.",
                "Use the target register_variant slug from the built catalog.",
            )
        variant_id = int(variant_rows[0][0])

        alias_rows = conn.execute(
            "SELECT register_variant_id, delivery_column_name FROM variable_alias "
            "WHERE variable_id = ? ORDER BY register_variant_id, delivery_column_name",
            (variable_id,),
        ).fetchall()
        folded_column = fold_column(declaration.column)
        owned_matches = [
            (int(row[0]), str(row[1]))
            for row in alias_rows
            if fold_column(str(row[1])) == folded_column
        ]
        if not owned_matches:
            raise curation_error(
                "alias_windows_unknown_column",
                f"alias_windows {declaration.fqid} does not own existing alias "
                f"{declaration.column!r}.",
                "Curated alias windows may only narrow an alias already present in "
                "variable_alias; correct the column or variable FQID.",
            )
        variant_matches = [
            column for rvid, column in owned_matches if rvid == variant_id
        ]
        if not variant_matches:
            variants = sorted({rvid for rvid, _column in owned_matches})
            raise curation_error(
                "alias_windows_cross_variant",
                f"alias_windows alias {declaration.column!r} belongs to "
                f"{declaration.fqid} on variant id(s) {variants}, not target "
                f"{variant_id} ({declaration.variant!r}).",
                "Name the variant that owns this alias; a declaration never "
                "copies an alias across variants.",
            )
        stored_columns = sorted(set(variant_matches))
        if len(stored_columns) != 1:
            raise curation_error(
                "alias_windows_ambiguous_column",
                f"alias_windows column {declaration.column!r} folds to several "
                f"aliases on {declaration.fqid}/{declaration.variant}: "
                f"{stored_columns}.",
                "Use an unambiguous existing delivery-column spelling.",
            )
        stored_column = stored_columns[0]

        same_header_owners = {
            int(row[0])
            for row in conn.execute(
                "SELECT variable_id, delivery_column_name FROM variable_alias "
                "WHERE register_variant_id = ?",
                (variant_id,),
            )
            if fold_column(str(row[1])) == folded_column
        }
        if same_header_owners != {variable_id}:
            raise curation_error(
                "alias_windows_ambiguous_column",
                f"alias_windows column {declaration.column!r} has ambiguous owners "
                f"{sorted(same_header_owners)} on variant {declaration.variant!r}.",
                "Resolve the delivery-column identity collision before curating "
                "an orderable window.",
            )

        if (
            conn.execute(
                "SELECT 1 FROM variable_state WHERE variable_id = ? "
                "AND register_variant_id = ? LIMIT 1",
                (variable_id, variant_id),
            ).fetchone()
            is None
        ):
            raise curation_error(
                "alias_windows_cross_variant",
                f"alias_windows {declaration.fqid} has no source state on "
                f"variant {declaration.variant!r}.",
                "Name a variant already delivering this variable; curation does "
                "not clone states across variants.",
            )

        existing_windows = [
            (str(row[0]), str(row[1]), str(row[2]))
            for row in conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to "
                "FROM variable_alias_window WHERE variable_id = ? "
                "AND register_variant_id = ?",
                (variable_id, variant_id),
            )
        ]
        base_provenance = _state_provenance(_CORRECTION_CLASS, declaration.evidence)

        for edition in declaration.source_editions:
            edition_rows = conn.execute(
                "SELECT regver_id FROM register_version "
                "WHERE register_variant_id = ? AND registerversionnamn = ? "
                "ORDER BY regver_id",
                (variant_id, edition),
            ).fetchall()
            if not edition_rows:
                raise curation_error(
                    "alias_windows_unsupported_edition",
                    f"alias_windows {declaration.fqid}/{declaration.variant}: "
                    f"source edition {edition!r} does not exist on the target variant.",
                    "Use an exact registerversionnamn from that variant; source "
                    "editions are never inferred from neighboring years.",
                )
            if len(edition_rows) > 1:
                raise curation_error(
                    "alias_windows_ambiguous_edition",
                    f"alias_windows {declaration.fqid}/{declaration.variant}: "
                    f"source edition {edition!r} resolves to {len(edition_rows)} "
                    "register_version rows.",
                    "Disambiguate the source editions before declaring an alias "
                    "window.",
                )
            source_columns = [
                str(row[0])
                for row in conn.execute(
                    "SELECT vab.delivery_column_name FROM variable_instance vi "
                    "JOIN variable_alias_build vab ON vab.cvid = vi.cvid "
                    "WHERE vi.regver_id = ? AND vi.register_variant_id = ? "
                    "AND vi.variable_id = ? ORDER BY vab.delivery_column_name",
                    (int(edition_rows[0][0]), variant_id, variable_id),
                )
            ]
            if not source_columns:
                raise curation_error(
                    "alias_windows_unsupported_edition",
                    f"alias_windows {declaration.fqid}/{declaration.variant} has "
                    f"no source instance in edition {edition!r}.",
                    "Choose an edition that already documents the target variable "
                    "and variant; alias-window curation does not add source states.",
                )
            if any(fold_column(column) == folded_column for column in source_columns):
                raise curation_error(
                    "alias_windows_source_covered",
                    f"alias_windows {declaration.column!r} is already documented "
                    f"on {declaration.fqid}/{declaration.variant} in source edition "
                    f"{edition!r}.",
                    "Retire this source edition from the declaration; it no longer "
                    "corrects an omission.",
                )
            claims = register_edition_claims(register_id, edition)
            if not claims:
                raise curation_error(
                    "alias_windows_unsupported_edition",
                    f"alias_windows source edition {edition!r} has no supported "
                    "period claim.",
                    "Use an edition name understood by register_edition_claims; "
                    "do not substitute a date or infer a neighboring year.",
                )

            for _year, claim_from, claim_to in claims:
                states = conn.execute(
                    "SELECT state_id, delivery_column_name "
                    "FROM variable_state WHERE variable_id = ? "
                    "AND register_variant_id = ? AND valid_from <= ? AND valid_to >= ? "
                    "ORDER BY state_id",
                    (variable_id, variant_id, claim_from, claim_to),
                ).fetchall()
                if not states:
                    raise curation_error(
                        "alias_windows_unsupported_edition",
                        f"alias_windows source edition {edition!r} claim "
                        f"{claim_from}..{claim_to} has no containing state on "
                        f"{declaration.fqid}/{declaration.variant}.",
                        "Choose a source edition the target variable and variant "
                        "already support; alias-window curation does not add states.",
                    )
                if len(states) > 1:
                    state_ids = [int(state[0]) for state in states]
                    raise curation_error(
                        "alias_windows_ambiguous_state",
                        f"alias_windows source edition {edition!r} claim "
                        f"{claim_from}..{claim_to} matches states {state_ids} on "
                        f"{declaration.fqid}/{declaration.variant}.",
                        "Narrow the declaration only after the source-state "
                        "ambiguity has been resolved.",
                    )
                state_id, state_column = states[0]
                if state_column is None:
                    raise curation_error(
                        "alias_windows_unsupported_edition",
                        f"alias_windows source state {state_id} for edition "
                        f"{edition!r} has no representative delivery column.",
                        "Curate the missing source representation before adding "
                        "an alias window.",
                    )
                state_column = str(state_column)
                if fold_column(state_column) == folded_column:
                    raise curation_error(
                        "alias_windows_source_covered",
                        f"alias_windows {declaration.column!r} is already the source "
                        f"representation for edition {edition!r} ({claim_from}.."
                        f"{claim_to}).",
                        "Retire this source edition from the declaration; it no "
                        "longer corrects an omission.",
                    )
                for window_column, window_from, window_to in existing_windows:
                    if fold_column(window_column) != folded_column:
                        continue
                    if window_from <= claim_from and window_to >= claim_to:
                        raise curation_error(
                            "alias_windows_source_covered",
                            f"alias_windows {declaration.column!r} already has a "
                            f"window covering edition {edition!r} "
                            f"({claim_from}..{claim_to}).",
                            "Retire this source edition from the declaration; its "
                            "orderable representation is already covered.",
                        )
                    if window_from <= claim_to and window_to >= claim_from:
                        raise curation_error(
                            "alias_windows_ambiguous_window",
                            f"alias_windows {declaration.column!r} has existing "
                            f"window {window_from}..{window_to} overlapping edition "
                            f"{edition!r} claim {claim_from}..{claim_to}.",
                            "Resolve the overlapping representation windows; the "
                            "curation pass never widens or coalesces them.",
                        )

                target = (
                    variable_id,
                    variant_id,
                    stored_column,
                    claim_from,
                    claim_to,
                )
                targets.setdefault(target, []).append((base_provenance, edition))

    before = conn.total_changes
    conn.executemany(
        "INSERT INTO variable_alias_window "
        "(variable_id, register_variant_id, delivery_column_name, valid_from, "
        "valid_to, provenance) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (*target, scoped_state_provenance(attributions))
            for target, attributions in sorted(targets.items())
        ],
    )
    inserted = conn.total_changes - before
    if progress is not None:
        progress(
            f"  {inserted:,} alias representation windows from "
            f"{len(active):,} curated existing-alias declaration(s)"
        )
    return {"entries": len(active), "windows": inserted}
