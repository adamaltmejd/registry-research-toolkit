"""Variable grafts (#365 PR1d).

Mint catalog `variable`s that reg_meta's machine metadata lacks but a steward
delivery documents, onto an EXISTING `(register, variant)`. Unlike the
description/alias overlays (`delivery_enrichment.py`), which only touch existing
rows, a graft CREATES identity: one `variable` + one `variable_state` + one
`variable_alias` + a slug.

Scope follows what a fact is *about*: a graft is a real column of a canonical
SCB/SOS register (it goes to the global build), surfaced because the steward
holds it. Candidates are curated upstream by the (untracked) generator —
flavor/SWECOV-constructed columns, pseudonymized aggregations, and columns
documented in reg_meta's own SCB docs (a metadata-completeness gap → #400, not a
graft) are all excluded there.

Guards / discipline:
* **Gap-fill only.** A graft whose delivery column ALREADY exists as a
  `variable_state` column in that `(register, variant)` is skipped — we never
  duplicate an existing variable.
* **Strict load, lenient resolve.** A structural TOML defect fails the build
  (EXIT_CONFIG); a `(register, variant)` that doesn't resolve is counted
  `unresolved`, not fatal (pre-v1 slug churn, mirrors the other overlays).
* **Banded ids.** SCB ids must be < 2^62 and SOS ids in [2^62, 2^63) (the
  minted-id-band invariant). `variable_id`/`state_id` are AUTOINCREMENT, and SOS
  rows hold high ids, so a graft must mint EXPLICIT ids in its provider's band —
  here, just above the SCB maximum. (SOS grafts are out of scope for now.)
* **No `SCHEMA_VERSION` bump** — rows on existing tables. Runs BEFORE
  `populate_variable_slugs`, so each graft auto-slugs from its delivery column.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ._curation import curation_error, fold_column, load_curation_entries

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

# SCB ids live below this bit, SOS at/above it (see validate._check_minted_id_bands).
_MINT_BIT = 1 << 62
_GRAFT_SOURCE_LABEL = "swecov-graft"


@dataclass(frozen=True)
class CuratedGraft:
    """One `[[graft]]`: mint a variable delivered as `column` under
    `provider/register` variant `variant`. `description` is the variable's name +
    description; `data_type` is the SQL type if the steward list carried one
    (else empty → NULL state type)."""

    provider: str
    register: str
    variant: str
    column: str
    description: str
    data_type: str


def repo_variable_grafts_path() -> Path | None:
    """`reg_meta_build/variable_grafts.toml` from a repo checkout, or None (wheel
    installs don't ship curation). Package-root sibling, like the other curation
    TOMLs."""
    candidate = Path(__file__).resolve().parent.parent.parent / "variable_grafts.toml"
    return candidate if candidate.is_file() else None


def _require_str(entry: dict, field: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise curation_error(
            "variable_grafts_invalid",
            f"variable_grafts [[graft]] needs `{field}` as a non-empty string, "
            f"got {value!r}.",
            f'Give `{field} = "<value>"` in reg_meta_build/variable_grafts.toml.',
        )
    return value.strip()


def load_variable_grafts(path: Path | None) -> tuple[CuratedGraft, ...]:
    """Parse the graft TOML. Empty when no file (synthetic builds, wheel
    installs). Load-time validation (EXIT_CONFIG): only `[[graft]]`; `register`
    is a 2-segment `provider/register` FQID; `variant`/`column`/`description`
    non-empty; `data_type` optional; each `(register, variant, column)` unique."""
    entries = load_curation_entries(
        path,
        entry_key="graft",
        label="variable-graft",
        prefix="variable_grafts",
        code_base="variable_grafts",
        file_name="variable_grafts.toml",
        entry_fields="register / variant / column / description",
    )
    out: list[CuratedGraft] = []
    seen: set[tuple[str, str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "variable_grafts_invalid",
                f"variable_grafts register {register_fqid!r} must be a 2-segment "
                "`provider/register` FQID.",
                'Give `register = "scb/agi"`-style 2-segment FQIDs.',
            )
        variant = _require_str(entry, "variant")
        column = _require_str(entry, "column")
        description = _require_str(entry, "description")
        data_type = entry.get("data_type", "")
        if not isinstance(data_type, str):
            raise curation_error(
                "variable_grafts_invalid",
                f"variable_grafts [[graft]] {register_fqid}/{variant}/{column} "
                f"`data_type` must be a string, got {data_type!r}.",
                "Give `data_type` as a string or omit it.",
            )
        key = (parts[0], parts[1], variant, fold_column(column))
        if key in seen:
            raise curation_error(
                "variable_grafts_invalid",
                f"variable_grafts duplicate graft {register_fqid}/{variant}/{column}.",
                "Each (register, variant, column) may appear once.",
            )
        seen.add(key)
        out.append(
            CuratedGraft(
                provider=parts[0],
                register=parts[1],
                variant=variant,
                column=column,
                description=description,
                data_type=data_type.strip(),
            )
        )
    return tuple(out)


def materialize_grafts(
    conn: sqlite3.Connection,
    grafts: tuple[CuratedGraft, ...],
    *,
    providers: frozenset[str],
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Mint variable + state + alias for each graft, onto its existing
    `(register, variant)`. Runs AFTER `populate_slugs` (register/variant slugs
    resolve) and BEFORE `populate_variable_slugs` (the minted variable auto-slugs
    from its delivery column). `providers` gates entries to this build.

    Returns `{minted, skipped, unresolved}`. `skipped` = the column already
    exists as a state in that variant (never duplicate); `unresolved` = the
    register or variant slug didn't resolve."""
    warn = warn or (lambda _msg: None)
    counts = {"minted": 0, "skipped": 0, "unresolved": 0}
    active = [g for g in grafts if g.provider in providers]
    if not active:
        return counts

    # Mint explicit ids just above the SCB max so they stay in the SCB band
    # (< 2^62) regardless of any high SOS-minted ids that AUTOINCREMENT would
    # otherwise jump past. Only SCB grafts are in scope (see module docstring).
    next_var = (
        conn.execute(
            "SELECT COALESCE(MAX(variable_id), 0) FROM variable WHERE variable_id < ?",
            (_MINT_BIT,),
        ).fetchone()[0]
        + 1
    )
    next_state = (
        conn.execute(
            "SELECT COALESCE(MAX(state_id), 0) FROM variable_state WHERE state_id < ?",
            (_MINT_BIT,),
        ).fetchone()[0]
        + 1
    )

    for g in active:
        variant_row = conn.execute(
            "SELECT rv.register_variant_id, r.register_id FROM register_variant rv "
            "JOIN register r ON r.register_id = rv.register_id "
            "JOIN provider p ON p.provider_id = r.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
            (g.provider, g.register, g.variant),
        ).fetchone()
        if variant_row is None:
            counts["unresolved"] += 1
            continue
        register_variant_id, register_id = variant_row
        # Gap-fill only: skip if the column is already a delivered state column in
        # this variant (case/diacritic-folded).
        exists = conn.execute(
            "SELECT 1 FROM variable_state vs JOIN variable v "
            "ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = ? AND vs.register_variant_id = ? "
            "AND LOWER(vs.delivery_column_name) = LOWER(?) LIMIT 1",
            (register_id, register_variant_id, g.column),
        ).fetchone()
        if exists:
            counts["skipped"] += 1
            continue
        if next_var >= _MINT_BIT:  # pragma: no cover - guards a pathological corpus
            raise curation_error(
                "variable_grafts_invalid",
                "graft variable ids would overflow the SCB minted band (2^62).",
                "Too many SCB variables to graft — investigate.",
            )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, "
            "description, source_label) VALUES (?, ?, ?, ?, ?, ?)",
            (
                next_var,
                register_id,
                f"graft:{g.column}",
                g.description,
                g.description,
                _GRAFT_SOURCE_LABEL,
            ),
        )
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, ?, ?, '0001-01-01', '9999-12-31', ?, ?)",
            (next_state, next_var, register_variant_id, g.data_type or None, g.column),
        )
        conn.execute(
            "INSERT INTO variable_alias (variable_id, register_variant_id, "
            "delivery_column_name) VALUES (?, ?, ?)",
            (next_var, register_variant_id, g.column),
        )
        next_var += 1
        next_state += 1
        counts["minted"] += 1

    if counts["unresolved"]:
        warn(
            f"  WARN variable-grafts: {counts['unresolved']:,} graft(s) did not "
            "resolve (register/variant slug churn) — regenerate variable_grafts.toml"
        )
    return counts
