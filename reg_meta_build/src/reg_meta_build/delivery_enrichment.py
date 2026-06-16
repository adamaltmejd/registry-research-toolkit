"""Delivery-list enrichment overlay (#365 PR1).

Gap-fill catalog facts extracted from steward delivery / variable lists that
describe the **shared** SCB/SOS world — not steward-private content — curated
into a committed TOML and applied to the normal *global* build. Two entry kinds
ship today, both in ``delivery_enrichment.toml``:

* ``[[description]]`` (PR1a) — fill an EMPTY ``variable.description`` from the
  delivery list's prose.
* ``[[alias]]`` (PR1c) — record an additional ``variable_alias`` delivery column
  for an existing variable (the name SWECOV delivers it under differs from the
  SCB metadata header). The alias becomes part of the variable's delivery-column
  history (``get_datacolumns`` / ``resolve`` read ``variable_alias``; the MONA
  bundle matches a delivered column → variable through it). Adding *extra* alias
  rows is safe: the validator invariant is one-directional (every state column
  must be in ``variable_alias``; the reverse is not required).

(Gap-fill variable grafts remain deferred — the candidate set needs type
curation; see #365.)

Scope follows what a fact is *about*, not where it was learned (revised
2026-06-12, #365): a description of an AGI column is an AGI fact regardless of
which project's delivery list surfaced it. Two guards keep that honest:

* **Gap-fill only.** A backfill NEVER overwrites a non-empty description, so an
  official SCB/SOS export always outranks the delivery list. Enforced in the
  ``UPDATE``'s ``WHERE`` clause, so it is idempotent across rebuilds and needs no
  separate "is it already set?" read.
* **Human-confirmed curation.** The committed TOML is a reviewed extract (no
  fuzzy auto-grounding at build time). A *structural* defect — a duplicate
  ``(register, variable)`` key, a malformed FQID — fails the build (EXIT_CONFIG),
  like the other curation surfaces.

**Deliberate deviation from `concept_groups` strictness:** a backfill whose
``(register, variable)`` no longer *resolves* against the built DB is skipped
with a warning and counted, NOT a build failure. Rationale: pre-v1 variable
slugs regenerate each build while their provider zone is `churning` (#470), and
a description backfill is non-structural — making the global build fragile to one
stale enrichment row would trade a real robustness loss for a cosmetic gain. The
unresolved count surfaces in the build summary so drift stays visible; regenerate
the TOML from the delivery lists when it grows.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ._curation import curation_error, load_curation_entries

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable


@dataclass(frozen=True)
class DescriptionBackfill:
    """One ``[[description]]`` entry: fill ``variable.description`` for the
    variable at ``provider/register`` slug ``variable`` with ``description``.
    ``provenance`` records the source delivery-list file for audit only — it is
    not stored in the DB (a backfilled description is an SCB fact, not a steward
    one; provenance lives in the curation input)."""

    provider: str
    register: str
    variable: str
    description: str
    provenance: str


@dataclass(frozen=True)
class CuratedAlias:
    """One ``[[alias]]`` entry: record ``delivery_column`` as an additional
    ``variable_alias`` delivery column for the existing variable at
    ``provider/register`` slug ``variable``. Attached to every register_variant
    in which that variable has a state, so the alias surfaces consistently with
    the variable's delivered data. ``provenance`` is audit-only (not stored)."""

    provider: str
    register: str
    variable: str
    delivery_column: str
    provenance: str


@dataclass(frozen=True)
class DeliveryEnrichment:
    """The parsed delivery-enrichment overlay: description backfills (PR1a) and
    delivery-column aliases (PR1c)."""

    descriptions: tuple[DescriptionBackfill, ...]
    aliases: tuple[CuratedAlias, ...] = ()


def repo_delivery_enrichment_path() -> Path | None:
    """``reg_meta_build/delivery_enrichment.toml`` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs and ``concept_groups.toml``). Sits at the package root, NOT under
    ``fqid_slugs/`` (that dir is glob-loaded as provider-slug TOMLs)."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent / "delivery_enrichment.toml"
    )
    return candidate if candidate.is_file() else None


def _require_str(entry: dict, field: str, context: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise curation_error(
            "delivery_enrichment_invalid",
            f"delivery_enrichment {context} needs `{field}` as a non-empty "
            f"string, got {value!r}.",
            f'Give `{field} = "<value>"` in reg_meta_build/delivery_enrichment.toml.',
        )
    return value


def _parse_register_variable(entry: dict, kind: str) -> tuple[str, str, str]:
    """Validate + split the shared ``register`` (2-segment ``provider/register``
    FQID) and ``variable`` (single register-local slug) fields. Returns
    ``(provider, register, variable)``. A multi-segment ``variable`` (``foo/bar``)
    is a maintainer typo and fails the strict load rather than being silently
    counted unresolved later."""
    register_fqid = _require_str(entry, "register", kind)
    parts = register_fqid.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            "delivery_enrichment_invalid",
            f"delivery_enrichment register {register_fqid!r} must be a "
            "2-segment `provider/register` FQID.",
            'Give `register = "scb/agi"`-style 2-segment FQIDs.',
        )
    variable = _require_str(entry, "variable", kind)
    if "/" in variable:
        raise curation_error(
            "delivery_enrichment_invalid",
            f"delivery_enrichment {kind} {register_fqid} variable {variable!r} "
            "must be a single slug segment, not a path.",
            'Give just the variable slug (`variable = "avdr-prel-skatt"`), '
            "not a `provider/register/variable` FQID.",
        )
    return parts[0], parts[1], variable


def load_delivery_enrichment(path: Path | None) -> DeliveryEnrichment:
    """Parse the delivery-enrichment TOML. Empty when no file (synthetic test
    builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only ``[[description]]``
    top-level; ``register`` is a 2-segment ``provider/register`` FQID; ``variable``
    / ``description`` non-empty strings; ``provenance`` optional; each
    ``(register, variable)`` appears at most once. Reference RESOLUTION (does the
    variable exist?) happens at materialize time against the built DB."""
    entries = load_curation_entries(
        path,
        entry_key="description",
        label="delivery-enrichment",
        prefix="delivery_enrichment",
        code_base="delivery_enrichment",
        file_name="delivery_enrichment.toml",
        entry_fields="register / variable / description",
        sibling_keys=frozenset({"alias"}),
    )
    out: list[DescriptionBackfill] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        provider, register, variable = _parse_register_variable(
            entry, "[[description]]"
        )
        description = _require_str(entry, "description", "[[description]]")
        provenance = _opt_provenance(
            entry, f"[[description]] {provider}/{register}/{variable}"
        )
        scope_key = (provider, register, variable)
        if scope_key in seen:
            raise curation_error(
                "delivery_enrichment_invalid",
                f"delivery_enrichment duplicate description for "
                f"{provider}/{register}/{variable}.",
                "Each (register, variable) may have at most one [[description]] "
                "— resolve the conflicting rows in "
                "reg_meta_build/delivery_enrichment.toml.",
            )
        seen.add(scope_key)
        out.append(
            DescriptionBackfill(
                provider=provider,
                register=register,
                variable=variable,
                description=description,
                provenance=provenance,
            )
        )
    return DeliveryEnrichment(descriptions=tuple(out), aliases=_load_aliases(path))


def _opt_provenance(entry: dict, context: str) -> str:
    provenance = entry.get("provenance", "")
    if not isinstance(provenance, str):
        raise curation_error(
            "delivery_enrichment_invalid",
            f"delivery_enrichment {context} `provenance` must be a string, "
            f"got {provenance!r}.",
            "Give `provenance` as a string or omit it.",
        )
    return provenance


def _load_aliases(path: Path | None) -> tuple[CuratedAlias, ...]:
    """Parse the ``[[alias]]`` entries (sibling to ``[[description]]`` in the same
    file). Each ``(register, variable, delivery_column)`` triple is unique."""
    entries = load_curation_entries(
        path,
        entry_key="alias",
        label="delivery-enrichment",
        prefix="delivery_enrichment",
        code_base="delivery_enrichment",
        file_name="delivery_enrichment.toml",
        entry_fields="register / variable / delivery_column",
        sibling_keys=frozenset({"description"}),
    )
    out: list[CuratedAlias] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        provider, register, variable = _parse_register_variable(entry, "[[alias]]")
        delivery_column = _require_str(entry, "delivery_column", "[[alias]]")
        provenance = _opt_provenance(
            entry, f"[[alias]] {provider}/{register}/{variable}"
        )
        key = (provider, register, variable + "\x00" + delivery_column.lower())
        if key in seen:
            raise curation_error(
                "delivery_enrichment_invalid",
                f"delivery_enrichment duplicate alias {delivery_column!r} for "
                f"{provider}/{register}/{variable}.",
                "Each (register, variable, delivery_column) may appear once.",
            )
        seen.add(key)
        out.append(
            CuratedAlias(
                provider=provider,
                register=register,
                variable=variable,
                delivery_column=delivery_column,
                provenance=provenance,
            )
        )
    return tuple(out)


def _apply_description_backfills(
    conn: sqlite3.Connection,
    backfills: tuple[DescriptionBackfill, ...],
    warn: Callable[[str], None],
) -> dict[str, int]:
    """Fill empty ``variable.description`` from the curated backfills. Resolution
    is lenient (see the module docstring): an entry whose register or variable
    does not resolve is counted ``unresolved`` and skipped, never a build
    failure. ``applied`` = rows whose empty description we filled; ``skipped`` =
    target already had a non-empty description (gap-fill only)."""
    counts = {"applied": 0, "skipped": 0, "unresolved": 0}
    for bf in backfills:
        row = conn.execute(
            "SELECT v.variable_id, v.description FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
            (bf.provider, bf.register, bf.variable),
        ).fetchone()
        if row is None:
            counts["unresolved"] += 1
            continue
        variable_id, existing = row
        if existing is not None and existing.strip():
            counts["skipped"] += 1
            continue
        # Gap-fill only: the WHERE guard re-checks emptiness so the pass stays
        # idempotent even if two entries somehow target one variable.
        cur = conn.execute(
            "UPDATE variable SET description = ? "
            "WHERE variable_id = ? AND (description IS NULL OR TRIM(description) = '')",
            (bf.description, variable_id),
        )
        counts["applied"] += cur.rowcount
    if counts["unresolved"]:
        warn(
            f"  WARN delivery-enrichment: {counts['unresolved']:,} description "
            "backfill(s) did not resolve (slug churn) — regenerate "
            "delivery_enrichment.toml from the delivery lists"
        )
    return counts


def _apply_aliases(
    conn: sqlite3.Connection,
    aliases: tuple[CuratedAlias, ...],
    warn: Callable[[str], None],
) -> dict[str, int]:
    """Insert curated ``variable_alias`` rows. Each alias is attached to every
    register_variant in which the target variable has a ``variable_state`` (so
    the alias surfaces consistently with the delivered data; a variable with no
    state — e.g. provider absent from this build — is counted ``unresolved``).
    Lenient like the backfill pass: a slug that doesn't resolve is counted, not
    fatal. ``INSERT OR IGNORE`` makes it idempotent and skips a column the
    variant already carries (``skipped``)."""
    counts = {"applied": 0, "skipped": 0, "unresolved": 0}
    for al in aliases:
        variant_rows = conn.execute(
            "SELECT DISTINCT vs.register_variant_id FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
            (al.provider, al.register, al.variable),
        ).fetchall()
        variable_row = conn.execute(
            "SELECT v.variable_id FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
            (al.provider, al.register, al.variable),
        ).fetchone()
        if variable_row is None or not variant_rows:
            counts["unresolved"] += 1
            continue
        variable_id = variable_row[0]
        for (register_variant_id,) in variant_rows:
            cur = conn.execute(
                "INSERT OR IGNORE INTO variable_alias "
                "(variable_id, register_variant_id, delivery_column_name) "
                "VALUES (?, ?, ?)",
                (variable_id, register_variant_id, al.delivery_column),
            )
            if cur.rowcount:
                counts["applied"] += 1
            else:
                counts["skipped"] += 1
    if counts["unresolved"]:
        warn(
            f"  WARN delivery-enrichment: {counts['unresolved']:,} alias(es) did "
            "not resolve (slug churn / no state) — regenerate "
            "delivery_enrichment.toml from the delivery lists"
        )
    return counts


def apply_delivery_enrichment(
    conn: sqlite3.Connection,
    enrichment: DeliveryEnrichment,
    *,
    providers: frozenset[str],
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Apply the delivery-enrichment overlay to the built DB. ``providers`` gates
    entries to the providers in this build (a ``--providers=sos`` build skips scb
    rows rather than counting them all unresolved), mirroring
    ``materialize_concept_groups``. Runs after ``populate_variable_slugs`` so
    ``(register, variable)`` resolves off stored slugs. Returns merged counts with
    ``alias_*`` keys for the alias pass."""
    warn = warn or (lambda _msg: None)
    descriptions = tuple(
        bf for bf in enrichment.descriptions if bf.provider in providers
    )
    aliases = tuple(al for al in enrichment.aliases if al.provider in providers)
    counts = _apply_description_backfills(conn, descriptions, warn)
    alias_counts = _apply_aliases(conn, aliases, warn)
    counts.update({f"alias_{k}": v for k, v in alias_counts.items()})
    return counts
