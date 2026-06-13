"""Delivery-list enrichment overlay (#365 PR1).

Gap-fill catalog facts extracted from steward delivery / variable lists that
describe the **shared** SCB/SOS world — not steward-private content — curated
into a committed TOML and applied to the normal *global* build. PR1a ships the
**description-backfill** kind only: filling an EMPTY ``variable.description``
from the delivery list's prose. (Delivery-column aliases and gap-fill variable
grafts are the next slice, #365 PR1b.)

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
slugs regenerate each build while the `UNFROZEN` sentinel is present (#209), and
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
class DeliveryEnrichment:
    """The parsed delivery-enrichment overlay. PR1a carries only description
    backfills; alias and graft tuples join here in PR1b."""

    descriptions: tuple[DescriptionBackfill, ...]


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
    )
    out: list[DescriptionBackfill] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register", "[[description]]")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "delivery_enrichment_invalid",
                f"delivery_enrichment register {register_fqid!r} must be a "
                "2-segment `provider/register` FQID.",
                'Give `register = "scb/agi"`-style 2-segment FQIDs.',
            )
        variable = _require_str(entry, "variable", "[[description]]")
        if "/" in variable:
            # The variable is a single register-local slug, not an FQID path. A
            # multi-segment value (`foo/bar`) is a maintainer typo that would
            # otherwise be silently counted `unresolved` (lenient resolve) — but
            # a malformed reference must fail the strict structural load path.
            raise curation_error(
                "delivery_enrichment_invalid",
                f"delivery_enrichment [[description]] {register_fqid} variable "
                f"{variable!r} must be a single slug segment, not a path.",
                'Give just the variable slug (`variable = "avdr-prel-skatt"`), '
                "not a `provider/register/variable` FQID.",
            )
        description = _require_str(entry, "description", "[[description]]")
        provenance = entry.get("provenance", "")
        if not isinstance(provenance, str):
            raise curation_error(
                "delivery_enrichment_invalid",
                f"delivery_enrichment [[description]] {register_fqid}/{variable} "
                f"`provenance` must be a string, got {provenance!r}.",
                "Give `provenance` as a string or omit it.",
            )
        scope_key = (parts[0], parts[1], variable)
        if scope_key in seen:
            raise curation_error(
                "delivery_enrichment_invalid",
                f"delivery_enrichment duplicate description for "
                f"{register_fqid}/{variable}.",
                "Each (register, variable) may have at most one [[description]] "
                "— resolve the conflicting rows in "
                "reg_meta_build/delivery_enrichment.toml.",
            )
        seen.add(scope_key)
        out.append(
            DescriptionBackfill(
                provider=parts[0],
                register=parts[1],
                variable=variable,
                description=description,
                provenance=provenance,
            )
        )
    return DeliveryEnrichment(descriptions=tuple(out))


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


def apply_delivery_enrichment(
    conn: sqlite3.Connection,
    enrichment: DeliveryEnrichment,
    *,
    providers: frozenset[str],
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Apply the delivery-enrichment overlay to the built DB. ``providers`` gates
    entries to the providers in this build (a ``--providers=sos`` build skips scb
    backfills rather than counting them all unresolved), mirroring
    ``materialize_concept_groups``. Runs after ``populate_variable_slugs`` so
    ``(register, variable)`` resolves off stored slugs."""
    warn = warn or (lambda _msg: None)
    active = tuple(bf for bf in enrichment.descriptions if bf.provider in providers)
    return _apply_description_backfills(conn, active, warn)
