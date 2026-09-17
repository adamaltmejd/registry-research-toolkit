"""Parse accepted delivery-list descriptions and aliases.

These declarations retain their source provenance for conversion into checked
common-layer decisions. They do not run a separate database enrichment pass.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._curation import curation_error, load_curation_entries, require_str

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class DescriptionBackfill:
    """An accepted description backfill and its delivery-list provenance."""

    provider: str
    register: str
    variable: str
    description: str
    provenance: str


@dataclass(frozen=True)
class CuratedAlias:
    """An accepted alias declaration; it does not establish annual availability."""

    provider: str
    register: str
    variable: str
    delivery_column: str
    provenance: str


@dataclass(frozen=True)
class DeliveryEnrichment:
    """Parsed description and alias declarations."""

    descriptions: tuple[DescriptionBackfill, ...]
    aliases: tuple[CuratedAlias, ...] = ()


_require_str = functools.partial(
    require_str,
    code="delivery_enrichment_invalid",
    prefix="delivery_enrichment",
    file_name="curation/delivery_enrichment.generated.toml",
)


def _parse_register_variable(entry: dict, kind: str) -> tuple[str, str, str]:
    """Validate + split the shared ``register`` (2-segment ``provider/register``
    FQID) and ``variable`` (single register-local slug) fields. Returns
    ``(provider, register, variable)``. A multi-segment ``variable`` (``foo/bar``)
    is a maintainer typo and fails the strict load rather than surfacing later
    as an unresolved target."""
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
    ``(register, variable)`` appears at most once. The common curation stage
    checks source applicability and catalog references before materialization."""
    entries = load_curation_entries(
        path,
        entry_key="description",
        label="delivery-enrichment",
        prefix="delivery_enrichment",
        code_base="delivery_enrichment",
        file_name="curation/delivery_enrichment.generated.toml",
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
                "reg_meta_build/curation/delivery_enrichment.generated.toml.",
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
        file_name="curation/delivery_enrichment.generated.toml",
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
