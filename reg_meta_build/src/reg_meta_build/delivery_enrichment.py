"""Parse accepted delivery-list descriptions and aliases.

These declarations retain their source provenance for conversion into checked
common-layer decisions. They do not run a separate database enrichment pass.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._curation import curation_error, require_str
from .curation_tree import load_register_files

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
    file_name="curation/registers/<provider>/<slug>.toml",
)


def _register_local_variable(variable: str, context: str) -> str:
    """Validate that an enrichment variable is one local slug segment."""
    variable = _require_str({"variable": variable}, "variable", context)
    if "/" in variable:
        raise curation_error(
            "delivery_enrichment_invalid",
            f"delivery_enrichment {context} variable {variable!r} must be a "
            "single slug segment, not a path.",
            'Give just the variable slug (`variable = "avdr-prel-skatt"`), '
            "not a `provider/register/variable` FQID.",
        )
    return variable


def load_delivery_enrichment(path: Path | None) -> DeliveryEnrichment:
    """Parse delivery descriptions and aliases from register files. ``path`` is
    the curation root. Empty when no tree (synthetic test builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only ``[[description]]``
    top-level; ``register`` is a 2-segment ``provider/register`` FQID; ``variable``
    / ``description`` non-empty strings; ``provenance`` optional; each
    ``(register, variable)`` appears at most once. The common curation stage
    checks source applicability and catalog references before materialization."""
    out: list[DescriptionBackfill] = []
    seen: set[tuple[str, str, str]] = set()
    for register_file in load_register_files(path) if path is not None else ():
        provider = register_file.register_info.provider
        register = register_file.register_info.slug
        for index, row in enumerate(register_file.enrichment.description, start=1):
            context = (
                f"{register_file.source_file} [[enrichment.description]] entry {index}"
            )
            variable = _register_local_variable(row.variable, context)
            description = _require_str(
                {"description": row.description}, "description", context
            )
            provenance = _opt_provenance({"provenance": row.provenance}, context)
            scope_key = (provider, register, variable)
            if scope_key in seen:
                raise curation_error(
                    "delivery_enrichment_invalid",
                    f"{context}: duplicate description for "
                    f"{provider}/{register}/{variable}.",
                    "Each (register, variable) may have at most one [[description]] "
                    "— keep one declaration in the register file.",
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
    """Flatten register-file enrichment aliases in sorted register order."""
    out: list[CuratedAlias] = []
    seen: set[tuple[str, str, str]] = set()
    for register_file in load_register_files(path) if path is not None else ():
        provider = register_file.register_info.provider
        register = register_file.register_info.slug
        for index, row in enumerate(register_file.enrichment.alias, start=1):
            context = f"{register_file.source_file} [[enrichment.alias]] entry {index}"
            variable = _register_local_variable(row.variable, context)
            delivery_column = _require_str(
                {"delivery_column": row.delivery_column}, "delivery_column", context
            )
            provenance = _opt_provenance({"provenance": row.provenance}, context)
            key = (provider, register, variable + "\x00" + delivery_column.lower())
            if key in seen:
                raise curation_error(
                    "delivery_enrichment_invalid",
                    f"{context}: duplicate alias {delivery_column!r} for "
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
