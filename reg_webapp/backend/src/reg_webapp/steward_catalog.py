"""Startup-only catalog adapter for steward catalog validation.

The steward boot path asks the catalog for semantic-validation facts, not for
the full browse-detail payload. Keep the public ``Catalog`` behaviour in the
researcher/API paths, but avoid hydrating full resolved variables for thousands
of steward bindings before Uvicorn can accept traffic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import Fqid, FqidKind, parse

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog, VariableRef, VariableState


@dataclass(frozen=True)
class _SemanticVariable:
    """The subset of ``ResolvedVariable`` consumed by semantic validation."""

    deprecated: bool
    replaced_by: tuple[VariableRef, ...]


def _period_key(period: Any) -> Any:
    """Hashable cache key for the ``Catalog.resolve_at`` period argument."""
    if isinstance(period, dict):
        return tuple(sorted(period.items()))
    return period


class StewardBootCatalog:
    """Catalog-shaped adapter used only while loading a steward catalog.

    It preserves the three methods the semantic/index loaders call:
    ``list_variants``, ``resolve``, and ``resolve_at``. Other attributes delegate
    to the wrapped ``Catalog`` for defensive compatibility.
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._variants_cache: dict[tuple[str, str], tuple[Any, ...]] = {}
        self._resolve_cache: dict[str, Any] = {}
        self._resolve_at_cache: dict[
            tuple[str, Any, str | None, str | None], tuple[VariableState, ...]
        ] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._catalog, name)

    def list_variants(self, provider_slug: str, register_slug: str) -> list[Any]:
        key = (provider_slug, register_slug)
        if key not in self._variants_cache:
            self._variants_cache[key] = tuple(
                self._catalog.list_variants(provider_slug, register_slug)
            )
        return list(self._variants_cache[key])

    def resolve(self, fqid: str | Fqid) -> Any:
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        key = str(parsed)
        if key in self._resolve_cache:
            return self._resolve_cache[key]
        if parsed.kind is not FqidKind.VARIABLE_BINDING:
            resolved = self._catalog.resolve(parsed)
            self._resolve_cache[key] = resolved
            return resolved

        resolved_identity = self._catalog._resolve_variable_identity(parsed)  # noqa: SLF001
        if resolved_identity is None:
            # Delegate the miss so the caller gets reg_meta's normal structured
            # RegMetaError rather than a webapp-local approximation.
            return self._catalog.resolve(parsed)
        var, _via_same_as = resolved_identity
        meta = self._catalog._lookup_variable_meta(var["variable_id"])  # noqa: SLF001
        semantic = _SemanticVariable(
            deprecated=bool(meta["deprecated"]),
            replaced_by=self._catalog._successor_edges(  # noqa: SLF001
                meta["provider_slug"], meta["register_slug"], meta["slug"]
            ),
        )
        self._resolve_cache[key] = semantic
        return semantic

    def resolve_at(
        self,
        fqid: str | Fqid,
        period: Any,
        *,
        variant: str | None = None,
        value_set_version: str | None = None,
    ) -> list[VariableState]:
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        key = (str(parsed), _period_key(period), variant, value_set_version)
        if key not in self._resolve_at_cache:
            self._resolve_at_cache[key] = tuple(
                self._catalog.resolve_at(
                    parsed,
                    period,
                    variant=variant,
                    value_set_version=value_set_version,
                )
            )
        return list(self._resolve_at_cache[key])
