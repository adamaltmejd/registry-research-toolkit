"""Startup-only catalog adapter for steward catalog validation.

The steward boot path asks the catalog for semantic-validation facts, not for
the full browse-detail payload. Keep the public ``Catalog`` behaviour in the
researcher/API paths, but avoid hydrating full resolved variables and full
``VariableState`` response models for thousands of steward bindings before
Uvicorn can accept traffic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from reg_meta.catalog import _period_bounds
from reg_meta.fqid import Fqid, FqidError, FqidKind, parse

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Sequence

    from reg_meta.catalog import Catalog
    from reg_schema.project_data import ProjectData

_SQLITE_PARAM_LIMIT = 900


@dataclass(frozen=True)
class _SemanticVariable:
    """The subset of ``ResolvedVariable`` consumed by semantic validation."""

    deprecated: bool
    replaced_by: tuple[_SemanticVariableRef, ...]


@dataclass(frozen=True)
class _SemanticVariableRef:
    """The subset of ``VariableRef`` consumed by semantic validation."""

    fqid: Fqid | None
    provider: str
    register_name: str
    variable: str
    effective_year: int | None


@dataclass(frozen=True)
class _VariableIdentity:
    """Resolved variable identity + metadata needed during steward boot."""

    variable_id: int
    register_id: int
    provider_slug: str
    register_slug: str
    variable_slug: str
    deprecated: bool


@dataclass(frozen=True)
class _SemanticVariableState:
    """The subset of ``VariableState`` consumed by semantic/index loading."""

    state_id: int
    register_variant_id: int
    delivery_column_name: str | None
    valid_from: str
    valid_to: str
    value_set_id: int | None
    value_set_version_label: str


@dataclass(frozen=True)
class _AliasWindow:
    register_variant_id: int
    delivery_column_name: str
    valid_from: str
    valid_to: str


def _period_key(period: Any) -> Any:
    """Hashable cache key for the ``Catalog.resolve_at`` period argument."""
    if isinstance(period, dict):
        return tuple(sorted(period.items()))
    return period


def _chunks[T](items: Iterable[T], size: int) -> Iterable[tuple[T, ...]]:
    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield tuple(chunk)
            chunk = []
    if chunk:
        yield tuple(chunk)


def _qmarks(count: int) -> str:
    return ",".join("?" for _ in range(count))


def _tuple_placeholders(width: int, count: int) -> str:
    row = "(" + ",".join("?" for _ in range(width)) + ")"
    return ",".join(row for _ in range(count))


class StewardBootCatalog:
    """Catalog-shaped adapter used only while loading a steward catalog.

    It preserves the three methods the semantic/index loaders call:
    ``list_variants``, ``resolve``, and ``resolve_at``. Other attributes delegate
    to the wrapped ``Catalog`` for defensive compatibility.
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._variants_cache: dict[tuple[str, str], tuple[Any, ...]] = {}
        self._identity_cache: dict[str, _VariableIdentity | None] = {}
        self._resolve_cache: dict[str, Any] = {}
        self._variant_id_cache: dict[tuple[int, str], int | None] = {}
        self._successor_cache: dict[
            tuple[str, str, str], tuple[_SemanticVariableRef, ...]
        ] = {}
        self._state_rows_cache: dict[
            tuple[int, int], tuple[_SemanticVariableState, ...]
        ] = {}
        self._windows_cache: dict[int, dict[int, tuple[_AliasWindow, ...]]] = {}
        self._expanded_cache: dict[
            tuple[int, int, tuple[str, str] | None],
            tuple[_SemanticVariableState, ...],
        ] = {}
        self._resolve_at_cache: dict[
            tuple[str, Any, str | None, str | None], tuple[Any, ...]
        ] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._catalog, name)

    def preload_project(self, project: ProjectData) -> None:
        """Bulk-load direct identities and state rows for a steward project.

        Generated steward catalogs mostly reference direct, pinned bindings. Load
        that common path in set-oriented SQL, while leaving same_as and miss
        fallback in the normal lazy helpers below.
        """
        triples_by_key: dict[str, tuple[str, str, str]] = {}
        refs: list[tuple[str, str]] = []
        for source in project.sources:
            variant = source.register_variant.split("/")[2]
            for binding in source.bindings:
                parsed = parse(binding.variable)
                key = str(parsed)
                if (
                    parsed.provider is None
                    or parsed.register is None
                    or parsed.variable is None
                ):
                    continue
                triples_by_key[key] = (
                    parsed.provider,
                    parsed.register,
                    parsed.variable,
                )
                refs.append((key, variant))

        self._preload_direct_identities(triples_by_key)

        variant_pairs: set[tuple[int, str]] = set()
        successor_keys: set[tuple[str, str, str]] = set()
        for key, variant in refs:
            identity = self._identity_cache.get(key)
            if identity is None:
                continue
            variant_pairs.add((identity.register_id, variant))
            successor_keys.add(
                (identity.provider_slug, identity.register_slug, identity.variable_slug)
            )
        self._preload_variant_ids(variant_pairs)
        self._preload_successors(successor_keys)

        needed_states = {
            (identity.variable_id, rvid)
            for key, variant in refs
            if (identity := self._identity_cache.get(key)) is not None
            and (rvid := self._variant_id_cache.get((identity.register_id, variant)))
            is not None
        }
        self._preload_state_rows(needed_states)

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

        identity = self._resolve_identity(parsed)
        if identity is None:
            # Delegate the miss so the caller gets reg_meta's normal structured
            # RegMetaError rather than a webapp-local approximation.
            return self._catalog.resolve(parsed)
        semantic = _SemanticVariable(
            deprecated=identity.deprecated,
            replaced_by=self._successors(
                identity.provider_slug, identity.register_slug, identity.variable_slug
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
    ) -> list[Any]:
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        key = (str(parsed), _period_key(period), variant, value_set_version)
        if key not in self._resolve_at_cache:
            states = self._resolve_at_minimal(
                parsed, period, variant=variant, value_set_version=value_set_version
            )
            self._resolve_at_cache[key] = tuple(states)
        return list(self._resolve_at_cache[key])

    def _conn(self) -> sqlite3.Connection:
        return self._catalog._conn

    def _preload_direct_identities(
        self, triples_by_key: dict[str, tuple[str, str, str]]
    ) -> None:
        pending = {
            triple
            for key, triple in triples_by_key.items()
            if key not in self._identity_cache
        }
        if not pending:
            return
        found: dict[tuple[str, str, str], _VariableIdentity] = {}
        chunk_size = max(1, _SQLITE_PARAM_LIMIT // 3)
        for chunk in _chunks(sorted(pending), chunk_size):
            rows = self._conn().execute(
                "SELECT p.slug AS provider_slug, r.slug AS register_slug, "
                "v.slug AS variable_slug, v.variable_id, v.register_id, "
                "v.deprecated "
                "FROM variable v "
                "JOIN register r ON v.register_id = r.register_id "
                "JOIN provider p ON r.provider_id = p.provider_id "
                f"WHERE (p.slug, r.slug, v.slug) IN ({_tuple_placeholders(3, len(chunk))})",
                [part for triple in chunk for part in triple],
            )
            for row in rows:
                identity = _VariableIdentity(
                    variable_id=row["variable_id"],
                    register_id=row["register_id"],
                    provider_slug=row["provider_slug"],
                    register_slug=row["register_slug"],
                    variable_slug=row["variable_slug"],
                    deprecated=bool(row["deprecated"]),
                )
                found[
                    (
                        identity.provider_slug,
                        identity.register_slug,
                        identity.variable_slug,
                    )
                ] = identity

        for key, triple in triples_by_key.items():
            if key not in self._identity_cache and triple in found:
                self._identity_cache[key] = found[triple]

    def _preload_variant_ids(self, pairs: set[tuple[int, str]]) -> None:
        pending = {pair for pair in pairs if pair not in self._variant_id_cache}
        if not pending:
            return
        for pair in pending:
            self._variant_id_cache[pair] = None
        chunk_size = max(1, _SQLITE_PARAM_LIMIT // 2)
        for chunk in _chunks(sorted(pending), chunk_size):
            rows = self._conn().execute(
                "SELECT register_id, slug, register_variant_id "
                "FROM register_variant "
                f"WHERE (register_id, slug) IN ({_tuple_placeholders(2, len(chunk))})",
                [part for pair in chunk for part in pair],
            )
            for row in rows:
                self._variant_id_cache[(row["register_id"], row["slug"])] = row[
                    "register_variant_id"
                ]

    def _preload_successors(self, keys: set[tuple[str, str, str]]) -> None:
        pending = {key for key in keys if key not in self._successor_cache}
        if not pending:
            return
        successors: dict[tuple[str, str, str], list[_SemanticVariableRef]] = {
            key: [] for key in pending
        }
        chunk_size = max(1, _SQLITE_PARAM_LIMIT // 3)
        for chunk in _chunks(sorted(pending), chunk_size):
            rows = self._conn().execute(
                "SELECT predecessor_provider, predecessor_register, "
                "predecessor_variable, successor_provider, successor_register, "
                "successor_variable, effective_year "
                "FROM variable_replaced_by "
                "WHERE (predecessor_provider, predecessor_register, "
                "predecessor_variable) IN "
                f"({_tuple_placeholders(3, len(chunk))}) "
                "ORDER BY successor_provider, successor_register, successor_variable",
                [part for triple in chunk for part in triple],
            )
            for row in rows:
                key = (
                    row["predecessor_provider"],
                    row["predecessor_register"],
                    row["predecessor_variable"],
                )
                successors[key].append(
                    _SemanticVariableRef(
                        fqid=self._ref_fqid(
                            row["successor_provider"],
                            row["successor_register"],
                            row["successor_variable"],
                        ),
                        provider=row["successor_provider"],
                        register_name=row["successor_register"],
                        variable=row["successor_variable"],
                        effective_year=row["effective_year"],
                    )
                )

        for key, refs in successors.items():
            self._successor_cache[key] = tuple(refs)

    def _preload_state_rows(self, keys: set[tuple[int, int]]) -> None:
        pending = {key for key in keys if key not in self._state_rows_cache}
        if not pending:
            return
        for key in pending:
            self._state_rows_cache[key] = ()
        rows_by_key: dict[tuple[int, int], list[_SemanticVariableState]] = {
            key: [] for key in pending
        }
        chunk_size = max(1, _SQLITE_PARAM_LIMIT // 2)
        for chunk in _chunks(sorted(pending), chunk_size):
            rows = self._conn().execute(
                "SELECT variable_id, state_id, register_variant_id, "
                "delivery_column_name, value_set_id, value_set_version_label, "
                "valid_from, valid_to "
                "FROM variable_state "
                "WHERE (variable_id, register_variant_id) IN "
                f"({_tuple_placeholders(2, len(chunk))}) "
                "ORDER BY variable_id, register_variant_id, valid_from, "
                "valid_to, value_set_version_label, state_id",
                [part for pair in chunk for part in pair],
            )
            for row in rows:
                key = (row["variable_id"], row["register_variant_id"])
                if key not in rows_by_key:
                    continue
                rows_by_key[key].append(
                    _SemanticVariableState(
                        state_id=row["state_id"],
                        register_variant_id=row["register_variant_id"],
                        delivery_column_name=row["delivery_column_name"],
                        valid_from=row["valid_from"],
                        valid_to=row["valid_to"],
                        value_set_id=row["value_set_id"],
                        value_set_version_label=row["value_set_version_label"],
                    )
                )
        for key, rows in rows_by_key.items():
            self._state_rows_cache[key] = tuple(rows)
        self._preload_alias_windows({variable_id for variable_id, _rvid in pending})

    def _preload_alias_windows(self, variable_ids: set[int]) -> None:
        pending = {
            variable_id
            for variable_id in variable_ids
            if variable_id not in self._windows_cache
        }
        if not pending:
            return
        for variable_id in pending:
            self._windows_cache[variable_id] = {}
        for chunk in _chunks(sorted(pending), _SQLITE_PARAM_LIMIT):
            rows = self._conn().execute(
                "SELECT variable_id, register_variant_id, delivery_column_name, "
                "valid_from, valid_to "
                "FROM variable_alias_window "
                f"WHERE variable_id IN ({_qmarks(len(chunk))}) "
                "ORDER BY variable_id, register_variant_id, valid_from, "
                "delivery_column_name",
                list(chunk),
            )
            windows: dict[int, dict[int, list[_AliasWindow]]] = {}
            for row in rows:
                windows.setdefault(row["variable_id"], {}).setdefault(
                    row["register_variant_id"], []
                ).append(
                    _AliasWindow(
                        register_variant_id=row["register_variant_id"],
                        delivery_column_name=row["delivery_column_name"],
                        valid_from=row["valid_from"],
                        valid_to=row["valid_to"],
                    )
                )
            for variable_id, by_variant in windows.items():
                self._windows_cache[variable_id] = {
                    rvid: tuple(items) for rvid, items in by_variant.items()
                }

    def _resolve_identity(self, fqid: Fqid) -> _VariableIdentity | None:
        key = str(fqid)
        if key in self._identity_cache:
            return self._identity_cache[key]
        resolved = self._catalog._resolve_variable_identity(fqid)
        if resolved is None:
            self._identity_cache[key] = None
            return None
        var, _via_same_as = resolved
        meta = self._catalog._lookup_variable_meta(var["variable_id"])
        identity = _VariableIdentity(
            variable_id=var["variable_id"],
            register_id=meta["register_id"],
            provider_slug=meta["provider_slug"],
            register_slug=meta["register_slug"],
            variable_slug=meta["slug"],
            deprecated=bool(meta["deprecated"]),
        )
        self._identity_cache[key] = identity
        return identity

    def _variant_id(self, register_id: int, variant: str) -> int | None:
        key = (register_id, variant)
        if key in self._variant_id_cache:
            return self._variant_id_cache[key]
        rvid = self._catalog._resolve_variant_id(register_id, variant)
        resolved: int | None = rvid if isinstance(rvid, int) else None
        self._variant_id_cache[key] = resolved
        return resolved

    def _successors(
        self, provider_slug: str, register_slug: str, variable_slug: str
    ) -> tuple[_SemanticVariableRef, ...]:
        key = (provider_slug, register_slug, variable_slug)
        if key not in self._successor_cache:
            self._preload_successors({key})
        return self._successor_cache[key]

    def _raw_state_rows(
        self, variable_id: int, register_variant_id: int
    ) -> tuple[_SemanticVariableState, ...]:
        key = (variable_id, register_variant_id)
        if key not in self._state_rows_cache:
            self._preload_state_rows({key})
        return self._state_rows_cache[key]

    def _alias_windows(self, variable_id: int) -> dict[int, tuple[_AliasWindow, ...]]:
        if variable_id not in self._windows_cache:
            self._preload_alias_windows({variable_id})
        return self._windows_cache[variable_id]

    def _expanded_states(
        self,
        variable_id: int,
        register_variant_id: int,
        bounds: tuple[str, str] | None,
    ) -> tuple[_SemanticVariableState, ...]:
        key = (variable_id, register_variant_id, bounds)
        if key in self._expanded_cache:
            return self._expanded_cache[key]
        rows = self._raw_state_rows(variable_id, register_variant_id)
        if bounds is not None:
            lo, hi = bounds
            rows = tuple(
                row for row in rows if row.valid_from <= hi and row.valid_to >= lo
            )
        expanded = self._expand_state_windows(variable_id, rows, bounds)
        self._expanded_cache[key] = tuple(expanded)
        return self._expanded_cache[key]

    def _expand_state_windows(
        self,
        variable_id: int,
        rows: Sequence[_SemanticVariableState],
        bounds: tuple[str, str] | None,
    ) -> list[Any]:
        windows_by_variant = self._alias_windows(variable_id)
        if not windows_by_variant:
            return list(rows)
        lo, hi = bounds if bounds is not None else ("0001-01-01", "9999-12-31")
        out: list[_SemanticVariableState] = []
        for row in rows:
            windows = windows_by_variant.get(row.register_variant_id, ())
            state_windows = [
                window
                for window in windows
                if row.valid_from <= window.valid_from
                and window.valid_to <= row.valid_to
            ]
            has_base_window = row.delivery_column_name is not None and any(
                window.delivery_column_name.lower() == row.delivery_column_name.lower()
                for window in state_windows
            )
            if state_windows and not has_base_window:
                out.append(row)
                continue
            matched = [
                window
                for window in state_windows
                if window.valid_from <= hi and window.valid_to >= lo
            ]
            if not matched:
                out.append(row)
                continue
            for window in matched:
                out.append(
                    _SemanticVariableState(
                        state_id=row.state_id,
                        register_variant_id=row.register_variant_id,
                        delivery_column_name=window.delivery_column_name,
                        valid_from=window.valid_from,
                        valid_to=window.valid_to,
                        value_set_id=row.value_set_id,
                        value_set_version_label=row.value_set_version_label,
                    )
                )
        out.sort(key=lambda s: (s.valid_from, s.valid_to, s.delivery_column_name or ""))
        return out

    def _resolve_at_minimal(
        self,
        fqid: Fqid,
        period: Any,
        *,
        variant: str | None,
        value_set_version: str | None,
    ) -> list[Any]:
        identity = self._resolve_identity(fqid)
        if identity is None:
            # Preserve reg_meta's structured not-found exception.
            return self._catalog.resolve_at(
                fqid,
                period,
                variant=variant,
                value_set_version=value_set_version,
            )
        register_variant_id = None
        if variant is not None:
            register_variant_id = self._variant_id(identity.register_id, variant)
            if register_variant_id is None:
                return []
        if register_variant_id is None:
            return self._catalog.resolve_at(
                fqid,
                period,
                variant=variant,
                value_set_version=value_set_version,
            )
        states = list(
            self._expanded_states(
                identity.variable_id, register_variant_id, _period_bounds(period)
            )
        )
        if value_set_version is not None:
            states = [
                state
                for state in states
                if state.value_set_version_label == value_set_version
            ]
        return states

    @staticmethod
    def _ref_fqid(provider: str, register: str, variable: str) -> Fqid | None:
        try:
            return Fqid.binding_fqid(provider, register, variable)
        except FqidError:
            return None
