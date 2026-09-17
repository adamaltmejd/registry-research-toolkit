"""Read the steward extension's explicit provider, variable and state declarations.

Global catalog preparation reads thin-provider TOMLs through curated_records.
This adapter serves extend-db only. It preserves steward-prefixed IDs, pooled
variable keys across distinct variants, authored windows and co-delivered aliases.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from reg_meta.fqid import FqidError, period_token_to_bounds

from reg_meta_build._curation import curation_error, require_bool
from reg_meta_build.classifications import declared_short_names
from reg_meta_build.db import _file_sha256
from reg_meta_build.id import mint
from reg_meta_build.ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from reg_meta_build.sources import IRObject

# Synthesized single-table variant — same sentinel SOS uses for variant-less
# registers (LSS/BU). `validate_slug(..., allow_default=True)` permits it for
# register_variant; the runtime resolver looks it up exactly.
_DEFAULT_VARIANT = "_default"

# Allowed keys per table type — rejected-on-unknown so a curated typo
# (`is_identifer`, `purpse`) fails the build loudly instead of silently
# defaulting, mirroring the IR's `extra="forbid"` strict contract.
_PROVIDER_KEYS = frozenset({"name", "source_label"})
_REGISTER_KEYS = frozenset(
    {
        "key",
        "name",
        "purpose",
        "description",
        "valid_from",
        "valid_to",
        "variant",
        "variable",
    }
)
_VARIANT_KEYS = frozenset({"key", "name", "description", "valid_from", "valid_to"})
_VARIABLE_KEYS = frozenset(
    {
        "name",
        "column",
        "definition",
        "description",
        "data_type",
        "measurement_unit",
        "is_identifier",
        "is_sensitive",
        "valid_from",
        "valid_to",
        "variants",
        "classification",
        "value_set",
        "key",
        "state",
    }
)
_STATE_KEYS = frozenset(
    {
        "column",
        "data_type",
        "valid_from",
        "valid_to",
        "aliases",
        "value_set_version_label",
    }
)


@dataclass(frozen=True)
class _CuratedState:
    column: str
    data_type: str | None
    valid_from: str | None
    valid_to: str | None
    aliases: tuple[str, ...]
    value_set_version_label: str | None

    @property
    def columns(self) -> tuple[str, ...]:
        return (self.column, *self.aliases)


@dataclass(frozen=True)
class _CuratedDelivery:
    variants: tuple[str, ...] | None
    states: tuple[_CuratedState, ...]


@dataclass(frozen=True)
class _CuratedVariable:
    key: str
    name: str
    definition: str | None
    description: str | None
    measurement_unit: str | None
    is_identifier: bool
    is_sensitive: bool
    valid_from: str | None  # None → inherit the register coverage start
    valid_to: str | None  # None → open-ended (materializer writes the sentinel)
    classification: str | None  # None → unlinked; else an existing catalog short_name
    value_set: str | None  # nonempty declarations are rejected for steward extensions
    deliveries: tuple[_CuratedDelivery, ...]


@dataclass(frozen=True)
class _CuratedVariant:
    key: str
    name: str
    description: str | None
    valid_from: str | None  # None → no variant-specific floor
    valid_to: str | None  # None → no variant-specific ceiling
    synthesized: bool


@dataclass(frozen=True)
class _CuratedRegister:
    key: str
    name: str
    purpose: str | None
    description: str | None
    valid_from: str | None
    valid_to: str | None  # None → open-ended; default valid_to for its variables
    variants: tuple[_CuratedVariant, ...]
    variables: tuple[_CuratedVariable, ...]


class CuratedAdapter:
    """Emit the steward extension graph from its `<provider>.toml`."""

    def __init__(
        self,
        provider: str,
        *,
        steward: str,
        classification_seed_path: Path | None = None,
    ) -> None:
        self.provider = provider
        self.steward = steward
        self.provider_name: str | None = None
        self.source_label: str | None = None
        # Validate authored classification names before extend-db rejects linkage.
        self._classification_seed_path = classification_seed_path
        self.row_counts: dict[str, int] = {}
        self.source_checksums: dict[str, str] = {}
        # extend-db rejects classification linkage before writing its output.
        self.classification_candidates: list[tuple[int, int | None, str]] = []

    def emit(self, source_dir: Path) -> Iterator[IRObject]:
        toml_path = source_dir / f"{self.provider}.toml"
        if not toml_path.is_file():
            raise curation_error(
                "curated_toml_not_found",
                f"Curated provider file not found: {toml_path}",
                f"Author {self.provider}.toml under {source_dir}.",
            )
        self.source_checksums[toml_path.name] = _file_sha256(toml_path)
        registers = self._load(toml_path)
        for reg in registers:
            yield from self._emit_register(reg)

    # -- loading / validation ------------------------------------------------

    def _load(self, path: Path) -> list[_CuratedRegister]:
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: cannot parse TOML: {exc}",
                "Fix the TOML syntax.",
            ) from exc

        top_level_keys = frozenset({"provider", "register"})
        self._reject_unknown(path, raw, top_level_keys, "top level")
        provider = raw.get("provider")
        if provider is not None:
            if not isinstance(provider, dict):
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: `provider` must be a table.",
                    "Declare [provider] with name and source_label.",
                )
            self._reject_unknown(path, provider, _PROVIDER_KEYS, "provider")
            self.provider_name = self._req_str(path, provider, "name", "provider")
            self.source_label = self._req_str(
                path, provider, "source_label", "provider"
            )
        else:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: steward provider needs a [provider] table.",
                "Declare the provider display name and source_label.",
            )
        reg_tables = raw.get("register")
        if (
            not isinstance(reg_tables, list)
            or not reg_tables
            or not all(isinstance(entry, dict) for entry in reg_tables)
        ):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: expected a non-empty `[[register]]` array of tables.",
                "Declare each register as [[register]] with a key and name.",
            )

        registers: list[_CuratedRegister] = []
        seen_reg_keys: set[str] = set()
        for entry in reg_tables:
            reg = self._load_register(path, entry, seen_reg_keys)
            registers.append(reg)

        # Validate `classification` references against the seed manifest in a
        # single pass once everything is parsed (PROVIDER-AGNOSTIC: any declared
        # short_name passes regardless of its `provider` tag — every declared
        # classification is seeded; only an UNDECLARED short_name, i.e. a typo,
        # fails). Resolve the seed only when something references a
        # classification, so a curated TOML with no `classification` keys needs
        # neither the seed nor `declared_short_names()`.
        if any(
            var.classification is not None for reg in registers for var in reg.variables
        ):
            declared = declared_short_names(self._classification_seed_path)
            for reg in registers:
                for var in reg.variables:
                    if (
                        var.classification is not None
                        and var.classification not in declared
                    ):
                        raise curation_error(
                            "curated_toml_invalid",
                            f"{path.name}: register {reg.key!r} variable "
                            f"{var.name!r}: classification {var.classification!r} "
                            f"is not a declared classification "
                            f"(curation/classifications.toml).",
                            "Use an existing classification short_name (e.g. "
                            "'ICD-10-SE', 'ATC') or declare it in "
                            "curation/classifications.toml.",
                        )
        return registers

    def _load_register(
        self, path: Path, entry: dict, seen_reg_keys: set[str]
    ) -> _CuratedRegister:
        key = self._req_str(path, entry, "key", "register")
        if key in seen_reg_keys:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: duplicate register key {key!r}.",
                "Each register key must be unique within the provider.",
            )
        seen_reg_keys.add(key)
        self._reject_unknown(path, entry, _REGISTER_KEYS, f"register {key!r}")
        name = self._req_str(path, entry, "name", f"register {key!r}")
        valid_from = self._opt_str(entry, "valid_from")
        valid_to = self._opt_str(entry, "valid_to")
        self._check_authored_window(path, valid_from, valid_to, f"register {key!r}")

        variant_entries = entry.get("variant", [])
        if not isinstance(variant_entries, list) or not all(
            isinstance(variant, dict) for variant in variant_entries
        ):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: register {key!r}: `variant` must be an array of tables.",
                "Use [[register.variant]] tables.",
            )
        variants: list[_CuratedVariant] = []
        seen_variant_keys: set[str] = set()
        for v in variant_entries:
            vk = self._req_str(path, v, "key", f"register {key!r} variant")
            if vk in seen_variant_keys:
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: register {key!r}: duplicate variant key {vk!r}.",
                    "Each variant key must be unique within the register.",
                )
            seen_variant_keys.add(vk)
            self._reject_unknown(
                path, v, _VARIANT_KEYS, f"register {key!r} variant {vk!r}"
            )
            variant_valid_from = self._opt_str(v, "valid_from")
            variant_valid_to = self._opt_str(v, "valid_to")
            self._check_authored_window(
                path,
                variant_valid_from,
                variant_valid_to,
                f"register {key!r} variant {vk!r}",
            )
            variants.append(
                _CuratedVariant(
                    key=vk,
                    name=self._req_str(
                        path, v, "name", f"register {key!r} variant {vk!r}"
                    ),
                    description=self._opt_str(v, "description"),
                    valid_from=variant_valid_from,
                    valid_to=variant_valid_to,
                    synthesized=False,
                )
            )
        if not variants:
            # Single-table register: synthesize the `_default` variant.
            variants.append(
                _CuratedVariant(
                    key=_DEFAULT_VARIANT,
                    name=_DEFAULT_VARIANT,
                    description=None,
                    valid_from=None,
                    valid_to=None,
                    synthesized=True,
                )
            )
        variant_keys = {v.key for v in variants}

        var_entries = entry.get("variable", [])
        if (
            not isinstance(var_entries, list)
            or not var_entries
            or not all(isinstance(variable, dict) for variable in var_entries)
        ):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: register {key!r}: expected a non-empty "
                "`[[register.variable]]` array of tables.",
                "Declare at least one variable per register.",
            )
        variables_by_key: dict[str, _CuratedVariable] = {}
        for ve in var_entries:
            var = self._load_variable(path, key, ve, variant_keys)
            prior = variables_by_key.get(var.key)
            if prior is None:
                variables_by_key[var.key] = var
                continue
            prior_variants = {
                variant
                for delivery in prior.deliveries
                for variant in (delivery.variants or tuple(variant_keys))
            }
            new_variants = set(var.deliveries[0].variants or tuple(variant_keys))
            if overlap := sorted(prior_variants & new_variants):
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: register {key!r} repeats variable {var.key!r} "
                    f"within variant(s) {overlap}.",
                    "List a variable key once per variant; repeat it only for "
                    "distinct variant deliveries.",
                )
            fields = (
                "name",
                "definition",
                "description",
                "measurement_unit",
                "is_identifier",
                "is_sensitive",
                "valid_from",
                "valid_to",
                "classification",
                "value_set",
            )
            for field in fields:
                first, other = getattr(prior, field), getattr(var, field)
                if first != other:
                    raise curation_error(
                        "curated_toml_invalid",
                        f"{path.name}: register {key!r} lists variable {var.key!r} "
                        f"with different `{field}` values: {first!r} vs {other!r}.",
                        "A repeated key is one register-scoped variable; keep its "
                        "metadata identical and vary only states/variants.",
                    )
            variables_by_key[var.key] = replace(
                prior, deliveries=(*prior.deliveries, *var.deliveries)
            )

        return _CuratedRegister(
            key=key,
            name=name,
            purpose=self._opt_str(entry, "purpose"),
            description=self._opt_str(entry, "description"),
            valid_from=valid_from,
            valid_to=valid_to,
            variants=tuple(variants),
            variables=tuple(variables_by_key.values()),
        )

    def _load_variable(
        self,
        path: Path,
        reg_key: str,
        entry: dict,
        variant_keys: set[str],
    ) -> _CuratedVariable:
        ctx = f"register {reg_key!r} variable"
        name = self._req_str(path, entry, "name", ctx)
        self._reject_unknown(
            path, entry, _VARIABLE_KEYS, f"register {reg_key!r} variable {name!r}"
        )
        valid_from = self._opt_str(entry, "valid_from")
        valid_to = self._opt_str(entry, "valid_to")
        self._check_authored_window(path, valid_from, valid_to, f"{ctx} {name!r}")

        variants = self._load_variant_refs(path, entry, ctx, name, variant_keys)
        state_entries = entry.get("state")
        if state_entries is None:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r} needs at least one `state`.",
                "Declare one or more [[register.variable.state]] tables.",
            )
        if "column" in entry or "data_type" in entry:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r} mixes flat fields with `state`.",
                "Put column/data_type on each [[register.variable.state]].",
            )
        key = self._req_str(path, entry, "key", f"{ctx} {name!r}")
        if (
            not isinstance(state_entries, list)
            or not state_entries
            or not all(isinstance(state, dict) for state in state_entries)
        ):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r}: `state` must be a non-empty array.",
                "Declare at least one [[register.variable.state]].",
            )
        states = tuple(
            self._load_state(path, reg_key, key, state, i)
            for i, state in enumerate(state_entries)
        )

        if "." in key:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: register {reg_key!r} variable key {key!r} must not "
                "contain '.'.",
                "Rename the variable key so it has no dot.",
            )

        return _CuratedVariable(
            key=key,
            name=name,
            definition=self._opt_str(entry, "definition"),
            description=self._opt_str(entry, "description"),
            measurement_unit=self._opt_str(entry, "measurement_unit"),
            is_identifier=self._opt_bool(
                path, entry, "is_identifier", f"{ctx} {name!r}"
            ),
            is_sensitive=self._opt_bool(path, entry, "is_sensitive", f"{ctx} {name!r}"),
            valid_from=valid_from,
            valid_to=valid_to,
            classification=self._opt_str(entry, "classification"),
            value_set=self._value_set_field(path, entry, name),
            deliveries=(_CuratedDelivery(variants=variants, states=states),),
        )

    def _load_variant_refs(
        self,
        path: Path,
        entry: dict,
        ctx: str,
        name: str,
        variant_keys: set[str],
    ) -> tuple[str, ...] | None:
        variants = entry.get("variants")
        if variants is None:
            return None
        if not isinstance(variants, list) or not all(
            isinstance(x, str) and x for x in variants
        ):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r}: `variants` must be a string array.",
                "List the variant keys this variable is delivered in.",
            )
        if not variants:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r}: `variants` must list at least one key.",
                "List variant keys, or omit `variants` to deliver in every variant.",
            )
        unknown = [x for x in variants if x not in variant_keys]
        if unknown:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r}: unknown variant(s) {unknown}.",
                f"Use declared variant keys: {sorted(variant_keys)}.",
            )
        if len(set(variants)) != len(variants):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r}: duplicate variant reference.",
                "List each variant key once.",
            )
        return tuple(variants)

    def _load_state(
        self,
        path: Path,
        reg_key: str,
        variable_key: str,
        entry: dict,
        index: int,
    ) -> _CuratedState:
        ctx = f"register {reg_key!r} variable {variable_key!r} state[{index}]"
        self._reject_unknown(path, entry, _STATE_KEYS, ctx)
        valid_from = self._opt_str(entry, "valid_from")
        valid_to = self._opt_str(entry, "valid_to")
        self._check_authored_window(path, valid_from, valid_to, ctx)
        aliases = entry.get("aliases", [])
        if not isinstance(aliases, list) or not all(
            isinstance(alias, str) and alias.strip() for alias in aliases
        ):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx}: `aliases` must be a string array.",
                "List each co-delivered spelling as a non-empty string.",
            )
        state = _CuratedState(
            column=self._req_str(path, entry, "column", ctx),
            data_type=self._opt_str(entry, "data_type"),
            valid_from=valid_from,
            valid_to=valid_to,
            aliases=tuple(alias.strip() for alias in aliases),
            value_set_version_label=self._opt_str(entry, "value_set_version_label"),
        )
        if len(set(state.columns)) != len(state.columns):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} repeats a delivery column.",
                "List each co-delivered column once and not as its own alias.",
            )
        return state

    def _value_set_field(self, path: Path, entry: dict, name: str) -> str | None:
        """Reject unsupported value membership instead of silently dropping it."""
        value_set = self._opt_str(entry, "value_set")
        if value_set is not None:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: variable {name!r} sets `value_set` but provider "
                f"{self.provider!r} does not support value sets.",
                "Steward extensions cannot introduce value sets.",
            )
        return value_set

    def _req_str(self, path: Path, entry: dict, field: str, ctx: str) -> str:
        value = entry.get(field)
        if not isinstance(value, str) or not value.strip():
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx}: `{field}` must be a non-empty string.",
                f"Set a string `{field}`.",
            )
        return value.strip()

    def _opt_str(self, entry: dict, field: str) -> str | None:
        value = entry.get(field)
        if value is None:
            return None
        if not isinstance(value, str):
            raise curation_error(
                "curated_toml_invalid",
                f"`{field}` must be a string when present.",
                f"Quote `{field}` or drop it.",
            )
        return value.strip() or None

    def _opt_bool(self, path: Path, entry: dict, field: str, ctx: str) -> bool:
        # Shared strict-bool leaf (`bool(...)` on a present non-bool is a footgun —
        # `bool("false")` is True, silently flipping a sensitivity flag). This
        # surface's `path.name` is the file context, so it's both `prefix` and
        # `file_name`; the established `curated_toml_invalid` code is preserved.
        return require_bool(
            entry,
            field,
            ctx,
            code="curated_toml_invalid",
            prefix=path.name,
            file_name=path.name,
        )

    def _check_boundary(self, path: Path, value: str, ctx: str) -> None:
        try:
            period_token_to_bounds(value)
        except FqidError as exc:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx}: {value!r} is not a valid ISO period.",
                "Use YYYY, YYYY-MM, YYYY-MM-DD, or omit an unknown/open bound.",
            ) from exc

    def _check_authored_window(
        self,
        path: Path,
        valid_from: str | None,
        valid_to: str | None,
        ctx: str,
    ) -> None:
        if valid_from is not None:
            self._check_boundary(path, valid_from, f"{ctx} valid_from")
        if valid_to is not None:
            self._check_boundary(path, valid_to, f"{ctx} valid_to")
        if valid_from is None or valid_to is None:
            return
        expanded_from = self._expanded_boundary(valid_from, end=False)
        expanded_to = self._expanded_boundary(valid_to, end=True)
        if expanded_from > expanded_to:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} has an inverted validity window "
                f"({expanded_from} > {expanded_to}).",
                "Set valid_from no later than valid_to.",
            )

    def _reject_unknown(
        self, path: Path, entry: dict, allowed: frozenset[str], ctx: str
    ) -> None:
        """Fail on any unrecognized key — the curated-TOML analogue of the IR's
        `extra="forbid"`, so a typo (`is_identifer`, `purpse`) is loud, not a
        silent default."""
        unknown = sorted(set(entry) - allowed)
        if unknown:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx}: unknown key(s) {unknown}.",
                f"Allowed keys: {sorted(allowed)}.",
            )

    # -- emit ----------------------------------------------------------------

    def _emit_register(self, reg: _CuratedRegister) -> Iterator[IRObject]:
        register_id = mint("register", self.provider, reg.key)
        self.row_counts[f"{self.provider}:{reg.key}"] = len(reg.variables)
        yield IRRegister(
            register_id=register_id,
            provider=self.provider,
            slug="",  # populate_slugs fills it from fqid_slugs/<provider>.toml
            name=reg.name,
            description=reg.description,
            purpose=reg.purpose,
        )

        variant_ids: dict[str, int] = {}
        for variant in reg.variants:
            variant_id = mint("variant", self.provider, reg.key, variant.key)
            variant_ids[variant.key] = variant_id
            yield IRVariant(
                register_variant_id=variant_id,
                register_id=register_id,
                slug="",
                name=variant.name,
                description=variant.description,
                valid_from=variant.valid_from,
                valid_to=variant.valid_to,
                synthesized=variant.synthesized,
            )

        all_variant_keys = tuple(v.key for v in reg.variants)
        variant_by_key = {v.key: v for v in reg.variants}
        for var in reg.variables:
            yield from self._emit_variable(
                reg, register_id, variant_ids, variant_by_key, all_variant_keys, var
            )

    def _emit_variable(
        self,
        reg: _CuratedRegister,
        register_id: int,
        variant_ids: dict[str, int],
        variant_by_key: dict[str, _CuratedVariant],
        all_variant_keys: tuple[str, ...],
        var: _CuratedVariable,
    ) -> Iterator[IRObject]:
        variable_id = mint("variable", self.provider, reg.key, var.key)
        if var.classification is not None:
            self.classification_candidates.append(
                (variable_id, None, var.classification)
            )
        yield IRVariable(
            variable_id=variable_id,
            register_id=register_id,
            provider_key=var.key,
            slug="",
            name=var.name,
            definition=var.definition,
            description=var.description,
            measurement_unit=var.measurement_unit,
            is_sensitive=var.is_sensitive,
            is_identifier=var.is_identifier,
            source_register_id=None,
            source_register_text=None,
            source_label=self.source_label,
        )

        seen_state_keys: set[tuple[str, str | None, str]] = set()
        for delivery in var.deliveries:
            target_keys = (
                delivery.variants if delivery.variants is not None else all_variant_keys
            )
            for vk in target_keys:
                variant_id = variant_ids[vk]
                for state in delivery.states:
                    valid_from, valid_to = self._state_window(
                        reg, var, state, variant_by_key[vk]
                    )
                    label = state.value_set_version_label or ""
                    state_key = (vk, valid_from, label)
                    if state_key in seen_state_keys:
                        raise curation_error(
                            "curated_toml_invalid",
                            f"{self.provider}.toml: register {reg.key!r} variable "
                            f"{var.key!r} has duplicate state key "
                            f"(variant={vk!r}, valid_from={valid_from!r}, "
                            f"value_set_version_label={label!r}).",
                            "Put co-delivered spellings in one state's aliases or "
                            "give distinct state windows.",
                        )
                    seen_state_keys.add(state_key)
                    state_id = mint(
                        "state",
                        self.provider,
                        reg.key,
                        vk,
                        var.key,
                        state.column,
                        valid_from or "0001-01-01",
                        label,
                    )
                    yield IRVariableState(
                        state_id=state_id,
                        variable_id=variable_id,
                        register_variant_id=variant_id,
                        valid_from=valid_from,
                        valid_to=valid_to,
                        data_type=state.data_type,
                        data_length=None,
                        delivery_column_name=state.column,
                        value_set_id=None,
                        value_set_version_label=state.value_set_version_label,
                        provenance=f"steward:{self.steward}",
                    )
                    for column in state.columns:
                        yield IRVariableAlias(
                            variable_id=variable_id,
                            register_variant_id=variant_id,
                            delivery_column_name=column,
                        )
                    if state.aliases:
                        for column in state.columns:
                            yield IRVariableAliasWindow(
                                variable_id=variable_id,
                                register_variant_id=variant_id,
                                delivery_column_name=column,
                                valid_from=valid_from,
                                valid_to=valid_to,
                            )

    def _state_window(
        self,
        reg: _CuratedRegister,
        var: _CuratedVariable,
        state: _CuratedState,
        variant: _CuratedVariant,
    ) -> tuple[str | None, str | None]:
        base_valid_from = state.valid_from or var.valid_from or reg.valid_from
        base_valid_to = state.valid_to or var.valid_to or reg.valid_to
        starts = [
            self._expanded_boundary(d, end=False)
            for d in (base_valid_from, variant.valid_from)
            if d is not None
        ]
        valid_from = max(starts) if starts else None
        valid_to = self._earliest_valid_to(base_valid_to, variant.valid_to)
        if valid_from is not None and valid_to is not None and valid_to < valid_from:
            raise curation_error(
                "curated_toml_invalid",
                f"{self.provider}.toml: register {reg.key!r} variable "
                f"{var.name!r} has no overlap with variant {variant.key!r} "
                f"({valid_from} > {valid_to}).",
                "Drop the variant from the variable or fix the validity windows.",
            )
        return valid_from, valid_to

    def _earliest_valid_to(self, *dates: str | None) -> str | None:
        present = [self._expanded_boundary(d, end=True) for d in dates if d is not None]
        return min(present) if present else None

    def _expanded_boundary(self, value: str, *, end: bool) -> str:
        lo, hi = period_token_to_bounds(value)
        return hi if end else lo
