"""Generic adapter for *thin curated* providers (FOHM, Försäkringskassan, …).

Most providers ship a machine-readable native delivery the build parses
(SCB's CSV exports, SOS's `.xlsx` workbooks). A *thin curated* provider has
none: a public agency whose register/variable documentation we transcribe by
hand from public sources into a maintainer-authored TOML. That TOML **is** the
provider's source delivery — the authoritative, citable artifact, committed
alongside the build (unlike the untracked SCB/SOS seed).

Rather than a near-identical `fohm.py` / `fk.py` / `iaf.py` adapter per agency
(they'd all do the same thing), one `CuratedAdapter` reads a
provider-parameterized TOML and emits the universal IR. Adding a thin provider
is then: append a `provider` seed row (`db._PROVIDER_SEED`), register the
agency's input dir (`db._CURATED_PROVIDERS`), drop the curated TOML under
`input_data/<Agency>/<provider>.toml`, and curate register/variant slugs in
`fqid_slugs/<provider>.toml`. See DESIGN.md → Curated thin providers.

Global thin-provider ids are `mint()`ed into the high band `[2^62, 2^63)` (the
provider name is the first `mint` part, so a thin provider never collides with SOS's
`mint("sos", …)` ids — same disjointness argument as DESIGN.md → Deterministic
ID minting). The adapter emits no value sets (categorical code *lists* are a
follow-up; see #422) and writes no build-scratch — it is pure IR, like the SOS
adapter. A categorical variable may still LINK to an existing catalog
classification via the optional `classification` key (it reuses the catalog
classification, minting no codes; see #446).

TOML shape (one entry per register; a register with no `[[register.variant]]`
gets a synthesized `_default` variant, the single-table case):

    [[register]]
    key = "sminet"                 # stable; mint("<provider>", key) → register_id
    name = "SmiNet"
    purpose = "…"                  # catalog browse-card prose (register.purpose)
    valid_from = "2004-01-01"      # coverage start; default for its variables
    valid_to = "2010-12-31"        # OPTIONAL; a closed register (Pliktverket
                                   # 1997-2010, a discontinued benefit) — bounds
                                   # every variable state unless the variable
                                   # overrides it. Omit → open-ended (9999 sentinel)

      [[register.variant]]         # OPTIONAL; omit for a single-table register
      key = "fall"
      name = "…"
      description = "…"
      valid_from = "2004-01-01"    # OPTIONAL per-variant coverage window
      valid_to = "2010-12-31"

      [[register.variable]]
      name = "Personnummer"        # source/display name → variable.name
      column = "personnummer"      # delivery column → variable_alias + auto-slug
      definition = "…"
      data_type = "text"
      is_identifier = true
      is_sensitive = true
      valid_from = "2004-01-01"    # OPTIONAL per-variable override
      variants = ["fall"]          # OPTIONAL; default = every variant of register
      classification = "ICD-10-SE" # OPTIONAL; short_name of an existing catalog
                                   # classification — links the variable's states,
                                   # mints no codes

A steward provider adds a required ``[provider]`` table (display ``name`` and
``source_label`` provenance), may omit unknown register dates, and uses an
explicit variable key with one or more ``[[register.variable.state]]`` tables.
Repeated variable keys are pooled across their ``variants`` deliveries; each
state may carry co-delivered ``aliases``. ``steward=...`` selects the established
prefixed identity inputs (``mint("register", provider, key)`` etc.); the global
unprefixed convention above remains unchanged.
"""

from __future__ import annotations

import csv
import re
import tomllib
from dataclasses import dataclass, replace
from datetime import date
from typing import TYPE_CHECKING

from reg_meta.fqid import FqidError, period_token_to_bounds

from reg_meta_build._curation import curation_error, require_bool
from reg_meta_build.classifications import declared_short_names
from reg_meta_build.db import _file_sha256, _value_set_hash
from reg_meta_build.id import mint, mint_canonical_scb
from reg_meta_build.ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path

    from reg_meta_build.sources import IRObject

# Synthesized single-table variant — same sentinel SOS uses for variant-less
# registers (LSS/BU). `validate_slug(..., allow_default=True)` permits it for
# register_variant; the runtime resolver looks it up exactly.
_DEFAULT_VARIANT = "_default"

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Allowed keys per table type — rejected-on-unknown so a curated typo
# (`is_identifer`, `purpse`) fails the build loudly instead of silently
# defaulting, mirroring the IR's `extra="forbid"` strict contract.
_PROVIDER_KEYS = frozenset({"name", "source_label"})
_GLOBAL_REGISTER_KEYS = frozenset(
    {
        "key",
        "name",
        "purpose",
        "valid_from",
        "valid_to",
        "variant",
        "variable",
    }
)
_STEWARD_REGISTER_KEYS = frozenset(
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
_GLOBAL_VARIABLE_KEYS = frozenset(
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
    }
)
_STEWARD_VARIABLE_KEYS = _GLOBAL_VARIABLE_KEYS | {"key", "state"}
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
    value_set: str | None  # None → no value set; else a code-list name (canonical-scb)
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
    """Emit IR for a thin curated provider from its `<provider>.toml`.

    `provider` is the seed slug (`'fohm'`, `'fk'`, …) and TOML basename.
    ``steward`` explicitly selects the steward contract and identity convention.
    """

    def __init__(
        self,
        provider: str,
        *,
        steward: str | None = None,
        classification_seed_path: Path | None = None,
    ) -> None:
        self.provider = provider
        self.steward = steward
        self.provider_name: str | None = None
        self.source_label: str | None = None
        # The id-minting function. The base thin-provider adapter mints into the
        # high band; `CanonicalScbAdapter` overrides this with `mint_canonical_scb`
        # to keep its `scb`-provider ids in the low band (#444).
        self._mint = mint
        # Value sets are a canonical-SCB-only feature (the base adapter has no conn
        # to intern codes). `False` here makes a `value_set` key on a thin-provider
        # TOML fail-fast at load rather than silently produce a code-less catalog.
        self._supports_value_sets = False
        # The seed the build was invoked with (`build_db(seed_path=...)`), so
        # `classification` validation checks the SAME manifest
        # `populate_classifications` seeds; None → the in-repo default.
        self._classification_seed_path = classification_seed_path
        # Side channels the materializer drains off every adapter (db.materialize).
        # A thin provider has no sibling edges, fold hints, or coalesce stats.
        self.row_counts: dict[str, int] = {}
        self.source_checksums: dict[str, str] = {}
        self.sibling_edges: list[tuple[int, int]] = []
        self.fold_slug_hints: dict[int, str] = {}
        # `(variable_id, value_set_id, short_name)` — the same provider-blind
        # classification side channel SOS feeds; the materializer drains it and
        # resolves short_name → classification_id at feed time. value_set_id is
        # always None here (curated emits no value sets).
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

        top_level_keys = (
            frozenset({"provider", "register"})
            if self.steward is not None
            else frozenset({"register"})
        )
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
        elif self.steward is not None:
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
        register_keys = (
            _STEWARD_REGISTER_KEYS
            if self.steward is not None
            else _GLOBAL_REGISTER_KEYS
        )
        self._reject_unknown(path, entry, register_keys, f"register {key!r}")
        name = self._req_str(path, entry, "name", f"register {key!r}")
        valid_from = self._opt_str(entry, "valid_from")
        if valid_from is None and self.steward is None:
            valid_from = self._req_str(path, entry, "valid_from", f"register {key!r}")
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
        seen_columns: set[str] = set()
        for ve in var_entries:
            var = self._load_variable(path, key, ve, variant_keys, seen_columns)
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
        seen_columns: set[str],
    ) -> _CuratedVariable:
        ctx = f"register {reg_key!r} variable"
        name = self._req_str(path, entry, "name", ctx)
        variable_keys = (
            _STEWARD_VARIABLE_KEYS
            if self.steward is not None
            else _GLOBAL_VARIABLE_KEYS
        )
        self._reject_unknown(
            path, entry, variable_keys, f"register {reg_key!r} variable {name!r}"
        )
        valid_from = self._opt_str(entry, "valid_from")
        valid_to = self._opt_str(entry, "valid_to")
        self._check_authored_window(path, valid_from, valid_to, f"{ctx} {name!r}")

        variants = self._load_variant_refs(path, entry, ctx, name, variant_keys)
        state_entries = entry.get("state")
        if self.steward is not None and state_entries is None:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx} {name!r} needs at least one `state`.",
                "Declare one or more [[register.variable.state]] tables.",
            )
        if state_entries is None:
            column = self._req_str(path, entry, "column", f"{ctx} {name!r}")
            if column in seen_columns:
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: register {reg_key!r}: duplicate column {column!r}.",
                    "Each variable's delivery column must be unique within the register.",
                )
            seen_columns.add(column)
            key = self._opt_str(entry, "key") or column
            states = (
                _CuratedState(
                    column=column,
                    data_type=self._opt_str(entry, "data_type"),
                    valid_from=None,
                    valid_to=None,
                    aliases=(),
                    value_set_version_label=None,
                ),
            )
        else:
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
        if self.steward is not None and len(set(variants)) != len(variants):
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
        """Parse the optional `value_set` key, rejecting it on adapters that can't
        intern code lists (every thin provider) — otherwise the code list would be
        silently dropped (the base `_value_set_id_for` returns None)."""
        value_set = self._opt_str(entry, "value_set")
        if value_set is not None and not self._supports_value_sets:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: variable {name!r} sets `value_set` but provider "
                f"{self.provider!r} does not support value sets.",
                "Value sets are canonical-SCB-only (CanonicalScbAdapter).",
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
        if self.steward is not None:
            try:
                period_token_to_bounds(value)
            except FqidError as exc:
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: {ctx}: {value!r} is not a valid ISO period.",
                    "Use YYYY, YYYY-MM, YYYY-MM-DD, or omit an unknown/open bound.",
                ) from exc
            return

        valid = bool(_ISO_DATE.match(value))
        if valid:
            try:
                date.fromisoformat(value)
            except ValueError:
                valid = False
        if not valid:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx}: {value!r} must be a valid ISO date YYYY-MM-DD.",
                "Use a real ten-character ISO 8601 date.",
            )

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
        register_id = (
            mint("register", self.provider, reg.key)
            if self.steward is not None
            else self._mint(self.provider, reg.key)
        )
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
            variant_id = (
                mint("variant", self.provider, reg.key, variant.key)
                if self.steward is not None
                else self._mint(self.provider, reg.key, variant.key)
            )
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

    def _value_set_id_for(
        self, reg: _CuratedRegister, var: _CuratedVariable
    ) -> int | None:
        """The shared `value_set_id` a variable's states reference. The base
        thin-provider adapter emits no value sets, so always None. Overridden by
        `CanonicalScbAdapter` to intern a column's code list (#444)."""
        return None

    def _emit_variable(
        self,
        reg: _CuratedRegister,
        register_id: int,
        variant_ids: dict[str, int],
        variant_by_key: dict[str, _CuratedVariant],
        all_variant_keys: tuple[str, ...],
        var: _CuratedVariable,
    ) -> Iterator[IRObject]:
        variable_id = (
            mint("variable", self.provider, reg.key, var.key)
            if self.steward is not None
            else self._mint(self.provider, reg.key, var.key)
        )
        # Per-variable value set: None for the base thin-provider adapter (it emits
        # no value sets); `CanonicalScbAdapter` interns a code list and returns its
        # shared value_set_id. Every state of this variable shares it.
        value_set_id = self._value_set_id_for(reg, var)
        if var.classification is not None:
            # ONE candidate per variable (not per state): the candidate keys on
            # variable_id. The link is by the catalog classification's `short_name`,
            # resolved provider-blind at feed time (db._feed_classification_candidates).
            self.classification_candidates.append(
                (variable_id, value_set_id, var.classification)
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
                    state_id = (
                        mint(
                            "state",
                            self.provider,
                            reg.key,
                            vk,
                            var.key,
                            state.column,
                            valid_from or "0001-01-01",
                            label,
                        )
                        if self.steward is not None
                        else self._mint(self.provider, reg.key, var.key, vk)
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
                        value_set_id=value_set_id,
                        value_set_version_label=state.value_set_version_label,
                        provenance=(
                            f"steward:{self.steward}"
                            if self.steward is not None
                            else None
                        ),
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
        if self.steward is None:
            return value
        lo, hi = period_token_to_bounds(value)
        return hi if end else lo


class CanonicalScbAdapter(CuratedAdapter):
    """Emit IR for CANONICAL-SCB content curated onto the `scb` provider (#444).

    SCB registers SWECOV holds but that are absent from SCB's machine export
    (Utrikeshandel med tjänster; the AGI employer-declaration header). It reuses
    `CuratedAdapter`'s TOML parsing but differs in two ways:

    - **Low-band ids.** `mint_canonical_scb` puts register/variant/variable/state
      ids in the reserved sub-band ``[2^61, 2^62)`` so they pass the SCB-provider
      band check (SCB ids must be ``< 2^62``) yet stay disjoint from real
      source-derived SCB ids. The provider is ``"scb"`` → the materializer attributes
      the rows to provider_id 1 (no new provider seed).
    - **Real value sets.** A categorical column carries ``value_set = "<name>"``; the
      adapter loads ``<name>.csv`` (``code,label``) and interns it into
      ``value_code`` / ``value_set`` / ``value_set_member`` content-addressed (the
      same INSERT-OR-IGNORE pattern SCB/SOS use), then links the state's
      ``value_set_id``. This needs a DB connection and MUST run AFTER the SCB adapter
      so the AUTOINCREMENT ``value_code`` ids pick up after SCB's high-water mark.

    The committed input is `input_data/scb_canonical/scb_canonical.toml` plus the
    `<name>.csv` code lists.
    """

    SOURCE_FILE = "scb_canonical.toml"

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        classification_seed_path: Path | None = None,
    ) -> None:
        super().__init__("scb", classification_seed_path=classification_seed_path)
        self._mint = mint_canonical_scb
        self._supports_value_sets = True
        self._conn = conn
        self._source_dir: Path | None = None
        self._set_id_by_hash: dict[bytes, int] = {}
        self._codes_cache: dict[str, list[tuple[str, str]]] = {}

    def emit(self, source_dir: Path) -> Iterator[IRObject]:
        self._source_dir = source_dir
        toml_path = source_dir / self.SOURCE_FILE
        if not toml_path.is_file():
            raise curation_error(
                "curated_toml_not_found",
                f"Canonical-SCB file not found: {toml_path}",
                f"Author {self.SOURCE_FILE} under {source_dir}.",
            )
        self.source_checksums[toml_path.name] = _file_sha256(toml_path)
        for reg in self._load(toml_path):
            yield from self._emit_register(reg)

    def _value_set_id_for(
        self, reg: _CuratedRegister, var: _CuratedVariable
    ) -> int | None:
        if var.value_set is None:
            return None
        return self._ensure_value_set(self._load_codes(var.value_set))

    def _load_codes(self, name: str) -> list[tuple[str, str]]:
        if name in self._codes_cache:
            return self._codes_cache[name]
        assert self._source_dir is not None
        csv_path = self._source_dir / f"{name}.csv"
        if not csv_path.is_file():
            raise curation_error(
                "curated_value_set_missing",
                f"Value-set code list not found: {csv_path}",
                f"Author {name}.csv (header `code,label`) under {self._source_dir}.",
            )
        pairs: list[tuple[str, str]] = []
        # utf-8-sig strips an Excel BOM so the first code never becomes "﻿…".
        label_of: dict[str, str] = {}
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            # Committed curation input → fail fast, not silently. A wrong delimiter
            # (`code;label` → one column) or a missing header would otherwise let
            # every row drop and yield an empty (= no) value set on a column that
            # must be categorical. Require the exact `code,label` header.
            if header is None or [c.strip().lower() for c in header[:2]] != [
                "code",
                "label",
            ]:
                raise curation_error(
                    "curated_value_set_invalid",
                    f"{csv_path.name}: expected a `code,label` header, got {header!r}.",
                    "Author the file comma-separated with a `code,label` first row.",
                )
            for lineno, row in enumerate(reader, start=2):
                if not any(cell.strip() for cell in row):
                    continue  # tolerate blank lines (e.g. a trailing newline)
                if len(row) < 2 or not row[0].strip():
                    raise curation_error(
                        "curated_value_set_invalid",
                        f"{csv_path.name}:{lineno}: malformed row {row!r} "
                        "(need a non-empty `code,label` pair).",
                        "Each non-blank row must be a comma-separated code,label pair.",
                    )
                code, label = row[0].strip(), row[1].strip()
                # Dedup + reject a code that maps to two labels, mirroring the SOS
                # value-set contract (a code is a single key into one label): a
                # duplicate would inflate the member_hash and break content-sharing
                # with an identical SCB/SOS set, and a conflicting label is a data bug.
                if code in label_of:
                    if label_of[code] != label:
                        raise curation_error(
                            "curated_value_set_invalid",
                            f"{csv_path.name}: code {code!r} maps to two labels "
                            f"({label_of[code]!r} vs {label!r}).",
                            "Each code must map to exactly one label.",
                        )
                    continue
                label_of[code] = label
                pairs.append((code, label))
        if not pairs:
            raise curation_error(
                "curated_value_set_invalid",
                f"{csv_path.name}: no code,label rows found.",
                "A value-set CSV must list at least one code.",
            )
        self.source_checksums[csv_path.name] = _file_sha256(csv_path)
        self._codes_cache[name] = pairs
        return pairs

    def _ensure_value_set(self, codes: list[tuple[str, str]]) -> int | None:
        """Content-addressed value-set write (mirrors sos._ensure_value_set):
        `value_code` dedups on (code, label), `value_set` on member_hash; both keep
        AUTOINCREMENT ids, provider-shared and unbanded. Returns the shared
        value_set_id, or None for an empty list."""
        if not codes:
            return None
        conn = self._conn
        member_hash = _value_set_hash(codes)
        cached = self._set_id_by_hash.get(member_hash)
        if cached is not None:
            return cached
        code_id_of: dict[tuple[str, str], int] = {}
        for code, label in codes:
            conn.execute(
                "INSERT OR IGNORE INTO value_code (code, label) VALUES (?, ?)",
                (code, label),
            )
            code_id_of[(code, label)] = conn.execute(
                "SELECT code_id FROM value_code WHERE code = ? AND label = ?",
                (code, label),
            ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO value_set (member_hash) VALUES (?)", (member_hash,)
        )
        set_id = conn.execute(
            "SELECT value_set_id FROM value_set WHERE member_hash = ?", (member_hash,)
        ).fetchone()[0]
        for code, label in codes:
            conn.execute(
                "INSERT OR IGNORE INTO value_set_member (value_set_id, code_id) "
                "VALUES (?, ?)",
                (set_id, code_id_of[(code, label)]),
            )
        self._set_id_by_hash[member_hash] = set_id
        return set_id
