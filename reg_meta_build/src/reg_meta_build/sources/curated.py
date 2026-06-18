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

Ids are `mint()`ed into the high band `[2^62, 2^63)` (the provider name is the
first `mint` part, so a thin provider never collides with SOS's
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
"""

from __future__ import annotations

import csv
import re
import tomllib
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from reg_meta_build._curation import curation_error
from reg_meta_build.classifications import declared_short_names
from reg_meta_build.db import _file_sha256, _value_set_hash
from reg_meta_build.id import mint, mint_canonical_scb
from reg_meta_build.ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
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
_REGISTER_KEYS = frozenset(
    {"key", "name", "purpose", "valid_from", "valid_to", "variant", "variable"}
)
_VARIANT_KEYS = frozenset({"key", "name", "description"})
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
    }
)


@dataclass(frozen=True)
class _CuratedVariable:
    name: str
    column: str
    definition: str | None
    description: str | None
    data_type: str | None
    measurement_unit: str | None
    is_identifier: bool
    is_sensitive: bool
    valid_from: str | None  # None → inherit the register coverage start
    valid_to: str | None  # None → open-ended (materializer writes the sentinel)
    variants: tuple[str, ...] | None  # None → delivered in every variant
    classification: str | None  # None → unlinked; else an existing catalog short_name
    value_set: str | None  # None → no value set; else a code-list name (canonical-scb)


@dataclass(frozen=True)
class _CuratedVariant:
    key: str
    name: str
    description: str | None
    synthesized: bool


@dataclass(frozen=True)
class _CuratedRegister:
    key: str
    name: str
    purpose: str | None
    valid_from: str
    valid_to: str | None  # None → open-ended; default valid_to for its variables
    variants: tuple[_CuratedVariant, ...]
    variables: tuple[_CuratedVariable, ...]


class CuratedAdapter:
    """Emit IR for a thin curated provider from its `<provider>.toml`.

    `provider` is the seed slug (`'fohm'`, `'fk'`, …); the same string is the
    first `mint()` part and the TOML basename `emit()` reads from `source_dir`.
    """

    def __init__(
        self, provider: str, *, classification_seed_path: Path | None = None
    ) -> None:
        self.provider = provider
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
        # A thin provider has no related edges, fold hints, or coalesce stats.
        self.row_counts: dict[str, int] = {}
        self.source_checksums: dict[str, str] = {}
        self.related_edges: list[tuple[int, int, str]] = []
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

        self._reject_unknown(path, raw, frozenset({"register"}), "top level")
        reg_tables = raw.get("register")
        if not isinstance(reg_tables, list) or not reg_tables:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: expected a non-empty `[[register]]` array.",
                "Declare at least one [[register]] with a key and name.",
            )

        registers: list[_CuratedRegister] = []
        seen_reg_keys: set[str] = set()
        for entry in reg_tables:
            reg = self._load_register(path, entry, seen_reg_keys)
            registers.append(reg)

        # Validate `classification` references against the seed manifest in a
        # single pass once everything is parsed (PROVIDER-AGNOSTIC: a declared
        # but provider-gated short_name passes even in a build that won't seed
        # it — the link just drops at feed time; only an UNDECLARED short_name,
        # i.e. a typo, fails). Resolve the seed only when something references a
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
                            f"(classifications.toml).",
                            "Use an existing classification short_name (e.g. "
                            "'ICD-10-SE', 'ATC') or declare it in "
                            "classifications.toml.",
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
        valid_from = self._req_str(path, entry, "valid_from", f"register {key!r}")
        self._check_iso(path, valid_from, f"register {key!r} valid_from")
        valid_to = self._opt_str(entry, "valid_to")
        if valid_to is not None:
            self._check_iso(path, valid_to, f"register {key!r} valid_to")

        variant_entries = entry.get("variant", [])
        if not isinstance(variant_entries, list):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: register {key!r}: `variant` must be an array.",
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
            variants.append(
                _CuratedVariant(
                    key=vk,
                    name=self._req_str(
                        path, v, "name", f"register {key!r} variant {vk!r}"
                    ),
                    description=self._opt_str(v, "description"),
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
                    synthesized=True,
                )
            )
        variant_keys = {v.key for v in variants}

        var_entries = entry.get("variable", [])
        if not isinstance(var_entries, list) or not var_entries:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: register {key!r}: expected a non-empty "
                "`[[register.variable]]` array.",
                "Declare at least one variable per register.",
            )
        variables: list[_CuratedVariable] = []
        seen_columns: set[str] = set()
        for ve in var_entries:
            var = self._load_variable(path, key, ve, variant_keys, seen_columns)
            variables.append(var)

        return _CuratedRegister(
            key=key,
            name=name,
            purpose=self._opt_str(entry, "purpose"),
            valid_from=valid_from,
            valid_to=valid_to,
            variants=tuple(variants),
            variables=tuple(variables),
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
        self._reject_unknown(
            path, entry, _VARIABLE_KEYS, f"register {reg_key!r} variable {name!r}"
        )
        column = self._req_str(
            path, entry, "column", f"register {reg_key!r} variable {name!r}"
        )
        # The auto-slug derives from `column`; a duplicate would mint a colliding
        # variable id and a non-unique slug, so reject it at load.
        if column in seen_columns:
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: register {reg_key!r}: duplicate column {column!r}.",
                "Each variable's delivery column must be unique within the register.",
            )
        seen_columns.add(column)

        valid_from = self._opt_str(entry, "valid_from")
        if valid_from is not None:
            self._check_iso(path, valid_from, f"{ctx} {name!r} valid_from")
        valid_to = self._opt_str(entry, "valid_to")
        if valid_to is not None:
            self._check_iso(path, valid_to, f"{ctx} {name!r} valid_to")

        variants = entry.get("variants")
        if variants is not None:
            if not isinstance(variants, list) or not all(
                isinstance(x, str) for x in variants
            ):
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: {ctx} {name!r}: `variants` must be a string array.",
                    "List the variant keys this variable is delivered in.",
                )
            if not variants:
                # An empty list passes the isinstance/all checks vacuously but
                # would pin the variable to NO variant — no states, no aliases.
                # Reject it; omitting the key delivers in every variant.
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: {ctx} {name!r}: `variants` must list at least "
                    "one variant key (omit the key to deliver in every variant).",
                    "List the variant keys, or drop the `variants` key entirely.",
                )
            unknown = [x for x in variants if x not in variant_keys]
            if unknown:
                raise curation_error(
                    "curated_toml_invalid",
                    f"{path.name}: {ctx} {name!r}: unknown variant(s) {unknown}.",
                    f"Use declared variant keys: {sorted(variant_keys)}.",
                )
            variants = tuple(variants)

        return _CuratedVariable(
            name=name,
            column=column,
            definition=self._opt_str(entry, "definition"),
            description=self._opt_str(entry, "description"),
            data_type=self._opt_str(entry, "data_type"),
            measurement_unit=self._opt_str(entry, "measurement_unit"),
            is_identifier=self._opt_bool(
                path, entry, "is_identifier", f"{ctx} {name!r}"
            ),
            is_sensitive=self._opt_bool(path, entry, "is_sensitive", f"{ctx} {name!r}"),
            valid_from=valid_from,
            valid_to=valid_to,
            variants=variants,
            classification=self._opt_str(entry, "classification"),
            value_set=self._value_set_field(path, entry, name),
        )

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
        # `bool(...)` on a present non-bool is a footgun — `bool("false")` is True,
        # silently flipping a sensitivity flag. Demand a real TOML boolean.
        value = entry.get(field)
        if value is None:
            return False
        if not isinstance(value, bool):
            raise curation_error(
                "curated_toml_invalid",
                f"{path.name}: {ctx}: `{field}` must be a boolean when present.",
                f"Use a bare true/false for `{field}` (no quotes).",
            )
        return value

    def _check_iso(self, path: Path, value: str, ctx: str) -> None:
        # Regex pins the exact YYYY-MM-DD shape; date.fromisoformat additionally
        # rejects a calendar-impossible date (e.g. 2021-13-01) that the DDL's
        # length/ordering CHECKs would otherwise let through.
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
        register_id = self._mint(self.provider, reg.key)
        self.row_counts[f"{self.provider}:{reg.key}"] = len(reg.variables)
        yield IRRegister(
            register_id=register_id,
            provider=self.provider,
            slug="",  # populate_slugs fills it from fqid_slugs/<provider>.toml
            name=reg.name,
            description=None,  # register table carries `purpose`, not description
            purpose=reg.purpose,
        )

        variant_ids: dict[str, int] = {}
        for variant in reg.variants:
            variant_id = self._mint(self.provider, reg.key, variant.key)
            variant_ids[variant.key] = variant_id
            yield IRVariant(
                register_variant_id=variant_id,
                register_id=register_id,
                slug="",
                name=variant.name,
                description=variant.description,
                synthesized=variant.synthesized,
            )

        all_variant_keys = tuple(v.key for v in reg.variants)
        for var in reg.variables:
            yield from self._emit_variable(
                reg, register_id, variant_ids, all_variant_keys, var
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
        all_variant_keys: tuple[str, ...],
        var: _CuratedVariable,
    ) -> Iterator[IRObject]:
        variable_id = self._mint(self.provider, reg.key, var.column)
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
            provider_key=var.column,  # non-unique join hint; the column is the natural key
            slug="",
            name=var.name,
            definition=var.definition,
            description=var.description,
            measurement_unit=var.measurement_unit,
            is_sensitive=var.is_sensitive,
            is_identifier=var.is_identifier,
            source_register_id=None,
            source_register_text=None,
            source_label=None,
        )

        valid_from = var.valid_from or reg.valid_from
        # A closed register (1997-2010 Pliktverket, a discontinued benefit, …) sets
        # register-level `valid_to`; a per-variable `valid_to` overrides it.
        valid_to = var.valid_to or reg.valid_to
        target_keys = var.variants if var.variants is not None else all_variant_keys
        for vk in target_keys:
            variant_id = variant_ids[vk]
            yield IRVariableState(
                state_id=self._mint(self.provider, reg.key, var.column, vk),
                variable_id=variable_id,
                register_variant_id=variant_id,
                valid_from=valid_from,
                valid_to=valid_to,  # None → materializer writes the open-ended sentinel
                data_type=var.data_type,
                data_length=None,
                delivery_column_name=var.column,
                value_set_id=value_set_id,
                value_set_version_label=None,
            )
            yield IRVariableAlias(
                variable_id=variable_id,
                register_variant_id=variant_id,
                delivery_column_name=var.column,
            )


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
            next(reader, None)  # header
            for row in reader:
                if len(row) < 2 or not row[0].strip():
                    continue
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
