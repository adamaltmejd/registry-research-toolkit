"""Steward delivery inventory: the physical delivery-topology contract.

A `project_data.json` source is a LOGICAL selection (register variant, variable,
period). What a steward actually delivers is separate data: exact tables, one
explicit physical edition per table, and literal physical column names. This
module is that contract — the typed models plus `load_inventory`, the structural
validator maintainers (and, later, the inventory generator) run before an
inventory is committed. REFACTOR_SPEC.md §12 is the decision text; the format is
documented in DESIGN.md → Steward delivery inventory.

Deliberately reg_schema-free: the contract needs only reg_meta's own period
grammar (`fqid.period_token_to_bounds`) and FQID parser, so the `reg_meta →
reg_schema` dependency §12 sanctions for the materializer is not taken here.
This module holds no DB access — it is pure domain code over an authored file.
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from .errors import EXIT_CONFIG, RegMetaError
from .fqid import (
    DEFAULT_VARIANT_SLUG,
    Fqid,
    FqidKind,
    period_token_to_bounds,
    validate_slug,
)

if TYPE_CHECKING:
    from pathlib import Path


class _InventoryModel(BaseModel):
    """Frozen Pydantic base for the inventory contract — same shape as
    `_CatalogModel` (frozen, extra-forbid, alias-aware). Separate base because
    the inventory is authored input, not a catalog return row: its aliases exist
    to keep the TOML spelling singular (`[[table]]`, `[[table.column]]`,
    `[[table.column.mapping]]`) while the Python attrs stay plural collections.
    `extra="forbid"` makes a misspelled key a loud error instead of a silently
    ignored curation."""

    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="forbid",
        serialize_by_alias=True,
    )


class EditionRange(_InventoryModel):
    """The `{ from = ..., to = ... }` finite range form of a table edition.

    Endpoints are period tokens (a bare TOML year int is normalized to its token
    string on the way in). `from` is a Python keyword, so the attr is `from_`
    with a `"from"` alias — same shape reg_schema's `PeriodRange` uses for a
    project period; the two converge when the materializer lane takes the
    `reg_meta → reg_schema` dependency."""

    from_: str = Field(alias="from")
    to: str

    @field_validator("from_", "to", mode="before")
    @classmethod
    def _year_int_to_token(cls, value: object) -> object:
        return _year_int_to_token(value)


# One contiguous piece of a table edition: a period token or an explicit range.
EditionSegment = str | EditionRange
# A table's edition: one segment, or a finite list of segments for a table that
# carries an interrupted series. NEVER `"_default"` and never an unbounded
# "all periods" sentinel (§12) — an edition is always finite and explicit.
Edition = EditionSegment | tuple[EditionSegment, ...]


def _year_int_to_token(value: object) -> object:
    """Canonicalize a bare TOML year int (`edition = 2019`) to its period-token
    string, so downstream sees ONE edition spelling. Non-ints pass through for
    the grammar check; a bool is an int subclass and must not become `"0001"`."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return f"{value:04d}"
    return value


def _segment_bounds(segment: EditionSegment) -> tuple[str, str]:
    if isinstance(segment, EditionRange):
        lo, _ = period_token_to_bounds(segment.from_)
        _, hi = period_token_to_bounds(segment.to)
        if lo > hi:
            raise ValueError(
                f"edition range 'from' is after 'to': {segment.from_!r}..{segment.to!r}"
            )
        return lo, hi
    if segment == DEFAULT_VARIANT_SLUG:
        raise ValueError(
            f"edition must be one explicit finite period, never {segment!r} "
            "(a table with no edition encoded in its name still needs a curated "
            "edition — see REFACTOR_SPEC.md §12)"
        )
    return period_token_to_bounds(segment)


def edition_bounds(edition: Edition) -> tuple[tuple[str, str], ...]:
    """Expand a table edition into its inclusive ISO `(lo, hi)` intervals.

    The finite-period expansion the coverage/materializer lane intersects
    against a requested period. Reuses the shared period grammar
    (`fqid.period_token_to_bounds`), so an inventory edition and a project
    period expand identically. Raises `ValueError` (`FqidError` for a malformed
    token) — the inventory validator surfaces it as `inventory_invalid`."""
    segments = edition if isinstance(edition, tuple) else (edition,)
    if not segments:
        raise ValueError("edition list must not be empty")
    bounds: list[tuple[str, str]] = []
    previous_hi: str | None = None
    for segment in segments:
        lo, hi = _segment_bounds(segment)
        if previous_hi is not None and lo <= previous_hi:
            raise ValueError(
                f"edition segments must be sorted ascending and non-overlapping; "
                f"segment starting {lo} follows a segment ending {previous_hi}"
            )
        bounds.append((lo, hi))
        previous_hi = hi
    return tuple(bounds)


class ColumnMapping(_InventoryModel):
    """One semantic mapping of a physical column: which logical coordinate this
    column IS.

    `register_variant` is the 3-part variant coordinate
    (`<provider>/<register>/<variant>`, `_default` allowed for a single-table
    register), `variable` the 3-segment binding FQID, and `representation` the
    canonical reg_meta `variable_alias.delivery_column_name` this column
    corresponds to — `None` when the concept has a single representation. The
    representation is a join discriminator, not an output substitute (§12)."""

    register_variant: str
    variable: Fqid
    representation: str | None = Field(default=None, min_length=1)

    @field_validator("register_variant")
    @classmethod
    def _check_variant_coord(cls, value: str) -> str:
        parts = value.split("/")
        if len(parts) != 3:
            raise ValueError(
                "register_variant must be a 3-part variant coordinate "
                f"<provider>/<register>/<variant>; got {value!r}"
            )
        provider, register, variant = parts
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variant, "register_variant", allow_default=True)
        return value

    @field_validator("variable")
    @classmethod
    def _check_binding_fqid(cls, value: Fqid) -> Fqid:
        if value.kind is not FqidKind.VARIABLE_BINDING:
            raise ValueError(
                "variable must be a 3-segment binding FQID "
                f"<provider>/<register>/<variable>; got {value!s} "
                f"({value.kind.value})"
            )
        return value

    @model_validator(mode="after")
    def _check_prefix_match(self) -> ColumnMapping:
        """The variable's `provider/register` prefix must equal the variant
        coordinate's — same cross-field rule reg_schema's structural validator
        enforces for a project source's bindings. A mapping that crosses
        registers is an authoring slip, not a legal combined table."""
        prefix = tuple(self.register_variant.split("/")[:2])
        if (self.variable.provider, self.variable.register) != prefix:
            raise ValueError(
                f"variable {self.variable!s} does not belong to register_variant "
                f"{self.register_variant!r} (prefix mismatch)"
            )
        return self


class InventoryColumn(_InventoryModel):
    """One literal, case-preserving physical column of a delivered table.

    `mappings` may be empty: an unresolved column is still inventoried, so it
    stays in the coverage denominator without being admitted or orderable (§12).
    Several mappings let one physical column serve several register variants
    (the combined Utrikeshandel table)."""

    name: str = Field(min_length=1)
    mappings: tuple[ColumnMapping, ...] = Field(default=(), alias="mapping")


class InventoryTable(_InventoryModel):
    """One delivered table: an opaque exact identifier, ONE explicit physical
    edition, and its physical columns.

    `id` is verbatim and opaque — an exact delivery filename
    (`LISA_Individ_2019.csv`) or a schema-qualified SQL table
    (`dbo.SoS_Patientregister`). It is never parsed for meaning here; a table
    whose name carries no period still requires an explicit curated
    `edition`."""

    id: str = Field(min_length=1)
    edition: Edition
    columns: tuple[InventoryColumn, ...] = Field(alias="column")

    @field_validator("edition", mode="before")
    @classmethod
    def _year_ints_to_tokens(cls, value: object) -> object:
        if isinstance(value, list):
            return [_year_int_to_token(item) for item in value]
        return _year_int_to_token(value)

    @field_validator("edition")
    @classmethod
    def _check_finite_edition(cls, value: Edition) -> Edition:
        """Every segment expands through the shared period grammar — so
        `"_default"`, an unbounded sentinel, an inverted range, and an
        out-of-order/overlapping list all fail here, located at `edition`."""
        edition_bounds(value)
        return value

    @field_validator("columns")
    @classmethod
    def _check_columns_present(
        cls, value: tuple[InventoryColumn, ...]
    ) -> tuple[InventoryColumn, ...]:
        """A column-less table states nothing: it can neither be ordered nor
        counted in the coverage denominator, so it is a curation error (an
        unparsed or half-authored table), not a valid holdings statement.

        A field validator rather than `min_length=1`, which reports "at least 1
        item AFTER validation" — that would fire a second, misleading line
        whenever a table's own fields failed for an unrelated reason."""
        if not value:
            raise ValueError(
                "table declares no columns — list every delivered physical "
                "column, including the unresolved ones that carry no mapping"
            )
        return value

    @model_validator(mode="after")
    def _check_unique_columns(self) -> InventoryTable:
        seen: set[str] = set()
        for column in self.columns:
            if column.name in seen:
                raise ValueError(
                    f"duplicate physical column {column.name!r} — declare each "
                    "column once and list all of its mappings under it"
                )
            seen.add(column.name)
        return self


class DeliveryInventory(_InventoryModel):
    """A steward's full delivery inventory — the public, version-controlled
    source of truth compiled into the released steward artifact.

    `steward` names the deployment this inventory belongs to (a project's
    `steward` provenance must match it before ordering); `version` is the
    contract version, bumped when the format changes (pre-v1: changed, not
    migrated)."""

    version: Literal[1]
    steward: str
    tables: tuple[InventoryTable, ...] = Field(alias="table")

    @field_validator("steward")
    @classmethod
    def _check_steward(cls, value: str) -> str:
        validate_slug(value, "steward")
        return value

    @field_validator("tables")
    @classmethod
    def _check_tables_present(
        cls, value: tuple[InventoryTable, ...]
    ) -> tuple[InventoryTable, ...]:
        """An inventory with no tables is a curation error (a mis-generated or
        truncated file), not a steward that delivers nothing: this file is the
        authoritative holdings statement, so an empty one must fail loudly
        rather than silently zero out admission, coverage, and browse unions.
        Same `min_length=1` caveat as `InventoryTable.columns`."""
        if not value:
            raise ValueError(
                "inventory declares no tables — an inventory is a steward's "
                "holdings statement, so an empty `table` array is a curation "
                "error, not a delivery topology"
            )
        return value

    @model_validator(mode="after")
    def _check_unique_tables(self) -> DeliveryInventory:
        """Each physical table is declared once: the identifier is exact, and
        one table carries exactly one edition, so a repeated id is an ambiguous
        edition rather than a second table. Several DIFFERENT tables mapping to
        the same logical coordinate stays legal (§12)."""
        seen: set[str] = set()
        for table in self.tables:
            if table.id in seen:
                raise ValueError(
                    f"duplicate table {table.id!r} — a table identifier is exact "
                    "and carries exactly one edition"
                )
            seen.add(table.id)
        return self


def _inventory_error(code: str, message: str, remediation: str) -> RegMetaError:
    """A configuration-class error (EXIT_CONFIG) for the authored inventory
    TOML: a syntax typo or a malformed entry is a config failure with actionable
    remediation, not an internal bug (mirrors `reg_meta_build._curation`'s
    `curation_error` for the curation TOMLs)."""
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code=code,
        error_class="configuration",
        message=message,
        remediation=remediation,
    )


def _error_path(raw: object, loc: tuple[int | str, ...], *, missing: bool) -> str:
    """Render a Pydantic error location as an author-facing path that names the
    offending TABLE and COLUMN (`table['LISA_2019.csv'].column['Kon'].mapping[0]
    .variable`) instead of bare indices — the author edits a TOML file, where an
    index is not a locator.

    The path stops at the deepest key that exists in the authored TOML, so a
    union arm Pydantic appends to the location (`edition.str`,
    `variable.is-instance[Fqid]`) doesn't masquerade as a key the author can
    look for. The one absent key worth naming is a `missing` error's own field.
    """
    node = raw
    parts: list[str] = []
    for index, key in enumerate(loc):
        if isinstance(key, int):
            node = node[key] if isinstance(node, list) and key < len(node) else None
            label = None
            if isinstance(node, dict):
                label = node.get("id") or node.get("name")
            token = f"[{label!r}]" if isinstance(label, str) else f"[{key}]"
            if parts:
                parts[-1] += token
            else:
                parts.append(token)
        else:
            child = node.get(key) if isinstance(node, dict) else None
            if child is None and not (missing and index == len(loc) - 1):
                break
            node = child
            parts.append(str(key))
    return ".".join(parts)


def load_inventory(path: Path) -> DeliveryInventory:
    """Read and structurally validate a steward delivery-inventory TOML file.

    Fail-fast: any malformed table, edition, or mapping raises `RegMetaError`
    (`inventory_toml_unreadable` / `inventory_invalid`, EXIT_CONFIG) naming
    every offending table/column — never a partially parsed inventory. Semantic
    consistency against the reg_meta DB (does each mapping's
    `(register_variant, variable, representation)` resolve?) is a separate
    build/CI gate, not this structural pass."""
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise _inventory_error(
            "inventory_toml_unreadable",
            f"Could not read delivery inventory {path}: {exc}",
            "Fix the TOML syntax (see reg_meta/DESIGN.md → Steward delivery "
            "inventory for the format).",
        ) from exc
    try:
        return DeliveryInventory.model_validate(raw)
    except ValidationError as exc:
        details = "\n".join(
            f"  {_error_path(raw, error['loc'], missing=error['type'] == 'missing')}: "
            f"{error['msg']}"
            for error in exc.errors()
        )
        raise _inventory_error(
            "inventory_invalid",
            f"Invalid delivery inventory {path}:\n{details}",
            "Each `[[table]]` needs an exact `id`, one explicit finite "
            "`edition`, and its literal `[[table.column]]` entries; see "
            "reg_meta/DESIGN.md → Steward delivery inventory.",
        ) from exc
