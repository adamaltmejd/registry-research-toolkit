"""A steward's authored delivery spec: `scope.toml` plus one file per series.

```
stewards/<id>/
  steward.json            branding, read by the Rust runtime
  scope.toml              census guard, table dispositions, census-holding
                          routing, flavor register definitions
  series/<series>.toml    one delivery series: its register, its tables and
                          its curated column decisions
```

A series is the unit a steward delivers: one register's tables across years.
Membership in a series is the only authority for which tables share decisions;
nothing groups tables by name. The steward generator
(`input_data/<steward>/build_catalog.py`) is the only reader and the only
producer of `inventory.toml`, so the rules below are checked when it loads the
spec. The inventory contract (`inventory.py`) does not change: series exist
only here and in the generator.

Rules, each refused with a located `RegMetaError`:

- `steward_series_register_mismatch`: a series names one register. A variant
  outside that register, a table `variants` set that is not a subset of the
  series' variants, or a curated mapping onto a variant one of its tables does
  not admit, refuses. The generator maps a table only onto its own admissible
  variants, so its auto-mapped columns stay inside the register by construction.
  A series boundary is a documented break, never a change of variant set: a
  table whose variant set differs carries its own `variants`.
- `steward_table_disposition_invalid`: every census table is in exactly one
  series or one scope disposition (lookup, exclude, retain_unknown).
- `steward_spec_invalid`: everything else, including a column decision naming
  a table outside its series and two decisions for one column of one table.
  Decisions select whole tables, never part of an edition.

`hold.follow_up` must parse as an issue reference. The build never reads the
issue's state: that would make it depend on the network.
"""

from __future__ import annotations

import hashlib
import re
import tomllib
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from reg_core_py import parse_fqid

from .errors import EXIT_CONFIG, RegMetaError
from .holdings_policy import UndatedHolding, retention_row_guards
from .inventory import ColumnMapping, EditionRange, edition_bounds
from .slug_grammar import validate_slug

if TYPE_CHECKING:
    from pathlib import Path

SPEC_INVALID = "steward_spec_invalid"
SERIES_REGISTER_MISMATCH = "steward_series_register_mismatch"
TABLE_DISPOSITION_INVALID = "steward_table_disposition_invalid"

SCOPE_FILE = "scope.toml"
SERIES_DIR = "series"
YEAR_INDEPENDENT_EDITION = "_default"

# How a series spells a bare-year table edition. SCB's school registers deliver
# school years: a table named by one year holds the school year that year
# anchors, `spring` (grades set in the spring: year Y is LA<Y-1>), `autumn` (an
# October snapshot: Y is LA<Y>), or one term, `vt` / `ht` (VT<Y> / HT<Y>). The
# anchor is curated against the register's documentation, never inferred.
EditionSpelling = Literal["annual", "spring", "autumn", "vt", "ht"]

_ISSUE_REFERENCE = re.compile(r"^(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#[1-9][0-9]*$")


def _register_coordinate(value: str) -> str:
    if parse_fqid(value).kind != "register":
        raise ValueError("policy registers must be register FQIDs")
    return value


def _provider_slug(value: str) -> str:
    validate_slug(value, "provider")
    return value


def _literal_name(value: str) -> str:
    if not value or value != value.strip():
        raise ValueError("names must be non-empty and carry no surrounding space")
    return value


def _reason(value: str) -> str:
    if not value.strip():
        raise ValueError("reasons must be nonblank")
    return value


ProviderSlug = Annotated[str, AfterValidator(_provider_slug)]
RegisterCoordinate = Annotated[str, AfterValidator(_register_coordinate)]
VariantCoordinate = Annotated[str, AfterValidator(ColumnMapping._check_variant_coord)]
LiteralName = Annotated[str, AfterValidator(_literal_name)]
Reason = Annotated[str, AfterValidator(_reason)]
# An authored edition kept in its TOML shape (a year int, a period token, a
# `{ from, to }` range or a list of those), because the generator emits it
# verbatim: `1990` and `"1990"` are different inventory bytes.
RawEdition = int | str | dict[str, int | str] | list[int | str | dict[str, int | str]]


def edition_bounds_of(edition: RawEdition) -> tuple[tuple[str, str], ...]:
    """Inclusive ISO bounds of an authored edition; `ValueError` if malformed."""

    def segment(value: object) -> str | EditionRange:
        if isinstance(value, dict):
            return EditionRange.model_validate(value)
        return str(value)

    if isinstance(edition, list):
        return edition_bounds(tuple(segment(value) for value in edition))
    return edition_bounds(segment(edition))


class _SpecModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _unique(values: list[str], label: str) -> list[str]:
    if len(set(values)) != len(values):
        raise ValueError(f"duplicate {label}")
    return values


# --- scope.toml ----------------------------------------------------------------


class FlavorDefinition(_SpecModel):
    """A steward-owned register variant and the census content it is built from.

    `holding` (a census `Category/Detail` key) and `tables` select the census
    columns that become the variant's catalog variables. That selection is
    catalog content, not delivery membership: a superseded table excluded from
    the holdings still documents its columns. `keep_columns` are Källa-sourced
    columns the flavor keeps as the steward's own variables; `sensitive_columns`
    are flagged `is_sensitive`. A `year_independent` variant's states carry no
    calendar window.
    """

    holding: str = Field(min_length=1)
    provider: ProviderSlug = Field(min_length=1)
    provider_name: str = Field(min_length=1)
    register_key: str = Field(alias="register", min_length=1)
    register_name: str = Field(min_length=1)
    variant: str = Field(min_length=1)
    variant_slug: str = Field(min_length=1)
    variant_name: str = Field(min_length=1)
    tables: list[str] | None = None
    keep_columns: list[str] = Field(default_factory=list)
    sensitive_columns: list[str] = Field(default_factory=list)
    period_scope: Literal["intervals", "year_independent"] = "intervals"

    @property
    def coordinate(self) -> str:
        return f"{self.provider}/{self.register_key}/{self.variant_slug}"

    @model_validator(mode="after")
    def _tables(self) -> FlavorDefinition:
        ColumnMapping._check_variant_coord(self.coordinate)
        if self.tables is not None and (
            not self.tables
            or any(not table.strip() for table in self.tables)
            or len(set(self.tables)) != len(self.tables)
        ):
            raise ValueError("flavor tables must be nonblank and unique")
        for names in (self.keep_columns, self.sensitive_columns):
            if any(not name.strip() for name in names) or len(set(names)) != len(names):
                raise ValueError("flavor column lists must be nonblank and unique")
        return self


class HoldingRoute(_SpecModel):
    """How the enrichment stages treat one census holding (`Category`/`Detail`).

    `mapped`: its content is a global register's, so its columns are scanned
    against the registers its series map to (plus `graft`). `flavor`: steward
    content, scanned only against `graft`. `unmapped`: retained without a
    verified catalog owner. A holding with no route is residue, and one whose
    tables are all lookups is a lookup. Table membership is never decided here.
    """

    category: str = Field(min_length=1)
    detail: str
    status: Literal["mapped", "flavor", "unmapped"]
    note: str | None = None
    graft: RegisterCoordinate | None = None

    @model_validator(mode="after")
    def _shape(self) -> HoldingRoute:
        if self.status == "unmapped" and self.graft is not None:
            raise ValueError("unmapped holdings cannot name a graft")
        return self


class TableDisposition(_SpecModel):
    """A census table that is not a series member: a lookup or an exclusion."""

    table: LiteralName
    reason: Reason


class Scope(_SpecModel):
    census_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    non_catalog_categories: dict[str, str] = Field(default_factory=dict)
    flavor_registers: list[RegisterCoordinate] = Field(default_factory=list)
    holdings: list[HoldingRoute] = Field(alias="holding", default_factory=list)
    lookups: list[TableDisposition] = Field(alias="lookup", default_factory=list)
    excludes: list[TableDisposition] = Field(alias="exclude", default_factory=list)
    retain_unknown: list[UndatedHolding] = Field(default_factory=list)
    flavors: list[FlavorDefinition] = Field(alias="flavor", default_factory=list)

    @field_validator("non_catalog_categories")
    @classmethod
    def _nonblank_categories(cls, value: dict[str, str]) -> dict[str, str]:
        if any(not key.strip() or not reason.strip() for key, reason in value.items()):
            raise ValueError("non-catalog categories and reasons must be nonblank")
        return value

    @model_validator(mode="after")
    def _unique_entries(self) -> Scope:
        _unique(self.flavor_registers, "flavor register")
        _unique([f"{h.category}/{h.detail}" for h in self.holdings], "holding route")
        for label, entries in (
            ("lookup", [entry.table for entry in self.lookups]),
            ("exclude", [entry.table for entry in self.excludes]),
            ("retain_unknown", [entry.table for entry in self.retain_unknown]),
        ):
            _unique(entries, f"{label} table")
        _unique(
            [f"{f.provider}/{f.register_key}/{f.variant}" for f in self.flavors],
            "flavor coordinate",
        )
        _unique([f.coordinate for f in self.flavors], "flavor variant slug")
        provider_names: dict[str, str] = {}
        register_names: dict[tuple[str, str], str] = {}
        for entry in self.flavors:
            if (
                provider_names.setdefault(entry.provider, entry.provider_name)
                != entry.provider_name
            ):
                raise ValueError(f"conflicting provider name {entry.provider!r}")
            key = entry.provider, entry.register_key
            if (
                register_names.setdefault(key, entry.register_name)
                != entry.register_name
            ):
                raise ValueError(f"conflicting register name {key!r}")
        return self


# --- series/<id>.toml ------------------------------------------------------------


class SeriesTable(_SpecModel):
    """One delivered table: an explicit edition, or a year-independent scope.

    `variants`, when present, is the table's own admissible variant set, a
    nonempty subset of the series' variants; absent, the table takes the
    series set. A change of variant set is not a series boundary.
    """

    table: LiteralName
    variants: list[VariantCoordinate] | None = None
    edition: RawEdition | None = None
    period_scope: Literal["year_independent"] | None = None
    partition: str | None = None

    @field_validator("partition")
    @classmethod
    def _partition(cls, value: str | None) -> str | None:
        if value is not None:
            validate_slug(value, "partition")
        return value

    @model_validator(mode="after")
    def _one_scope(self) -> SeriesTable:
        if self.variants is not None:
            _unique(self.variants, "table variant")
            if not self.variants:
                raise ValueError("table variants, when given, must be nonempty")
        if (self.edition is None) == (self.period_scope is None):
            raise ValueError(
                "a table states exactly one of `edition` and "
                '`period_scope = "year_independent"`'
            )
        if self.edition is not None:
            if self.edition == YEAR_INDEPENDENT_EDITION:
                raise ValueError(
                    'a year-independent table states `period_scope = "year_independent"`'
                )
            edition_bounds_of(self.edition)
        return self

    @property
    def inventory_edition(self) -> RawEdition:
        """The inventory's whole-edition spelling: `_default` when year-independent."""
        return YEAR_INDEPENDENT_EDITION if self.edition is None else self.edition


class Hold(_SpecModel):
    """An interim unmap: the column waits on a named follow-up issue."""

    follow_up: str
    reason: Reason

    @field_validator("follow_up")
    @classmethod
    def _issue(cls, value: str) -> str:
        if not _ISSUE_REFERENCE.fullmatch(value):
            raise ValueError(
                f"follow_up must be an issue reference (#123 or owner/repo#123); got {value!r}"
            )
        return value


class SeriesColumn(_SpecModel):
    """One curated decision about a column, for whole tables of its series.

    Exactly one of: `mappings` (a reviewed representation, with `reason`, and
    `select_owner` to pick one documented owner among several), `unmap` (a
    permanent reason), `hold` (interim, with a follow-up), `storage_header`
    (the header SWECOV storage holds this curated column under) or
    `graft_onto` (with `reason`: the variant the grafts stage places this
    column on as a gap-fill candidate, a column the catalog has no variable
    for yet). `tables` narrows the decision to whole tables of the series; it
    defaults to all.

    `storage_header` and `graft_onto` never reach the inventory, so they sit
    beside a column's one mapping, unmap or hold decision. A `graft_onto`
    column may live only in the holding's excluded tables: its placement is a
    holding fact, not a table one.
    """

    column: LiteralName
    tables: list[LiteralName] | None = None
    mappings: list[ColumnMapping] | None = None
    reason: Reason | None = None
    select_owner: bool = False
    unmap: Reason | None = None
    hold: Hold | None = None
    storage_header: LiteralName | None = None
    graft_onto: VariantCoordinate | None = None

    @model_validator(mode="after")
    def _one_decision(self) -> SeriesColumn:
        kinds = [
            name
            for name in ("mappings", "unmap", "hold", "storage_header", "graft_onto")
            if getattr(self, name) is not None
        ]
        if len(kinds) != 1:
            raise ValueError(
                "a column decision states exactly one of mappings, unmap, hold, "
                f"storage_header and graft_onto; got {kinds or 'none'}"
            )
        if self.graft_onto is not None:
            if self.reason is None or self.select_owner:
                raise ValueError("a graft_onto decision states a reason only")
            return self
        if self.tables is not None and (
            not self.tables or len(set(self.tables)) != len(self.tables)
        ):
            raise ValueError("tables must be a nonempty list of unique tables")
        if self.mappings is not None:
            if not self.mappings or self.reason is None:
                raise ValueError("a mapping decision names its mappings and a reason")
            if any(mapping.member is not None for mapping in self.mappings):
                raise ValueError("a curated mapping cannot name a wide-pivot member")
            _unique(
                [mapping.register_variant for mapping in self.mappings],
                "mapping register_variant",
            )
        elif self.reason is not None or self.select_owner:
            raise ValueError("reason and select_owner belong to a mapping decision")
        return self

    @property
    def unmapped_reason(self) -> str | None:
        """The inventory `unmapped_reason` an unmap or a hold emits."""
        if self.hold is not None:
            return self.hold.reason
        return self.unmap


class Series(_SpecModel):
    id: str
    # Required whenever the series admits a variant. A series of tables with no
    # catalog owner (every column unmapped or held) names none, since no
    # register is established for it.
    register_coord: RegisterCoordinate | None = Field(default=None, alias="register")
    variants: list[VariantCoordinate] = Field(default_factory=list)
    edition_spelling: EditionSpelling = "annual"
    tables: list[SeriesTable] = Field(alias="table", min_length=1)
    columns: list[SeriesColumn] = Field(alias="column", default_factory=list)

    @field_validator("id")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "series")
        return value

    @model_validator(mode="after")
    def _members(self) -> Series:
        _unique(self.variants, "variant")
        _unique([table.table for table in self.tables], "table")
        if self.variants and self.register_coord is None:
            raise ValueError("a series that admits variants names its register")
        return self

    def table_variants(self) -> dict[str, list[str]]:
        """Table → the variants admissible for it (its own set, else the series')."""
        return {
            table.table: table.variants
            if table.variants is not None
            else list(self.variants)
            for table in self.tables
        }

    def decisions(self) -> dict[tuple[str, str], SeriesColumn]:
        """`(table, column)` → its one mapping, unmap or hold decision."""
        result: dict[tuple[str, str], SeriesColumn] = {}
        every = [table.table for table in self.tables]
        for entry in self.columns:
            if entry.storage_header is None and entry.graft_onto is None:
                for table in entry.tables or every:
                    result[table, entry.column] = entry
        return result

    def graft_targets(self) -> dict[str, str]:
        """Gap-fill column → the variant the grafts stage places it on."""
        return {
            entry.column: entry.graft_onto
            for entry in self.columns
            if entry.graft_onto is not None
        }

    def storage_headers(self) -> dict[str, str]:
        """Curated column → the header SWECOV storage holds it under."""
        return {
            entry.column: entry.storage_header
            for entry in self.columns
            if entry.storage_header is not None
        }


# --- loading -------------------------------------------------------------------


class StewardSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    directory: str
    scope: Scope
    series: tuple[Series, ...]

    def series_of(self) -> dict[str, Series]:
        """Table → its series."""
        return {
            table.table: series for series in self.series for table in series.tables
        }

    def holding_routes(self) -> dict[tuple[str, str], HoldingRoute]:
        return {(route.category, route.detail): route for route in self.scope.holdings}

    def dispositions(self) -> dict[str, list[str]]:
        """Table → every place the spec disposes of it (series or scope entry)."""
        places: dict[str, list[str]] = {}
        for series in self.series:
            for table in series.tables:
                places.setdefault(table.table, []).append(
                    f"{SERIES_DIR}/{series.id}.toml"
                )
        for label, entries in (
            ("lookup", self.scope.lookups),
            ("exclude", self.scope.excludes),
            ("retain_unknown", self.scope.retain_unknown),
        ):
            for entry in entries:
                places.setdefault(entry.table, []).append(f"{SCOPE_FILE} [[{label}]]")
        return places


def _spec_error(code: str, message: str) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code=code,
        error_class="configuration",
        message=message,
        remediation=(
            "Edit the steward spec (scope.toml and series/*.toml) at the place the "
            "message names; see reg_meta_build/DESIGN.md → Steward spec."
        ),
    )


def _read(path: Path) -> dict:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise _spec_error(SPEC_INVALID, f"{path}: unreadable: {exc}") from exc


def _validated[T: BaseModel](model: type[T], path: Path, raw: dict) -> T:
    try:
        return model.model_validate(raw)
    except (ValidationError, ValueError) as exc:
        raise _spec_error(SPEC_INVALID, f"{path}: {exc}") from exc


def _check_series(path: Path, series: Series) -> None:
    if path.stem != series.id:
        raise _spec_error(
            SPEC_INVALID, f"{path}: series id {series.id!r} must equal the file name"
        )
    every = [table.table for table in series.tables]
    tables = ", ".join(every)
    for variant in series.variants:
        register = "/".join(variant.split("/")[:2])
        if register != series.register_coord:
            raise _spec_error(
                SERIES_REGISTER_MISMATCH,
                f"{path}: series {series.id!r} variants: {variant} is not of the "
                f"series register {series.register_coord}; registers found: "
                f"{series.register_coord}, {register} across its tables {tables}",
            )
    for index, table in enumerate(series.tables, 1):
        stray = sorted(set(table.variants or ()) - set(series.variants))
        if stray:
            raise _spec_error(
                SERIES_REGISTER_MISMATCH,
                f"{path}: series {series.id!r} table[{index}] ({table.table}) "
                f"variants: {', '.join(stray)} not among the series variants "
                f"{', '.join(series.variants) or '(none)'}",
            )
    admissible = series.table_variants()
    seen: dict[tuple[str, str], int] = {}
    headers: dict[str, int] = {}
    grafts: dict[str, int] = {}
    for index, entry in enumerate(series.columns, 1):
        where = f"{path}: series {series.id!r} column[{index}] ({entry.column})"
        outside = sorted(set(entry.tables or ()) - set(every))
        if outside:
            raise _spec_error(
                SPEC_INVALID,
                f"{where}: tables outside the series: {', '.join(outside)}",
            )
        for mapping in entry.mappings or ():
            refusing = [
                table
                for table in entry.tables or every
                if mapping.register_variant not in admissible[table]
            ]
            if refusing:
                register = "/".join(mapping.register_variant.split("/")[:2])
                registers = ", ".join(
                    sorted({r for r in (series.register_coord, register) if r})
                )
                raise _spec_error(
                    SERIES_REGISTER_MISMATCH,
                    f"{where}: mapping onto {mapping.register_variant}, a variant "
                    f"tables {', '.join(refusing)} do not admit; registers found: "
                    f"{registers}",
                )
        if entry.graft_onto is not None:
            if entry.graft_onto not in series.variants:
                raise _spec_error(
                    SERIES_REGISTER_MISMATCH,
                    f"{where}: graft_onto {entry.graft_onto} is not among the "
                    f"series variants {', '.join(series.variants) or '(none)'}",
                )
            if entry.column in grafts:
                raise _spec_error(
                    SPEC_INVALID,
                    f"{where}: second graft_onto for the column "
                    f"(first at column[{grafts[entry.column]}])",
                )
            grafts[entry.column] = index
            continue
        if entry.storage_header is not None:
            if entry.column in headers:
                raise _spec_error(
                    SPEC_INVALID,
                    f"{where}: second storage_header for the column "
                    f"(first at column[{headers[entry.column]}])",
                )
            headers[entry.column] = index
            continue
        for table in entry.tables or every:
            if (table, entry.column) in seen:
                raise _spec_error(
                    SPEC_INVALID,
                    f"{where}: second decision for table {table} (first at "
                    f"column[{seen[table, entry.column]}]); a column of a table "
                    "takes one decision",
                )
            seen[table, entry.column] = index


def load_steward_spec(directory: Path) -> StewardSpec:
    """Read and check `scope.toml` and every `series/*.toml` under `directory`."""
    scope_path = directory / SCOPE_FILE
    scope = _validated(Scope, scope_path, _read(scope_path))
    series: list[Series] = []
    for path in sorted((directory / SERIES_DIR).glob("*.toml")):
        entry = _validated(Series, path, _read(path))
        _check_series(path, entry)
        series.append(entry)
    spec = StewardSpec(directory=str(directory), scope=scope, series=tuple(series))
    for table, places in sorted(spec.dispositions().items()):
        if len(places) > 1:
            raise _spec_error(
                TABLE_DISPOSITION_INVALID,
                f"{directory}: table {table} has {len(places)} dispositions "
                f"({'; '.join(places)}); a table is in one series or one scope entry",
            )
    return spec


def check_census(spec: StewardSpec, census_path: Path, census_tables: set[str]) -> None:
    """Hold the spec to the census it was reviewed against.

    The census bytes must match `census_sha256`, each retained table's rows must
    match its row guard, and every census table needs exactly one disposition.
    A spec table the census no longer has is stale, reported by the generator.
    """
    source = census_path.read_bytes()
    actual = hashlib.sha256(source).hexdigest()
    if actual != spec.scope.census_sha256:
        raise _spec_error(
            SPEC_INVALID,
            f"{spec.directory}/{SCOPE_FILE}: census_sha256 {spec.scope.census_sha256} "
            f"does not match {census_path.name} ({actual}); review the decisions "
            "against the new census, then update census_sha256",
        )
    try:
        retention_row_guards(
            source,
            tuple(spec.scope.retain_unknown),
            {entry.table for entry in spec.scope.excludes},
        )
    except ValueError as exc:
        raise _spec_error(
            SPEC_INVALID, f"{spec.directory}/{SCOPE_FILE} [[retain_unknown]]: {exc}"
        ) from exc
    disposed = spec.dispositions()
    missing = sorted(census_tables - set(disposed))
    if missing:
        raise _spec_error(
            TABLE_DISPOSITION_INVALID,
            f"{spec.directory}: {len(missing)} census table(s) in no series and "
            f"with no scope disposition: {', '.join(missing)}",
        )
