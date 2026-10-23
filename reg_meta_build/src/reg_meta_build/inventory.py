"""Steward delivery inventory: the physical delivery-topology contract.

A `project_data.json` source is a LOGICAL selection (register variant, variable,
period). What a steward actually delivers is separate data: exact tables, one
explicit physical edition per table, and literal physical column names. This
module is that contract — the typed models plus `load_inventory`, the structural
validator maintainers (and, later, the inventory generator) run before an
inventory is committed. Its load-bearing rule is the one-to-one resolution
invariant: every admitted `(register_variant, variable, representation, period)`
cell resolves to exactly one physical `(table, column)` — per the
disjoint-partition arm, per `(cell × partition)` — so the extraction tool never
chooses between sources. reg_meta_build/DESIGN.md → "Holdings resolution
invariants" is the decision text; the format is documented in
reg_meta_build/DESIGN.md → "Inventory TOML".

The contract needs only reg-core's period grammar and FQID parser
(`reg_core_py`).
This module holds no DB access — it is pure domain code over an authored file.
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING, Literal, NamedTuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    ValidationError,
    field_validator,
    model_validator,
)
from reg_core_py import (
    fold_identity,
    merge,
    overlap,
    parse_fqid,
    period_bounds,
    period_token_for_bounds,
    render,
)

from .errors import EXIT_CONFIG, RegMetaError
from .slug_grammar import DEFAULT_VARIANT_SLUG, validate_slug

if TYPE_CHECKING:
    from pathlib import Path

# Inventory refusals, one code per rule family (as `curation_tree` does for the
# curation TOMLs) so a curator and a test can tell an empty table from a
# duplicated one or a cell two tables both serve. `inventory_invalid` stays the
# code for field grammar Pydantic or a shared grammar reports (wrong type,
# missing or unknown key, contract version, slug and FQID shape).
_INVALID = "inventory_invalid"
_NO_TABLES = "inventory_no_tables"
_DUPLICATE_TABLE = "inventory_duplicate_table"
_TABLE_NO_COLUMNS = "inventory_table_no_columns"
_DUPLICATE_COLUMN = "inventory_duplicate_column"
_DUPLICATE_MAPPING = "inventory_duplicate_mapping"
_UNMAPPED_REASON = "inventory_unmapped_reason_invalid"
_UNACCOUNTED = "inventory_column_unaccounted"
_EDITION = "inventory_edition_invalid"
_PERIOD_SCOPE = "inventory_period_scope_invalid"
_CELL_CONFLICT = "inventory_cell_conflict"
_MEMBER = "inventory_member_invalid"
_PROJECTION = "inventory_projection_invalid"
_PERIOD_COLUMN = "inventory_period_column_invalid"
_CARRIED = "inventory_carried_key_invalid"
# Public: the steward spec refuses a mapping warning that misses every table
# under the same code (#1684).
WINDOW_INVALID = "inventory_window_invalid"


class _InventoryGuardError(ValueError):
    """A hand-written inventory guard's refusal, carrying its rule family's code.

    A `ValueError` so Pydantic collects it as a located field error and so
    `validate_inventory_placements`' direct callers still catch a `ValueError`;
    `load_inventory` reads `code` back off the collected error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


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
    """The `{ from = ..., to = ... }` multi-period form of a table edition.

    Endpoints are period tokens (a bare TOML year int is normalized to its token
    string on the way in). `from` is a Python keyword, so the attr is `from_`
    with a `"from"` alias."""

    from_: str = Field(alias="from")
    to: str

    @field_validator("from_", "to", mode="before")
    @classmethod
    def _year_int_to_token(cls, value: object) -> object:
        return _year_int_to_token(value)


# One token denotes ONE delivered period; an explicit range denotes a
# multi-period file.
EditionSegment = str | EditionRange
# A table's edition: one segment, or a finite list for a multi-period file that
# carries an interrupted series. NEVER `"_default"` and never an unbounded "all
# periods" sentinel — an edition is always finite and explicit.
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
        lo, _ = period_bounds(segment.from_)
        _, hi = period_bounds(segment.to)
        if lo > hi:
            raise ValueError(
                f"edition range 'from' is after 'to': {segment.from_!r}..{segment.to!r}"
            )
        return lo, hi
    if segment == DEFAULT_VARIANT_SLUG:
        raise ValueError(
            f"edition must be one explicit finite period, never {segment!r} "
            "(a table with no edition encoded in its name still needs a curated "
            "edition — see reg_meta_build/DESIGN.md → Holdings curation rules)"
        )
    return period_bounds(segment)


def edition_bounds(edition: Edition) -> tuple[tuple[str, str], ...]:
    """Expand a table edition into its inclusive ISO `(lo, hi)` intervals.

    The finite-period expansion the coverage/materializer lane intersects
    against a requested period. Reuses the shared period grammar
    (`reg_core_py.period_bounds`), so an inventory edition and a project
    period expand identically. Raises `ValueError` (`GrammarError` for a malformed
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


# An inclusive ISO `(lo, hi)` date interval — the currency of every edition,
# overlap, clip and coverage computation. Its algebra (`merge`, `overlap`,
# `render`, ...) is reg-core's, through `reg_core_py`, so an edition, a project
# period and a browse row can never disagree with the reader about bounds or
# spelling.
_Interval = tuple[str, str]


def _representations_conflate(left: str, right: str) -> bool:
    """Explicit representations describe the same cell only when equal."""
    return left == right


def _partitions_separate(left: str | None, right: str | None) -> bool:
    """Do two tables carry DISTINCT partition labels — i.e. are they shards
    of one sub-population split rather than two claims on one cell?

    Only two explicit, different labels separate. `None` on either side does
    not: an unlabelled table states the whole population of its edition, so it
    necessarily overlaps any shard of it. Equal labels do not either — the same
    shard delivered twice is the ordinary supersession conflict."""
    return left is not None and right is not None and left != right


def _partition_hint(left: str | None, right: str | None) -> str:
    """The remediation suffix for a conflict where exactly ONE side is labelled.

    That mix is the diagnostic case: the curator already recognized a
    partition split on one table and left the other unlabelled, so the fix is a
    label rather than a supersession discard. Two unlabelled tables are
    indistinguishable from an uncurated re-delivery, so they get the error's
    standing supersession remediation instead."""
    if (left is None) == (right is None):
        return ""
    return (
        " — only one of these carries a `partition` label; label both when they "
        "are genuinely disjoint shards of one sub-population split"
    )


def _location(table_id: str, column_name: str) -> str:
    """One physical location in the author-facing spelling `_error_path` uses,
    for messages a root validator cannot attach a location to."""
    return f"table[{table_id!r}].column[{column_name!r}]"


def _member_location(table_id: str, representation: str) -> str:
    """A wide-pivot member set as one location: all of a table's columns that map
    one `(variant, variable, representation)` with a member index (#1495)."""
    return f"table[{table_id!r}] member set {representation!r}"


def _representation_label(representation: str) -> str:
    return f"representation {representation!r}"


class ColumnMember(_InventoryModel):
    """A wide-pivot member index (#1495): which member of an N:1 column set this
    physical column is. `DIA1..DIA30` each map the base representation `DIA` with
    `{ position = N }`; an `Hushallsid_<year>` or monthly column maps its base with
    `{ year = YYYY }` or `{ month = "YYYY-MM" }` and serves only that sub-period.

    Exactly one kind. The set rules (one kind, unique indexes, a year or month
    inside the table's edition, a member column's only mapping) need the whole
    table, so `validate_inventory_placements` owns them."""

    position: StrictInt | None = Field(default=None, ge=1)
    year: StrictInt | None = Field(default=None, ge=1000, le=9999)
    month: str | None = Field(default=None, pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")

    @model_validator(mode="after")
    def _one_kind(self) -> ColumnMember:
        if (
            sum(value is not None for value in (self.position, self.year, self.month))
            != 1
        ):
            raise _InventoryGuardError(
                _MEMBER,
                "member must state exactly one of position, year or month",
            )
        return self

    @property
    def kind(self) -> str:
        return (
            "position"
            if self.position is not None
            else "year"
            if self.year is not None
            else "month"
        )

    @property
    def index(self) -> int | str:
        return next(
            value
            for value in (self.position, self.year, self.month)
            if value is not None
        )

    def bounds(self) -> _Interval | None:
        """The ISO period a year or month member serves; None for a position."""
        if self.year is not None:
            return period_bounds(f"{self.year:04d}")
        if self.month is not None:
            return period_bounds(self.month)
        return None

    @classmethod
    def from_columns(
        cls, position: int | None, lo: str | None, hi: str | None
    ) -> ColumnMember | None:
        """The inverse of `holding_mapping`'s member columns: a position, or the
        year or month whose canonical token renders `(lo, hi)`."""
        if position is not None:
            return cls(position=position)
        if lo is None or hi is None:
            return None
        token = period_token_for_bounds(lo, hi)
        return cls(year=int(token)) if len(token) == 4 else cls(month=token)


class MappingWarning(_InventoryModel):
    """A steward-scoped data warning on one column mapping (#1684): a caveat about
    part of what this steward delivers, which only the steward's catalog shows.

    The author writes both texts. `window` uses the mapping-window grammar and
    narrows the warning to part of the mapping's served period; without one it
    covers all of it. `_check_windows` owns its rules, which need the table."""

    summary: str
    detail: str
    window: EditionSegment | None = None

    @field_validator("summary", "detail")
    @classmethod
    def _nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("a mapping warning's summary and detail must be nonblank")
        return value

    @field_validator("window", mode="before")
    @classmethod
    def _window_year_int_to_token(cls, value: object) -> object:
        return _year_int_to_token(value)


class ColumnMapping(_InventoryModel):
    """One semantic mapping of a physical column: which logical coordinate this
    column IS.

    `register_variant` is the 3-part variant coordinate
    (`<provider>/<register>/<variant>`, `_default` allowed for a single-table
    register), `variable` the 3-segment binding FQID, and `representation` the
    canonical reg_meta `variable_alias.delivery_column_name` this column
    corresponds to. The
    representation is a required join discriminator, not an output substitute.
    `member` marks the column as one indexed member of a wide pivot (#1495); the
    member set, not the column, is then the location that serves the cell.
    `window` narrows the mapping to its own delivered period inside the table's
    edition (#1645): the column serves this coordinate only there.
    `data_warning` attaches a steward-scoped caveat to part or all of what the
    mapping serves (#1684). `_check_windows` owns the rules of both windows,
    which need the whole table.

    `carried` marks an auxiliary table's join key: the column is the variant's row
    key, so every file of the table carries it, but it serves no ordered cell. Another
    table of the variant serves that key, so a carried mapping is outside the
    one-to-one rule, and a key order never reaches the auxiliary table. It serves its
    table's whole edition, so it takes no member or window, and carries no warning.
    Compile refuses one that is not a panel key of its variant
    (`holdings_validation`)."""

    register_variant: str
    variable: str
    representation: str = Field(min_length=1)
    member: ColumnMember | None = None
    window: EditionSegment | None = None
    data_warning: MappingWarning | None = None
    carried: bool = False

    @field_validator("window", mode="before")
    @classmethod
    def _window_year_int_to_token(cls, value: object) -> object:
        return _year_int_to_token(value)

    def served(self, edition: tuple[_Interval, ...]) -> tuple[_Interval, ...]:
        """The period this mapping serves in a table of `edition`: its window, its
        year or month member's period, else the whole edition."""
        if self.window is not None:
            return (_segment_bounds(self.window),)
        if self.member is not None and (period := self.member.bounds()) is not None:
            return (period,)
        return edition

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
        validate_slug(provider, "provider")
        validate_slug(register, "register")
        validate_slug(variant, "register_variant", allow_default=True)
        return value

    @field_validator("variable")
    @classmethod
    def _check_binding_fqid(cls, value: str) -> str:
        if (kind := parse_fqid(value).kind) != "variable":
            raise ValueError(
                "variable must be a 3-segment binding FQID "
                f"<provider>/<register>/<variable>; got {value} ({kind})"
            )
        return value

    @model_validator(mode="after")
    def _check_carried(self) -> ColumnMapping:
        if self.carried and (
            self.member is not None
            or self.window is not None
            or self.data_warning is not None
        ):
            raise _InventoryGuardError(
                _CARRIED,
                f"carried key {self.register_variant} {self.variable} takes no "
                "member, window or data_warning: it rides along with its table's "
                "whole edition and serves no cell",
            )
        return self

    @model_validator(mode="after")
    def _check_prefix_match(self) -> ColumnMapping:
        """The variable's `provider/register` prefix must equal the variant
        coordinate's — same cross-field rule reg-core's structural validator
        enforces for a project source's bindings. A mapping that crosses
        registers is an authoring slip, not a legal combined table."""
        prefix = tuple(self.register_variant.split("/")[:2])
        if tuple(self.variable.split("/")[:2]) != prefix:
            raise ValueError(
                f"variable {self.variable!s} does not belong to register_variant "
                f"{self.register_variant!r} (prefix mismatch)"
            )
        return self


class InventoryColumn(_InventoryModel):
    """One literal, case-preserving physical column of a delivered table.

    A column either maps or carries an `unmapped_reason`; a mapping-less column
    stays in the coverage denominator without being admitted or orderable. One
    with neither would drop out of the holdings catalog unexplained (#1294), so
    it is refused.
    Several mappings let one physical column serve several register variants
    (the combined Utrikeshandel table), unless one of them is a wide-pivot
    member: a member column carries exactly that one mapping
    (`_check_member_sets`). A column whose field set two variants deliver over
    different parts of the edition maps once per variant, each with its own
    `window` (`_check_windows`)."""

    name: str = Field(min_length=1)
    mappings: tuple[ColumnMapping, ...] = Field(default=(), alias="mapping")
    unmapped_reason: str | None = None

    @field_validator("unmapped_reason")
    @classmethod
    def _nonblank_unmapped_reason(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise _InventoryGuardError(
                _UNMAPPED_REASON, "unmapped_reason must be nonblank"
            )
        return value

    @model_validator(mode="after")
    def _check_accounted(self) -> InventoryColumn:
        if self.unmapped_reason is None and not self.mappings:
            raise _InventoryGuardError(
                _UNACCOUNTED,
                "column has neither a mapping nor an unmapped_reason — map it or "
                "state why it stays unmapped",
            )
        return self

    @model_validator(mode="after")
    def _check_unique_mappings(self) -> InventoryColumn:
        """The same `(register_variant, variable, representation)` triple twice
        under one column states one cell twice — a generator or merge slip, not
        a second holding, and the degenerate case of the one-to-one
        resolution invariant (the cross-location arm is
        `DeliveryInventory._check_one_to_one_resolution`). A member index does
        not make it a second coordinate, and a member column carries no other
        mapping at all (`_check_member_sets`)."""
        seen: set[tuple[str, str, str]] = set()
        if self.unmapped_reason is not None and self.mappings:
            raise _InventoryGuardError(
                _UNMAPPED_REASON, "unmapped_reason cannot accompany mappings"
            )
        for mapping in self.mappings:
            triple = (
                mapping.register_variant,
                mapping.variable,
                mapping.representation,
            )
            if triple in seen:
                raise _InventoryGuardError(
                    _DUPLICATE_MAPPING,
                    f"duplicate mapping {mapping.register_variant} "
                    f"{mapping.variable!s} "
                    f"({_representation_label(mapping.representation)}) — state "
                    "each logical coordinate once per column",
                )
            seen.add(triple)
        return self


class InventoryProjection(_InventoryModel):
    """A table's `projection = { of = ..., token = ... }` declaration (#1634).

    `of` names the parent table whose delivery this one projects; `token` is the
    authored, opaque name of the projection's own extract file, as a partition
    label names a shard's. It is never derived from the table id, so it rides the
    partition's slug grammar."""

    of: str = Field(min_length=1)
    token: str

    @field_validator("token")
    @classmethod
    def _check_token(cls, value: str) -> str:
        validate_slug(value, "projection token")
        return value


class PeriodColumn(_InventoryModel):
    """A table's `period_column = { name = ..., encoding = ... }`: the physical
    column whose value dates each row, so an order can filter a multi-period
    table to the requested period.

    `encoding` is the value's shape, whatever its storage type: `date` a SQL
    date or datetime, `year` a year number, `yyyymm` and `yyyymmdd` the packed
    digits (stored as a number or as text). The extract runner reads the
    storage type from the database at run time, so it is not authored here."""

    name: str = Field(min_length=1)
    encoding: Literal["date", "year", "yyyymm", "yyyymmdd"]


class InventoryTable(_InventoryModel):
    """One delivered table: an opaque exact identifier, ONE explicit physical
    edition or explicit year-independent scope, and its physical columns.

    An authored year int or period token means the file delivers ONE period; an
    explicit range or list means the file delivers multiple periods.

    `id` is verbatim and opaque — an exact delivery filename
    (`LISA_Individ_2019.csv`) or a schema-qualified SQL table
    (`dbo.SoS_Patientregister`). It is never parsed for meaning here; a table
    whose name carries no period still requires an explicit curated `edition`.

    `partition` is the optional disjoint-partition label: a short slug naming
    which sub-population shard of one edition this table carries (a survey
    stratum, a reporter stream, a municipality). It is an explicit curated fact,
    never inferred — so a true re-delivery cannot hide behind a partition
    without a reviewable curation line saying so — and it is an inventory/order
    concept only, never a catalog one.

    `projection` declares the table a projection of another table's delivery
    (#1634): the same delivery under other columns, at the parent's grain or a
    finer one, extracted as its own file. A cell both map is one claim the parent
    serves, not a conflict. Like `partition` it is curated, never inferred, and an
    inventory/order concept only.

    `period_column` names the column that dates each row of a dated table, so
    an order can take only the rows of its requested period. Curated, never
    inferred from a column name; without one an order takes the whole table."""

    id: str = Field(min_length=1)
    edition: Edition
    period_scope: Literal["intervals", "year_independent"] = "intervals"
    partition: str | None = None
    projection: InventoryProjection | None = None
    period_column: PeriodColumn | None = None
    columns: tuple[InventoryColumn, ...] = Field(alias="column")

    @field_validator("partition")
    @classmethod
    def _check_partition(cls, value: str | None) -> str | None:
        """A partition label rides the shared slug grammar, like every other
        slug-shaped coordinate — the order manifest names each shard by it, so
        it must be lowercase and period-shape-free (`over70` is legal, `70plus` and `2019` are not)."""
        if value is not None:
            validate_slug(value, "partition")
        return value

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
        An inverted range and an out-of-order/overlapping list fail here.
        A whole `_default` is admitted only by the year-independent scope
        validator below; it never expands into calendar bounds."""
        if value != "_default":
            try:
                edition_bounds(value)
            except ValueError as exc:
                raise _InventoryGuardError(_EDITION, str(exc)) from exc
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
            raise _InventoryGuardError(
                _TABLE_NO_COLUMNS,
                "table declares no columns — list every delivered physical "
                "column, including the unresolved ones that carry no mapping",
            )
        return value

    @model_validator(mode="after")
    def _check_period_scope(self) -> InventoryTable:
        if self.period_scope == "year_independent":
            if self.edition != "_default":
                raise _InventoryGuardError(
                    _PERIOD_SCOPE, "year-independent tables require edition='_default'"
                )
            if any(
                m.register_variant.split("/")[-1] == "_default"
                for c in self.columns
                for m in c.mappings
            ):
                raise _InventoryGuardError(
                    _PERIOD_SCOPE,
                    "year-independent mappings require a concrete variant",
                )
        elif self.edition == "_default":
            raise _InventoryGuardError(
                _PERIOD_SCOPE,
                "edition='_default' requires year-independent period_scope",
            )
        return self

    @model_validator(mode="after")
    def _check_period_column(self) -> InventoryTable:
        """A period column the table lacks would make the extract fail or filter
        on nothing. A year-independent table has no period to filter, and a
        one-token edition delivers one period, which its editions already
        select: a filter inside it could only drop rows the order asked for."""
        if self.period_column is None:
            return self
        if self.period_scope == "year_independent":
            raise _InventoryGuardError(
                _PERIOD_COLUMN, "a year-independent table has no period column"
            )
        if isinstance(self.edition, str):
            raise _InventoryGuardError(
                _PERIOD_COLUMN,
                f"edition {self.edition!r} delivers one period, so the table has "
                "no period column — declare one only on a range or list edition",
            )
        if self.period_column.name not in {column.name for column in self.columns}:
            raise _InventoryGuardError(
                _PERIOD_COLUMN,
                f"period column {self.period_column.name!r} is not a column of "
                "the table — name one of its listed columns, exactly as spelled",
            )
        return self

    @model_validator(mode="after")
    def _check_unique_columns(self) -> InventoryTable:
        seen: set[str] = set()
        for column in self.columns:
            if column.name in seen:
                raise _InventoryGuardError(
                    _DUPLICATE_COLUMN,
                    f"duplicate physical column {column.name!r} — declare each "
                    "column once and list all of its mappings under it",
                )
            seen.add(column.name)
        return self


class _Placement(NamedTuple):
    """One mapping's physical placement, as `_check_one_to_one_resolution`
    compares them. The location is a `(table, column)`, or a table's member set;
    the served bounds are the table's coalesced edition, a year or month
    member's own period, or a column window (#1645); `position` is a position
    member's index, None for a plain column, a windowed column or a year or month
    member."""

    location: str
    representation: str
    served: tuple[_Interval, ...]
    partition: str | None
    table: str
    position: int | None


def _edition_intervals(table: InventoryTable) -> tuple[_Interval, ...]:
    """A table's edition as coalesced intervals, so one edition compares and
    covers alike however it is spelled: `[2018, 2019]` is `{ from = 2018, to =
    2019 }`. Empty for a year-independent table."""
    if table.period_scope != "intervals":
        return ()
    return merge(list(edition_bounds(table.edition)))


def _parent_serves(placement: _Placement, parent: _Placement) -> bool:
    """Whether a projection's `placement` is a cell its parent's `parent`
    placement serves (#1634): the representations conflate, both are the same
    position member or neither is a position member, and the parent serves every
    instant the projection's does. A position set is served member by member, so
    a projection's positions must be positions of the parent's set; a plain
    column, a windowed column or a year or month member is served only by a
    parent mapping that is not a position member and whose own served period
    (edition, member or window) covers it. `crates/reg-catalog`'s `PARENT_SERVES`
    is the same rule at order time."""
    return (
        _representations_conflate(placement.representation, parent.representation)
        and placement.position == parent.position
        and overlap(placement.served, parent.served) == placement.served
    )


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
            raise _InventoryGuardError(
                _NO_TABLES,
                "inventory declares no tables — an inventory is a steward's "
                "holdings statement, so an empty `table` array is a curation "
                "error, not a delivery topology",
            )
        return value

    # The cross-table rules are `tables` field validators, not model
    # validators, so their refusal is located at `table` rather than at the
    # file root. Same-field after-validators run in definition order and stop
    # at the first refusal.
    @field_validator("tables")
    @classmethod
    def _check_unique_tables(
        cls, value: tuple[InventoryTable, ...]
    ) -> tuple[InventoryTable, ...]:
        """Each physical table is declared once: the identifier is exact, and
        one table carries exactly one edition, so a repeated id is an ambiguous
        edition rather than a second table. Several DIFFERENT tables mapping the
        same logical coordinate over DISJOINT editions stays legal — the
        ordinary annual series (`_check_one_to_one_resolution` owns the
        overlapping case)."""
        seen: set[str] = set()
        for table in value:
            if table.id in seen:
                raise _InventoryGuardError(
                    _DUPLICATE_TABLE,
                    f"duplicate table {table.id!r} — a table identifier is exact "
                    "and carries exactly one edition",
                )
            seen.add(table.id)
        return value

    @field_validator("tables")
    @classmethod
    def _check_projections(
        cls, value: tuple[InventoryTable, ...]
    ) -> tuple[InventoryTable, ...]:
        """A projection is the same delivery under other columns (#1634), so it
        names another, unprojected table of the same physical edition (compared
        by bounds, not spelling) and period scope, maps only variants that table
        maps, and neither carries a partition: a shard or a different edition is
        a different delivery, which the one-to-one rule judges as such. Every
        offending table is reported."""
        tables = {table.id: table for table in value}
        problems: list[str] = []
        for table in value:
            if table.projection is None:
                continue
            parent_id = table.projection.of
            where = f"table[{table.id!r}] projection of {parent_id!r}"
            parent = tables.get(parent_id)
            if parent is None or parent_id == table.id:
                problems.append(f"{where}: names no other table of this inventory")
                continue
            if parent.projection is not None:
                problems.append(
                    f"{where}: names a projection; name the table it projects"
                )
            if table.partition is not None or parent.partition is not None:
                problems.append(
                    f"{where}: a projection and its parent carry no partition"
                )
            if table.period_scope != parent.period_scope or _edition_intervals(
                table
            ) != _edition_intervals(parent):
                problems.append(
                    f"{where}: a projection has its parent's edition and period scope"
                )
            variants = {
                mapping.register_variant
                for column in parent.columns
                for mapping in column.mappings
            }
            extra = sorted(
                {
                    mapping.register_variant
                    for column in table.columns
                    for mapping in column.mappings
                }
                - variants
            )
            if extra:
                problems.append(
                    f"{where}: maps variants its parent does not: {', '.join(extra)}"
                )
        problems.extend(_placement_name_collisions(value))
        if problems:
            raise _InventoryGuardError(
                _PROJECTION,
                "a projection must be the same delivery as its parent and name its own file:\n"
                + "\n".join(f"    {line}" for line in problems),
            )
        return value

    @field_validator("tables")
    @classmethod
    def _check_one_to_one_resolution(
        cls, value: tuple[InventoryTable, ...]
    ) -> tuple[InventoryTable, ...]:
        validate_inventory_placements(value)
        return value


def _placement_name_collisions(tables: tuple[InventoryTable, ...]) -> list[str]:
    """A projection token names the projection's own extract file, beside the
    variant's partition files, so among tables mapping a common variant over a
    common period it must differ from every other projection token and every
    partition label: two placements of one name would write one file name, the
    later overwriting the earlier. The file name carries the period, so tables of
    disjoint editions (an annual series of projections) may share a name, while
    two year-independent tables share the `_default` period. One line per
    projection and colliding table."""
    variants = {
        table.id: {m.register_variant for c in table.columns for m in c.mappings}
        for table in tables
    }
    editions = {table.id: _edition_intervals(table) for table in tables}
    problems: list[str] = []
    for table in tables:
        if table.projection is None:
            continue
        token = table.projection.token
        for other in tables:
            if other.id == table.id:
                continue
            if other.projection is not None and other.projection.token == token:
                kind = "projection token"
            elif other.partition == token:
                kind = "partition label"
            else:
                continue
            if table.period_scope != other.period_scope or (
                table.period_scope == "intervals"
                and not overlap(editions[table.id], editions[other.id])
            ):
                continue
            if shared := sorted(variants[table.id] & variants[other.id]):
                problems.append(
                    f"table[{table.id!r}] projection token {token!r}: equals "
                    f"table[{other.id!r}]'s {kind} under {', '.join(shared)}; each "
                    "placement of a variant names its own extract file"
                )
    return problems


def validate_inventory_placements(tables: tuple[InventoryTable, ...]) -> None:
    """Enforce the one-to-one resolution invariant (ratified 2026-09-01;
    reg_meta_build/DESIGN.md → "Holdings resolution invariants").

    Every admitted `(register_variant, variable, representation, period)` cell
    resolves to exactly one physical `(table, column)`. Two mappings that could
    serve one cell at different locations are an error: extraction must not
    choose between sources or order the same observations twice.

    Locations conflict when their editions overlap and their representations
    conflate. Disjoint editions remain legal, as in an ordinary annual series.
    Inventory validation passes literal representations; compilation passes the
    resolver's canonical spellings, catching additional case-only conflicts.
    Grouping preserves variant and period scope.

    Disjoint-partition arm: the invariant holds per `(cell × partition)`.
    Distinct explicit partition labels denote separate population shards and
    never conflict. Equal labels still conflict. An unlabelled location claims
    the whole population and overlaps every shard. Columns of one table share
    its partition label, so the across-column rule remains unchanged.

    Projection arm (#1634): a projection's mapping the parent serves
    (`_parent_serves`: the same position, or neither a position, over every
    instant) does not conflict with the parent; the order then serves the cell
    from the parent (a key cell from both, each file carrying its own keys). Two
    projections of one parent do not conflict where the parent serves each
    one's mapping, since the parent serves that cell. Anywhere else, two columns
    of one projection included, and against every third table, the rule is
    unchanged.

    Report every conflicting pair with both locations, the coordinate and the
    overlapping period. There is deliberately no auto-pick-latest rule: a file
    date does not prove supersession. Curators remove superseded deliveries so
    the inventory states current holdings only.

    Wide-pivot members (#1495): the columns of one table that map one
    `(register_variant, variable, representation)` with a member index are ONE
    location, its member set. Its members never conflict with each other, and the
    set conflicts with any other location, a plain mapping of the same
    representation in the same table included. Position members serve the
    table's whole edition; a year or month member serves only its own period, so
    the overlap check compares those. `_check_member_sets` holds the set's own
    rules first.

    Column windows (#1645): a mapping with a `window` serves only that period, so
    the overlap check compares the window, as for a year or month member.
    `_check_windows` holds the window's own rules first.

    A carried key (`ColumnMapping.carried`) serves no cell, so it is no location:
    the table that serves the key is the one location, and the carried column only
    rides along in its own table's files.

    Cost: group by `(register_variant, variable, period_scope)` and compare
    pairwise only within each group. An inventory with many variables avoids a
    global quadratic scan; the usual largest group is one variable's annual
    series with a few dozen editions.
    """
    _check_member_sets(tables)
    _check_windows(tables)
    parents = {table.id: table.projection.of for table in tables if table.projection}
    located: dict[tuple[str, str, str], list[_Placement]] = {}
    for table in tables:
        bounds = _edition_intervals(table)
        for column in table.columns:
            for mapping in column.mappings:
                if mapping.carried:
                    continue
                key = (
                    mapping.register_variant,
                    mapping.variable,
                    table.period_scope,
                )
                served = mapping.served(bounds)
                if mapping.member is None:
                    location = _location(table.id, column.name)
                else:
                    location = _member_location(table.id, mapping.representation)
                located.setdefault(key, []).append(
                    _Placement(
                        location,
                        mapping.representation,
                        served,
                        table.partition,
                        table.id,
                        mapping.member.position if mapping.member else None,
                    )
                )
    conflicts: list[str] = []
    reported: set[tuple[str, ...]] = set()
    for (variant, variable, scope), placements in located.items():
        # The placements of this coordinate whose table's declared parent serves them.
        served = {
            placement
            for placement in placements
            if any(
                other.table == parents.get(placement.table)
                and _parent_serves(placement, other)
                for other in placements
            )
        }
        for index, a in enumerate(placements):
            a_location, a_rep, a_bounds, a_part, a_table, _ = a
            for b in placements[index + 1 :]:
                b_location, b_rep, b_bounds, b_part, b_table, _ = b
                if a_location == b_location:
                    # One location serving one cell IS the invariant; a
                    # repeated triple there is `InventoryColumn`'s error.
                    continue
                if not _representations_conflate(a_rep, b_rep):
                    continue
                if _partitions_separate(a_part, b_part):
                    continue
                if any(
                    parents.get(projection.table) == parent.table
                    and projection in served
                    for projection, parent in ((a, b), (b, a))
                ):
                    continue
                if (
                    a_table != b_table
                    and a_table in parents
                    and parents[a_table] == parents.get(b_table)
                    and a in served
                    and b in served
                ):
                    continue
                shared = overlap(a_bounds, b_bounds)
                if not shared and scope == "intervals":
                    continue
                pair = (variant, variable, a_location, b_location)
                if pair in reported:
                    # One conflicting location pair is one curation decision,
                    # even when several mappings locate the same coordinate.
                    continue
                reported.add(pair)
                conflicts.append(
                    f"{a_location} "
                    f"({_representation_label(a_rep)}) and "
                    f"{b_location} "
                    f"({_representation_label(b_rep)}) both map "
                    f"{variant} {variable} over {'_default' if scope == 'year_independent' else render(shared)}"
                    + _partition_hint(a_part, b_part)
                )
    if conflicts:
        raise _InventoryGuardError(
            _CELL_CONFLICT,
            "a cell must resolve to exactly one physical (table, column), "
            "but these mappings could each serve the same cell:\n"
            + "\n".join(f"    {line}" for line in conflicts)
            + "\n  An inventory states CURRENT holdings only: discard the "
            "superseded delivery at curation instead of choosing here (a "
            "filename date is not proof of supersession) — reg_meta_build/"
            "DESIGN.md → Holdings curation rules.",
        )


def _check_member_sets(tables: tuple[InventoryTable, ...]) -> None:
    """A wide-pivot member set's own rules (#1495), per table and
    `(register_variant, variable, representation)`: one member kind and unique
    indexes (gaps are allowed: real sets have them, and one member is a set). A
    year or month member lies inside one segment of the table's edition, so a
    year-independent table takes position members only.

    A member column carries exactly one mapping. The order reader keys a held
    column by `(table, column)` and takes the member from the column, not the
    mapping, so a second mapping would inherit or drop the member's period.

    Sets group by the representation's `fold_identity`, the fold
    `canonical_inventory` resolves through, so `DIA` and `dia` are one set here
    as they are once compiled."""
    # simplify: two member sets of one table are never declared paired; a shared
    # position is the only slot key (`kva_typN` beside `opN`). Add a pairing
    # declaration when a consumer must know two sets are paired, such as an order
    # that has to keep `kva_typN` and `opN` together.
    problems: list[str] = []
    for table in tables:
        sets: dict[tuple[str, str, str], list[tuple[str, str, ColumnMember]]] = {}
        for column in table.columns:
            if len(column.mappings) > 1 and any(
                mapping.member is not None for mapping in column.mappings
            ):
                problems.append(
                    f"{_location(table.id, column.name)}: a wide-pivot member "
                    f"column carries {len(column.mappings)} mappings — a member "
                    "column maps exactly one coordinate"
                )
            for mapping in column.mappings:
                if mapping.member is not None:
                    key = (
                        mapping.register_variant,
                        mapping.variable,
                        fold_identity(mapping.representation),
                    )
                    sets.setdefault(key, []).append(
                        (column.name, mapping.representation, mapping.member)
                    )
        if not sets:
            continue
        segments = (
            edition_bounds(table.edition) if table.period_scope == "intervals" else ()
        )
        for (variant, variable, _), members in sets.items():
            where = (
                f"{_member_location(table.id, members[0][1])} ({variant} {variable})"
            )
            kinds = sorted({member.kind for _, _, member in members})
            if len(kinds) > 1:
                problems.append(
                    f"{where} mixes member kinds {', '.join(kinds)} — index one "
                    "set one way"
                )
                continue
            claimed: dict[int | str, str] = {}
            for name, _, member in members:
                if member.index in claimed:
                    problems.append(
                        f"{where}: columns {claimed[member.index]!r} and {name!r} "
                        f"both claim {member.kind} {member.index!r}"
                    )
                    continue
                claimed[member.index] = name
                period = member.bounds()
                if period is None:
                    continue
                if not segments:
                    problems.append(
                        f"{where}: column {name!r} is a {member.kind} member of a "
                        "year-independent table — only position members serve a "
                        "table without periods"
                    )
                elif not any(
                    lo <= period[0] and period[1] <= hi for lo, hi in segments
                ):
                    problems.append(
                        f"{where}: column {name!r} {member.kind} {member.index!r} "
                        f"lies outside the table's edition {render(segments)}"
                    )
    if problems:
        raise _InventoryGuardError(
            _MEMBER,
            "invalid wide-pivot member set:\n"
            + "\n".join(f"    {line}" for line in problems),
        )


def _check_windows(tables: tuple[InventoryTable, ...]) -> None:
    """A column window's own rules (#1645), per table. A window lies inside one
    segment of the table's coalesced edition (`_edition_intervals`), so a
    year-independent table takes none, and a member, which serves its own period,
    takes none.

    A windowed mapping is its column's only mapping of its
    `(register_variant, variable)`: the order reader keys a held column by table
    and column under one variant, so a second mapping there would inherit or drop
    the window.

    Overlap is not a window rule. Two windows of one variant overlapping on
    conflating representations are a cell conflict
    (`validate_inventory_placements`), and windows of different variants may
    overlap as their unwindowed mappings may. Gaps are allowed, because a column
    is not delivered outside its windows. A mapping without a window keeps the
    whole-edition rule.

    A mapping warning's window (#1684) lies inside one segment of what the
    mapping serves (`ColumnMapping.served`), so one warning row states one
    contiguous caveat. A year-independent table has no calendar to narrow, so its
    warnings take no window; one without a window there is a single row without
    bounds."""
    problems: list[str] = []
    for table in tables:
        segments = _edition_intervals(table)
        for column in table.columns:
            where = _location(table.id, column.name)
            for mapping in column.mappings:
                coordinate = f"{mapping.register_variant} {mapping.variable}"
                if mapping.window is not None:
                    problem = _window_problem(mapping, column, segments)
                    if problem is not None:
                        problems.append(f"{where}: {coordinate} {problem}")
                        continue
                warning = mapping.data_warning
                if warning is None or warning.window is None:
                    continue
                if not segments:
                    problems.append(
                        f"{where}: {coordinate} states a data_warning window in a "
                        "year-independent table"
                    )
                    continue
                try:
                    window = _segment_bounds(warning.window)
                except ValueError as exc:
                    problems.append(f"{where}: {coordinate} data_warning window: {exc}")
                    continue
                served = mapping.served(segments)
                if not any(lo <= window[0] and window[1] <= hi for lo, hi in served):
                    problems.append(
                        f"{where}: {coordinate} data_warning window "
                        f"{render((window,))} lies outside the period the mapping "
                        f"serves {render(served)}"
                    )
    if problems:
        raise _InventoryGuardError(
            WINDOW_INVALID,
            "invalid column window:\n" + "\n".join(f"    {line}" for line in problems),
        )


def _window_problem(
    mapping: ColumnMapping,
    column: InventoryColumn,
    segments: tuple[_Interval, ...],
) -> str | None:
    """Why a windowed mapping's own window is invalid, or None (#1645)."""
    assert mapping.window is not None
    if mapping.member is not None:
        return "states a window and a member — a member serves its own period"
    if not segments:
        return "states a window in a year-independent table"
    try:
        window = _segment_bounds(mapping.window)
    except ValueError as exc:
        return f"window: {exc}"
    if not any(lo <= window[0] and window[1] <= hi for lo, hi in segments):
        return (
            f"window {render((window,))} lies outside the table's edition "
            f"{render(segments)}"
        )
    if (
        sum(
            (other.register_variant, other.variable)
            == (mapping.register_variant, mapping.variable)
            for other in column.mappings
        )
        > 1
    ):
        return (
            "has a window and another mapping in this column — a windowed "
            "coordinate maps once per column"
        )
    return None


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
    union arm Pydantic appends to the location (`edition.str`) doesn't
    masquerade as a key the author can look for. The one absent key worth naming is a `missing` error's own field.
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
    (EXIT_CONFIG) naming every offending table/column — never a partially
    parsed inventory. The code is `inventory_toml_unreadable` for a file that
    is not UTF-8 TOML, else the rule family of the FIRST offending line: a
    hand-written guard's own code (`inventory_cell_conflict`, ...) or
    `inventory_invalid` for field grammar. Semantic
    consistency against the reg_meta DB (does each mapping's
    `(register_variant, variable, representation)` resolve?) is a separate
    build/CI gate, not this structural pass."""
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    # `UnicodeDecodeError` alongside the OSError/TOMLDecodeError pair: TOML is
    # UTF-8 by definition, so a mis-encoded inventory is unreadable input on the
    # same documented path — not an uncaught traceback out of `read_text`.
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise _inventory_error(
            "inventory_toml_unreadable",
            f"Could not read delivery inventory {path}: {exc}",
            "The inventory must be UTF-8 TOML (see reg_meta_build/DESIGN.md → "
            "Inventory TOML for the format).",
        ) from exc
    try:
        return DeliveryInventory.model_validate(raw)
    except ValidationError as exc:
        errors = exc.errors()
        first = errors[0].get("ctx", {}).get("error")
        code = first.code if isinstance(first, _InventoryGuardError) else _INVALID
        details = "\n".join(
            f"  {_error_path(raw, error['loc'], missing=error['type'] == 'missing')}: "
            f"{error['msg']}"
            for error in errors
        )
        raise _inventory_error(
            code,
            f"Invalid delivery inventory {path}:\n{details}",
            "Each `[[table]]` needs an exact `id`, one explicit finite "
            "`edition`, and its literal `[[table.column]]` entries; see "
            "reg_meta_build/DESIGN.md → Inventory TOML.",
        ) from exc
