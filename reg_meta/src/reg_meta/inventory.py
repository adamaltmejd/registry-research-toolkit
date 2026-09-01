"""Steward delivery inventory: the physical delivery-topology contract.

A `project_data.json` source is a LOGICAL selection (register variant, variable,
period). What a steward actually delivers is separate data: exact tables, one
explicit physical edition per table, and literal physical column names. This
module is that contract — the typed models plus `load_inventory`, the structural
validator maintainers (and, later, the inventory generator) run before an
inventory is committed. Its load-bearing rule is §12's one-to-one resolution
invariant: every admitted `(register_variant, variable, representation, period)`
cell resolves to exactly one physical `(table, column)` — per §12's
disjoint-partition arm, per `(cell × partition)` — so the extraction tool never
chooses between sources. REFACTOR_SPEC.md §12 is the decision text; the format
is documented in DESIGN.md → Steward delivery inventory.

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
    period_token_for_bounds,
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


# ── interval algebra and period rendering over inclusive ISO dates ──────────
#
# Shared with `order.py`, which imports these: an inventory edition, a project
# period, an availability window and a §12 resolution conflict must all expand
# and render through ONE grammar, so a clip, an overlap and an edition can never
# disagree about bounds or spelling.

# An inclusive ISO `(lo, hi)` date interval — the currency of every edition,
# overlap, clip and coverage computation.
_Interval = tuple[str, str]


def _intersect(a: _Interval, b: _Interval) -> _Interval | None:
    lo, hi = max(a[0], b[0]), min(a[1], b[1])
    return (lo, hi) if lo <= hi else None


def _render(intervals: tuple[_Interval, ...]) -> str:
    """Canonical period rendering, reusing the shared grammar's inverse
    (`period_token_for_bounds`): the coarsest exact token per interval
    (`2019`, `2019-Q3`), an explicit `lo..hi` range when no single token
    expands to it, and the comma-joined wire form for a disjoint series — the
    same grammar `Source.period` and an inventory `edition` are authored in."""
    return ",".join(_render_interval(lo, hi) for lo, hi in intervals)


def _render_interval(lo: str, hi: str) -> str:
    token = period_token_for_bounds(lo, hi)
    if ".." not in token:
        return token
    # A multi-year span has no single token; render the ENDPOINTS as tokens so
    # a whole-year range reads `2019..2020` (the authored range spelling) rather
    # than `2019-01-01..2020-12-31`. Each endpoint token is exact — a year token
    # is used only when the bound really is that year's first/last day — so the
    # rendering still expands back to exactly `(lo, hi)`.
    return f"{_boundary_token(lo, start=True)}..{_boundary_token(hi, start=False)}"


def _boundary_token(iso: str, *, start: bool) -> str:
    if iso.endswith("-01-01" if start else "-12-31"):
        return iso[:4]
    return iso


def _overlap(
    a: tuple[_Interval, ...], b: tuple[_Interval, ...]
) -> tuple[_Interval, ...]:
    """The intervals two editions share, over `edition_bounds` output. Both
    sides are ascending and non-overlapping (`edition_bounds` enforces it), so
    the result is too and renders in period order."""
    return tuple(x for left in a for right in b if (x := _intersect(left, right)))


def _representations_conflate(left: str | None, right: str | None) -> bool:
    """Do two mapping representations describe the SAME cell (§12)?

    Equal explicit representations do. So does a `None` on either side: `None`
    asserts "the concept's single representation", so it conflates with any
    explicit one — and with another `None`. Two DIFFERENT explicit
    representations are different cells (parallel representations, or the two
    ends of a rename), which `order.py`'s slicing and `Binding.representation`
    already choose between; the inventory must not second-guess that with
    period arithmetic."""
    return left is None or right is None or left == right


def _partitions_separate(left: str | None, right: str | None) -> bool:
    """Do two tables carry DISTINCT §12 partition labels — i.e. are they shards
    of one sub-population split rather than two claims on one cell?

    Only two explicit, different labels separate. `None` on either side does
    not: an unlabelled table states the whole population of its edition, so it
    necessarily overlaps any shard of it. Equal labels do not either — the same
    shard delivered twice is the ordinary supersession conflict."""
    return left is not None and right is not None and left != right


def _partition_hint(left: str | None, right: str | None) -> str:
    """The remediation suffix for a conflict where exactly ONE side is labelled.

    That mix is the diagnostic case: the curator already recognized a §12
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


def _representation_label(representation: str | None) -> str:
    return (
        f"representation {representation!r}"
        if representation is not None
        else "no representation"
    )


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

    @model_validator(mode="after")
    def _check_unique_mappings(self) -> InventoryColumn:
        """The same `(register_variant, variable, representation)` triple twice
        under one column states one cell twice — a generator or merge slip, not
        a second holding, and the degenerate case of §12's one-to-one
        resolution invariant (the cross-location arm is
        `DeliveryInventory._check_one_to_one_resolution`)."""
        seen: set[ColumnMapping] = set()
        for mapping in self.mappings:
            if mapping in seen:
                raise ValueError(
                    f"duplicate mapping {mapping.register_variant} "
                    f"{mapping.variable!s} "
                    f"({_representation_label(mapping.representation)}) — state "
                    "each logical coordinate once per column"
                )
            seen.add(mapping)
        return self


class InventoryTable(_InventoryModel):
    """One delivered table: an opaque exact identifier, ONE explicit physical
    edition, and its physical columns.

    `id` is verbatim and opaque — an exact delivery filename
    (`LISA_Individ_2019.csv`) or a schema-qualified SQL table
    (`dbo.SoS_Patientregister`). It is never parsed for meaning here; a table
    whose name carries no period still requires an explicit curated `edition`.

    `partition` is §12's optional disjoint-partition label: a short slug naming
    which sub-population shard of one edition this table carries (a survey
    stratum, a reporter stream, a municipality). It is an explicit curated fact,
    never inferred — so a true re-delivery cannot hide behind a partition
    without a reviewable curation line saying so — and it is an inventory/order
    concept only, never a catalog one."""

    id: str = Field(min_length=1)
    edition: Edition
    partition: str | None = None
    columns: tuple[InventoryColumn, ...] = Field(alias="column")

    @field_validator("partition")
    @classmethod
    def _check_partition(cls, value: str | None) -> str | None:
        """A partition label rides the shared slug grammar, like every other
        slug-shaped coordinate — it becomes a filename token in
        `order.extraction_filenames`, so it must be lowercase and
        period-shape-free (`over70` is legal, `70plus` and `2019` are not)."""
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


# One mapping's physical placement, as `_check_one_to_one_resolution` compares
# them: `(table id, column name, representation, edition bounds, partition)`.
_Placement = tuple[str, str, str | None, tuple[_Interval, ...], str | None]


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
        edition rather than a second table. Several DIFFERENT tables mapping the
        same logical coordinate over DISJOINT editions stays legal — the
        ordinary annual series (`_check_one_to_one_resolution` owns the
        overlapping case)."""
        seen: set[str] = set()
        for table in self.tables:
            if table.id in seen:
                raise ValueError(
                    f"duplicate table {table.id!r} — a table identifier is exact "
                    "and carries exactly one edition"
                )
            seen.add(table.id)
        return self

    @model_validator(mode="after")
    def _check_one_to_one_resolution(self) -> DeliveryInventory:
        """§12's ONE-TO-ONE RESOLUTION INVARIANT (ratified 2026-09-01): every
        admitted `(register_variant, variable, representation, period)` cell
        resolves to exactly ONE physical `(table, column)`. Two mappings that
        could each serve one cell are an error here, because the extraction tool
        never chooses between sources — the materializer matches every mapping
        that fits and emits its table whole, so a conflict orders the same
        observations twice from two layouts.

        Two mappings at DIFFERENT physical locations conflict when their
        editions overlap AND their representations conflate
        (`_representations_conflate`) — across tables (the cumulative
        `FHM_NVR_Covid*` re-delivery) or across two columns of one table (whose
        single edition always overlaps itself). Several tables mapping one
        coordinate over DISJOINT editions stays legal: that is the ordinary
        annual series.

        DISJOINT-PARTITION ARM (§12): the invariant holds per `(cell ×
        partition)`, so two tables carrying DISTINCT `partition` labels never
        conflict — they are shards of one sub-population split (survey strata,
        reporter streams, per-municipality deliveries), unified as one
        user-facing variant and extracted as one file each. Everything else
        still conflicts: equal labels, and a label opposite an unlabelled table
        (which claims the whole population, so it necessarily overlaps the
        shard). Two columns of ONE table share its label, so the
        across-columns arm is untouched.

        Every conflicting pair is reported in one pass with both locations, the
        coordinate and the overlapping period: the error IS the maintainer's
        supersession worklist. There is deliberately no auto-pick-latest arm —
        a filename date is not proof of supersession, so the curator discards
        the superseded delivery and the inventory keeps stating CURRENT
        holdings only (§12).

        Cost: mappings are grouped by `(register_variant, variable)` and
        compared pairwise only WITHIN a group, so a SWECOV-sized inventory
        (thousands of mappings across many variables) never pays a global
        O(n²) — the largest realistic group is one variable's annual series, a
        few dozen editions.
        """
        located: dict[tuple[str, str], list[_Placement]] = {}
        for table in self.tables:
            bounds = edition_bounds(table.edition)
            for column in table.columns:
                for mapping in column.mappings:
                    key = (mapping.register_variant, str(mapping.variable))
                    located.setdefault(key, []).append(
                        (
                            table.id,
                            column.name,
                            mapping.representation,
                            bounds,
                            table.partition,
                        )
                    )
        conflicts: list[str] = []
        reported: set[tuple[str, ...]] = set()
        for (variant, variable), placements in located.items():
            for index, (a_table, a_column, a_rep, a_bounds, a_part) in enumerate(
                placements
            ):
                for b_table, b_column, b_rep, b_bounds, b_part in placements[
                    index + 1 :
                ]:
                    if (a_table, a_column) == (b_table, b_column):
                        # One location serving one cell IS the invariant; a
                        # repeated triple there is `InventoryColumn`'s error.
                        continue
                    if not _representations_conflate(a_rep, b_rep):
                        continue
                    if _partitions_separate(a_part, b_part):
                        continue
                    overlap = _overlap(a_bounds, b_bounds)
                    if not overlap:
                        continue
                    pair = (variant, variable, a_table, a_column, b_table, b_column)
                    if pair in reported:
                        # Two locations conflicting over one coordinate is ONE
                        # curation decision, whatever mix of representations
                        # spelled it (a column carrying both an unqualified and
                        # an explicit mapping conflates twice with one opposite).
                        continue
                    reported.add(pair)
                    conflicts.append(
                        f"{_location(a_table, a_column)} "
                        f"({_representation_label(a_rep)}) and "
                        f"{_location(b_table, b_column)} "
                        f"({_representation_label(b_rep)}) both map "
                        f"{variant} {variable} over {_render(overlap)}"
                        + _partition_hint(a_part, b_part)
                    )
        if conflicts:
            raise ValueError(
                "a cell must resolve to exactly one physical (table, column), "
                "but these mappings could each serve the same cell:\n"
                + "\n".join(f"    {line}" for line in conflicts)
                + "\n  An inventory states CURRENT holdings only: discard the "
                "superseded delivery at curation instead of choosing here (a "
                "filename date is not proof of supersession) — REFACTOR_SPEC.md "
                "§12."
            )
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
    # `UnicodeDecodeError` alongside the OSError/TOMLDecodeError pair: TOML is
    # UTF-8 by definition, so a mis-encoded inventory is unreadable input on the
    # same documented path — not an uncaught traceback out of `read_text`.
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise _inventory_error(
            "inventory_toml_unreadable",
            f"Could not read delivery inventory {path}: {exc}",
            "The inventory must be UTF-8 TOML (see reg_meta/DESIGN.md → Steward "
            "delivery inventory for the format).",
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
