"""Order materializer + the JSON order-manifest contract (REFACTOR_SPEC.md §12).

A `project_data.json` source is a LOGICAL selection; `inventory.py` is the
steward's PHYSICAL delivery topology. This module is the one place the two meet:
`materialize_order(project, inventory, conn)` turns a validated project plus a
steward inventory plus an open reg_meta DB into either a complete physical order
manifest or a fail-closed result naming every gap. It is shared domain code —
the FastAPI endpoint and the CLI/plugin are thin adapters over this function, so
both emit byte-identical results (§12).

Pipeline, per `sources[*].bindings[*]` in project declaration order:

1. **Availability clip.** A source period means "these columns, wherever each is
   available inside this window" (§12 intersection semantics). Each binding is
   clipped to its own documented availability — the union of its
   `variable_state` windows at the source's variant — so a column delivered only
   for a suffix of the window does not widen the order into a cross-product.
   Every clip is reported per binding (`OrderResult.clips`, and on the manifest
   itself when one is produced), never silently, and never as an error.
2. **Representation slicing.** The clipped request is partitioned into slices of
   constant canonical representation (`delivery_column_name`), via
   `Catalog.resolve_at` — the resolution logic is not re-derived here. Two
   columns co-existing at one instant with no `Binding.representation` pin is
   ambiguity, and blocks.
3. **Steward matching + coverage gate.** A table matches a slice only when one
   of its columns carries a mapping matching `(register_variant, variable,
   representation)` AND its physical edition overlaps THAT slice; the edition
   contributes only its overlap. A mapping that OMITS `representation` is §12's
   single-representation arm: it matches only a binding that resolves to one
   canonical representation across the request; otherwise an overlapping table
   carrying one blocks the order rather than claiming one column is two
   representations (a table that cannot overlap is inert). Any subperiod of the
   availability-clipped request left uncovered blocks the WHOLE order with the
   exact gaps — overlap alone never yields a partial manifest.
4. **Emission.** Every matching table is emitted whole (v1 has no table chooser
   and no row filter — the §12 `simplify:` stands). Entries keep project
   source/binding order; the fan-out within a binding sorts by table, canonical
   edition, then physical column.

Fail-closed and one-pass: a blocking result enumerates EVERY finding, so a
researcher fixes the whole order at once instead of one gap per round trip.

Pure domain code: no FastAPI, no filesystem writes, no timestamps — the only
time-shaped values in the manifest come from the DB manifest and the project.
Repeated runs over the same inputs are byte-identical (`OrderManifest.to_json`).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

# Runtime import (not just TYPE_CHECKING): the requested-period conversion
# branches on `isinstance(..., PeriodRange)`. This is the `reg_meta →
# reg_schema` dependency §12 sanctions — the materializer consumes
# `ProjectData`, and adding a third package to hold one function was
# explicitly ruled out.
from reg_schema.project_data import PeriodRange

from .catalog import Catalog
from .db import get_manifest
from .errors import RegMetaError
from .fqid import (
    Fqid,
    FqidError,
    parse,
    period_token_for_bounds,
    snap_to_real_month_end,
)
from .inventory import EditionRange, edition_bounds

if TYPE_CHECKING:
    import sqlite3

    from reg_schema.project_data import Binding, Period, ProjectData, Source

    from .inventory import DeliveryInventory, EditionSegment, InventoryColumn

# Contract version of the emitted JSON manifest. Bumped when the shape changes;
# pre-v1 there is no migration path (CLAUDE.md → maturity), and both boundaries
# (this writer, the steward-side extract reader) validate against the models.
ORDER_MANIFEST_VERSION = 1

# An inclusive ISO `(lo, hi)` date interval — the currency of every clip,
# slice, edition and coverage computation below.
_Interval = tuple[str, str]


class _OrderModel(BaseModel):
    """Frozen, extra-forbidding, alias-aware base for the order contract (same
    shape as `inventory._InventoryModel`). `extra="forbid"` is the read-boundary
    guard: a steward-side extractor validating a manifest that carries an
    unknown key fails loudly instead of silently ignoring an instruction."""

    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="forbid",
        serialize_by_alias=True,
    )


class LogicalCoordinate(_OrderModel):
    """What the researcher asked for: the project-side coordinate of one entry.

    `variable` is the 3-segment binding FQID; `representation` is the canonical
    reg_meta `delivery_column_name` the slice resolved to — a join discriminator
    carried for provenance, NOT the output column (that is
    `PhysicalCoordinate.column`).

    `register` is a `BaseModel` method, so the Python attr is `register_name`
    with a `"register"` alias — the wire key stays the §12 coordinate spelling
    (same pattern as `catalog.BindingGroupRef`)."""

    provider: str
    register_name: str = Field(alias="register")
    variant: str
    variable: str
    representation: str


class PhysicalCoordinate(_OrderModel):
    """What the steward delivers: the entry's physical coordinate.

    `table` is the inventory's opaque exact identifier (a delivery filename or a
    schema-qualified SQL table), `column` its literal case-preserving physical
    column, and `edition` the table's curated edition rendered canonically (a
    period token, an explicit `lo..hi` range, or a comma-joined list for an
    interrupted series)."""

    edition: str
    table: str
    column: str


class OrderEntry(_OrderModel):
    """One resolved logical→physical binding of the order.

    `source` is the project source name, so an entry stays traceable to the
    binding that produced it. `requested_period` is the AVAILABILITY-CLIPPED
    period this table serves (canonically rendered), not the source's raw
    declared period — the table itself is ordered whole regardless (§12)."""

    source: str
    logical: LogicalCoordinate
    requested_period: str
    physical: PhysicalCoordinate


class ClipReport(_OrderModel):
    """One informational availability clip (§12: reported, never silent, never
    an error): the binding asked for `requested_period` and is ordered for
    `ordered_period`, because that is where the column is documented as
    available. Emitted only when the clip actually narrows the request."""

    source: str
    variable: str
    requested_period: str
    ordered_period: str


class OrderProvenance(_OrderModel):
    """Everything the steward-side extract system needs to know WHICH project,
    against WHICH catalog, for WHICH deployment — so the manifest is
    self-contained offline (§12: no network, no catalog lookup at extract time).

    `project_hash` is the SHA-256 of the project's canonical JSON, so a manifest
    can be tied back to the exact uploaded project bytes."""

    steward: str
    project_name: str
    project_schema_version: str
    project_reg_meta_version: str
    project_hash: str
    catalog_schema_version: str
    catalog_import_date: str


class OrderManifest(_OrderModel):
    """The versioned JSON order manifest — machine-written here, machine-read by
    the steward-side extract system, never hand-edited."""

    version: Literal[1]
    provenance: OrderProvenance
    entries: tuple[OrderEntry, ...]
    clips: tuple[ClipReport, ...]

    def to_json(self) -> str:
        """The canonical serialization: sorted keys, stable entry order, UTF-8,
        trailing newline. Deterministic — two runs over the same project,
        inventory and DB produce byte-identical output, which is what lets the
        FastAPI and CLI adapters be compared byte-for-byte (§12)."""
        return (
            json.dumps(
                self.model_dump(mode="json"),
                sort_keys=True,
                indent=2,
                ensure_ascii=False,
            )
            + "\n"
        )


class OrderFinding(_OrderModel):
    """One blocking reason the order cannot be materialized.

    Codes: `steward_mismatch`, `project_empty`, `period_not_orderable`,
    `variable_unresolved`, `binding_unavailable`, `representation_unknown`,
    `representation_unresolved`, `representation_ambiguous`, `mapping_missing`,
    `mapping_ambiguous`, `coverage_gap`. `period` carries the EXACT offending
    subperiod for the coverage codes, so a researcher can fix the request in one
    edit."""

    code: str
    message: str
    source: str | None = None
    variable: str | None = None
    period: str | None = None


class OrderResult(_OrderModel):
    """The materializer's single deterministic output: either a complete
    manifest or a non-empty finding set. Never both, never partial.

    `clips` carries every availability clip the pass accumulated, blocked or
    not: §12 reports clips per binding and never silently, and a blocked
    researcher fixing the whole order in one pass needs to see the clipped
    windows the findings are stated against. A produced manifest repeats them —
    it is the self-contained artifact record."""

    manifest: OrderManifest | None
    findings: tuple[OrderFinding, ...]
    clips: tuple[ClipReport, ...] = ()

    @property
    def ok(self) -> bool:
        return self.manifest is not None


def extraction_filenames(entry: OrderEntry) -> tuple[str, ...]:
    """The extraction output file name(s) for `entry` — one UTF-8 CSV per
    variant + period unit (§12 pins the convention in the order contract so the
    extractor never improvises it), e.g. `lisa_individer-15plus_2019.csv`.

    A deterministic function of the entry alone: register + variant slugs from
    the logical coordinate, one file per segment of the physical edition (an
    interrupted-series edition delivers one file per segment).

    simplify: names use the reg_meta SLUG spelling (`lisa_individer-15plus_…`),
    not the steward's display casing (§12's illustrative `LISA_Individ_2019` is
    not derivable from a slug); a non-grammar edition segment renders as its
    `lo..hi` range. Revisit when a steward's extractor needs its own casing —
    the rule lives here, so it changes in one place.
    """
    return tuple(
        f"{entry.logical.register_name}_{entry.logical.variant}_{unit}.csv"
        for unit in entry.physical.edition.split(",")
    )


# ── interval algebra over inclusive ISO date strings ────────────────────────


def _next_day(iso: str) -> str:
    """The day after an inclusive upper bound. Bounds reaching the open-ended
    `9999-12-31` sentinel have no successor and stay put (they are always
    clipped against a finite requested period before the arithmetic runs)."""
    if iso >= "9999-12-31":
        return iso
    return (
        date.fromisoformat(snap_to_real_month_end(iso)) + timedelta(days=1)
    ).isoformat()


def _merge(intervals: list[_Interval]) -> tuple[_Interval, ...]:
    """Sort and coalesce intervals, joining overlapping AND day-adjacent ones
    (`..2018-12-31` + `2019-01-01..` is one continuous window, not two)."""
    merged: list[_Interval] = []
    for lo, hi in sorted(intervals):
        if merged and lo <= _next_day(merged[-1][1]):
            if hi > merged[-1][1]:
                merged[-1] = (merged[-1][0], hi)
        else:
            merged.append((lo, hi))
    return tuple(merged)


def _intersect(a: _Interval, b: _Interval) -> _Interval | None:
    lo, hi = max(a[0], b[0]), min(a[1], b[1])
    return (lo, hi) if lo <= hi else None


def _gaps(
    whole: tuple[_Interval, ...], covered: list[_Interval]
) -> tuple[_Interval, ...]:
    """The subintervals of `whole` that `covered` does not reach — the coverage
    gate's output. Both sides are merged first, so day-adjacent contributions
    leave no phantom gap."""
    out: list[_Interval] = []
    merged_cover = _merge(covered)
    for lo, hi in whole:
        cursor = lo
        complete = False
        for c_lo, c_hi in merged_cover:
            if c_hi < cursor or c_lo > hi:
                continue
            if c_lo > cursor:
                out.append((cursor, _prev_day(c_lo)))
            if c_hi >= hi:
                # Coverage reached the upper bound: complete, and no successor
                # arithmetic — `_next_day` saturates at the open-ended
                # `9999-12-31` sentinel, which would leave a phantom zero-width
                # gap on a fully covered open-ended interval.
                complete = True
                break
            cursor = _next_day(c_hi)
        if not complete and cursor <= hi:
            out.append((cursor, hi))
    return tuple(out)


def _prev_day(iso: str) -> str:
    return (
        date.fromisoformat(snap_to_real_month_end(iso)) - timedelta(days=1)
    ).isoformat()


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


def _requested_intervals(period: Period) -> tuple[_Interval, ...]:
    """Expand a `Source.period` into inclusive ISO intervals through the SAME
    expansion an inventory edition uses (`inventory.edition_bounds`), so a
    project period and a physical edition can never disagree about bounds.

    Raises `ValueError` / `TypeError` for a period that is not orderable —
    notably the `"_default"` sentinel, which §12 removes from `Source.period`:
    a project with no explicit requested period cannot be ordered."""
    segments = period if isinstance(period, tuple) else (period,)
    converted: list[EditionSegment] = []
    for segment in segments:
        if isinstance(segment, PeriodRange):
            # `EditionRange` canonicalizes int endpoints itself.
            converted.append(EditionRange(from_=segment.from_, to=segment.to))
        elif isinstance(segment, bool):  # bool is an int subclass — never a year
            raise TypeError(f"period segment must be a year or token: {segment!r}")
        elif isinstance(segment, int):
            # A bare year int takes the same spelling an inventory `edition =
            # 2019` canonicalizes to, so both sides expand identically.
            converted.append(f"{segment:04d}")
        else:
            converted.append(segment)
    # Merged: a #307 list period is sorted and non-overlapping but MAY be
    # day-adjacent (`[2018, 2019]`), and the clip/coverage math downstream
    # compares against this tuple — two adjacent segments are one continuous
    # request, not a request with a zero-width hole in it.
    return _merge(
        [
            (lo, snap_to_real_month_end(hi))
            for lo, hi in edition_bounds(tuple(converted))
        ]
    )


# ── materializer ───────────────────────────────────────────────────────────


def materialize_order(
    project: ProjectData,
    inventory: DeliveryInventory,
    conn: sqlite3.Connection,
) -> OrderResult:
    """Materialize `project` against `inventory` and the open reg_meta DB.

    Returns a complete `OrderManifest` or a non-empty finding set — never a
    partial order. `conn` is read only (the caller owns its lifetime, mirroring
    `validate_semantic`)."""
    if project.steward != inventory.steward:
        return _blocked(
            OrderFinding(
                code="steward_mismatch",
                message=(
                    f"project steward {project.steward!r} does not match the "
                    f"deployment inventory steward {inventory.steward!r}; "
                    "a project is validated against the inventory it is "
                    "uploaded to, and provenance retargeting is not an "
                    "application feature (REFACTOR_SPEC.md §12)"
                ),
            )
        )
    if not any(source.bindings for source in project.sources):
        return _blocked(
            OrderFinding(
                code="project_empty",
                message=(
                    "project binds no variables — an empty project is a valid "
                    "editable draft but cannot produce a header-only manifest"
                ),
            )
        )

    catalog = Catalog(conn)
    findings: list[OrderFinding] = []
    entries: list[OrderEntry] = []
    clips: list[ClipReport] = []
    for source in project.sources:
        _materialize_source(source, inventory, catalog, entries, clips, findings)

    if findings:
        return OrderResult(manifest=None, findings=tuple(findings), clips=tuple(clips))
    return OrderResult(
        manifest=OrderManifest(
            version=ORDER_MANIFEST_VERSION,
            provenance=_provenance(project, inventory, conn),
            entries=tuple(entries),
            clips=tuple(clips),
        ),
        findings=(),
        clips=tuple(clips),
    )


def _blocked(finding: OrderFinding) -> OrderResult:
    return OrderResult(manifest=None, findings=(finding,))


def _provenance(
    project: ProjectData, inventory: DeliveryInventory, conn: sqlite3.Connection
) -> OrderProvenance:
    manifest = get_manifest(conn)
    return OrderProvenance(
        steward=inventory.steward,
        project_name=project.name,
        project_schema_version=project.schema_version,
        project_reg_meta_version=project.reg_meta_version,
        project_hash=_project_hash(project),
        catalog_schema_version=manifest.get("schema_version", "unknown"),
        catalog_import_date=manifest.get("import_date", "unknown"),
    )


def _project_hash(project: ProjectData) -> str:
    """SHA-256 over the project's canonical JSON (sorted keys, no insignificant
    whitespace) — the project identity a steward can re-derive from the uploaded
    file itself."""
    payload = json.dumps(
        project.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _materialize_source(
    source: Source,
    inventory: DeliveryInventory,
    catalog: Catalog,
    entries: list[OrderEntry],
    clips: list[ClipReport],
    findings: list[OrderFinding],
) -> None:
    try:
        requested = _requested_intervals(source.period)
    # `FqidError` subclasses `ValueError`; `TypeError` covers a non-period
    # segment type. Either way the source has no orderable period.
    except (TypeError, ValueError) as exc:
        findings.append(
            OrderFinding(
                code="period_not_orderable",
                message=f"source {source.name!r} has no orderable period: {exc}",
                source=source.name,
            )
        )
        return
    variant = source.register_variant.split("/")[2]
    for binding in source.bindings:
        _materialize_binding(
            binding,
            source,
            variant,
            requested,
            inventory,
            catalog,
            entries,
            clips,
            findings,
        )


def _materialize_binding(
    binding: Binding,
    source: Source,
    variant: str,
    requested: tuple[_Interval, ...],
    inventory: DeliveryInventory,
    catalog: Catalog,
    entries: list[OrderEntry],
    clips: list[ClipReport],
    findings: list[OrderFinding],
) -> None:
    def finding(code: str, message: str, period: str | None = None) -> None:
        findings.append(
            OrderFinding(
                code=code,
                message=message,
                source=source.name,
                variable=binding.variable,
                period=period,
            )
        )

    try:
        parsed = parse(binding.variable)
        states = [
            state
            for interval in requested
            for state in catalog.resolve_at(
                parsed, {"from": interval[0], "to": interval[1]}, variant=variant
            )
        ]
    except (FqidError, RegMetaError) as exc:
        finding(
            "variable_unresolved",
            f"binding {binding.variable!r} does not resolve against the "
            f"catalog at {source.register_variant}: {exc}",
        )
        return

    # STEP 1+2: availability clip and representation slicing in one pass — each
    # state contributes its window ∩ the request under its canonical column.
    by_column: dict[str, list[_Interval]] = {}
    for state in states:
        window = (state.valid_from, snap_to_real_month_end(state.valid_to))
        overlaps = [x for req in requested if (x := _intersect(window, req))]
        if not overlaps:
            continue
        if state.delivery_column_name is None:
            finding(
                "representation_unresolved",
                f"binding {binding.variable!r} resolves to a state with no "
                f"delivery column at {source.register_variant}; an unresolved "
                "representation cannot be ordered",
                _render(_merge(overlaps)),
            )
            return
        if (
            binding.representation is not None
            and state.delivery_column_name != binding.representation
        ):
            continue
        by_column.setdefault(state.delivery_column_name, []).extend(overlaps)

    if not by_column:
        available = sorted(
            {s.delivery_column_name for s in states if s.delivery_column_name}
        )
        if binding.representation is not None and states:
            finding(
                "representation_unknown",
                f"binding {binding.variable!r} pins representation "
                f"{binding.representation!r}, which is not a delivery column at "
                f"{source.register_variant} in {_render(requested)} "
                f"(available: {available})",
                _render(requested),
            )
        else:
            finding(
                "binding_unavailable",
                f"binding {binding.variable!r} has no state covering "
                f"{source.register_variant} anywhere in {_render(requested)}",
                _render(requested),
            )
        return

    slices = tuple(
        sorted(
            (lo, hi, column)
            for column, intervals in by_column.items()
            for lo, hi in _merge(intervals)
        )
    )
    if overlapping := _coexisting_columns(slices):
        finding(
            "representation_ambiguous",
            f"binding {binding.variable!r} resolves to co-existing "
            f"representations {overlapping} at {source.register_variant}; pin "
            "one with `representation` — a manifest never guesses",
            _render(requested),
        )
        return

    availability = _merge([(lo, hi) for lo, hi, _ in slices])
    if availability != requested:
        clips.append(
            ClipReport(
                source=source.name,
                variable=binding.variable,
                requested_period=_render(requested),
                ordered_period=_render(availability),
            )
        )

    # An inventory mapping may omit `representation` — §12's "the concept has a
    # single representation" arm. That is only unambiguous when the binding
    # really resolves to ONE canonical representation across the request. When
    # it changed, an unqualified mapping cannot say WHICH slice its column is,
    # so it matches nothing and blocks: a manifest never claims one physical
    # column represents two canonical representations.
    representations = sorted(by_column)
    unqualified_ok = len(representations) == 1
    if not unqualified_ok:
        blocked_by_unqualified = False
        for table in inventory.tables:
            # §12: a table matches a slice only where its edition overlaps, and
            # overlap elsewhere in the request is not a match. A table that
            # cannot reach this binding's clipped request contributes nothing,
            # so its unqualified mapping is inert and must not block either.
            if not any(
                _intersect(bounds, window)
                for bounds in edition_bounds(table.edition)
                for window in availability
            ):
                continue
            for inv_column in table.columns:
                if not _has_unqualified_mapping(
                    inv_column, source.register_variant, parsed
                ):
                    continue
                blocked_by_unqualified = True
                finding(
                    "mapping_ambiguous",
                    f"steward table {table.id!r} column {inv_column.name!r} maps "
                    f"{source.register_variant} {binding.variable!r} with no "
                    f"`representation`, but the binding delivers "
                    f"{representations} across the request; qualify the mapping "
                    "with the canonical representation its column carries",
                    _render(requested),
                )
        if blocked_by_unqualified:
            return

    # STEP 3+4: match each slice against the inventory, gate on full coverage of
    # the clipped request, then emit.
    contributions: dict[tuple[str, str, str], list[_Interval]] = {}
    editions: dict[str, tuple[_Interval, ...]] = {}
    blocked = False
    for lo, hi, column in slices:
        matched = False
        covered: list[_Interval] = []
        for table in inventory.tables:
            bounds = edition_bounds(table.edition)
            for inv_column in table.columns:
                if not _column_matches(
                    inv_column,
                    source.register_variant,
                    parsed,
                    column,
                    unqualified_ok=unqualified_ok,
                ):
                    continue
                matched = True
                overlaps = [x for b in bounds if (x := _intersect(b, (lo, hi)))]
                if not overlaps:
                    continue
                editions[table.id] = bounds
                key = (table.id, inv_column.name, column)
                contributions.setdefault(key, []).extend(overlaps)
                covered.extend(overlaps)
        if not matched:
            blocked = True
            finding(
                "mapping_missing",
                f"no steward table maps {source.register_variant} "
                f"{binding.variable!r} representation {column!r}; a missing "
                "logical-to-physical mapping blocks the order",
                _render(((lo, hi),)),
            )
            continue
        for gap_lo, gap_hi in _gaps(((lo, hi),), covered):
            blocked = True
            gap = _render_interval(gap_lo, gap_hi)
            finding(
                "coverage_gap",
                f"no steward edition delivers {source.register_variant} "
                f"{binding.variable!r} representation {column!r} for {gap}; the "
                "whole order is blocked until every requested subperiod inside "
                "availability is covered",
                gap,
            )
    if blocked:
        return

    provider, register, _ = source.register_variant.split("/")
    for (table_id, physical_column, column), intervals in sorted(
        contributions.items(),
        key=lambda item: (item[0][0], editions[item[0][0]], item[0][1]),
    ):
        entries.append(
            OrderEntry(
                source=source.name,
                logical=LogicalCoordinate(
                    provider=provider,
                    register_name=register,
                    variant=variant,
                    variable=binding.variable,
                    representation=column,
                ),
                requested_period=_render(_merge(intervals)),
                physical=PhysicalCoordinate(
                    edition=_render(editions[table_id]),
                    table=table_id,
                    column=physical_column,
                ),
            )
        )


def _coexisting_columns(slices: tuple[tuple[str, str, str], ...]) -> list[str]:
    """The distinct canonical columns whose slices OVERLAP in time — genuine
    parallel representations the binding must choose between (SSYK 3/4/5-digit,
    age brackets). Distinct columns in disjoint windows are a sequential rename
    across the request, which fans out into slices rather than blocking.
    O(n²) over the handful of slices one binding produces."""
    ambiguous: set[str] = set()
    for index, (a_lo, a_hi, a_col) in enumerate(slices):
        for b_lo, b_hi, b_col in slices[index + 1 :]:
            if a_col != b_col and a_lo <= b_hi and b_lo <= a_hi:
                ambiguous.update((a_col, b_col))
    return sorted(ambiguous)


def _column_matches(
    inv_column: InventoryColumn,
    variant_coordinate: str,
    variable: Fqid,
    representation: str,
    *,
    unqualified_ok: bool,
) -> bool:
    """Does this physical column carry a mapping for exactly this slice?

    Exact match on `(register_variant, variable, representation)` — matching
    anywhere else in the overall request is not a match (§12). A mapping that
    OMITS `representation` matches only when `unqualified_ok`, i.e. the caller
    has proven the binding resolves to exactly one canonical representation
    across the request (§12's "the concept has a single representation" arm);
    otherwise it is ambiguous and the caller has already blocked the order.
    """
    return any(
        mapping.register_variant == variant_coordinate
        and mapping.variable == variable
        and (
            mapping.representation == representation
            or (unqualified_ok and mapping.representation is None)
        )
        for mapping in inv_column.mappings
    )


def _has_unqualified_mapping(
    inv_column: InventoryColumn, variant_coordinate: str, variable: Fqid
) -> bool:
    """Does this physical column map the logical coordinate with NO
    `representation`? The ambiguity probe for a binding whose representation
    changes across the request."""
    return any(
        mapping.register_variant == variant_coordinate
        and mapping.variable == variable
        and mapping.representation is None
        for mapping in inv_column.mappings
    )
