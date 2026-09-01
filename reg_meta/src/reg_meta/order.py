"""Order materializer + the JSON order-manifest contract (REFACTOR_SPEC.md §12).

A `project_data.json` source is a LOGICAL selection; `inventory.py` is the
steward's PHYSICAL delivery topology. This module is the one place the two meet:
`materialize_order(project, inventory, conn)` turns a validated project plus a
steward inventory plus an open reg_meta DB into either a complete physical order
manifest or a fail-closed result naming every gap. It is shared domain code —
the FastAPI endpoint and the CLI/plugin are thin adapters over this function, so
both emit byte-identical results (§12).

`inventory=None` selects §12's confirmed GLOBAL-DEPLOYMENT FALLBACK: the global
deployment has no physical inventory, so canonical resolution alone grounds the
order. It is the same pipeline — only step 3's matching differs (see below) —
and produces the same entry shape with a blank `table`, the resolved canonical
column in `column`, and `edition` equal to that slice's requested period.
`OrderProvenance.mode` names which of the two grounded the manifest.

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
   contributes only its overlap. In global-fallback mode the slice's own
   canonical column is what serves it, so the slice covers itself exactly and
   the gate below runs unchanged over that contribution. A mapping that OMITS
   `representation` is §12's single-representation arm: it matches only a
   binding that resolves to one canonical representation across the request;
   otherwise an overlapping table carrying one blocks the order rather than
   claiming one column is two representations (a table that cannot overlap is
   inert). Any subperiod of the availability-clipped request left uncovered
   blocks the WHOLE order with the exact gaps — overlap alone never yields a
   partial manifest. There is no table CHOOSING here and none is needed: §12's
   one-to-one resolution invariant, enforced by `inventory.py`, guarantees a
   valid inventory offers at most one `(table, column)` per cell instant PER
   PARTITION, so several contributions to one slice are either disjoint pieces
   of it (the annual series) or distinct partitions of it (the sub-population
   split), and both are wanted whole. The unqualified-mapping block above
   survives that invariant because the inventory validator is DB-blind: a lone
   unqualified mapping is structurally fine, and only the catalog knows the
   binding's representation changed across the request.
4. **Emission.** Every matching table is emitted whole, every partition
   included (v1 has no table chooser, no population field and no row filter —
   the §12 `simplify:` stands). Entries keep project source/binding order; the
   fan-out within a binding sorts by table, canonical edition, then physical
   column. A partitioned entry carries its label, and `extraction_filenames`
   gives it its own output file.

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

from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Runtime imports (not just TYPE_CHECKING): the requested-period conversion
# branches on `isinstance(..., PeriodRange)`, and the adapter door below builds
# and structurally gates the `ProjectData` model itself. This is the `reg_meta →
# reg_schema` dependency §12 sanctions — the materializer consumes
# `ProjectData`, and adding a third package to hold one function was
# explicitly ruled out.
from reg_schema.project_data import PeriodRange, ProjectData
from reg_schema.structural import validate_structural

from .catalog import Catalog
from .db import get_manifest
from .errors import EXIT_CONFIG, RegMetaError
from .fqid import Fqid, FqidError, parse, snap_to_real_month_end

# The interval/period primitives live in `inventory.py` (which cannot import
# this module): an edition, a requested period, an availability clip and a
# resolution conflict all expand and render through one grammar.
from .inventory import (
    EditionRange,
    _intersect,
    _Interval,
    _render,
    _render_interval,
    edition_bounds,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path
    from typing import Any

    from reg_schema.project_data import Binding, Period, Source

    from .inventory import DeliveryInventory, EditionSegment, InventoryColumn

# Contract version of the emitted JSON manifest. Bumped when the shape changes;
# pre-v1 there is no migration path (CLAUDE.md → maturity), and both boundaries
# (this writer, the steward-side extract reader) validate against the models.
ORDER_MANIFEST_VERSION = 1

# The deployment a `None` inventory is: the full-universe global deployment,
# which has no physical delivery topology (§12's fallback). It is a value of
# `reg_schema`'s `Steward` literal and the webapp's default steward id, and the
# provenance gate below compares `ProjectData.steward` against it exactly as it
# does against a real inventory's steward.
GLOBAL_STEWARD = "global"


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
    interrupted series).

    `partition` is the table's §12 disjoint-partition label when it carries one
    — the shard of the edition's population this table delivers — so the
    extractor can see it; it is absent from the JSON otherwise. Extraction
    preserves delivery topology: what goes in as two partitions comes out as two
    files, distinguished by the partition token `extraction_filenames` adds.

    In §12's global-deployment fallback there is no physical topology: `table`
    is blank, `column` carries the resolved canonical column, `edition` equals
    the entry's requested period, and there is no partition."""

    edition: str
    table: str
    column: str
    partition: str | None = None


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
    can be tied back to the exact uploaded project bytes.

    `mode` names what GROUNDED the entries — a steward's physical inventory or
    §12's global fallback (canonical resolution alone, blank `table`) — so a
    reader never has to infer it from the entry shape."""

    mode: Literal["steward_inventory", "global_fallback"]
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
        FastAPI and CLI adapters be compared byte-for-byte (§12).

        `exclude_none` is the spelling of an absent optional: an unpartitioned
        entry omits `partition` entirely rather than carrying an explicit
        `null`, so an inventory with no partitions serializes exactly as it did
        before the arm existed. Every other manifest field is required, and any
        future optional one must accept the same "absent means None" reading
        (both boundaries validate the same models, and absent restores the
        default on the way back in)."""
        return (
            json.dumps(
                self.model_dump(mode="json", exclude_none=True),
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
    variant + partition + period unit (§12 pins the convention in the order
    contract so the extractor never improvises it), e.g.
    `lisa_individer-15plus_2019.csv`.

    A deterministic function of the entry alone: register + variant slugs from
    the logical coordinate, one file per segment of the physical edition (an
    interrupted-series edition delivers one file per segment). A global-fallback
    entry carries `edition = requested_period`, so the same rule gives it one
    file per requested period segment.

    A partitioned entry inserts its label after the variant slug
    (`agi_individuppgifter-agi_arb_2021-03.csv`), which is what keeps two
    partitions of one (variant, edition segment) from colliding into one file —
    §12 extracts them separately because shard identity (reporter stream,
    municipality) may not exist as a column, so a union would destroy it. An
    unpartitioned entry renders exactly as before the arm existed.

    simplify: names use the reg_meta SLUG spelling (`lisa_individer-15plus_…`),
    not the steward's display casing (§12's illustrative `LISA_Individ_2019` is
    not derivable from a slug); a non-grammar edition segment renders as its
    `lo..hi` range. Revisit when a steward's extractor needs its own casing —
    the rule lives here, so it changes in one place.
    """
    partition = entry.physical.partition
    shard = f"{partition}_" if partition is not None else ""
    return tuple(
        f"{entry.logical.register_name}_{entry.logical.variant}_{shard}{unit}.csv"
        for unit in entry.physical.edition.split(",")
    )


# ── interval algebra over inclusive ISO date strings ────────────────────────
#
# The day-arithmetic half (adjacency joining, gaps) lives here because only the
# coverage gate needs it; intersection and period rendering live in
# `inventory.py`, which shares them with the §12 conflict validator.


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
    inventory: DeliveryInventory | None,
    conn: sqlite3.Connection,
) -> OrderResult:
    """Materialize `project` against `inventory` and the open reg_meta DB.

    `inventory=None` is §12's global-deployment fallback — the deployment has no
    physical topology, so canonical resolution grounds the order and entries
    carry a blank `table`. Everything else (clip, slice, coverage gate, emission
    order, determinism) is the same pipeline.

    Returns a complete `OrderManifest` or a non-empty finding set — never a
    partial order. `conn` is read only (the caller owns its lifetime, mirroring
    `validate_semantic`)."""
    steward = GLOBAL_STEWARD if inventory is None else inventory.steward
    if project.steward != steward:
        return _blocked(
            OrderFinding(
                code="steward_mismatch",
                message=(
                    f"project steward {project.steward!r} does not match the "
                    f"deployment steward {steward!r}; a project is validated "
                    "against the deployment it is uploaded to, and provenance "
                    "retargeting is not an application feature "
                    "(REFACTOR_SPEC.md §12)"
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


# ── adapter door ───────────────────────────────────────────────────────────
# The FastAPI endpoint and the `reg-meta order` CLI are THIN adapters over
# `materialize_order` (§12), so the two things they would otherwise each re-type
# — the untrusted-input gate and the blocked-result wording — live here, once.


def load_project(path: Path) -> ProjectData:
    """Read a `project_data.json` file and gate it into a `ProjectData`.

    The CLI adapter's input door (mirrors `inventory.load_inventory`); the
    FastAPI adapter already holds the raw body and calls `project_from_raw`
    directly. Fail-closed: an unreadable or non-JSON-object file raises
    `RegMetaError` (`project_unreadable`, EXIT_CONFIG)."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _order_config_error(
            "project_unreadable",
            f"Could not read project {path}: {exc}",
            "The project must be a UTF-8 `project_data.json` document (see "
            "reg_schema/DESIGN.md).",
        ) from exc
    if not isinstance(raw, dict):
        raise _order_config_error(
            "project_unreadable",
            f"Project {path} is not a JSON object (got {type(raw).__name__}).",
            "The project must be a UTF-8 `project_data.json` document (see "
            "reg_schema/DESIGN.md).",
        )
    return project_from_raw(raw)


def project_from_raw(raw: dict[str, Any]) -> ProjectData:
    """Structurally gate `raw` and build the model `materialize_order` takes.

    The `ProjectData` model enforces field TYPES only, while the structural
    rules (FQID shape, period grammar, the binding/source-prefix match) live in
    `reg_schema.validate_structural` — so without this gate a model-valid but
    structurally invalid spec would materialize a bad provider order. Both
    adapters go through here, so both reject the same specs with the same
    words. Fail-closed: an invalid spec raises `RegMetaError`
    (`project_invalid`, EXIT_CONFIG) naming every structural error, never a
    partial order."""
    structural = validate_structural(raw)
    if not structural.ok:
        errors = [issue for issue in structural.issues if issue.level == "error"]
        raise _order_config_error(
            "project_invalid",
            "cannot materialize an order for a structurally invalid project: "
            + "; ".join(f"{issue.code}@{issue.path}" for issue in errors),
            "Fix the reported structural errors (an order is materialized only "
            "from a valid project).",
        )
    try:
        return ProjectData.model_validate(raw)
    except ValidationError as exc:
        # `validate_structural` passed but the closed models still reject an
        # unrecognized or invalid nested field — a broken project either way,
        # reported on the same path rather than as an opaque traceback.
        raise _order_config_error(
            "project_invalid",
            "project passed structural validation but failed model construction "
            f"(an unrecognized or invalid field?): {exc}",
            "Fix the reported field (an order is materialized only from a valid "
            "project).",
        ) from exc


def blocked_message(result: OrderResult) -> str:
    """Every blocking finding of `result` as one human-readable message.

    §12's byte-identical-adapters rule covers the FAIL-CLOSED path too, not just
    a produced manifest: the FastAPI 422 detail and the CLI's error envelope say
    exactly the same thing about the same order because both render it here.
    Findings come in the materializer's own accumulation order, each prefixed
    with the exact source/variable/period it names. Deliberately ONE line: this
    string is read inside a JSON error envelope and inside the SPA's
    request-error banner, and an embedded newline is an escape sequence in the
    first and collapsed whitespace in the second."""
    count = len(result.findings)
    return f"order blocked by {count} finding{'' if count == 1 else 's'}: " + "; ".join(
        f"{finding.code}: {_finding_locator(finding)}{finding.message}"
        for finding in result.findings
    )


def _finding_locator(finding: OrderFinding) -> str:
    """The `[source variable period]` prefix naming what a finding is about, or
    `""` for a whole-project finding (`steward_mismatch`, `project_empty`)."""
    parts = [
        part
        for part in (finding.source, finding.variable, finding.period)
        if part is not None
    ]
    return f"[{' '.join(parts)}] " if parts else ""


def _order_config_error(code: str, message: str, remediation: str) -> RegMetaError:
    """A configuration-class error (EXIT_CONFIG) for the authored project input
    — the same class `inventory._inventory_error` gives the authored inventory,
    so a CLI user sees one exit code for "your input file is broken"."""
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code=code,
        error_class="configuration",
        message=message,
        remediation=remediation,
    )


def _blocked(finding: OrderFinding) -> OrderResult:
    return OrderResult(manifest=None, findings=(finding,))


def _provenance(
    project: ProjectData, inventory: DeliveryInventory | None, conn: sqlite3.Connection
) -> OrderProvenance:
    manifest = get_manifest(conn)
    return OrderProvenance(
        mode="global_fallback" if inventory is None else "steward_inventory",
        steward=GLOBAL_STEWARD if inventory is None else inventory.steward,
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
    inventory: DeliveryInventory | None,
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
    inventory: DeliveryInventory | None,
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
    # The clip is reported BEFORE the ambiguity gate: a binding that is both
    # clipped and ambiguous must surface both, since §12 reports every clip per
    # binding, never silently, and the finding is stated against the clipped
    # window the researcher has to reason about.
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

    if overlapping := _coexisting_columns(slices):
        finding(
            "representation_ambiguous",
            f"binding {binding.variable!r} resolves to co-existing "
            f"representations {overlapping} at {source.register_variant}; pin "
            "one with `representation` — a manifest never guesses",
            _render(requested),
        )
        return

    # An inventory mapping may omit `representation` — §12's "the concept has a
    # single representation" arm. That is only unambiguous when the binding
    # really resolves to ONE canonical representation across the request. When
    # it changed, an unqualified mapping cannot say WHICH slice its column is,
    # so it matches nothing and blocks: a manifest never claims one physical
    # column represents two canonical representations.
    representations = sorted(by_column)
    unqualified_ok = len(representations) == 1
    if inventory is not None and not unqualified_ok:
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
    editions: dict[tuple[str, str, str], tuple[_Interval, ...]] = {}
    # §12 partition label per contributing table; absent for the global
    # fallback, which has no physical topology to shard.
    partitions: dict[tuple[str, str, str], str | None] = {}
    blocked = False
    for lo, hi, column in slices:
        matched = False
        covered: list[_Interval] = []
        if inventory is None:
            # §12 global fallback: canonical resolution IS the topology. The
            # slice is served by the canonical column it resolved to, under a
            # blank table; it covers itself exactly, so the gate below (which
            # still runs) can only fail on what resolution itself did not
            # deliver — the block already raised as an unresolved, unavailable
            # or ambiguous binding.
            matched = True
            key = ("", column, column)
            contributions.setdefault(key, []).append((lo, hi))
            covered.append((lo, hi))
        else:
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
                    key = (table.id, inv_column.name, column)
                    editions[key] = bounds
                    partitions[key] = table.partition
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

    if inventory is None:
        # §12: a fallback entry's `edition` IS its requested period — known only
        # once the slices contributing to one canonical column are collected.
        for key, intervals in contributions.items():
            editions[key] = _merge(intervals)

    provider, register, _ = source.register_variant.split("/")
    for key, intervals in sorted(
        contributions.items(),
        key=lambda item: (item[0][0], editions[item[0]], item[0][1]),
    ):
        table_id, physical_column, column = key
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
                    edition=_render(editions[key]),
                    table=table_id,
                    column=physical_column,
                    partition=partitions.get(key),
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
