import type { PickerRepresentation } from "./catalog";
import {
  mergePeriods,
  type PeriodBounds,
  periodFromWire,
  periodTokenForBounds,
  periodToWire,
  periodWireBounds,
} from "./period";
import type { Period, ProjectData } from "./project_data";
import type { StagedPeriodChange, StagedRemove } from "./project_store.svelte";

export interface StagedPickerBand {
  key: string;
  registerPrefix: string;
  rows: PickerRepresentation[];
}

export interface PickerCommittedRow {
  key: string;
  registerVariant: string;
  variable: string;
  representation: string | null;
  sourceName: string;
  sourcePeriod: Period;
}

export interface PickerAddPeriod {
  registerVariant: string;
  period: Period;
}

export interface PickerSourcePeriod {
  registerVariant: string;
  period: Period;
}

export function rowRegisterVariant(
  band: StagedPickerBand,
  row: PickerRepresentation,
): string {
  return `${band.registerPrefix}/${row.variant}`;
}

/** The picker row identity seam (#995). Today it is concrete variant + variable +
 * row representation grain; #376 can swap the variant-family grain here without
 * rewriting the staging consumers. */
export function pickerRowKey(
  band: StagedPickerBand,
  row: PickerRepresentation,
): string {
  return [
    rowRegisterVariant(band, row),
    band.key,
    row.representation ?? row.column,
  ].join("::");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function sourceRegisterVariant(source: unknown): string {
  return isRecord(source) && typeof source.register_variant === "string"
    ? source.register_variant
    : "";
}

function sourceBindings(source: unknown): unknown[] {
  return isRecord(source) && Array.isArray(source.bindings)
    ? source.bindings
    : [];
}

function sourceName(source: unknown): string {
  return isRecord(source) && typeof source.name === "string" ? source.name : "";
}

function sourcePeriod(source: unknown): Period {
  return isRecord(source) && "period" in source
    ? (source.period as Period)
    : "";
}

function bindingVariable(binding: unknown): string {
  return isRecord(binding) && typeof binding.variable === "string"
    ? binding.variable
    : "";
}

function bindingRepresentation(binding: unknown): string | null {
  return isRecord(binding) && typeof binding.representation === "string"
    ? binding.representation
    : null;
}

function periodBoundsSegments(period: Period): PeriodBounds[] | null {
  const wire = periodToWire(period);
  if (!wire || wire === "_default") {
    return null;
  }
  const segments: PeriodBounds[] = [];
  for (const part of wire.split(",")) {
    const bounds = periodWireBounds(part);
    if (!bounds) {
      return null;
    }
    segments.push(bounds);
  }
  return segments.length > 0 ? segments : null;
}

function boundsOverlap(a: PeriodBounds, b: PeriodBounds): boolean {
  return a.from <= b.to && b.from <= a.to;
}

interface BoundedPeriodSegment {
  wire: string;
  bounds: PeriodBounds;
}

function rowWindowBounds(row: PickerRepresentation): PeriodBounds[] {
  return (row.windows.length > 0 ? row.windows : [row]).map((window) => ({
    from: window.from,
    to: window.to,
  }));
}

function rowOverlapsPeriod(row: PickerRepresentation, period: Period): boolean {
  if (periodToWire(period) === "_default") {
    return true;
  }
  const sourceBounds = periodBoundsSegments(period);
  if (!sourceBounds) {
    return false;
  }
  const windows = rowWindowBounds(row);
  return sourceBounds.some((sourceWindow) =>
    windows.some((rowWindow) => boundsOverlap(sourceWindow, rowWindow)),
  );
}

function rowMatchesBinding(
  binding: unknown,
  row: PickerRepresentation,
  sourcePeriod: Period,
): boolean {
  const representation = bindingRepresentation(binding);
  if (representation !== null) {
    return (
      representation === row.column ||
      row.renamedColumns.includes(representation)
    );
  }
  return rowOverlapsPeriod(row, sourcePeriod);
}

export function committedPickerRows(
  draft: ProjectData | null,
  bands: readonly StagedPickerBand[],
): Map<string, PickerCommittedRow> {
  const committed = new Map<string, PickerCommittedRow>();
  const sources: unknown[] = Array.isArray(draft?.sources) ? draft.sources : [];
  for (const band of bands) {
    for (const row of band.rows) {
      const registerVariant = rowRegisterVariant(band, row);
      const source = sources.find(
        (s) => sourceRegisterVariant(s) === registerVariant,
      );
      if (!source) {
        continue;
      }
      const period = sourcePeriod(source);
      const binding = sourceBindings(source).find(
        (b) =>
          bindingVariable(b) === band.key && rowMatchesBinding(b, row, period),
      );
      if (!binding) {
        continue;
      }
      committed.set(pickerRowKey(band, row), {
        key: pickerRowKey(band, row),
        registerVariant,
        variable: band.key,
        representation: bindingRepresentation(binding),
        sourceName: sourceName(source),
        sourcePeriod: period,
      });
    }
  }
  return committed;
}

export function sourcePeriodsFromDraft(
  draft: ProjectData | null,
): PickerSourcePeriod[] {
  const out: PickerSourcePeriod[] = [];
  const sources: unknown[] = Array.isArray(draft?.sources) ? draft.sources : [];
  for (const source of sources) {
    const registerVariant = sourceRegisterVariant(source);
    if (registerVariant) {
      out.push({ registerVariant, period: sourcePeriod(source) });
    }
  }
  return out;
}

export function stagedRemoveForCommitted(
  committed: PickerCommittedRow,
): StagedRemove {
  return {
    registerVariant: committed.registerVariant,
    variable: committed.variable,
    representation: committed.representation,
  };
}

export function nullBindingCommittedRowKeys(
  committed: Iterable<PickerCommittedRow>,
  target: PickerCommittedRow,
): string[] {
  if (target.representation !== null) {
    return [target.key];
  }
  const keys: string[] = [];
  for (const row of committed) {
    if (
      row.representation === null &&
      row.registerVariant === target.registerVariant &&
      row.variable === target.variable &&
      periodToWire(row.sourcePeriod) === periodToWire(target.sourcePeriod)
    ) {
      keys.push(row.key);
    }
  }
  return keys.length > 0 ? keys : [target.key];
}

export function stagedPeriodChanges(
  committed: Iterable<PickerCommittedRow>,
  removedKeys: ReadonlySet<string>,
  periodWire: string | null,
): StagedPeriodChange[] {
  if (!periodWire) {
    return [];
  }
  const nextPeriod = periodFromWire(periodWire);
  const nextWire = periodToWire(nextPeriod);
  if (!nextWire) {
    return [];
  }
  const changes = new Map<string, StagedPeriodChange>();
  for (const row of committed) {
    if (removedKeys.has(row.key)) {
      continue;
    }
    if (periodToWire(row.sourcePeriod) === nextWire) {
      continue;
    }
    changes.set(row.registerVariant, {
      registerVariant: row.registerVariant,
      period: nextPeriod,
    });
  }
  return [...changes.values()];
}

export function periodChangesWithStagedAdds(
  changes: readonly StagedPeriodChange[],
  adds: readonly PickerAddPeriod[],
): StagedPeriodChange[] {
  const addPeriods = new Map<string, Period>();
  for (const add of adds) {
    const current = addPeriods.get(add.registerVariant);
    addPeriods.set(
      add.registerVariant,
      current === undefined
        ? add.period
        : periodCoverageUnion(current, add.period),
    );
  }
  return changes.map((change) => {
    const addPeriod = addPeriods.get(change.registerVariant);
    if (addPeriod === undefined) {
      return change;
    }
    return {
      ...change,
      period: periodReplacementCoveringAdd(change.period, addPeriod),
    };
  });
}

export function finalSourcePeriodsForStagedAdds(
  existing: Iterable<PickerSourcePeriod>,
  changes: readonly StagedPeriodChange[],
  adds: readonly PickerAddPeriod[],
): Map<string, Period> {
  const periods = new Map<string, Period>();
  for (const source of existing) {
    if (!periods.has(source.registerVariant)) {
      periods.set(source.registerVariant, source.period);
    }
  }
  for (const add of adds) {
    const current = periods.get(add.registerVariant);
    periods.set(
      add.registerVariant,
      current === undefined
        ? add.period
        : periodCoverageUnion(current, add.period),
    );
  }
  for (const change of periodChangesWithStagedAdds(changes, adds)) {
    periods.set(change.registerVariant, change.period);
  }
  return periods;
}

function periodReplacementCoveringAdd(
  replacement: Period,
  addPeriod: Period,
): Period {
  return periodCoverageUnion(replacement, addPeriod);
}

function periodCoverageUnion(existing: Period, incoming: Period): Period {
  const existingWire = periodToWire(existing);
  const incomingWire = periodToWire(incoming);
  if (existingWire === "_default") {
    return existing;
  }
  if (incomingWire === "_default") {
    return incoming;
  }
  if (isYearMergeablePeriod(existing) && isYearMergeablePeriod(incoming)) {
    return mergePeriods(existing, incoming);
  }
  return (
    unionBoundedPeriodSegments(existing, incoming) ??
    mergePeriods(existing, incoming)
  );
}

function boundedPeriodSegments(period: Period): BoundedPeriodSegment[] | null {
  const wire = periodToWire(period);
  if (!wire || wire === "_default") {
    return null;
  }
  const segments: BoundedPeriodSegment[] = [];
  for (const raw of wire.split(",")) {
    const member = raw.trim();
    const bounds = periodWireBounds(member);
    if (!member || !bounds) {
      return null;
    }
    segments.push({ wire: member, bounds });
  }
  return segments.length > 0 ? segments : null;
}

function unionBoundedPeriodSegments(
  replacement: Period,
  addPeriod: Period,
): Period | null {
  const replacementSegments = boundedPeriodSegments(replacement);
  const addSegments = boundedPeriodSegments(addPeriod);
  if (!replacementSegments || !addSegments) {
    return null;
  }
  const sorted = [...replacementSegments, ...addSegments].sort(
    (a, b) =>
      a.bounds.from.localeCompare(b.bounds.from) ||
      a.bounds.to.localeCompare(b.bounds.to) ||
      a.wire.localeCompare(b.wire),
  );
  const merged: BoundedPeriodSegment[] = [];
  for (const segment of sorted) {
    const previous = merged.at(-1);
    if (!previous) {
      merged.push(segment);
      continue;
    }
    if (previous.bounds.to < segment.bounds.from) {
      merged.push(segment);
      continue;
    }
    if (segment.bounds.to <= previous.bounds.to) {
      continue;
    }
    previous.bounds = {
      from: previous.bounds.from,
      to: segment.bounds.to,
    };
    previous.wire = periodTokenForBounds(
      previous.bounds.from,
      previous.bounds.to,
    );
  }
  return periodFromWire(merged.map((segment) => segment.wire).join(","));
}

function isYearMergeablePeriod(period: Period): boolean {
  const wire = periodToWire(period);
  if (!wire) {
    return false;
  }
  return wire.split(",").every((segment) => {
    const endpoints = segment.includes("..")
      ? segment.split("..")
      : [segment, segment];
    return (
      endpoints.length === 2 &&
      endpoints.every((endpoint) => /^\d{4}$/.test(endpoint.trim()))
    );
  });
}
