import type { PickerRepresentation } from "./catalog";
import {
  mergePeriods,
  type PeriodBounds,
  periodFromWire,
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
  if (!wire) {
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

function rowWindowBounds(row: PickerRepresentation): PeriodBounds[] {
  return (row.windows.length > 0 ? row.windows : [row]).map((window) => ({
    from: window.from,
    to: window.to,
  }));
}

function rowOverlapsPeriod(row: PickerRepresentation, period: Period): boolean {
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

export function stagedRemoveForCommitted(
  committed: PickerCommittedRow,
): StagedRemove {
  return {
    registerVariant: committed.registerVariant,
    variable: committed.variable,
    representation: committed.representation,
  };
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
      current === undefined ? add.period : mergePeriods(current, add.period),
    );
  }
  return changes.map((change) => {
    const addPeriod = addPeriods.get(change.registerVariant);
    if (addPeriod === undefined) {
      return change;
    }
    return {
      ...change,
      period: mergePeriods(change.period, addPeriod),
    };
  });
}
