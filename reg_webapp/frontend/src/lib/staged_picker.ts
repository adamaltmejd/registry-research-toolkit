import type { PickerRepresentation } from "./catalog";
import { periodFromWire, periodToWire } from "./period";
import type { Binding, Period, ProjectData, Source } from "./project_data";
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

function sourceRegisterVariant(source: Source): string {
  return typeof source.register_variant === "string"
    ? source.register_variant
    : "";
}

function bindingVariable(binding: Binding): string {
  return typeof binding.variable === "string" ? binding.variable : "";
}

function bindingRepresentation(binding: Binding): string | null {
  return typeof binding.representation === "string"
    ? binding.representation
    : null;
}

function rowMatchesBinding(
  binding: Binding,
  row: PickerRepresentation,
  variantRows: readonly PickerRepresentation[],
): boolean {
  const representation = bindingRepresentation(binding);
  if (representation !== null) {
    return (
      representation === row.column ||
      row.renamedColumns.includes(representation)
    );
  }
  return row.representation === null || variantRows.length === 1;
}

export function committedPickerRows(
  draft: ProjectData | null,
  bands: readonly StagedPickerBand[],
): Map<string, PickerCommittedRow> {
  const committed = new Map<string, PickerCommittedRow>();
  const sources = Array.isArray(draft?.sources) ? draft.sources : [];
  for (const band of bands) {
    for (const row of band.rows) {
      const registerVariant = rowRegisterVariant(band, row);
      const source = sources.find(
        (s) => sourceRegisterVariant(s) === registerVariant,
      );
      if (!source) {
        continue;
      }
      const variantRows = band.rows.filter((r) => r.variant === row.variant);
      const binding = (
        Array.isArray(source.bindings) ? source.bindings : []
      ).find(
        (b) =>
          bindingVariable(b) === band.key &&
          rowMatchesBinding(b, row, variantRows),
      );
      if (!binding) {
        continue;
      }
      committed.set(pickerRowKey(band, row), {
        key: pickerRowKey(band, row),
        registerVariant,
        variable: band.key,
        representation: bindingRepresentation(binding),
        sourceName: typeof source.name === "string" ? source.name : "",
        sourcePeriod: source.period as Period,
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
