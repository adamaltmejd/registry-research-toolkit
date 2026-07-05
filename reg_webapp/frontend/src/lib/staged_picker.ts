import {
  addWindowBounds,
  type PickerRepresentation,
  type PickerVariantSegment,
  pickerRowVariantFamily,
  rowAddPeriod,
  windowsAddPeriod,
  windowsOverlapWindow,
} from "./catalog";
import {
  type PeriodBounds,
  periodCoverageUnion,
  periodToWire,
  periodWireBounds,
} from "./period";
import {
  isPlainObject,
  type Period,
  type ProjectData,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  safeSourceSlots,
} from "./project_data";
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
  removals?: StagedRemove[];
}

export interface PickerAddPeriod {
  registerVariant: string;
  period: Period;
}

export interface PickerSourcePeriod {
  registerVariant: string;
  period: Period;
}

export interface PickerCommitScope {
  period?: string | null | undefined;
  window?: [number, number] | null;
}

type CommitVariantSegment = Pick<PickerVariantSegment, "variant" | "windows">;

export function rowRegisterVariantForVariant(
  band: StagedPickerBand,
  variant: string,
): string {
  return `${band.registerPrefix}/${variant}`;
}

export function pickerRowKey(
  band: StagedPickerBand,
  row: PickerRepresentation,
): string {
  return [
    `${band.registerPrefix}/${pickerRowVariantFamily(row)}`,
    band.key,
    row.representation ?? row.column,
  ].join("::");
}

function sourcePeriod(source: unknown): Period {
  return safeSourcePeriod(source) ?? "";
}

function bindingVariable(binding: unknown): string {
  return isPlainObject(binding) && typeof binding.variable === "string"
    ? binding.variable
    : "";
}

function bindingRepresentation(binding: unknown): string | null {
  return isPlainObject(binding) && typeof binding.representation === "string"
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

/** The concrete `register_variant` segments a folded picker row spans (#376): its
 * `variantSegments` when folded, else the single-segment fallback on `row.variant`
 * (the unfolded HEAD — see the per-concrete-segment invariant in catalog.ts). The
 * ONE place the row → concrete-segment fan-out is derived. */
function rowVariantSegments(row: PickerRepresentation): CommitVariantSegment[] {
  return row.variantSegments && row.variantSegments.length > 0
    ? row.variantSegments
    : [{ variant: row.variant, windows: row.windows }];
}

/** The concrete segments a row commits under `scope`, plus the scope machinery each
 * consumer needs. Shared by `rowRelevantSegments` (staging-match) and `rowAddSegments`
 * (Apply fan-out) so the two can't drift on WHICH segments a period-scoped add touches
 * (the #376 whack-a-mole seam). A single-segment (unfolded) row is always fully
 * relevant; a folded family narrows to the segments whose delivery windows overlap the
 * active add window, falling back to ALL segments when none do (an explicitly-selected
 * out-of-window row is never silently dropped). */
function relevantSegments(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): {
  segments: CommitVariantSegment[];
  addWindow: { from: string; to: string } | null;
  clipped: boolean;
  folded: boolean;
} {
  const addWindow = addWindowBounds(scope.period, scope.window ?? null);
  const segments = rowVariantSegments(row);
  if (segments.length === 1) {
    return { segments, addWindow, clipped: true, folded: false };
  }
  const overlapping = segments.filter((segment) =>
    windowsOverlapWindow(segment.windows, addWindow),
  );
  const clipped = overlapping.length > 0;
  return {
    segments: clipped ? overlapping : segments,
    addWindow,
    clipped,
    folded: true,
  };
}

function rowRelevantSegments(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): CommitVariantSegment[] {
  return relevantSegments(row, scope).segments;
}

/** One concrete `register_variant` an Apply must stage for a (folded or plain) picker
 * row, with its scope-clipped add period. */
export interface RowAddSegment {
  variant: string;
  registerVariant: string;
  periodWire: string | null;
}

/** The per-concrete-segment Apply plan for a picker row (#376): ONE source per concrete
 * `register_variant` the row's active scope touches, each with its own era-clipped wire
 * period. The single home for the picker-row → staged-add fan-out, consumed by every
 * view's `stagedAddCandidates` so the per-concrete-segment invariant (catalog.ts) is
 * enforced once, not re-derived per view.
 *   - An UNFOLDED row stages its one variant with `rowAddPeriod` (the whole-row window,
 *     fallback allowed so an out-of-window add still commits the row's own span).
 *   - A FOLDED family stages each relevant concrete segment with its OWN delivery
 *     windows clipped to the add window (no fallback: a family segment's period is
 *     era-precise so a partial-family add can't leak coverage into the other era). */
export function rowAddSegments(
  band: StagedPickerBand,
  row: PickerRepresentation,
  scope: PickerCommitScope,
): RowAddSegment[] {
  const { segments, addWindow, clipped, folded } = relevantSegments(row, scope);
  return segments.map((segment) => ({
    variant: segment.variant,
    registerVariant: rowRegisterVariantForVariant(band, segment.variant),
    periodWire: folded
      ? windowsAddPeriod(segment.windows, clipped ? addWindow : null, false)
      : rowAddPeriod(row, addWindow),
  }));
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
  scope: PickerCommitScope = {},
): Map<string, PickerCommittedRow> {
  const committed = new Map<string, PickerCommittedRow>();
  const sources = safeSourceSlots(draft?.sources);
  for (const band of bands) {
    for (const row of band.rows) {
      const rowKey = pickerRowKey(band, row);
      const segments = rowRelevantSegments(row, scope);
      const matched: PickerCommittedRow[] = [];
      for (const segment of segments) {
        const registerVariant = rowRegisterVariantForVariant(
          band,
          segment.variant,
        );
        const source = sources.find(
          (s) => safeSourceRegisterVariant(s) === registerVariant,
        );
        if (!source) {
          continue;
        }
        const period = sourcePeriod(source);
        const binding = safeSourceBindings(source).find(
          (b) =>
            bindingVariable(b) === band.key &&
            rowMatchesBinding(b, row, period),
        );
        if (!binding) {
          continue;
        }
        matched.push({
          key: rowKey,
          registerVariant,
          variable: band.key,
          representation: bindingRepresentation(binding),
          sourceName: safeSourceName(source),
          sourcePeriod: period,
        });
      }
      if (matched.length !== segments.length) {
        continue;
      }
      const first = matched[0];
      if (!first) {
        continue;
      }
      committed.set(rowKey, {
        ...first,
        removals:
          matched.length > 1
            ? matched.map((match) => ({
                registerVariant: match.registerVariant,
                variable: match.variable,
                representation: match.representation,
              }))
            : undefined,
      });
    }
  }
  return committed;
}

export function sourcePeriodsFromDraft(
  draft: ProjectData | null,
): PickerSourcePeriod[] {
  const out: PickerSourcePeriod[] = [];
  const sources = safeSourceSlots(draft?.sources);
  for (const source of sources) {
    const registerVariant = safeSourceRegisterVariant(source);
    if (registerVariant) {
      out.push({ registerVariant, period: sourcePeriod(source) });
    }
  }
  return out;
}

export function stagedRemoveForCommitted(
  committed: PickerCommittedRow,
): StagedRemove[] {
  return committed.removals && committed.removals.length > 0
    ? committed.removals
    : [
        {
          registerVariant: committed.registerVariant,
          variable: committed.variable,
          representation: committed.representation,
        },
      ];
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
      period: periodCoverageUnion(change.period, addPeriod),
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
