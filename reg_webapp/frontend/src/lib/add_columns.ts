import type { CatalogSegment, Coverage, RegisterShow } from "./api";
import { catalogLastDate } from "./catalog_period";
import { periodCoverageUnion } from "./period";
import type {
  ProjectBinding,
  ProjectData,
  ProjectSource,
  ProjectSourcePeriod,
  ProjectStudyWindow,
} from "./project_data";
import {
  checkProject,
  type Interval,
  mergeIntervals,
  overlapIntervals,
  renderIntervals,
  sourcePeriodFromWire,
  sourcePeriodIntervals,
  studyWindowInterval,
} from "./reg_core";

export type TablePeriodMetadata = Pick<
  RegisterShow["tables"][number],
  "fqid" | "timeless"
>;

export interface VariableColumn {
  id: string;
  table: string;
  spelling: string;
  segments: CatalogSegment[];
  /** The file the steward holds the column in; absent off a steward
   * deployment, where the table's name stands for it. */
  file?: string;
}

type Placement = NonNullable<Coverage["held"]>[number];

/** Overlap of two periods (`end` exclusive, null unbounded), or null. */
function overlapPeriod(
  a: CatalogSegment["period"],
  b: CatalogSegment["period"],
): CatalogSegment["period"] | null {
  const start =
    a.start === null
      ? b.start
      : b.start === null || a.start > b.start
        ? a.start
        : b.start;
  const end =
    a.end === null ? b.end : b.end === null || a.end < b.end ? a.end : b.end;
  return start !== null && end !== null && start >= end ? null : { start, end };
}

/** A steward deployment's columns: one per held file and column, over the
 * years held, never the reference catalog's wider periods. Each placement
 * borrows the facts (type, coding) of the catalog segments it overlaps in its
 * table, preferring the same spelling among those; a placement no segment
 * covers in its held years states only its role there, since another
 * period's facts would be a guess. */
export function heldColumns(
  segments: readonly CatalogSegment[],
  held: readonly Placement[],
): VariableColumn[] {
  const columns = new Map<string, VariableColumn>();
  for (const placement of held) {
    const overlapping = segments.flatMap((segment) => {
      if (segment.table !== placement.table) return [];
      const period = overlapPeriod(segment.period, placement.period);
      return period ? [{ ...segment, period }] : [];
    });
    // Prefer the held spelling per interval: a segment spelled otherwise stays
    // where no same-spelled segment overlaps it (a rename the file did not take).
    const sameSpelling = (s: CatalogSegment) =>
      s.spelling.toLowerCase() === placement.column.toLowerCase();
    const chosen = overlapping
      .filter(
        (s) =>
          sameSpelling(s) ||
          !overlapping.some(
            (o) => sameSpelling(o) && overlapPeriod(o.period, s.period),
          ),
      )
      .sort((a, b) =>
        (a.period.start ?? "").localeCompare(b.period.start ?? ""),
      );
    const bare = (period: CatalogSegment["period"]): CatalogSegment => ({
      classification: null,
      coded: "unknown",
      format: null,
      id_kind: null,
      identifier: null,
      length: null,
      period,
      role: placement.role,
      sensitive: null,
      spelling: placement.column,
      state: "clear",
      table: placement.table,
      temporality: "unknown",
      type: null,
      value_set: null,
    });
    // Held years no catalog segment covers state only the role. Open ends are
    // compared as the earliest and latest days.
    const FIRST = "0000-01-01";
    const LAST = "9999-12-31";
    const startOf = (p: CatalogSegment["period"]) => p.start ?? FIRST;
    const endOf = (p: CatalogSegment["period"]) => p.end ?? LAST;
    const gap = (start: string, end: string) =>
      bare({
        start: start === FIRST ? null : start,
        end: end === LAST ? null : end,
      });
    const parts: CatalogSegment[] = [];
    let covered = startOf(placement.period);
    for (const segment of chosen) {
      if (covered < startOf(segment.period))
        parts.push(gap(covered, startOf(segment.period)));
      parts.push(segment);
      if (endOf(segment.period) > covered) covered = endOf(segment.period);
    }
    if (covered < endOf(placement.period))
      parts.push(gap(covered, endOf(placement.period)));
    const id = JSON.stringify([
      placement.table,
      placement.column,
      placement.physical_table,
    ]);
    let column = columns.get(id);
    if (!column) {
      column = {
        id,
        table: placement.table,
        spelling: placement.column,
        segments: [],
        file: placement.physical_table,
      };
      columns.set(id, column);
    }
    column.segments.push(
      ...parts.map((part) => ({
        ...part,
        table: placement.table,
        spelling: placement.column,
      })),
    );
  }
  return [...columns.values()];
}

/** A delivery column's identity is its table and exact delivered spelling. */
export function variableColumns(
  segments: readonly CatalogSegment[],
): VariableColumn[] {
  const columns = new Map<string, VariableColumn>();
  for (const segment of segments) {
    const id = JSON.stringify([segment.table, segment.spelling]);
    let column = columns.get(id);
    if (!column) {
      column = {
        id,
        table: segment.table,
        spelling: segment.spelling,
        segments: [],
      };
      columns.set(id, column);
    }
    column.segments.push(segment);
  }
  return [...columns.values()];
}

export function columnInProject(
  project: ProjectData | null,
  variable: string,
  table: string,
): boolean {
  return (
    project?.sources.some(
      (source) =>
        source.table === table &&
        source.bindings.some((binding) => binding.variable === variable),
    ) ?? false
  );
}

function columnWindowOverlap(
  column: VariableColumn,
  window: ProjectStudyWindow,
): { segments: CatalogSegment[]; intervals: Interval[] } {
  const requested = studyWindowInterval(window);
  const segments: CatalogSegment[] = [];
  const intervals: Interval[] = [];
  if (requested === null) return { segments, intervals };
  for (const segment of column.segments) {
    const start = segment.period.start ?? requested[0];
    const end = catalogLastDate(segment.period) ?? requested[1];
    if (start > end) continue;
    const overlap = overlapIntervals([[start, end]], [requested]);
    if (overlap.length > 0) {
      segments.push(segment);
      intervals.push(...overlap);
    }
  }
  return { segments, intervals: mergeIntervals(intervals) };
}

/** Exact delivered days in the add window, retaining gaps between deliveries.
 * Timeless tables are handled from their metadata, never inferred from open dates. */
export function columnWindowPeriod(
  column: VariableColumn,
  window: ProjectStudyWindow,
): ProjectSourcePeriod | null {
  const { intervals } = columnWindowOverlap(column, window);
  return intervals.length > 0
    ? sourcePeriodFromWire(renderIntervals(intervals))
    : null;
}

function segmentType(segment: CatalogSegment): ProjectBinding["type"] {
  if (segment.identifier === true) return "id";
  if (segment.coded === "yes") return "categorical";
  if (segment.type === "int" || segment.type === "decimal") return "numeric";
  if (segment.type === "date" || segment.type === "datetime")
    return segment.type;
  return "opaque";
}

function defaultBinding(
  variable: string,
  segments: readonly CatalogSegment[],
): ProjectBinding {
  const types = new Set(segments.map(segmentType));
  const type = types.size === 1 ? segmentType(segments[0]) : "opaque";
  const classification = segments[0]?.classification;
  return {
    variable,
    type,
    follow_successor: false,
    ...(type === "categorical" &&
    classification &&
    segments.every((segment) => segment.classification === classification)
      ? { classification }
      : {}),
  };
}

function sourceName(sources: readonly ProjectSource[], table: string): string {
  const leaf = table.split("/").at(-1) ?? "source";
  const names = new Set(sources.map((source) => source.name));
  let name = leaf;
  for (let suffix = 2; names.has(name); suffix++) name = `${leaf}_${suffix}`;
  return name;
}

/** Build and check a candidate before the caller sends it through the store. */
export function addColumns(
  project: ProjectData,
  variable: string,
  columns: readonly VariableColumn[],
  window: ProjectStudyWindow,
  tables: readonly TablePeriodMetadata[],
): ProjectData {
  const sources = project.sources.map((source) => ({
    ...source,
    bindings: [...source.bindings],
  }));
  const byTable = new Map<
    string,
    { segments: CatalogSegment[]; intervals: Interval[]; timeless: boolean }
  >();
  for (const column of columns) {
    const table = tables.find((entry) => entry.fqid === column.table);
    if (!table)
      throw new Error(
        "Table availability is not loaded. Reload the page and try again.",
      );
    const overlap = table.timeless
      ? { segments: column.segments, intervals: [] }
      : columnWindowOverlap(column, window);
    if (!table.timeless && overlap.intervals.length === 0)
      throw new Error(
        `${column.spelling} has no delivery in the add window. Change the window or untick the column.`,
      );
    const requested = byTable.get(column.table) ?? {
      segments: [],
      intervals: [],
      timeless: table.timeless,
    };
    requested.segments.push(...overlap.segments);
    requested.intervals.push(...overlap.intervals);
    byTable.set(column.table, requested);
  }
  for (const [table, requested] of byTable) {
    const period: ProjectSourcePeriod = requested.timeless
      ? "_default"
      : sourcePeriodFromWire(
          renderIntervals(mergeIntervals(requested.intervals)),
        );
    let source = sources.find((entry) => entry.table === table);
    if (!source) {
      source = {
        name: sourceName(sources, table),
        table,
        period,
        bindings: [],
      };
      sources.push(source);
    } else {
      const existing = sourcePeriodIntervals(source.period);
      if ("error" in existing) throw new Error(existing.message);
      // An unbounded source already covers every year. An add never narrows it.
      if (requested.timeless) source.period = "_default";
      else if (existing.intervals !== null)
        source.period = periodCoverageUnion(source.period, period);
    }
    if (!source.bindings.some((binding) => binding.variable === variable))
      source.bindings.push(defaultBinding(variable, requested.segments));
  }
  const candidate = { ...project, sources };
  const checked = checkProject(JSON.stringify(candidate));
  if (!checked.ok)
    throw new Error(
      checked.issues.map((issue) => issue.message).join(" ") ||
        "The selected columns could not be added. Check the project and try again.",
    );
  return candidate;
}
