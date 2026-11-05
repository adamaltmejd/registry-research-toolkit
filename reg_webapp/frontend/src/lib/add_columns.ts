import type { CatalogSegment, RegisterShow } from "./api";
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
  /** The files the steward holds the column in, in period order; empty off a
   * steward deployment, where the table's name stands for them. */
  files: string[];
}

/** A delivery column's identity is its table and exact column name. A steward's
 * yearly files of one table are the same column delivered in several files. */
export function variableColumns(
  segments: readonly CatalogSegment[],
): VariableColumn[] {
  const columns = new Map<string, VariableColumn>();
  for (const segment of segments) {
    const id = JSON.stringify([segment.table, segment.column]);
    let column = columns.get(id);
    if (!column) {
      column = {
        id,
        table: segment.table,
        spelling: segment.column,
        segments: [],
        files: [],
      };
      columns.set(id, column);
    }
    column.segments.push(segment);
    if (segment.file && !column.files.includes(segment.file))
      column.files.push(segment.file);
  }
  return [...columns.values()];
}

/** The files as one name: yearly files of one stem read as year runs after the
 * stem (`RTB_2015–2017, 2019`), so a gap stays visible; anything else is a list. */
export function filesLabel(files: readonly string[]): string | undefined {
  if (files.length <= 1) return files[0];
  // A year suffix is exactly four digits after a non-digit, 1900–2100, so a
  // batch number or a date is listed unchanged.
  const parts = files.map((file) => /^(.*\D)(\d{4})$/.exec(file));
  const stem = parts[0]?.[1];
  const years = parts.map((part) => Number(part?.[2]));
  if (
    !parts.every((part) => part && part[1] === stem) ||
    !years.every((year) => year >= 1900 && year <= 2100)
  )
    return files.join(", ");
  years.sort((a, b) => a - b);
  const runs: [number, number][] = [];
  for (const year of years) {
    const run = runs.at(-1);
    if (run && year === run[1] + 1) run[1] = year;
    else runs.push([year, year]);
  }
  return `${stem}${runs.map(([from, to]) => (from === to ? `${from}` : `${from}–${to}`)).join(", ")}`;
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

/** A segment's binding type: what an add writes and what the variable page's
 * Type pill shows, so the two never disagree. */
export function segmentType(segment: CatalogSegment): ProjectBinding["type"] {
  if (segment.identifier === true) return "id";
  if (segment.coded === "yes") return "categorical";
  if (segment.type === "integer" || segment.type === "decimal") return "numeric";
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
        `${column.spelling} has no delivery in the years to add. Change the years or untick the column.`,
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
