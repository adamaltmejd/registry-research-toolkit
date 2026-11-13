import type { CatalogSegment, RegisterShow } from "./api";
import { catalogLastDate } from "./catalog_period";
import { periodCoverageUnion } from "./period";
import {
  isCatalogBinding,
  isCatalogSource,
  type ProjectCatalogBinding,
  type ProjectCatalogSource,
  type ProjectData,
  type ProjectSource,
  type ProjectSourcePeriod,
  type ProjectStudyWindow,
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
import { removeBinding } from "./source_edits";

export type TablePeriodMetadata = Pick<
  RegisterShow["tables"][number],
  "fqid" | "timeless"
>;

export interface VariableColumn {
  id: string;
  table: string;
  spelling: string;
  segments: CatalogSegment[];
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
      };
      columns.set(id, column);
    }
    column.segments.push(segment);
  }
  return [...columns.values()];
}

/** The project's sources that bind `variable` in `table`. A binding has no
 * column, so a source binds the variable whatever its columns there (reg-core
 * `Binding`); two sources of one table can each bind it. Sources entered by
 * hand name no table and never match. */
export function bindingSources(
  project: ProjectData | null,
  variable: string,
  table: string,
): ProjectCatalogSource[] {
  return (project?.sources ?? []).filter(
    (source): source is ProjectCatalogSource =>
      isCatalogSource(source) &&
      source.table === table &&
      source.bindings.some(
        (binding) => isCatalogBinding(binding) && binding.variable === variable,
      ),
  );
}

/** Inclusive days of a segment, open ends as far as they go. */
const segmentDays = (segment: CatalogSegment): Interval => [
  segment.period.start ?? "0001-01-01",
  catalogLastDate(segment.period) ?? "9999-12-31",
];

/** Whether a source's period takes any of the column's rows: a binding has
 * no column, so the column is in the project only where its years meet the
 * source's (#2408). An unbounded period takes every column. */
export function columnInSource(
  column: VariableColumn,
  source: ProjectSource,
): boolean {
  const period = sourcePeriodIntervals(source.period);
  if ("error" in period) return false;
  if (period.intervals === null) return true;
  const { intervals } = period;
  return column.segments.some(
    (segment) => overlapIntervals(intervals, [segmentDays(segment)]).length > 0,
  );
}

/** A variable's columns in one table, as rows that never overlap in time:
 * successive spellings share a row; columns delivered side by side in one
 * file (a slot's members) take a row each (#2408). */
export function columnRows(
  columns: readonly VariableColumn[],
): VariableColumn[][] {
  const first = (column: VariableColumn) =>
    column.segments.reduce(
      (least, segment) =>
        segmentDays(segment)[0] < least ? segmentDays(segment)[0] : least,
      "9999-12-31",
    );
  const rows: VariableColumn[][] = [];
  for (const column of columns.toSorted((a, b) =>
    first(a) < first(b) ? -1 : first(a) > first(b) ? 1 : 0,
  )) {
    const days = column.segments.map(segmentDays);
    const free = rows.filter((placed) =>
      placed.every((other) =>
        other.segments.every(
          (segment) =>
            overlapIntervals(days, [segmentDays(segment)]).length === 0,
        ),
      ),
    );
    // Only where one row takes it: after a slot's members, which row a
    // later column continues is unknown, and sharing one would read as a
    // rename of that member.
    const row = free.length === 1 ? free[0] : undefined;
    if (row) row.push(column);
    else rows.push([column]);
  }
  return rows;
}

/** The whole years of a column's deliveries inside the add window: each
 * delivery's calendar years, cut to the window, so an add writes years, never
 * a date end (#2409), and keeps the gaps between deliveries. */
function columnWindowOverlap(
  column: VariableColumn,
  window: ProjectStudyWindow,
): { segments: CatalogSegment[]; intervals: Interval[] } {
  const requested = studyWindowInterval(window);
  const segments: CatalogSegment[] = [];
  const intervals: Interval[] = [];
  if (requested === null) return { segments, intervals };
  for (const segment of column.segments) {
    const [start, end] = segmentDays(segment);
    if (start > end) continue;
    const years: Interval = [
      `${start.slice(0, 4)}-01-01`,
      `${end.slice(0, 4)}-12-31`,
    ];
    const overlap = overlapIntervals([years], [requested]);
    if (overlap.length > 0) {
      segments.push(segment);
      intervals.push(...overlap);
    }
  }
  return { segments, intervals: mergeIntervals(intervals) };
}

/** The whole years delivered in the add window, retaining gaps between
 * deliveries. Timeless tables are handled from their metadata, never inferred
 * from open dates. */
export function columnWindowPeriod(
  column: VariableColumn,
  window: ProjectStudyWindow,
): ProjectSourcePeriod | null {
  const { intervals } = columnWindowOverlap(column, window);
  return intervals.length > 0
    ? sourcePeriodFromWire(renderIntervals(intervals))
    : null;
}

/** An added binding: no `type`, so the order reads the catalog's (#2071); the
 * classification is pinned when every coded segment states the same one. */
function defaultBinding(
  variable: string,
  segments: readonly CatalogSegment[],
): ProjectCatalogBinding {
  const classification = segments[0]?.classification;
  return {
    variable,
    follow_successor: false,
    ...(classification &&
    segments.every(
      (segment) =>
        segment.coded === "yes" &&
        segment.identifier !== true &&
        segment.classification === classification,
    )
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

/** Take each variable out of every source of its table that binds it: the
 * binding goes, and a source with it when it was the last (with its panel
 * memberships). Checked as `addColumns` is. */
export function removeBindings(
  project: ProjectData,
  bindings: readonly { variable: string; table: string }[],
): ProjectData {
  let candidate = project;
  for (const { variable, table } of bindings) {
    // Two sources of one table can each bind the variable.
    for (;;) {
      const sourceIndex = candidate.sources.findIndex(
        (source) =>
          isCatalogSource(source) &&
          source.table === table &&
          source.bindings.some(
            (binding) =>
              isCatalogBinding(binding) && binding.variable === variable,
          ),
      );
      if (sourceIndex < 0) break;
      candidate = removeBinding(
        candidate,
        sourceIndex,
        candidate.sources[sourceIndex].bindings.findIndex(
          (binding) =>
            isCatalogBinding(binding) && binding.variable === variable,
        ),
      );
    }
  }
  const checked = checkProject(JSON.stringify(candidate));
  if (!checked.ok)
    throw new Error(
      checked.issues.map((issue) => issue.message).join(" ") ||
        "The variables could not be removed. Check the project and try again.",
    );
  return candidate;
}

/** Build and check a candidate before the caller sends it through the store. */
export function addColumns(
  project: ProjectData,
  variable: string,
  columns: readonly VariableColumn[],
  window: ProjectStudyWindow,
  tables: readonly TablePeriodMetadata[],
): ProjectData {
  // A copy, so the loop below edits the candidate and never the open project.
  const sources: ProjectSource[] = structuredClone(project.sources);
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
        `${column.spelling} has no delivery in the years to add. Change the years.`,
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
    // An add goes to a catalog source only, never into one entered by hand.
    let source = sources.find(
      (entry): entry is ProjectCatalogSource =>
        isCatalogSource(entry) && entry.table === table,
    );
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
    if (
      !source.bindings.some(
        (binding) => isCatalogBinding(binding) && binding.variable === variable,
      )
    )
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
