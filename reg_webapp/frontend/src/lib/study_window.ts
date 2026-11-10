import type { CatalogPeriod, CatalogSegment, RegisterShow } from "./api";
import { catalogLastDate } from "./catalog_period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";

/** No open project means no study band, even when browsing has a saved window. */
export function studyWindow(
  project: ProjectData | null,
): ProjectStudyWindow | null {
  return project?.window ?? null;
}

/** Inclusive delivered years; an end the period leaves open is drawn to the
 * catalog bound but is not a delivery year, so it is flagged. */
export interface YearRun {
  from: number;
  to: number;
  openStart?: boolean;
  openEnd?: boolean;
}

function yearRuns(
  segments: readonly Pick<CatalogSegment, "period">[],
  bounds: readonly [number, number],
  axisStart: number,
): YearRun[] {
  const eras = segments
    .map(({ period }) => {
      const end = catalogLastDate(period);
      return {
        from: period.start
          ? Number(period.start.slice(0, 4))
          : period.end === null
            ? axisStart
            : bounds[0],
        to: end ? Number(end.slice(0, 4)) : bounds[1],
        openStart: period.start === null,
        openEnd: end === null,
      };
    })
    .filter((era) => era.from <= era.to)
    .sort((a, b) => a.from - b.from || a.to - b.to);
  const runs: YearRun[] = [];
  for (const era of eras) {
    const last = runs.at(-1);
    if (last && era.from <= last.to + 1) {
      if (era.to > last.to) last.openEnd = era.openEnd;
      else if (era.to === last.to) last.openEnd ||= era.openEnd;
      if (era.from === last.from) last.openStart ||= era.openStart;
      last.to = Math.max(last.to, era.to);
    } else runs.push({ ...era });
  }
  return runs;
}

/** Segments have half-open dates; the drawing uses inclusive year cells. */
export function columnYearRuns(
  segments: readonly Pick<CatalogSegment, "period">[],
  bounds: readonly [number, number],
  axisStart = bounds[0],
): ProjectStudyWindow[] {
  return yearRuns(segments, bounds, axisStart).map(({ from, to }) => ({
    from,
    to,
  }));
}

/** A list row's delivered years (`VariableRow`): `columnYearRuns` less the
 * year-independent segments (both ends null, a timeless table's), which have
 * no dated years, so the row draws a bare track and they do not stretch the
 * axis (the server's `period_span` leaves them out too). A lane keeps them,
 * from the axis start, so their column stays pickable. Open ends stay
 * flagged, so the row says "from" or "through" rather than a bound year. */
export function datedYearRuns(
  segments: readonly Pick<CatalogSegment, "period">[],
  bounds: readonly [number, number],
): YearRun[] {
  return yearRuns(
    segments.filter(
      ({ period }) => period.start !== null || period.end !== null,
    ),
    bounds,
    bounds[0],
  );
}

/** Table coverage, with open ends projected to catalog bounds only for drawing. */
export function tableYears(
  table: RegisterShow["tables"][number] | undefined,
  bounds: [number, number],
): ProjectStudyWindow | null {
  if (!table || table.timeless) return null;
  const last = catalogLastDate(table.period);
  return {
    from: table.period.start
      ? Number(table.period.start.slice(0, 4))
      : bounds[0],
    to: last ? Number(last.slice(0, 4)) : bounds[1],
  };
}

/** A register list row's delivered years. The variable's own period is the
 * hull of its tables', so a variable in a timeless table as well as dated ones
 * has an open hull, which `datedYearRuns` drops whole; its dated tables'
 * periods then give its years. */
export function variableYearRuns(
  variable: Pick<RegisterShow["variables"][number], "period" | "tables">,
  tables: readonly RegisterShow["tables"][number][],
  bounds: readonly [number, number],
): YearRun[] {
  return datedYearRuns(variablePeriods(variable, tables), bounds);
}

/** What `variableYearRuns` draws, as catalog periods (for `segmentYears`). */
export function variablePeriods(
  variable: Pick<RegisterShow["variables"][number], "period" | "tables">,
  tables: readonly RegisterShow["tables"][number][],
): { period: CatalogPeriod }[] {
  const { start, end } = variable.period;
  if (start !== null || end !== null) return [variable];
  return tables.filter(
    (table) => !table.timeless && variable.tables.includes(table.fqid),
  );
}
