import type { CatalogSegment } from "./api";
import { catalogLastDate } from "./catalog_period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";

/** No open project means no study band, even when browsing has a saved window. */
export function studyWindow(
  project: ProjectData | null,
): ProjectStudyWindow | null {
  return project?.window ?? null;
}

/** Segments have half-open dates; the drawing uses inclusive year cells. */
export function columnYearRuns(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
  axisStart = bounds[0],
): ProjectStudyWindow[] {
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
      };
    })
    .filter((era) => era.from <= era.to)
    .sort((a, b) => a.from - b.from || a.to - b.to);
  const runs: ProjectStudyWindow[] = [];
  for (const era of eras) {
    const last = runs.at(-1);
    if (last && era.from <= last.to + 1) last.to = Math.max(last.to, era.to);
    else runs.push({ ...era });
  }
  return runs;
}
