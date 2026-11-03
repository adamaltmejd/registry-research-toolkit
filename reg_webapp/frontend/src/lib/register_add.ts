import {
  addColumns,
  columnInProject,
  columnWindowPeriod,
  type TablePeriodMetadata,
  type VariableColumn,
  variableColumns,
} from "./add_columns";
import type { CatalogPeriod, CatalogSegment } from "./api";
import { catalogLastDate } from "./catalog_period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";

/** A row uses the study window, or its whole hull within the catalog bounds. */
export function rowWindow(
  period: CatalogPeriod,
  study: ProjectStudyWindow | null,
  bounds: { min: number; max: number },
): ProjectStudyWindow | null {
  const last = catalogLastDate(period);
  const from = Math.max(
    period.start ? Number(period.start.slice(0, 4)) : bounds.min,
    bounds.min,
  );
  const to = Math.min(last ? Number(last.slice(0, 4)) : bounds.max, bounds.max);
  if (from > to || (study && (study.to < from || study.from > to))) return null;
  return study ?? { from, to };
}

/** The row tick selects every unbound delivery column in its window. */
export function rowColumns(
  segments: readonly CatalogSegment[],
  variable: string,
  project: ProjectData | null,
  window: ProjectStudyWindow,
): VariableColumn[] {
  return variableColumns(segments).filter(
    (column) =>
      columnWindowPeriod(column, window) !== null &&
      !columnInProject(project, variable, column.table),
  );
}

export interface VariableSelection {
  variable: string;
  columns: readonly VariableColumn[];
  window: ProjectStudyWindow;
}

/** Build one candidate for the store's single sources write. */
export function addVariables(
  project: ProjectData,
  selections: readonly VariableSelection[],
  tables: readonly TablePeriodMetadata[],
): ProjectData {
  return selections.reduce(
    (candidate, { variable, columns, window }) =>
      addColumns(candidate, variable, columns, window, tables),
    project,
  );
}
