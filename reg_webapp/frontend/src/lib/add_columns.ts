import type { CatalogSegment } from "./api";
import { periodCoverageUnion } from "./period";
import type {
  ProjectBinding,
  ProjectData,
  ProjectSource,
  ProjectStudyWindow,
} from "./project_data";
import { checkProject, sourcePeriodIntervals } from "./reg_core";

export interface VariableColumn {
  id: string;
  table: string;
  spelling: string;
  segments: CatalogSegment[];
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
): ProjectData {
  const sources = project.sources.map((source) => ({
    ...source,
    bindings: [...source.bindings],
  }));
  const byTable = new Map<string, CatalogSegment[]>();
  for (const column of columns) {
    const spans = byTable.get(column.table) ?? [];
    spans.push(...column.segments);
    byTable.set(column.table, spans);
  }
  for (const [table, segments] of byTable) {
    let source = sources.find((entry) => entry.table === table);
    if (!source) {
      source = {
        name: sourceName(sources, table),
        table,
        period: { ...window },
        bindings: [],
      };
      sources.push(source);
    } else {
      const existing = sourcePeriodIntervals(source.period);
      if ("error" in existing) throw new Error(existing.message);
      // An unbounded source already covers every year. An add never narrows it.
      if (existing.intervals !== null)
        source.period = periodCoverageUnion(source.period, window);
    }
    if (!source.bindings.some((binding) => binding.variable === variable))
      source.bindings.push(defaultBinding(variable, segments));
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
