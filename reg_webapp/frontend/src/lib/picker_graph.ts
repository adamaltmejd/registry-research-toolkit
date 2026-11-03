/** Projection of 5.1 catalog segments onto the variable page's first lane. */
import {
  columnInProject,
  columnWindowPeriod,
  type VariableColumn,
} from "./add_columns";
import type { Graph, VariableShow } from "./api";
import { catalogHref } from "./catalog";
import type { LaneEdgeModel, LaneModel, LaneSpanModel } from "./LaneGraph";
import {
  coverageMarks,
  periodLabel,
  periodWindowRelation,
  yearWindowLabel,
} from "./period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";
import { sourcePeriodYears } from "./reg_core";
import { columnYearRuns, studyWindow } from "./study_window";

export function variableSpans({
  columns,
  variable,
  names,
  project,
  selection,
  axis,
  catalogBounds,
  selected,
  idPrefix = "",
}: {
  columns: readonly VariableColumn[];
  variable: Pick<VariableShow, "fqid" | "name">;
  names: ReadonlyMap<string, string>;
  project: ProjectData | null;
  selection: ProjectStudyWindow;
  axis: [number, number];
  catalogBounds: [number, number];
  selected: ReadonlySet<string>;
  idPrefix?: string;
}): LaneSpanModel[] {
  const study = studyWindow(project);
  return columns.flatMap((column) => {
    const eras = columnYearRuns(column.segments, catalogBounds, axis[0]);
    if (!eras.length) return [];
    const first = eras[0].from;
    const last = eras[eras.length - 1].to;
    const source = project?.sources.find(
      (entry) =>
        entry.table === column.table &&
        entry.bindings.some((binding) => binding.variable === variable.fqid),
    );
    const inside = columnWindowPeriod(column, selection) !== null;
    const relation = source
      ? periodWindowRelation(
          source.period,
          study,
          eras.map((era) => [`${era.from}-01-01`, `${era.to}-12-31`]),
        )
      : null;
    const sourceYears = sourcePeriodYears(source?.period);
    const sourceLabel = sourceYears
      ? sourceYears
          .map(([from, to]) => yearWindowLabel({ from, to }))
          .join(", ")
      : periodLabel(source?.period);
    const note =
      relation && relation !== "same"
        ? {
            text:
              relation === "disjoint"
                ? `In project, outside the study window (${sourceLabel}).`
                : `In project for ${sourceLabel}, which differs from the study window.`,
            tone:
              relation === "disjoint" ? ("error" as const) : ("info" as const),
          }
        : null;
    const years = eras.map(yearWindowLabel).join(", ");
    const coding = [...column.segments].sort((a, b) =>
      (a.period.start ?? "").localeCompare(b.period.start ?? ""),
    );
    const changes = [
      ...new Set(
        coding.flatMap((segment, index) =>
          index > 0 &&
          (segment.classification !== coding[index - 1].classification ||
            segment.value_set?.hash !== coding[index - 1].value_set?.hash) &&
          segment.period.start
            ? [Number(segment.period.start.slice(0, 4))]
            : [],
        ),
      ),
    ].sort((a, b) => a - b);
    const changeYears = coverageMarks({ from: first, to: last }, changes)
      .delivered.map(yearWindowLabel)
      .join(", ");
    return [
      {
        id: `${idPrefix}${column.id}`,
        lane: `${idPrefix}${column.table}`,
        name: column.spelling,
        from: first,
        to: last,
        segments: eras.map((era) => ({ name: null, ...era })),
        years,
        state: source ? "in" : inside ? "default" : "out",
        selectable: !!project && !source && inside,
        isSelected: inside && selected.has(column.id),
        staged: inside && selected.has(column.id) && !source ? "add" : null,
        blocked: relation === "disjoint",
        note,
        changes,
        accessibleName: `${column.spelling}, ${variable.name}, ${names.get(column.table) ?? "Table name unavailable"}, ${years}, ${note?.text ?? (source ? "in project" : inside ? "click to select" : "outside the add window")}${changes.length ? `. Coding changes in ${changeYears}.` : ""}`,
      },
    ];
  });
}

/** One-hop succession on the coverage drawing; API subjects are the newer end. */
export function successionLanes({
  focusSpans,
  graph,
  neighbourColumns,
  names,
  project,
  axis,
  catalogBounds,
}: {
  focusSpans: readonly LaneSpanModel[];
  graph: Graph | null;
  neighbourColumns: ReadonlyMap<string, readonly VariableColumn[]>;
  names: ReadonlyMap<string, string>;
  project: ProjectData | null;
  axis: [number, number];
  catalogBounds: [number, number];
}): {
  lanes: LaneModel[];
  spans: LaneSpanModel[];
  edges: LaneEdgeModel[];
  relations: string[];
} {
  const lanes: LaneModel[] = [];
  const spans: LaneSpanModel[] = [];
  const edges: LaneEdgeModel[] = [];
  const relations = new Map<string, string[]>();
  const byVariable = new Map<string, readonly LaneSpanModel[]>();
  if (!graph) return { lanes, spans, edges, relations: [] };
  byVariable.set(graph.focus, focusSpans);
  const nodes = new Map(graph.nodes.map((node) => [node.fqid, node]));
  for (const [fqid, columns] of neighbourColumns) {
    const node = nodes.get(fqid);
    if (!node || node.kind !== "variable" || fqid === graph.focus) continue;
    const projected = variableSpans({
      columns,
      variable: node,
      names,
      project,
      axis,
      catalogBounds,
      selection: { from: axis[0], to: axis[1] },
      selected: new Set(),
      idPrefix: `${fqid}|`,
    }).map((span): LaneSpanModel => {
      const inside = columnInProject(
        project,
        fqid,
        span.lane.slice(fqid.length + 1),
      );
      return {
        ...span,
        state: inside ? "in" : "default",
        selectable: false,
        isSelected: undefined,
        staged: null,
        blocked: false,
        note: null,
        accessibleName: `${span.name}, ${node.name}, ${names.get(span.lane.slice(fqid.length + 1)) ?? "Table name unavailable"}, ${span.years}, ${inside ? "in project" : "read only"}${span.changes?.length ? `. Coding changes in ${span.changes.join(", ")}.` : ""}`,
      };
    });
    byVariable.set(fqid, projected);
    spans.push(...projected);
    for (const table of new Set(columns.map((column) => column.table))) {
      const id = `${fqid}|${table}`;
      if (!projected.some((span) => span.lane === id)) continue;
      lanes.push({
        id,
        name: node.name,
        sub: { text: names.get(table) ?? "Table name unavailable" },
        href: catalogHref(fqid),
        current: false,
      });
    }
  }
  for (const edge of graph.edges) {
    if (edge.kind !== "continues") continue;
    const predecessor = byVariable
      .get(edge.to)
      ?.toSorted((a, b) => b.to - a.to)[0];
    const successor = byVariable
      .get(edge.from)
      ?.toSorted((a, b) => a.from - b.from)[0];
    const newer = nodes.get(edge.from);
    const older = nodes.get(edge.to);
    if (!newer || !older) continue;
    const year = edge.at?.start ? Number(edge.at.start.slice(0, 4)) : null;
    // LaneGraph supplies the final period for each lane's relation sentences.
    const caveat = edge.caveat ? `. ${edge.caveat.replace(/\.$/, "")}` : "";
    relations.set(edge.to, [
      ...(relations.get(edge.to) ?? []),
      `Succeeded by ${newer.name}${year === null ? "" : ` from ${year}`}${caveat}`,
    ]);
    relations.set(edge.from, [
      ...(relations.get(edge.from) ?? []),
      `Continues ${older.name}${caveat}`,
    ]);
    if (predecessor && successor)
      edges.push({
        id: `${edge.to}|${edge.from}|${edge.at?.start ?? ""}`,
        from: predecessor.id,
        to: successor.id,
        kind: "succession",
        year,
      });
  }
  const firstYear = new Map(
    lanes.map((lane) => [
      lane.id,
      Math.min(
        ...spans
          .filter((span) => span.lane === lane.id)
          .map((span) => span.from),
      ),
    ]),
  );
  lanes.sort((a, b) => (firstYear.get(a.id) ?? 0) - (firstYear.get(b.id) ?? 0));
  for (const lane of lanes) {
    const fqid = lane.id.split("|")[0];
    lane.relations = relations.get(fqid);
  }
  return { lanes, spans, edges, relations: relations.get(graph.focus) ?? [] };
}
