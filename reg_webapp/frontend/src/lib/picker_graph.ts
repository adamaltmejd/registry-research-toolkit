/** Projection of 5.1 catalog segments onto the variable page's first lane. */
import {
  columnInProject,
  columnWindowPeriod,
  type VariableColumn,
} from "./add_columns";
import type { Graph, SetShow, VariableShow } from "./api";
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
  readOnly = false,
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
  readOnly?: boolean;
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
    const span: LaneSpanModel = {
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
    };
    return [
      readOnly
        ? readOnlySpan(span, !!source, variable.name, names.get(column.table))
        : span,
    ];
  });
}

/** A span that shows its "in project" state but cannot be ticked. */
function readOnlySpan(
  span: LaneSpanModel,
  inProject: boolean,
  variableName: string,
  tableName = "Table name unavailable",
): LaneSpanModel {
  return {
    ...span,
    state: inProject ? "in" : "default",
    selectable: false,
    isSelected: undefined,
    staged: null,
    blocked: false,
    note: null,
    accessibleName: `${span.name}, ${variableName}, ${tableName}, ${span.years}, ${inProject ? "in project" : "read only"}${span.changes?.length ? `. Coding changes in ${span.changes.join(", ")}.` : ""}`,
  };
}

/** A group member's columns as read-only spans on the member's lane. */
export function memberSpans({
  member,
  names,
  project,
  axis,
  catalogBounds,
}: {
  member: SetShow["members"][number];
  names: ReadonlyMap<string, string>;
  project: ProjectData | null;
  axis: [number, number];
  catalogBounds: [number, number];
}): LaneSpanModel[] {
  return member.columns.flatMap((column) => {
    const eras = columnYearRuns(
      column.periods.map((period) => ({ period })),
      catalogBounds,
      axis[0],
    );
    if (!eras.length) return [];
    return [
      readOnlySpan(
        {
          id: `${member.fqid}|${JSON.stringify([column.table, column.spelling])}`,
          lane: member.fqid,
          name: column.spelling,
          from: eras[0].from,
          to: eras[eras.length - 1].to,
          segments: eras.map((era) => ({ name: null, ...era })),
          years: eras.map(yearWindowLabel).join(", "),
          state: "default",
          selectable: false,
          accessibleName: "",
        },
        columnInProject(project, member.fqid, column.table),
        member.name,
        names.get(column.table),
      ),
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
  relations: ReadonlyMap<string, string[]>;
} {
  const lanes: LaneModel[] = [];
  const spans: LaneSpanModel[] = [];
  const edges: LaneEdgeModel[] = [];
  const relations = new Map<string, string[]>();
  const byVariable = new Map<string, readonly LaneSpanModel[]>();
  if (!graph) return { lanes, spans, edges, relations };
  byVariable.set(graph.focus, focusSpans);
  const nodes = new Map(graph.nodes.map((node) => [node.fqid, node]));
  for (const [fqid, columns] of neighbourColumns) {
    const node = nodes.get(fqid);
    if (!node || node.kind !== "variable" || fqid === graph.focus) continue;
    const register = fqid.split("/").slice(0, 2).join("/");
    const crossRegister =
      register !== graph.focus.split("/").slice(0, 2).join("/");
    const tableNames = new Map(
      columns.map((column) => {
        const tableName = names.get(column.table) ?? "Table name unavailable";
        const registerName = names.get(register) ?? "Register name unavailable";
        return [
          column.table,
          crossRegister && column.table !== register
            ? `${registerName}, ${tableName}`
            : crossRegister
              ? registerName
              : tableName,
        ];
      }),
    );
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
      return readOnlySpan(
        span,
        inside,
        node.name,
        tableNames.get(span.lane.slice(fqid.length + 1)),
      );
    });
    byVariable.set(fqid, projected);
    spans.push(...projected);
    for (const table of new Set(columns.map((column) => column.table))) {
      const id = `${fqid}|${table}`;
      if (!projected.some((span) => span.lane === id)) continue;
      lanes.push({
        id,
        name: node.name,
        sub: { text: tableNames.get(table) ?? "Table name unavailable" },
        href: catalogHref(fqid),
        current: false,
      });
    }
  }
  for (const edge of graph.edges) {
    if (edge.kind !== "continues") continue;
    const predecessors =
      byVariable.get(edge.to)?.toSorted((a, b) => b.to - a.to) ?? [];
    const successors =
      byVariable.get(edge.from)?.toSorted((a, b) => a.from - b.from) ?? [];
    let predecessor = predecessors[0];
    let successor = successors[0];
    // Anchor on the neighbour's end, then prefer its table on the focus variable.
    if (edge.from === graph.focus && predecessor) {
      const table = predecessor.lane.slice(edge.to.length + 1);
      successor = successors.find((span) => span.lane === table) ?? successor;
    } else if (edge.to === graph.focus && successor) {
      const table = successor.lane.slice(edge.from.length + 1);
      predecessor =
        predecessors.find((span) => span.lane === table) ?? predecessor;
    }
    const newer = nodes.get(edge.from);
    const older = nodes.get(edge.to);
    if (!newer || !older || !predecessor || !successor) continue;
    const year = edge.at?.start ? Number(edge.at.start.slice(0, 4)) : null;
    // LaneGraph supplies the final period for each lane's relation sentences.
    const caveat = edge.caveat ? `. ${edge.caveat.replace(/\.$/, "")}` : "";
    relations.set(predecessor.lane, [
      ...(relations.get(predecessor.lane) ?? []),
      `Succeeded by ${newer.name}${year === null ? "" : ` from ${year}`}${caveat}`,
    ]);
    relations.set(successor.lane, [
      ...(relations.get(successor.lane) ?? []),
      `Continues ${older.name}${caveat}`,
    ]);
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
    lane.relations = relations.get(lane.id);
  }
  return { lanes, spans, edges, relations };
}
