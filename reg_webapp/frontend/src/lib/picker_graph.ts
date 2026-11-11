/** Projection of 5.1 catalog segments onto the variable page's lanes. */
import {
  bindingSource,
  columnInSource,
  columnRows,
  columnWindowPeriod,
  type VariableColumn,
} from "./add_columns";
import type { Graph, VariableShow } from "./api";
import { catalogHref } from "./catalog";
import { segmentYears, sourcePeriodLabel } from "./catalog_period";
import { inDatabase, type Space } from "./databases";
import type {
  LaneEdgeModel,
  LaneModel,
  LaneSpanModel,
  LaneSpanSegment,
} from "./LaneGraph";
import { periodWindowRelation } from "./period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";
import { columnDateRuns, columnYearRuns, studyWindow } from "./study_window";
import type { ColumnState } from "./ui/Pill";

const RANK: Record<ColumnState, number> = { out: 0, default: 1, in: 2 };

/**
 * A variable's spans, one lane per table and one row per run of its columns
 * there (#2408): a project binds the variable once per table, so a lane is
 * picked whole. Pressing any of its spans stages the variable's add, which
 * takes every column of that table inside the years to add, or, once it is in
 * the project, its removal. Successive spellings share a row; columns
 * delivered side by side take a row each (`columnRows`).
 */
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
  database = null,
  notInDatabase = new Set(),
  tableName = (table) => names.get(table) ?? "Table name unavailable",
}: {
  columns: readonly VariableColumn[];
  variable: Pick<VariableShow, "fqid" | "name">;
  names: ReadonlyMap<string, string>;
  project: ProjectData | null;
  selection: ProjectStudyWindow;
  axis: [number, number];
  catalogBounds: [number, number];
  /** Lane ids (`idPrefix` and table) staged for a change. */
  selected: ReadonlySet<string>;
  idPrefix?: string;
  /** Shows each column's "in project" state, with nothing to pick. */
  readOnly?: boolean;
  /** The database the project names: a span draws that database's years, and
   * a column it lacks draws its own years as outside it (#2344). */
  database?: Space | null;
  /** The tables whose binding of this variable the project's check says the
   * database lacks over its years (`not_in_database`). */
  notInDatabase?: ReadonlySet<string>;
  /** How the accessible name names a table. */
  tableName?: (table: string) => string;
}): LaneSpanModel[] {
  const study = studyWindow(project);
  const tables = [...new Set(columns.map((column) => column.table))];
  return tables.flatMap((table) => {
    const lane = `${idPrefix}${table}`;
    const source = bindingSource(project, variable.fqid, table);
    const parts = new Map(
      columns
        .filter((column) => column.table === table)
        .map((all) => {
          const column = {
            ...all,
            segments: inDatabase(all.segments, database),
          };
          const absent = column.segments.length === 0;
          const drawn = absent ? all : column;
          return [
            all.id,
            {
              drawn,
              absent,
              inside: !absent && columnWindowPeriod(column, selection) !== null,
              taken: !!source && columnInSource(drawn, source),
            },
          ];
        }),
    );
    const every = [...parts.values()];
    const absent = every.every((part) => part.absent);
    const inside = every.some((part) => part.inside);
    // Whether a committed binding can be ordered from the database turns on
    // its requested years, its other column names and its successor: the
    // project's check decides, and the page shows its verdict.
    const unusable = !!source && notInDatabase.has(table);
    const relation = source
      ? periodWindowRelation(
          source.period,
          study,
          every.flatMap((part) =>
            columnYearRuns(part.drawn.segments, catalogBounds, axis[0]).map(
              (era): [string, string] => [
                `${era.from}-01-01`,
                `${era.to}-12-31`,
              ],
            ),
          ),
        )
      : null;
    const sourceLabel = sourcePeriodLabel(source?.period);
    // How a committed binding stands to the study window and the project's
    // database, in its accessible name.
    const note = readOnly
      ? source
        ? "in project"
        : "read only"
      : unusable && database
        ? `In project, not in ${database.name} for its years.`
        : absent && database
          ? source
            ? `In project; this column name is not in ${database.name}.`
            : `Not in ${database.name}.`
          : relation === "disjoint"
            ? `In project, outside the study years (${sourceLabel}).`
            : relation && relation !== "same"
              ? `In project for ${sourceLabel}, which differs from the study years.`
              : source
                ? "in project, check to remove"
                : inside
                  ? "check to add"
                  : "outside the years to add";
    const selectable = !readOnly && !!project && (!!source || inside);
    const isSelected = selectable && selected.has(lane);
    const staged = isSelected ? (source ? "remove" : "add") : null;
    return columnRows(every.map((part) => part.drawn)).flatMap(
      (row, index): LaneSpanModel[] => {
        // In date order, so a spelling that returns draws its changes.
        const segments = row
          .flatMap((column): LaneSpanSegment[] => {
            const part = parts.get(column.id);
            if (!part) return [];
            const state: ColumnState = source
              ? part.taken
                ? "in"
                : readOnly
                  ? "default"
                  : "out"
              : readOnly || part.inside
                ? "default"
                : "out";
            return columnDateRuns(column.segments, catalogBounds, axis[0]).map(
              (run) => ({
                name: column.spelling,
                ...run,
                state,
                // The columns the staged change takes in or out.
                staged: staged === "add" ? part.inside : part.taken,
              }),
            );
          })
          .sort((a, b) => a.from - b.from);
        if (!segments.length) return [];
        const spellings = row.map((column) => column.spelling);
        const years = segmentYears(row.flatMap((column) => column.segments));
        return [
          {
            id: `${lane}#${index}`,
            lane,
            name: spellings.join(", "),
            tableRef: table,
            from: Math.min(...segments.map((segment) => segment.from)),
            to: Math.max(...segments.map((segment) => segment.to)),
            segments,
            years,
            columns:
              row.length > 1
                ? row.map((column) => ({
                    name: column.spelling,
                    years: segmentYears(column.segments),
                  }))
                : undefined,
            state: segments.reduce<ColumnState>(
              (best, segment) =>
                segment.state && RANK[segment.state] > RANK[best]
                  ? segment.state
                  : best,
              "out",
            ),
            selectable,
            isSelected: readOnly ? undefined : isSelected,
            staged,
            blocked: !readOnly && (relation === "disjoint" || unusable),
            accessibleName: [
              ...spellings,
              variable.name,
              tableName(table),
              years,
              note,
            ]
              .filter(Boolean)
              .join(", "),
          },
        ];
      },
    );
  });
}

/** One-hop succession on the coverage drawing; API subjects are the newer end. */
export function successionLanes({
  focusSpans,
  members = new Map(),
  graph,
  neighbourColumns,
  names,
  project,
  axis,
  catalogBounds,
}: {
  focusSpans: readonly LaneSpanModel[];
  /** The page's other subjects' spans (a group's other members): a relation
   * to one is drawn between the two lanes, with no neighbour lane. */
  members?: ReadonlyMap<string, readonly LaneSpanModel[]>;
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
  for (const [fqid, memberSpans] of members) byVariable.set(fqid, memberSpans);
  byVariable.set(graph.focus, focusSpans);
  const nodes = new Map(graph.nodes.map((node) => [node.fqid, node]));
  for (const [fqid, columns] of neighbourColumns) {
    const node = nodes.get(fqid);
    if (
      !node ||
      node.kind !== "variable" ||
      fqid === graph.focus ||
      members.has(fqid)
    )
      continue;
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
      readOnly: true,
      tableName: (table) => tableNames.get(table) ?? "Table name unavailable",
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
      successor =
        successors.find((span) => span.tableRef === predecessor?.tableRef) ??
        successor;
    } else if (edge.to === graph.focus && successor) {
      predecessor =
        predecessors.find((span) => span.tableRef === successor?.tableRef) ??
        predecessor;
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
