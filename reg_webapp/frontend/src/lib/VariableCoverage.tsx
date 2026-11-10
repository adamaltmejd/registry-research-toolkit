import { useRef, useState } from "react";
import {
  addColumns,
  columnInProject,
  columnWindowPeriod,
  type TablePeriodMetadata,
  type VariableColumn,
  variableColumns,
} from "./add_columns";
import {
  type CatalogSegment,
  type Context,
  errMessage,
  type Graph,
  type OrderCheck,
  reportPicks,
} from "./api";
import {
  graphRead,
  groupGraphReads,
  type Read,
  registerRead,
  registerSubjectNames,
  segmentsRead,
} from "./catalog_names";
import { segmentYears } from "./catalog_period";
import {
  chosenDatabase,
  databasesLine,
  inDatabase,
  stewardDatabases,
} from "./databases";
import {
  type LaneEdgeModel,
  LaneGraph,
  type LaneModel,
  type LaneSpanModel,
} from "./LaneGraph";
import { PeriodThumbs } from "./PeriodThumbs";
import {
  clampYearWindow,
  dataAxis,
  intersectCoverageWindow,
  sameYearWindow,
  uncoveredYears,
  yearWindowLabel,
} from "./period";
import { successionLanes, variableSpans } from "./picker_graph";
import type { ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import { columnYearRuns, studyWindow } from "./study_window";
import { AddBar } from "./ui/AddBar";
import { Button } from "./ui/Button";
import { StatusRow } from "./ui/StatusRow";
import { TypeTag } from "./ui/TypeTag";
import { useProject } from "./use_project";
import { useCatalogRead, useDistinct } from "./use_store_read";
import styles from "./VariableCoverage.module.css";
import { windowBounds } from "./window_bounds";

/** A variable the page draws and adds from. */
export interface CoverageSubject {
  fqid: string;
  name: string;
  segments: readonly CatalogSegment[];
  /** A group member: its heading row, over one lane per table. Without it
   * (the variable page) the tables' lanes are the lanes. */
  lane?: Omit<LaneModel, "id" | "relations">;
}

/** `key` is `[group, ...fqids]` as JSON: a group page reads its members' graphs
 * in one request, a variable page its own. */
const readGraphs = (key: string) => {
  const [group, ...fqids] = JSON.parse(key) as [string | null, ...string[]];
  return group
    ? groupGraphReads(group, fqids)
    : Object.fromEntries(fqids.map((fqid) => [fqid, graphRead(fqid)]));
};
const readNeighbourSegments = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((fqid) => [fqid, segmentsRead(fqid)]));
const readNeighbourRegisters = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((fqid) => [fqid, registerRead(fqid)]));

const failed = (reads: Record<string, Read<unknown>>) =>
  Object.values(reads).some((read) => !read.loading && read.value === null);
const pending = (reads: Record<string, Read<unknown>>) =>
  Object.values(reads).some((read) => read.loading);

/** The one-hop succession of the page's variables: their graphs, the columns
 * of the neighbours outside them, and the names of the neighbours' other
 * registers. A group page names its `group`, whose one read answers for every
 * member. */
export function useSuccession(
  fqids: readonly string[],
  register: string,
  group: string | null = null,
) {
  const graphReads = useCatalogRead(
    readGraphs,
    JSON.stringify([group, ...useDistinct(fqids)]),
  );
  const own = new Set(fqids);
  const neighbours = useCatalogRead(
    readNeighbourSegments,
    useDistinct(
      Object.values(graphReads).flatMap((read) =>
        (read.value?.nodes ?? [])
          .filter((entry) => entry.kind === "variable" && !own.has(entry.fqid))
          .map((entry) => entry.fqid),
      ),
    ),
  );
  const neighbourRegisters = useCatalogRead(
    readNeighbourRegisters,
    useDistinct(
      Object.keys(neighbours)
        .map((fqid) => fqid.split("/").slice(0, 2).join("/"))
        .filter((ref) => ref !== register),
    ),
  );
  const names = new Map<string, string>();
  for (const read of Object.values(neighbourRegisters)) {
    for (const [fqid, name] of registerSubjectNames(read.value))
      names.set(fqid, name);
  }
  return {
    graphs: Object.values(graphReads).flatMap((read) =>
      read.value ? [read.value] : [],
    ),
    neighbourColumns: new Map(
      Object.entries(neighbours).map(([fqid, read]) => [
        fqid,
        variableColumns(read.value ?? []),
      ]),
    ) as ReadonlyMap<string, readonly VariableColumn[]>,
    names: names as ReadonlyMap<string, string>,
    error: failed(graphReads)
      ? "Succession relations did not load."
      : failed(neighbours)
        ? "Neighbour columns did not load."
        : failed(neighbourRegisters)
          ? "Neighbour register names did not load."
          : null,
    loading:
      pending(graphReads) || pending(neighbours) || pending(neighbourRegisters),
  };
}

/** The page's columns and their read-only succession neighbours on one axis,
 * with the add flow over them: one subject on a variable page, every member
 * on a group page. */
export function useVariableCoverage({
  subjects,
  label,
  names,
  context,
  tables,
  validation,
  successions = [],
  neighbourColumns = new Map(),
  successionError = null,
  allowAdd = true,
  focus = null,
}: {
  subjects: readonly CoverageSubject[];
  /** The drawing's accessible name. */
  label: string;
  names: ReadonlyMap<string, string>;
  context: Context | null;
  tables: readonly TablePeriodMetadata[] | null;
  validation: OrderCheck | null;
  successions?: readonly Graph[];
  neighbourColumns?: ReadonlyMap<string, readonly VariableColumn[]>;
  successionError?: string | null;
  allowAdd?: boolean;
  /** The subject whose lane takes focus when first drawn. */
  focus?: string | null;
}) {
  const { draft, raw } = useProject();
  const [selected, setSelected] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [period, setPeriod] = useState<ProjectStudyWindow | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [added, setAdded] = useState(false);
  const heading = useRef<HTMLDivElement>(null);
  const bounds = windowBounds(context);
  const catalogBounds: [number, number] = [bounds.min, bounds.max];
  const study = studyWindow(draft);
  const databases = stewardDatabases(context);
  const database = chosenDatabase(context, draft);
  // What the add flow reads of a column: its rows in the project's database.
  const offered = (column: VariableColumn): VariableColumn => ({
    ...column,
    segments: inDatabase(column.segments, database),
  });
  // Every column of every subject by its span id: a group member's carries
  // its fqid, since two members can share a table and a column name.
  const prefix = (subject: CoverageSubject) =>
    subject.lane ? `${subject.fqid}|` : "";
  const columnsOf = new Map(
    subjects.map((subject) => [
      subject.fqid,
      variableColumns(subject.segments),
    ]),
  );
  const columns = subjects.flatMap((subject) =>
    (columnsOf.get(subject.fqid) ?? []).map((column) => ({
      id: `${prefix(subject)}${column.id}`,
      subject,
      column,
    })),
  );
  const runs = columnYearRuns(
    subjects.flatMap((subject) => subject.segments),
    catalogBounds,
  );
  const delivered = segmentYears(
    subjects.flatMap((subject) => subject.segments),
  );
  const available = runs.length
    ? { from: runs[0].from, to: runs[runs.length - 1].to }
    : null;
  const seed = intersectCoverageWindow(
    available,
    study,
    bounds.min,
    bounds.max,
  );
  const active = available
    ? clampYearWindow(period ?? seed, available.from, available.to)
    : seed;
  const differs = !sameYearWindow(active, seed);
  const axis = dataAxis(
    [
      ...runs.flatMap((run) => [run.from, run.to]),
      ...[...neighbourColumns.values()].flatMap((entries) =>
        entries.flatMap((column) =>
          columnYearRuns(column.segments, catalogBounds).flatMap((run) => [
            run.from,
            run.to,
          ]),
        ),
      ),
      active.from,
      active.to,
    ],
    study,
    catalogBounds,
  );
  const picked = columns.filter(
    ({ id, subject, column }) =>
      selected.has(id) &&
      columnWindowPeriod(offered(column), active) !== null &&
      !columnInProject(draft, subject.fqid, column.table),
  );
  const spansOf = new Map(
    subjects.map((subject) => [
      subject.fqid,
      variableSpans({
        columns: columnsOf.get(subject.fqid) ?? [],
        variable: subject,
        names,
        project: draft,
        selection: active,
        axis,
        catalogBounds,
        selected,
        idPrefix: prefix(subject),
        readOnly: !allowAdd,
        database,
      }),
    ]),
  );
  const spans = [...spansOf.values()].flat();
  const neighbours = mergeSuccession(
    successions.map((graph) =>
      successionLanes({
        focusSpans: spansOf.get(graph.focus) ?? [],
        members: new Map([...spansOf].filter(([fqid]) => fqid !== graph.focus)),
        graph,
        neighbourColumns,
        names,
        project: draft,
        axis,
        catalogBounds,
      }),
    ),
  );
  const years = [
    ...new Set(
      runs.flatMap((run) =>
        Array.from({ length: run.to - run.from + 1 }, (_, i) => run.from + i),
      ),
    ),
  ];
  const outside = study ? uncoveredYears(study, available) : [];
  const setAddWindow = (next: ProjectStudyWindow | null) => {
    setPeriod(next);
    setSelected(
      (previous) =>
        new Set(
          columns
            .filter(
              ({ id, column }) =>
                previous.has(id) &&
                columnWindowPeriod(offered(column), next ?? seed) !== null,
            )
            .map(({ id }) => id),
        ),
    );
  };
  const onRest = (next: ProjectStudyWindow) =>
    setAddWindow(sameYearWindow(next, seed) ? null : next);
  const classificationWarning =
    added &&
    validation?.issues.some(
      (issue) =>
        issue.code === "classification_version_ambiguous" &&
        subjects.some((subject) => subject.fqid === issue.variable),
    );
  const add = () => {
    const current = projectStore.draft;
    if (!allowAdd || !current || picked.length === 0) return;
    try {
      if (!tables)
        throw new Error(
          "Table availability is not loaded. Reload the page and try again.",
        );
      // A project binds a variable once per table, so ticking one span adds the
      // variable's years in every column and file of that table inside the
      // window; each variable is added on top of the one before.
      let candidate = current;
      const picks: string[] = [];
      for (const subject of subjects) {
        const adding = columns
          .filter(
            ({ subject: owner, column }) =>
              owner === subject &&
              picked.some(
                (pick) =>
                  pick.subject === subject &&
                  pick.column.table === column.table,
              ) &&
              columnWindowPeriod(offered(column), active) !== null,
          )
          .map(({ column }) => offered(column));
        if (adding.length) {
          candidate = addColumns(
            candidate,
            subject.fqid,
            adding,
            active,
            tables,
          );
          picks.push(subject.fqid);
        }
      }
      projectStore.updateField("sources", candidate.sources);
      reportPicks(picks);
      setSelected(new Set());
      setError(null);
      setAdded(true);
      // The add bar and the selected checkboxes disappear after committing.
      heading.current?.focus();
    } catch (cause) {
      setError(`Columns were not added. ${errMessage(cause)}`);
    }
  };
  return {
    graph: !context ? null : columns.length === 0 ? (
      <p className={styles.hint}>
        No years recorded for this {subjects.length > 1 ? "group" : "variable"}.
      </p>
    ) : (
      <div ref={heading} tabIndex={-1} className={styles.timeline}>
        {allowAdd && differs && (
          <div className={styles.header}>
            <Button
              variant="ghost"
              onPress={() => {
                setAddWindow(null);
                heading.current?.focus();
              }}
            >
              Clear period
            </Button>
          </div>
        )}
        <span className="visually-hidden">
          {delivered && `Years ${delivered}.`}
          {allowAdd && ` Years to add ${yearWindowLabel(active)}.`}
          {allowAdd && outside.length > 0
            ? ` No delivery in ${outside.map(yearWindowLabel).join(", ")}.`
            : ""}
        </span>
        <LaneGraph
          label={label}
          lanes={[
            ...subjects.flatMap((subject): LaneModel[] => {
              // A lane per table, titled by the table, as on the variable page;
              // a group member heads its tables with its own row, which draws
              // no spans (maintainer ruling, 2026-11-10, #2395).
              const tables = [
                ...new Set(
                  (columnsOf.get(subject.fqid) ?? []).map(
                    (column) => column.table,
                  ),
                ),
              ].map((table): LaneModel => {
                const id = `${prefix(subject)}${table}`;
                const line = databasesLine(
                  subject.segments.filter((segment) => segment.table === table),
                  databases,
                  database,
                );
                return {
                  id,
                  name: names.get(table) ?? "Table name unavailable",
                  // Which databases have the table's rows (#2344): the line
                  // wraps, since a name cut off would hide a database.
                  sub: line ? { text: line } : null,
                  wrap: !!line,
                  pill: <TypeTag type="table" glyph />,
                  nested: !!subject.lane,
                  relations: neighbours.relations.get(id),
                };
              });
              return subject.lane
                ? [{ ...subject.lane, id: subject.fqid }, ...tables]
                : tables;
            }),
            ...neighbours.lanes,
          ]}
          spans={[...spans, ...neighbours.spans]}
          edges={neighbours.edges}
          axis={axis}
          window={study ? [study.from, study.to] : null}
          addWindow={
            allowAdd && (differs || !study) ? [active.from, active.to] : null
          }
          coverage={{
            covered: available,
            years,
            outside,
            // A variable page heads its drawing with the variable: its lanes
            // are its tables (#2319).
            caption:
              subjects.length === 1 && !subjects[0].lane
                ? subjects[0].name
                : undefined,
          }}
          overlay={
            allowAdd && available ? (
              <PeriodThumbs
                axis={axis}
                selection={active}
                bounds={available}
                label="Years to add"
                onMove={setAddWindow}
                onRest={onRest}
              />
            ) : undefined
          }
          isDisabled={!tables}
          focus={focus}
          onToggle={
            allowAdd
              ? (id) => {
                  setSelected((previous) => {
                    const next = new Set(previous);
                    if (next.has(id)) next.delete(id);
                    else next.add(id);
                    return next;
                  });
                  setAdded(false);
                  setError(null);
                }
              : undefined
          }
        />
        {successionError && (
          <StatusRow tone="error" live="alert">
            {successionError} Reload the page to try again.
          </StatusRow>
        )}
        {/* A rejected project is a recovery copy: fixing or replacing it is the
            project page's, behind its confirmation, never a one-click create. */}
        {allowAdd && !draft && raw !== null && (
          <p className={styles.create}>
            Fix the project on the{" "}
            <a className="link" href="/project">
              project page
            </a>{" "}
            to add columns.
          </p>
        )}
      </div>
    ),
    addBar: allowAdd && (
      <>
        <StatusRow
          tone={
            error
              ? "error"
              : classificationWarning
                ? "info"
                : added
                  ? "ok"
                  : null
          }
          live="status"
        >
          {error ??
            (added ? (
              <>
                Added columns to project.
                {classificationWarning && (
                  <>
                    {" "}
                    Choose the classification on the{" "}
                    <a className="link" href="/project">
                      project page
                    </a>
                    .
                  </>
                )}
              </>
            ) : null)}
        </StatusRow>
        {picked.length > 0 && <AddBar count={picked.length} onAdd={add} />}
      </>
    ),
  };
}

/** One page's succession merged over its variables' graphs: a neighbour two
 * members share is one lane, and a relation between two members, which both
 * their graphs carry, one arrow. */
function mergeSuccession(parts: readonly ReturnType<typeof successionLanes>[]) {
  const lanes = new Map<string, LaneModel>();
  const spans = new Map<string, LaneSpanModel>();
  const edges = new Map<string, LaneEdgeModel>();
  const relations = new Map<string, string[]>();
  for (const part of parts) {
    for (const lane of part.lanes)
      if (!lanes.has(lane.id)) lanes.set(lane.id, lane);
    for (const span of part.spans)
      if (!spans.has(span.id)) spans.set(span.id, span);
    for (const edge of part.edges)
      if (!edges.has(edge.id)) edges.set(edge.id, edge);
    for (const [lane, sentences] of part.relations)
      relations.set(lane, [
        ...new Set([...(relations.get(lane) ?? []), ...sentences]),
      ]);
  }
  return {
    lanes: [...lanes.values()].map((lane) => ({
      ...lane,
      relations: relations.get(lane.id),
    })),
    spans: [...spans.values()],
    edges: [...edges.values()],
    relations,
  };
}
