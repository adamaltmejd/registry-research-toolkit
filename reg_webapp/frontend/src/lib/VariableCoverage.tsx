import { useRef, useState } from "react";
import {
  addColumns,
  bindingSource,
  columnWindowPeriod,
  removeBindings,
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
  // Only a live project can take columns: without one there are no thumbs,
  // no shading and no years to add (maintainer, #2399, 2026-11-11).
  const live = allowAdd && draft !== null;
  const [selected, setSelected] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [period, setPeriod] = useState<ProjectStudyWindow | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<"added" | "removed" | null>(null);
  const added = done === "added";
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
  // What is picked is a variable in a table, its lane (#2408): a project
  // binds a variable once per table, so a press stages every column of that
  // table inside the years to add, or, once bound, the binding's removal.
  const bindings = subjects.flatMap((subject) => {
    const columns = columnsOf.get(subject.fqid) ?? [];
    return [...new Set(columns.map((column) => column.table))].map((table) => ({
      key: `${prefix(subject)}${table}`,
      subject,
      table,
      columns: columns.filter((column) => column.table === table),
      source: bindingSource(draft, subject.fqid, table),
    }));
  });
  const inWindow = (
    binding: (typeof bindings)[number],
    window: ProjectStudyWindow,
  ) =>
    binding.columns.filter(
      (column) => columnWindowPeriod(offered(column), window) !== null,
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
  // A period dragged before the project closed no longer applies.
  const active =
    live && available
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
  const adds = bindings.filter(
    (binding) =>
      selected.has(binding.key) &&
      !binding.source &&
      inWindow(binding, active).length > 0,
  );
  const removes = bindings.filter(
    (binding) => selected.has(binding.key) && binding.source,
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
        notInDatabase: new Set(
          (validation?.issues ?? []).flatMap((issue) =>
            issue.code === "not_in_database" &&
            issue.variable === subject.fqid &&
            issue.table
              ? [issue.table]
              : [],
          ),
        ),
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
          bindings
            .filter(
              (binding) =>
                previous.has(binding.key) &&
                (binding.source || inWindow(binding, next ?? seed).length > 0),
            )
            .map(({ key }) => key),
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
  // The bar's buttons and the checked spans go after committing.
  const committed = (outcome: "added" | "removed", keys: Set<string>) => {
    setSelected(
      (previous) => new Set([...previous].filter((key) => !keys.has(key))),
    );
    setError(null);
    setDone(outcome);
    heading.current?.focus();
  };
  const add = () => {
    const current = projectStore.draft;
    if (!allowAdd || !current || adds.length === 0) return;
    try {
      if (!tables)
        throw new Error(
          "Table availability is not loaded. Reload the page and try again.",
        );
      // Each variable is added on top of the one before, with the years of
      // every column of its picked tables inside the window.
      let candidate = current;
      const picks: string[] = [];
      for (const subject of subjects) {
        const adding = adds
          .filter((binding) => binding.subject === subject)
          .flatMap((binding) => inWindow(binding, active).map(offered));
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
      committed("added", new Set(adds.map(({ key }) => key)));
    } catch (cause) {
      setError(`Not added to the project. ${errMessage(cause)}`);
    }
  };
  // A source whose every variable is staged to go leaves the project with
  // them; the bar says so before the press (Operator, #2408).
  const emptied = [
    ...new Set(removes.flatMap(({ source }) => (source ? [source] : []))),
  ].filter((source) =>
    source.bindings.every((binding) =>
      removes.some(
        (staged) =>
          staged.source === source && staged.subject.fqid === binding.variable,
      ),
    ),
  );
  const removalNote = emptied.length
    ? `Also removes ${listNames(
        emptied.map((source) => names.get(source.table) ?? source.name),
      )} from the project.`
    : null;
  const remove = () => {
    const current = projectStore.draft;
    if (!allowAdd || !current || removes.length === 0) return;
    try {
      const candidate = removeBindings(
        current,
        removes.map(({ subject, table }) => ({
          variable: subject.fqid,
          table,
        })),
      );
      // A source that goes takes its panel memberships with it.
      projectStore.updateField({
        sources: candidate.sources,
        panels: candidate.panels,
      });
      committed("removed", new Set(removes.map(({ key }) => key)));
    } catch (cause) {
      setError(`Not removed from the project. ${errMessage(cause)}`);
    }
  };
  return {
    graph: !context ? null : bindings.length === 0 ? (
      <p className={styles.hint}>
        No years recorded for this {subjects.length > 1 ? "group" : "variable"}.
      </p>
    ) : (
      <div ref={heading} tabIndex={-1} className={styles.timeline}>
        {/* Its row is kept while it is hidden, so nothing moves when it
            shows (#2408); hidden, it is neither a tab stop nor read. */}
        {live && (
          <div
            className={styles.header}
            style={differs ? undefined : { visibility: "hidden" }}
          >
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
          {live && ` Years to add ${yearWindowLabel(active)}.`}
          {live && outside.length > 0
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
                const onTable = subject.segments.filter(
                  (segment) => segment.table === table,
                );
                const line = databasesLine(onTable, databases, database);
                const own = inDatabase(onTable, database);
                return {
                  id,
                  name: names.get(table) ?? "Table name unavailable",
                  years: segmentYears(own.length ? own : onTable),
                  // Which databases have the table's rows (#2344): the line
                  // wraps, since a name cut off would hide a database.
                  sub: line ? { text: line } : null,
                  wrap: !!line,
                  row: "table",
                  nested: !!subject.lane,
                  relations: neighbours.relations.get(id),
                };
              });
              return subject.lane
                ? [
                    { ...subject.lane, id: subject.fqid, row: "heading" },
                    ...tables,
                  ]
                : tables;
            }),
            ...neighbours.lanes,
          ]}
          spans={[...spans, ...neighbours.spans]}
          edges={neighbours.edges}
          axis={axis}
          window={study ? [study.from, study.to] : null}
          addWindow={
            live && (differs || !study) ? [active.from, active.to] : null
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
            live && available ? (
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
                  // A span stands for its lane: the variable in that table.
                  const lane = spans.find((span) => span.id === id)?.lane;
                  if (!lane) return;
                  setSelected((previous) => {
                    const next = new Set(previous);
                    if (next.has(lane)) next.delete(lane);
                    else next.add(lane);
                    return next;
                  });
                  setDone(null);
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
                : done
                  ? "ok"
                  : null
          }
          live="status"
        >
          {error ??
            (done === "removed" ? (
              "Removed from project."
            ) : added ? (
              <>
                Added to project.
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
        {(adds.length > 0 || removes.length > 0) && (
          <AddBar
            adds={new Set(adds.map(({ subject }) => subject.fqid)).size}
            removes={removes.length > 0}
            note={removalNote}
            onAdd={add}
            onRemove={remove}
          />
        )}
      </>
    ),
  };
}

/** "A", "A and B", "A, B and C". */
function listNames(items: readonly string[]): string {
  return items.length < 2
    ? (items[0] ?? "")
    : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`;
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
