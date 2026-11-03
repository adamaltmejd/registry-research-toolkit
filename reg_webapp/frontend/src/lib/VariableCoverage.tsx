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
  type ValidationResultModel,
  type VariableShow,
} from "./api";
import { LaneGraph } from "./LaneGraph";
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
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useProject } from "./use_project";
import styles from "./VariableCoverage.module.css";
import { windowBounds } from "./window_bounds";

/** The focus columns and their read-only succession neighbours on one axis. */
export function useVariableCoverage({
  node,
  segments,
  names,
  context,
  tables,
  validation,
  succession = null,
  neighbourColumns = new Map(),
  successionError = null,
  successionLoading = false,
}: {
  node: VariableShow;
  segments: readonly CatalogSegment[];
  names: ReadonlyMap<string, string>;
  context: Context | null;
  tables: readonly TablePeriodMetadata[] | null;
  validation: ValidationResultModel | null;
  succession?: Graph | null;
  neighbourColumns?: ReadonlyMap<string, readonly VariableColumn[]>;
  successionError?: string | null;
  successionLoading?: boolean;
}) {
  const { draft } = useProject();
  const [selected, setSelected] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [period, setPeriod] = useState<ProjectStudyWindow | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [added, setAdded] = useState(false);
  const heading = useRef<HTMLSpanElement>(null);
  const bounds = windowBounds(context);
  const study = studyWindow(draft);
  const columns = variableColumns(segments);
  const runs = columnYearRuns(segments, [bounds.min, bounds.max]);
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
          columnYearRuns(column.segments, [bounds.min, bounds.max]).flatMap(
            (run) => [run.from, run.to],
          ),
        ),
      ),
      active.from,
      active.to,
    ],
    study,
    [bounds.min, bounds.max],
  );
  const picked = columns.filter(
    (column) =>
      selected.has(column.id) &&
      columnWindowPeriod(column, active) !== null &&
      !columnInProject(draft, node.fqid, column.table),
  );
  const spans = variableSpans({
    columns,
    variable: node,
    names,
    project: draft,
    selection: active,
    axis,
    catalogBounds: [bounds.min, bounds.max],
    selected,
  });
  const neighbours = successionLanes({
    focusSpans: spans,
    graph: succession,
    neighbourColumns,
    names,
    project: draft,
    axis,
    catalogBounds: [bounds.min, bounds.max],
  });
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
              (column) =>
                previous.has(column.id) &&
                columnWindowPeriod(column, next ?? seed) !== null,
            )
            .map((column) => column.id),
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
        issue.variable === node.fqid,
    );
  const add = () => {
    const current = projectStore.draft;
    if (!current || picked.length === 0) return;
    try {
      if (!tables)
        throw new Error(
          "Table availability is not loaded. Reload the page and try again.",
        );
      const candidate = addColumns(current, node.fqid, picked, active, tables);
      projectStore.updateField("sources", candidate.sources);
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
    graph: segments.length > 0 && context && (
      <Panel
        title={
          <span className={styles.header}>
            <span ref={heading} tabIndex={-1} className={styles.heading}>
              <span className="visually-hidden">Coverage </span>
              <span>{runs.map(yearWindowLabel).join(", ")}</span>
            </span>
            {differs && (
              <Button
                variant="ghost"
                onPress={() => {
                  setAddWindow(null);
                  heading.current?.focus();
                }}
              >
                Clear period
              </Button>
            )}
          </span>
        }
      >
        <span className="visually-hidden">
          Add window {yearWindowLabel(active)}.
          {outside.length > 0
            ? ` No delivery in ${outside.map(yearWindowLabel).join(", ")}.`
            : ""}
        </span>
        <LaneGraph
          label="Variable columns"
          lanes={[
            ...[...new Set(columns.map((column) => column.table))].map(
              (table) => ({
                id: table,
                name: node.name,
                sub: { text: names.get(table) ?? "Table name unavailable" },
                current: true,
                relations: neighbours.relations,
              }),
            ),
            ...neighbours.lanes,
          ]}
          spans={[...spans, ...neighbours.spans]}
          edges={neighbours.edges}
          axis={axis}
          window={study ? [study.from, study.to] : null}
          addWindow={differs || !study ? [active.from, active.to] : null}
          coverage={{
            covered: available,
            years,
            outside,
            bandName: study ? "Study window" : null,
          }}
          overlay={
            available ? (
              <PeriodThumbs
                axis={axis}
                selection={active}
                bounds={available}
                label="Add window (years)"
                onMove={setAddWindow}
                onRest={onRest}
              />
            ) : undefined
          }
          isDisabled={!tables}
          onToggle={(id) => {
            setSelected((previous) => {
              const next = new Set(previous);
              if (next.has(id)) next.delete(id);
              else next.add(id);
              return next;
            });
            setAdded(false);
            setError(null);
          }}
        />
        <div aria-busy={successionLoading || undefined}>
          {successionError && (
            <StatusRow tone="error" live="alert">
              Succession lanes did not load ({successionError}). Reload the page
              to try again.
            </StatusRow>
          )}
        </div>
        {!draft && (
          <p className={styles.hint}>
            Create or open a project to select columns.
          </p>
        )}
      </Panel>
    ),
    addBar: (
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
