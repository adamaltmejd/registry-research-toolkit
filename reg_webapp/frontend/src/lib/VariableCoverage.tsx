import { useRef, useState } from "react";
import { addColumns, columnInProject, variableColumns } from "./add_columns";
import {
  type CatalogSegment,
  type Context,
  errMessage,
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
import { variableSpans } from "./picker_graph";
import type { ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import { columnYearRuns, studyWindow } from "./study_window";
import { Button } from "./ui/Button";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useProject } from "./use_project";
import styles from "./VariableCoverage.module.css";
import { windowBounds } from "./window_bounds";

/** The variable's first lane. Succession lanes can join this same drawing. */
export function useVariableCoverage({
  node,
  segments,
  names,
  context,
}: {
  node: VariableShow;
  segments: readonly CatalogSegment[];
  names: ReadonlyMap<string, string>;
  context: Context | null;
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
    [...runs.flatMap((run) => [run.from, run.to]), active.from, active.to],
    study,
    [bounds.min, bounds.max],
  );
  const picked = columns.filter(
    (column) =>
      selected.has(column.id) &&
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
  const years = [
    ...new Set(
      runs.flatMap((run) =>
        Array.from({ length: run.to - run.from + 1 }, (_, i) => run.from + i),
      ),
    ),
  ];
  const outside = study ? uncoveredYears(study, available) : [];
  const onRest = (next: ProjectStudyWindow) =>
    setPeriod(sameYearWindow(next, seed) ? null : next);
  const add = () => {
    const current = projectStore.draft;
    if (!current || picked.length === 0) return;
    try {
      const candidate = addColumns(current, node.fqid, picked, active);
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
                  setPeriod(null);
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
          lanes={[{ id: node.fqid, name: node.name, current: true }]}
          spans={spans}
          edges={[]}
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
                onMove={setPeriod}
                onRest={onRest}
              />
            ) : undefined
          }
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
        {!draft && (
          <p className={styles.hint}>
            Create or open a project to select columns.
          </p>
        )}
      </Panel>
    ),
    addBar: (
      <>
        <StatusRow tone={error ? "error" : added ? "ok" : null} live="status">
          {error ?? (added ? "Added columns to project." : null)}
        </StatusRow>
        {picked.length > 0 && (
          <div className={styles.addBar}>
            <Button variant="primary" onPress={add}>
              Add {picked.length} {picked.length === 1 ? "column" : "columns"}{" "}
              to project
            </Button>
          </div>
        )}
      </>
    ),
  };
}
