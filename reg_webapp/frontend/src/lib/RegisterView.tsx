import { useMemo, useState } from "react";
import type { Context, RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel, segmentYears } from "./catalog_period";
import { dataAxis } from "./period";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import {
  datedYearRuns,
  studyWindow,
  variablePeriods,
  variableYearRuns,
} from "./study_window";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { FilterChip } from "./ui/FilterChip";
import { Panel } from "./ui/Panel";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { windowBounds } from "./window_bounds";

/** A register's page. A table's address renders it too, with that table's
 * chip on (`focus`): a table has no page of its own (maintainer, #2316). */
export function RegisterView({
  node,
  focus,
  context,
}: {
  node: RegisterShow;
  /** The table whose address this is, or null. */
  focus: string | null;
  context: Context | null;
}) {
  const { draft } = useProject();
  const bound = useMemo(
    () =>
      new Set(
        draft?.sources.flatMap((source) =>
          source.bindings.map((binding) => binding.variable),
        ) ?? [],
      ),
    [draft],
  );
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const bounds = [catalogBounds.min, catalogBounds.max] as const;
  const deliveries = datedYearRuns(node.tables, bounds);
  const axis = dataAxis(
    deliveries.flatMap((run) => [run.from, run.to]),
    study,
    bounds,
  );
  const names = registerSubjectNames(node);
  // The table chips (the archived variant lens, maintainer 2026-11-03): OR
  // within the selection, a lens on the list only. A chip per table that
  // delivers a variable, so no chip narrows the list to nothing; with one such
  // table there is nothing to filter on, and a table address with no chip
  // shows the whole list. The page is keyed by register and table address, so
  // the selection starts afresh on each.
  const delivering = new Set(node.variables.flatMap((v) => v.tables));
  const chipTables = [...node.tables]
    .sort((a, b) => svCollator.compare(a.name, b.name))
    .filter((table) => delivering.has(table.fqid));
  const [selected, setSelected] = useState<ReadonlySet<string>>(() =>
    chipTables.length > 1 && focus !== null && delivering.has(focus)
      ? new Set([focus])
      : new Set(),
  );
  const shown =
    selected.size === 0
      ? node.variables
      : node.variables.filter((v) => v.tables.some((t) => selected.has(t)));
  function toggleTable(fqid: string): void {
    const next = new Set(selected);
    if (!next.delete(fqid)) next.add(fqid);
    setSelected(next);
  }
  return (
    <article className={styles.page}>
      <Definitions
        kind="register"
        title={node.name}
        description={node.description ?? undefined}
      />
      {/* The list heads itself on its axis row (#2316); only the empty state
          needs the panel's title. */}
      <Panel
        title={node.variables.length === 0 ? "Variables" : undefined}
        flush={node.variables.length > 0}
      >
        {node.variables.length === 0 ? (
          <EmptyState title="This register lists no variables." />
        ) : (
          <>
            {chipTables.length > 1 && (
              <fieldset className={styles.tableFilter}>
                <legend className="micro-label">Tables</legend>
                {chipTables.map((table) => (
                  <FilterChip
                    key={table.fqid}
                    isSelected={selected.has(table.fqid)}
                    onChange={() => toggleTable(table.fqid)}
                  >
                    {table.name}
                    {catalogPeriodLabel(table.period) && (
                      <span className={styles.period}>
                        {catalogPeriodLabel(table.period)}
                      </span>
                    )}
                  </FilterChip>
                ))}
                {/* Mounted empty: a live region inserted with its first text
                    is not announced. */}
                <span className={styles.shown} aria-live="polite">
                  {selected.size > 0
                    ? `${shown.length} of ${node.variables.length} shown`
                    : ""}
                </span>
              </fieldset>
            )}
            {/* Never empty: every chip's table delivers a variable. */}
            <VariableRowList label="Variables" axis={axis}>
              {[...shown]
                .sort((a, b) => svCollator.compare(a.name, b.name))
                .map((variable) => {
                  const coverage = variableYearRuns(
                    variable,
                    node.tables,
                    bounds,
                  );
                  return (
                    <VariableRow
                      key={variable.fqid}
                      name={variable.name}
                      href={catalogHref(variable.fqid)}
                      inProject={bound.has(variable.fqid)}
                      coverage={coverage}
                      years={segmentYears(
                        variablePeriods(variable, node.tables),
                      )}
                      axis={axis}
                      window={study ? [study.from, study.to] : null}
                    />
                  );
                })}
            </VariableRowList>
          </>
        )}
      </Panel>
      <RelatedDocumentsPanel fqids={[node.fqid]} names={names} filterable />
    </article>
  );
}
