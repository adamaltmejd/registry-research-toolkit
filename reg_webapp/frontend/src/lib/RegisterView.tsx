import { useMemo, useState } from "react";
import type { Context, RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { dataAxis } from "./period";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { columnYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { FilterChip } from "./ui/FilterChip";
import { Panel } from "./ui/Panel";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { windowBounds } from "./window_bounds";

export function RegisterView({
  node,
  context,
}: {
  node: RegisterShow;
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
  const deliveries = columnYearRuns(node.tables, bounds);
  const axis = dataAxis(
    deliveries.flatMap((run) => [run.from, run.to]),
    study,
    bounds,
  );
  const names = registerSubjectNames(node);
  // The table chips (the archived variant lens, maintainer 2026-11-03): OR
  // within the selection, a lens on the list only. A chip per table that
  // delivers a variable, so no chip narrows the list to nothing; with one such
  // table there is nothing to filter on. The selection belongs to the register.
  const [filter, setFilter] = useState<{
    register: string;
    tables: ReadonlySet<string>;
  }>({ register: node.fqid, tables: new Set() });
  const selected: ReadonlySet<string> =
    filter.register === node.fqid ? filter.tables : new Set();
  const tables = [...node.tables].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
  const delivering = new Set(node.variables.flatMap((v) => v.tables));
  const chipTables = tables.filter((table) => delivering.has(table.fqid));
  const shown =
    selected.size === 0
      ? node.variables
      : node.variables.filter((v) => v.tables.some((t) => selected.has(t)));
  function toggleTable(fqid: string): void {
    const next = new Set(selected);
    if (!next.delete(fqid)) next.add(fqid);
    setFilter({ register: node.fqid, tables: next });
  }
  return (
    <article className={styles.page}>
      <Definitions
        kind="register"
        title={node.name}
        definitions={[
          ...(node.description
            ? [{ term: "Description", value: node.description }]
            : []),
          {
            term: "Years",
            value: <code>{catalogPeriodLabel(node.period)}</code>,
          },
          // The route to each table's page (role, cadence, placements),
          // including tables no chip names.
          ...(tables.length > 0
            ? [
                {
                  term: "Tables",
                  value: tables.map((table, i) => (
                    <span key={table.fqid}>
                      {i > 0 && ", "}
                      <a className="link" href={catalogHref(table.fqid)}>
                        {table.name}
                      </a>
                    </span>
                  )),
                },
              ]
            : []),
        ]}
      />
      <Panel title="Variables" flush={node.variables.length > 0}>
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
                    <span className={styles.period}>
                      {catalogPeriodLabel(table.period)}
                    </span>
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
            <VariableRowList label="Variables">
              {[...shown]
                .sort((a, b) => svCollator.compare(a.name, b.name))
                .map((variable) => {
                  const coverage = columnYearRuns([variable], bounds, axis[0]);
                  return (
                    <VariableRow
                      key={variable.fqid}
                      name={variable.name}
                      href={catalogHref(variable.fqid)}
                      inProject={bound.has(variable.fqid)}
                      coverage={coverage.map(
                        (run) => [run.from, run.to] as const,
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
      <ScopedDataWarnings fqids={[node.fqid]} names={names} />
      <RelatedDocumentsPanel fqids={[node.fqid]} names={names} filterable />
    </article>
  );
}
