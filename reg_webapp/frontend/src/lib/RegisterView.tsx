import { useMemo, useState } from "react";
import type { Context, RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, rankVariables, svCollator } from "./catalog";
import { registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel, segmentYears } from "./catalog_period";
import { FilterInput } from "./FilterInput";
import { Highlight } from "./Highlight";
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
import { EntityPill } from "./ui/EntityPill";
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
  const chosen =
    selected.size === 0
      ? node.variables
      : node.variables.filter((v) => v.tables.some((t) => selected.has(t)));
  // The variable filter (#2403): the variables column's box, on the list the
  // chips leave. The page is keyed by address, so it starts empty on each.
  const [needle, setNeedle] = useState("");
  const shown = rankVariables(
    [...chosen].sort((a, b) => svCollator.compare(a.name, b.name)),
    needle,
  );
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
            <div className={styles.listFilter}>
              <FilterInput
                value={needle}
                onChange={setNeedle}
                total={chosen.length}
                shown={shown.length}
                label="Filter this register's variables"
                placeholder="Filter variables…"
              />
            </div>
            {/* The box first, then the table pills, flush with it, then the
                list (#2407, maintainer decisions 2026-11-11). The pills have
                no visible label: their glyph and pine "on" face carry it. */}
            {chipTables.length > 1 && (
              <fieldset className={styles.tableFilter}>
                <legend className="visually-hidden">Filter by table</legend>
                {chipTables.map((table) => (
                  <FilterChip
                    key={table.fqid}
                    isSelected={selected.has(table.fqid)}
                    onChange={() => toggleTable(table.fqid)}
                  >
                    <EntityPill
                      entity="table"
                      name={table.name}
                      years={catalogPeriodLabel(table.period) ?? undefined}
                    />
                  </FilterChip>
                ))}
                {/* Mounted empty: a live region inserted with its first text
                    is not announced. */}
                <span className={styles.shown} aria-live="polite">
                  {selected.size > 0
                    ? `${chosen.length} of ${node.variables.length} shown`
                    : ""}
                </span>
              </fieldset>
            )}
            {/* Empty only through the filter: every chip's table delivers a
                variable. */}
            {shown.length === 0 ? (
              <EmptyState
                title={`No variables match “${needle.trim()}”.`}
                description="Clear the filter to list them all."
              />
            ) : (
              <VariableRowList label="Variables" axis={axis}>
                {shown.map((variable) => {
                  const coverage = variableYearRuns(
                    variable,
                    node.tables,
                    bounds,
                  );
                  return (
                    <VariableRow
                      key={variable.fqid}
                      name={<Highlight text={variable.name} needle={needle} />}
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
            )}
          </>
        )}
      </Panel>
      <RelatedDocumentsPanel fqid={node.fqid} names={names} filterable />
    </article>
  );
}
