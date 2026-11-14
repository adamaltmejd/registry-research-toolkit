import { type ReactNode, useEffect, useMemo, useState } from "react";
import type { CatalogPeriod, Context, Coverage, RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, rankVariables, svCollator } from "./catalog";
import { coverageRead, registerSubjectNames } from "./catalog_names";
import { segmentYears } from "./catalog_period";
import { type Space, stewardDatabases } from "./databases";
import { FilterInput } from "./FilterInput";
import { Highlight } from "./Highlight";
import { dataAxis } from "./period";
import { catalogBindings } from "./project_data";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import {
  lensKeyId,
  lensTables,
  publishRegisterLens,
  useRegisterLensOf,
} from "./register_lens";
import { SeriesBreaks } from "./SeriesBreaks";
import { type PlacedBreak, placeBreaks } from "./series_breaks";
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
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { useCatalogRead } from "./use_store_read";
import { windowBounds } from "./window_bounds";

type Table = RegisterShow["tables"][number];

/** A table row of the register page: the table's years in all its databases,
 * and each database's on a steward with several. */
interface TableRow {
  table: Table;
  spans: CatalogPeriod[];
  databases: { space: Space; spans: CatalogPeriod[] }[];
}

/** The rows of the tables a variable is delivered in, by name, from the
 * register's `coverage`; their databases in the steward's order. */
function tableRows(
  tables: readonly Table[],
  coverage: Coverage,
  databases: readonly Space[] | null,
): TableRow[] {
  return [...tables]
    .sort((a, b) => svCollator.compare(a.name, b.name))
    .map((table) => {
      const held = coverage.tables.filter((t) => t.fqid === table.fqid);
      return {
        table,
        spans: held.flatMap((t) => t.spans),
        databases: (databases ?? []).flatMap((space) => {
          const spans = held
            .filter((t) => t.space === space.id)
            .flatMap((t) => t.spans);
          return spans.length > 0 ? [{ space, spans }] : [];
        }),
      };
    });
}

/** A row's bar and its years in words, from its periods. */
function bar(
  periods: readonly CatalogPeriod[],
  bounds: readonly [number, number],
) {
  const rows = periods.map((period) => ({ period }));
  return { coverage: datedYearRuns(rows, bounds), years: segmentYears(rows) };
}

/** A register's page. A table's address renders it too, with that table's
 * filter on (`focus`): a table has no page of its own (maintainer, #2316). */
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
          catalogBindings(source).map((binding) => binding.variable),
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
  const studyBlock = study ? ([study.from, study.to] as const) : null;
  const names = registerSubjectNames(node);
  // The table filter (the archived variant lens, maintainer 2026-11-03; table
  // timelines, #2110, maintainer 2026-11-11): OR within the selection, a lens
  // on the lists only. A row per table that delivers a variable, so no filter
  // narrows the list to nothing, and under it a row per database on a steward
  // with several. It is offered when there are two things to filter by. The
  // page is keyed by register and table address, so the selection starts
  // afresh on each.
  const delivering = new Set(node.variables.flatMap((v) => v.tables));
  const filterTables = node.tables.filter((table) =>
    delivering.has(table.fqid),
  );
  const coverage = useCatalogRead(coverageRead, node.fqid);
  const databases = stewardDatabases(context);
  const rows = coverage.value
    ? tableRows(filterTables, coverage.value, databases)
    : null;
  const filterable =
    filterTables.length > 1 ||
    (rows?.some((row) => row.databases.length > 1) ?? false);
  const [selected, setSelected] = useState<ReadonlySet<string>>(() =>
    filterTables.length > 1 && focus !== null && delivering.has(focus)
      ? new Set([focus])
      : new Set(),
  );
  // The browse column narrows with the page (#2110, maintainer 2026-11-11).
  useEffect(() => {
    publishRegisterLens(node.fqid, selected);
    return () => publishRegisterLens(null);
  }, [node.fqid, selected]);
  const lens = useRegisterLensOf(node, selected);
  const chosen = lens.state === "on" ? lens.variables : node.variables;
  // The variable filter (#2403): the variables column's box, on the list the
  // table filter leaves. The page is keyed by address, so it starts empty on
  // each.
  const [needle, setNeedle] = useState("");
  const shown = rankVariables(
    [...chosen].sort((a, b) => svCollator.compare(a.name, b.name)),
    needle,
  );
  function toggle(key: string): void {
    const next = new Set(selected);
    if (!next.delete(key)) next.add(key);
    setSelected(next);
  }
  // The register's own breaks, then its tables' (and their releases'): the
  // selected tables' with a filter on, else every table's.
  const lensed = new Set(lensTables(node, selected).map((table) => table.fqid));
  const breaks: PlacedBreak[] = [
    ...placeBreaks(null, node.series_breaks),
    ...node.tables
      .filter((table) => selected.size === 0 || lensed.has(table.fqid))
      .flatMap((table) =>
        placeBreaks(table.implicit ? null : table.name, table.series_breaks),
      ),
  ];
  const chip = (key: string, pill: ReactNode) => (
    <FilterChip isSelected={selected.has(key)} onChange={() => toggle(key)}>
      {pill}
    </FilterChip>
  );
  return (
    <article className={styles.page}>
      <Definitions
        kind="register"
        title={node.name}
        description={node.description ?? undefined}
      />
      <SeriesBreaks breaks={breaks} />
      {/* The lists head themselves on their axis rows (#2316); only the empty
          state needs the panel's title. */}
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
            {/* The box first, then the tables' timelines, then the variables
                (#2110, maintainer 2026-11-11, arrangement Y). */}
            {coverage.loading && filterTables.length > 1 ? (
              <div className={styles.tableRowsLoading} aria-busy="true">
                <Skeleton count={filterTables.length} />
              </div>
            ) : coverage.loading ? null : !rows ? (
              filterTables.length > 1 && (
                <StatusRow tone="error">
                  The tables' years did not load. Reload the page to try again.
                </StatusRow>
              )
            ) : (
              filterable && (
                <VariableRowList
                  label="Tables"
                  note={<span className={styles.note}>Click to filter</span>}
                  axis={axis}
                >
                  {rows.flatMap(({ table, spans, databases: held }) => {
                    const several = held.length > 1;
                    return [
                      <VariableRow
                        key={table.fqid}
                        name={
                          <span className={styles.tableRowName}>
                            {/* With one table, only its databases filter. */}
                            {filterTables.length > 1 ? (
                              chip(
                                lensKeyId({
                                  table: table.fqid,
                                  database: null,
                                }),
                                <EntityPill entity="table" name={table.name} />,
                              )
                            ) : (
                              <EntityPill entity="table" name={table.name} />
                            )}
                            {held.length === 1 && (
                              <EntityPill
                                entity="database"
                                name={held[0].space.name}
                              />
                            )}
                          </span>
                        }
                        {...bar(spans, bounds)}
                        axis={axis}
                        window={studyBlock}
                      />,
                      ...(several
                        ? held.map(({ space, spans: years }) => (
                            <VariableRow
                              key={`${table.fqid}:${space.id}`}
                              indent
                              name={
                                <span className={styles.tableRowName}>
                                  {chip(
                                    lensKeyId({
                                      table: table.fqid,
                                      database: space.id,
                                    }),
                                    <EntityPill
                                      entity="database"
                                      name={
                                        <>
                                          {space.name}
                                          <span className="visually-hidden">
                                            {` in ${table.name}`}
                                          </span>
                                        </>
                                      }
                                    />,
                                  )}
                                </span>
                              }
                              {...bar(years, bounds)}
                              axis={axis}
                              window={studyBlock}
                            />
                          ))
                        : []),
                    ];
                  })}
                </VariableRowList>
              )
            )}
            {/* The list stays mounted through loading, a failed read and an
                empty filter, so its count's live region is never inserted
                with its first text (which is not announced). */}
            <VariableRowList
              label="Variables"
              note={
                <span className={styles.note} aria-live="polite">
                  {lens.state === "on"
                    ? `${chosen.length} of ${node.variables.length} variables shown`
                    : ""}
                </span>
              }
              axis={axis}
            >
              {lens.state !== "loading" &&
                lens.state !== "failed" &&
                shown.map((variable) => {
                  // With a filter on, the bar draws only the selected tables'
                  // and databases' years (#2110, maintainer 2026-11-11).
                  const years =
                    lens.state === "on"
                      ? bar(lens.years.get(variable.fqid) ?? [], bounds)
                      : {
                          coverage: variableYearRuns(
                            variable,
                            node.tables,
                            bounds,
                          ),
                          years: segmentYears(
                            variablePeriods(variable, node.tables),
                          ),
                        };
                  return (
                    <VariableRow
                      key={variable.fqid}
                      name={<Highlight text={variable.name} needle={needle} />}
                      href={catalogHref(variable.fqid)}
                      inProject={bound.has(variable.fqid)}
                      {...years}
                      axis={axis}
                      window={studyBlock}
                    />
                  );
                })}
            </VariableRowList>
            {lens.state === "loading" ? (
              <div className={styles.tableRowsLoading} aria-busy="true">
                <Skeleton count={3} />
              </div>
            ) : lens.state === "failed" ? (
              <StatusRow tone="error">
                The years of{" "}
                {lens.tables
                  .map(
                    (fqid) =>
                      node.tables.find((table) => table.fqid === fqid)?.name ??
                      fqid,
                  )
                  .join(", ")}{" "}
                did not load. Reload the page to try again.
              </StatusRow>
            ) : (
              shown.length === 0 && (
                // Empty only through the box: every table row delivers a
                // variable.
                <EmptyState
                  title={`No variables match “${needle.trim()}”.`}
                  description="Clear the filter to list them all."
                />
              )
            )}
          </>
        )}
      </Panel>
      <RelatedDocumentsPanel fqid={node.fqid} names={names} filterable />
    </article>
  );
}
