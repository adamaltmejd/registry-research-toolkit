import { useMemo } from "react";
import type { Context, RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { dataAxis } from "./period";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { columnYearRuns, studyWindow } from "./study_window";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
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
            term: "Period",
            value: <code>{catalogPeriodLabel(node.period)}</code>,
          },
        ]}
      />
      <Panel title="Tables" flush={node.tables.length > 0}>
        {node.tables.length === 0 ? (
          <EmptyState title="This register lists no tables." />
        ) : (
          <DataTable
            rowNavigation
            columns={[
              { key: "name", label: "Table" },
              { key: "role", label: "Role" },
              { key: "cadence", label: "Cadence" },
              { key: "period", label: "Period", mono: true },
            ]}
            rows={[...node.tables].sort((a, b) =>
              svCollator.compare(a.name, b.name),
            )}
            getRowId={(row) => row.fqid}
            renderCell={(row, column) => {
              switch (column.key) {
                case "name":
                  return (
                    <a className="link" href={catalogHref(row.fqid)}>
                      {row.name}
                    </a>
                  );
                case "period":
                  return catalogPeriodLabel(row.period);
                default:
                  return row[column.key] ?? "Not stated";
              }
            }}
          />
        )}
      </Panel>
      <Panel title="Variables" flush={node.variables.length > 0}>
        {node.variables.length === 0 ? (
          <EmptyState title="This register lists no variables." />
        ) : (
          <VariableRowList label="Variables">
            {[...node.variables]
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
        )}
      </Panel>
      {node.unplaced.length > 0 && (
        <Panel title="Unplaced releases">
          <p className={styles.prose}>
            These releases could not be placed in time.
          </p>
          <ul className={styles.plainList}>
            {node.unplaced.map((name) => (
              <li key={name}>{name}</li>
            ))}
          </ul>
        </Panel>
      )}
      <ScopedDataWarnings fqid={node.fqid} names={names} />
      <RelatedDocumentsPanel fqid={node.fqid} names={names} filterable />
    </article>
  );
}
