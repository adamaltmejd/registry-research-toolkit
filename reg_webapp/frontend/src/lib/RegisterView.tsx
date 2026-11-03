import { useRef } from "react";
import { Checkbox } from "react-aria-components";
import type { Context, RegisterShow, ValidationResultModel } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { dataAxis } from "./period";
import rowStyles from "./RegisterView.module.css";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { rowWindow } from "./register_add";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { columnYearRuns } from "./study_window";
import { AddBar } from "./ui/AddBar";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useRegisterAdd } from "./useRegisterAdd";

export function RegisterView({
  node,
  context,
  validation,
}: {
  node: RegisterShow;
  context: Context | null;
  validation: ValidationResultModel | null;
}) {
  const heading = useRef<HTMLSpanElement>(null);
  const add = useRegisterAdd({ node, context, validation });
  const bounds = [add.bounds.min, add.bounds.max] as const;
  const deliveries = columnYearRuns(node.tables, bounds);
  const axis = dataAxis(
    deliveries.flatMap((run) => [run.from, run.to]),
    add.study,
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
      <Panel
        title={
          <span ref={heading} tabIndex={-1} className={rowStyles.heading}>
            Variables
          </span>
        }
      >
        {!add.draft && (
          <p className={rowStyles.hint}>
            Create or open a project to select columns.
          </p>
        )}
        {node.variables.length === 0 ? (
          <EmptyState title="This register lists no variables." />
        ) : (
          <VariableRowList label="Variables">
            {[...node.variables]
              .sort((a, b) => svCollator.compare(a.name, b.name))
              .map((variable) => {
                const inProject = add.bound.has(variable.fqid);
                const window = rowWindow(
                  variable.period,
                  add.study,
                  add.bounds,
                );
                const coverage = columnYearRuns([variable], bounds, axis[0]);
                const columns = add.columns.get(variable.fqid);
                return (
                  <VariableRow
                    key={variable.fqid}
                    name={variable.name}
                    href={catalogHref(variable.fqid)}
                    inProject={inProject}
                    coverage={coverage.map(
                      (run) => [run.from, run.to] as const,
                    )}
                    axis={axis}
                    window={add.study ? [add.study.from, add.study.to] : null}
                    control={
                      add.draft &&
                      !inProject &&
                      window && (
                        <Checkbox
                          className={rowStyles.checkbox}
                          aria-label={`Add ${variable.name}`}
                          isSelected={add.selected.has(variable.fqid)}
                          aria-busy={
                            (add.selected.has(variable.fqid) && !columns) ||
                            undefined
                          }
                          onChange={(checked) => add.toggle(variable, checked)}
                        >
                          <span className={rowStyles.tick} aria-hidden="true">
                            ✓
                          </span>
                        </Checkbox>
                      )
                    }
                  >
                    {!window ? (
                      <span className="visually-hidden">
                        outside the study window
                      </span>
                    ) : columns?.length ? (
                      columns.map((column) => (
                        <ColumnChip key={column.id} name={column.spelling} />
                      ))
                    ) : null}
                  </VariableRow>
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
      <StatusRow
        tone={
          add.error
            ? "error"
            : add.classificationWarning
              ? "info"
              : add.added
                ? "ok"
                : null
        }
        live="status"
      >
        {add.error ??
          (add.added ? (
            <>
              Added columns to project.
              {add.classificationWarning && (
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
      {add.selected.size > 0 && (
        <AddBar
          count={add.count}
          onAdd={() => {
            if (add.add()) heading.current?.focus();
          }}
          isDisabled={add.loading || add.count === 0}
        />
      )}
    </article>
  );
}
