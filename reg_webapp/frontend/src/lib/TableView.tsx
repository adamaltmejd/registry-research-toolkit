import { useMemo } from "react";
import type { Context, TableShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { registerRead, registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { dataAxis } from "./period";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { datedYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { useCatalogRead } from "./use_store_read";
import { windowBounds } from "./window_bounds";

/** A table's page (DESIGN.md → Detail pane): its name, its description where
 * stated, its years, and its variables on the register page's axis, each over
 * the years it has in this table. */
export function TableView({
  node,
  context,
}: {
  node: TableShow;
  context: Context | null;
}) {
  const register = useCatalogRead(registerRead, node.register).value;
  const names = registerSubjectNames(register);
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
  const variables = [...node.variables].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
  const axis = dataAxis(
    datedYearRuns(node.variables, bounds).flatMap((run) => [run.from, run.to]),
    study,
    bounds,
  );
  return (
    <article className={styles.page}>
      <Definitions
        title={node.name}
        memberOf={[
          {
            relation: "Register",
            label: register?.name ?? node.register,
            href: catalogHref(node.register),
          },
        ]}
        definitions={[
          ...(node.description
            ? [{ term: "Description", value: node.description }]
            : []),
          ...(node.spans.length > 0
            ? [
                {
                  term: "Years",
                  value: (
                    <code>{node.spans.map(catalogPeriodLabel).join(", ")}</code>
                  ),
                },
              ]
            : []),
        ]}
      />
      <Panel title="Variables" flush={variables.length > 0}>
        {variables.length === 0 ? (
          <EmptyState title="This table lists no variables." />
        ) : (
          <VariableRowList label="Variables" axis={axis}>
            {variables.map((variable) => (
              <VariableRow
                key={variable.fqid}
                name={variable.name}
                href={catalogHref(variable.fqid)}
                inProject={bound.has(variable.fqid)}
                coverage={datedYearRuns([variable], bounds)}
                axis={axis}
                window={study ? [study.from, study.to] : null}
              />
            ))}
          </VariableRowList>
        )}
      </Panel>
      <ScopedDataWarnings fqids={[node.fqid]} names={names} />
      <RelatedDocumentsPanel fqids={[node.fqid]} names={names} />
    </article>
  );
}
