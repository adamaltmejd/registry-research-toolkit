import type { RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";

export function RegisterView({ node }: { node: RegisterShow }) {
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
          <DataTable
            rowNavigation
            columns={[
              { key: "name", label: "Variable" },
              { key: "period", label: "Period", mono: true },
            ]}
            rows={[...node.variables].sort((a, b) =>
              svCollator.compare(a.name, b.name),
            )}
            getRowId={(row) => row.fqid}
            renderCell={(row, column) =>
              column.key === "name" ? (
                <a className="link" href={catalogHref(row.fqid)}>
                  {row.name}
                </a>
              ) : (
                catalogPeriodLabel(row.period)
              )
            }
          />
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
      <ScopedDataWarnings fqid={node.fqid} register={node} />
    </article>
  );
}
