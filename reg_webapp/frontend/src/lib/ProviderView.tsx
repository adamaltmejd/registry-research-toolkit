import type { ProviderShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";

export function ProviderView({ node }: { node: ProviderShow }) {
  const registers = node.registers;
  return (
    <article className={styles.page}>
      <Definitions title={node.name} />
      <Panel title="Registers" flush={registers.length > 0}>
        {registers.length === 0 ? (
          <EmptyState title="This provider has no registers in the catalog." />
        ) : (
          <DataTable
            rowNavigation
            columns={[
              { key: "name", label: "Register" },
              { key: "period", label: "Period", mono: true },
            ]}
            rows={[...registers].sort((a, b) =>
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
    </article>
  );
}
