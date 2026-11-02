import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { TableShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { TreeChevron } from "./ui/TreeChevron";

export function TableView({ node }: { node: TableShow }) {
  const releases = [...node.releases].sort(
    (a, b) =>
      svCollator.compare(a.edition, b.edition) ||
      svCollator.compare(a.name, b.name),
  );
  const populations = node.releases.flatMap((release) =>
    release.populations.map((population) => ({
      ...population,
      release: release.name,
      edition: release.edition,
    })),
  );
  return (
    <article className={styles.page}>
      <Definitions
        title={node.name}
        memberOf={[
          {
            relation: "Register",
            label: <code>{node.register}</code>,
            href: catalogHref(node.register),
          },
        ]}
        definitions={[
          ...(node.description
            ? [{ term: "Description", value: node.description }]
            : []),
          { term: "Role", value: node.role },
          { term: "Cadence", value: node.cadence ?? "Not stated" },
          { term: "Unit type", value: node.unit_type ?? "Not stated" },
          { term: "Timeless", value: node.timeless ? "Yes" : "No" },
          { term: "Implicit", value: node.implicit ? "Yes" : "No" },
        ]}
      />
      <Panel title="Spans">
        {node.spans.length === 0 ? (
          <EmptyState title="This table has no dated spans." />
        ) : (
          <ul className={styles.plainList}>
            {node.spans.map((span) => (
              <li key={JSON.stringify(span)}>
                <code>{catalogPeriodLabel(span)}</code>
              </li>
            ))}
          </ul>
        )}
      </Panel>
      <Panel title="Row keys" flush={node.row_keys.length > 0}>
        {node.row_keys.length === 0 ? (
          <EmptyState title="No row keys recorded." />
        ) : (
          <DataTable
            rowNavigation
            columns={[
              { key: "spelling", label: "Row key" },
              { key: "period", label: "Period", mono: true },
            ]}
            rows={node.row_keys}
            getRowId={(row) => JSON.stringify(row)}
            renderCell={(row, column) =>
              column.key === "spelling" ? (
                <a className="link" href={catalogHref(row.fqid)}>
                  <ColumnChip name={row.spelling} />
                </a>
              ) : (
                catalogPeriodLabel(row.period)
              )
            }
          />
        )}
      </Panel>
      <Panel title="Populations" flush={populations.length > 0}>
        {populations.length === 0 ? (
          <EmptyState title="No populations recorded for this table’s releases." />
        ) : (
          <DataTable
            columns={[
              { key: "name", label: "Population" },
              { key: "definition", label: "Definition" },
              { key: "ref_day", label: "Reference day", mono: true },
              { key: "release", label: "Release" },
              { key: "edition", label: "Edition" },
            ]}
            rows={populations}
            getRowId={(row) => JSON.stringify(row)}
            renderCell={(row, column) => row[column.key] ?? "Not stated"}
          />
        )}
      </Panel>
      <Panel title="Releases">
        {releases.length === 0 ? (
          <EmptyState title="No releases recorded for this table." />
        ) : (
          <Disclosure className={styles.disclosure}>
            {({ isExpanded }) => (
              <>
                <Heading level={3} className={styles.disclosureHeading}>
                  <Button slot="trigger" className={styles.disclosureButton}>
                    <TreeChevron />
                    <span>
                      Releases (<code>{releases.length}</code>)
                    </span>
                  </Button>
                </Heading>
                <DisclosurePanel>
                  {isExpanded && (
                    <DataTable
                      columns={[
                        { key: "edition", label: "Edition" },
                        { key: "name", label: "Release" },
                        { key: "period", label: "Period", mono: true },
                        { key: "primary", label: "Primary" },
                        { key: "qualifier", label: "Qualifier" },
                        { key: "parse_state", label: "Parse state" },
                      ]}
                      rows={releases}
                      getRowId={(row) => row.name}
                      renderCell={(row, column) => {
                        switch (column.key) {
                          case "period":
                            return row.period
                              ? catalogPeriodLabel(row.period)
                              : "Not placed in time";
                          case "primary":
                            return row.primary === null
                              ? "Not determined"
                              : row.primary
                                ? "Primary"
                                : "No";
                          default:
                            return String(row[column.key] || "None");
                        }
                      }}
                    />
                  )}
                </DisclosurePanel>
              </>
            )}
          </Disclosure>
        )}
      </Panel>
      <ScopedDataWarnings fqid={node.fqid} />
    </article>
  );
}
