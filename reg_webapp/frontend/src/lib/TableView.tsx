import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { Context, TableShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { registerRead, registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { HeldPanels } from "./HeldPanels";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { TreeChevron } from "./ui/TreeChevron";
import { useCatalogRead } from "./use_store_read";

type Release = TableShow["releases"][number];
type Population = Release["populations"][number];
type PopulationApplication = Pick<Release, "name" | "period"> &
  Pick<Population, "ref_day">;
type PopulationGroup = Pick<Population, "name" | "definition"> & {
  releases: PopulationApplication[];
};

export function TableView({
  node,
  context,
}: {
  node: TableShow;
  context: Context | null;
}) {
  const register = useCatalogRead(registerRead, node.register).value;
  const names = registerSubjectNames(register);
  const releases = [...node.releases].sort(
    (a, b) =>
      svCollator.compare(a.edition, b.edition) ||
      svCollator.compare(a.name, b.name),
  );
  const populations = new Map<string, PopulationGroup>();
  for (const release of releases) {
    for (const population of release.populations) {
      const key = JSON.stringify([population.name, population.definition]);
      const application = {
        name: release.name,
        period: release.period,
        ref_day: population.ref_day,
      };
      const group = populations.get(key);
      if (group) {
        if (
          !group.releases.some(
            (applied) =>
              applied.name === application.name &&
              applied.ref_day === application.ref_day,
          )
        ) {
          group.releases.push(application);
        }
      } else {
        populations.set(key, {
          name: population.name,
          definition: population.definition,
          releases: [application],
        });
      }
    }
  }
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
          { term: "Role", value: node.role },
          { term: "Cadence", value: node.cadence ?? "Not stated" },
          { term: "Unit type", value: node.unit_type ?? "Not stated" },
          { term: "Timeless", value: node.timeless ? "Yes" : "No" },
          { term: "Implicit", value: node.implicit ? "Yes" : "No" },
        ]}
      />
      {context?.steward_config != null && (
        <HeldPanels fqid={node.fqid} kind="table" names={names} />
      )}
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
      <Panel title="Populations" flush={populations.size > 0}>
        {populations.size === 0 ? (
          <EmptyState title="No populations recorded for this table’s releases." />
        ) : (
          <DataTable
            columns={[
              { key: "name", label: "Population" },
              { key: "definition", label: "Definition" },
              { key: "releases", label: "Applies to" },
            ]}
            rows={[...populations.values()]}
            getRowId={(row) => JSON.stringify([row.name, row.definition])}
            renderCell={(row, column) =>
              column.key === "releases" ? (
                <Disclosure className={styles.disclosure}>
                  {({ isExpanded }) => (
                    <>
                      <Heading level={3} className={styles.disclosureHeading}>
                        <Button
                          slot="trigger"
                          className={styles.disclosureButton}
                          aria-label={`Applies to releases for ${row.name}`}
                        >
                          <TreeChevron />
                          <span>Applies to releases</span>
                        </Button>
                      </Heading>
                      <DisclosurePanel
                        className={`motion-disclose ${isExpanded ? "open" : ""}`}
                      >
                        <div>
                          {isExpanded && (
                            <ul className={styles.plainList}>
                              {row.releases.map((release) => (
                                <li
                                  key={JSON.stringify([
                                    release.name,
                                    release.ref_day,
                                  ])}
                                >
                                  {release.name}:{" "}
                                  <code>
                                    {release.period
                                      ? catalogPeriodLabel(release.period)
                                      : "Not stated"}
                                  </code>
                                  {release.ref_day && (
                                    <>
                                      ; reference day:{" "}
                                      <code>{release.ref_day}</code>
                                    </>
                                  )}
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      </DisclosurePanel>
                    </>
                  )}
                </Disclosure>
              ) : (
                row[column.key] || "Not stated"
              )
            }
          />
        )}
      </Panel>
      <Panel title="Releases">
        {releases.length === 0 ? (
          <EmptyState title="No releases recorded for this table." />
        ) : (
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
                    : "Not stated";
                case "primary":
                  return row.primary === null
                    ? "Not stated"
                    : row.primary
                      ? "Yes"
                      : "No";
                default:
                  return String(row[column.key] || "Not stated");
              }
            }}
          />
        )}
      </Panel>
      <ScopedDataWarnings fqid={node.fqid} names={names} />
      <RelatedDocumentsPanel fqid={node.fqid} names={names} />
    </article>
  );
}
