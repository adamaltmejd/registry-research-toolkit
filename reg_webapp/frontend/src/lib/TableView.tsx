import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import { type Context, getCoverage, type TableShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { catalogWords } from "./catalog_labels";
import { registerRead, registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { yearWindowLabel } from "./period";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { columnYearRuns } from "./study_window";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogRead } from "./use_store_read";
import { windowBounds } from "./window_bounds";

type Release = TableShow["releases"][number];
type Population = Release["populations"][number];
type PopulationApplication = Pick<Release, "name"> &
  Pick<Population, "ref_day">;
type PopulationGroup = Pick<Population, "name" | "definition"> & {
  releases: PopulationApplication[];
};

// The primary is final, then unstated, then preliminary (crates/DESIGN.md →
// Editions), so a period's releases list in that order.
const EDITION_RANK: Record<string, number> = {
  final: 0,
  unstated: 1,
  preliminary: 2,
};

function compareReleases(a: Release, b: Release): number {
  // An unparsed release has no period and lists last; an open start first.
  const aStart = a.period ? (a.period.start ?? "") : "\uffff";
  const bStart = b.period ? (b.period.start ?? "") : "\uffff";
  return (
    (aStart < bStart ? -1 : aStart > bStart ? 1 : 0) ||
    (EDITION_RANK[a.edition] ?? 3) - (EDITION_RANK[b.edition] ?? 3) ||
    svCollator.compare(a.name, b.name)
  );
}

function stated(term: string, value: string | null) {
  return value && value !== "unknown" && value !== "unstated"
    ? [{ term, value: catalogWords(value) }]
    : [];
}

function releaseCount(count: number): string {
  return count === 1 ? "1 release" : `${count} releases`;
}

export function TableView({
  node,
  context,
}: {
  node: TableShow;
  context: Context | null;
}) {
  const register = useCatalogRead(registerRead, node.register).value;
  const names = registerSubjectNames(register);
  // A steward deployment shows the years it holds, not the reference spans.
  const steward = context?.steward_config != null;
  const held = useAsyncResource(steward ? node.fqid : null, (signal) =>
    getCoverage(node.fqid, { signal }),
  );
  const bounds = windowBounds(context);
  const years = steward
    ? columnYearRuns(held.data?.held ?? [], [bounds.min, bounds.max]).map(
        yearWindowLabel,
      )
    : node.spans.map(catalogPeriodLabel);
  const releases = [...node.releases].sort(compareReleases);
  // Most providers state no edition: the column shows only where one does.
  const editioned = releases.some((release) => release.edition !== "unstated");
  const populations = new Map<string, PopulationGroup>();
  for (const release of releases) {
    for (const population of release.populations) {
      const key = JSON.stringify([population.name, population.definition]);
      const application = {
        name: release.name,
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
  // Many providers restate a population's name as its definition: the column
  // shows only where some definition says more than the name.
  const defined = [...populations.values()].some(
    (population) =>
      population.definition && population.definition !== population.name,
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
          // A fact the catalog does not state is left out, not printed as
          // "Not stated" (DESIGN.md → Detail pane).
          ...stated("Role", node.role),
          ...stated("Cadence", node.cadence),
          ...(node.unit_type
            ? [{ term: "Unit type", value: node.unit_type }]
            : []),
          { term: "Timeless", value: node.timeless ? "Yes" : "No" },
          { term: "Implicit", value: node.implicit ? "Yes" : "No" },
        ]}
      />
      <Panel title="Years">
        {held.loading ? (
          <div aria-busy="true">
            <Skeleton count={1} />
          </div>
        ) : held.error ? (
          <StatusRow tone="error" live="alert">
            The years held did not load ({held.error}). Reload the page to try
            again.
          </StatusRow>
        ) : years.length === 0 ? (
          <EmptyState title="This table has no dated years." />
        ) : (
          <ul className={styles.plainList}>
            {years.map((label) => (
              <li key={label}>
                <code>{label}</code>
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
              ...(defined
                ? [{ key: "definition" as const, label: "Definition" }]
                : []),
              { key: "releases", label: "Applies to" },
            ]}
            rows={[...populations.values()]}
            getRowId={(row) => JSON.stringify([row.name, row.definition])}
            renderCell={(row, column) =>
              column.key === "releases" ? (
                <PopulationReleases
                  population={row}
                  allReleases={releases.length}
                />
              ) : (
                row[column.key] || "Not stated"
              )
            }
          />
        )}
      </Panel>
      <Panel title="Releases" flush={releases.length > 0}>
        {releases.length === 0 ? (
          <EmptyState title="No releases recorded for this table." />
        ) : (
          <DataTable
            columns={[
              { key: "name", label: "Release" },
              { key: "period", label: "Period", mono: true },
              ...(editioned
                ? [{ key: "edition" as const, label: "Edition" }]
                : []),
              { key: "primary", label: "Primary" },
            ]}
            rows={releases}
            getRowId={(row) => row.name}
            renderCell={(row, column) => {
              switch (column.key) {
                case "period":
                  // Only an unparsed release has no period.
                  return row.period
                    ? catalogPeriodLabel(row.period)
                    : "Name not parsed";
                case "edition":
                  return catalogWords(row.edition);
                case "primary":
                  // A release without a period is never the primary.
                  return row.primary ? "Yes" : "No";
                default:
                  return row.name;
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

/** The releases a population applies to: "All releases" when it is every one
 * on one reference day, else the one release or a disclosure listing them, with
 * a reference day the releases share stated once. */
function PopulationReleases({
  population,
  allReleases,
}: {
  population: PopulationGroup;
  allReleases: number;
}) {
  const applied = population.releases;
  const refDays = new Set(applied.map((release) => release.ref_day));
  const sharedRefDay = refDays.size === 1 ? applied[0]?.ref_day : null;
  const names = new Set(applied.map((release) => release.name));
  const item = (release: PopulationApplication) => (
    <>
      {release.name}
      {!sharedRefDay && release.ref_day && (
        <>
          , reference day <code>{release.ref_day}</code>
        </>
      )}
    </>
  );
  return (
    <>
      {names.size === allReleases && allReleases > 1 && refDays.size === 1 ? (
        "All releases"
      ) : applied.length === 1 && applied[0] ? (
        item(applied[0])
      ) : (
        <Disclosure className={styles.disclosure}>
          {({ isExpanded }) => (
            <>
              <Heading level={3} className={styles.disclosureHeading}>
                <Button
                  slot="trigger"
                  className={styles.disclosureButton}
                  aria-label={`${releaseCount(names.size)} for ${population.name}`}
                >
                  <TreeChevron />
                  <span className={styles.countTrigger}>
                    {releaseCount(names.size)}
                  </span>
                </Button>
              </Heading>
              <DisclosurePanel
                className={`motion-disclose ${isExpanded ? "open" : ""}`}
              >
                <div>
                  {isExpanded && (
                    <ul className={styles.plainList}>
                      {applied.map((release) => (
                        <li
                          key={JSON.stringify([release.name, release.ref_day])}
                        >
                          {item(release)}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </DisclosurePanel>
            </>
          )}
        </Disclosure>
      )}
      {sharedRefDay && (
        <div>
          Reference day <code>{sharedRefDay}</code>
        </div>
      )}
    </>
  );
}
