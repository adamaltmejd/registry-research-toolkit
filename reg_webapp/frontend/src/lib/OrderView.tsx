import {
  Button,
  Dialog,
  DialogTrigger,
  Heading,
  Popover,
} from "react-aria-components";
import {
  type CatalogFinding,
  type CatalogPeriod,
  getProjectOrder,
  getValueSet,
  type LogicalOrder,
} from "./api";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { DataWarnings } from "./DataWarnings";
import styles from "./OrderView.module.css";
import type { ProjectData } from "./project_data";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogNames } from "./use_catalog_names";

type Entry = LogicalOrder["entries"][number];

function Period({ period }: { period: CatalogPeriod }) {
  const label = catalogPeriodLabel(period);
  return period.start === null && period.end === null ? (
    label
  ) : (
    <code>{label}</code>
  );
}

/** The materialized logical order; no extraction manifest is produced here. */
export function OrderView({ draft }: { draft: ProjectData }) {
  const resource = useAsyncResource(JSON.stringify(draft), (signal) =>
    getProjectOrder(draft, { signal }),
  );
  return (
    <article className={styles.page}>
      <p className={styles.prose}>
        Review the columns and periods supplied by the catalog for this project.
        <br />
        <a href="/project">Back to the project</a>
      </p>
      {resource.loading ? (
        <section aria-label="Order" aria-busy="true">
          <Skeleton variant="block" />
        </section>
      ) : resource.error || !resource.data ? (
        <StatusRow tone="error" live="alert">
          The order could not be prepared ({resource.error}). Check the project
          findings and try again.
        </StatusRow>
      ) : (
        <OrderContent order={resource.data} />
      )}
    </article>
  );
}

function OrderContent({ order }: { order: LogicalOrder }) {
  const refs = [
    ...order.entries.flatMap((entry) => [entry.table, entry.variable]),
    ...order.row_keys.flatMap((key) => [
      key.table,
      ...key.columns.map((column) => column.variable),
    ]),
    ...order.clips.map((clip) => clip.variable),
    ...order.findings.flatMap((issue) =>
      [issue.variable, issue.table, issue.successor_fqid].filter(
        (ref): ref is string => ref !== null,
      ),
    ),
  ];
  const names = useCatalogNames(refs);
  const nameOf = (ref: string, fallback: string) => names.get(ref) ?? fallback;
  const hashes = [
    ...new Set(
      order.entries.flatMap((entry) =>
        entry.value_set ? [entry.value_set.hash] : [],
      ),
    ),
  ];
  const valueSets = useAsyncResource(
    hashes.length > 0 ? JSON.stringify(hashes) : null,
    async (signal) =>
      new Map(
        await Promise.all(
          hashes.map(async (hash) => {
            const values = await getValueSet(hash, { signal });
            return [hash, values.names] as const;
          }),
        ),
      ),
  );
  const groups = new Map<
    string,
    { source: string; table: string; entries: Entry[] }
  >();
  for (const entry of order.entries) {
    const key = JSON.stringify([entry.source, entry.table]);
    const group = groups.get(key);
    if (group) group.entries.push(entry);
    else
      groups.set(key, {
        source: entry.source,
        table: entry.table,
        entries: [entry],
      });
  }
  const findings: CatalogFinding[] = order.findings.map((issue) => {
    let message = issue.message;
    for (const ref of [issue.variable, issue.table, issue.successor_fqid]) {
      if (ref)
        message = message.replaceAll(ref, nameOf(ref, "this catalog item"));
    }
    return {
      caveat: null,
      code: issue.code,
      message: issue.source ? `${issue.source}: ${message}` : message,
      period: issue.period,
      severity: issue.level === "warning" ? "review" : "notice",
      subject: {
        kind: issue.variable ? "variable" : "table",
        fqid: issue.variable ?? issue.table,
        table: issue.table,
        period: issue.period,
        release: null,
        spelling: null,
        value_set: null,
      },
    };
  });
  // DataWarnings' fallback is an identifier; name every subject, including failed
  // catalog lookups, so an order never exposes FQIDs as display text.
  const findingNames = new Map(names);
  for (const finding of findings) {
    for (const ref of [finding.subject.fqid, finding.subject.table]) {
      if (ref && !findingNames.has(ref)) findingNames.set(ref, "Catalog item");
    }
  }
  return (
    <>
      <section className={styles.entries} aria-label="Entries">
        {groups.size === 0 ? (
          <EmptyState
            title="The order has no columns."
            description="Open a project with sources and bindings to review its order."
          />
        ) : (
          [...groups].map(([key, group]) => (
            <Panel
              key={key}
              title={
                <>
                  {group.source}: {nameOf(group.table, "Table")}
                </>
              }
              flush
            >
              <DataTable
                columns={[
                  { key: "variable", label: "Variable", width: "48%" },
                  { key: "spelling", label: "Column and period", width: "37%" },
                  { key: "binding", label: "Details", width: "15%" },
                ]}
                rows={group.entries}
                getRowId={(entry) => JSON.stringify(entry)}
                renderCell={(entry, column) => {
                  switch (column.key) {
                    case "variable": {
                      const name = nameOf(entry.variable, "Variable");
                      return (
                        <a
                          className={`link ${styles.variable}`}
                          href={catalogHref(entry.variable)}
                          title={name}
                        >
                          {name}
                        </a>
                      );
                    }
                    case "spelling":
                      return (
                        <span className={styles.columnPeriod}>
                          <ColumnChip
                            name={entry.spelling}
                            className={styles.column}
                          />
                          <Period period={entry.period} />
                        </span>
                      );
                    case "period":
                      return (
                        <span
                          className={styles.period}
                          title={catalogPeriodLabel(entry.period)}
                        >
                          <Period period={entry.period} />
                        </span>
                      );
                    default: {
                      const valueNames = entry.value_set
                        ? valueSets.data?.get(entry.value_set.hash)
                        : null;
                      return (
                        <EntryDetails
                          entry={entry}
                          variableName={nameOf(entry.variable, "Variable")}
                          valueSetName={
                            !entry.value_set
                              ? "Not stated"
                              : valueSets.error
                                ? "Names unavailable"
                                : valueNames
                                  ? valueNames.join(", ") || "Unnamed value set"
                                  : "Reading value set names"
                          }
                        />
                      );
                    }
                  }
                }}
              />
            </Panel>
          ))
        )}
      </section>
      {valueSets.error && (
        <StatusRow tone="error">
          Value-set names did not load ({valueSets.error}). Reload the page to
          try again.
        </StatusRow>
      )}
      <Panel title="Row keys">
        {order.row_keys.length === 0 ? (
          <EmptyState title="No row keys recorded for this order." />
        ) : (
          <ul className={styles.plainList}>
            {order.row_keys.map((key) => (
              <li key={JSON.stringify(key)} className={styles.rowKey}>
                <span>
                  {key.source}, {nameOf(key.table, "Table")},{" "}
                  <Period period={key.period} />
                </span>
                <span className={styles.columns}>
                  {key.columns.map((column) => (
                    <a
                      key={column.variable}
                      className="link"
                      href={catalogHref(column.variable)}
                      aria-label={`${nameOf(column.variable, "Row key")}: ${column.spelling}`}
                    >
                      <ColumnChip name={column.spelling} />
                    </a>
                  ))}
                </span>
              </li>
            ))}
          </ul>
        )}
      </Panel>
      {order.clips.length > 0 && (
        <Panel title="Period adjustments">
          <ul className={styles.plainList}>
            {order.clips.map((clip) => (
              <li key={JSON.stringify(clip)}>
                {clip.source}, {nameOf(clip.variable, "Variable")}: requested{" "}
                {clip.requested.map(catalogPeriodLabel).join(", ") ||
                  "no period"}
                ; ordered{" "}
                {clip.ordered.map(catalogPeriodLabel).join(", ") || "no period"}
                .
              </li>
            ))}
          </ul>
        </Panel>
      )}
      <DataWarnings findings={findings} names={findingNames} />
    </>
  );
}

function EntryDetails({
  entry,
  variableName,
  valueSetName,
}: {
  entry: Entry;
  variableName: string;
  valueSetName: string;
}) {
  return (
    <DialogTrigger>
      <Button
        className={styles.details}
        aria-label={`Details for ${variableName}, ${catalogPeriodLabel(entry.period)}`}
      >
        Details
      </Button>
      <Popover
        // RAC 1.22 keeps its fallback dialog label after detecting a nested
        // Dialog. The inner Dialog owns the accessible name instead.
        render={(props) => <div {...props} aria-labelledby={undefined} />}
        placement="bottom end"
        className={({ isEntering, isExiting }) =>
          `${styles.popover} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
        }
      >
        <Dialog
          className={styles.dialog}
          aria-label={`Details for ${variableName}`}
        >
          <Heading slot="title" className={styles.heading}>
            {variableName}
          </Heading>
          <div className={styles.detailColumn}>
            <ColumnChip name={entry.spelling} />
            <Period period={entry.period} />
          </div>
          <KeyValue
            rows={[
              {
                label: "Display name",
                value: entry.display_name ?? "Not stated",
                mono: entry.display_name !== null,
              },
              {
                label: "Type and length",
                value: `${entry.type ?? "Not stated"}${entry.length === null ? "" : ` (${entry.length})`}`,
              },
              { label: "Value set", value: valueSetName },
              {
                label: "Classification",
                value: entry.classification ?? "Not stated",
                mono: entry.classification !== null,
              },
            ]}
          />
        </Dialog>
      </Popover>
    </DialogTrigger>
  );
}
