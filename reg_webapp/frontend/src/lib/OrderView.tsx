import { Dialog, DialogTrigger, Heading, Popover } from "react-aria-components";
import {
  ApiError,
  type CatalogFinding,
  type CatalogPeriod,
  getProjectOrder,
  getValueSet,
  type LogicalOrder,
} from "./api";
import { catalogHref, humanizeClassificationSlug } from "./catalog";
import { catalogWords } from "./catalog_labels";
import { catalogPeriodLabel } from "./catalog_period";
import { DataWarnings } from "./DataWarnings";
import styles from "./OrderView.module.css";
import type { ProjectData } from "./project_data";
import { projectStore } from "./project_store";
import { Button } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogName, useCatalogNames } from "./use_catalog_names";
import { ValueSetLabel } from "./VariableView";
import {
  issuePlace,
  issueRefs,
  plainIssueText,
  type ValidationIssue,
} from "./validation";

type Entry = LogicalOrder["entries"][number];

function Period({ period }: { period: CatalogPeriod }) {
  const label = catalogPeriodLabel(period);
  return period.start === null && period.end === null ? (
    label
  ) : (
    <code>{label}</code>
  );
}

type OrderResult =
  | { kind: "ready"; order: LogicalOrder }
  | { kind: "blocked"; findings: ValidationIssue[] };

function blockedFindings(error: unknown): ValidationIssue[] | null {
  if (
    !(error instanceof ApiError) ||
    !error.body ||
    typeof error.body !== "object"
  )
    return null;
  const body = error.body as {
    error?: { code?: unknown; fields?: { findings?: unknown } };
  };
  const findings = body.error?.fields?.findings;
  if (body.error?.code !== "order_blocked" || !Array.isArray(findings))
    return null;
  if (
    !findings.every((issue: unknown) => {
      if (!issue || typeof issue !== "object") return false;
      const value = issue as Record<string, unknown>;
      return (
        ["code", "message", "path"].every(
          (field) => typeof value[field] === "string",
        ) &&
        ["error", "warning", "info"].includes(String(value.level)) &&
        ["source", "variable", "table", "successor_fqid"].every(
          (field) => value[field] === null || typeof value[field] === "string",
        ) &&
        (value.period === null ||
          (typeof value.period === "object" &&
            value.period !== null &&
            ["start", "end"].every((field) => {
              const endpoint = (value.period as Record<string, unknown>)[field];
              return endpoint === null || typeof endpoint === "string";
            })))
      );
    })
  )
    return null;
  return findings as ValidationIssue[];
}

/** The materialized logical order; no extraction manifest is produced here. */
export function OrderView({ draft }: { draft: ProjectData }) {
  const resource = useAsyncResource<OrderResult>(
    JSON.stringify(draft),
    async (signal) => {
      try {
        const order = await getProjectOrder(draft, { signal });
        if (!signal.aborted) projectStore.setOrderBlocked(draft, false);
        return { kind: "ready", order };
      } catch (error) {
        const findings = blockedFindings(error);
        if (!findings) throw error;
        if (!signal.aborted) projectStore.setOrderBlocked(draft, true);
        return { kind: "blocked", findings };
      }
    },
  );
  return (
    <article className={styles.page}>
      <p className={styles.prose}>
        Review the columns and periods supplied by the catalog for this project.
      </p>
      {resource.loading ? (
        <section aria-label="Order" aria-busy="true">
          <Skeleton variant="block" />
        </section>
      ) : resource.error || !resource.data ? (
        <StatusRow tone="error" live="alert">
          The order could not be prepared. Check the project and try again.
        </StatusRow>
      ) : resource.data.kind === "blocked" ? (
        <>
          <StatusRow tone="error" live="alert">
            The order is blocked. Review the findings and update the project.
          </StatusRow>
          <OrderFindings findings={resource.data.findings} draft={draft} />
        </>
      ) : (
        <OrderContent order={resource.data.order} draft={draft} />
      )}
    </article>
  );
}

function OrderContent({
  order,
  draft,
}: {
  order: LogicalOrder;
  draft: ProjectData;
}) {
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
                  {group.source}:{" "}
                  <CatalogName
                    fqid={group.table}
                    fallback="Table name unavailable"
                  />
                </>
              }
              flush
            >
              <DataTable
                columns={[
                  { key: "variable", label: "Variable", width: "42%" },
                  { key: "spelling", label: "Column and period", width: "36%" },
                  { key: "binding", label: "Column details", width: "22%" },
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
                          <CatalogName
                            fqid={entry.variable}
                            fallback="Variable name unavailable"
                          />
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
                    default: {
                      const valueNames = entry.value_set
                        ? valueSets.data?.get(entry.value_set.hash)
                        : null;
                      return (
                        <EntryDetails
                          entry={entry}
                          variableName={nameOf(entry.variable, "Variable")}
                          valueSetNames={entry.value_set ? valueNames : null}
                          valueSetError={valueSets.error !== null}
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
          Value-set names did not load. Reload the page to try again.
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
      <OrderFindings findings={order.findings} draft={draft} />
    </>
  );
}

function EntryDetails({
  entry,
  variableName,
  valueSetNames,
  valueSetError,
}: {
  entry: Entry;
  variableName: string;
  valueSetNames: readonly string[] | null | undefined;
  valueSetError: boolean;
}) {
  return (
    <DialogTrigger>
      <Button
        variant="ghost"
        onPress={() => {}}
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
                value: storageLabel(entry),
              },
              {
                label: "Value set",
                value: valueSetError ? (
                  "Names unavailable"
                ) : valueSetNames === undefined ? (
                  <Skeleton />
                ) : (
                  <ValueSetLabel
                    valueSet={
                      valueSetNames === null ? null : { names: valueSetNames }
                    }
                  />
                ),
              },
              {
                label: "Classification",
                value: <ClassificationName fqid={entry.classification} />,
              },
            ]}
          />
        </Dialog>
      </Popover>
    </DialogTrigger>
  );
}

function CatalogName({ fqid, fallback }: { fqid: string; fallback: string }) {
  const read = useCatalogName(fqid);
  return read.loading ? <Skeleton /> : (read.value ?? fallback);
}

function ClassificationName({ fqid }: { fqid: string | null }) {
  const read = useCatalogName(fqid);
  if (!fqid) return <>Not stated</>;
  const fallback = humanizeClassificationSlug(fqid.split("/").at(-1) ?? "");
  return (
    <a className="link" href={catalogHref(fqid)}>
      {read.value ?? fallback}
    </a>
  );
}

function storageLabel(entry: Entry): string {
  const type = entry.type?.toLowerCase();
  if (!type) return "Not stated";
  if (["char", "varchar", "nvarchar", "text", "string"].includes(type)) {
    return entry.length !== null && entry.length > 0
      ? `Text, ${entry.length} ${entry.length === 1 ? "character" : "characters"}`
      : "Text";
  }
  if (["int", "integer", "bigint", "smallint", "tinyint"].includes(type))
    return "Integer";
  if (["decimal", "double", "float", "numeric", "number"].includes(type))
    return "Number";
  if (type === "datetime") return "Date and time";
  if (type === "date") return "Date";
  return catalogWords(type);
}

function OrderFindings({
  findings,
  draft,
}: {
  findings: ValidationIssue[];
  draft: ProjectData;
}) {
  const raw = JSON.stringify(draft);
  const refs = findings.flatMap((issue) => issueRefs(issue, raw));
  const names = useCatalogNames(refs);
  const located: CatalogFinding[] = findings.map((issue) => {
    const place = issuePlace(issue, raw);
    const message = plainIssueText(
      issue.message,
      issueRefs(issue, raw),
      names,
      raw,
    );
    const fqid = place.variable ?? place.table;
    return {
      caveat: null,
      code: issue.code,
      message: place.source ? `${place.source}: ${message}` : message,
      period: issue.period,
      severity: issue.level === "info" ? "notice" : "review",
      subject: {
        kind: place.variable ? "variable" : "table",
        fqid,
        name: fqid ? (names.get(fqid) ?? null) : null,
        table: place.table,
        table_name: place.table ? (names.get(place.table) ?? null) : null,
        period: issue.period,
        release: null,
        spelling: null,
        value_set: null,
      },
    };
  });
  const findingNames = new Map(names);
  for (const ref of refs)
    if (!findingNames.has(ref)) findingNames.set(ref, "Catalog item");
  return <DataWarnings findings={located} names={findingNames} />;
}
