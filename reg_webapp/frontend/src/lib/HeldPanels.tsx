import { type Coverage, getCoverage } from "./api";
import { catalogHref, svCollator } from "./catalog";
import { catalogWords } from "./catalog_labels";
import { catalogPeriodLabel, catalogPeriodRuns } from "./catalog_period";
import styles from "./HeldPanels.module.css";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

type Placement = NonNullable<Coverage["held"]>[number];

function HeldGroup({
  physicalTable,
  rows,
  names,
  showVariable,
}: {
  physicalTable: string;
  rows: Placement[];
  names: ReadonlyMap<string, string>;
  showVariable: boolean;
}) {
  const first = rows[0];
  const sameRole = rows.every((row) => row.role === first.role);
  const sameCarried = rows.every((row) => row.carried === first.carried);
  const sameCaveat = rows.every((row) => row.caveat === first.caveat);
  const notesVary = !sameCarried || !sameCaveat;
  // Facts every placement shares, stated once. A caveat is text, not a
  // tooltip: a tooltip opens on hover or focus only, so a touch screen never
  // showed it (#2110).
  const facts = [
    ...(sameRole ? [{ label: "Role", value: catalogWords(first.role) }] : []),
    ...(sameCarried && first.carried
      ? [{ label: "Carried forward", value: "Yes" }]
      : []),
    ...(sameCaveat && first.caveat
      ? [{ label: "Caveat", value: first.caveat }]
      : []),
  ];
  return (
    <section className={styles.group}>
      <h3 className={`micro-label ${styles.heading}`}>
        <code>{physicalTable}</code>
      </h3>
      {facts.length > 0 && <KeyValue rows={facts} />}
      <DataTable
        columns={[
          ...(showVariable
            ? [{ key: "variable" as const, label: "Variable", width: "30%" }]
            : []),
          {
            key: "column",
            label: "Column",
            width: showVariable ? "25%" : "40%",
          },
          { key: "period", label: "Period", width: "25%", mono: true },
          ...(!sameRole ? [{ key: "role" as const, label: "Role" }] : []),
          ...(notesVary ? [{ key: "caveat" as const, label: "Notes" }] : []),
        ]}
        rows={rows}
        getRowId={(row) => JSON.stringify(row)}
        renderCell={(row, column) => {
          switch (column.key) {
            case "variable":
              return (
                <a className="link" href={catalogHref(row.variable)}>
                  {names.get(row.variable) ?? "Variable name unavailable"}
                </a>
              );
            case "column":
              return <ColumnChip name={row.column} />;
            case "period":
              return catalogPeriodLabel(row.period);
            case "role":
              return catalogWords(row.role);
            case "caveat":
              return (
                <span className={styles.notes}>
                  {!sameCarried && row.carried && <span>Carried forward</span>}
                  {!sameCaveat && row.caveat && <span>{row.caveat}</span>}
                </span>
              );
          }
        }}
      />
    </section>
  );
}

/** A register's holdings, one row per table: coverage lists every placement
 * of every table, too many to read as per-table groups (#2110). The table's
 * page has the placements. */
function HeldSummary({
  held,
  names,
}: {
  held: Placement[];
  names: ReadonlyMap<string, string>;
}) {
  const tables = new Map<string, Placement[]>();
  for (const row of held) {
    const placements = tables.get(row.table);
    if (placements) placements.push(row);
    else tables.set(row.table, [row]);
  }
  const rows = [...tables]
    .map(([fqid, placements]) => ({
      fqid,
      name: names.get(fqid) ?? "Table name unavailable",
      variables: new Set(placements.map((row) => row.variable)).size,
      period: catalogPeriodRuns(placements.map((row) => row.period)),
    }))
    .sort((a, b) => svCollator.compare(a.name, b.name));
  return (
    <DataTable
      rowNavigation
      columns={[
        { key: "name", label: "Table" },
        { key: "variables", label: "Held variables", numeric: true },
        { key: "period", label: "Period", mono: true },
      ]}
      rows={rows}
      getRowId={(row) => row.fqid}
      renderCell={(row, column) => {
        switch (column.key) {
          case "name":
            return (
              <a className="link" href={catalogHref(row.fqid)}>
                {row.name}
              </a>
            );
          case "variables":
            return String(row.variables);
          default:
            return row.period;
        }
      }}
    />
  );
}

/** Coverage owns holdings; Show remains the reference metadata for the page. */
export function HeldPanels({
  fqid,
  kind,
  names,
}: {
  fqid: string;
  kind: "variable" | "table" | "register";
  names: ReadonlyMap<string, string>;
}) {
  const resource = useAsyncResource(fqid, (signal) =>
    getCoverage(fqid, { signal }),
  );
  // Global pages and paired pages without documentation must not flash empty panels.
  if (resource.data == null && !resource.error) return null;
  if (resource.error)
    return (
      <StatusRow tone="error" live="alert">
        Holdings coverage did not load ({resource.error}). Reload the page to
        try again.
      </StatusRow>
    );
  const held = resource.data?.held;
  const occurrences = resource.data?.steward_occurrences;
  const groups = new Map<string, Placement[]>();
  for (const row of held ?? []) {
    const group = groups.get(row.physical_table);
    if (group) group.push(row);
    else groups.set(row.physical_table, [row]);
  }
  return (
    <>
      {held != null && (
        <Panel title="Held" flush={kind === "register" && held.length > 0}>
          {held.length === 0 ? (
            <EmptyState title={`Nothing held for this ${kind}.`} />
          ) : kind === "register" ? (
            <HeldSummary held={held} names={names} />
          ) : (
            <div className={styles.groups}>
              {[...groups].map(([physicalTable, rows]) => (
                <HeldGroup
                  key={physicalTable}
                  physicalTable={physicalTable}
                  rows={rows}
                  names={names}
                  showVariable={kind === "table"}
                />
              ))}
            </div>
          )}
        </Panel>
      )}
      {!!occurrences?.length && (
        <Panel title="Steward documentation">
          <div className={styles.groups}>
            {occurrences.map((row) => (
              <section className={styles.group} key={JSON.stringify(row)}>
                <h3 className={`micro-label ${styles.heading}`}>
                  <ColumnChip name={row.spelling} />
                </h3>
                <KeyValue
                  rows={[
                    { label: "Label", value: row.label ?? "Not stated" },
                    {
                      label: "Period",
                      value: catalogPeriodLabel(row.period),
                      mono: true,
                    },
                    { label: "Type", value: catalogWords(row.type) },
                    {
                      label: "Definition",
                      value: row.definition ?? "Not stated",
                    },
                  ]}
                />
              </section>
            ))}
          </div>
        </Panel>
      )}
    </>
  );
}
