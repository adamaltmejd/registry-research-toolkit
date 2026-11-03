import { Button, Tooltip, TooltipTrigger } from "react-aria-components";
import { type Coverage, getCoverage } from "./api";
import { catalogHref } from "./catalog";
import { catalogWords } from "./catalog_labels";
import { catalogPeriodLabel } from "./catalog_period";
import findingStyles from "./Finding.module.css";
import styles from "./HeldPanels.module.css";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

type Placement = NonNullable<Coverage["held"]>[number];

function Caveat({ text }: { text: string }) {
  return (
    <TooltipTrigger delay={600}>
      <Button className={findingStyles.titleButton}>Caveat</Button>
      <Tooltip
        className={({ isEntering, isExiting }) =>
          `${findingStyles.tooltip} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
        }
        style={{ zIndex: "var(--z-popover)" }}
      >
        {text}
      </Tooltip>
    </TooltipTrigger>
  );
}

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
  return (
    <section className={styles.group}>
      <h3 className={`micro-label ${styles.heading}`}>
        <code>{physicalTable}</code>
      </h3>
      {sameRole && (
        <KeyValue rows={[{ label: "Role", value: catalogWords(first.role) }]} />
      )}
      {sameCarried && first.carried && <span>Carried forward</span>}
      {sameCaveat && first.caveat && <Caveat text={first.caveat} />}
      <div className={styles.table}>
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
            { key: "period", label: "Period", width: "25%" },
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
                return <code>{catalogPeriodLabel(row.period)}</code>;
              case "role":
                return catalogWords(row.role);
              case "caveat":
                return (
                  <span className={styles.notes}>
                    {!sameCarried && row.carried && (
                      <span>Carried forward</span>
                    )}
                    {!sameCaveat && row.caveat && <Caveat text={row.caveat} />}
                  </span>
                );
            }
          }}
        />
      </div>
    </section>
  );
}

/** Coverage owns holdings; Show remains the reference metadata for the page. */
export function HeldPanels({
  fqid,
  kind,
  names,
}: {
  fqid: string;
  kind: "variable" | "table";
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
        <Panel title="Held">
          {held.length === 0 ? (
            <EmptyState title={`Nothing held for this ${kind}.`} />
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
