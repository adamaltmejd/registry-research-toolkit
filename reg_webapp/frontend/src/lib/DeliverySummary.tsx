import type { CatalogPeriod, LogicalOrder } from "./api";
import { catalogHref } from "./catalog";
import { catalogPeriodRuns, inTableGrain, periodsLess } from "./catalog_period";
import styles from "./DeliverySummary.module.css";
import { MANUAL_LABEL } from "./manual_items";
import { EntityPill } from "./ui/EntityPill";
import { NameSkeleton } from "./ui/Skeleton";
import { type ColumnType, TypePill } from "./ui/TypePill";
import {
  useCatalogNames,
  useCatalogNamesPending,
  useTablePeriods,
} from "./use_catalog_names";

type Entry = LogicalOrder["entries"][number];
type ManualItem = LogicalOrder["manual"][number];

/** What the order would deliver (the logical order, maintainer ruling Q1,
 * 2026-11-04): per table its columns and years, the years a column does not
 * have, and the row identifier. No order file: the steward's instructions say
 * how to order. */
export function DeliverySummary({
  order,
  busy,
}: {
  order: LogicalOrder;
  /** A newer check is in flight: this order is the previous one. */
  busy: boolean;
}) {
  const refs = [
    ...order.entries.flatMap((entry) => [entry.table, entry.variable]),
    ...order.manual.flatMap((item) => (item.table ? [item.table] : [])),
    ...order.clips.map((clip) => clip.variable),
    ...order.row_keys.flatMap((key) =>
      key.columns.map((column) => column.variable),
    ),
  ];
  // The order names every row; those stand in where the site has no page to
  // read a name from (#2220).
  const ordered = new Map<string, string>([
    ...order.entries.flatMap((entry) => [
      [entry.table, entry.table_name] as const,
      [entry.variable, entry.variable_name] as const,
    ]),
    ...order.clips.map((clip) => [clip.variable, clip.variable_name] as const),
    ...order.row_keys.flatMap((key) =>
      key.columns.map(
        (column) => [column.variable, column.variable_name] as const,
      ),
    ),
  ]);
  const names = new Map([...ordered, ...useCatalogNames(refs)]);
  const pending = useCatalogNamesPending(refs);
  const tablePeriods = useTablePeriods([
    ...order.entries.map((entry) => entry.table),
    ...order.manual.flatMap((item) => (item.table ? [item.table] : [])),
  ]);
  // A name still loading is a placeholder, not a link; only a settled miss
  // says the name is unavailable.
  const loadingName = (ref: string) => pending.has(ref) && !names.has(ref);
  const variableName = (ref: string) =>
    loadingName(ref) ? (
      <NameSkeleton label="Loading variable name" />
    ) : (
      (names.get(ref) ?? "Variable name unavailable")
    );
  const variableLink = (ref: string) =>
    loadingName(ref) ? (
      <span className={styles.pendingLink}>{variableName(ref)}</span>
    ) : (
      <a className="link" href={catalogHref(ref)}>
        {variableName(ref)}
      </a>
    );
  // A source entered by hand has no table: it is its own group, and the
  // columns entered by hand in a catalog table join that table's group.
  const groups = new Map<
    string,
    {
      source: string;
      table: string | null;
      entries: Entry[];
      manual: ManualItem[];
    }
  >();
  const groupOf = (source: string, table: string | null) => {
    const key = JSON.stringify([source, table]);
    let group = groups.get(key);
    if (!group) {
      group = { source, table, entries: [], manual: [] };
      groups.set(key, group);
    }
    return group;
  };
  for (const entry of order.entries)
    groupOf(entry.source, entry.table).entries.push(entry);
  for (const item of order.manual)
    groupOf(item.source, item.table).manual.push(item);
  // A clip is per project table, not per catalog table: it goes under the
  // first group of its project table.
  const firstOfSource = new Map<string, string>();
  for (const [key, group] of groups)
    if (!firstOfSource.has(group.source)) firstOfSource.set(group.source, key);
  return (
    <section
      aria-labelledby="delivery-heading"
      aria-busy={busy || undefined}
      className={styles.summary}
    >
      <h2 id="delivery-heading" className={styles.heading}>
        What will be delivered
      </h2>
      {groups.size === 0 ? (
        <p className={styles.note}>No columns yet.</p>
      ) : (
        [...groups].map(([key, group]) => {
          const clips = order.clips.filter(
            (clip) =>
              firstOfSource.get(clip.source) === key &&
              periodsLess(clip.requested, clip.ordered).length > 0,
          );
          // A table's row identifier can change over time: each column keeps
          // the periods it identifies a row in.
          const rowKeys = new Map<
            string,
            { spelling: string; variable: string; periods: CatalogPeriod[] }
          >();
          for (const rowKey of order.row_keys) {
            if (rowKey.source !== group.source || rowKey.table !== group.table)
              continue;
            for (const column of rowKey.columns) {
              const id = JSON.stringify([column.spelling, column.variable]);
              const seen = rowKeys.get(id);
              if (seen) seen.periods.push(rowKey.period);
              else rowKeys.set(id, { ...column, periods: [rowKey.period] });
            }
          }
          // Every period here is cut to the study years; it reads in the
          // table's grain, as delivered (#2416).
          const yearsOf = (periods: readonly CatalogPeriod[]) =>
            catalogPeriodRuns(
              inTableGrain(
                periods,
                group.table ? tablePeriods[group.table] : undefined,
              ),
            );
          // A group with no catalog column states the years the project asks
          // for, as written (the order does not cut them).
          const years = yearsOf(
            group.entries.length > 0
              ? group.entries.map((entry) => entry.period)
              : group.manual.flatMap((item) => item.period),
          );
          // A column's own years only where they differ from the table's.
          const ownYears = (periods: readonly CatalogPeriod[]) => {
            const own = yearsOf(periods);
            return own === years ? undefined : (own ?? undefined);
          };
          // A table can serve one column over several stretches: one row per
          // column, with all its years. A column whose name changes between
          // stretches keeps a row per name.
          const columns = new Map<
            string,
            { spelling: string; variable: string; periods: CatalogPeriod[] }
          >();
          for (const entry of group.entries) {
            const id = JSON.stringify([
              entry.binding,
              entry.variable,
              entry.spelling,
            ]);
            const seen = columns.get(id);
            if (seen) seen.periods.push(entry.period);
            else
              columns.set(id, {
                spelling: entry.spelling,
                variable: entry.variable,
                periods: [entry.period],
              });
          }
          // A table the catalog merged from several of the provider's tables
          // is delivered under the provider's name for each stretch of years.
          const delivered = new Map<string, CatalogPeriod[]>();
          if (
            group.entries.some(
              (entry) => entry.delivered_table !== entry.table_name,
            )
          )
            for (const entry of group.entries) {
              const seen = delivered.get(entry.delivered_table);
              if (seen) seen.push(entry.period);
              else delivered.set(entry.delivered_table, [entry.period]);
            }
          return (
            <section
              key={key}
              aria-labelledby={`delivery-${encodeURIComponent(key)}`}
              className={styles.table}
            >
              <h3
                id={`delivery-${encodeURIComponent(key)}`}
                className={styles.tableHeading}
                aria-busy={
                  (group.table && loadingName(group.table)) || undefined
                }
              >
                {/* As on the table's card: the file's join key stands in
                    only where the catalog has no name. A source entered by
                    hand has no catalog page: its provider and register, as
                    written. */}
                {group.table === null ? (
                  `${group.manual[0]?.provider_name}: ${group.manual[0]?.register_name}`
                ) : loadingName(group.table) ? (
                  <NameSkeleton label="Loading table name" />
                ) : (
                  <a className="link" href={catalogHref(group.table)}>
                    {names.get(group.table) ?? group.source}
                  </a>
                )}{" "}
                <span className={styles.years}>{years}</span>
              </h3>
              <ul className={styles.columns} aria-label="Columns">
                {[...columns].map(([id, column]) => (
                  <li
                    key={id}
                    className={styles.column}
                    aria-busy={loadingName(column.variable) || undefined}
                  >
                    <EntityPill
                      entity="column"
                      name={column.spelling}
                      years={ownYears(column.periods)}
                    />
                    {variableLink(column.variable)}
                  </li>
                ))}
                {group.manual.map((item) => (
                  <li key={item.binding} className={styles.column}>
                    <EntityPill
                      entity="column"
                      name={item.column}
                      years={ownYears(item.period)}
                    />
                    <TypePill type={item.type as ColumnType} />
                    <span className={styles.note}>{MANUAL_LABEL}</span>
                  </li>
                ))}
              </ul>
              {clips.length > 0 && (
                <div className={styles.fact}>
                  <span className="micro-label">Years not available</span>
                  <ul className={styles.plainList}>
                    {clips.map((clip) => (
                      <li
                        key={JSON.stringify(clip)}
                        aria-busy={loadingName(clip.variable) || undefined}
                      >
                        {variableName(clip.variable)}{" "}
                        <span className={styles.years}>
                          {yearsOf(periodsLess(clip.requested, clip.ordered))}
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {delivered.size > 0 && (
                <div className={styles.fact}>
                  <span className="micro-label">Delivered as</span>
                  <ul className={styles.plainList}>
                    {[...delivered].map(([name, periods]) => (
                      <li key={name}>
                        {name}{" "}
                        <span className={styles.years}>{yearsOf(periods)}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {rowKeys.size > 0 && (
                <div className={styles.fact}>
                  <span className="micro-label">Row identifier</span>
                  <ul className={styles.plainList}>
                    {[...rowKeys].map(([id, column]) => (
                      <li
                        key={id}
                        className={styles.column}
                        aria-busy={loadingName(column.variable) || undefined}
                      >
                        <EntityPill
                          entity="column"
                          name={column.spelling}
                          years={ownYears(column.periods)}
                        />
                        {variableLink(column.variable)}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </section>
          );
        })
      )}
    </section>
  );
}
