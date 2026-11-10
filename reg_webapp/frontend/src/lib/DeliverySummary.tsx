import type { CatalogPeriod, LogicalOrder } from "./api";
import { catalogHref } from "./catalog";
import { catalogPeriodRuns, periodsLess } from "./catalog_period";
import styles from "./DeliverySummary.module.css";
import { ColumnChip } from "./ui/ColumnChip";
import { NameSkeleton } from "./ui/Skeleton";
import { useCatalogNames, useCatalogNamesPending } from "./use_catalog_names";

type Entry = LogicalOrder["entries"][number];

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
          const years = catalogPeriodRuns(
            group.entries.map((entry) => entry.period),
          );
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
          return (
            <section
              key={key}
              aria-labelledby={`delivery-${encodeURIComponent(key)}`}
              className={styles.table}
            >
              <h3
                id={`delivery-${encodeURIComponent(key)}`}
                className={styles.tableHeading}
                aria-busy={loadingName(group.table) || undefined}
              >
                {/* As on the table's card: the file's join key stands in
                    only where the catalog has no name. */}
                {loadingName(group.table) ? (
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
                    <ColumnChip
                      name={column.spelling}
                      years={
                        catalogPeriodRuns(column.periods) === years
                          ? undefined
                          : catalogPeriodRuns(column.periods)
                      }
                    />
                    {variableLink(column.variable)}
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
                          {catalogPeriodRuns(
                            periodsLess(clip.requested, clip.ordered),
                          )}
                        </span>
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
                        <ColumnChip
                          name={column.spelling}
                          years={
                            catalogPeriodRuns(column.periods) === years
                              ? undefined
                              : catalogPeriodRuns(column.periods)
                          }
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
