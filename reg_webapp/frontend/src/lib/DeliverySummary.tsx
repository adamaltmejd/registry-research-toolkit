import type { CatalogPeriod, LogicalOrder } from "./api";
import { catalogHref } from "./catalog";
import { catalogPeriodRuns } from "./catalog_period";
import styles from "./DeliverySummary.module.css";
import { ColumnChip } from "./ui/ColumnChip";
import { useCatalogNames } from "./use_catalog_names";

type Entry = LogicalOrder["entries"][number];

/** `requested` less `ordered`: the years a clip cannot deliver. Half-open ISO
 * days compare as strings; a null end is open. */
function missing(
  requested: readonly CatalogPeriod[],
  ordered: readonly CatalogPeriod[],
): CatalogPeriod[] {
  let rest = requested.map((period) => ({ ...period }));
  for (const cut of ordered) {
    rest = rest.flatMap((period) => {
      const overlaps =
        (cut.end === null || period.start === null || period.start < cut.end) &&
        (cut.start === null || period.end === null || cut.start < period.end);
      if (!overlaps) return [period];
      const pieces: CatalogPeriod[] = [];
      if (
        cut.start !== null &&
        (period.start === null || period.start < cut.start)
      )
        pieces.push({ start: period.start, end: cut.start });
      if (cut.end !== null && (period.end === null || period.end > cut.end))
        pieces.push({ start: cut.end, end: period.end });
      return pieces;
    });
  }
  return rest;
}

/** What the order would deliver (the logical order, maintainer ruling Q1,
 * 2026-11-04): per table its columns and years, the years a column does not
 * have, and the row identifier. No order file: the steward's instructions say
 * how to order. */
export function DeliverySummary({ order }: { order: LogicalOrder }) {
  const names = useCatalogNames([
    ...order.entries.flatMap((entry) => [entry.table, entry.variable]),
    ...order.clips.map((clip) => clip.variable),
    ...order.row_keys.flatMap((key) =>
      key.columns.map((column) => column.variable),
    ),
  ]);
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
    <section aria-labelledby="delivery-heading" className={styles.summary}>
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
              missing(clip.requested, clip.ordered).length > 0,
          );
          const rowKeys = [
            ...new Map(
              order.row_keys
                .filter(
                  (rowKey) =>
                    rowKey.source === group.source &&
                    rowKey.table === group.table,
                )
                .flatMap((rowKey) => rowKey.columns)
                .map((column) => [column.spelling, column] as const),
            ).values(),
          ];
          const years = catalogPeriodRuns(
            group.entries.map((entry) => entry.period),
          );
          return (
            <section
              key={key}
              aria-labelledby={`delivery-${encodeURIComponent(key)}`}
              className={styles.table}
            >
              <h3
                id={`delivery-${encodeURIComponent(key)}`}
                className={styles.tableHeading}
              >
                <a className="link" href={catalogHref(group.table)}>
                  {names.get(group.table) ?? "Table name unavailable"}
                </a>{" "}
                <span className={styles.years}>{years}</span>
              </h3>
              <ul className={styles.columns} aria-label="Columns">
                {group.entries.map((entry) => (
                  <li
                    key={`${entry.binding}:${entry.period.start}`}
                    className={styles.column}
                  >
                    <ColumnChip
                      name={entry.spelling}
                      years={
                        catalogPeriodRuns([entry.period]) === years
                          ? undefined
                          : catalogPeriodRuns([entry.period])
                      }
                    />
                    <a className="link" href={catalogHref(entry.variable)}>
                      {names.get(entry.variable) ?? "Variable name unavailable"}
                    </a>
                  </li>
                ))}
              </ul>
              {clips.length > 0 && (
                <div className={styles.fact}>
                  <span className="micro-label">Years not available</span>
                  <ul className={styles.plainList}>
                    {clips.map((clip) => (
                      <li key={JSON.stringify(clip)}>
                        {names.get(clip.variable) ??
                          "Variable name unavailable"}{" "}
                        <span className={styles.years}>
                          {catalogPeriodRuns(
                            missing(clip.requested, clip.ordered),
                          )}
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {rowKeys.length > 0 && (
                <div className={styles.fact}>
                  <span className="micro-label">Row identifier</span>
                  <span className={styles.chips}>
                    {rowKeys.map((column) => (
                      <a
                        key={column.spelling}
                        className="link"
                        href={catalogHref(column.variable)}
                        aria-label={`${names.get(column.variable) ?? "Row identifier"}: ${column.spelling}`}
                      >
                        <ColumnChip name={column.spelling} />
                      </a>
                    ))}
                  </span>
                </div>
              )}
            </section>
          );
        })
      )}
    </section>
  );
}
