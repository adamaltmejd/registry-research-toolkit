import styles from "./KeyValue.module.css";
import type { KeyValueRow } from "./types";

/** Labelled metadata rows (#804): a `<dl>` whose terms are muted labels and whose
 * values are `text`, mono where the row is an identifier. */
export function KeyValue({ rows }: { rows: readonly KeyValueRow[] }) {
  return (
    <dl className={styles.keyValue}>
      {rows.map((row, i) => (
        // Labels can repeat, and the list is replaced whole: index keys fit.
        // oxlint-disable-next-line react/no-array-index-key
        <div key={i} className={styles.row}>
          <dt className="micro-label">{row.label}</dt>
          <dd className={row.mono ? styles.mono : undefined}>
            {row.value ?? ""}
          </dd>
        </div>
      ))}
    </dl>
  );
}
