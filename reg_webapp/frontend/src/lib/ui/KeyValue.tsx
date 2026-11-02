import styles from "./KeyValue.module.css";
import type { KeyValueRow } from "./types";

/** Labelled metadata rows (#804): a `<dl>` whose terms are muted `label`-level
 * text and whose values are `--text`, mono-faced where the row is an
 * identifier. */
export function KeyValue({ rows }: { rows: KeyValueRow[] }) {
  return (
    <dl className={styles.keyValue}>
      {rows.map((row) => (
        // A label can repeat (two "Type" rows); with its value it does not.
        <div
          key={`${row.label}\u0000${row.value ?? ""}`}
          className={styles.row}
        >
          <dt className="micro-label">{row.label}</dt>
          <dd
            className={
              row.mono ? `${styles.value} ${styles.mono}` : styles.value
            }
          >
            {row.value ?? ""}
          </dd>
        </div>
      ))}
    </dl>
  );
}
