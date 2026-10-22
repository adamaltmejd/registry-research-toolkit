import styles from "./FilterCount.module.css";

interface Props {
  /** The top-bar filter's text (browse_filter.ts), which narrows this list too. */
  filter: string;
  /** How many rows the filter leaves, out of `total`. */
  shown: number;
  total: number;
  /** The plural noun the list counts, e.g. "variables". */
  noun: string;
}

/** The count of a detail list narrowed by the top-bar filter. It stays mounted
 * and empty while nothing is typed, because a live region inserted together with
 * its first text is not announced. */
export function FilterCount({ filter, shown, total, noun }: Props) {
  return (
    <p className={styles.count} aria-live="polite">
      {filter.trim() !== "" && `Showing ${shown} of ${total} ${noun}`}
    </p>
  );
}
