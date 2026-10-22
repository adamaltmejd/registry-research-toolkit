import styles from "./ColumnChip.module.css";

export type ColumnChipState = "default" | "in" | "out";

/** The one treatment for a column name, the identifier a researcher types in
 * code (DESIGN.md → ColumnChip): `column` type in an outlined `sm` chip, its
 * years inside in muted mono. Three states: outlined, in the project (`accent`
 * fill) and outside the study window (`viz-span-out`, `text-faint`). A lane
 * span in a `LaneGraph` is the same shape, so the two read as one object.
 * Never for a variable name, an FQID or a slug. */
export function ColumnChip({
  name,
  years,
  state = "default",
  className,
}: {
  name: string;
  /** Already formatted ("2015–2019", "2020–"). */
  years?: string;
  state?: ColumnChipState;
  /** Stretches the chip into a lane span (LaneGraph); never restyles it. */
  className?: string;
}) {
  return (
    <span className={`${styles.chip} ${styles[state]} ${className ?? ""}`}>
      {name}
      {years && <span className={styles.years}>{years}</span>}
    </span>
  );
}
