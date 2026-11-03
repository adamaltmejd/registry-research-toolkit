import styles from "./Skeleton.module.css";

interface Props {
  variant?: "line" | "block";
  /** A CSS width ("12rem", "60%"); full width when omitted. */
  width?: string;
  /** How many stacked placeholders. */
  count?: number;
}

/** A loading placeholder. Decorative: the container that swaps it for content
 * carries `aria-busy`. The shimmer stops under reduced motion. */
export function Skeleton({ variant = "line", width, count = 1 }: Props) {
  const placeholders = Array.from(
    { length: count },
    (_, i) => `${variant}-${i}`,
  );
  return (
    <div className={styles.stack} aria-hidden="true">
      {placeholders.map((id) => (
        <div
          key={id}
          className={`${styles.skeleton} ${styles[variant]}`}
          style={width ? { width } : undefined}
        />
      ))}
    </div>
  );
}
