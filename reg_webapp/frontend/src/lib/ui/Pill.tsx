import type { ReactNode } from "react";
import styles from "./Pill.module.css";

/** The pill hues: a node kind's, an identifier's, and every other column
 * type's (maintainer 2026-11-10, #2317). */
export type PillTone =
  | "reg"
  | "var"
  | "code"
  | "class"
  | "group"
  | "id"
  | "type";

/**
 * The one pill (DESIGN.md → Components → Pill): an optional glyph, the word,
 * and years in muted mono where a caller has them, in its tone's hue.
 * `TypeTag` (a node's kind) and `TypePill` (a column's type) are its callers;
 * use those, which fix the word and glyph per type.
 */
export function Pill({
  tone,
  glyph,
  years,
  children,
}: {
  tone: PillTone;
  /** Paths on a 14 px box; decorative, the word carries the meaning. */
  glyph?: ReactNode;
  /** Already formatted ("2020–2021"). */
  years?: string;
  children: ReactNode;
}) {
  return (
    <span className={`${styles.pill} ${styles[tone]}`}>
      {glyph && (
        <svg
          className={styles.glyph}
          viewBox="0 0 14 14"
          width="12"
          height="12"
          aria-hidden="true"
        >
          {glyph}
        </svg>
      )}
      {children}
      {years && <span className={styles.years}>{years}</span>}
    </span>
  );
}
