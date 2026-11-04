import type { ReactNode } from "react";
import type { ProjectBinding } from "../project_data";
import styles from "./TypePill.module.css";

/** A column's type as a project binding states it: the one type list of the
 * site (maintainer 2026-11-04). Uncoded text is "Other". */
export type ColumnType = ProjectBinding["type"];

const WORDS: Record<ColumnType, string> = {
  id: "Identifier",
  categorical: "Categorical",
  numeric: "Numeric",
  date: "Date",
  datetime: "Date and time",
  opaque: "Other",
};

/** Decorative: the word beside the glyph carries the type. Drawn on a 12 px
 * box in the pill's muted ink. */
const GLYPHS: Record<ColumnType, ReactNode> = {
  // A key: the column picks out one unit.
  id: (
    <>
      <circle cx="4" cy="6" r="2.5" />
      <path d="M6.5 6h4.5M9.5 6v2" />
    </>
  ),
  // A list of codes with their labels.
  categorical: (
    <>
      <path className={styles.fill} d="M1 1.5h2v2H1zM1 5h2v2H1zM1 8.5h2v2H1z" />
      <path d="M4.5 2.5H11M4.5 6H11M4.5 9.5H11" />
    </>
  ),
  numeric: <path d="M4 1 3 11M9 1 8 11M1 4h10M1 8h10" />,
  date: (
    <>
      <rect x="1" y="2" width="10" height="9" rx="1" />
      <path d="M1 5h10M4 1v2M8 1v2" />
    </>
  ),
  // A clock: a point in time on the day as well.
  datetime: (
    <>
      <circle cx="6" cy="6" r="4.5" />
      <path d="M6 3.5V6l2 1.5" />
    </>
  ),
  opaque: <circle cx="6" cy="6" r="4.5" />,
};

/**
 * A column's type (DESIGN.md → Components → TypePill): a glyph and the word in
 * a hairline `sm` box, with the years it holds in muted mono where a column's
 * type changes over time. One pill per type; never for a node's kind (that is
 * `TypeTag`) or for status.
 */
export function TypePill({
  type,
  years,
}: {
  type: ColumnType;
  /** Already formatted ("2020–2021"); only where the type differs by period. */
  years?: string;
}) {
  return (
    <span className={styles.pill}>
      <svg
        className={styles.glyph}
        viewBox="0 0 12 12"
        width="12"
        height="12"
        aria-hidden="true"
      >
        {GLYPHS[type]}
      </svg>
      {WORDS[type]}
      {years && <span className={styles.years}>{years}</span>}
    </span>
  );
}
