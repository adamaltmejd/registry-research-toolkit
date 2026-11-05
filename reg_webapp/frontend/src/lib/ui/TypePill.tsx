import type { ReactNode } from "react";
import type { ProjectBinding } from "../project_data";
import styles from "./TypePill.module.css";

/** A column's catalog type: the names a project binding's optional `type`
 * override uses too, the one type list of the site (#2071). */
export type ColumnType = NonNullable<ProjectBinding["type"]>;

const WORDS: Record<ColumnType, string> = {
  integer: "Whole number",
  decimal: "Decimal number",
  text: "Text",
  date: "Date",
  datetime: "Date and time",
  boolean: "Yes or no",
  geometry: "Geometry",
  uuid: "UUID",
  binary: "Binary",
  identifier: "Identifier",
};

const NUMBER = <path d="M4 1 3 11M9 1 8 11M1 4h10M1 8h10" />;
const OTHER = <circle cx="6" cy="6" r="4.5" />;

/** Decorative: the word beside the glyph carries the type. Drawn on a 12 px
 * box in the pill's muted ink. */
const GLYPHS: Record<ColumnType, ReactNode> = {
  integer: NUMBER,
  decimal: NUMBER,
  // Lines of text.
  text: <path d="M1 2.5h10M1 6h10M1 9.5h6" />,
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
  // A tick.
  boolean: <path d="M1.5 6.5 4.5 9.5 10.5 2.5" />,
  geometry: <path d="M6 1.5 11 10.5H1z" />,
  uuid: OTHER,
  binary: OTHER,
  // A key: the column picks out one unit.
  identifier: (
    <>
      <circle cx="4" cy="6" r="2.5" />
      <path d="M6.5 6h4.5M9.5 6v2" />
    </>
  ),
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
