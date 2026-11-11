import type { ReactNode } from "react";
import type { ProjectBinding } from "../project_data";
import { Pill } from "./Pill";

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
 * box in the pill's ink. */
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
 * A column's type (DESIGN.md → Components → Pill): its glyph and the word,
 * with the years it holds where a column's type changes over time. An
 * identifier takes its own hue, every other type one shared hue (maintainer
 * ruling 2026-11-10, quoted in PR #2391). One pill per type; never for a node's kind (that is
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
    <Pill
      tone={type === "identifier" ? "id" : "type"}
      // Drawn on a 12 px box, centred on the pill's 14 px one.
      glyph={<g transform="translate(1 1)">{GLYPHS[type]}</g>}
      years={years}
      kind
    >
      {WORDS[type]}
    </Pill>
  );
}
