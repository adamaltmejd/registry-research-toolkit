import type { ReactNode } from "react";
import { Pill, type PillTone } from "./Pill";

const TYPES = {
  register: { label: "Register", tone: "reg" },
  // A table is a part of its register, so it takes the register's hue; the
  // word tells the two apart.
  table: { label: "Table", tone: "reg" },
  variable: { label: "Variable", tone: "var" },
  codeList: { label: "Code list", tone: "code" },
  classification: { label: "Classification", tone: "class" },
  group: { label: "Variable group", tone: "group" },
} as const satisfies Record<string, { label: string; tone: PillTone }>;

export type NodeType = keyof typeof TYPES;

/** The kind glyphs on a page's pill row, drawn in the pill's ink: a register
 * is a grid, a table one sheet of it, a variable one column, a group several
 * columns.
 * Decorative: the word beside it carries the type. */
const GLYPHS: Partial<Record<NodeType, ReactNode>> = {
  register: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <path d="M1.5 5.5h11M1.5 8.5h11M5.5 5.5v6M9.5 5.5v6" />
    </>
  ),
  // One of a register's tables: a sheet with its header row filled.
  table: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <rect x="1.5" y="2.5" width="11" height="3" fill="currentColor" />
      <path d="M1.5 8.5h11" />
    </>
  ),
  variable: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <rect x="5.5" y="2.5" width="3" height="9" fill="currentColor" />
    </>
  ),
  group: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <rect x="3" y="2.5" width="2.5" height="9" fill="currentColor" />
      <rect x="8.5" y="2.5" width="2.5" height="9" fill="currentColor" />
    </>
  ),
};

/** A node's kind (DESIGN.md → Components → Pill): the word is fixed per type,
 * so a caller passes only the type. In a mixed list (the palette, mentions, a
 * project's sources) without its glyph; with it (`glyph`) as the kind pill on
 * a register's, variable's or group's page, and before each table lane of a
 * variable page's drawing (#2319). Never in a homogeneous list and never for
 * status or selection. */
export function TypeTag({
  type,
  glyph = false,
}: {
  type: NodeType;
  glyph?: boolean;
}) {
  const { label, tone } = TYPES[type];
  return (
    <Pill tone={tone} glyph={glyph ? GLYPHS[type] : undefined}>
      {label}
    </Pill>
  );
}
