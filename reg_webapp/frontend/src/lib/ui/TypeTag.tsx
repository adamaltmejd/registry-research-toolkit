import { ENTITY_GLYPHS } from "./entity_glyphs";
import { Pill } from "./Pill";

/** The kind words, hues and glyphs: a variable group is a variable's colour
 * and glyph, told apart by its word (maintainer, #2399, 2026-11-11). */
const KINDS = {
  register: { label: "Register", tone: "reg", glyph: ENTITY_GLYPHS.register },
  variable: { label: "Variable", tone: "var", glyph: ENTITY_GLYPHS.column },
  group: { label: "Variable group", tone: "var", glyph: ENTITY_GLYPHS.column },
} as const;

export type NodeType = keyof typeof KINDS;

/** A page's kind pill (DESIGN.md → Components → Pill), first in its pill
 * row: the word is fixed per kind, so a caller passes only the kind. Never
 * for status or selection. */
export function TypeTag({ type }: { type: NodeType }) {
  const { label, tone, glyph } = KINDS[type];
  return (
    <Pill tone={tone} glyph={glyph} kind>
      {label}
    </Pill>
  );
}
