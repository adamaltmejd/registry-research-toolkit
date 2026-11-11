import type { ReactNode } from "react";
import { ENTITY_GLYPHS } from "./entity_glyphs";
import { type ColumnState, Pill, type PillTone } from "./Pill";

/** Each entity's hue and glyph: a variable and a variable group share both. */
const ENTITIES = {
  register: { tone: "reg", glyph: ENTITY_GLYPHS.register },
  table: { tone: "table", glyph: ENTITY_GLYPHS.table },
  variable: { tone: "var", glyph: ENTITY_GLYPHS.column },
  group: { tone: "var", glyph: ENTITY_GLYPHS.column },
  column: { tone: "column", glyph: ENTITY_GLYPHS.column },
  database: { tone: "database", glyph: ENTITY_GLYPHS.database },
} as const satisfies Record<string, { tone: PillTone; glyph: unknown }>;

export type Entity = keyof typeof ENTITIES;

/**
 * A catalog entity by name (DESIGN.md → Components → Pill): a table pill in
 * a lane, a register's table filter, a project card and a value set; a column
 * pill wherever a column name is shown; a register's or variable's in a
 * search row; a register's table and variable counts on its provider's
 * page; a table's database on its register's table rows. The same size everywhere; only the hue and glyph differ.
 */
export function EntityPill({
  entity,
  name,
  href,
  years,
  state,
  className,
}: {
  entity: Entity;
  /** Empty for a lane span too narrow to hold it; a count's number with its
   * word (a provider's register row). */
  name: ReactNode;
  href?: string;
  /** Already formatted ("2015–2019", "2020–"). */
  years?: string;
  /** A column's: in the project, or outside the years to add. */
  state?: ColumnState;
  /** Stretches the pill into a lane span (LaneGraph); never restyles it. */
  className?: string;
}) {
  const { tone, glyph } = ENTITIES[entity];
  return (
    <Pill
      tone={tone}
      glyph={glyph}
      href={href}
      years={years}
      state={state}
      className={className}
    >
      {name}
    </Pill>
  );
}
