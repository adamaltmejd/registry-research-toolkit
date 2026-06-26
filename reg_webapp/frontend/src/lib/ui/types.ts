// Public TS types for the shared visual primitives (#804). Consumers import
// these alongside the components from the `../ui` barrel so the downstream
// design-system children (#806–#809) get typed APIs (column defs, tones,
// breadcrumb items, key-value rows, button variants) instead of re-declaring
// them per call site.

/** A `Tag` tone. Three disjoint sub-systems (DESIGN.md → Color):
 *  - chrome: `neutral` (default) | `accent` — brand chrome, never status.
 *  - categorical: tags result/node TYPE only (`--cat-*`); never reused for
 *    status/accent or "is a variable" collides with "is selected/an error".
 *  - status: `error`/`warn`/`info`/`ok` — paired with a glyph, never hue alone. */
export type TagTone =
  | "neutral"
  | "accent"
  | "reg"
  | "var"
  | "code"
  | "class"
  | "group"
  | "error"
  | "warn"
  | "info"
  | "ok";

/** A `Button` visual variant. `primary` is the single accent-filled CTA per
 *  view; `default`/`ghost` are the quiet chrome; `danger` is destructive. */
export type ButtonVariant = "primary" | "default" | "ghost" | "danger";

/** A `Button` size. */
export type ButtonSize = "sm" | "md";

/** A `DataTable` column definition. `key` indexes into the row object;
 *  `numeric` right-aligns + mono-faces a measure; `mono` mono-faces an
 *  identifier without forcing alignment; `align` overrides; `width` is a CSS
 *  track size (e.g. `"8rem"`). */
export interface Column<Row = Record<string, unknown>> {
  key: keyof Row & string;
  label: string;
  align?: "start" | "end";
  /** Mono-face the cell (codes, FQIDs, slugs, years). */
  mono?: boolean;
  /** A measure: right-aligned + mono-faced unless overridden by `align`. */
  numeric?: boolean;
  /** CSS track width, e.g. `"8rem"`. */
  width?: string;
}

/** A `Breadcrumbs` item. The last item is the current page (no `href`). */
export interface BreadcrumbItem {
  label: string;
  href?: string;
}

/** A `KeyValue` row. `mono` mono-faces the value (identifiers). */
export interface KeyValueRow {
  label: string;
  value: string;
  mono?: boolean;
}
