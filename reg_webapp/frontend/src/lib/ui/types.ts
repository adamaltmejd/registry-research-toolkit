// Shared TS types for the visual primitives in `ui/`.

/** A `StatusRow` tone, always paired with its glyph, never hue alone (DESIGN.md → Colors). */
export type StatusTone = "error" | "warn" | "info" | "ok";

/** A `Button` visual variant. `primary` is the single accent-filled CTA per
 *  view; `default`/`ghost` are the quiet chrome; `danger` is destructive. */
export type ButtonVariant = "primary" | "default" | "ghost" | "danger";

/** A `DataTable` column definition. `key` indexes into the row object;
 *  `numeric` right-aligns + mono-faces a measure; `mono` mono-faces an
 *  identifier without forcing alignment; `align` overrides; `width` is a CSS
 *  track size (e.g. `"8rem"`). The FIRST column is the primary/title column by
 *  position (no API flag): it gets a min-width floor on wide screens and, when
 *  the table stacks on narrow canvases, becomes the card title with no
 *  micro-label prefix. Order columns so the title is first. */
export interface Column<Row = object> {
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

/** A `Breadcrumbs` item: an ancestor page, by name. */
export interface BreadcrumbItem {
  label: string;
  href: string;
}
