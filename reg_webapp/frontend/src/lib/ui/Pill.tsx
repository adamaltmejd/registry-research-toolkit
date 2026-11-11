import type { PointerEvent, ReactNode } from "react";
import styles from "./Pill.module.css";

/** The pill hues: an entity's (register, table, variable, the white column),
 * a column type's (identifier, every other type), and a steward's database's
 * (no hue). #2391, #2399, #2110. */
export type PillTone =
  | "reg"
  | "table"
  | "var"
  | "column"
  | "id"
  | "type"
  | "database";

/** A column pill's state: outlined, in the project, or outside the years to
 * add. */
export type ColumnState = "default" | "in" | "out";

/**
 * The one pill (DESIGN.md → Components → Pill): a glyph in its hue's ink, the
 * name in `text`, and years in muted mono where a caller has them. One size
 * for every pill, so a table pill, a column pill, a span and a filter read as
 * one object (maintainer, #2399, 2026-11-11). A long name ends in an ellipsis
 * and shows the whole of it as hover text only when cut. Callers fix the word
 * and glyph: `EntityPill`, `TypeTag`, `TypePill`.
 */
export function Pill({
  tone,
  glyph,
  years,
  href,
  kind = false,
  state,
  className,
  children,
}: {
  tone: PillTone;
  /** Paths on a 14 px box; decorative, the name carries the meaning. */
  glyph?: ReactNode;
  /** Already formatted ("2020–2021"). */
  years?: string;
  /** The pill is a link to its page. */
  href?: string;
  /** A page's kind pill ("Variable"): the word at weight 400. */
  kind?: boolean;
  /** A column pill's state. */
  state?: ColumnState;
  /** Stretches the pill (a lane span); never restyles it. */
  className?: string;
  children: ReactNode;
}) {
  const body = (
    <>
      {/* A nameless pill (a span's bare bar) has no glyph either. */}
      {glyph && children !== "" && (
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
      {children !== "" && (
        <span className={styles.name} onPointerEnter={titleWhenCut}>
          {children}
        </span>
      )}
      {years && <span className={styles.years}>{years}</span>}
    </>
  );
  const classes = `${styles.pill} ${styles[tone]} ${kind ? styles.kind : ""} ${className ?? ""}`;
  return href ? (
    <a className={classes} href={href} data-state={state}>
      {body}
    </a>
  ) : (
    <span className={classes} data-state={state}>
      {body}
    </span>
  );
}

/** Hover text only where the name is cut: set when the pointer arrives, since
 * the pill's width follows its container. */
function titleWhenCut(event: PointerEvent<HTMLSpanElement>) {
  const node = event.currentTarget;
  const cut = node.scrollWidth > node.clientWidth;
  if (cut) node.title = node.textContent ?? "";
  else node.removeAttribute("title");
}
