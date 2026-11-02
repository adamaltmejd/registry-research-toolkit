import type { ReactNode } from "react";
import styles from "./TypeTag.module.css";

const TYPES = {
  register: { label: "Register", style: styles.reg },
  variable: { label: "Variable", style: styles.var },
  codeList: { label: "Code list", style: styles.code },
  classification: { label: "Classification", style: styles.class },
  group: { label: "Group", style: styles.group },
} as const;

export type NodeType = keyof typeof TYPES;

/** The kind glyphs, drawn in the tag's ink: a register is a table, a variable
 * one column of it. Decorative: the word beside it carries the type. */
const GLYPHS: Partial<Record<NodeType, ReactNode>> = {
  register: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <path d="M1.5 5.5h11M1.5 8.5h11M5.5 5.5v6M9.5 5.5v6" />
    </>
  ),
  variable: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <rect x="5.5" y="2.5" width="3" height="9" className={styles.glyphFill} />
    </>
  ),
};

/** A node's type (DESIGN.md → Components → TypeTag), in its `cat-*` hue: fill
 * the hue at 10% over `surface`, border at 35%, text the `-ink` stop. The word
 * is the carrier and is fixed per type, so a caller passes only the type. Where
 * a list mixes types (the palette, mentions, a project's sources), and as the
 * kind mark with its glyph (`glyph`) over a register's or a variable's page
 * title; never in a homogeneous list and never for status or selection. */
export function TypeTag({
  type,
  glyph = false,
}: {
  type: NodeType;
  glyph?: boolean;
}) {
  const { label, style } = TYPES[type];
  const drawn = glyph ? GLYPHS[type] : undefined;
  return (
    <span className={`${styles.tag} ${style}`}>
      {drawn && (
        <svg
          className={styles.glyph}
          viewBox="0 0 14 14"
          width="14"
          height="14"
          aria-hidden="true"
        >
          {drawn}
        </svg>
      )}
      {label}
    </span>
  );
}
