import styles from "./TypeTag.module.css";

const TYPES = {
  register: { label: "Register", style: styles.reg },
  variable: { label: "Variable", style: styles.var },
  codeList: { label: "Code list", style: styles.code },
  classification: { label: "Classification", style: styles.class },
  group: { label: "Group", style: styles.group },
} as const;

export type NodeType = keyof typeof TYPES;

/** A node's type (DESIGN.md → Components → TypeTag), in its `cat-*` hue: fill
 * the hue at 10% over `surface`, border at 35%, text the `-ink` stop. The word
 * is the carrier and is fixed per type, so a caller passes only the type. Only
 * where a list mixes types (the palette, mentions, a project's sources), never
 * in a homogeneous list and never for status or selection. */
export function TypeTag({ type }: { type: NodeType }) {
  const { label, style } = TYPES[type];
  return <span className={`${styles.tag} ${style}`}>{label}</span>;
}
