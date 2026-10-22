import type { ReactNode } from "react";
import styles from "./Tag.module.css";
import type { PlainTone, StatusTone } from "./types";

/** A status tone requires a glyph (DESIGN.md → Colors: never hue alone); a plain
 * tone takes one optionally. The glyph is visual only, so the label text or its
 * context must carry the meaning too. */
type Props = { children: ReactNode } & (
  | { tone?: PlainTone; glyph?: string }
  | { tone: StatusTone; glyph: string }
);

const TONE: Record<PlainTone | StatusTone, string> = {
  neutral: styles.neutral,
  accent: styles.accent,
  reg: `${styles.category} ${styles.reg}`,
  var: `${styles.category} ${styles.var}`,
  code: `${styles.category} ${styles.code}`,
  class: `${styles.category} ${styles.class}`,
  group: `${styles.category} ${styles.group}`,
  error: styles.error,
  warn: styles.warn,
  info: styles.info,
  ok: styles.ok,
};

/** A small inline label in one of the three colour sub-systems: chrome
 * (neutral, accent), node type (`cat-*`, mixed lists only) or status. */
export function Tag({ tone = "neutral", glyph, children }: Props) {
  return (
    <span className={`${styles.tag} ${TONE[tone]}`}>
      {glyph && (
        <span className={styles.glyph} aria-hidden="true">
          {glyph}
        </span>
      )}
      <span>{children}</span>
    </span>
  );
}
