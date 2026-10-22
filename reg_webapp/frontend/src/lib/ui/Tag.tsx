import type { ReactNode } from "react";
import styles from "./Tag.module.css";
import type { StatusTone } from "./types";

type ChromeTone = "neutral" | "accent";

/** A status tone requires a glyph (DESIGN.md → Colors: never hue alone); a chrome
 * tone takes one optionally. The glyph is visual only, so the label text or its
 * context must carry the meaning too. */
type Props = { children: ReactNode } & (
  | { tone?: ChromeTone; glyph?: string }
  | { tone: StatusTone; glyph: string }
);

const TONE: Record<ChromeTone | StatusTone, string> = {
  neutral: styles.neutral,
  accent: styles.accent,
  error: styles.error,
  warn: styles.warn,
  info: styles.info,
  ok: styles.ok,
};

/** A small inline label: chrome (neutral, accent) or status. A node's type is
 * a `TypeTag`, not a tone here. */
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
