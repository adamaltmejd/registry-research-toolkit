import { type ReactNode, useState } from "react";
import styles from "./StatusRow.module.css";
import type { StatusTone } from "./types";

const GLYPH: Record<StatusTone, string> = {
  error: "✕",
  warn: "▲",
  info: "i",
  ok: "✓",
};

interface Props {
  /** The verdict. `null` renders the bare live region with no fill, border or
   * height: a row that exists so its first message lands in a region the page
   * already had (the year entries' refusals), not a new node that screen
   * readers may skip. */
  tone: StatusTone | null;
  /** `alert` for a failure the reader must hear now, `status` for a result they
   * asked for (an `<output>`). Omit for a standing note. */
  live?: "alert" | "status";
  id?: string;
  /** The caller's own placement (margin); never colour or type. */
  className?: string;
  /** A control after the text (Dismiss, a link), pushed to the row's end. */
  action?: ReactNode;
  children?: ReactNode;
}

/** A verdict in one line (DESIGN.md → Components → StatusRow): the status tint
 * as fill, its foreground as text and border (`err-border` for an error), the
 * glyph first and `aria-hidden`, then a sentence that says what is wrong and
 * what to do. It rises in when it appears, never on page load. */
export function StatusRow({
  tone,
  live,
  id,
  className,
  action,
  children,
}: Props) {
  // The rise is for a verdict that arrives or changes after mount, never for
  // the tone the row first paints with (page load): it turns on at the first
  // tone change and stays on, so a re-render does not replay it.
  const [seenTone, setSeenTone] = useState(tone);
  const [rises, setRises] = useState(false);
  if (seenTone !== tone) {
    setSeenTone(tone);
    setRises(tone !== null);
  }
  const own =
    tone === null
      ? ""
      : `${styles.row} ${styles[tone]}${rises ? " motion-rise-in" : ""}`;
  const Root = live === "status" ? "output" : "div";
  return (
    <Root
      id={id}
      role={live === "alert" ? "alert" : undefined}
      className={`${own} ${className ?? ""}`.trim()}
    >
      {tone !== null && (
        <>
          <span aria-hidden="true">{GLYPH[tone]}</span>
          <span className={styles.text}>{children}</span>
          {action}
        </>
      )}
    </Root>
  );
}
