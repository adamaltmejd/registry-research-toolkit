import type { ReactNode } from "react";
import styles from "./Panel.module.css";

interface Props {
  title: ReactNode;
  /** No body padding: the body is a table or list that owns its own. */
  flush?: boolean;
  children: ReactNode;
}

/** The unit of grouping (DESIGN.md → Components): a `label`-level title over a
 * body, on the raised surface. Omit-when-empty is the caller's concern: a Panel
 * always renders its chrome. */
export function Panel({ title, flush = false, children }: Props) {
  return (
    <section className={styles.panel}>
      <h2 className={`micro-label ${styles.title}`}>{title}</h2>
      <div className={flush ? undefined : styles.body}>{children}</div>
    </section>
  );
}
