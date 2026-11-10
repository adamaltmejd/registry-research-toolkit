import type { ReactNode } from "react";
import styles from "./Panel.module.css";

interface Props {
  /** None where the body heads itself (a VariableRowList's axis row). */
  title?: ReactNode;
  action?: ReactNode;
  headingLevel?: 2 | 3;
  /** No body padding: the body is a table or list that owns its own. */
  flush?: boolean;
  children: ReactNode;
}

/** The unit of grouping (DESIGN.md → Components): a `label`-level title over a
 * body, on the raised surface. Omit-when-empty is the caller's concern: a Panel
 * always renders its chrome. */
export function Panel({
  title,
  action,
  headingLevel = 2,
  flush = false,
  children,
}: Props) {
  const Heading = headingLevel === 3 ? "h3" : "h2";
  return (
    <section className={styles.panel}>
      {title === undefined ? null : action ? (
        <header className={styles.header}>
          <Heading className="micro-label">{title}</Heading>
          {action}
        </header>
      ) : (
        <Heading className={`micro-label ${styles.title}`}>{title}</Heading>
      )}
      <div className={flush ? undefined : styles.body}>{children}</div>
    </section>
  );
}
