import type { ReactNode } from "react";
import styles from "./EmptyState.module.css";

interface Props {
  /** What is empty, as a statement. The next step goes in `action`, never here. */
  title: string;
  description?: string;
  /** The next move: a default `Button` that names it. */
  action?: ReactNode;
}

/** A quiet empty region (DESIGN.md → Empty and loading states): an `h3`-weight
 * title, a `body-sm` description and a button that names the next action. */
export function EmptyState({ title, description, action }: Props) {
  return (
    <div className={styles.empty}>
      <p className={styles.title}>{title}</p>
      {description && <p className={styles.description}>{description}</p>}
      {action && <div className={styles.action}>{action}</div>}
    </div>
  );
}
