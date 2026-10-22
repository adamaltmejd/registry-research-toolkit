import styles from "./EmptyState.module.css";

interface Props {
  title: string;
  description?: string;
}

/** A quiet empty region (DESIGN.md → Empty and loading states): the headline
 * and, optionally, one line that names the next move. */
export function EmptyState({ title, description }: Props) {
  return (
    <div className={styles.empty}>
      <p className={styles.title}>{title}</p>
      {description && <p className={styles.description}>{description}</p>}
    </div>
  );
}
