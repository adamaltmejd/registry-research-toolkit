import { useId } from "react";
import styles from "./AddBar.module.css";
import { Button } from "./Button";

/** The pane's actions while a change is staged: adding counts variables, not
 * columns, since a variable is added whole to each table (#2408); removing is
 * the `danger` button. */
export function AddBar({
  adds,
  removes,
  note = null,
  onAdd,
  onRemove,
}: {
  /** What else a removal takes, said before the press: a table whose last
   * variable goes leaves the project with it. */
  note?: string | null;
  /** The variables staged to add. */
  adds: number;
  /** Whether any variable in the project is staged to go. */
  removes: boolean;
  onAdd: () => void;
  onRemove: () => void;
}) {
  const noteId = useId();
  return (
    <div className={styles.addBar}>
      {note && (
        <p id={noteId} className={styles.note}>
          {note}
        </p>
      )}
      {removes && (
        <Button
          variant="danger"
          aria-describedby={note ? noteId : undefined}
          onPress={onRemove}
        >
          Remove from project
        </Button>
      )}
      {adds > 0 && (
        <Button variant="primary" onPress={onAdd}>
          Add {adds} {adds === 1 ? "variable" : "variables"} to project
        </Button>
      )}
    </div>
  );
}
