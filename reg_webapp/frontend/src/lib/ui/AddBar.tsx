import styles from "./AddBar.module.css";
import { Button } from "./Button";

/** The pane's one primary action while columns are staged. */
export function AddBar({ count, onAdd }: { count: number; onAdd: () => void }) {
  return (
    <div className={styles.addBar}>
      <Button variant="primary" onPress={onAdd}>
        Add {count} {count === 1 ? "column" : "columns"} to project
      </Button>
    </div>
  );
}
