import styles from "./AddBar.module.css";
import { Button } from "./Button";

/** The pane's one primary action while columns are staged. */
export function AddBar({
  count,
  onAdd,
  isDisabled = false,
}: {
  count: number;
  onAdd: () => void;
  isDisabled?: boolean;
}) {
  return (
    <div className={styles.addBar}>
      <Button variant="primary" onPress={onAdd} isInert={isDisabled}>
        Add {count} {count === 1 ? "column" : "columns"} to project
      </Button>
    </div>
  );
}
