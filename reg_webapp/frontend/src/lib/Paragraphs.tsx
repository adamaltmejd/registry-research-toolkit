import styles from "./CatalogNodeView.module.css";
import { repeatKeys } from "./series_breaks";

/** Provider text as prose paragraphs, one per line. */
export function Paragraphs({ text }: { text: string }) {
  const lines = text.split("\n");
  const keys = repeatKeys(lines);
  return lines.map((line, index) => (
    <p key={keys[index]} className={styles.prose}>
      {line}
    </p>
  ));
}
