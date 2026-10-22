import { Input, SearchField } from "react-aria-components";
import styles from "./FilterInput.module.css";

interface Props {
  value: string;
  onChange: (value: string) => void;
  /** The list's size, and how many rows the filter leaves; `shown` is null while
   * the count is not known yet. The count shows only while filtering. */
  total: number;
  shown: number | null;
  label: string;
  placeholder: string;
}

/** Type-to-filter over a list the caller owns and filters with `rankFilter`
 * (catalog.ts), so the matcher stays single-sourced. Escape clears it. */
export function FilterInput({
  value,
  onChange,
  total,
  shown,
  label,
  placeholder,
}: Props) {
  return (
    <div className={styles.filter}>
      <SearchField
        aria-label={label}
        value={value}
        onChange={onChange}
        className={styles.field}
      >
        <Input
          className={styles.input}
          placeholder={placeholder}
          autoComplete="off"
        />
      </SearchField>
      {value.trim() !== "" && shown !== null && (
        <span className={styles.count} aria-live="polite">
          {shown} of {total}
        </span>
      )}
    </div>
  );
}
