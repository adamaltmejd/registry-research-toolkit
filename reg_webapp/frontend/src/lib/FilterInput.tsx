import { Input, SearchField } from "react-aria-components";
import styles from "./FilterInput.module.css";

interface Props {
  value: string;
  onChange: (value: string) => void;
  /** The list's full size, for the "12 of 740" count. */
  total: number;
  /** How many match; null while a server-paged count is not known yet, so the
   * count is withheld rather than guessed. */
  shown: number | null;
  label: string;
  placeholder: string;
}

/** A type-to-filter field over one list on a page. It owns only the field and
 * the count; the caller filters (with `matchesFilter`, or on the server) so the
 * matcher stays single-sourced. The count shows only while filtering. Escape
 * clears it (RAC `SearchField`). */
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
        className={styles.field}
        aria-label={label}
        value={value}
        onChange={onChange}
      >
        <Input
          className={styles.input}
          placeholder={placeholder}
          autoComplete="off"
        />
      </SearchField>
      <span className={styles.count} aria-live="polite">
        {value.trim() !== "" && shown !== null && `${shown} of ${total}`}
      </span>
    </div>
  );
}
