import type { ReactNode } from "react";
import { Checkbox } from "react-aria-components";
import styles from "./FilterChip.module.css";

interface Props {
  isSelected: boolean;
  /** The caller owns the filter set. */
  onChange: (isSelected: boolean) => void;
  /** The pill it filters by (a register's table, `EntityPill`). */
  children: ReactNode;
}

/** A lens over a list (DESIGN.md → FilterChip): a real checkbox whose face is
 * the pill it filters by, neutral at rest and pine when on, at the pill's
 * size (F1, #2399). Toggling narrows what is shown, never the project. */
export function FilterChip({ isSelected, onChange, children }: Props) {
  return (
    <Checkbox
      className={styles.chip}
      isSelected={isSelected}
      onChange={onChange}
    >
      {children}
    </Checkbox>
  );
}
