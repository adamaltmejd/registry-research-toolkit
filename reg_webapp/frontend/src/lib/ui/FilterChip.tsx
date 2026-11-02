import type { ReactNode } from "react";
import { Checkbox } from "react-aria-components";
import styles from "./FilterChip.module.css";

interface Props {
  isSelected: boolean;
  /** The caller owns the filter set. */
  onChange: (isSelected: boolean) => void;
  children: ReactNode;
}

/** A lens over a list (DESIGN.md → FilterChip): a real checkbox behind an
 * `sm`-radius face, `accent-bg` with an `accent` border when on. Toggling
 * narrows what is shown, never the project. */
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
