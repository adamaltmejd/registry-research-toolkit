import type { ReactNode } from "react";
import { Button as AriaButton } from "react-aria-components";
import styles from "./Button.module.css";
import type { ButtonSize } from "./types";

interface Props {
  variant?: "primary" | "default";
  size?: ButtonSize;
  onPress: () => void;
  /** Refuses the press while keeping the button focusable (`aria-disabled`,
   * never `disabled`): a natively disabled button drops keyboard focus to
   * `<body>` when it disables under the user, mid-action or after one. The
   * caller's handler still re-checks every reason it would refuse. */
  isInert?: boolean;
  children: ReactNode;
}

/** A button that says what happens (DESIGN.md → Buttons): `primary` is the
 * pine fill, at most one per pane; `default` is `surface` with a
 * `border-strong` hairline. A ghost button is the `ghost-button` utility on a
 * RAC `Button`. */
export function Button({
  variant = "default",
  size = "md",
  onPress,
  isInert = false,
  children,
}: Props) {
  return (
    <AriaButton
      className={`${styles.button} ${styles[variant]} ${styles[size]}`}
      onPress={isInert ? undefined : onPress}
      render={(props) => (
        <button {...props} aria-disabled={isInert || undefined} />
      )}
    >
      {children}
    </AriaButton>
  );
}
