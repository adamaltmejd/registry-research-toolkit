import type { ReactNode } from "react";
import { Button as AriaButton } from "react-aria-components";
import styles from "./Button.module.css";
import type { ButtonSize } from "./types";

type Props = {
  variant?: "primary" | "default";
  size?: ButtonSize;
  /** Refuses the press while keeping the button focusable (`aria-disabled`,
   * never `disabled`): a natively disabled button drops keyboard focus to
   * `<body>` when it disables under the user, mid-action or after one. The
   * caller's handler still re-checks every reason it would refuse. */
  isInert?: boolean;
  /** A request this button started is in flight (`aria-busy`). Busy, not
   * inert: the button keeps working, so its handler ignores a second press. */
  isBusy?: boolean;
  /** Only where the visible words lean on the surrounding card ("Apply" in the
   * period card is "Apply period"). */
  "aria-label"?: string;
  children: ReactNode;
} & (
  | { type?: "button"; onPress: () => void }
  // Submits its form: the form's `onSubmit` is then the one handler, so Enter
  // in a field and a press on the button run the same code.
  | { type: "submit"; onPress?: undefined }
);

/** A button that says what happens (DESIGN.md → Buttons): `primary` is the
 * pine fill, at most one per pane; `default` is `surface` with a
 * `border-strong` hairline. A ghost button is the `ghost-button` utility on a
 * RAC `Button`. */
export function Button({
  variant = "default",
  size = "md",
  type = "button",
  onPress,
  isInert = false,
  isBusy = false,
  "aria-label": ariaLabel,
  children,
}: Props) {
  return (
    <AriaButton
      type={type}
      aria-label={ariaLabel}
      className={`${styles.button} ${styles[variant]} ${styles[size]}`}
      onPress={isInert ? undefined : onPress}
      render={(props) => (
        <button
          {...props}
          aria-disabled={isInert || undefined}
          aria-busy={isBusy || undefined}
        />
      )}
    >
      {children}
    </AriaButton>
  );
}
