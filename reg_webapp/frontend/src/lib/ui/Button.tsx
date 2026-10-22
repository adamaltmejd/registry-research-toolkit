import type { ReactNode } from "react";
import { Button as AriaButton } from "react-aria-components";
import styles from "./Button.module.css";
import type { ButtonVariant } from "./types";

type Props = {
  variant?: ButtonVariant;
  /** Refuses the press while keeping the button focusable (`aria-disabled`,
   * never `disabled`): a natively disabled button drops keyboard focus to
   * `<body>` when it disables under the user, mid-action or after one. The
   * caller's handler still re-checks every reason it would refuse. */
  isInert?: boolean;
  /** A request this button started is in flight (`aria-busy`). Busy, not
   * inert: the button keeps working, so its handler ignores a second press. */
  isBusy?: boolean;
  /** Only where the visible words lean on the surrounding card ("Apply" on a
   * source card is "Apply period"), or repeat down a list (label-in-name: it
   * starts with the visible text). */
  "aria-label"?: string;
  /** Hover text for a pointer; never the only place a reason is given. */
  title?: string;
  children: ReactNode;
} & (
  | { type?: "button"; onPress: () => void }
  // Submits its form: the form's `onSubmit` is then the one handler, so Enter
  // in a field and a press on the button run the same code.
  | { type: "submit"; onPress?: undefined }
);

/** A button that says what happens (DESIGN.md → Buttons): `primary` is the
 * pine fill, at most one per pane; `default` is `surface` with a
 * `border-strong` hairline; `danger` is the `err` fill, for destructive actions
 * only; `ghost` has no box and a 2px `accent` underline. One size: a caller that
 * wants a smaller button wants a different layout. */
export function Button({
  variant = "default",
  type = "button",
  onPress,
  isInert = false,
  isBusy = false,
  "aria-label": ariaLabel,
  title,
  children,
}: Props) {
  return (
    <AriaButton
      type={type}
      aria-label={ariaLabel}
      className={`${styles.button} ${styles[variant]}`}
      onPress={isInert ? undefined : onPress}
      render={(props) => (
        <button
          {...props}
          title={title}
          aria-disabled={isInert || undefined}
          aria-busy={isBusy || undefined}
        />
      )}
    >
      {children}
    </AriaButton>
  );
}
