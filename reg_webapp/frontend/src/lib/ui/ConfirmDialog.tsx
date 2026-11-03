import { type ReactNode, useId } from "react";
import { Dialog, Heading, Modal, ModalOverlay } from "react-aria-components";
import styles from "./ConfirmDialog.module.css";

interface Props {
  isOpen: boolean;
  /** Escape routes here with `false`; the caller's own buttons answer the
   * question themselves. */
  onOpenChange: (isOpen: boolean) => void;
  /** The question, as a heading. */
  title: string;
  /** What answering it costs. */
  children: ReactNode;
  /** The answers, right-aligned: Cancel first, the destructive one last. */
  actions: ReactNode;
}

/** The shared confirmation (DESIGN.md → Components → Dialog): RAC `Modal` +
 * `Dialog` as an `alertdialog`, over the scrim. A click on the scrim does not
 * answer it (`isDismissable` stays off); Escape and the caller's Cancel are the
 * ways out. RAC traps focus and returns it to whatever opened the dialog. */
export function ConfirmDialog({
  isOpen,
  onOpenChange,
  title,
  children,
  actions,
}: Props) {
  const description = useId();
  return (
    <ModalOverlay
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      className={({ isEntering, isExiting }) =>
        `${styles.scrim} ${isEntering ? "motion-fade-in" : ""} ${isExiting ? "motion-fade-out" : ""}`
      }
    >
      <Modal
        className={({ isEntering, isExiting }) =>
          `${styles.modal} ${isEntering ? "motion-dialog-in" : ""} ${isExiting ? "motion-dialog-out" : ""}`
        }
      >
        <Dialog
          role="alertdialog"
          aria-describedby={description}
          className={styles.dialog}
          // RAC hides the page behind the modal from assistive tech but neither
          // says so on the dialog nor forwards `aria-modal`: state it here.
          render={(props) => (
            <section {...props} role="alertdialog" aria-modal="true" />
          )}
        >
          <Heading slot="title" className={styles.title}>
            {title}
          </Heading>
          <div id={description} className={styles.body}>
            {children}
          </div>
          <div className={styles.actions}>{actions}</div>
        </Dialog>
      </Modal>
    </ModalOverlay>
  );
}
