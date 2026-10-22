import styles from "./StagedAddStatus.module.css";
import { type StagedApplyOutcome, stagedDiffSummary } from "./staged_picker";
import { StatusRow } from "./ui/StatusRow";

/** The one line a catalog page says after an Apply: the refusal when the batch
 * was declined whole and nothing was authored, else the confirmation of what
 * was committed. One component, because the binding leaf, the concept group and
 * the register list all say it. Both announce as `status`: every refusal is one
 * the researcher's own next move retires. */
export function StagedAddStatus({
  outcome,
  blocked,
}: {
  /** What the last Apply committed, or null. */
  outcome: StagedApplyOutcome | null;
  /** The page's own refusal copy; it wins over a stale confirmation. */
  blocked: string | null;
}) {
  if (blocked) {
    return (
      <StatusRow tone="warn" live="status" className={styles.line}>
        {blocked}
      </StatusRow>
    );
  }
  if (!outcome) return null;
  return (
    <StatusRow
      tone="ok"
      live="status"
      className={styles.line}
      action={<a href="/project">View the project</a>}
    >
      Applied {stagedDiffSummary(outcome)}.
    </StatusRow>
  );
}
