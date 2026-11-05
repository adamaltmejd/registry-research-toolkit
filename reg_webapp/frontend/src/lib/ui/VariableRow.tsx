import type { ReactNode } from "react";
import type { YearRun } from "../study_window";
import styles from "./VariableRow.module.css";
import { YearAxis, type YearSpan, YearTrack } from "./YearTrack";

interface Props {
  name: string;
  /** The row's own page; the name links there. */
  href: string;
  /** Something of this row is in the open project: the dot and the wash. */
  inProject: boolean;
  /** The years delivered, merged and ordered; empty draws a bare track. */
  coverage: readonly YearRun[];
  /** The pane's axis, shared by every row. */
  axis: YearSpan;
  /** The study window, as a block on the track; null draws none. */
  window: YearSpan | null;
  /** A muted suffix after the name: where the row lives, in a list that spans
   * registers (a classification's versions). */
  detail?: ReactNode;
}

/**
 * One row of a pane's list (DESIGN.md → Time → VariableRow): a variable in a
 * register's list, or a register in a provider's. The in-project dot, the
 * name in the text face (no FQID: the page already says where the rows live),
 * and a thin `YearTrack` on the pane's axis with the study window as an
 * `accent-bg` block, so a row that does not cover the window shows before it
 * is added. Never a table row: the list's one header is its axis.
 */
export function VariableRow({
  name,
  href,
  inProject,
  coverage,
  axis,
  window,
  detail,
}: Props) {
  return (
    <li className={styles.row} data-in-project={inProject || undefined}>
      <InProjectDot inProject={inProject} />
      <div className={styles.head}>
        <a className={`link ${styles.name}`} href={href}>
          {name}
        </a>
        {inProject && <span className="visually-hidden">, in the project</span>}
        {detail && <span className={styles.detail}>{detail}</span>}
      </div>
      <span className={styles.bar}>
        <YearTrack
          coverage={coverage}
          axis={axis}
          window={window}
          inProject={inProject}
        />
      </span>
    </li>
  );
}

/** The list a pane's VariableRows sit in, under the axis their bars share:
 * its first and last year over the bars' column (DESIGN.md → Time). */
export function VariableRowList({
  label,
  axis,
  children,
}: {
  label: string;
  axis: YearSpan;
  children: ReactNode;
}) {
  return (
    <div className={styles.rows}>
      <div className={styles.axisRow}>
        <YearAxis axis={axis} className={styles.bar} />
      </div>
      <ul className={styles.list} aria-label={label}>
        {children}
      </ul>
    </div>
  );
}

/** The shared read-only project mark on register and group member rows. */
function InProjectDot({ inProject }: { inProject: boolean }) {
  return (
    <span
      className={styles.dot}
      data-in-project={inProject || undefined}
      aria-hidden="true"
    />
  );
}
