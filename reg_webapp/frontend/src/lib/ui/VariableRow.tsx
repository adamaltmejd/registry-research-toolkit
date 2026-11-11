import { type ReactNode, useId } from "react";
import type { YearRun } from "../study_window";
import { type Entity, EntityPill } from "./EntityPill";
import styles from "./VariableRow.module.css";
import { YearAxis, type YearSpan, YearTrack } from "./YearTrack";

/** A count on a row, as its entity's pill: "4 tables". */
export interface RowCount {
  entity: Entity;
  count: number;
  /** The word after the number, in the count's number ("1 table"). */
  word: string;
}

interface Props {
  /** The variable's name, or it with a filter's match marked. */
  name: ReactNode;
  /** The row's own page; the name links there. */
  href: string;
  /** Something of this row is in the open project: the dot and the wash. */
  inProject: boolean;
  /** The years delivered, merged and ordered; empty draws a bare track. */
  coverage: readonly YearRun[];
  /** The delivered years in words (`segmentYears`). */
  years: string;
  /** The pane's axis, shared by every row. */
  axis: YearSpan;
  /** The study window, as a block on the track; null draws none. */
  window: YearSpan | null;
  /** Under the name, cut at about four lines (a provider's register row). */
  description?: string | null;
  /** One per heading in the list's `counts`, in its order. */
  counts?: readonly RowCount[];
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
  years,
  axis,
  window,
  description,
  counts,
}: Props) {
  return (
    <li className={styles.row} data-in-project={inProject || undefined}>
      <InProjectDot inProject={inProject} />
      <div className={styles.head}>
        <a className={`link ${styles.name}`} href={href}>
          {name}
        </a>
        {inProject && <span className="visually-hidden">, in the project</span>}
        {description && <p className={styles.description}>{description}</p>}
      </div>
      {counts && (
        <div className={styles.counts}>
          {counts.map(({ entity, count, word }) => (
            <span key={entity} className={styles.count}>
              <EntityPill
                entity={entity}
                name={
                  <>
                    {count}
                    <span className={styles.countWord}> {word}</span>
                  </>
                }
              />
            </span>
          ))}
        </div>
      )}
      <span className={styles.bar}>
        <YearTrack
          coverage={coverage}
          years={years}
          axis={axis}
          window={window}
          inProject={inProject}
        />
      </span>
    </li>
  );
}

/** The list a pane's VariableRows sit in, under the axis their bars share:
 * its first and last year over the bars' column, beside `label` as the
 * section's heading over the names (DESIGN.md → Time; #2316). */
export function VariableRowList({
  label,
  axis,
  counts,
  children,
}: {
  label: string;
  axis: YearSpan;
  /** A column heading per count the rows carry, between the names and the
   * bars on wide screens. Decorative: each count's pill names its word. */
  counts?: readonly string[];
  children: ReactNode;
}) {
  const labelId = useId();
  return (
    <div className={`${styles.rows} ${counts ? styles.withCounts : ""}`}>
      <div className={styles.axisRow}>
        <h2 id={labelId} className={`micro-label ${styles.label}`}>
          {label}
        </h2>
        {counts && (
          <div
            className={`${styles.counts} ${styles.countHeadings}`}
            aria-hidden="true"
          >
            {counts.map((heading) => (
              <span key={heading} className="micro-label">
                {heading}
              </span>
            ))}
          </div>
        )}
        <YearAxis axis={axis} className={styles.bar} />
      </div>
      <ul className={styles.list} aria-labelledby={labelId}>
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
