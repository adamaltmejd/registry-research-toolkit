import type { CSSProperties, ReactNode } from "react";
import styles from "./VariableRow.module.css";

/** Inclusive years. */
export type YearSpan = readonly [number, number];

interface Props {
  name: string;
  /** The variable's (or group's) own page; the name links there. */
  href: string;
  /** The machine identifier under the name, in mono. */
  fqid: string;
  /** Something of this row is in the open project: the dot and the wash. */
  inProject: boolean;
  /** The years delivered, merged and ordered; empty draws a bare track. */
  coverage: readonly YearSpan[];
  /** The pane's axis, shared by every row. */
  axis: YearSpan;
  /** The study window, as a block on the track; null draws none. */
  window: YearSpan | null;
  /** The delivery columns under the name: chips and their ticks. */
  children?: ReactNode;
}

/**
 * One variable in a register's list (DESIGN.md → Time → VariableRow): the
 * in-project dot, the name in the text face, the FQID in `mono` muted, and a
 * thin coverage bar on the pane's axis with the study window as an
 * `accent-bg` block, so a variable that does not cover the window shows before
 * it is added. Never a table row: the list has no column headers.
 */
export function VariableRow({
  name,
  href,
  fqid,
  inProject,
  coverage,
  axis,
  window,
  children,
}: Props) {
  // A year is a cell [year, year + 1), so a one-year span has width.
  const [first, last] = axis;
  const cells = Math.max(1, last - first + 1);
  const place = ([from, to]: YearSpan): CSSProperties | null => {
    const a = Math.max(from, first);
    const b = Math.min(to, last);
    if (b < a) return null;
    return {
      left: `${((a - first) / cells) * 100}%`,
      width: `${((b - a + 1) / cells) * 100}%`,
    };
  };
  const windowAt = window ? place(window) : null;
  const years =
    coverage.length === 0
      ? "no dated delivery"
      : coverage
          .map(([from, to]) => (from === to ? `${from}` : `${from}–${to}`))
          .join(", ");
  return (
    <li className={styles.row} data-in-project={inProject || undefined}>
      <span className={styles.dot} aria-hidden="true" />
      <div className={styles.head}>
        <a className={styles.name} href={href}>
          {name}
        </a>
        {inProject && <span className="visually-hidden">, in the project</span>}
        <code className={styles.fqid}>{fqid}</code>
      </div>
      {/* Decorative: the years are read out beside it for assistive tech. */}
      <span className={styles.bar} title={`Delivered ${years}`}>
        <span className={styles.track} aria-hidden="true">
          {windowAt && <span className={styles.window} style={windowAt} />}
          {coverage.map((span) => {
            const at = place(span);
            return at ? (
              <span
                key={`${span[0]}:${span[1]}`}
                className={styles.segment}
                style={at}
              />
            ) : null;
          })}
        </span>
        <span className="visually-hidden">Delivered {years}</span>
      </span>
      {children && <div className={styles.columns}>{children}</div>}
    </li>
  );
}

/** The list a pane's VariableRows sit in. */
export function VariableRowList({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <ul className={styles.list} aria-label={label}>
      {children}
    </ul>
  );
}
