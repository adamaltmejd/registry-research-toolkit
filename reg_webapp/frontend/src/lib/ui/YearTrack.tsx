import type { CSSProperties } from "react";
import type { YearRun } from "../study_window";
import styles from "./YearTrack.module.css";

/** Inclusive years. */
export type YearSpan = readonly [number, number];

/**
 * The thin year bar of a list row (DESIGN.md → Time → VariableRow): a hairline
 * track on the pane's axis, the study window as an `accent-bg` block, and the
 * delivered years as ink segments (accent when `inProject`). Every row of a
 * list and its `YearAxis` share one `axis`, so the bars line up under it.
 */
export function YearTrack({
  coverage,
  years: delivered,
  axis,
  window,
  inProject = false,
}: {
  /** The years delivered, merged and ordered; empty draws a bare track. An
   * open end is drawn to the axis but said as "from" or "through". */
  coverage: readonly YearRun[];
  /** The delivered years in words (`segmentYears`): the cells cannot say a
   * school year. */
  years: string;
  /** The pane's axis, shared by every row. */
  axis: YearSpan;
  /** The study window, as a block on the track; null draws none. */
  window: YearSpan | null;
  inProject?: boolean;
}) {
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
      ? "No dated delivery"
      : delivered
        ? `Delivered ${delivered.replace(/\b(From|Through)\b/g, (bound) => bound.toLowerCase())}`
        : "Delivered in every year";
  return (
    // Decorative: the years are read out beside it for assistive tech.
    <span
      className={styles.bar}
      title={years}
      data-in-project={inProject || undefined}
    >
      <span className={styles.track} aria-hidden="true">
        {windowAt && <span className={styles.window} style={windowAt} />}
        {coverage.map(({ from, to }) => {
          const at = place([from, to]);
          return at ? (
            <span key={`${from}:${to}`} className={styles.segment} style={at} />
          ) : null;
        })}
      </span>
      <span className="visually-hidden">{years}.</span>
    </span>
  );
}

/**
 * The axis over a list's `YearTrack`s: its first year at the track's start and
 * its last at the track's end. Decorative: each row states its own years.
 */
export function YearAxis({
  axis,
  className,
}: {
  axis: YearSpan;
  className?: string;
}) {
  return (
    <span className={`${styles.axis} ${className ?? ""}`} aria-hidden="true">
      <span>{axis[0]}</span>
      <span>{axis[1]}</span>
    </span>
  );
}
