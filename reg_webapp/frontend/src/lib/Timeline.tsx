import {
  type ComponentProps,
  type CSSProperties,
  type ReactNode,
  useId,
  useState,
} from "react";
import { Slider, SliderThumb, SliderTrack } from "react-aria-components";
import {
  type Coverage,
  clampYearPeriodWire,
  clampYearWindow,
  coverageBandEdges,
  coverageMarks,
  intersectCoverageWindow,
  periodWindowRelation,
  sameYearWindow,
  uncoveredYears,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";
import type { ProjectStudyWindow } from "./project_data";
import { RepresentationPicker } from "./RepresentationPicker";
import { studyWindowInterval } from "./reg_core";
import styles from "./Timeline.module.css";
import { Button } from "./ui/Button";
import { StatusRow } from "./ui/StatusRow";

type Years = ProjectStudyWindow;

const span = (r: Years) =>
  r.from === r.to ? `${r.from}` : `${r.from}–${r.to}`;

/** Axis labels: the two ends and round years between them, never closer than
 * `MIN_GAP_PCT` to a label already placed (the window's years win). */
const MIN_GAP_PCT = 12;
/** Round-year steps for the axis labels, smallest first. */
const AXIS_STEPS = [5, 10, 20, 25, 50];

interface DrawingProps {
  /** The axis, inclusive years: the catalog's first to last. */
  axis: Years;
  /** The active window: the source period when one is set, else the project
   * window; null draws no band. */
  band: Years | null;
  /** The subject's coverage edges, open ends resolved; null when unknown. */
  covered: Years | null;
  /** The delivered years, or null to draw coverage as one bar. */
  years: readonly number[] | null;
  /** The selection's slider, laid over the drawing (variable page). */
  overlay?: ReactNode;
}

/** The drawing: the band, coverage as an ink bar cut by edition ticks, a gap
 * inside coverage as a hatch, the uncovered part of the band in `info-bg`,
 * then the axis. A year is a cell, [year, year + 1). Hidden from assistive
 * technology: the header and the legend say the same in words. */
function Drawing({ axis, band, covered, years, overlay }: DrawingProps) {
  const cells = axis.to - axis.from + 1;
  const pct = (year: number) => ((year - axis.from) / cells) * 100;
  const place = (r: Years): CSSProperties => ({
    left: `${pct(r.from)}%`,
    width: `${pct(r.to + 1) - pct(r.from)}%`,
  });

  const { delivered, gaps, ticks } = coverageMarks(covered, years);
  const outside = band ? uncoveredYears(band, covered) : [];

  const labels: {
    year: number;
    at: number;
    window: boolean;
    text?: string;
    edge?: "start" | "end";
    /** The run the label's text covers, when wider than one year's label. */
    reach?: [number, number];
  }[] = [];
  const add = (year: number, at: number, window: boolean) => {
    // The window's own bounds always show; only the other ticks give way.
    // A year is labelled once (the axis end can be the window end).
    if (labels.some((l) => l.year === year)) return;
    const clear = (l: (typeof labels)[number]) =>
      l.reach
        ? at <= l.reach[0] - MIN_GAP_PCT || at >= l.reach[1] + MIN_GAP_PCT
        : Math.abs(l.at - at) >= MIN_GAP_PCT;
    if (window || labels.every(clear)) {
      labels.push({ year, at, window });
    }
  };
  if (band) {
    const from = pct(band.from);
    const to = pct(band.to + 1);
    if (band.from !== band.to && to - from < MIN_GAP_PCT) {
      // Bounds too close to stand apart read as one "2018–2020" label, so
      // the two years never run together ("20182020"). It is anchored at the
      // band edge nearer the drawing's middle and runs toward the middle, so
      // it never runs off either end; axis years keep a label gap clear of its
      // whole run, taken as one gap wide (about the label's width at 375 px).
      const late = from + to > 100;
      const at = late ? to : from;
      labels.push({
        year: band.from,
        at,
        window: true,
        text: span(band),
        edge: late ? "end" : "start",
        reach: late ? [at - MIN_GAP_PCT, at] : [at, at + MIN_GAP_PCT],
      });
    } else {
      add(band.from, from, true);
      add(band.to, to, true);
    }
  }
  add(axis.from, 0, false);
  add(axis.to, 100, false);
  const step = AXIS_STEPS.find((s) => cells / s <= 6) ?? 100;
  for (let y = Math.ceil(axis.from / step) * step; y < axis.to; y += step) {
    add(y, pct(y), false);
  }

  return (
    <div className={styles.drawing}>
      <div className={styles.plot} aria-hidden="true">
        {outside.map((r) => (
          <div
            key={`u${r.from}`}
            className={styles.uncovered}
            style={place(r)}
          />
        ))}
        {band && (
          <div className={styles.band} style={place(band)}>
            {outside.map((r) => (
              // The band's wash stops where coverage stops; its bounds stay.
              <div
                key={`c${r.from}`}
                className={styles.bandCut}
                style={{
                  left: `${((r.from - band.from) / (band.to - band.from + 1)) * 100}%`,
                  width: `${((r.to - r.from + 1) / (band.to - band.from + 1)) * 100}%`,
                }}
              />
            ))}
          </div>
        )}
        {delivered.map((r) => (
          <div
            key={`d${r.from}`}
            className={styles.coverage}
            style={place(r)}
          />
        ))}
        {gaps.map((r) => (
          <div key={`g${r.from}`} className={styles.gap} style={place(r)} />
        ))}
        {ticks.map((y) => (
          <div
            key={`t${y}`}
            className={styles.tick}
            style={{ left: `${pct(y)}%` }}
          />
        ))}
      </div>
      {overlay}
      <div className={styles.axis} aria-hidden="true">
        {labels.map((l) => (
          <span
            key={`${l.window ? "w" : "a"}${l.year}`}
            className={
              l.window ? `${styles.year} ${styles.windowYear}` : styles.year
            }
            data-edge={
              l.edge ?? (l.at <= 0 ? "start" : l.at >= 100 ? "end" : undefined)
            }
            style={{ left: `${l.at}%` }}
          >
            {l.text ?? l.year}
          </span>
        ))}
      </div>
    </div>
  );
}

/** What the drawing is against. */
function headLabel(hasBand: boolean, source: boolean): string {
  return !hasBand
    ? "Coverage"
    : source
      ? "Coverage against your source period"
      : "Coverage against your study window";
}

interface HeadProps {
  id?: string;
  label: string;
  covered: Years | null;
  /** The band's years outside coverage. The drawing shows them in `info-bg`;
   * the header names them for a screen reader only (#1833). */
  outside: readonly Years[];
  /** Clear, while a source period is applied (variable page). */
  action?: ReactNode;
}

/** The `label` header: what the drawing is against, then the covered years. */
function Head({ id, label, covered, outside, action }: HeadProps) {
  return (
    <div className={styles.head}>
      <span id={id} className="micro-label">
        {label}
      </span>
      {action}
      {covered && (
        <span className={styles.covered}>
          <span className="visually-hidden">Covered </span>
          {span(covered)}
          {outside.length > 0 && (
            <span className="visually-hidden">
              . Not covered {outside.map(span).join(", ")}
            </span>
          )}
        </span>
      )}
    </div>
  );
}

/** The legend, in `label` text: only the marks the drawing shows. */
function Legend({ band, gaps }: { band: string | null; gaps: boolean }) {
  return (
    <ul className={styles.legend} aria-hidden="true">
      {band && <li className={styles.legendBand}>{band}</li>}
      <li className={styles.legendCoverage}>Delivered years</li>
      {gaps && <li className={styles.legendGap}>Gap inside coverage</li>}
    </ul>
  );
}

interface TimelineProps {
  /** The catalog's first and last year (DESIGN.md → Time). */
  axis: Years;
  /** The active window, or null with no project window and no source period:
   * then there is no band. */
  window: Years | null;
  /** Says what the band is: the project's study window or a source period. */
  windowKind?: "study" | "source";
  /** The subject's coverage (`coverageFromStates`, a union), or null. */
  coverage: Coverage | null;
  /** Delivered years (`deliveredYears`), or null to draw coverage solid. */
  years: readonly number[] | null;
  /** The ceiling an open coverage end projects to: the catalog vintage. */
  vintageYear: number;
}

/**
 * The read-only Timeline (DESIGN.md → Time): one axis over the catalog's
 * years, the active window as an `accent-bg` band, coverage as an ink bar with
 * edition ticks, a gap inside coverage hatched, and the window's uncovered part
 * in `info-bg`. For register and group panes; the variable page's is
 * `VariableTimeline`.
 */
export function Timeline({
  axis,
  window,
  windowKind = "study",
  coverage,
  years,
  vintageYear,
}: TimelineProps) {
  const covered = coverageBandEdges(coverage, axis.from, axis.to, vintageYear);
  const band = window ? clampYearWindow(window, axis.from, axis.to) : null;
  const outside = band ? uncoveredYears(band, covered) : [];
  const bandName =
    band === null
      ? null
      : windowKind === "source"
        ? "Source period"
        : "Study window";
  return (
    <section className={styles.timeline} aria-label="Coverage">
      <Head
        label={headLabel(band !== null, windowKind === "source")}
        covered={covered}
        outside={outside}
      />
      <Drawing axis={axis} band={band} covered={covered} years={years} />
      <Legend
        band={bandName}
        gaps={coverageMarks(covered, years).gaps.length > 0}
      />
    </section>
  );
}

type PickerProps = Omit<
  ComponentProps<typeof RepresentationPicker>,
  "window" | "timeline" | "catalogYears"
>;

interface VariableTimelineProps extends PickerProps {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** The global project window (#614), or null = none set. */
  window: ProjectStudyWindow | null;
  /** The window the columns are read against: a valid `?period`, else the
   * project window (`pickerWindowYears`). */
  pickerWindow: [number, number] | null;
  /** The subject's data-availability span, or null = unknown. */
  coverage: Coverage | null;
  /** The subject's delivered years, or null to draw coverage solid. */
  years: readonly number[] | null;
  /** The catalog's first year: the axis floor. */
  windowMinYear: number;
  /** The catalog's last year: the axis ceiling and the open-ended coverage
   * end. */
  windowMaxYear: number;
  /** Treat the outer bounds as hard steward limits, not global fallbacks. */
  enforcePeriodBounds: boolean;
  /** Called with the chosen wire value when a thumb comes to rest. */
  onSubmit: (period: string) => void;
  /** Called by Clear (drop `?period`). */
  onClear: () => void;
}

/**
 * The variable page's one Timeline (#615, #1833; DESIGN.md → Time): coverage,
 * the variable's columns and the variables a succession reaches, drawn as one
 * LaneGraph on one axis. The band is the source period, a selection whose two
 * thumbs over the coverage row stop at the variable's availability. The thumbs
 * move live while dragged (the band and the columns' in/out follow them) and
 * the period is applied when they come to rest (on release, or on each
 * committed key press), so a drag is one URL change; there are no year fields
 * and no Apply (maintainer ruling 2026-10-22).
 *
 * A source period that differs from the project window inside the variable's
 * availability says so in a `warn` row with Reset; a window wider than the
 * availability is the `info-bg` part of the band, not a warning, and a period
 * narrowed only to that availability is no deviation.
 *
 * A non-year `?period` (a term, a list, `_default`) is valid URL state this UI
 * does not author: it reads as text with Clear and is never rewritten.
 *
 * Nothing here remounts when the applied `?period` changes (Y-65), so the
 * keyboard focus on a thumb stays put while each key press applies.
 */
export function VariableTimeline({
  period,
  window,
  pickerWindow,
  coverage,
  years,
  windowMinYear,
  windowMaxYear,
  enforcePeriodBounds,
  onSubmit,
  onClear,
  ...picker
}: VariableTimelineProps) {
  const labelId = useId();
  // The thumbs mid-drag; null when they rest on the applied selection.
  const [dragging, setDragging] = useState<ProjectStudyWindow | null>(null);
  const ceiling = windowMaxYear;
  const boundedPeriod = enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, ceiling)
    : period;

  // Coverage clipped to hard steward bounds only: a fallback floor must not
  // hide real coverage before it.
  let boundedCoverage: Coverage | null = null;
  if (coverage !== null) {
    const from =
      coverage.from === null
        ? null
        : enforcePeriodBounds
          ? Math.max(coverage.from, windowMinYear)
          : coverage.from;
    const to =
      coverage.to === null
        ? null
        : enforcePeriodBounds
          ? Math.min(coverage.to, ceiling)
          : coverage.to;
    boundedCoverage =
      enforcePeriodBounds && (from ?? windowMinYear) > (to ?? ceiling)
        ? null
        : { from, to };
  }

  const periodWindow = yearWindowFromWire(boundedPeriod);
  const boundedWindow =
    window === null || !enforcePeriodBounds
      ? window
      : clampYearWindow(window, windowMinYear, ceiling);
  const boundedPeriodWindow =
    periodWindow === null || !enforcePeriodBounds
      ? periodWindow
      : clampYearWindow(periodWindow, windowMinYear, ceiling);
  const subAnnualPeriod =
    boundedPeriod !== null && !yearWindowRepresentable(boundedPeriod)
      ? boundedPeriod
      : null;

  // The axis: the catalog's years, widened only to what is drawn on it (which
  // the steward's bounds already clamp, so a steward axis is its span). The
  // lanes may widen it further to a column's years; the slider takes the axis
  // the graph draws.
  const drawn: number[] = [];
  for (const w of [boundedWindow, boundedPeriodWindow]) {
    if (w) drawn.push(w.from, w.to);
  }
  if (boundedCoverage) {
    if (boundedCoverage.from !== null) drawn.push(boundedCoverage.from);
    drawn.push(boundedCoverage.to ?? ceiling);
  }
  const max = Math.max(ceiling, ...drawn);
  const min = Math.min(windowMinYear, ...drawn, max);

  // Seed (#671): explicit year `?period` > window∩coverage > coverage > window.
  const seeded =
    boundedPeriodWindow ??
    intersectCoverageWindow(boundedCoverage, boundedWindow, min, ceiling);
  const covered = coverageBandEdges(boundedCoverage, min, max, ceiling);
  // The one legal span for the thumbs: the selection never leaves
  // availability.
  const lo = covered?.from ?? min;
  const hi = covered?.to ?? max;

  // The selection shown: the thumbs mid-drag, else the seed.
  const selection = clampYearWindow(dragging ?? seeded, min, max);
  const userChosen = periodWindow !== null || dragging !== null;

  /** The thumbs' next position: only the thumb that moved is clamped into
   * availability, and never past the other end, so a selection seeded
   * outside it does not drag or swap its other end. */
  function moved(next: number[]): ProjectStudyWindow {
    const [a = selection.from, b = selection.to] = next;
    const clamp = (y: number) => Math.min(hi, Math.max(lo, y));
    return {
      from: a === selection.from ? a : Math.min(clamp(a), b),
      to: b === selection.to ? b : Math.max(clamp(b), a),
    };
  }

  // Periods are compared inside the variable's availability
  // (`periodWindowRelation`, the rule /project judges sources by): years it has
  // no data for are the data's narrowing, not the user's, and show as the
  // band's `info-bg` part, never the ▲ row. So the default seed
  // (window∩coverage), that seed applied, and the window itself all match the
  // window. A window wholly outside availability has no part to match: any
  // selection other than the window itself differs.
  const coveredDays = covered && studyWindowInterval(covered);
  const matchesWindow =
    boundedWindow !== null &&
    periodWindowRelation(
      selection,
      boundedWindow,
      coveredDays ? [coveredDays] : null,
    ) === "same";
  const sourceKind = userChosen && !matchesWindow;
  const differs =
    boundedWindow !== null && subAnnualPeriod === null && !matchesWindow;
  const windowHeld =
    boundedWindow !== null &&
    (covered === null ||
      (boundedWindow.to >= covered.from && boundedWindow.from <= covered.to));
  // What Reset writes: the window as this variable can hold it. Clipped only
  // at FINITE coverage edges: a "still delivered" variable keeps the window's
  // years past the vintage, which it will get.
  const resetWindow =
    !windowHeld || boundedWindow === null
      ? boundedWindow
      : {
          from: Math.max(
            boundedWindow.from,
            boundedCoverage?.from ?? boundedWindow.from,
          ),
          to: Math.min(
            boundedWindow.to,
            boundedCoverage?.to ?? boundedWindow.to,
          ),
        };
  // The band is the active window: a source period that differs, else the
  // project window (the thumbs may sit on the narrower seed), so the uncovered
  // years show for as long as the period only follows availability.
  const showBand =
    subAnnualPeriod === null && (userChosen || boundedWindow !== null);
  const band = showBand ? (sourceKind ? selection : boundedWindow) : null;
  const outside = band ? uncoveredYears(band, covered) : [];

  function slider(axis: [number, number]): ReactNode {
    // RAC places a thumb at (value − min) / (max − min) of its track; inset by
    // half a year cell each side, each thumb sits on the centre of its year.
    const trackInset = `${(0.5 / (axis[1] - axis[0] + 1)) * 100}%`;
    return (
      <Slider
        className={styles.slider}
        aria-label="Period window (years)"
        minValue={axis[0]}
        maxValue={axis[1]}
        value={[selection.from, selection.to]}
        formatOptions={{ useGrouping: false }}
        style={{ left: trackInset, right: trackInset }}
        onChange={(next) => {
          const at = moved(next);
          if (!sameYearWindow(at, selection)) setDragging(at);
        }}
        onChangeEnd={(next) => {
          const at = moved(next);
          setDragging(null);
          // A rest where the period already is applies nothing: a click
          // on a thumb must not turn the seed into a URL period.
          if (
            !sameYearWindow(at, boundedPeriodWindow ?? seeded) ||
            (periodWindow === null && dragging !== null)
          ) {
            onSubmit(yearWindowToWire(at));
          }
        }}
      >
        <SliderTrack className={styles.track}>
          <SliderThumb
            index={0}
            aria-label="From year"
            className={styles.thumb}
          />
          <SliderThumb
            index={1}
            aria-label="To year"
            className={styles.thumb}
          />
        </SliderTrack>
      </Slider>
    );
  }

  return (
    // `period-picker` names the card for the flows driver; the picker inside
    // lays it out.
    <section className="period-picker" aria-labelledby={labelId}>
      <RepresentationPicker
        {...picker}
        window={dragging ? [dragging.from, dragging.to] : pickerWindow}
        catalogYears={[min, max]}
        timeline={{
          head: (
            <Head
              id={labelId}
              label={headLabel(band !== null, sourceKind)}
              covered={covered}
              outside={outside}
              action={
                period !== null && (
                  <Button variant="ghost" onPress={onClear}>
                    Clear period
                  </Button>
                )
              }
            />
          ),
          band: band && [band.from, band.to],
          coverage: {
            covered,
            years,
            outside,
            bandName:
              band === null
                ? null
                : sourceKind
                  ? "Source period"
                  : "Study window",
          },
          overlay: subAnnualPeriod === null ? slider : undefined,
          foot: (
            <>
              {/* One standing live region for the period's warning, so a row
                  that appears after a move is announced. */}
              <StatusRow
                live="status"
                tone={
                  subAnnualPeriod !== null || (differs && boundedWindow)
                    ? "warn"
                    : null
                }
                action={
                  subAnnualPeriod === null &&
                  differs &&
                  resetWindow && (
                    <Button
                      variant="ghost"
                      onPress={() => onSubmit(yearWindowToWire(resetWindow))}
                    >
                      Reset
                    </Button>
                  )
                }
              >
                {subAnnualPeriod !== null ? (
                  <>
                    The active period <code>{subAnnualPeriod}</code> is not a
                    span of whole years, so the Timeline shows the project
                    window.
                  </>
                ) : (
                  differs &&
                  boundedWindow && (
                    <>
                      Differs from the project window ({boundedWindow.from}–
                      {boundedWindow.to}).
                    </>
                  )
                )}
              </StatusRow>
            </>
          ),
        }}
      />
    </section>
  );
}
