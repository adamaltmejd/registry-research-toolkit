import {
  type CSSProperties,
  type FormEvent,
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
  intersectCoverageWindow,
  resolveYearEntry,
  sameYearWindow,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";
import type { ProjectStudyWindow } from "./project_data";
import styles from "./Timeline.module.css";
import { Button } from "./ui/Button";
import { StatusRow } from "./ui/StatusRow";

type Years = ProjectStudyWindow;

/** Inclusive runs of consecutive years. */
function runs(years: readonly number[]): Years[] {
  const out: Years[] = [];
  for (const y of years) {
    const last = out.at(-1);
    if (last && y === last.to + 1) last.to = y;
    else out.push({ from: y, to: y });
  }
  return out;
}

/** The years of `window` outside `covered`: what the window asks for that the
 * subject does not deliver. */
function uncovered(window: Years, covered: Years | null): Years[] {
  if (covered === null) return [];
  const out: Years[] = [];
  if (window.from < covered.from) {
    out.push({ from: window.from, to: Math.min(window.to, covered.from - 1) });
  }
  if (window.to > covered.to) {
    out.push({ from: Math.max(window.from, covered.to + 1), to: window.to });
  }
  return out.filter((r) => r.from <= r.to);
}

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

  const delivered =
    covered === null
      ? []
      : years === null
        ? [covered]
        : runs(years.filter((y) => y >= covered.from && y <= covered.to));
  const gaps: Years[] = [];
  for (let i = 1; i < delivered.length; i += 1) {
    gaps.push({ from: delivered[i - 1].to + 1, to: delivered[i].from - 1 });
  }
  const ticks =
    years === null
      ? []
      : delivered.flatMap((r) =>
          Array.from({ length: r.to - r.from }, (_, i) => r.from + i + 1),
        );
  const outside = band ? uncovered(band, covered) : [];

  const labels: { year: number; at: number; window: boolean }[] = [];
  const add = (year: number, at: number, window: boolean) => {
    if (labels.every((l) => Math.abs(l.at - at) >= MIN_GAP_PCT)) {
      labels.push({ year, at, window });
    }
  };
  if (band) {
    add(band.from, pct(band.from), true);
    add(band.to, pct(band.to + 1), true);
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
            data-edge={l.at <= 0 ? "start" : l.at >= 100 ? "end" : undefined}
            style={{ left: `${l.at}%` }}
          >
            {l.year}
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
  /** The band's years outside coverage: an informational note, not a warning. */
  outside: readonly Years[];
}

/** The `label` header: what the drawing is against, then the covered years. */
function Head({ id, label, covered, outside }: HeadProps) {
  return (
    <div id={id} className={styles.head}>
      <span className="micro-label">{label}</span>
      <span className={styles.headFacts}>
        {outside.length > 0 && (
          <span className={styles.info}>
            <span aria-hidden="true">i</span> Not covered{" "}
            {outside.map(span).join(", ")}
          </span>
        )}
        {covered && (
          <span className={styles.covered}>
            <span className="visually-hidden">Covered </span>
            {span(covered)}
          </span>
        )}
      </span>
    </div>
  );
}

/** The legend, in `label` text: only the marks the drawing shows. */
function Legend({
  band,
  gaps,
  outside,
}: {
  band: string | null;
  gaps: boolean;
  outside: boolean;
}) {
  return (
    <ul className={styles.legend} aria-hidden="true">
      {band && <li className={styles.legendBand}>{band}</li>}
      <li className={styles.legendCoverage}>Delivered years</li>
      {gaps && <li className={styles.legendGap}>Gap inside coverage</li>}
      {outside && <li className={styles.legendOutside}>Window not covered</li>}
    </ul>
  );
}

function hasGaps(covered: Years | null, years: readonly number[] | null) {
  if (covered === null || years === null) return false;
  const inside = years.filter((y) => y >= covered.from && y <= covered.to);
  return inside.length < covered.to - covered.from + 1;
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
 * in `info-bg` with an "i" note in the header. For register and group panes;
 * the variable page's selectable one is `PeriodTimeline`.
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
  const outside = band ? uncovered(band, covered) : [];
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
        gaps={hasGaps(covered, years)}
        outside={outside.length > 0}
      />
    </section>
  );
}

interface PeriodTimelineProps {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** The global project window (#614), or null = none set. */
  window: ProjectStudyWindow | null;
  /** The subject's data-availability span, or null = unknown. */
  coverage: Coverage | null;
  /** The subject's delivered years, or null to draw coverage solid. */
  years: readonly number[] | null;
  /** The catalog's first year: the axis floor. */
  windowMinYear: number;
  /** The catalog vintage year (#631): the axis ceiling and the open-ended
   * coverage end. */
  vintageYear: number;
  /** Treat the outer bounds as hard steward limits, not global fallbacks. */
  enforcePeriodBounds: boolean;
  /** Called with the chosen wire value on Apply. */
  onSubmit: (period: string) => void;
  /** Called by Clear (drop `?period`). */
  onClear: () => void;
}

/** What the pending selection and the year fields were armed against, as a
 * value: a consumer may rebuild an input (coverage, the window) as a fresh
 * object without moving anything, which must not wipe a year half-typed. */
function armKey(parts: {
  period: string | null;
  active: ProjectStudyWindow | null;
  ceiling: number;
  seed: ProjectStudyWindow;
  selectable: ProjectStudyWindow;
}): string {
  const w = (x: ProjectStudyWindow | null) => (x ? `${x.from}..${x.to}` : "-");
  return [
    parts.period ?? "",
    w(parts.active),
    parts.ceiling,
    w(parts.seed),
    w(parts.selectable),
  ].join("|");
}

interface Local {
  /** The `armKey` this state belongs to; a different key resets it. */
  key: string;
  /** The pending selection, from a thumb move or a valid typed entry. */
  pending: ProjectStudyWindow | null;
  /** The year fields' raw text, or null while they mirror the selection. */
  entry: { from: string; to: string } | null;
  /** The entry was committed (blur, Enter or Apply): only then does a refusal
   * show, so a half-typed year announces nothing. */
  committed: boolean;
}

/**
 * The variable page's Timeline (#615, Y-16; DESIGN.md → Time): the band is
 * the source period, a selection whose two thumbs stop at the variable's
 * availability, beside two exact year fields. Both write one pending selection,
 * which the band shows and Apply submits — so a range whose bounds must move
 * through each other lands in one Apply, and an entry that does not parse,
 * crosses, or falls outside the selectable years is refused with the reason
 * instead of being clamped into another range.
 *
 * A source period that differs from the project window says so in a `warn`
 * row with Reset; a window wider than the variable's availability is an `info`
 * note in the header and an `info-bg` band, not a warning.
 *
 * A non-year `?period` (a term, a list, `_default`) is valid URL state this UI
 * does not author: it reads as text with Clear and is never rewritten.
 *
 * Nothing here remounts when the applied `?period` changes (Y-65): the
 * pending state re-arms by value, so the keyboard focus of whoever pressed
 * Enter stays put.
 */
export function PeriodTimeline({
  period,
  window,
  coverage,
  years,
  windowMinYear,
  vintageYear,
  enforcePeriodBounds,
  onSubmit,
  onClear,
}: PeriodTimelineProps) {
  const uid = useId();
  const ceiling = vintageYear;
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
  // A year-representable `?period` wins; else the project window.
  const activeYearSelection = boundedPeriodWindow ?? boundedWindow;
  const subAnnualPeriod =
    boundedPeriod !== null && !yearWindowRepresentable(boundedPeriod)
      ? boundedPeriod
      : null;

  // The axis: the catalog's years, widened only to what is drawn on it.
  const drawn: number[] = [];
  for (const w of [boundedWindow, activeYearSelection]) {
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
  // The one legal span behind every authoring path: the thumbs' clamp, the
  // fields' refusal and the re-arm. The selection never leaves availability.
  const selectableYears = covered ?? { from: min, to: max };

  const key = armKey({
    period,
    active: activeYearSelection,
    ceiling,
    seed: seeded,
    selectable: selectableYears,
  });
  const [stored, setLocal] = useState<Local>({
    key,
    pending: null,
    entry: null,
    committed: false,
  });
  // Re-armed when the URL, the window, coverage or the ceiling actually move
  // the seed or the selectable years — then a stale drag or typed range would
  // submit something the screen no longer shows.
  const local: Local =
    stored.key === key
      ? stored
      : { key, pending: null, entry: null, committed: false };
  if (stored.key !== key) setLocal(local);
  const update = (patch: Partial<Omit<Local, "key">>) =>
    setLocal((current) => ({
      ...(current.key === key
        ? current
        : { key, pending: null, entry: null, committed: false }),
      ...patch,
    }));

  // The selection shown and submitted: the pending one, else the seed.
  const selection = clampYearWindow(local.pending ?? seeded, min, max);
  const userChosen = periodWindow !== null || local.pending !== null;

  const entryFrom = local.entry?.from ?? String(selection.from);
  const entryTo = local.entry?.to ?? String(selection.to);
  const resolve = (entry: { from: string; to: string } | null) =>
    resolveYearEntry(entry?.from ?? null, entry?.to ?? null, {
      selectableYears,
      yearRule: `four-digit year, like ${selectableYears.from}.`,
    });
  const resolution = resolve(local.entry);
  const problem =
    resolution !== null && "problem" in resolution ? resolution : null;
  const refusal = local.committed ? problem : null;
  const problemId = `${uid}-problem`;
  const labelId = `${uid}-label`;

  function editEntry(side: "from" | "to", value: string): void {
    const entry =
      side === "from"
        ? { from: value, to: entryTo }
        : { from: entryFrom, to: value };
    const next = resolve(entry);
    // A valid entry is the pending selection at once, so the band shows what
    // Apply would submit; a refused one leaves it alone.
    update(
      next !== null && "years" in next
        ? { entry, pending: next.years }
        : { entry },
    );
  }

  function apply(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    if (problem !== null) {
      update({ committed: true });
      return;
    }
    if (local.pending === null) {
      // A sub-annual period is not rewritten by accepting the slider's
      // projection of it; nothing at all to narrow to is a no-op.
      if (subAnnualPeriod !== null) return;
      if (activeYearSelection === null && window === null && covered === null) {
        return;
      }
    }
    onSubmit(yearWindowToWire(selection));
  }

  // The band is the active window: the selection once there is one to show
  // (a source period, a move, or the project window it starts from).
  const showBand =
    subAnnualPeriod === null && (userChosen || boundedWindow !== null);
  const band = showBand ? selection : null;
  const outside = band ? uncovered(band, covered) : [];
  const sourceKind = userChosen && !sameYearWindow(selection, boundedWindow);
  // The default seed narrowed to window∩coverage is the data's choice, not
  // the user's; a seed snapped outside a disjoint window is worth reporting.
  const seedWithinWindow =
    boundedWindow !== null &&
    selection.from >= boundedWindow.from &&
    selection.to <= boundedWindow.to;
  const differs =
    boundedWindow !== null &&
    subAnnualPeriod === null &&
    !sameYearWindow(selection, boundedWindow) &&
    (userChosen || !seedWithinWindow);

  const lo = selectableYears.from;
  const hi = selectableYears.to;
  const cells = max - min + 1;
  // RAC places a thumb at (value − min) / (max − min) of its track; inset by
  // half a year cell each side, each thumb sits on the centre of its year.
  const trackInset = `${(0.5 / cells) * 100}%`;

  return (
    // `period-picker` names the card for the flows driver.
    <form
      className={`period-picker ${styles.timeline} ${styles.form}`}
      onSubmit={apply}
      aria-labelledby={labelId}
    >
      <Head
        id={labelId}
        label={headLabel(band !== null, sourceKind)}
        covered={covered}
        outside={outside}
      />
      <Drawing
        axis={{ from: min, to: max }}
        band={band}
        covered={covered}
        years={years}
        overlay={
          <Slider
            className={styles.slider}
            aria-label="Period window (years)"
            minValue={min}
            maxValue={max}
            value={[selection.from, selection.to]}
            formatOptions={{ useGrouping: false }}
            style={{ left: trackInset, right: trackInset }}
            onChange={(next) => {
              const [a = selection.from, b = selection.to] = next;
              // The moved thumb stays inside availability; RAC keeps the
              // order. Only the thumb that moved is clamped, and never past
              // the other end: a selection seeded outside it must not drag or
              // swap its other end.
              const clamp = (y: number) => Math.min(hi, Math.max(lo, y));
              const moved = {
                from: a === selection.from ? a : Math.min(clamp(a), b),
                to: b === selection.to ? b : Math.max(clamp(b), a),
              };
              if (!sameYearWindow(moved, selection)) {
                // The thumbs are now the pending selection; the fields
                // mirror it again and drop any refusal.
                update({ pending: moved, entry: null, committed: false });
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
        }
      />
      <Legend
        band={
          band === null ? null : sourceKind ? "Source period" : "Study window"
        }
        gaps={hasGaps(covered, years)}
        outside={outside.length > 0}
      />
      <div className={styles.controls}>
        <fieldset className={styles.exact}>
          <legend className="visually-hidden">Exact years</legend>
          <label className="micro-label" htmlFor={`${uid}-from`}>
            From
          </label>
          <input
            id={`${uid}-from`}
            className="year"
            type="text"
            inputMode="numeric"
            autoComplete="off"
            value={entryFrom}
            aria-invalid={refusal?.at.from === true}
            aria-describedby={refusal ? problemId : undefined}
            onChange={(event) => editEntry("from", event.currentTarget.value)}
            onBlur={() => local.entry && update({ committed: true })}
          />
          <label className="micro-label" htmlFor={`${uid}-to`}>
            To
          </label>
          <input
            id={`${uid}-to`}
            className="year"
            type="text"
            inputMode="numeric"
            autoComplete="off"
            value={entryTo}
            aria-invalid={refusal?.at.to === true}
            aria-describedby={refusal ? problemId : undefined}
            onChange={(event) => editEntry("to", event.currentTarget.value)}
            onBlur={() => local.entry && update({ committed: true })}
          />
        </fieldset>
        <div className={styles.actions}>
          <Button type="submit" aria-label="Apply period">
            Apply
          </Button>
          {period !== null && <Button onPress={onClear}>Clear</Button>}
        </div>
      </div>
      {/* One standing live region for the period's warning, so a row that
          appears after an Apply is announced. */}
      <StatusRow
        live="status"
        tone={
          subAnnualPeriod !== null || (differs && boundedWindow) ? "warn" : null
        }
        action={
          subAnnualPeriod === null &&
          differs &&
          boundedWindow && (
            <Button
              variant="ghost"
              onPress={() => onSubmit(yearWindowToWire(boundedWindow))}
            >
              Reset
            </Button>
          )
        }
      >
        {subAnnualPeriod !== null ? (
          <>
            The active period <code>{subAnnualPeriod}</code> is not a span of
            whole years, so the Timeline shows the project window.
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
      {/* Always rendered, so a refusal lands in a live region that already
          existed. Empty, it holds no line box. */}
      <StatusRow id={problemId} live="status" tone={refusal ? "error" : null}>
        {refusal?.problem}
      </StatusRow>
    </form>
  );
}
