import { type CSSProperties, type FormEvent, useId, useState } from "react";
import {
  Slider,
  SliderOutput,
  SliderThumb,
  SliderTrack,
} from "react-aria-components";
import styles from "./PeriodPicker.module.css";
import {
  type Coverage,
  clampYearPeriodWire,
  clampYearWindow,
  coverageBandEdges,
  intersectCoverageWindow,
  notDeliveredGaps,
  resolveYearEntry,
  sameYearWindow,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";
import type { ProjectStudyWindow } from "./project_data";
import { Button } from "./ui/Button";

interface Props {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** The global project window (#614), or null = none set. */
  window: ProjectStudyWindow | null;
  /** The subject's data-availability span, or null = unknown. */
  coverage: Coverage | null;
  /** The outer floor for the year slider; steward deployments may narrow it. */
  windowMinYear: number;
  /** The catalog vintage year (#631), the open-ended coverage ceiling. */
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
 * The subject page's period control (#615, Y-16): a year-grain two-thumb
 * slider over the subject's coverage, seeded from the project window, beside
 * two exact year fields. Both write one pending selection, which the slider
 * and its readout show and Apply submits — so a range whose bounds must move
 * through each other lands in one Apply, and an entry that does not parse,
 * crosses, or falls outside the selectable years is refused with the reason
 * instead of being clamped into another range.
 *
 * A non-year `?period` (a term, a list, `_default`) is valid URL state this UI
 * does not author: it reads as text with Clear and is never rewritten.
 *
 * Nothing here remounts when the applied `?period` changes (Y-65): the
 * pending state re-arms by value, so the keyboard focus of whoever pressed
 * Enter stays put.
 */
export function PeriodPicker({
  period,
  window,
  coverage,
  windowMinYear,
  vintageYear,
  enforcePeriodBounds,
  onSubmit,
  onClear,
}: Props) {
  const uid = useId();
  const ceiling = vintageYear;
  const boundedPeriod = enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, ceiling)
    : period;

  // Coverage clipped to hard steward bounds only: the global 1960 floor is a
  // fallback, so real pre-1960 coverage still widens the track.
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

  // Bounds that fit what is drawn: the window, the selection and coverage.
  const years: number[] = [];
  for (const w of [boundedWindow, activeYearSelection]) {
    if (w) years.push(w.from, w.to);
  }
  if (boundedCoverage) {
    if (boundedCoverage.from !== null) years.push(boundedCoverage.from);
    years.push(boundedCoverage.to ?? ceiling);
  }
  const max = years.length > 0 ? Math.max(...years) : ceiling;
  const min = Math.min(windowMinYear, ...years, max);

  // Seed (#671): explicit year `?period` > window∩coverage > coverage > window.
  const seeded =
    boundedPeriodWindow ??
    intersectCoverageWindow(boundedCoverage, boundedWindow, min, ceiling);
  const coverageBand = coverageBandEdges(boundedCoverage, min, max, ceiling);
  // The one legal span behind every authoring path: the thumbs' clamp, the
  // fields' refusal and the re-arm.
  const selectableYears = coverageBand ?? { from: min, to: max };

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
    // A valid entry is the pending selection at once, so the slider shows what
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
      if (
        activeYearSelection === null &&
        window === null &&
        coverageBand === null
      ) {
        return;
      }
    }
    onSubmit(yearWindowToWire(selection));
  }

  return (
    // `period-picker` names the card for the flows driver.
    <form
      className={`period-picker ${styles.card}`}
      onSubmit={apply}
      aria-labelledby={labelId}
    >
      <span id={labelId} className="micro-label">
        Period
      </span>
      <div className={styles.row}>
        <PeriodWindowSlider
          min={min}
          max={max}
          selection={selection}
          window={boundedWindow}
          coverage={boundedCoverage}
          vintageYear={ceiling}
          subAnnualPeriod={subAnnualPeriod}
          userChosen={userChosen}
          onChange={(next) =>
            // The thumbs are now the pending selection; the fields mirror it
            // again and drop any refusal.
            update({ pending: next, entry: null, committed: false })
          }
          onReset={() =>
            boundedWindow !== null
              ? onSubmit(yearWindowToWire(boundedWindow))
              : onClear()
          }
        />
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
          <Button type="submit" size="sm" aria-label="Apply period">
            Apply
          </Button>
          {period !== null && (
            <Button size="sm" onPress={onClear}>
              Clear
            </Button>
          )}
        </div>
      </div>
      {/* Always rendered, so a refusal lands in a live region that already
          existed. Empty, it holds no line box. */}
      <output
        id={problemId}
        className={`problem ${refusal ? "refused" : ""} ${styles.problem}`}
      >
        {refusal && (
          <>
            <span aria-hidden="true">✕</span> {refusal.problem}
          </>
        )}
      </output>
    </form>
  );
}

interface SliderProps {
  /** The track's bounds (inclusive years), computed by the picker. */
  min: number;
  max: number;
  /** The selection the thumbs show. */
  selection: ProjectStudyWindow;
  window: ProjectStudyWindow | null;
  coverage: Coverage | null;
  /** The ceiling an open-ended coverage end projects to (#631). */
  vintageYear: number;
  /** A non-year active `?period`: the slider then shows the window's
   * projection, and says so instead of claiming it is selected. */
  subAnnualPeriod: string | null;
  /** The user chose a year window (an explicit year `?period` or a move): gates
   * the deviation note, so the untouched coverage-clamped seed stays quiet. */
  userChosen: boolean;
  onChange: (next: ProjectStudyWindow) => void;
  onReset: () => void;
}

// Below this width (% of the track) the coverage band gets a dot to find it by.
const MIN_COVERAGE_CUE_PCT = 3;

/**
 * The availability-aware slider (#615/#671): RAC `Slider` with two thumbs over
 * a track that draws the subject's coverage up front — the span outside it is
 * a hatched, non-selectable backdrop, and a selection reaching past it (only an
 * explicit out-of-coverage `?period` can) draws its not-delivered gap.
 *
 * A year is a cell, not a point: the decoration draws [year, year+1) so a
 * one-year selection has width. User moves are clamped to coverage in
 * `onChange`; the thumbs keep RAC's own bounds (the track and the other
 * thumb), and the readout beside them names the coverage in words.
 */
function PeriodWindowSlider({
  min,
  max,
  selection,
  window: projectWindow,
  coverage,
  vintageYear,
  subAnnualPeriod,
  userChosen,
  onChange,
  onReset,
}: SliderProps) {
  const { from, to } = selection;
  const span = max - min || 1;
  const leftPct = (year: number) => ((year - min) / (span + 1)) * 100;
  const widthPct = (a: number, b: number) => ((b - a + 1) / (span + 1)) * 100;
  const place = (a: number, b: number): CSSProperties => ({
    left: `${leftPct(a)}%`,
    width: `${widthPct(a, b)}%`,
  });

  // An open end ("still delivered") projects to the vintage: the catalog
  // knows delivery only up to it. The readout keeps the raw ellipsis.
  const effective =
    coverage === null
      ? null
      : { from: coverage.from, to: coverage.to ?? vintageYear };
  const band = coverageBandEdges(effective, min, max);
  const lo = band?.from ?? min;
  const hi = band?.to ?? max;

  const unavailable: [number, number][] = [];
  if (band !== null && subAnnualPeriod === null) {
    if (band.from > min) unavailable.push([min, band.from - 1]);
    if (band.to < max) unavailable.push([band.to + 1, max]);
  }
  const gaps = notDeliveredGaps(selection, band);
  const coverageWidth = band ? widthPct(band.from, band.to) : 0;
  const cueLeft =
    band && coverageWidth < MIN_COVERAGE_CUE_PCT
      ? leftPct(band.from) + coverageWidth / 2
      : null;

  // The default seed narrowed to window∩coverage is the data's choice, not
  // the user's; a seed snapped outside a disjoint window is worth reporting.
  const seedWithinWindow =
    projectWindow !== null &&
    from >= projectWindow.from &&
    to <= projectWindow.to;
  const userDeviation =
    projectWindow !== null &&
    subAnnualPeriod === null &&
    !sameYearWindow(selection, projectWindow) &&
    (userChosen || !seedWithinWindow);
  const hasGaps = gaps.length > 0;
  // Without a window the gap is an FYI, not a deviation from anything.
  const soft = hasGaps && projectWindow === null;
  let availabilityNote = "";
  if (hasGaps && band !== null) {
    const before = from < band.from;
    const after = to > band.to;
    availabilityNote =
      before && after
        ? `Not delivered before ${band.from} or after ${band.to}`
        : before
          ? `Not delivered before ${band.from}`
          : `Not delivered after ${band.to}`;
  }
  const coverageThrough =
    band !== null && coverage !== null && coverage.to === null
      ? (effective?.to ?? null)
      : null;

  return (
    <Slider
      className={styles.slider}
      aria-label="Period window (years)"
      minValue={min}
      maxValue={max}
      value={[from, to]}
      formatOptions={{ useGrouping: false }}
      onChange={(next) => {
        const [a = from, b = to] = next;
        // The moved thumb stays inside the delivered span; RAC keeps the
        // order. Only the thumb that moved is clamped, and never past the
        // other end: a selection seeded outside the band must not drag or
        // swap its other end.
        const clamp = (y: number) => Math.min(hi, Math.max(lo, y));
        const moved = {
          from: a === from ? a : Math.min(clamp(a), b),
          to: b === to ? b : Math.max(clamp(b), a),
        };
        if (!sameYearWindow(moved, selection)) onChange(moved);
      }}
    >
      <div className={styles.readoutRow}>
        <SliderOutput className={styles.readout}>
          {({ state }) =>
            `${state.getThumbValueLabel(0)}–${state.getThumbValueLabel(1)}`
          }
        </SliderOutput>
        {coverage && (
          <span className={`muted ${styles.coverage}`}>
            data {coverage.from ?? "…"}–{coverage.to ?? "…"}
          </span>
        )}
        {coverageThrough !== null && (
          <span className={`muted ${styles.coverage}`}>
            coverage through {coverageThrough}
          </span>
        )}
      </div>
      <SliderTrack className={styles.track}>
        {unavailable.map(([a, b]) => (
          <div
            key={`${a}:${b}`}
            className={styles.unavailable}
            style={place(a, b)}
          />
        ))}
        {band && (
          <div className={styles.band} style={place(band.from, band.to)} />
        )}
        {cueLeft !== null && (
          <div
            className={styles.cue}
            style={{ "--cue-left": `${cueLeft}%` } as CSSProperties}
          />
        )}
        <div className={styles.fill} style={place(from, to)} />
        {gaps.map((gap) => (
          <div
            key={`${gap.from}:${gap.to}`}
            className={soft ? `${styles.gap} ${styles.soft}` : styles.gap}
            style={place(gap.from, gap.to)}
            title={`Not delivered ${gap.from}–${gap.to}`}
          />
        ))}
        <SliderThumb
          index={0}
          aria-label="From year"
          className={styles.thumb}
        />
        <SliderThumb index={1} aria-label="To year" className={styles.thumb} />
      </SliderTrack>
      {subAnnualPeriod !== null ? (
        <output className={`${styles.note} ${styles.warn}`}>
          <span aria-hidden="true">▲</span> Active period{" "}
          <code>{subAnnualPeriod}</code> isn't year-grain — the slider shows the
          project-window default.
        </output>
      ) : (
        userDeviation &&
        projectWindow && (
          // Plain text (Y-100): the window is an authoring seed, and a
          // selection covering more is an ordinary choice.
          <p className={styles.note}>
            Deviates from project window ({projectWindow.from}–
            {projectWindow.to})
            <Button size="sm" onPress={onReset}>
              Reset to project window
            </Button>
          </p>
        )
      )}
      {hasGaps && (
        <output
          className={
            soft ? `${styles.note} muted` : `${styles.note} ${styles.warn}`
          }
        >
          {!soft && <span aria-hidden="true">▲ </span>}
          {availabilityNote}
        </output>
      )}
    </Slider>
  );
}
