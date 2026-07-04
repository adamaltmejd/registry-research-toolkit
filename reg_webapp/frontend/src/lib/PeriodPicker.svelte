<script lang="ts">
import PeriodWindowSlider from "./PeriodWindowSlider.svelte";
import {
  type Coverage,
  clampYearPeriodWire,
  clampYearWindow,
  intersectCoverageWindow,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";
import type { StudyWindow } from "./project_data";

// WINDOW-ANCHORED period selection (#615): a year-grain availability slider
// seeded from the project window and the subject's data-coverage track. The
// server-side `?period` wire grammar still accepts richer values (terms,
// `_default`, comma lists), but this UI no longer authors them. A non-year active
// value renders as read-only text with Clear and is never silently rewritten.
let {
  period,
  window = null,
  coverage = null,
  windowMinYear = 1960,
  vintageYear = undefined,
  enforcePeriodBounds = false,
  onsubmit,
  onclear,
}: {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** The global project window (#614), or null = none set. */
  window?: StudyWindow | null;
  /** The subject's data-availability span, or null = unknown. */
  coverage?: Coverage | null;
  /** The outer floor for the year slider; steward deployments may narrow it. */
  windowMinYear?: number;
  /** The catalog vintage year (#631), used as the open-ended coverage ceiling. */
  vintageYear?: number;
  /** Treat the outer bounds as hard steward limits rather than global fallbacks. */
  enforcePeriodBounds?: boolean;
  /** Emitted with the chosen wire value on submit. */
  onsubmit: (period: string) => void;
  /** Emitted when the clear button is pressed (drop `?period`). */
  onclear: () => void;
} = $props();

// The slider's open-ended ceiling (#631). Undefined only before `/api/context`
// resolves; fall back to wall-clock so a pre-context leaf still renders.
const ceilingYear = $derived(vintageYear ?? new Date().getFullYear());
const boundedPeriod = $derived(
  enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, ceilingYear)
    : period,
);

/** Subject coverage clipped to hard steward bounds only. The global 1960 floor is
 * a fallback for unknown/empty cases, so real pre-1960 coverage must still widen
 * the track in non-steward deployments. */
const boundedCoverage = $derived.by<Coverage | null>(() => {
  if (coverage === null) {
    return null;
  }
  const bounded = {
    from:
      coverage.from === null
        ? null
        : enforcePeriodBounds
          ? Math.max(coverage.from, windowMinYear)
          : coverage.from,
    to:
      coverage.to === null
        ? null
        : enforcePeriodBounds
          ? Math.min(coverage.to, ceilingYear)
          : coverage.to,
  };
  if (enforcePeriodBounds) {
    const boundedFrom = bounded.from ?? windowMinYear;
    const boundedTo = bounded.to ?? ceilingYear;
    if (boundedFrom > boundedTo) {
      return null;
    }
  }
  return bounded;
});

const periodWindow = $derived<StudyWindow | null>(
  yearWindowFromWire(boundedPeriod),
);

const boundedWindow = $derived<StudyWindow | null>(
  window === null || !enforcePeriodBounds
    ? window
    : clampYearWindow(window, windowMinYear, ceilingYear),
);

const boundedPeriodWindow = $derived<StudyWindow | null>(
  periodWindow === null || !enforcePeriodBounds
    ? periodWindow
    : clampYearWindow(periodWindow, windowMinYear, ceilingYear),
);

/** The year window the slider treats as the active selection: a
 * year-representable `?period` wins; else the project window; else null. */
const activeYearSelection = $derived<StudyWindow | null>(
  boundedPeriodWindow ?? boundedWindow,
);

/** The active `?period` wire when it is set but NOT year-representable. */
const subAnnualPeriod = $derived<string | null>(
  boundedPeriod !== null && !yearWindowRepresentable(boundedPeriod)
    ? boundedPeriod
    : null,
);

/** Bounds that fit what is drawn: project window, active selection, and coverage. */
const sliderBounds = $derived.by(() => {
  const years: number[] = [];
  for (const w of [boundedWindow, activeYearSelection]) {
    if (w) {
      years.push(w.from, w.to);
    }
  }
  if (boundedCoverage) {
    if (boundedCoverage.from !== null) {
      years.push(boundedCoverage.from);
    }
    years.push(boundedCoverage.to ?? ceilingYear);
  }
  const max = years.length > 0 ? Math.max(...years) : ceilingYear;
  const min = Math.min(windowMinYear, ...years, max);
  return { min, max };
});

/** Thumb seed precedence (#671): explicit year `?period` > window∩coverage >
 * coverage > window > full bounds. */
const seededSelection = $derived<StudyWindow>(
  boundedPeriodWindow ??
    intersectCoverageWindow(
      boundedCoverage,
      boundedWindow,
      sliderBounds.min,
      ceilingYear,
    ),
);

/** The clamped slider selection shown to the user. */
const sliderSelection = $derived<StudyWindow>(
  clampYearWindow(seededSelection, sliderBounds.min, sliderBounds.max),
);

// The slider's live selection (null until the user moves a thumb). Re-armed on
// URL/window/coverage/ceiling re-seed by the effect below.
let sliderWire = $state<string | null>(null);

/** Whether the slider shows a real user-meaningful selection. */
const hasSliderSelection = $derived(
  activeYearSelection !== null || sliderWire !== null,
);

/** Whether the user has actually chosen a year-window value distinct from the
 * project window. The untouched coverage-clamped default seed is not a user
 * deviation. */
const userChosen = $derived(periodWindow !== null || sliderWire !== null);

$effect(() => {
  // Re-arm the slider buffer on URL, window, coverage, or ceiling changes. This
  // prevents a stale dragged value from surviving a re-seed and being submitted
  // instead of the newly displayed selection.
  void period;
  void activeYearSelection;
  void ceilingYear;
  void seededSelection;
  sliderWire = null;
});

/** Whether `coverage` yields a usable non-inverted seed. */
const effectiveCoverageUsable = $derived(
  boundedCoverage !== null &&
    (boundedCoverage.from ?? sliderBounds.min) <=
      (boundedCoverage.to ?? ceilingYear),
);

function applySlider(): void {
  let wire: string | null = sliderWire;
  // A token/list/default active period is valid URL state but not represented by
  // the year slider. Rendering it must not rewrite the URL just because the user
  // accepts the fallback slider projection; only an actual thumb move replaces it
  // with a year-window wire.
  if (wire === null && subAnnualPeriod !== null) {
    return;
  }
  if (
    wire === null &&
    (activeYearSelection !== null || window !== null || effectiveCoverageUsable)
  ) {
    wire = yearWindowToWire(seededSelection);
  }
  if (wire !== null) {
    onsubmit(wire);
  }
}

function resetToWindow(): void {
  if (boundedWindow !== null) {
    onsubmit(yearWindowToWire(boundedWindow));
  } else {
    onclear();
  }
}

function submit(event: SubmitEvent): void {
  event.preventDefault();
  applySlider();
}
</script>

<form class="period-picker" onsubmit={submit}>
  <div class="head">
    <span class="title micro-label" id="period-label">Period</span>
  </div>

  {#key period}
    <div class="slider-row">
      <PeriodWindowSlider
        min={sliderBounds.min}
        max={sliderBounds.max}
        selection={sliderSelection}
        window={boundedWindow}
        coverage={boundedCoverage}
        vintageYear={ceilingYear}
        {subAnnualPeriod}
        hasSelection={hasSliderSelection}
        {userChosen}
        onchange={(next) => (sliderWire = yearWindowToWire(next))}
        onreset={() => resetToWindow()}
      />
      <div class="actions">
        <button
          type="button"
          class="apply"
          aria-label="Apply period"
          onclick={() => applySlider()}
        >
          Apply
        </button>
        {#if period !== null}
          <button type="button" class="clear" onclick={() => onclear()}>
            Clear
          </button>
        {/if}
      </div>
    </div>
  {/key}
</form>

<style>
  .period-picker {
    margin: 1.25rem 0;
    padding: var(--space-3) 0.9rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
  }
  .head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: 0.45rem;
  }
  .slider-row {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: var(--space-3);
  }
  .actions {
    display: flex;
    gap: var(--space-2);
  }
  button.apply,
  button.clear {
    padding: 0.4rem 0.9rem;
    border: 1px solid var(--accent);
    border-radius: var(--radius-sm);
    background: var(--accent);
    color: var(--accent-fg);
    font: inherit;
    cursor: pointer;
  }
  button.clear {
    background: var(--surface);
    color: var(--accent);
  }
  button.apply:hover,
  button.clear:hover {
    filter: brightness(0.95);
  }
</style>
