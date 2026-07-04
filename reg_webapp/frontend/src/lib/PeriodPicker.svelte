<script lang="ts">
import PeriodWindowSlider from "./PeriodWindowSlider.svelte";
import {
  type Coverage,
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
  vintageYear = undefined,
  onsubmit,
  onclear,
}: {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** The global project window (#614), or null = none set. */
  window?: StudyWindow | null;
  /** The subject's data-availability span, or null = unknown. */
  coverage?: Coverage | null;
  /** The catalog vintage year (#631), used as the open-ended coverage ceiling. */
  vintageYear?: number;
  /** Emitted with the chosen wire value on submit. */
  onsubmit: (period: string) => void;
  /** Emitted when the clear button is pressed (drop `?period`). */
  onclear: () => void;
} = $props();

// A sensible earliest register year — the slider's floor when neither the
// window nor coverage reaches further back.
const SLIDER_FLOOR_YEAR = 1960;

// The slider's open-ended ceiling (#631). Undefined only before `/api/context`
// resolves; fall back to wall-clock so a pre-context leaf still renders.
const ceilingYear = $derived(vintageYear ?? new Date().getFullYear());

/** The year window the slider treats as the active selection: a
 * year-representable `?period` wins; else the project window; else null. */
const activeYearSelection = $derived<StudyWindow | null>(
  yearWindowFromWire(period) ?? window,
);

/** The active `?period` wire when it is set but NOT year-representable. */
const subAnnualPeriod = $derived<string | null>(
  period !== null && !yearWindowRepresentable(period) ? period : null,
);

/** Bounds that fit what is drawn: project window, active selection, and coverage. */
const sliderBounds = $derived.by(() => {
  const years: number[] = [];
  for (const w of [window, activeYearSelection]) {
    if (w) {
      years.push(w.from, w.to);
    }
  }
  if (coverage) {
    if (coverage.from !== null) {
      years.push(coverage.from);
    }
    years.push(coverage.to ?? ceilingYear);
  }
  const max = years.length > 0 ? Math.max(...years) : ceilingYear;
  const min = Math.min(SLIDER_FLOOR_YEAR, ...years, max);
  return { min, max };
});

/** Thumb seed precedence (#671): explicit year `?period` > window∩coverage >
 * coverage > window > full bounds. */
const seededSelection = $derived<StudyWindow>(
  yearWindowFromWire(period) ??
    intersectCoverageWindow(coverage, window, sliderBounds.min, ceilingYear),
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
const userChosen = $derived(
  yearWindowFromWire(period) !== null || sliderWire !== null,
);

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
  coverage !== null &&
    (coverage.from ?? sliderBounds.min) <= (coverage.to ?? ceilingYear),
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
  if (window !== null) {
    onsubmit(yearWindowToWire(window));
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
        {window}
        {coverage}
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
