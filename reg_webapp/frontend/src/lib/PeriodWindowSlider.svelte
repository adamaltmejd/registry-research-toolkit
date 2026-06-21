<script lang="ts">
// The #615 availability-aware local period slider — the subject page's DEFAULT
// period control (#611 → Period model). A year-grain dual-thumb slider (mirrors
// the header's YearWindowSlider mechanics) over the project WINDOW, drawn on a
// track that also shows the subject's data COVERAGE: the not-delivered span
// (inside the selection but outside coverage) is GREYED, and two amber
// deviation hints fire:
//   • USER deviation — the active selection ≠ the project window → "deviates
//     from project window · reset to project window" (reset = clear `?period`,
//     which falls back to the window; the parent owns that URL write).
//   • AVAILABILITY deviation — coverage doesn't cover the active selection →
//     the greyed gap + a "not delivered before/after Y" note. SOFTENED when no
//     project window is set (browsing without a window is not a "deviation",
//     just an FYI), per the spec.
//
// Self-contained presentation (props in, callbacks out — no store import) so
// it's unit-testable in isolation; the PeriodPicker owns the wire seam (it
// shapes the slider's `{from,to}` to/from the `?period` grammar) and the URL
// write flows through BindingLeafView's onsubmit/onclear, unchanged.

import { untrack } from "svelte";
import { type Coverage, notDeliveredGaps, sameYearWindow } from "./period";
import type { StudyWindow } from "./project_data";

interface Props {
  // The slider bounds (inclusive years) — the union of the window + coverage,
  // computed by the picker so both fit on one track.
  min: number;
  max: number;
  // The active selection to seed the thumbs from (the resolved precedence
  // `?period` > window > full history, already year-shaped by the picker).
  selection: StudyWindow;
  // The project window (#614), or null = none set. Drives the user-deviation
  // hint + the "reset to project window" affordance.
  window: StudyWindow | null;
  // The subject's data-availability span (derived from embedded states), or
  // null = unknown. Sides are INDEPENDENTLY bounded — a null side is open
  // (start/end unknown). The open END ("still delivered") projects to the
  // catalog VINTAGE (`vintageYear`), not the track edge: the band stops at the
  // vintage and a selection past it gaps as "not delivered after <vintage>"
  // (#631). The open START stays open (band runs to `min`, never gaps). Drives
  // the greyed not-delivered track + the availability hint.
  coverage: Coverage | null;
  // The catalog VINTAGE year (#631) — the ceiling an OPEN-ENDED coverage end
  // projects to ("delivered as far as the catalog knows = the vintage"). It
  // bounds the coverage BAND and the availability GAP, NOT the slider bounds
  // (the picker computes those). Optional — when a caller doesn't cap (the
  // standalone tests) the open end falls back to the track edge `max`.
  vintageYear?: number;
  // The active `?period` wire when it is set but NOT year-representable (a
  // sub-annual token / segment list / `_default` / text), else null. When
  // non-null the slider's `selection` is the project-window PROJECTION, not the
  // active selection — so we suppress the (misleading) user-deviation hint and
  // show an honest cue pointing at the real value in the (already-open) "more"
  // expander, instead of presenting the window as if it were selected.
  subAnnualPeriod: string | null;
  // Emitted with the live selection as the thumbs move (the picker holds it and
  // submits the wire on Apply).
  onchange: (next: StudyWindow) => void;
  // Emitted when the user clicks "reset to project window" (the picker clears
  // `?period`, falling back to the window).
  onreset: () => void;
}
let {
  min,
  max,
  selection,
  window: projectWindow,
  coverage,
  vintageYear,
  subAnnualPeriod,
  onchange,
  onreset,
}: Props = $props();

function clamp(year: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, year));
}

// Local thumb buffer for smooth dragging; the source of truth stays the
// `selection` prop. `untrack` makes the seed a one-shot init — the $effect below
// owns re-seeding when the prop changes (a controlled-component re-sync, the
// YearWindowSlider doctrine).
let from = $state(untrack(() => clamp(selection.from, min, max)));
let to = $state(untrack(() => clamp(selection.to, min, max)));
$effect(() => {
  from = clamp(selection.from, min, max);
  to = clamp(selection.to, min, max);
});

function onFrom(event: Event): void {
  const v = clamp(
    Number((event.currentTarget as HTMLInputElement).value),
    min,
    max,
  );
  from = Math.min(v, to); // never cross past `to`
  onchange({ from, to });
}
function onTo(event: Event): void {
  const v = clamp(
    Number((event.currentTarget as HTMLInputElement).value),
    min,
    max,
  );
  to = Math.max(v, from);
  onchange({ from, to });
}

// ── Track geometry (left/width % of the [min,max] span) ──────────────────────
const span = $derived(max - min || 1);
// A YEAR is a unit, not a point: render the [year, year+1) cell so a single-year
// selection has visible width and the right edge reaches `max`'s far side.
function leftPct(year: number): number {
  return ((year - min) / (span + 1)) * 100;
}
function widthPct(fromYear: number, toYear: number): number {
  return ((toYear - fromYear + 1) / (span + 1)) * 100;
}

// The selected span fill.
const fillLeft = $derived(leftPct(from));
const fillWidth = $derived(widthPct(from, to));

// The EFFECTIVE coverage for the band + gaps: an open END ("still delivered")
// projects to the catalog VINTAGE (#631) — the catalog only knows delivery up to
// its vintage, so the band stops there and a selection past it gaps as "not
// delivered after <vintage>", instead of reading as covered to the track edge.
// Falls back to `max` when no `vintageYear` is supplied (standalone callers). The
// open START is left open (null → band runs to `min`, never gaps). The READOUT
// below keeps the RAW `coverage` so the open end still shows an ellipsis, not a
// year.
const effectiveCoverage = $derived<Coverage | null>(
  coverage === null
    ? null
    : { from: coverage.from, to: coverage.to ?? vintageYear ?? max },
);

// The coverage (available-data) band — a solid track segment; the not-delivered
// gaps draw OVER the selection fill as greyed cells. An open start extends the
// band to the track edge (`min`); the open end stops at the vintage ceiling (via
// `effectiveCoverage`).
const coverageBand = $derived.by(() => {
  if (effectiveCoverage === null) {
    return null;
  }
  const bandFrom = effectiveCoverage.from ?? min;
  const bandTo = effectiveCoverage.to ?? max;
  return { left: leftPct(bandFrom), width: widthPct(bandFrom, bandTo) };
});

// The not-delivered gaps of the ACTIVE selection (fires relative to the active
// selection, per the spec), as greyed cells over the fill — against the vintage-
// projected `effectiveCoverage`, so a selection past an open-ended coverage's
// vintage ceiling gaps too (#631). SUPPRESSED for a sub-annual `?period`: the
// shown span is then the window PROJECTION, not the real selection, so a gap
// against it is meaningless (a genuinely-covered `HT2020` would otherwise show
// unrelated "Not delivered" warnings) — the sub-annual cue already points the
// user at the real value (Codex P2).
const gaps = $derived(
  subAnnualPeriod !== null
    ? []
    : notDeliveredGaps({ from, to }, effectiveCoverage).map((g) => ({
        left: leftPct(g.from),
        width: widthPct(g.from, g.to),
        from: g.from,
        to: g.to,
      })),
);

// ── Deviation states ─────────────────────────────────────────────────────────
// USER deviation: an active selection different from the project window. Only
// meaningful WHEN a window is set (no window → nothing to deviate from) AND the
// shown span IS the active selection — for a sub-annual `?period` the span is
// the window PROJECTION, so `same==window` here is an artefact, not a real
// "no deviation"; the sub-annual cue below speaks for that case instead.
const userDeviation = $derived(
  projectWindow !== null &&
    subAnnualPeriod === null &&
    !sameYearWindow({ from, to }, projectWindow),
);

// AVAILABILITY deviation: coverage doesn't cover the active selection. SOFTENED
// (advisory, not amber) when no window is set — the spec's "softened when no
// window is set". `hasGaps` drives both the greyed track and the note; it is
// empty for a sub-annual `?period` (gaps are suppressed at the source above).
const hasGaps = $derived(gaps.length > 0);
const availabilitySoft = $derived(hasGaps && projectWindow === null);

/** The human note for a not-delivered gap relative to the EFFECTIVE coverage
 * (before / after the band, or both) — so an open-ended coverage's "after" note
 * names the VINTAGE ceiling (`effectiveCoverage.to`), not the raw open end (#631).
 * `effectiveCoverage` is non-null whenever `hasGaps`, and a gap can only fire
 * against a FINITE side (after vintage-projection the end is always finite), so a
 * `before`/`after` note implies that side is non-null. */
const availabilityNote = $derived.by(() => {
  if (!hasGaps || effectiveCoverage === null) {
    return "";
  }
  const before =
    effectiveCoverage.from !== null && from < effectiveCoverage.from;
  const after = effectiveCoverage.to !== null && to > effectiveCoverage.to;
  if (before && after) {
    return `Not delivered before ${effectiveCoverage.from} or after ${effectiveCoverage.to}`;
  }
  if (before) {
    return `Not delivered before ${effectiveCoverage.from}`;
  }
  return `Not delivered after ${effectiveCoverage.to}`;
});
</script>

<div class="period-window">
  <div class="readout-row">
    <span class="readout" aria-live="polite">{from}–{to}</span>
    {#if coverage}
      <!-- An open (null) side reads as an ellipsis, not a bound — the start/end
           is unknown / still-delivered, never a literal year. -->
      <span class="coverage-readout muted"
        >data {coverage.from ?? "…"}–{coverage.to ?? "…"}</span
      >
    {/if}
  </div>

  <div class="track" role="group" aria-label="Period window (years)">
    <!-- The coverage (available-data) band: a solid segment of the rail. -->
    {#if coverageBand}
      <div
        class="coverage"
        style="left: {coverageBand.left}%; width: {coverageBand.width}%;"
      ></div>
    {/if}
    <!-- The selected span fill. -->
    <div class="fill" style="left: {fillLeft}%; width: {fillWidth}%;"></div>
    <!-- Not-delivered gaps (inside the selection, outside coverage): greyed
         (amber-tinted unless softened) cells over the fill. -->
    {#each gaps as gap (`${gap.from}:${gap.to}`)}
      <div
        class="gap"
        class:soft={availabilitySoft}
        style="left: {gap.left}%; width: {gap.width}%;"
        title="Not delivered {gap.from}–{gap.to}"
      ></div>
    {/each}
    <!-- Two overlaid native range thumbs (real slider role + keyboard for
         free); the `from` thumb sits above so a coincident pair stays grabbable. -->
    <input
      class="thumb thumb-from"
      type="range"
      {min}
      {max}
      step="1"
      value={from}
      aria-label="From year"
      oninput={onFrom}
    />
    <input
      class="thumb thumb-to"
      type="range"
      {min}
      {max}
      step="1"
      value={to}
      aria-label="To year"
      oninput={onTo}
    />
  </div>

  {#if subAnnualPeriod !== null}
    <!-- The active `?period` is sub-annual (or otherwise not year-grain): the
         slider above shows the project-window default as a year projection, NOT
         the active value. Be honest about it and point at the real value, which
         the picker's "more" expander already opens to. -->
    <p class="deviation sub-annual" role="status">
      Active period <code>{subAnnualPeriod}</code> isn't year-grain — the slider shows
      the project-window default; edit it in More options.
    </p>
  {:else if userDeviation}
    <p class="deviation user" role="status">
      Deviates from project window ({projectWindow?.from}–{projectWindow?.to})
      <button type="button" class="reset" onclick={() => onreset()}>
        reset to project window
      </button>
    </p>
  {/if}

  {#if hasGaps}
    <!-- AMBER when a window frames the selection; advisory (muted) when softened
         (no window set) — an FYI, not a deviation from anything. -->
    <p class="deviation availability" class:soft={availabilitySoft} role="status">
      {availabilityNote}
    </p>
  {/if}
</div>

<style>
  .period-window {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    flex: 1 1 18rem;
    min-width: 0;
  }
  .readout-row {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
  }
  .readout {
    font-variant-numeric: tabular-nums;
    font-weight: 600;
  }
  .coverage-readout {
    font-size: 0.8rem;
    font-variant-numeric: tabular-nums;
  }
  .track {
    position: relative;
    width: 100%;
    height: 1.4rem;
  }
  .track::before {
    /* The base rail. */
    content: "";
    position: absolute;
    left: 0;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
    height: 4px;
    border-radius: 4px;
    background: var(--border);
  }
  /* The available-data band — a muted solid segment so the user sees where data
     actually exists relative to their selection. */
  .coverage {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 8px;
    border-radius: 4px;
    background: var(--border);
    opacity: 0.7;
  }
  .fill {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 4px;
    border-radius: 4px;
    background: var(--accent);
  }
  /* Not-delivered gap inside the selection: greyed (amber-tinted) cell. The
     `soft` variant (no window set) drops the amber for a plain grey FYI. */
  .gap {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 8px;
    border-radius: 2px;
    background: repeating-linear-gradient(
      45deg,
      #d9a441 0,
      #d9a441 3px,
      transparent 3px,
      transparent 6px
    );
    opacity: 0.75;
  }
  .gap.soft {
    background: repeating-linear-gradient(
      45deg,
      var(--muted) 0,
      var(--muted) 3px,
      transparent 3px,
      transparent 6px
    );
    opacity: 0.55;
  }
  /* Thumbs: both range inputs stack on the same track; only the thumbs show
     (native rail hidden so the rail/fill/coverage show through). */
  .thumb {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    margin: 0;
    background: none;
    pointer-events: none;
    -webkit-appearance: none;
    appearance: none;
  }
  .thumb-from {
    z-index: 2;
  }
  .thumb::-webkit-slider-runnable-track {
    background: none;
  }
  .thumb::-moz-range-track {
    background: none;
  }
  .thumb::-webkit-slider-thumb {
    -webkit-appearance: none;
    appearance: none;
    pointer-events: auto;
    width: 0.95rem;
    height: 0.95rem;
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
    cursor: pointer;
  }
  .thumb::-moz-range-thumb {
    pointer-events: auto;
    width: 0.95rem;
    height: 0.95rem;
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
    cursor: pointer;
  }
  .deviation {
    margin: 0;
    font-size: 0.8rem;
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .deviation.user,
  .deviation.availability,
  .deviation.sub-annual {
    color: #92600a; /* amber — matches the picker's advisory tone */
  }
  .deviation.sub-annual code {
    font-size: 0.95em;
  }
  .deviation.availability.soft {
    color: var(--muted); /* softened: advisory FYI, not amber */
  }
  .reset {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.1rem 0.45rem;
    border: 1px solid currentColor;
    border-radius: 999px;
    background: none;
    color: inherit;
    cursor: pointer;
  }
  .reset:hover {
    background: var(--surface);
  }
</style>
