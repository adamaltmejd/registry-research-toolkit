<script lang="ts">
import PeriodListInput from "./PeriodListInput.svelte";
import PeriodRangeInput from "./PeriodRangeInput.svelte";
import PeriodWindowSlider from "./PeriodWindowSlider.svelte";
import {
  type Coverage,
  clampYearWindow,
  looksLikePeriod,
  type PeriodGrain,
  periodFieldFromQuery,
  periodQueryFromField,
  rangeRepresentable,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";
import type { StudyWindow } from "./project_data";

// WINDOW-ANCHORED period selection (#615): the DEFAULT control is a year-grain
// availability slider (PeriodWindowSlider) seeded from the project window, over
// the subject's data-coverage track. The rich grammar lives behind a "More
// options" expander: the #308 range-first from/to picker (PeriodRangeInput),
// the #338/#340 Segments interrupted-series input (PeriodListInput → the #307
// comma wire), and a Text escape hatch for the wire grammar (special tokens,
// `_default`, mixed-grain ranges) with an ADVISORY-only hint. The wire grammar
// stays the SERIALIZATION throughout — the slider emits the same `?period`
// value the range picker already does (a bare year or a `from..to` year range).
// The server (`reg_meta.fqid`) is the canonical validator; submit is never
// blocked. The chosen value is emitted UP to BindingLeafView, which writes it
// to the URL query (apply-on-submit; #306 named the affordance Apply).
//
// PRECEDENCE for what's shown/active (#615 → #611 Period model):
//   ?period (explicit local) > project window > full history.
// A local change writes `?period` ONLY — it NEVER mutates the global window
// (the slider's `onreset` clears `?period`, which falls back to the window).
let {
  period,
  grains = undefined,
  window = null,
  coverage = null,
  vintageYear = undefined,
  onsubmit,
  onclear,
}: {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** Grains offered by the range picker, pre-narrowed to those the variable's
   * states exhibit (#308 option b); undefined → the component's default. */
  grains?: PeriodGrain[];
  /** The global project window (#614), or null = none set. Seeds the slider
   * (precedence below) and drives the user-deviation hint. */
  window?: StudyWindow | null;
  /** The subject's data-availability span (derived from embedded states, #615),
   * or null = unknown. Sides are independently nullable (an open start/end);
   * drives the slider's greyed not-delivered track. */
  coverage?: Coverage | null;
  /** The catalog VINTAGE year (#631) — the slider's open-ended ceiling, matching
   * the header window slider (App caps both at `context.reg_meta.import_date`'s
   * year). undefined only before `/api/context` resolves; falls back to
   * wall-clock so a pre-context leaf still renders (mirrors App's fallback). */
  vintageYear?: number;
  /** Emitted with the chosen wire value on submit (empty → cleared). */
  onsubmit: (period: string) => void;
  /** Emitted when the clear button is pressed (drop `?period`). */
  onclear: () => void;
} = $props();

type PickerMode = "range" | "list" | "text";

// A sensible earliest register year — the slider's floor when neither the
// window nor coverage reaches further back (mirrors App.svelte's
// WINDOW_FLOOR_YEAR; the bounds widen to fit window/coverage/selection below).
const SLIDER_FLOOR_YEAR = 1960;

// The slider's open-ended CEILING (#631): the catalog vintage year, matching the
// header window slider (both cap at `context.reg_meta.import_date`'s year). The
// `vintageYear` prop is undefined only before `/api/context` resolves — then fall
// back to wall-clock so a pre-context leaf still renders (mirrors App's `||
// new Date().getFullYear()`).
const ceilingYear = $derived(vintageYear ?? new Date().getFullYear());

// Mode inference: range-first; an ACTIVE comma list opens in Segments; any
// other period the range UI can't represent opens in text mode (it must be
// visible/editable, not silently blanked). Grains-aware: a period at a grain
// this variable doesn't offer must open in text mode too.
function inferMode(p: string | null): PickerMode {
  if (p === null || p === "") {
    return "range";
  }
  if (p.includes(",")) {
    return "list";
  }
  return rangeRepresentable(p, grains) ? "range" : "text";
}

// svelte-ignore state_referenced_locally — intentional one-time seed; the
// $effect below re-syncs on URL changes.
let mode = $state<PickerMode>(inferMode(period));

// ── Text mode (the wire-grammar escape hatch — unchanged semantics) ─────────
// svelte-ignore state_referenced_locally — intentional one-time seed (the
// $effect keeps it in sync with the URL afterward).
let field = $state(periodFieldFromQuery(period));

// ── Range + Segments modes ───────────────────────────────────────────────────
// Each input's latest emit (null while incomplete/empty). The controls remount
// via {#key period} so back/forward (or an external narrowing) re-seeds them
// from the URL — and the $effect below re-syncs every mode's value on the same
// trigger (without it, Apply after back/forward would re-submit the stale
// pre-navigation value).
let rangeWire = $state<string | null>(null);
let listWire = $state<string | null>(null);

/** The list input's seed for the CURRENT period: any non-sentinel wire (a
 * scalar carries over as the first segment — the upgrade path); `_default`
 * is not a segment and seeds empty. */
function listSeed(p: string | null): string | null {
  return p !== null && p !== "" && p !== "_default" ? p : null;
}

$effect(() => {
  field = periodFieldFromQuery(period);
  // Only a range-REPRESENTABLE value seeds the range buffer: an active
  // `_default`/comma/mixed-grain period must not sit invisibly behind blank
  // range controls where a manual switch to Picker + Apply would re-submit
  // it (the #347/#349 stale-buffer class) — Apply no-ops on null instead.
  rangeWire =
    period !== null && rangeRepresentable(period, grains) ? period : null;
  listWire = listSeed(period);
  // Re-derive the mode too: back/forward can land on a period the active UI
  // can't represent (`_default`, a mixed-grain range, a comma list) — staying
  // put would show BLANK controls while Apply re-submits the invisible value.
  mode = inferMode(period);
});

// ── Year-window slider (#615 default control) ───────────────────────────────
// PRECEDENCE for the slider's active selection: a year-representable `?period`
// (explicit local) > the project window > full history. A sub-annual / list /
// `_default` / text `?period` is NOT year-representable — it can't live on the
// year slider, so the picker opens the "more" expander straight away (the
// range/list/text modes hold it), with the slider showing only the window/full
// fallback when collapsed back.

/** The year window the slider should show as ACTIVE, applying the precedence. A
 * year-representable `?period` wins; else the project window; else null (full
 * history → the slider parks at the coverage/bounds span as a no-op default). */
const activeYearSelection = $derived<StudyWindow | null>(
  yearWindowFromWire(period) ?? window,
);

/** The active `?period` wire when it is set but NOT year-representable (a
 * sub-annual token / segment list / `_default` / text) — null otherwise. In
 * this case `activeYearSelection` fell back to the window, so the slider's span
 * is the window PROJECTION, not the active selection; the slider takes this to
 * suppress the misleading "no deviation" reading and show an honest cue
 * pointing at the real value in the (already-open) "more" expander. */
const subAnnualPeriod = $derived<string | null>(
  period !== null && !yearWindowRepresentable(period) ? period : null,
);

/** The slider bounds: the floor/ceiling that fit ONLY what's drawn — the window,
 * the active selection, and the coverage END — so none is clipped off-track.
 * Coverage sides are INDEPENDENTLY nullable (#615) — an open START doesn't extend
 * the bounds (the slider edge stands in for "open" via `coverageBand`), and an
 * open END contributes the catalog VINTAGE (#631), the ceiling the slider projects
 * "still delivered" to. The vintage is NOT a forced floor on `max`: a finite
 * coverage (1995–2008) stays bounded by its real end, not pushed to the vintage —
 * so the default span doesn't spuriously report the post-coverage years as
 * not-delivered. A window/selection past the vintage still WIDENS the bounds
 * (never clip a real thumb value), but that doesn't extend the coverage band — the
 * gap then flags the beyond-vintage span. */
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
    // The coverage END: the finite end, or the vintage when open-ended — so an
    // open-ended coverage reaches the vintage but a finite one is NOT extended
    // to it (Codex P2).
    years.push(coverage.to ?? ceilingYear);
  }
  const max = years.length > 0 ? Math.max(...years) : ceilingYear;
  const min = Math.min(SLIDER_FLOOR_YEAR, ...years, max);
  return { min, max };
});

/** The selection to seed the slider thumbs from — the active selection clamped
 * to the bounds, or the full bounds span when none is set (a no-op "full
 * history" default the user can then narrow). */
const sliderSelection = $derived<StudyWindow>(
  activeYearSelection
    ? clampYearWindow(activeYearSelection, sliderBounds.min, sliderBounds.max)
    : { from: sliderBounds.min, to: sliderBounds.max },
);

// The slider's live selection (null until the user moves a thumb — Apply then
// submits it). Re-armed by the {#key period} remount on URL change.
let sliderWire = $state<string | null>(null);

// The "more options" expander. Opens by default for an active `?period` the year
// slider can't represent (sub-annual / list / `_default` / text) so it's
// visible + editable, never hidden behind the year-only slider.
// svelte-ignore state_referenced_locally — intentional one-time seed; the
// $effect below re-syncs it on URL change.
let showMore = $state(period !== null && !yearWindowRepresentable(period));

$effect(() => {
  // Re-arm the slider buffer + re-open the expander on URL change (back/forward
  // or an external narrowing) — mirrors the range/list re-seed above. Also track
  // the SEED (`activeYearSelection`): with no `?period`, changing the global
  // window (header) or opening a project re-seeds the slider to the new
  // selection, so a stale dragged `sliderWire` must clear too — else the next
  // Apply submits the OLD dragged value, not the now-displayed window (Codex P2).
  // And track the CEILING (`ceilingYear`): a leaf rendered before `/api/context`
  // resolves seeds the ceiling at wall-clock, then flips to the catalog vintage
  // when the prop threads down — without clearing, a stale drag past the vintage
  // would survive the display correction and Apply a beyond-vintage wire (Codex P2).
  // A drag alone changes none of these, so the legitimate drag-then-Apply path is
  // untouched (the effect only re-fires on a re-seed / ceiling flip).
  void period;
  void activeYearSelection;
  void ceilingYear;
  sliderWire = null;
  showMore = period !== null && !yearWindowRepresentable(period);
});

/** Apply the slider's current selection. A moved thumb (`sliderWire`) wins;
 * otherwise fall back to the SEEDED selection so accepting the displayed
 * window-default actually applies it — without this, Apply on an untouched
 * seeded window no-ops and BindingLeafView (narrowing only on `?period`) leaves
 * the user on full history (Codex P2). No `activeYearSelection` (full-history
 * default) → nothing to apply, so the no-op stands. */
function applySlider(): void {
  const wire =
    sliderWire ??
    (activeYearSelection !== null
      ? yearWindowToWire(activeYearSelection)
      : null);
  if (wire !== null) {
    onsubmit(wire);
  }
}

/** "Reset to project window" — narrow BACK to the window, like Apply (Codex P2).
 * It SUBMITS the window's wire (`?period == window` → the user-deviation clears,
 * and BindingLeafView, which narrows only on `?period`, lands on the window),
 * NOT the clear path that would drop `?period` and fall through to full history.
 * No window → nothing to reset to, so it clears (full history). The standalone
 * "Clear" button keeps the explicit drop-to-full-history affordance. */
function resetToWindow(): void {
  if (window !== null) {
    onsubmit(yearWindowToWire(window));
  } else {
    onclear();
  }
}

// ADVISORY only (text mode): a non-empty field that doesn't match the grammar
// shows a hint. Submit is NEVER gated on it.
const advisoryInvalid = $derived(
  mode === "text" && field.trim() !== "" && !looksLikePeriod(field),
);

function submit(event: SubmitEvent): void {
  event.preventDefault();
  // The default (collapsed) control is the year slider — Enter applies it. The
  // expander's mode-specific Apply buttons are inside `showMore`.
  if (!showMore) {
    applySlider();
    return;
  }
  if (mode === "text") {
    const value = periodQueryFromField(field);
    if (value === null) {
      onclear();
    } else {
      onsubmit(value);
    }
    return;
  }
  const wire = mode === "list" ? listWire : rangeWire;
  if (wire !== null) {
    onsubmit(wire);
  }
}

const MODE_LABELS: Record<PickerMode, string> = {
  range: "Picker",
  list: "Segments",
  text: "Text",
};
</script>

<form class="period-picker" onsubmit={submit}>
  <div class="head">
    <span class="title" id="period-label">Period</span>
    <!-- The "more" expander (#615): the rich grammar (range/list/text) lives
         behind it; the year slider is the default. -->
    <button
      type="button"
      class="more-toggle"
      aria-expanded={showMore}
      aria-controls="period-more"
      onclick={() => (showMore = !showMore)}
    >
      {showMore ? "Fewer options" : "More options"}
    </button>
  </div>

  <!-- DEFAULT: the year-window availability slider (#615). Keyed on the URL
       period so back/forward re-seeds the thumbs; submit applies the slider's
       selection, reset NARROWS back to the project window (submits its wire, like
       Apply — not a clear; the standalone Clear button drops to full history). -->
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
        onchange={(next) => (sliderWire = yearWindowToWire(next))}
        onreset={() => resetToWindow()}
      />
      <div class="actions">
        <!-- Distinct accessible name from the expander modes' "Apply" (both
             would otherwise collide as one role+name). -->
        <button
          type="button"
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
  {#if !window}
    <p class="muted help">
      No project window set — narrow the slider to focus a period, or set a
      window in the header.
    </p>
  {/if}

  {#if showMore}
  <div id="period-more" class="more">
  <div class="mode-toggles" role="group" aria-label="Period input mode">
    {#each ["range", "list", "text"] as const as m (m)}
      <button
        type="button"
        class="mode-toggle"
        aria-pressed={mode === m}
        onclick={() => (mode = m)}
      >
        {MODE_LABELS[m]}
      </button>
    {/each}
  </div>

  {#if mode === "text"}
    <div class="row">
      <input
        id="period-input"
        type="text"
        bind:value={field}
        placeholder="e.g. 2020, HT2020, 2018..2020, _default"
        autocomplete="off"
        spellcheck="false"
        aria-labelledby="period-label"
        aria-describedby="period-help{advisoryInvalid ? ' period-advisory' : ''}"
      />
      <!-- #306: no "Resolve" verb — the period just applies. -->
      <button type="submit">Apply</button>
      {#if period !== null}
        <button type="button" class="clear" onclick={() => onclear()}>
          Clear
        </button>
      {/if}
    </div>
    <p id="period-help" class="muted help">
      A year (<code>2020</code>), a term/quarter/month/day token
      (<code>HT2020</code>, <code>2020-Q3</code>, <code>2020-08</code>,
      <code>2020-12-31</code>), a range (<code>2018..2020</code>), or
      <code>_default</code>. Leave blank for full history.
    </p>
    {#if advisoryInvalid}
      <!-- ADVISORY: the server validates; this never blocks Apply. -->
      <p id="period-advisory" class="advisory" role="status">
        This doesn't look like a period — you can still apply it; the server
        will confirm.
      </p>
    {/if}
  {:else if mode === "list"}
    <div class="row range-row">
      <!-- Keyed on the URL period: back/forward or an external change
           re-seeds the segments (the component itself seeds once at mount). -->
      {#key period}
        <PeriodListInput
          value={listSeed(period)}
          {grains}
          onchange={(wire) => (listWire = wire)}
        />
      {/key}
      <div class="actions">
        <button type="submit">Apply</button>
        {#if period !== null}
          <button type="button" class="clear" onclick={() => onclear()}>
            Clear
          </button>
        {/if}
      </div>
    </div>
    <p class="muted help">
      An interrupted series: add each segment, then Apply narrows to their
      union.
    </p>
  {:else}
    <div class="row range-row">
      <!-- Keyed on the URL period: back/forward or an external change
           re-seeds the controls (the component itself seeds once at mount). -->
      {#key period}
        <PeriodRangeInput
          value={period}
          {grains}
          onchange={(wire) => (rangeWire = wire)}
        />
      {/key}
      <div class="actions">
        <button type="submit">Apply</button>
        {#if period !== null}
          <button type="button" class="clear" onclick={() => onclear()}>
            Clear
          </button>
        {/if}
      </div>
    </div>
  {/if}
  </div>
  {/if}
</form>

<style>
  .period-picker {
    margin: 1.25rem 0;
    padding: 0.75rem 0.9rem;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface);
  }
  .head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
    margin-bottom: 0.45rem;
  }
  .title {
    font-weight: 600;
  }
  .more-toggle {
    font: inherit;
    font-size: 0.75rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--surface);
    color: var(--accent);
    cursor: pointer;
  }
  .more-toggle:hover {
    border-color: var(--accent);
  }
  /* The default slider row + its Apply/Clear actions. */
  .slider-row {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 0.75rem;
  }
  /* The "more" expander panel — a separated block below the slider. */
  .more {
    margin-top: 0.75rem;
    padding-top: 0.6rem;
    border-top: 1px solid var(--border);
  }
  .mode-toggles {
    display: flex;
    gap: 0.3rem;
    margin-bottom: 0.45rem;
  }
  .mode-toggle {
    font: inherit;
    font-size: 0.75rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--surface);
    color: var(--accent);
    cursor: pointer;
  }
  .mode-toggle:hover {
    border-color: var(--accent);
  }
  .mode-toggle[aria-pressed="true"] {
    border-color: var(--accent);
    background: var(--accent);
    color: #fff;
  }
  .row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    align-items: center;
  }
  .range-row {
    align-items: flex-end;
    justify-content: space-between;
  }
  .actions {
    display: flex;
    gap: 0.5rem;
  }
  input[type="text"] {
    flex: 1 1 16rem;
    min-width: 0;
    padding: 0.4rem 0.55rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    font: inherit;
  }
  button[type="submit"],
  button.clear {
    padding: 0.4rem 0.9rem;
    border: 1px solid var(--accent);
    border-radius: 4px;
    background: var(--accent);
    color: #fff;
    font: inherit;
    cursor: pointer;
  }
  button.clear {
    background: var(--surface);
    color: var(--accent);
  }
  button[type="submit"]:hover,
  button.clear:hover {
    filter: brightness(0.95);
  }
  .help {
    margin: 0.5rem 0 0;
    font-size: 0.85rem;
  }
  .help code {
    font-size: 0.95em;
  }
  .advisory {
    margin: 0.35rem 0 0;
    font-size: 0.85rem;
    color: #92600a;
  }
</style>
