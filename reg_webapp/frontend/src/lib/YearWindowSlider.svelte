<script lang="ts">
// A compact DUAL-THUMB year slider for the app header — sets the global project
// window (#611 → Period model). Self-contained: it takes the bounds + the active
// window in, and reports a change out via `onchange` (commit) / `onclear`
// (reset to full history). The header (App.svelte) owns the wiring to the window
// runtime layer (`window.svelte.ts`); this component is pure presentation so
// it's unit-testable in isolation (props in, callbacks out, no store import).
//
// Implementation: two overlaid native `<input type="range">` thumbs (one "from",
// one "to"). Native ranges give us real `slider` ARIA roles + keyboard support
// for free; we clamp so the thumbs can't cross (from <= to). When no window is
// set, the thumbs seed at the full [min, max] span (a no-op visual default) and
// the readout shows "full history" until the user moves a thumb.
//
// COMMIT-ON-RELEASE (#629 item 2): the thumbs/readout update LIVE on each native
// `input` tick (the local `from`/`to` $state below — smooth dragging), but we
// only COMMIT out via `onchange` on the native `change` event (pointer release /
// keyboard commit). The header writes the window store on `onchange`, so a drag
// is one store write (one draft clone + autosave / one localStorage write), not
// one per tick.

import { untrack } from "svelte";
import type { StudyWindow } from "./project_data";

interface Props {
  // The slider bounds (inclusive). `min` is a fixed floor (a sensible earliest
  // register year); `max` is the catalog vintage year (or the current year).
  min: number;
  max: number;
  // The active window, or null = no window set (full history).
  window: StudyWindow | null;
  // Commit a new window (clamped to [min, max], from <= to). Fired on RELEASE
  // (native `change`), not per live `input` tick. Never emits null — clearing
  // back to full history goes through `onclear`.
  onchange: (next: StudyWindow) => void;
  // Reset the window to full history (the header maps this to `set(null)` so
  // `isFullHistory` becomes reachable again after any interaction — #629 item 1).
  onclear: () => void;
}
let { min, max, window: active, onchange, onclear }: Props = $props();

function clamp(year: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, year));
}

// The thumbs seed from the active window, or the full span when none is set.
// Local buffer so dragging is smooth; the source of truth stays the `window`
// prop. `untrack` keeps this seed read out of the reactive graph so it's a
// one-shot initializer, not a tracked dependency — the $effect below owns
// re-seeding on prop change (a controlled-component re-sync, see below).
let from = $state(untrack(() => clamp(active?.from ?? min, min, max)));
let to = $state(untrack(() => clamp(active?.to ?? max, min, max)));
// Re-seed the buffer whenever the active window (or bounds) changes — keeps the
// thumbs in sync when the window is set elsewhere (a project opens, the picker
// writes it). Reading `active`/`min`/`max` here registers the dependency.
$effect(() => {
  from = clamp(active?.from ?? min, min, max);
  to = clamp(active?.to ?? max, min, max);
});

// Whether the current thumbs cover the FULL bounds and no window is set — the
// "full history" readout state (a slider parked at the extremes with no explicit
// window means the user hasn't narrowed anything).
const isFullHistory = $derived(active === null && from === min && to === max);

// LIVE display on each `input` tick: update the local thumb buffer only (smooth
// drag, no commit). The clamp keeps the thumbs from crossing.
function onFromInput(event: Event): void {
  const v = clamp(
    Number((event.currentTarget as HTMLInputElement).value),
    min,
    max,
  );
  from = Math.min(v, to); // never cross past `to`
}
function onToInput(event: Event): void {
  const v = clamp(
    Number((event.currentTarget as HTMLInputElement).value),
    min,
    max,
  );
  to = Math.max(v, from);
}
// COMMIT on `change` (pointer release / keyboard commit): emit the buffered
// window once. `from`/`to` already hold the clamped live values from `input`.
function onCommit(): void {
  onchange({ from, to });
}

// The highlighted span between the thumbs, as left/width percentages of the
// track, for the visual fill.
const span = $derived(max - min || 1);
const fillLeft = $derived(((from - min) / span) * 100);
const fillWidth = $derived(((to - from) / span) * 100);
</script>

<div class="year-window" role="group" aria-label="Project window (years)">
  <span class="readout" aria-live="polite">
    {#if isFullHistory}
      full history
    {:else}
      {from}–{to}
    {/if}
  </span>
  <div class="track">
    <div
      class="fill"
      style="left: {fillLeft}%; width: {fillWidth}%;"
    ></div>
    <!-- Two overlaid thumbs. Each is a real range input (slider role); the
         `from` thumb sits above so it stays grabbable when the two coincide.
         `oninput` updates the LIVE thumb buffer; `onchange` COMMITS on release
         (#629 item 2). -->
    <input
      class="thumb thumb-from"
      type="range"
      {min}
      {max}
      step="1"
      value={from}
      aria-label="From year"
      oninput={onFromInput}
      onchange={onCommit}
    />
    <input
      class="thumb thumb-to"
      type="range"
      {min}
      {max}
      step="1"
      value={to}
      aria-label="To year"
      oninput={onToInput}
      onchange={onCommit}
    />
  </div>
  <!-- Clear control (#629 item 1): an explicit reset to full history — the only
       way back to a `null` window once dragged (a moved slider always expresses
       an explicit span). Hidden when already at full history so it's not a no-op. -->
  {#if active !== null}
    <button
      type="button"
      class="clear"
      aria-label="Clear project window (full history)"
      title="Clear — full history"
      onclick={() => onclear()}
    >
      ✕
    </button>
  {/if}
</div>

<style>
  .year-window {
    display: flex;
    align-items: center;
    gap: 0.6rem;
  }
  .readout {
    font-variant-numeric: tabular-nums;
    font-size: 0.8rem;
    color: var(--muted);
    white-space: nowrap;
    min-width: 5.5rem;
    text-align: right;
  }
  .clear {
    /* A small, low-emphasis reset glyph next to the track. Sized to a >=24px
       hit target (WCAG 2.5.8) while staying visually compact via an inline-flex
       box that centers the glyph; the readout · slider · ✕ row stays aligned. */
    display: inline-flex;
    align-items: center;
    justify-content: center;
    box-sizing: border-box;
    min-width: 28px;
    min-height: 28px;
    font: inherit;
    font-size: 0.7rem;
    line-height: 1;
    padding: 0.25rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: none;
    color: var(--muted);
    cursor: pointer;
  }
  .clear:hover {
    color: var(--accent);
    border-color: var(--accent);
  }
  .clear:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
  }
  .track {
    position: relative;
    width: 9rem;
    height: 1.25rem;
  }
  .track::before {
    /* The base rail. */
    content: "";
    position: absolute;
    left: 0;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
    height: 3px;
    border-radius: 3px;
    background: var(--border);
  }
  .fill {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 3px;
    border-radius: 3px;
    background: var(--accent);
  }
  /* Both range inputs stack on the same track; only their thumbs are visible
     (the native rail is hidden so our `.track::before` + `.fill` show through).
     `pointer-events: none` on the input lets clicks reach whichever thumb is
     under the cursor; the thumbs re-enable pointer events. */
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
    /* Above the `to` thumb so a coincident pair stays draggable apart. */
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
    width: 0.85rem;
    height: 0.85rem;
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
    cursor: pointer;
  }
  .thumb::-moz-range-thumb {
    pointer-events: auto;
    width: 0.85rem;
    height: 0.85rem;
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
    cursor: pointer;
  }
</style>
