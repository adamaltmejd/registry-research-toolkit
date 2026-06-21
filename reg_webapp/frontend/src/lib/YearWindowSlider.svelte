<script lang="ts">
// A compact DUAL-THUMB year slider for the app header — sets the global project
// window (#611 → Period model). Self-contained: it takes the bounds + the active
// window in, and reports a change out via `onchange` (commit) / `onclear`
// (reset to full history). The header (App.svelte) owns the wiring to the window
// runtime layer (`window.svelte.ts`); this component is pure presentation so
// it's unit-testable in isolation (props in, callbacks out, no store import).
//
// Implementation: the shared DualThumbTrack primitive (#632) draws the two
// overlaid native `<input type="range">` thumbs (real `slider` ARIA + keyboard,
// clamped non-crossing); this component layers the readout, the `.fill`, and the
// clear control on top. When no window is set, the thumbs seed at the full
// [min, max] span (a no-op visual default) and the readout shows "full history"
// until the user moves a thumb.
//
// COMMIT-ON-RELEASE (#629 item 2): the thumbs/readout update LIVE on each native
// `input` tick (the bound `from`/`to` below — smooth dragging), but we only
// COMMIT out via `onchange` on the primitive's `onCommit` (native `change`:
// pointer release / keyboard commit). The header writes the window store on
// `onchange`, so a drag is one store write (one draft clone + autosave / one
// localStorage write), not one per tick.

import { untrack } from "svelte";
import DualThumbTrack from "./DualThumbTrack.svelte";
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

// The live thumb values, owned/clamped/re-synced by the DualThumbTrack primitive
// and bound back here so the readout + fill geometry react to them. The initial
// value is a one-shot placeholder (`untrack` — the primitive seeds the real value
// from `selection` via `bind:` and owns the re-sync); the $derived `selection`
// below is the tracked source.
let from = $state(untrack(() => active?.from ?? min));
let to = $state(untrack(() => active?.to ?? max));

// The source the primitive seeds + re-syncs the thumbs from: the active window,
// or the full span when none is set (a no-op visual default). Tracking `active`
// here keeps the thumbs in sync when the window is set elsewhere (a project
// opens, the picker writes it).
const selection = $derived({
  from: active?.from ?? min,
  to: active?.to ?? max,
});

// Whether the current thumbs cover the FULL bounds and no window is set — the
// "full history" readout state (a slider parked at the extremes with no explicit
// window means the user hasn't narrowed anything).
const isFullHistory = $derived(active === null && from === min && to === max);

// COMMIT on `change` (pointer release / keyboard commit): emit the buffered
// window once. `from`/`to` already hold the clamped live values from `input`.
function onCommit(): void {
  onchange({ from, to });
}

// The highlighted span between the thumbs, as left/width percentages of the
// track, for the visual fill (plain max−min geometry — distinct from the
// PeriodWindowSlider's year-as-cell model).
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
  <!-- The shared dual-thumb track (#632). `onCommit` (native `change`) is the
       COMMIT (#629 item 2) — no `onLiveInput`, so a drag updates only the bound
       `from`/`to` (live readout/fill) and commits once on release. The `.fill`
       is this slider's own decoration, drawn inside the track behind the thumbs. -->
  <DualThumbTrack {min} {max} {selection} bind:from bind:to {onCommit}>
    <div class="fill" style="left: {fillLeft}%; width: {fillWidth}%;"></div>
  </DualThumbTrack>
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
  /* The dual-thumb track is the shared DualThumbTrack primitive; this slider only
     fixes its WIDTH (the header chip is a fixed 9rem) — track height, rail, and
     thumb size keep the primitive's defaults (1.25rem / 3px / 0.85rem). The var
     inherits into the primitive's `.track`. */
  .year-window :global(.track) {
    --track-width: 9rem;
  }
  /* The selected-span fill — this slider's own in-track decoration (plain max−min
     geometry), drawn behind the primitive's thumbs. The native rail is hidden by
     the primitive, so the `.track::before` rail + this fill show through. */
  .fill {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 3px;
    border-radius: 3px;
    background: var(--accent);
  }
</style>
