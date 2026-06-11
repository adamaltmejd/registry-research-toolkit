<script lang="ts">
import PeriodRangeInput from "./PeriodRangeInput.svelte";
import {
  looksLikePeriod,
  type PeriodGrain,
  periodFieldFromQuery,
  periodQueryFromField,
  rangeRepresentable,
} from "./period";

// RANGE-FIRST period selection (#308): the default UI is a grain-aware
// from/to picker (PeriodRangeInput) — no token grammar knowledge required;
// the wire grammar stays the SERIALIZATION. A "Text" escape hatch keeps the
// free-text wire field (special tokens, `_default`, mixed-grain ranges) with
// its ADVISORY-only hint — the server is the CANONICAL validator and submit
// is never blocked. A "clear" button removes the period (full history). The
// chosen value is emitted UP to BindingLeafView, which writes it to the URL
// query (apply-on-submit; #306 named the affordance Apply).
let {
  period,
  grains = undefined,
  onsubmit,
  onclear,
}: {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** Grains offered by the range picker, pre-narrowed to those the variable's
   * states exhibit (#308 option b); undefined → the component's default. */
  grains?: PeriodGrain[];
  /** Emitted with the chosen wire value on submit (empty → cleared). */
  onsubmit: (period: string) => void;
  /** Emitted when the clear button is pressed (drop `?period`). */
  onclear: () => void;
} = $props();

// Mode: range-first; an ACTIVE period the range UI can't represent opens in
// text mode (it must be visible/editable, not silently blanked).
let textMode = $state(
  // svelte-ignore state_referenced_locally — intentional one-time seed; the
  // $effect below re-syncs on URL changes.
  period !== null && period !== "" && !rangeRepresentable(period),
);

// ── Text mode (the wire-grammar escape hatch — unchanged semantics) ─────────
// svelte-ignore state_referenced_locally — intentional one-time seed (the
// $effect keeps it in sync with the URL afterward).
let field = $state(periodFieldFromQuery(period));

// ── Range mode ───────────────────────────────────────────────────────────────
// The range input's latest emit (null while FROM is incomplete). The controls
// remount via {#key period} so back/forward (or an external narrowing)
// re-seeds them from the URL — and the $effect below re-syncs BOTH modes'
// values on the same trigger (without it, Apply after back/forward would
// re-submit the stale pre-navigation value).
let rangeWire = $state<string | null>(null);
$effect(() => {
  field = periodFieldFromQuery(period);
  rangeWire = period;
  // Re-derive the mode too: back/forward can land on a period the range UI
  // can't represent (`_default`, a mixed-grain range) — staying in range mode
  // would show BLANK controls while Apply re-submits the invisible value.
  textMode = period !== null && period !== "" && !rangeRepresentable(period);
});

// ADVISORY only (text mode): a non-empty field that doesn't match the grammar
// shows a hint. Submit is NEVER gated on it.
const advisoryInvalid = $derived(
  textMode && field.trim() !== "" && !looksLikePeriod(field),
);

function submit(event: SubmitEvent): void {
  event.preventDefault();
  if (textMode) {
    const value = periodQueryFromField(field);
    if (value === null) {
      onclear();
    } else {
      onsubmit(value);
    }
    return;
  }
  if (rangeWire !== null) {
    onsubmit(rangeWire);
  }
}
</script>

<form class="period-picker" onsubmit={submit}>
  <div class="head">
    <span class="title" id="period-label">Period</span>
    <button
      type="button"
      class="mode-toggle"
      aria-pressed={textMode}
      onclick={() => (textMode = !textMode)}
    >
      {textMode ? "Use the picker" : "Type a period"}
    </button>
  </div>

  {#if textMode}
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
