<script lang="ts">
import {
  looksLikePeriod,
  periodFieldFromQuery,
  periodQueryFromField,
} from "./period";

// A single free-text period field accepting the wire grammar (year `2020`;
// token `HT2020`/`VT2020`/`2020-Q3`/`2020-H1`/`2020-08`/`2020-12-31`; range
// `2018..2020`; `_default`; see reg_webapp/DESIGN.md → Catalog router structure).
// The server is the CANONICAL validator — the inline
// hint is ADVISORY only and NEVER blocks submit (a "looks wrong" value is still
// sent so the server's 422 detail is the authority). A "clear" button removes the
// period (full history). The chosen value is emitted UP to BindingLeafView, which
// writes it to the URL query.
let {
  period,
  onsubmit,
  onclear,
}: {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** Emitted with the trimmed field value on submit (empty → cleared). */
  onsubmit: (period: string) => void;
  /** Emitted when the clear button is pressed (drop `?period`). */
  onclear: () => void;
} = $props();

// Local editable field text, seeded from the URL period, then re-synced when the
// URL period changes underneath us (back/forward, a state-picker narrowing). The
// initializer's `period` read is the intentional SEED (the `$effect` keeps it in
// sync afterward), so the "only captures initial value" lint is expected here.
// svelte-ignore state_referenced_locally
let field = $state(periodFieldFromQuery(period));
$effect(() => {
  field = periodFieldFromQuery(period);
});

// ADVISORY only: a non-empty field that doesn't match the grammar shows a hint.
// Submit is NEVER gated on it.
const advisoryInvalid = $derived(
  field.trim() !== "" && !looksLikePeriod(field),
);

function submit(event: SubmitEvent): void {
  event.preventDefault();
  const value = periodQueryFromField(field);
  if (value === null) {
    onclear();
  } else {
    onsubmit(value);
  }
}
</script>

<form class="period-picker" onsubmit={submit}>
  <label for="period-input">Period</label>
  <div class="row">
    <input
      id="period-input"
      type="text"
      bind:value={field}
      placeholder="e.g. 2020, HT2020, 2018..2020, _default"
      autocomplete="off"
      spellcheck="false"
      aria-describedby="period-help{advisoryInvalid ? ' period-advisory' : ''}"
    />
    <!-- #306: no "Resolve" verb — from the user's view they just choose a period
         (it applies on submit/Enter; the URL-query mechanics are unchanged). -->
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
      This doesn't look like a period — you can still apply it; the server will
      confirm.
    </p>
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
  .period-picker label {
    display: block;
    font-weight: 600;
    margin-bottom: 0.35rem;
  }
  .row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    align-items: center;
  }
  input {
    flex: 1 1 16rem;
    min-width: 0;
    padding: 0.4rem 0.55rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    font: inherit;
  }
  button {
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
  button:hover {
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
