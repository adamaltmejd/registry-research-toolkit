<script lang="ts">
import type { VariableStateModel } from "./api";

// Presentational view of a variable's `variable_state` rows (from the full
// node's embedded `states` OR a `?period`-narrowed StatesResponse). Pure
// presentation + selection callbacks — it never fetches and never navigates;
// BindingLeafView owns the URL writes.
//
//   length === 1 → single-state DETAIL (variant, validity, type/length, column,
//                  value-set version + the (code, label) table in a
//                  height-constrained scroll container).
//   length  > 1 → a PICKER grouping by `variant` (cross-variant) AND by
//                  `value_set_version_label` (multi-vintage). Selecting writes
//                  `&variant=`/`&value_set_version=` to narrow to length-1.
//   length === 0 → a clean "no state delivered for this period" message (a valid
//                  period outside every validity window — NOT an error).
let {
  states,
  narrowed,
  activeVariant = null,
  activeValueSetVersion = null,
  onpickVariant,
  onpickValueSetVersion,
}: {
  states: VariableStateModel[];
  /** True when these are the `?period`-narrowed subset (drives empty wording). */
  narrowed: boolean;
  activeVariant?: string | null;
  activeValueSetVersion?: string | null;
  onpickVariant: (variant: string) => void;
  onpickValueSetVersion: (valueSetVersion: string) => void;
} = $props();

const single = $derived(states.length === 1 ? states[0] : null);

// Distinct variants / value-set versions across the (multi-state) set — the two
// narrowing axes. Order-preserving de-dup so the picker is stable.
function distinct<K>(xs: K[]): K[] {
  return [...new Set(xs)];
}
const variants = $derived(distinct(states.map((s) => s.variant)));
const versions = $derived(
  distinct(states.map((s) => s.value_set_version_label)),
);
</script>

{#if states.length === 0}
  <p class="muted empty">
    {#if narrowed}
      No state delivered for this period.
    {:else}
      This variable has no recorded states.
    {/if}
  </p>
{:else if single}
  {@const s = single}
  <div class="state-detail">
    <dl class="meta">
      <dt>Variant</dt>
      <dd><code>{s.variant}</code></dd>
      <dt>Valid</dt>
      <dd>{s.valid_from} – {s.valid_to}</dd>
      {#if s.data_type}
        <dt>Data type</dt>
        <dd>{s.data_type}{#if s.data_length}({s.data_length}){/if}</dd>
      {/if}
      {#if s.delivery_column_name}
        <dt>Delivery column</dt>
        <dd><code>{s.delivery_column_name}</code></dd>
      {/if}
      <dt>Value-set version</dt>
      <dd>{s.value_set_version_label}</dd>
    </dl>

    {#if s.value_set && s.value_set.length > 0}
      <h4 class="vs-heading">
        Value set <span class="muted">({s.value_set.length})</span>
      </h4>
      <!-- Height-constrained: LISA value sets can be hundreds of codes. -->
      <div class="value-set-scroll">
        <table class="value-set">
          <thead>
            <tr><th>Code</th><th>Label</th></tr>
          </thead>
          <tbody>
            {#each s.value_set as member (member.code)}
              <tr>
                <td><code>{member.code}</code></td>
                <td>{member.label}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else}
      <p class="muted">No value set.</p>
    {/if}
  </div>
{:else}
  <!-- Multiple states. The narrowing picker writes `?variant`/`?value_set_version`,
       which the server only honors WITH a `?period` (it 422s them otherwise), so
       it's shown only when a period is active (`narrowed`). Without a period the
       list is the full state history — set a period to narrow to one. -->
  {#if narrowed}
    <p class="muted picker-hint">
      {states.length} states at this period across {variants.length}
      {variants.length === 1 ? "variant" : "variants"}. Narrow to a single state:
    </p>
    {#if variants.length > 1}
      <fieldset class="picker">
        <legend>Variant</legend>
        <div class="chips">
          {#each variants as variant (variant)}
            <button
              type="button"
              class="chip"
              class:active={variant === activeVariant}
              aria-pressed={variant === activeVariant}
              onclick={() => onpickVariant(variant)}
            >
              {variant}
            </button>
          {/each}
        </div>
      </fieldset>
    {/if}
    {#if versions.length > 1}
      <fieldset class="picker">
        <legend>Value-set version</legend>
        <div class="chips">
          {#each versions as version (version)}
            <button
              type="button"
              class="chip"
              class:active={version === activeValueSetVersion}
              aria-pressed={version === activeValueSetVersion}
              onclick={() => onpickValueSetVersion(version)}
            >
              {version}
            </button>
          {/each}
        </div>
      </fieldset>
    {/if}
  {:else}
    <p class="muted picker-hint">
      {states.length} states over time. Set a period above to resolve to one.
    </p>
  {/if}

  <ul class="state-list">
    {#each states as s (s.state_id)}
      <li>
        <span class="state-variant"><code>{s.variant}</code></span>
        <span class="state-validity muted">{s.valid_from} – {s.valid_to}</span>
        <span class="state-vsv muted">{s.value_set_version_label}</span>
        {#if s.delivery_column_name}
          <code class="state-col">{s.delivery_column_name}</code>
        {/if}
      </li>
    {/each}
  </ul>
{/if}

<style>
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.3rem 1rem;
    margin: 0.5rem 0 1rem;
  }
  .meta dt {
    font-weight: 600;
  }
  .vs-heading {
    margin: 0.5rem 0 0.4rem;
  }
  .value-set-scroll {
    max-height: 18rem;
    overflow-y: auto;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  table.value-set {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
  }
  table.value-set th {
    position: sticky;
    top: 0;
    background: var(--surface);
    text-align: left;
    border-bottom: 1px solid var(--border);
    padding: 0.35rem 0.6rem;
  }
  table.value-set td {
    padding: 0.3rem 0.6rem;
    border-bottom: 1px solid #eee;
    vertical-align: top;
  }
  .picker {
    border: 1px solid var(--border);
    border-radius: 6px;
    margin: 0.5rem 0;
    padding: 0.5rem 0.75rem 0.75rem;
  }
  .picker legend {
    font-weight: 600;
    padding: 0 0.3rem;
  }
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .chip {
    padding: 0.25rem 0.7rem;
    border: 1px solid var(--accent);
    border-radius: 999px;
    background: var(--surface);
    color: var(--accent);
    font: inherit;
    font-size: 0.85rem;
    cursor: pointer;
  }
  .chip.active {
    background: var(--accent);
    color: #fff;
  }
  .chip:hover {
    background: var(--accent-bg);
  }
  .chip.active:hover {
    background: var(--accent);
    filter: brightness(0.95);
  }
  .state-list {
    list-style: none;
    padding: 0;
    margin: 0.75rem 0 0;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .state-list li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.75rem;
    padding: 0.35rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  .state-col {
    margin-left: auto;
    font-size: 0.85em;
  }
</style>
