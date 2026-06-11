<script lang="ts">
import type { VariableStateModel } from "./api";
import { formatDataType, formatStateWindow, stateChangeHints } from "./catalog";
import { VALUE_SET_VERSION_NONE } from "./period";

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
// No add affordance here: the page-level "Add to project" (#306) lives in
// BindingLeafView — a state is an implementation concept, not a pick target.
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
// ALL distinct versions, INCLUDING the empty default label (`value_set_version_label`
// is `TEXT NOT NULL DEFAULT ''`): the states DIFFER by version when this has >1,
// so it drives whether the version axis can narrow. The CHIPS, though, are only
// the non-empty labels — you can't narrow to "no version" via `?value_set_version=`
// (it would be omitted), so an empty-label state is narrowed by variant instead.
const versionsAll = $derived(
  distinct(states.map((s) => s.value_set_version_label)),
);
const versionChips = $derived(versionsAll.filter((v) => v !== ""));
// A state may carry the empty/default label; it gets a "(no version)" chip
// (sending the `_none` sentinel) so it's individually selectable too.
const hasEmptyVersion = $derived(versionsAll.includes(""));
// Whether either narrowing axis can actually resolve the multi-state set to one.
const canNarrow = $derived(variants.length > 1 || versionsAll.length > 1);

// #309: per-transition "what changed" hints (keyed by the LATER state's id) —
// adjacent same-variant states that differ only invisibly (the int→bigint
// case) get an explicit diff line under the row.
const changeHints = $derived(stateChangeHints(states));

// #310: inline per-state value-set expansion in LIST mode (keyed by state_id;
// reassigned-on-toggle so the runes see the change). Cleared when the state
// set changes underneath (narrowing) — keys are state_ids, so a surviving
// state's open table legitimately survives a narrow.
let expanded = $state<Record<number, boolean>>({});
function toggleExpanded(stateId: number): void {
  expanded = { ...expanded, [stateId]: !expanded[stateId] };
}
</script>

<!-- The scrollable (code, label) table — the SAME rendering for the detail
     mode and a list row's inline expansion (#310). Height-constrained: LISA
     value sets can be hundreds of codes. -->
{#snippet valueSetTable(valueSet: NonNullable<VariableStateModel["value_set"]>)}
  <div class="value-set-scroll">
    <table class="value-set">
      <thead>
        <tr><th>Code</th><th>Label</th></tr>
      </thead>
      <tbody>
        {#each valueSet as member (member.code)}
          <tr>
            <td><code>{member.code}</code></td>
            <td>{member.label}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/snippet}

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
      <!-- #309/#321: sentinel-free, coarsest-exact window ("since 2016",
           "VT2009"); the raw ISO window stays on the tooltip. -->
      <dd title="{s.valid_from} – {s.valid_to}">{formatStateWindow(s)}</dd>
      {#if s.data_type}
        <dt>Data type</dt>
        <!-- formatDataType drops a meaningless "(0)"/empty length parenthetical
             (the "bigint(0)" artifact) while keeping real ones like "char(25)". -->
        <dd>{formatDataType(s.data_type, s.data_length)}</dd>
      {/if}
      {#if s.delivery_column_name}
        <dt>Delivery column</dt>
        <dd><code>{s.delivery_column_name}</code></dd>
      {/if}
      <dt>Value-set version</dt>
      <dd>{s.value_set_version_label || "(no version)"}</dd>
    </dl>

    {#if s.value_set && s.value_set.length > 0}
      <h4 class="vs-heading">
        Value set <span class="muted">({s.value_set.length})</span>
      </h4>
      {@render valueSetTable(s.value_set)}
    {:else}
      <p class="muted">No value set.</p>
    {/if}
  </div>
{:else}
  <!-- Multiple states. The narrowing picker writes `?variant`/`?value_set_version`,
       which the server only honors WITH a `?period` (it 422s them otherwise), so
       it's shown only when a period is active (`narrowed`). Without a period the
       list is the full state history — set a period to narrow to one. -->
  {#if narrowed && canNarrow}
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
    {#if versionsAll.length > 1}
      <fieldset class="picker">
        <legend>Value-set version</legend>
        <div class="chips">
          {#each versionChips as version (version)}
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
          {#if hasEmptyVersion}
            <!-- The empty/default-label state — selectable via the `_none`
                 sentinel (an empty `?value_set_version` can't ride in the URL). -->
            <button
              type="button"
              class="chip"
              class:active={activeValueSetVersion === VALUE_SET_VERSION_NONE}
              aria-pressed={activeValueSetVersion === VALUE_SET_VERSION_NONE}
              onclick={() => onpickValueSetVersion(VALUE_SET_VERSION_NONE)}
            >
              (no version)
            </button>
          {/if}
        </div>
      </fieldset>
    {/if}
  {:else if narrowed}
    <!-- A period that resolves to several states which share one variant AND one
         value-set version (e.g. a RANGE crossing validity windows): no narrowing
         axis can pick one, so don't promise "narrow to a single state". -->
    <p class="muted picker-hint">
      {states.length} overlapping states at this period — narrow to a single
      point period to resolve to one.
    </p>
  {:else}
    <p class="muted picker-hint">
      {states.length} states over time. Set a period above to resolve to one.
    </p>
  {/if}

  <ul class="state-list">
    {#each states as s (s.state_id)}
      {@const typeLabel = formatDataType(s.data_type, s.data_length)}
      {@const hints = changeHints.get(s.state_id)}
      <li>
        <div class="state-row">
          <span class="state-variant"><code>{s.variant}</code></span>
          <!-- #309/#321: sentinel-free, coarsest-exact window; raw ISO on the
               tooltip. -->
          <span class="state-validity muted" title="{s.valid_from} – {s.valid_to}">
            {formatStateWindow(s)}
          </span>
          {#if typeLabel}
            <!-- #309: every diffed field must be VISIBLE — states can differ
                 by data type alone (int → bigint). -->
            <span class="state-type muted">{typeLabel}</span>
          {/if}
          <span class="state-vsv muted">
            {s.value_set_version_label || "(no version)"}
          </span>
          {#if s.delivery_column_name}
            <code class="state-col">{s.delivery_column_name}</code>
          {/if}
          {#if s.value_set && s.value_set.length > 0}
            <!-- #310: inspect a state's codes WITHOUT narrowing to one state. -->
            <button
              type="button"
              class="vs-toggle"
              aria-expanded={!!expanded[s.state_id]}
              onclick={() => toggleExpanded(s.state_id)}
            >
              {expanded[s.state_id] ? "Hide values" : "Values"} ({s.value_set.length})
            </button>
          {/if}
        </div>
        {#if hints && hints.length > 0}
          <!-- #309: what actually changed vs the previous state of this
               variant — the int→bigint case renders an explicit diff. -->
          <p class="state-changed">changed: {hints.join(" · ")}</p>
        {/if}
        {#if expanded[s.state_id] && s.value_set && s.value_set.length > 0}
          {@render valueSetTable(s.value_set)}
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
    flex-direction: column;
    gap: 0.35rem;
    padding: 0.35rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  .state-row {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.75rem;
  }
  .state-col {
    margin-left: auto;
    font-size: 0.85em;
  }
  .vs-toggle {
    font: inherit;
    font-size: 0.75rem;
    padding: 0.1rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--surface);
    color: var(--accent);
    cursor: pointer;
  }
  .vs-toggle:hover {
    border-color: var(--accent);
  }
  /* #309: the per-transition diff line — advisory amber, not an error. */
  .state-changed {
    margin: 0;
    font-size: 0.8rem;
    color: var(--level-warning, #92600a);
  }
</style>
