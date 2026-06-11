<script lang="ts">
// Shared type-to-filter input for every catalog/authoring list surface (browse
// pages + the embedded pickers). It owns ONLY the input + the result-count label;
// the parent owns the list data and does the actual filtering with
// `matchesFilter` (catalog.ts) so the matcher is single-sourced. `value` is
// bindable so the parent's $derived filtered list stays the source of truth.
//
// `shown`/`total` drive the "12 of 740" count — shown only while filtering (a
// non-empty query) so the unfiltered list reads as the plain full list.

interface Props {
  value: string;
  total: number;
  shown: number;
  placeholder?: string;
  // Autofocus on mount — the pickers want the cursor in the filter the moment
  // they open (it's the single worst authoring blocker without it). Browse pages
  // leave it off so a page load doesn't steal focus / scroll.
  autofocus?: boolean;
  label?: string;
}
let {
  value = $bindable(),
  total,
  shown,
  placeholder = "Filter…",
  autofocus = false,
  label = "Filter list",
}: Props = $props();

const filtering = $derived(value.trim().length > 0);
</script>

<div class="filter">
  <input
    type="text"
    class="filter-input"
    {placeholder}
    aria-label={label}
    autocomplete="off"
    bind:value
    {@attach (el) => {
      if (autofocus) {
        (el as HTMLInputElement).focus();
      }
    }}
  />
  {#if filtering}
    <span class="filter-count" aria-live="polite">{shown} of {total}</span>
  {/if}
</div>

<style>
  .filter {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    margin-bottom: 0.6rem;
  }
  .filter-input {
    flex: 1;
    font: inherit;
    padding: 0.35rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
  }
  .filter-count {
    color: var(--muted);
    font-size: 0.85rem;
    white-space: nowrap;
  }
</style>
