<script lang="ts">
import { matchesFilter } from "./catalog";
import FilterInput from "./FilterInput.svelte";

// The UNIFIED value-set / code viewer (#638 PR3). A variable's value set and a
// classification's code list are the same thing — a code→label set (a value set
// often IS a classification) — so they render IDENTICALLY here: the
// classification-list style (a <ul> of code rows), used for BOTH.
//
// Size-dependent filter (the maintainer's call): sizes vary wildly on both sides
// (tiny classifications, huge LISA value sets), so the search box appears only once
// a set is big enough to be worth filtering — hidden below the threshold where it'd
// be pointless. Large lists scroll in a height-constrained container so hundreds of
// codes stay bounded.
//
// Defensive empty-guard only — callers already omit the surrounding section when
// the set is empty; this just never crashes on `[]`.

// A code→label set member. Covers BOTH shapes: classification codes and variable
// value-set members.
interface Code {
  code: string;
  label: string;
  is_valid?: boolean | null;
}

let {
  codes,
  filterLabel = "Filter codes",
  filterPlaceholder = "Filter codes…",
}: {
  codes: Code[];
  filterLabel?: string;
  filterPlaceholder?: string;
} = $props();

// Below this many codes the filter box is hidden — per the maintainer: pointless
// for a handful of items (a small classification or short value set). At or above
// it, the FilterInput appears.
const CODE_FILTER_THRESHOLD = 5;
const showFilter = $derived(codes.length >= CODE_FILTER_THRESHOLD);

// In-memory type-to-filter over code + label (matchesFilter folds diacritics and
// treats an empty needle as match-all — the unfiltered full list). Reset when the
// `codes` prop changes (navigation / state switch) so a new set opens unfiltered.
let filter = $state("");
$effect(() => {
  void codes;
  filter = "";
});
const shown = $derived(
  codes.filter((c) => matchesFilter(filter, c.code, c.label)),
);
</script>

{#if codes.length > 0}
  {#if showFilter}
    <FilterInput
      bind:value={filter}
      total={codes.length}
      shown={shown.length}
      placeholder={filterPlaceholder}
      label={filterLabel}
    />
  {/if}

  {#if shown.length > 0}
    <div class="code-scroll">
      <ul class="codes">
        {#each shown as code, i (i)}
          <li class="code-row">
            <code class="code-key">{code.code}</code>
            <span class="code-label">{code.label}</span>
          </li>
        {/each}
      </ul>
    </div>
  {:else}
    <p class="muted">No codes match “{filter}”.</p>
  {/if}
{/if}

<style>
  .muted {
    color: var(--text-muted);
  }
  /* Height-constrained so large lists (LISA value sets run to hundreds of codes)
     stay bounded — the variable table's former `.value-set-scroll` idiom, now the
     shared scroll for both contexts. */
  .code-scroll {
    max-height: 18rem;
    overflow-y: auto;
  }
  .codes {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .code-row {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.2rem 0;
  }
  .code-key {
    flex: 0 0 auto;
    min-width: 3.5rem;
    /* A value-set code — a machine identifier, so mono-faced (DESIGN.md). */
    font-family: var(--font-mono);
    color: var(--text-muted);
    font-size: 0.9em;
  }
  .code-label {
    flex: 1;
  }
</style>
