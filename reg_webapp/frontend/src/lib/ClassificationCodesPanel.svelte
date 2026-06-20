<script lang="ts">
import type { ClassificationNodeData } from "./api";
import { matchesFilter } from "./catalog";
import FilterInput from "./FilterInput.svelte";

// The classification-leaf value-set / code viewer (#609) — the section a user
// drilling into a standard ("Utbildningsnivå") reads to see and SEARCH its codes.
// The node EMBEDS the resolved edition's codes (`codes`, code-ordered), so the
// panel renders SYNCHRONOUSLY — no fetch (mirrors ClassificationLineagePanels'
// embedded `edition_chain`). The codes are PUBLIC classification codes, not
// row-level data. Codes are per-edition: this list is the VIEWED edition's only;
// other editions are reached via the edition-chain panel (each loads its own).
//
// Omit-when-empty (the LineagePanels ethos): no codes → no section at all. Since
// the codes arrive embedded (resolved with the node), "empty" is a confirmed
// absence, not an unknown-loading state, so there is no loading/error arm here.
let { node }: { node: ClassificationNodeData } = $props();

// Tolerate the optional wire field's absence on a stale edge-cache payload —
// degrade to empty rather than crash (mirrors edition_chain's `?? []`).
const codes = $derived(node.codes ?? []);

// In-memory type-to-filter over code + label (matchesFilter folds diacritics and
// treats an empty needle as match-all — the unfiltered full list). Reset on
// navigation so a new edition opens unfiltered.
let filter = $state("");
$effect(() => {
  void node.fqid; // navigation key — clear the filter when the leaf changes.
  filter = "";
});
const shown = $derived(
  codes.filter((c) => matchesFilter(filter, c.code, c.label)),
);
</script>

{#if codes.length > 0}
  <section aria-labelledby="cls-codes-heading" class="cls-codes">
    <h3 id="cls-codes-heading">Codes</h3>

    <FilterInput
      bind:value={filter}
      total={codes.length}
      shown={shown.length}
      placeholder="Filter codes…"
      label="Filter codes"
    />

    {#if shown.length > 0}
      <ul class="codes">
        {#each shown as code, i (i)}
          <li class="code-row" class:observed={code.is_valid === false}>
            <code class="code-key">{code.code}</code>
            <span class="code-label">{code.label}</span>
            {#if code.is_valid === false}
              <!-- Observed-only: seen in data but not in the canonical list. -->
              <span class="muted tag" title="Observed in data, not in the canonical list">
                observed
              </span>
            {/if}
          </li>
        {/each}
      </ul>
    {:else}
      <p class="muted">No codes match “{filter}”.</p>
    {/if}
  </section>
{/if}

<style>
  .cls-codes {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .muted {
    color: var(--muted);
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
    color: var(--muted);
    font-size: 0.9em;
  }
  .code-label {
    flex: 1;
  }
  /* Observed-only codes are de-emphasised — the canonical list is the primary
     signal; observed codes are supplementary. */
  .code-row.observed .code-label {
    color: var(--muted);
  }
  .tag {
    font-size: 0.8em;
    font-style: italic;
  }
</style>
