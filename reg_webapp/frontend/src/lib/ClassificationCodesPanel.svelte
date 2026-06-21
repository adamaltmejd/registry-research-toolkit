<script lang="ts">
import type { ClassificationNodeData } from "./api";
import CodeList from "./CodeList.svelte";

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
//
// The list itself is the shared CodeList (#638 PR3) — the same viewer the variable
// value set uses. CodeList owns the size-dependent filter + the navigation reset.
let { node }: { node: ClassificationNodeData } = $props();

// Tolerate the optional wire field's absence on a stale edge-cache payload —
// degrade to empty rather than crash (mirrors edition_chain's `?? []`).
const codes = $derived(node.codes ?? []);
</script>

{#if codes.length > 0}
  <section aria-labelledby="cls-codes-heading" class="cls-codes">
    <h3 id="cls-codes-heading">Codes</h3>
    <CodeList {codes} filterLabel="Filter codes" filterPlaceholder="Filter codes…" />
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
</style>
