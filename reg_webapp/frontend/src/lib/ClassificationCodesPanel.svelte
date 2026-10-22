<script lang="ts">
import type { ClassificationShow } from "./api";
import ValueSetCodes from "./ValueSetCodes.svelte";

// The classification-leaf value-set / code viewer (#609) — the section a user
// drilling into a standard ("Utbildningsnivå") reads to see and SEARCH its codes.
// The codes are the edition's `values` facet, read a page at a time with a
// server-side filter by the shared ValueSetCodes (the same viewer a variable's
// value set uses). The codes are PUBLIC classification codes, not row-level data.
// Codes are per-edition: this list is the VIEWED edition's only.
//
// The section always renders: the edition's code count is unknown until the
// first page answers (`codeCount` null), and ValueSetCodes says so itself when
// the edition has no codes.
let { node }: { node: ClassificationShow } = $props();
</script>

<section aria-labelledby="cls-codes-heading" class="cls-codes">
  <h2 id="cls-codes-heading">Codes</h2>
  <ValueSetCodes
    ref={node.fqid}
    codeCount={null}
    filterLabel="Filter codes"
    filterPlaceholder="Filter codes…"
    emptyText="This classification has no codes."
  />
</section>

<style>
  .cls-codes {
    margin-top: 1.5rem;
  }
  h2 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
    font-weight: var(--heading-weight);
  }
  /* A section heading's size is its level's token, not the browser default
     for the tag. */
  h2 {
    font-size: var(--text-h2);
  }
</style>
