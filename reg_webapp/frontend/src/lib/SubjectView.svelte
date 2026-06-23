<script lang="ts">
import type { Snippet } from "svelte";

// The unified catalog-SUBJECT shell (#638 PR1): the single layout every catalog
// leaf kind (variable / classification / concept-group) renders through, so the
// three pages share ONE article wrapper, ONE title/fqid header, and ONE canonical
// section order. A thin PRESENTATIONAL component — it owns NO data, NO headings of
// its own beyond the title, and NO restyling: each section's existing markup +
// headings arrive as a snippet from the leaf view, and the shell just renders them
// in the fixed order. The sections are all OPTIONAL — a leaf that has no value set
// (a concept group) simply doesn't pass `valueSet`, and the slot renders nothing
// (no empty wrapper). The canonical order is the contract: description → picker →
// value set → relationships → docs.
let {
  title,
  fqid,
  showFqid = true,
  description,
  picker,
  valueSet,
  relationships,
  docs,
}: {
  title: string;
  // Absent for a concept group (a group has no single fqid — its key shows inside
  // the description meta block instead); present for variable + classification.
  fqid?: string;
  // #670: opt OUT of the under-header fqid line (the binding leaf passes false —
  // its breadcrumb already ends in the slug, so the line is redundant there). The
  // classification leaf keeps the default `true` (its breadcrumb shows the class
  // axis, not the leaf slug). A no-op when `fqid` is absent.
  showFqid?: boolean;
  description?: Snippet;
  picker?: Snippet;
  valueSet?: Snippet;
  relationships?: Snippet;
  docs?: Snippet;
} = $props();
</script>

<article>
  <h2>{title}</h2>
  {#if fqid && showFqid}
    <p class="fqid"><code>{fqid}</code></p>
  {/if}
  {@render description?.()}
  {@render picker?.()}
  {@render valueSet?.()}
  {@render relationships?.()}
  {@render docs?.()}
</article>

<style>
  .fqid {
    margin-top: -0.25rem;
    color: var(--muted);
  }
</style>
