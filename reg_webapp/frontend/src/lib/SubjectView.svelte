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
// value set → relationships → docs → technical.
let {
  title,
  fqid,
  showFqid = true,
  description,
  picker,
  valueSet,
  relationships,
  docs,
  technical,
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
  technical?: Snippet;
} = $props();
</script>

<article>
  <header class="subject-header">
    <h2>{title}</h2>
    {#if fqid && showFqid}
      <p class="fqid"><code>{fqid}</code></p>
    {/if}
  </header>
  {@render description?.()}
  {@render picker?.()}
  {@render valueSet?.()}
  {@render relationships?.()}
  {@render docs?.()}
  {@render technical?.()}
</article>

<style>
  .subject-header {
    margin-bottom: var(--space-4);
  }
  .subject-header h2 {
    margin: 0;
    font-size: var(--text-h1);
    line-height: 1.2;
  }
  /* The fqid identifier line under the title — a mono machine identifier, muted. */
  .fqid {
    margin: var(--space-1) 0 0;
    color: var(--text-muted);
  }
  .fqid code {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
  }
</style>
