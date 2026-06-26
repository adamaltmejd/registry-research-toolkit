<script lang="ts">
import type { ClassificationNodeData } from "./api";
import ConceptGroupRow from "./ConceptGroupRow.svelte";

// The classification-leaf "Related granularities / dimensions" panel (#609) — the
// niva ↔ aggregate granularity cross-reference (#585/#608). The node EMBEDS the
// curated umbrella group(s) this edition belongs to (`dimensions`), so the panel
// renders SYNCHRONOUSLY — no fetch (mirrors the embedded `edition_chain`/`codes`).
// Each group renders via the shared browse `ConceptGroupRow` (no `onpick` → members
// are catalog links), so "Utbildningsnivå — also published as a 7-level / 5-level
// aggregate" surfaces as a sibling-edition list. This is the classification dual of
// the binding-leaf DimensionsPanel.
//
// Omit-when-empty (the LineagePanels ethos): a classification in no umbrella group
// (the common case) shows nothing. The codes arrive embedded, so "empty" is a
// confirmed absence — no loading/error arm.
let { node }: { node: ClassificationNodeData } = $props();

// Tolerate the optional wire field's absence on a stale edge-cache payload.
const dimensions = $derived(node.dimensions ?? []);
</script>

{#if dimensions.length > 0}
  <section aria-labelledby="cls-dimensions-heading" class="cls-dimensions">
    <h3 id="cls-dimensions-heading">Related granularities</h3>
    <p class="muted note">
      Also published at other granularities — pick a sibling to view its codes.
    </p>
    <div class="dimension-groups">
      {#each dimensions as group (group.key)}
        <ConceptGroupRow {group} noun="granularities" />
      {/each}
    </div>
  </section>
{/if}

<style>
  .cls-dimensions {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .note {
    margin: 0 0 0.5rem;
    font-size: 0.85em;
  }
  .muted {
    color: var(--text-muted);
  }
  .dimension-groups {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
</style>
