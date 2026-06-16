<script lang="ts">
import { getBindingDimensions } from "./api";
import { asyncResource } from "./async.svelte";
import ConceptGroupRow from "./ConceptGroupRow.svelte";

// The binding-leaf "Variants / dimensions" panel (#489): the concept-group
// dimension memberships ("pick your variant" facet groups — level / population /
// rank / …) that contain this variable. A SIBLING of LineagePanels — a separate
// component over its own fetch (`/dimensions`), so it's an independent FAILURE
// DOMAIN: a dimensions fetch error / timeout never blanks or wedges the leaf
// (mirrors DocMentionsPanel's rationale). Each group renders via the shared
// browse `ConceptGroupRow` (no `onpick` → members are catalog links); the current
// variable appears as one member of its group.
//
// Omit-when-empty (the LineagePanels ethos): the WHOLE section is omitted when
// the variable is in no group — but NOT while still loading or on error (we never
// hide a section whose state is unknown, which would read as a confirmed absence).
let { fqidPath }: { fqidPath: string } = $props();

// Read `fqidPath` synchronously inside `fn` so the resource refetches when the
// leaf changes (same pattern as LineagePanels' predecessors resource).
const resource = asyncResource(() => getBindingDimensions(fqidPath));

const groups = $derived(resource.data?.dimensions ?? []);
// Show the section while loading / on error / when it has groups; omit it only
// once we KNOW it's empty (resolved with no groups).
const show = $derived(
  resource.loading || !!resource.error || groups.length > 0,
);
</script>

{#if show}
  <section aria-labelledby="dimensions-heading">
    <h3 id="dimensions-heading">Variants / dimensions</h3>

    {#if resource.loading}
      <p class="muted" aria-busy="true">Loading…</p>
    {:else if resource.error}
      <!-- Any dimensions failure stays INLINE — it never blanks the leaf. -->
      <p class="error" role="alert">
        Failed to load dimensions: {resource.error}
      </p>
    {:else}
      <div class="dimension-groups">
        {#each groups as group (group.key)}
          <ConceptGroupRow {group} />
        {/each}
      </div>
    {/if}
  </section>
{/if}

<style>
  section {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .dimension-groups {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .muted {
    color: var(--muted);
  }
</style>
