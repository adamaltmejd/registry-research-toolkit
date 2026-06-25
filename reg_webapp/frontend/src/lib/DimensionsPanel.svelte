<script lang="ts">
import type { ConceptGroup } from "./api";
import ConceptGroupRow from "./ConceptGroupRow.svelte";

// The binding-leaf "Variants / dimensions" panel (#489): the concept-group
// dimension memberships ("pick your variant" facet groups — level / population /
// rank / …) that contain this variable. Each group renders via the shared browse
// `ConceptGroupRow` (no `onpick` → members are catalog links); the current
// variable appears as one member of its group.
//
// PRESENTATIONAL (#670): the `/dimensions` fetch is now OWNED by the parent
// BindingLeafView (which also derives the header qualifier + group link from it),
// so this panel receives the resolved `groups` + `loading` + `error` as props —
// ONE shared fetch, no duplicate `/dimensions` request. The FAILURE-DOMAIN
// isolation is unchanged: the parent's dimensions resource is independent of the
// leaf node, so an error here renders this section's inline alert WITHOUT blanking
// the leaf (mirrors DocMentionsPanel's rationale).
//
// Omit-when-empty: the WHOLE section is omitted when
// the variable is in no group — but NOT while still loading or on error (we never
// hide a section whose state is unknown, which would read as a confirmed absence).
let {
  groups,
  loading,
  error,
}: {
  groups: ConceptGroup[];
  loading: boolean;
  error: string | null;
} = $props();

// Show the section while loading / on error / when it has groups; omit it only
// once we KNOW it's empty (resolved with no groups).
const show = $derived(loading || !!error || groups.length > 0);
</script>

{#if show}
  <section aria-labelledby="dimensions-heading">
    <h3 id="dimensions-heading">Variants / dimensions</h3>

    {#if loading}
      <p class="muted" aria-busy="true">Loading…</p>
    {:else if error}
      <!-- Any dimensions failure stays INLINE — it never blanks the leaf. -->
      <p class="error" role="alert">
        Failed to load dimensions: {error}
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
