<script lang="ts">
import { getRegisterVariants } from "./api";
import { asyncResource } from "./async.svelte";

// The variant axis is a register SUB-RESOURCE (NOT an FQID path segment; see
// reg_meta/DESIGN.md → Two-level variable model). A5.3a DISPLAYS the variants for
// a register; the selection + the
// period/state resolution that consumes `?variant` is A5.3b (which makes these
// interactive and wires them to `resolve_at`).
const { registerFqid }: { registerFqid: string } = $props();

const variants = asyncResource(() => getRegisterVariants(registerFqid));

// `display_group` duplicates `name` for most variants (SCB delivers them
// identical), so show it only when it ADDS information. Compare trimmed: some
// source rows carry trailing-whitespace noise on one side ("…AGI/KU " vs
// "…AGI/KU") that a strict !== would treat as a difference, re-printing the name.
function showsDistinctGroup(
  name: string | null | undefined,
  group: string | null | undefined,
): boolean {
  return !!group && group.trim() !== (name ?? "").trim();
}
</script>

<section class="variants" aria-labelledby="variants-heading">
  <h3 id="variants-heading">Variants</h3>
  {#if variants.loading}
    <p class="muted" aria-busy="true">Loading variants…</p>
  {:else if variants.error}
    <p class="error" role="alert">Failed to load variants: {variants.error}</p>
  {:else if variants.data && variants.data.variants.length > 0}
    <ul class="variant-list">
      {#each variants.data.variants as variant (variant.slug)}
        <li>
          <div class="variant">
            <span class="slug">{variant.slug}</span>
            {#if variant.name}<span class="name">{variant.name}</span>{/if}
            <!-- Omit display_group when it just repeats `name` (the common case;
                 "Arbetsställen Arbetsställen") — trimmed compare, see the helper. -->
            {#if showsDistinctGroup(variant.name, variant.display_group)}
              <span class="group">{variant.display_group}</span>
            {/if}
          </div>
          {#if variant.description}
            <p class="desc muted">{variant.description}</p>
          {/if}
        </li>
      {/each}
    </ul>
  {:else}
    <p class="muted">No variants.</p>
  {/if}
</section>

<style>
  .variants {
    margin-top: 1.5rem;
  }
  .variant-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .variant {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    padding: 0.4rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
  }
  .slug {
    font-family: ui-monospace, monospace;
    font-weight: 600;
  }
  .group {
    margin-left: auto;
    color: var(--muted);
    font-size: 0.85em;
  }
  .desc {
    margin: 0.2rem 0 0 0.6rem;
    font-size: 0.9em;
  }
</style>
