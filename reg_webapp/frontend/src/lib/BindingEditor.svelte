<script lang="ts">
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { Button } from "./ui";
import { bindingAnchorId } from "./validation";

// READ-ONLY binding row in the #991 data-order cart: it DISPLAYS the picked
// variable (+ the pinned representation when set) and offers "Remove binding" only.
// Editing a binding — variable, type, display_name, representation — happens by
// re-picking in the catalog browser (the cart shows the cart, it doesn't edit it).
// See reg_webapp/DESIGN.md and issue #991.
const { sourceIndex, bindingIndex, binding } = $props<{
  sourceIndex: number;
  bindingIndex: number;
  binding: Binding;
}>();

// A binding field coerced to a display string (non-string → "").
function strField(field: keyof Binding): string {
  const v = binding[field];
  return typeof v === "string" ? v : "";
}
const variable = $derived(strField("variable"));
const representation = $derived(strField("representation"));
</script>

<!-- `id` is the click-to-locate anchor the ValidationPanel scrolls to (matched via
     `bindingAnchorId`); `.locate-flash` (defined globally in SourceEditor) briefly
     highlights it. -->
<div class="binding" id={bindingAnchorId(sourceIndex, bindingIndex)}>
  <div class="binding-body">
    <!-- The variable + its pinned representation are machine FQIDs/identifiers →
         mono, like every code/identifier (DESIGN.md). -->
    <code class="variable-value">{variable || "(no variable)"}</code>
    {#if representation}
      <code class="representation" title="Pinned delivery column">{representation}</code>
    {/if}
  </div>
  <!-- Per-binding accessible name so a screen-reader controls list disambiguates
       the delete buttons (visible text kept as the label prefix — label-in-name). -->
  <Button
    variant="danger"
    size="sm"
    aria-label={`Remove binding ${variable || "(no variable)"}`}
    onclick={() => projectStore.removeBinding(sourceIndex, bindingIndex)}
  >
    Remove binding
  </Button>
</div>

<style>
  .binding {
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    padding: var(--space-2) var(--space-3);
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    scroll-margin-top: var(--space-4);
  }
  .binding-body {
    display: flex;
    align-items: baseline;
    flex-wrap: wrap;
    gap: var(--space-2);
    min-width: 0;
  }
  /* The variable is a machine FQID — mono, like every code/identifier. As flex
     items of `.binding-body` they default to `min-width: auto`, so a long unbroken
     FQID would refuse to shrink and overflow the card on mobile; `min-width: 0` +
     `overflow-wrap: anywhere` lets it break within the row instead (#1110). */
  .variable-value {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    min-width: 0;
    overflow-wrap: anywhere;
  }
  /* The pinned representation (delivery column) is a subtler mono chip. */
  .representation {
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    color: var(--text-muted);
    padding: 0.05rem var(--space-2);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    min-width: 0;
    overflow-wrap: anywhere;
  }
</style>
