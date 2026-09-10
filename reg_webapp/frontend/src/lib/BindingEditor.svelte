<script lang="ts">
import { catalogHref } from "./catalog";
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { Button } from "./ui";
import { bindingAnchorId } from "./validation";

// READ-ONLY column row in the #991 data-order cart: it DISPLAYS the delivery
// COLUMN this binding orders and the variable it came from, and offers "Remove
// column" only. Editing a binding — variable, type, display_name, representation —
// happens by re-picking in the catalog browser (the cart shows the cart, it doesn't
// edit it). See reg_webapp/DESIGN.md and issue #991.
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

// The row LEADS with the delivery column, because that is what the researcher
// ordered and what lands in the extract: the pinned `representation` when the pick
// chose between co-existing columns, else the `display_name` the picker resolved
// and wrote at pick time (`bindingFieldsFromResolution` — the column the catalog
// says this variable is delivered as). A binding carrying NEITHER is not broken —
// a project_data.json authored outside this app has neither field and still orders
// — it just has no column name of its own, so the FQID it was picked from leads
// instead of a placeholder. Stamping a name here is not this row's job.
const columnName = $derived(
  strField("representation") || strField("display_name"),
);
// What the delete button calls this row: whichever of the two the row shows.
const columnLabel = $derived(columnName || variable);
</script>

<!-- `id` is the click-to-locate anchor the ValidationPanel scrolls to (matched via
     `bindingAnchorId`); `.locate-flash` (defined globally in SourceEditor) briefly
     highlights it. -->
<div class="binding" id={bindingAnchorId(sourceIndex, bindingIndex)}>
  <div class="binding-body">
    {#if columnName}
      <span class="column-name">{columnName}</span>
    {/if}
    <!-- The variable is a machine FQID → mono and demoted under the column name (the
         row's own subject when there is none), linked to its catalog subject page:
         the cart is read-only, so re-picking or checking a column happens there
         (#991). A binding with no variable has no subject page to send anyone to, so
         it stays plain text. -->
    {#if variable}
      <a class="variable-value" href={catalogHref(variable)}>{variable}</a>
    {:else}
      <span class="variable-value">(no variable)</span>
    {/if}
  </div>
  <!-- Per-column accessible name so a screen-reader controls list disambiguates
       the delete buttons (visible text kept as the label prefix — label-in-name).
       A row showing neither name nor variable has nothing to disambiguate WITH, so
       it keeps the visible text alone. -->
  <Button
    variant="danger"
    size="sm"
    aria-label={columnLabel ? `Remove column ${columnLabel}` : undefined}
    onclick={() => projectStore.removeBinding(sourceIndex, bindingIndex)}
  >
    Remove column
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
    gap: var(--space-1) var(--space-2);
    min-width: 0;
  }
  /* The delivery column NAME is the row's subject — UI face, not mono: it is the
     column the extract delivers, read as a word, not a coordinate. */
  .column-name {
    font-weight: 600;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  /* The variable is a machine FQID — mono, like every code/identifier, and small
     because it is where the column came from, not the subject of the row. Its link
     color + hover underline come from the app-wide `a` role rule. As flex items of
     `.binding-body` they default to `min-width: auto`, so a long unbroken FQID would
     refuse to shrink and overflow the card on mobile; `min-width: 0` +
     `overflow-wrap: anywhere` lets it break within the row instead (#1110). */
  .variable-value {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    min-width: 0;
    overflow-wrap: anywhere;
  }
  /* A link is ink at weight 600 (DESIGN.md → Color): at rest this row sits beside
     mono metadata that is NOT interactive, and the shared `a` rule paints ink
     without underlining until hover, so weight is what says "this goes somewhere".
     The `(no variable)` span shares the face but not the weight — it goes nowhere. */
  a.variable-value {
    font-weight: 600;
  }
  .variable-value:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
</style>
