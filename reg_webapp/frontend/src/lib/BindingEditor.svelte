<script lang="ts">
import { catalogHref } from "./catalog";
import { columnNames, UNASKED } from "./catalog_names.svelte";
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { Button } from "./ui";
import { bindingAnchorId } from "./validation";

// READ-ONLY column row in the #991 data-order cart: it DISPLAYS the delivery
// COLUMN this binding orders and the variable it came from, and offers "Remove
// column" only. Editing a binding — variable, type, display_name, representation —
// happens by re-picking in the catalog browser (the cart shows the cart, it doesn't
// edit it). See reg_webapp/DESIGN.md and issue #991.
const { sourceIndex, bindingIndex, binding, variant, period } = $props<{
  sourceIndex: number;
  bindingIndex: number;
  binding: Binding;
  /** The owning source's concrete variant slug and its stored period (wire form,
   * null when it has none) — where this column's default name is resolved. */
  variant: string;
  period: string | null;
}>();

// A binding field coerced to a display string (non-string → "").
function strField(field: keyof Binding): string {
  const v = binding[field];
  return typeof v === "string" ? v : "";
}
const variable = $derived(strField("variable"));

// The row LEADS with the delivery column — that is what the researcher ordered and
// what lands in the extract. Where the FILE names it, the file wins: an explicit
// `display_name` first (reg_schema makes it the binding's OUTPUT column name, so it
// wins even over a pinned `representation`), then the `representation` a pick pins
// when it chose between co-existing columns.
//
// Carrying NEITHER is the ORDINARY case, not a broken one — a pick writes neither
// (`representation: null` when the variable resolves to one column, issue #992),
// and neither does a project_data.json authored outside this app. That column's name
// is the reg_meta default at the source's (variant, period), which lives only in the
// CATALOG — so the row resolves it there, through the same leaf resolve the picker
// runs on a pick, cached per (fqid, period, variant) so a hundred-column cart asks
// once. Nothing resolvable (no period, an unreachable backend, a coordinate outside
// this steward's catalog) keeps today's answer: the FQID it was picked from, never
// an invented placeholder. Nothing here is written back to the draft.
const fileColumn = $derived(
  strField("display_name") || strField("representation"),
);
const resolved = $derived(
  fileColumn ? UNASKED : columnNames(variable, period, variant),
);
const resolvedNames = $derived(resolved.value ?? []);
const columnName = $derived(fileColumn || resolvedNames[0] || "");
// A column RENAMED within the source's period resolves to several names: the
// current one leads, and the superseded ones trail it OLDEST-first so the hint
// reads as a progression — the order the picker's own "was X, Y" hint uses.
const supersededColumns = $derived(resolvedNames.slice(1).reverse());
// What the delete button calls this row: the column it shows, else the variable.
const columnLabel = $derived(columnName || variable);
</script>

<!-- `id` is the click-to-locate anchor the ValidationPanel scrolls to (matched via
     `bindingAnchorId`); `.locate-flash` (defined globally in SourceEditor) briefly
     highlights it. -->
<div class="binding" id={bindingAnchorId(sourceIndex, bindingIndex)}>
  <!-- `aria-busy` while the column's catalog name is in flight: the row is showing
       its FQID alone as a stand-in, and the screenshot driver waits on it. -->
  <div class="binding-body" aria-busy={resolved.loading ? "true" : undefined}>
    {#if columnName}
      <span class="column-name">{columnName}</span>
    {/if}
    <!-- The superseded names of a rename — quiet, after the current one, as the
         picker's own rows say it. The columns are identifiers and take the machine
         face; the word that introduces them is copy and does not. -->
    {#if supersededColumns.length > 0}
      <span class="rename-hint"
        >was <span class="superseded">{supersededColumns.join(", ")}</span></span
      >
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
  /* The superseded delivery columns of a rename: quiet — they name the same column
     under an earlier name, not another column to order — with the columns themselves
     in mono, as the picker's own rename hint sets them. */
  .rename-hint {
    font-size: var(--text-sm);
    color: var(--text-muted);
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .superseded {
    font-family: var(--font-mono);
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
