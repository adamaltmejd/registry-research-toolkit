<script lang="ts">
import BindingEditor from "./BindingEditor.svelte";
import { variantDisplayLabel } from "./catalog";
import { periodToWire } from "./period";
import { isPlainObject, type Period, type Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { Button, EmptyState, KeyValue, type KeyValueRow, Tag } from "./ui";
import {
  issuesUnderPointer,
  jsonPointer,
  sourceAnchorId,
  type ValidationIssue,
} from "./validation";

// READ-ONLY source card in the #991 data-order cart: the cart SHOWS what has been
// picked (sources + bindings) and supports delete + navigate-out only — adding or
// changing data happens in the catalog browser, not here. So this card DISPLAYS the
// source's name / register_variant / period and offers "Remove source"; it carries
// no inputs, pickers, or PeriodEditor. The header still rolls up all errors under
// `/sources/{i}` as a badge (fixes are reached via the ValidationPanel's catalog
// link). See reg_webapp/DESIGN.md and issue #991.
const { sourceIndex, source, issues } = $props<{
  sourceIndex: number;
  source: Source;
  issues: ValidationIssue[];
}>();

const sourcePtr = $derived(jsonPointer(["sources", sourceIndex]));
const rolledUp = $derived(issuesUnderPointer(issues, sourcePtr));
const errorCount = $derived(rolledUp.filter((i) => i.level === "error").length);

// Defensive: a malformed opened spec may carry a null/non-object SLOT in `sources`
// (e.g. `sources: [null, {...}]`). The draft is loaded VERBATIM (the backend
// diagnoses structure; see reg_webapp/DESIGN.md → Pydantic boundary), so this card
// must render a degraded fallback instead of crashing on `source.<field>`. It stays
// counted (its `sourceIndex` / `/sources/{i}` addressing must still line up), so the
// field derefs below use `source?.` to never throw, and the template branches on
// `sourceMalformed` — the same full-replace-with-guards doctrine as `bindingsMalformed`.
const sourceMalformed = $derived(!isPlainObject(source));
const sourceLabel = $derived(
  sourceMalformed ? "(malformed source)" : source?.name || "(unnamed source)",
);

const registerVariant = $derived(
  typeof source?.register_variant === "string" ? source.register_variant : "",
);
const registerVariantLabel = $derived(variantDisplayLabel(registerVariant));

// Defensive: a malformed opened spec may carry `bindings` as a non-array. Show an
// inline note instead of the list (full-replace-with-guards, maintainer decision)
// rather than crashing — the draft stays verbatim for serialize/validate.
const bindings = $derived(
  Array.isArray(source?.bindings) ? source.bindings : [],
);
const bindingsMalformed = $derived(
  source?.bindings !== undefined && !Array.isArray(source?.bindings),
);

// The period as a read-only display string (list-period aware — `periodToWire`
// already joins list segments); null → the "(no period)" fallback.
const periodDisplay = $derived(periodToWire(source?.period as Period));

// The read-only coordinate rows, rendered through the shared KeyValue primitive
// (#804) — same metadata-row styling ProjectEditor uses. The register_variant is a
// machine FQID coordinate (mono), routed through `variantDisplayLabel` so #376's
// variant-family labels swap in at one seam.
const metaRows = $derived([
  {
    label: "Register variant",
    value: registerVariantLabel,
    mono: registerVariantLabel === registerVariant,
  },
  { label: "Period", value: periodDisplay ?? "(no period)" },
] satisfies KeyValueRow[]);
</script>

<!-- `id` is the click-to-locate anchor the ValidationPanel scrolls to (matched via
     `sourceAnchorId`). `.locate-flash` (toggled by the panel on the element) briefly
     highlights the card. -->
<section
  class="source"
  id={sourceAnchorId(sourceIndex)}
  aria-label="Source {sourceIndex + 1}"
>
  <header class="source-head">
    <h3>
      {sourceLabel}
      {#if errorCount > 0}
        <!-- Status badge: cool error tone + ✕ glyph (aria-hidden); the count text
             carries the meaning for assistive tech (DESIGN.md accent-vs-status). -->
        <Tag tone="error">
          {#snippet glyph()}✕{/snippet}
          {errorCount} error{errorCount === 1 ? "" : "s"}
        </Tag>
      {/if}
    </h3>
    <!-- Per-source accessible name so a screen-reader controls list disambiguates
         the delete buttons (visible text kept as the label prefix — label-in-name). -->
    <Button
      variant="danger"
      size="sm"
      aria-label={`Remove source ${sourceLabel}`}
      onclick={() => projectStore.removeSource(sourceIndex)}
    >
      Remove source
    </Button>
  </header>

  {#if sourceMalformed}
    <!-- A null/non-object slot: render a degraded card (not a crash) that still
         occupies its index so validation addressing lines up; the malformed value
         stays verbatim on the draft for serialize/validate. -->
    <p class="error" role="alert">
      This source entry is malformed — fix via re-download or hand-edit.
    </p>
  {:else}
    <KeyValue rows={metaRows} />

    <div class="bindings" aria-label="Bindings">
      <h4>Bindings ({bindings.length})</h4>

      {#if bindingsMalformed}
        <p class="error" role="alert">
          This source's <code>bindings</code> are malformed — fix via re-download or hand-edit.
        </p>
      {:else if bindings.length === 0}
        <EmptyState title="No bindings yet." />
      {:else}
        <ul class="binding-list">
          <!-- Keyed by the store-owned STABLE client id (issue #200), not the index,
               so a middle binding remove remounts the correct BindingEditor instance.
               The id lives only in the store, never in the draft. -->
          {#each bindings as binding, j (projectStore.bindingId(sourceIndex, j))}
            <li>
              <BindingEditor
                sourceIndex={sourceIndex}
                bindingIndex={j}
                binding={binding}
              />
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  {/if}
</section>

<style>
  .source {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-4);
    margin-bottom: var(--space-4);
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
    scroll-margin-top: var(--space-4);
  }
  /* Briefly highlights a card when the findings panel locates it. `:global` because
     the class is toggled imperatively on the DOM node by ValidationPanel, not bound
     here (Svelte would otherwise prune the unused selector). */
  :global(.locate-flash) {
    animation: locate-flash 1.6s ease-out;
  }
  @keyframes locate-flash {
    0%,
    25% {
      box-shadow: 0 0 0 2px var(--accent);
    }
    100% {
      box-shadow: 0 0 0 2px transparent;
    }
  }
  .source-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
  }
  .source-head h3 {
    margin: 0;
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
  }
  .bindings h4 {
    margin: 0 0 var(--space-2);
  }
  .binding-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
</style>
