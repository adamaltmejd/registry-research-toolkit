<script lang="ts">
import BindingEditor from "./BindingEditor.svelte";
import CatalogPicker from "./CatalogPicker.svelte";
import { registerPrefixOf, variantSeg } from "./catalog";
import FieldIssues from "./FieldIssues.svelte";
import PeriodEditor from "./PeriodEditor.svelte";
import { periodToWire } from "./period";
import type { Period, Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { Button, Tag } from "./ui";
import {
  issuesForPointer,
  issuesUnderPointer,
  jsonPointer,
  sourceAnchorId,
  type ValidationIssue,
} from "./validation";

// Editable single source (see reg_schema/DESIGN.md → Two layers: models vs.
// validator). Header carries a rolled-up error badge (every
// issue under /sources/{i}); the fields edit name / register_variant / period /
// bindings. Every edit funnels through the c-i store mutators — NO new store API.
const { sourceIndex, source, issues } = $props<{
  sourceIndex: number;
  source: Source;
  issues: ValidationIssue[];
}>();

// Whether the inline variant picker is expanded.
let pickingVariant = $state(false);

const sourcePtr = $derived(jsonPointer(["sources", sourceIndex]));
const rolledUp = $derived(issuesUnderPointer(issues, sourcePtr));
const errorCount = $derived(rolledUp.filter((i) => i.level === "error").length);

const registerVariant = $derived(
  typeof source.register_variant === "string" ? source.register_variant : "",
);
const registerPrefix = $derived(registerPrefixOf(registerVariant));
const variant = $derived(variantSeg(registerVariant));

// Defensive: a malformed opened spec may carry `bindings` as a non-array. Show an
// inline note instead of the list (full-replace-with-guards, maintainer decision)
// rather than crashing — the draft stays verbatim for serialize/validate.
const bindings = $derived(
  Array.isArray(source.bindings) ? source.bindings : [],
);
const bindingsMalformed = $derived(
  source.bindings !== undefined && !Array.isArray(source.bindings),
);

// The period as a wire string for the binding/variant pickers' resolve.
const periodWire = $derived(periodToWire(source.period as Period));

function ptr(field: string): string {
  return jsonPointer(["sources", sourceIndex, field]);
}

function onPickVariant(registerVariant: string): void {
  // C2: the picker emits the WHOLE 3-seg register_variant (it owns the register —
  // either the hand-typed prefix or one the user browsed to). Set it directly.
  projectStore.updateSource(sourceIndex, { register_variant: registerVariant });
  pickingVariant = false;
}
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
      {source.name || "(unnamed source)"}
      {#if errorCount > 0}
        <!-- Status badge: cool error tone + ✕ glyph (aria-hidden); the count text
             carries the meaning for assistive tech (DESIGN.md accent-vs-status). -->
        <Tag tone="error">
          {#snippet glyph()}✕{/snippet}
          {errorCount} error{errorCount === 1 ? "" : "s"}
        </Tag>
      {/if}
    </h3>
    <Button variant="danger" size="sm" onclick={() => projectStore.removeSource(sourceIndex)}>
      Remove source
    </Button>
  </header>

  <div class="fields">
    <label class="field">
      <span class="field-label">Name</span>
      <input
        type="text"
        value={source.name}
        placeholder="Source name"
        oninput={(e) => projectStore.updateSource(sourceIndex, { name: e.currentTarget.value })}
      />
      <FieldIssues issues={issuesForPointer(issues, ptr("name"))} />
    </label>

    <div class="field">
      <span class="field-label">Register variant</span>
      <!-- An editable text field (the researcher often knows the FQID): type the
           full `provider/register/variant`, OR type the `provider/register` prefix
           and use "Pick variant" to choose the variant slug from the catalog. -->
      <div class="rv-row">
        <input
          type="text"
          class="rv-input"
          value={registerVariant}
          placeholder="provider/register/variant"
          oninput={(e) =>
            projectStore.updateSource(sourceIndex, {
              register_variant: e.currentTarget.value,
            })}
        />
        <Button variant="default" size="sm" onclick={() => (pickingVariant = !pickingVariant)}>
          {pickingVariant ? "Close" : "Pick variant"}
        </Button>
      </div>
      <FieldIssues issues={issuesForPointer(issues, ptr("register_variant"))} />
      {#if pickingVariant}
        <!-- C2: the picker BROWSES providers → registers → variants when no valid
             register prefix is hand-typed, and jumps straight to the variant list
             when one is. It emits the WHOLE register_variant either way. -->
        <CatalogPicker
          mode="variant"
          register={registerPrefix}
          onpickVariant={onPickVariant}
          oncancel={() => (pickingVariant = false)}
        />
      {/if}
    </div>

    <div class="field">
      <PeriodEditor
        period={source.period as Period}
        issues={issuesForPointer(issues, ptr("period"))}
        onchange={(p: Period) => projectStore.updateSource(sourceIndex, { period: p })}
      />
    </div>
  </div>

  <div class="bindings" aria-label="Bindings">
    <div class="bindings-head">
      <h4>Bindings ({bindings.length})</h4>
      <Button variant="default" size="sm" onclick={() => projectStore.addBinding(sourceIndex)}>
        Add binding
      </Button>
    </div>
    <!-- empty_bindings is surfaced on the /sources/{i}/bindings pointer. -->
    <FieldIssues issues={issuesForPointer(issues, ptr("bindings"))} />

    {#if bindingsMalformed}
      <p class="error" role="alert">
        This source's <code>bindings</code> are malformed — fix via re-download or hand-edit.
      </p>
    {:else if bindings.length === 0}
      <p class="muted">No bindings yet.</p>
    {:else}
      <ul class="binding-list">
        <!-- Keyed by the store-owned STABLE client id (issue #200), not the index,
             so a middle binding remove remounts the correct BindingEditor instance
             (its `picking` flag / open CatalogPicker stay bound to the right
             binding). The id lives only in the store, never in the draft. -->
        {#each bindings as binding, j (projectStore.bindingId(sourceIndex, j))}
          <li>
            <BindingEditor
              sourceIndex={sourceIndex}
              bindingIndex={j}
              binding={binding}
              {registerPrefix}
              period={periodWire}
              {variant}
              {issues}
            />
          </li>
        {/each}
      </ul>
    {/if}
  </div>
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
  .fields {
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    max-width: 32rem;
  }
  .field-label {
    font-weight: 600;
    font-size: var(--text-sm);
  }
  .field input {
    font: inherit;
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
  }
  .rv-row {
    display: flex;
    align-items: center;
    gap: var(--space-2);
  }
  /* The register_variant is a machine FQID — mono, like every code/identifier. */
  .rv-input {
    flex: 1;
    font-family: var(--font-mono);
  }
  .bindings-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: var(--space-2);
  }
  .bindings-head h4 {
    margin: 0;
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
