<script lang="ts">
import BindingEditor from "./BindingEditor.svelte";
import CatalogPicker from "./CatalogPicker.svelte";
import { registerPrefixOf, variantSeg } from "./catalog";
import FieldIssues from "./FieldIssues.svelte";
import PeriodEditor from "./PeriodEditor.svelte";
import { periodToWire } from "./period";
import type { Period, Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import {
  issuesForPointer,
  issuesUnderPointer,
  jsonPointer,
  type ValidationIssue,
} from "./validation";

// Editable single source (§6.2). Header carries a rolled-up error badge (every
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

function onPickVariant(slug: string): void {
  // Build the 3-seg register_variant: keep the register prefix, set the variant.
  const prefix = registerPrefix || registerVariant;
  const next = prefix ? `${prefix}/${slug}` : slug;
  projectStore.updateSource(sourceIndex, { register_variant: next });
  pickingVariant = false;
}
</script>

<section class="source" aria-label="Source {sourceIndex + 1}">
  <header class="source-head">
    <h3>
      {source.name || "(unnamed source)"}
      {#if errorCount > 0}
        <span class="error-badge" title="{errorCount} error{errorCount === 1 ? '' : 's'} in this source">
          {errorCount} error{errorCount === 1 ? "" : "s"}
        </span>
      {/if}
    </h3>
    <button type="button" class="small remove" onclick={() => projectStore.removeSource(sourceIndex)}>
      Remove source
    </button>
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
      <div class="rv-row">
        <code class="rv-value">{registerVariant || "(none)"}</code>
        <button type="button" class="small" onclick={() => (pickingVariant = !pickingVariant)}>
          {pickingVariant ? "Close" : "Pick variant"}
        </button>
      </div>
      <FieldIssues issues={issuesForPointer(issues, ptr("register_variant"))} />
      {#if pickingVariant}
        {#if registerPrefix}
          <CatalogPicker
            mode="variant"
            register={registerPrefix}
            onpickVariant={onPickVariant}
            oncancel={() => (pickingVariant = false)}
          />
        {:else}
          <p class="hint muted">
            Set the register (e.g. <code>scb/lisa</code>) in <code>register_variant</code> first to pick a variant.
          </p>
        {/if}
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
      <button type="button" class="small" onclick={() => projectStore.addBinding(sourceIndex)}>
        Add binding
      </button>
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
        {#each bindings as binding, j (j)}
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
    border-radius: 6px;
    padding: 1rem;
    margin-bottom: 1rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }
  .source-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
  }
  .source-head h3 {
    margin: 0;
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
  }
  .error-badge {
    font-size: 0.75rem;
    font-weight: 600;
    color: #b00020;
    background: #fef2f2;
    border: 1px solid #fca5a5;
    border-radius: 999px;
    padding: 0.05rem 0.5rem;
  }
  .fields {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    max-width: 32rem;
  }
  .field-label {
    font-weight: 600;
    font-size: 0.85rem;
  }
  .field input {
    font: inherit;
    padding: 0.4rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  .rv-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .rv-value {
    font-size: 0.9em;
  }
  .small {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.2rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
    cursor: pointer;
  }
  .small:hover {
    border-color: var(--accent);
  }
  .remove {
    color: #b00020;
  }
  .bindings-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
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
    gap: 0.5rem;
  }
  .hint {
    font-size: 0.8rem;
    margin: 0.4rem 0 0;
  }
</style>
