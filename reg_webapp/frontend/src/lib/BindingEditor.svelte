<script lang="ts">
import CatalogPicker from "./CatalogPicker.svelte";
import FieldIssues from "./FieldIssues.svelte";
import { type Binding, COLUMN_TYPES } from "./project_data";
import { projectStore } from "./project_store.svelte";
import {
  issuesForPointer,
  jsonPointer,
  type ValidationIssue,
} from "./validation";

// Editable single binding (§6.3). DERIVE CORE + ADVANCED DISCLOSURE (maintainer
// decision): the common fields (variable / type / display_name) are the surface;
// the rare type-conditional overrides hide in an Advanced disclosure.
//
// §9.6: every edit funnels through the c-i store mutators (updateBinding /
// removeBinding) — NO new store API. NEVER bind:value on the immutable draft;
// mirror c-i's name input EXACTLY (value=… + oninput=… → mutator).
const {
  sourceIndex,
  bindingIndex,
  binding,
  registerPrefix,
  period,
  variant,
  issues,
} = $props<{
  sourceIndex: number;
  bindingIndex: number;
  binding: Binding;
  registerPrefix: string; // first 2 segs of the source's register_variant
  period: string | null; // the source's period as a wire string (null → can't resolve)
  variant: string; // 3rd seg of register_variant
  issues: ValidationIssue[];
}>();

// Whether the inline variable picker is expanded.
let picking = $state(false);

// The pointer to a binding field, for the inline FieldIssues lookup.
function ptr(field: string): string {
  return jsonPointer(["sources", sourceIndex, "bindings", bindingIndex, field]);
}

// DERIVE-ON-PICK: apply the picked variable + the derived type + the display_name
// default (overridable). The variable is a bare 3-segment FQID (no @version pin).
// When the concept has several co-existing delivery columns at the period, the
// picker's chooser supplies a `representation` (the chosen column) — set it; else
// clear any stale one. Only set display_name when the resolve gave a default.
function onPickVariable(picked: {
  variable: string;
  type: string;
  displayNameDefault: string | null;
  representation?: string | null;
}): void {
  const patch: Partial<Binding> = {
    variable: picked.variable,
    type: picked.type,
    representation: picked.representation ?? null,
  };
  if (
    picked.displayNameDefault != null &&
    (binding.display_name ?? "") === ""
  ) {
    patch.display_name = picked.displayNameDefault;
  }
  projectStore.updateBinding(sourceIndex, bindingIndex, patch);
  picking = false;
}

// An override field (display_name or an advanced subtype/format): blank → null
// (clears the field, so the backend resolves/ignores it). display_name funnels
// through here too — it's just the non-advanced override.
function onAdvanced(field: keyof Binding, value: string): void {
  projectStore.updateBinding(sourceIndex, bindingIndex, {
    [field]: value === "" ? null : value,
  });
}

// A binding field coerced to a string for an input value (non-string → "").
function strField(field: keyof Binding): string {
  const v = binding[field];
  return typeof v === "string" ? v : "";
}
const type = $derived(strField("type"));
const displayName = $derived(strField("display_name"));
</script>

<div class="binding">
  <div class="binding-grid">
    <!-- variable: read-ish + a Pick button (the derive-on-pick entry point). -->
    <div class="field variable">
      <span class="field-label">Variable</span>
      <div class="variable-row">
        <code class="variable-value">{binding.variable || "(no variable)"}</code>
        <button type="button" class="small" onclick={() => (picking = !picking)}>
          {picking ? "Close" : "Pick variable"}
        </button>
      </div>
      <FieldIssues issues={issuesForPointer(issues, ptr("variable"))} />
    </div>

    <!-- type: the 6 ColumnType values, prefilled from the pick, overridable. -->
    <div class="field">
      <span class="field-label">Type</span>
      <select
        value={type}
        onchange={(e) =>
          projectStore.updateBinding(sourceIndex, bindingIndex, {
            type: e.currentTarget.value,
          })}
      >
        {#if !COLUMN_TYPES.includes(type as (typeof COLUMN_TYPES)[number])}
          <!-- An unknown/blank type round-trips as a disabled option so we never
               silently drop a malformed value (the backend flags it). -->
          <option value={type} disabled>{type || "(unset)"}</option>
        {/if}
        {#each COLUMN_TYPES as ct (ct)}
          <option value={ct}>{ct}</option>
        {/each}
      </select>
      <FieldIssues issues={issuesForPointer(issues, ptr("type"))} />
    </div>

    <!-- display_name: defaults to the picked delivery_column_name, overridable. -->
    <div class="field">
      <span class="field-label">Display name</span>
      <input
        type="text"
        value={displayName}
        placeholder="(resolved from reg_meta)"
        oninput={(e) => onAdvanced("display_name", e.currentTarget.value)}
      />
      <FieldIssues issues={issuesForPointer(issues, ptr("display_name"))} />
    </div>
  </div>

  {#if picking}
    <CatalogPicker
      mode="variable"
      {registerPrefix}
      {period}
      {variant}
      onpickVariable={onPickVariable}
      oncancel={() => (picking = false)}
    />
  {/if}

  <!-- Advanced overrides: type-CONDITIONAL (only the field owning the chosen type
       renders, to avoid subtype_on_wrong_type). All optional / rarely set. -->
  <details class="advanced">
    <summary>Advanced</summary>
    <div class="advanced-fields">
      {#if type === "id"}
        <label>
          <span>id_subtype</span>
          <select
            value={strField("id_subtype")}
            onchange={(e) => onAdvanced("id_subtype", e.currentTarget.value)}
          >
            <option value="">(unset)</option>
            <option value="integer">integer</option>
            <option value="string">string</option>
          </select>
          <FieldIssues issues={issuesForPointer(issues, ptr("id_subtype"))} />
        </label>
      {:else if type === "numeric"}
        <label>
          <span>numeric_subtype</span>
          <select
            value={strField("numeric_subtype")}
            onchange={(e) => onAdvanced("numeric_subtype", e.currentTarget.value)}
          >
            <option value="">(unset)</option>
            <option value="integer">integer</option>
            <option value="double">double</option>
          </select>
          <FieldIssues issues={issuesForPointer(issues, ptr("numeric_subtype"))} />
        </label>
      {:else if type === "date"}
        <label>
          <span>date_format</span>
          <input
            type="text"
            value={strField("date_format")}
            placeholder="%Y-%m-%d"
            oninput={(e) => onAdvanced("date_format", e.currentTarget.value)}
          />
          <FieldIssues issues={issuesForPointer(issues, ptr("date_format"))} />
        </label>
      {:else if type === "datetime"}
        <label>
          <span>datetime_format</span>
          <input
            type="text"
            value={strField("datetime_format")}
            placeholder="%Y-%m-%d %H:%M:%S"
            oninput={(e) => onAdvanced("datetime_format", e.currentTarget.value)}
          />
          <FieldIssues issues={issuesForPointer(issues, ptr("datetime_format"))} />
        </label>
      {:else if type === "categorical"}
        <label>
          <span>value_set</span>
          <input
            type="text"
            value={strField("value_set")}
            placeholder="class/<slug>"
            oninput={(e) => onAdvanced("value_set", e.currentTarget.value)}
          />
          <FieldIssues issues={issuesForPointer(issues, ptr("value_set"))} />
        </label>
      {:else}
        <p class="muted">No type-specific overrides for <code>{type || "(unset)"}</code>.</p>
      {/if}
    </div>
  </details>

  <button type="button" class="small remove" onclick={() => projectStore.removeBinding(sourceIndex, bindingIndex)}>
    Remove binding
  </button>
</div>

<style>
  .binding {
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.6rem 0.75rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .binding-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem 1.25rem;
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .field.variable {
    flex: 1 1 18rem;
  }
  .field-label {
    font-weight: 600;
    font-size: 0.8rem;
  }
  .variable-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .variable-value {
    font-size: 0.9em;
  }
  select,
  input {
    font: inherit;
    padding: 0.3rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
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
    align-self: flex-start;
    color: var(--level-error);
  }
  .advanced summary {
    cursor: pointer;
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--muted);
  }
  .advanced-fields {
    margin-top: 0.5rem;
  }
  .advanced-fields label {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    max-width: 18rem;
    font-size: 0.8rem;
  }
</style>
