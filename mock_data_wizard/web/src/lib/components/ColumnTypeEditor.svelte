<script lang="ts">
  import { untrack } from "svelte";

  import { store } from "../store.svelte";
  import type { ColumnInfo, ColumnType } from "../types";
  import Modal from "./Modal.svelte";

  interface Props {
    sourceName: string;
    column: ColumnInfo;
    onClose: () => void;
  }

  let { sourceName, column, onClose }: Props = $props();

  const TYPES: ColumnType[] = ["id", "categorical", "numeric", "opaque", "date"];

  // Modal editor: snapshot the prop once on mount and let local edit
  // state diverge. `untrack` signals to the compiler that not reacting
  // to upstream changes is intentional.
  let selectedType: ColumnType = $state(untrack(() => column.current_type));
  let idSubtype: string = $state(
    untrack(() => (column.hint?.id_subtype as string | undefined) ?? ""),
  );
  let numericSubtype: string = $state(
    untrack(() => (column.hint?.numeric_subtype as string | undefined) ?? ""),
  );
  let dateFormat: string = $state(
    untrack(() => (column.hint?.date_format as string | undefined) ?? ""),
  );
  let submitting = $state(false);

  function buildHint(): Record<string, unknown> | null {
    // Always send an explicit hint based on form state. Earlier we
    // returned `undefined` to preserve the server's existing value
    // when type was unchanged, but that made the "unset" option in
    // the dropdown a no-op — users couldn't clear an existing hint
    // without first changing the type. Explicit-always trades the
    // UNCHANGED micro-optimization for a form that actually does
    // what it says.
    if (selectedType === "id" && idSubtype) return { id_subtype: idSubtype };
    if (selectedType === "numeric" && numericSubtype)
      return { numeric_subtype: numericSubtype };
    if (selectedType === "date" && dateFormat)
      return { date_format: dateFormat };
    return null;
  }

  async function submit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    submitting = true;
    const ok = await store.setColumnType({
      source: sourceName,
      column: column.name,
      type: selectedType,
      expected_version: version,
      hint: buildHint(),
    });
    submitting = false;
    if (ok) {
      store.pushToast(
        "info",
        `Saved ${column.name} → ${selectedType}`,
      );
      onClose();
    }
  }
</script>

<Modal headingId="column-type-editor-heading" {onClose}>
  <form onsubmit={submit}>
    <header>
      <div class="heading-stack">
        <span class="source-line" title={sourceName}>{sourceName}</span>
        <h3 id="column-type-editor-heading">{column.name}</h3>
      </div>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

    {#if column.regmeta_signal}
      <p class="regmeta-context" aria-label="regmeta context">
        regmeta:
        {#if column.regmeta_signal.classification_short_name}
          <code>{column.regmeta_signal.classification_short_name}</code>
        {/if}
        {#if column.regmeta_signal.has_value_codes}
          {#if column.regmeta_signal.classification_short_name}·{/if}
          value codes available
        {/if}
        {#if column.regmeta_signal.datatyp_kind}
          · datatype <code>{column.regmeta_signal.datatyp_kind}</code>
        {/if}
        {#if !column.regmeta_signal.classification_short_name && !column.regmeta_signal.has_value_codes && !column.regmeta_signal.datatyp_kind}
          column known to regmeta but with no classification, codes, or
          datatype hint.
        {/if}
      </p>
    {:else}
      <p class="regmeta-context regmeta-missing">
        regmeta: no record for this column name.
      </p>
    {/if}

    <fieldset>
      <legend>Type</legend>
      {#each TYPES as t (t)}
        <label class="radio">
          <input type="radio" name="type" value={t} bind:group={selectedType} />
          {t}
          {#if t === column.regmeta_implied_type}
            <span class="hint" title="regmeta-implied type for this column"
              >· regmeta</span
            >
          {/if}
        </label>
      {/each}
    </fieldset>

    {#if selectedType === "id"}
      <label class="row">
        <span>id_subtype</span>
        <select bind:value={idSubtype}>
          <option value="">(unset — sample at extract)</option>
          <option value="integer">integer</option>
          <option value="string">string</option>
        </select>
      </label>
    {:else if selectedType === "numeric"}
      <label class="row">
        <span>numeric_subtype</span>
        <select bind:value={numericSubtype}>
          <option value="">(unset — sample at extract)</option>
          <option value="integer">integer</option>
          <option value="double">double</option>
        </select>
      </label>
    {:else if selectedType === "date"}
      <label class="row">
        <span>date_format</span>
        <input
          type="text"
          placeholder="e.g. %Y%m%d"
          bind:value={dateFormat}
          spellcheck="false"
        />
      </label>
    {/if}

    <footer>
      <button type="button" onclick={onClose} disabled={submitting}
        >Cancel</button
      >
      <button type="submit" class="primary" disabled={submitting}>
        {submitting ? "Saving…" : "Save"}
      </button>
    </footer>
  </form>
</Modal>

<style>
  form {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }
  .heading-stack {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .source-line {
    color: #777;
    font-size: 0.85rem;
    font-family: ui-monospace, monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  h3 {
    margin: 0;
    font-size: 1.05rem;
    font-family: ui-monospace, monospace;
    word-break: break-word;
  }
  .close {
    background: transparent;
    border: 0;
    font-size: 1.4rem;
    cursor: pointer;
    color: #666;
    flex: 0 0 auto;
    padding: 0;
    line-height: 1;
  }
  .regmeta-context {
    margin: 0;
    padding: 0.4rem 0.6rem;
    background: #f4f6fb;
    border-left: 3px solid #c8d3ec;
    font-size: 0.85rem;
    color: #444;
    border-radius: 3px;
  }
  .regmeta-context code {
    background: #fff;
    padding: 0.05rem 0.3rem;
    border-radius: 3px;
    font-size: 0.95em;
  }
  .regmeta-missing {
    background: #fff8e9;
    border-left-color: #f0c14b;
    color: #5b4a14;
  }
  fieldset {
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
  }
  legend {
    padding: 0 0.25rem;
    font-size: 0.85rem;
    color: #666;
  }
  .radio {
    display: block;
    padding: 0.15rem 0;
    cursor: pointer;
  }
  .hint {
    color: #888;
    font-size: 0.85rem;
  }
  .row {
    display: grid;
    grid-template-columns: 9rem 1fr;
    align-items: center;
    gap: 0.5rem;
  }
  .row span {
    color: #666;
    font-size: 0.9rem;
  }
  input[type="text"],
  select {
    padding: 0.3rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
  }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }
  button {
    padding: 0.4rem 0.9rem;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: #fff;
    cursor: pointer;
    font: inherit;
  }
  button.primary {
    background: #1656c0;
    color: #fff;
    border-color: #1656c0;
  }
  button:disabled {
    opacity: 0.6;
    cursor: progress;
  }
</style>
