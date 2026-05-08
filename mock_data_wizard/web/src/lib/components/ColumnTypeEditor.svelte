<script lang="ts">
  import { untrack } from "svelte";

  import { store } from "../store.svelte";
  import type { ColumnInfo, ColumnType } from "../types";

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

  function buildHint(): Record<string, unknown> | null | undefined {
    if (selectedType === "id" && idSubtype) {
      return { id_subtype: idSubtype };
    }
    if (selectedType === "numeric" && numericSubtype) {
      return { numeric_subtype: numericSubtype };
    }
    if (selectedType === "date" && dateFormat) {
      return { date_format: dateFormat };
    }
    // No inline hint applicable: explicitly clear any existing one
    // when the type changed; preserve when unchanged.
    if (selectedType !== column.current_type) {
      return null;
    }
    return undefined;
  }

  async function submit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    submitting = true;
    const hint = buildHint();
    const ok = await store.setColumnType({
      source: sourceName,
      column: column.name,
      type: selectedType,
      expected_version: version,
      ...(hint === undefined ? {} : { hint }),
    });
    submitting = false;
    if (ok) {
      onClose();
    }
  }
</script>

<div
  class="overlay"
  role="dialog"
  aria-modal="true"
  aria-label="Edit column type"
  tabindex="-1"
  onclick={(e) => {
    if (e.target === e.currentTarget) onClose();
  }}
  onkeydown={(e) => {
    if (e.key === "Escape") onClose();
  }}
>
  <form class="card" onsubmit={submit}>
    <header>
      <h3>{sourceName} → {column.name}</h3>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

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
</div>

<style>
  .overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.35);
    display: grid;
    place-items: center;
    z-index: 100;
  }
  .card {
    background: #fff;
    border-radius: 6px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
    padding: 1rem 1.25rem;
    width: min(28rem, 90vw);
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
  }
  h3 {
    margin: 0;
    font-size: 1rem;
    font-family: ui-monospace, monospace;
  }
  .close {
    background: transparent;
    border: 0;
    font-size: 1.4rem;
    cursor: pointer;
    color: #666;
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
