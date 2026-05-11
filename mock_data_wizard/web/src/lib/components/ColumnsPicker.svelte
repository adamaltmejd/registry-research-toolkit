<script lang="ts">
  import { store, type OptionalColumnId } from "../store.svelte";

  interface ColumnEntry {
    id: OptionalColumnId;
    label: string;
    description: string;
  }

  const COLUMNS: ColumnEntry[] = [
    { id: "sql", label: "SQL", description: "Raw SQL type from discover" },
    { id: "type", label: "Type", description: "Classifier output (editable)" },
    {
      id: "coverage",
      label: "Coverage",
      description: "Per-source presence map",
    },
  ];

  let open = $state(false);
  let rootEl: HTMLDivElement | undefined = $state();

  function toggleOpen(): void {
    open = !open;
  }

  function close(): void {
    open = false;
  }

  // Listeners only while open — leaving Escape attached when idle would
  // intercept it from any other component on the page.
  $effect(() => {
    if (!open) return;
    const onPointerDown = (e: PointerEvent): void => {
      if (rootEl && !rootEl.contains(e.target as Node)) close();
    };
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown, true);
      document.removeEventListener("keydown", onKey);
    };
  });

  let visibleCount = $derived(
    COLUMNS.filter((c) => store.visibleColumns[c.id]).length,
  );
</script>

<div class="columns-picker" bind:this={rootEl}>
  <button
    type="button"
    class="trigger"
    aria-expanded={open}
    onclick={toggleOpen}
    title="Choose which fields to display in the per-group table"
  >
    Fields
    <span class="count">{visibleCount + 1}/{COLUMNS.length + 1}</span>
    <span class="caret" aria-hidden="true">▾</span>
  </button>
  {#if open}
    <div class="popover">
      <p class="hint">Variable “Name” is always shown.</p>
      <ul>
        {#each COLUMNS as col (col.id)}
          <li>
            <label>
              <input
                type="checkbox"
                checked={store.visibleColumns[col.id]}
                onchange={() => store.toggleColumnVisibility(col.id)}
              />
              <span class="label">{col.label}</span>
              <span class="desc">{col.description}</span>
            </label>
          </li>
        {/each}
      </ul>
    </div>
  {/if}
</div>

<style>
  .columns-picker {
    position: relative;
    flex: 0 0 auto;
  }
  .trigger {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.35rem 0.7rem;
    border: 1px solid #ccc;
    background: #fff;
    border-radius: 4px;
    cursor: pointer;
    font: inherit;
    font-size: 0.9rem;
    color: #333;
  }
  .trigger:hover {
    background: #f5f5f5;
  }
  .trigger:focus-visible {
    outline: 2px solid #1656c0;
    outline-offset: 1px;
  }
  .count {
    background: rgba(0, 0, 0, 0.08);
    color: #555;
    padding: 0 0.4rem;
    border-radius: 999px;
    font-size: 0.78em;
    font-weight: 600;
  }
  .caret {
    color: #888;
    font-size: 0.75em;
    line-height: 1;
  }
  .popover {
    position: absolute;
    right: 0;
    top: calc(100% + 0.35rem);
    min-width: 18rem;
    background: #fff;
    border: 1px solid #d6d6d6;
    border-radius: 6px;
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.12);
    padding: 0.6rem 0.75rem;
    z-index: 60;
  }
  .hint {
    margin: 0 0 0.4rem;
    color: #777;
    font-size: 0.78rem;
  }
  ul {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }
  label {
    display: grid;
    grid-template-columns: auto auto 1fr;
    align-items: baseline;
    gap: 0.5rem;
    padding: 0.35rem 0.4rem;
    border-radius: 3px;
    cursor: pointer;
    font-size: 0.9rem;
  }
  label:hover {
    background: #f5f5f5;
  }
  input[type="checkbox"] {
    margin: 0;
    cursor: pointer;
  }
  .label {
    font-weight: 500;
    color: #222;
  }
  .desc {
    color: #888;
    font-size: 0.82rem;
  }
</style>
