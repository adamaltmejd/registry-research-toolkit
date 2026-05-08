<script lang="ts">
  import type { ColumnInfo, RegisterGroupView } from "../types";
  import ColumnTypeEditor from "./ColumnTypeEditor.svelte";
  import RegisterEditor from "./RegisterEditor.svelte";

  interface Props {
    group: RegisterGroupView;
  }

  let { group }: Props = $props();

  let editingColumn:
    | { source: string; column: ColumnInfo }
    | null = $state(null);
  let editingRegister = $state(false);

  const CONFIDENCE_LABEL: Record<string, string> = {
    high: "high confidence",
    partial: "partial confidence",
    none: "no confidence",
  };

  function hintSummary(col: ColumnInfo): string {
    if (!col.hint) return "";
    return Object.entries(col.hint)
      .map(([k, v]) => `${k}=${String(v)}`)
      .join(", ");
  }
</script>

<section class="group" class:no-register={group.register_id === null}>
  <header>
    <div>
      <h2>
        {#if group.register_name}
          {group.register_name}
        {:else}
          <span class="unassigned">unassigned</span>
        {/if}
        <small>· {group.group_id}</small>
      </h2>
      <p class="meta">
        <span class="conf conf-{group.confidence}"
          >{CONFIDENCE_LABEL[group.confidence]}</span
        >
        · {group.sources.length} source{group.sources.length === 1 ? "" : "s"}
        · {group.schema_variants} schema{group.schema_variants === 1 ? "" : "s"}
        {#if group.panel_candidate}
          · panel candidate ({group.panel_candidate.members.length})
        {/if}
      </p>
    </div>
    <button class="link" onclick={() => (editingRegister = true)}>
      Edit register…
    </button>
  </header>

  {#each group.sources as sourceName (sourceName)}
    {@const cols = group.columns_by_source[sourceName] ?? []}
    <div class="source">
      <h3 class="source-name">{sourceName}</h3>
      <table>
        <thead>
          <tr>
            <th>Column</th>
            <th>SQL</th>
            <th>Type</th>
            <th>Hint</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          {#each cols as col (col.name)}
            <tr>
              <td class="mono">{col.name}</td>
              <td class="mono dim">{col.sql_type ?? "—"}</td>
              <td>
                <button
                  class="type-pill type-{col.current_type}"
                  onclick={() =>
                    (editingColumn = { source: sourceName, column: col })}
                >
                  {col.current_type}
                </button>
              </td>
              <td class="mono dim">{hintSummary(col) || "—"}</td>
              <td>
                <span class="prov prov-{col.provenance}">{col.provenance}</span>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/each}
</section>

{#if editingColumn}
  <ColumnTypeEditor
    sourceName={editingColumn.source}
    column={editingColumn.column}
    onClose={() => (editingColumn = null)}
  />
{/if}

{#if editingRegister}
  <RegisterEditor
    {group}
    onClose={() => (editingRegister = false)}
  />
{/if}

<style>
  .group {
    background: #fff;
    border: 1px solid #e1e1e1;
    border-radius: 6px;
    padding: 1rem 1.25rem;
    margin-bottom: 1rem;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
  }
  .group.no-register {
    border-color: #f0c14b;
    background: #fffaf0;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    margin-bottom: 0.5rem;
  }
  h2 {
    margin: 0;
    font-size: 1.1rem;
  }
  h2 small {
    color: #888;
    font-weight: normal;
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
  }
  .unassigned {
    color: #a06400;
    font-style: italic;
  }
  .meta {
    margin: 0.25rem 0 0;
    color: #555;
    font-size: 0.9rem;
  }
  .conf {
    padding: 0.05rem 0.4rem;
    border-radius: 3px;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .conf-high {
    background: #d8f0d8;
    color: #1a661a;
  }
  .conf-partial {
    background: #fff0c4;
    color: #7a5b00;
  }
  .conf-none {
    background: #f0e0e0;
    color: #883333;
  }
  .source {
    margin-top: 0.75rem;
  }
  .source-name {
    font-size: 0.95rem;
    margin: 0 0 0.25rem 0;
    font-family: ui-monospace, monospace;
    color: #444;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
  }
  th,
  td {
    text-align: left;
    padding: 0.3rem 0.4rem;
    border-bottom: 1px solid #f0f0f0;
  }
  th {
    color: #777;
    font-weight: 500;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .mono {
    font-family: ui-monospace, monospace;
  }
  .dim {
    color: #888;
  }
  .type-pill {
    background: #eef2fb;
    color: #1a3b80;
    border: 1px solid #c8d3ec;
    border-radius: 3px;
    padding: 0.1rem 0.5rem;
    cursor: pointer;
    font: inherit;
    font-family: ui-monospace, monospace;
  }
  .type-pill:hover {
    background: #e0e7f7;
  }
  .type-id {
    background: #e8f1fa;
    color: #114a85;
  }
  .type-categorical {
    background: #efe8fa;
    color: #5d2b8c;
  }
  .type-numeric {
    background: #e8f6ec;
    color: #185a2b;
  }
  .type-date {
    background: #faefe0;
    color: #7c4400;
  }
  .type-opaque {
    background: #f0f0f0;
    color: #555;
  }
  .prov {
    font-size: 0.78rem;
    color: #666;
  }
  .prov-manual {
    color: #b34a00;
    font-weight: 600;
  }
  .link {
    background: transparent;
    border: 0;
    color: #1656c0;
    cursor: pointer;
    font: inherit;
    padding: 0;
  }
  .link:hover {
    text-decoration: underline;
  }
</style>
