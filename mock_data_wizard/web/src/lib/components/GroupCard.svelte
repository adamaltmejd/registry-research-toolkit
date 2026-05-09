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

  // Inline subtype/format suffix shown on the type pill — only the
  // value, since the key is implied by the type ("integer" under id is
  // an id_subtype; "%Y%m%d" under date is a date_format).
  function hintSuffix(col: ColumnInfo): string {
    if (!col.hint) return "";
    return Object.values(col.hint).map(String).join(" · ");
  }

  // Regmeta context tacked onto the pill: classification short name when
  // present, otherwise "value codes" if regmeta has a code lookup. The
  // bare-match case (regmeta knew the column but had no classification
  // or codes) is intentionally silent — it adds nothing the type pill
  // doesn't already show.
  function regmetaSuffix(col: ColumnInfo): string {
    const sig = col.regmeta_signal;
    if (!sig) return "";
    if (sig.classification_short_name) return sig.classification_short_name;
    if (sig.has_value_codes) return "value codes";
    return "";
  }

  // Categoricals that aren't backed by a regmeta classification or
  // value-code set are an audit gap — most likely candidates for
  // manual review or for a missing regmeta entry. Marker stays subtle.
  function isUnmatchedCategorical(col: ColumnInfo): boolean {
    if (col.current_type !== "categorical") return false;
    return regmetaSuffix(col) === "";
  }

  interface SourceStats {
    total: number;
    unmatched: number;
    manual: number;
  }
  function statsFor(cols: ColumnInfo[]): SourceStats {
    let unmatched = 0;
    let manual = 0;
    for (const c of cols) {
      if (isUnmatchedCategorical(c)) unmatched++;
      if (c.provenance === "manual") manual++;
    }
    return { total: cols.length, unmatched, manual };
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
    {@const stats = statsFor(cols)}
    <details class="source">
      <summary>
        <span class="source-name mono">{sourceName}</span>
        <span class="source-stats">
          <span class="stat-cols">{stats.total} col{stats.total === 1 ? "" : "s"}</span>
          {#if stats.unmatched > 0}
            <span class="stat-unmatched" title="categoricals without regmeta classification or value codes"
              >● {stats.unmatched} unmatched</span
            >
          {/if}
          {#if stats.manual > 0}
            <span class="stat-manual" title="manual type overrides"
              >★ {stats.manual} manual</span
            >
          {/if}
        </span>
      </summary>
      <table>
        <thead>
          <tr>
            <th>Column</th>
            <th>SQL</th>
            <th>Type</th>
          </tr>
        </thead>
        <tbody>
          {#each cols as col (col.name)}
            {@const hint = hintSuffix(col)}
            {@const regmeta = regmetaSuffix(col)}
            <tr>
              <td class="mono">{col.name}</td>
              <td class="mono dim">{col.sql_type ?? "—"}</td>
              <td>
                <button
                  class="type-pill type-{col.current_type} prov-{col.provenance}"
                  title={col.provenance === "manual"
                    ? "manual override"
                    : "auto-classified"}
                  onclick={() =>
                    (editingColumn = { source: sourceName, column: col })}
                >
                  <span class="type-name">{col.current_type}</span>
                  {#if hint}
                    <span class="type-suffix">· {hint}</span>
                  {/if}
                  {#if regmeta}
                    <span class="type-suffix regmeta">· {regmeta}</span>
                  {/if}
                </button>
                {#if isUnmatchedCategorical(col)}
                  <span
                    class="unmatched-marker"
                    title="categorical without regmeta classification or value codes"
                    aria-label="unmatched categorical">●</span
                  >
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </details>
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
    margin-top: 0.5rem;
    border-top: 1px solid #f0f0f0;
  }
  .source > summary {
    list-style: none;
    cursor: pointer;
    padding: 0.4rem 0.2rem;
    display: flex;
    align-items: center;
    gap: 0.75rem;
    user-select: none;
  }
  .source > summary::-webkit-details-marker {
    display: none;
  }
  /* Custom chevron — rotates when the source expands. */
  .source > summary::before {
    content: "▸";
    color: #888;
    font-size: 0.75rem;
    transition: transform 0.12s ease;
    display: inline-block;
    width: 0.7rem;
  }
  .source[open] > summary::before {
    transform: rotate(90deg);
  }
  .source > summary:hover {
    background: #fafafa;
  }
  .source-name {
    font-size: 0.92rem;
    color: #444;
    flex: 1 1 auto;
    word-break: break-all;
  }
  .source-stats {
    display: inline-flex;
    align-items: center;
    gap: 0.65rem;
    font-size: 0.82rem;
    color: #666;
    flex: 0 0 auto;
  }
  .stat-cols {
    color: #888;
  }
  .stat-unmatched {
    color: #b34a00;
    opacity: 0.85;
  }
  .stat-manual {
    color: #b34a00;
    font-weight: 600;
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
  /* Provenance is folded onto the pill itself: auto = quiet (default),
     manual = solid left-border accent so edits stand out at a glance. */
  .type-pill.prov-manual {
    border-left: 3px solid #b34a00;
    padding-left: calc(0.5rem - 2px);
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
  .type-suffix {
    margin-left: 0.15rem;
    opacity: 0.65;
    font-size: 0.85em;
  }
  /* Subtle audit marker: faint orange dot beside categoricals without a
     regmeta classification or value-code set. Big enough to scan, quiet
     enough not to scream. */
  .unmatched-marker {
    margin-left: 0.4rem;
    color: #b34a00;
    opacity: 0.55;
    font-size: 0.7rem;
    line-height: 1;
    vertical-align: middle;
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
