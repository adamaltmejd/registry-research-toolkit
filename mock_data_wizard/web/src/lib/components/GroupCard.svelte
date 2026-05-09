<script lang="ts">
  import type { ColumnInfo, Panel, RegisterGroupView } from "../types";
  import { store } from "../store.svelte";
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

  // Per-source year metadata lives on the snapshot's config.sources, not
  // on the column-level data the table renders. Pull it out once for
  // each source row so panels of yearly CSVs read at a glance.
  let sourceYears = $derived.by(() => {
    const m: Record<string, number | null> = {};
    const sources = store.snapshot?.config.sources ?? {};
    for (const s of group.sources) {
      m[s] = sources[s]?.year ?? null;
    }
    return m;
  });

  // Panel definitions touching this group. Typical case is zero or one,
  // but we render every match rather than `find`-ing the first to avoid
  // silently dropping a second panel if the data model ever lets a
  // group's sources span more than one panel.
  let panelsForGroup = $derived.by(() => {
    const panels = store.snapshot?.config.panels ?? [];
    const groupSourceSet = new Set(group.sources);
    return panels.filter((p) =>
      p.members.some((m) => groupSourceSet.has(m.source)),
    );
  });

  function panelPeriodRange(panel: Panel): string | null {
    const periods = panel.members
      .map((m) => m.period)
      .filter((p): p is number => typeof p === "number");
    if (periods.length === 0) return null;
    const min = Math.min(...periods);
    const max = Math.max(...periods);
    return min === max ? `${min}` : `${min}–${max}`;
  }

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

  // Current type disagrees with what regmeta would classify the column
  // as. This is the main signal that a manual override may be needed.
  // Manual overrides are exempt — the user has already decided.
  function isRegmetaMismatch(col: ColumnInfo): boolean {
    if (col.provenance === "manual") return false;
    if (col.regmeta_implied_type === null) return false;
    return col.regmeta_implied_type !== col.current_type;
  }

  interface SourceStats {
    total: number;
    unmatched: number;
    manual: number;
    mismatch: number;
  }
  function statsFor(cols: ColumnInfo[]): SourceStats {
    let unmatched = 0;
    let manual = 0;
    let mismatch = 0;
    for (const c of cols) {
      if (isUnmatchedCategorical(c)) unmatched++;
      if (c.provenance === "manual") manual++;
      if (isRegmetaMismatch(c)) mismatch++;
    }
    return { total: cols.length, unmatched, manual, mismatch };
  }
</script>

<section class="group" class:no-register={group.register_id === null}>
  <header>
    <div class="title-block">
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
        {#if group.panel_candidate && panelsForGroup.length === 0}
          <!-- Candidate detection runs even after a panel is registered;
               only flag it as a *candidate* when the group hasn't been
               promoted yet, otherwise the PANEL summary block below
               shows the same files twice. -->
          · panel candidate ({group.panel_candidate.members.length})
        {/if}
      </p>
    </div>
    <button class="link" onclick={() => (editingRegister = true)}>
      Edit register…
    </button>
  </header>

  {#each panelsForGroup as panel (panel.panel_id)}
    {@const range = panelPeriodRange(panel)}
    <p class="panel-summary" title="Panel definition (config.panels)">
      <span class="panel-tag">panel</span>
      <code>{panel.panel_id}</code>
      · keyed on <code>{panel.panel_key}</code>
      {#if range}
        · {range} ({panel.members.length} files)
      {:else}
        · {panel.members.length} members
      {/if}
    </p>
  {/each}

  {#each group.sources as sourceName (sourceName)}
    {@const cols = group.columns_by_source[sourceName] ?? []}
    {@const stats = statsFor(cols)}
    {@const year = sourceYears[sourceName]}
    <details class="source">
      <summary>
        <span class="source-name mono">{sourceName}</span>
        <span class="source-stats">
          {#if year !== null && year !== undefined}
            <span class="stat-year" title="detected source year">{year}</span>
          {/if}
          <span class="stat-cols">{stats.total} col{stats.total === 1 ? "" : "s"}</span>
          {#if stats.unmatched > 0}
            <span class="stat-unmatched" title="categoricals without regmeta classification or value codes"
              >● {stats.unmatched} unmatched</span
            >
          {/if}
          {#if stats.mismatch > 0}
            <span class="stat-mismatch" title="auto-classified type disagrees with regmeta-implied type"
              >⚠ {stats.mismatch} regmeta mismatch</span
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
            {@const mismatch = isRegmetaMismatch(col)}
            {@const provLabel = col.provenance === "manual" ? "manual override" : "auto-classified"}
            <tr>
              <td class="mono col-name" title={col.name}>{col.name}</td>
              <td class="mono dim">{col.sql_type ?? "—"}</td>
              <td>
                <button
                  class="type-pill type-{col.current_type} prov-{col.provenance}"
                  title={[col.current_type, hint, regmeta].filter(Boolean).join(" · ") + ` (${provLabel})`}
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
                {#if mismatch}
                  <span
                    class="mismatch-marker"
                    title={`regmeta implies '${col.regmeta_implied_type}' — current is '${col.current_type}'`}
                    aria-label="regmeta type mismatch">⚠</span
                  >
                {:else if isUnmatchedCategorical(col)}
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
    flex-wrap: wrap;
  }
  .title-block {
    min-width: 0;
    flex: 1 1 16rem;
  }
  h2 {
    margin: 0;
    font-size: 1.1rem;
    word-break: break-word;
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
  .panel-summary {
    margin: 0 0 0.5rem;
    padding: 0.4rem 0.6rem;
    background: #f4f6fb;
    border-left: 3px solid #c8d3ec;
    border-radius: 3px;
    font-size: 0.85rem;
    color: #444;
  }
  .panel-summary code {
    background: #fff;
    padding: 0.05rem 0.3rem;
    border-radius: 3px;
    font-family: ui-monospace, monospace;
    font-size: 0.95em;
  }
  .panel-tag {
    text-transform: uppercase;
    font-size: 0.7rem;
    letter-spacing: 0.06em;
    color: #1a3b80;
    margin-right: 0.25rem;
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
    flex: 0 0 auto;
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
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .source-stats {
    display: inline-flex;
    align-items: center;
    gap: 0.65rem;
    font-size: 0.82rem;
    color: #666;
    flex: 0 0 auto;
    flex-wrap: wrap;
    justify-content: flex-end;
  }
  .stat-year {
    background: #eef2fb;
    color: #1a3b80;
    padding: 0.05rem 0.35rem;
    border-radius: 3px;
    font-family: ui-monospace, monospace;
    font-size: 0.78rem;
  }
  .stat-cols {
    color: #888;
  }
  .stat-unmatched {
    color: #b34a00;
    opacity: 0.85;
  }
  .stat-mismatch {
    color: #b34a00;
    font-weight: 600;
  }
  .stat-manual {
    color: #b34a00;
    font-weight: 600;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
    table-layout: fixed;
  }
  th,
  td {
    text-align: left;
    padding: 0.3rem 0.4rem;
    border-bottom: 1px solid #f0f0f0;
    vertical-align: middle;
  }
  th {
    color: #777;
    font-weight: 500;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  th:nth-child(1) {
    width: 50%;
  }
  th:nth-child(2) {
    width: 18%;
  }
  th:nth-child(3) {
    width: 32%;
  }
  .mono {
    font-family: ui-monospace, monospace;
  }
  .col-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
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
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
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
  /* Warmer than disabled-grey so the user doesn't read it as "not
     interactive". Kept neutral enough not to compete with the typed
     pills. */
  .type-opaque {
    background: #f4f0e8;
    color: #5a523f;
    border-color: #d8d0bf;
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
  /* Stronger signal than .unmatched-marker — current type *contradicts*
     regmeta, which usually means the auto-classifier picked something
     the user should look at. */
  .mismatch-marker {
    margin-left: 0.4rem;
    color: #b34a00;
    font-size: 0.85rem;
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
    flex: 0 0 auto;
    white-space: nowrap;
  }
  .link:hover {
    text-decoration: underline;
  }
</style>
