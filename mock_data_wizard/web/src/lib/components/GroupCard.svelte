<script lang="ts">
  import type { ColumnInfo, Panel, RegisterGroupView } from "../types";
  import { store } from "../store.svelte";
  import ColumnTypeEditor from "./ColumnTypeEditor.svelte";
  import RegisterEditor from "./RegisterEditor.svelte";

  interface Props {
    group: RegisterGroupView;
  }

  let { group }: Props = $props();

  // Modal target: list of sources to apply the column-type edit to. In
  // grouped mode this is a partition's source members; in per-source
  // mode it is a single-element list.
  let editingColumn: {
    sources: string[];
    registerSourcesWithColumn: string[];
    column: ColumnInfo;
  } | null = $state(null);
  let editingRegister = $state(false);

  const CONFIDENCE_LABEL: Record<string, string> = {
    high: "high confidence",
    partial: "partial confidence",
    none: "no confidence",
  };

  let sourceYears = $derived.by(() => {
    const m: Record<string, number | null> = {};
    const sources = store.snapshot?.config.sources ?? {};
    for (const s of group.sources) {
      m[s] = sources[s]?.year ?? null;
    }
    return m;
  });

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

  function hintSuffix(col: ColumnInfo): string {
    if (!col.hint) return "";
    return Object.values(col.hint).map(String).join(" · ");
  }

  function regmetaSuffix(col: ColumnInfo): string {
    const sig = col.regmeta_signal;
    if (!sig) return "";
    if (sig.classification_short_name) return sig.classification_short_name;
    if (sig.has_value_codes) return "value codes";
    return "";
  }

  // Per-source mode helper: list every source in the register that
  // carries `colName`. Used to feed ColumnTypeEditor's register-wide
  // reconcile target with the carriers only — sources missing the
  // column would otherwise fail server-side pair validation.
  function carriersForColumn(colName: string): string[] {
    const out: string[] = [];
    for (const sn of group.sources) {
      const cols = group.columns_by_source[sn] ?? [];
      if (cols.some((c) => c.name === colName)) out.push(sn);
    }
    return out;
  }

  function isUnmatchedCategorical(col: ColumnInfo): boolean {
    if (col.current_type !== "categorical") return false;
    return regmetaSuffix(col) === "";
  }

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

  // Partition equality key. Two (source, column-of-name) cells agree when
  // they share current_type and every active hint slot. provenance is
  // intentionally excluded — a manual override and an auto match that
  // landed on the same type are "agreeing" for bulk-edit purposes; the
  // manual count is still surfaced on the row.
  function partitionKey(col: ColumnInfo): string {
    const h = col.hint ?? {};
    return [
      col.current_type,
      String(h.id_subtype ?? ""),
      String(h.numeric_subtype ?? ""),
      String(h.date_format ?? ""),
    ].join("|");
  }

  interface ColumnPartition {
    name: string;
    variant_index: number;
    variant_count: number;
    sources: string[];
    /** Union of `sources` across every variant of this column-name —
     * i.e. all register sources that actually carry the column. Used
     * by the editor's register-wide reconcile target so the request
     * never includes sources where the column is missing. */
    carrier_sources: string[];
    sample: ColumnInfo;
    sql_type_summary: string;
    manual_count: number;
    /** Sources in the register that do not carry this column at all.
     * Counted only on variant_index === 0 to avoid double-attribution
     * when a column has multiple type variants. */
    missing_in_count: number;
  }

  let partitions: ColumnPartition[] = $derived.by(() => {
    if (!store.groupColumnsByName) return [];
    // Column name order = first-seen order across the register's sources.
    const nameOrder: string[] = [];
    const seenNames = new Set<string>();
    for (const sn of group.sources) {
      for (const c of group.columns_by_source[sn] ?? []) {
        if (!seenNames.has(c.name)) {
          nameOrder.push(c.name);
          seenNames.add(c.name);
        }
      }
    }
    interface PartBuild {
      key: string;
      sources: string[];
      sample: ColumnInfo;
      sql_types: Map<string, number>;
      manual_count: number;
    }
    const out: ColumnPartition[] = [];
    for (const name of nameOrder) {
      const groups = new Map<string, PartBuild>();
      let missing = 0;
      for (const sn of group.sources) {
        const col = (group.columns_by_source[sn] ?? []).find(
          (c) => c.name === name,
        );
        if (!col) {
          missing++;
          continue;
        }
        const key = partitionKey(col);
        let part = groups.get(key);
        if (!part) {
          part = {
            key,
            sources: [],
            sample: col,
            sql_types: new Map(),
            manual_count: 0,
          };
          groups.set(key, part);
        }
        part.sources.push(sn);
        const sqlT = col.sql_type ?? "—";
        part.sql_types.set(sqlT, (part.sql_types.get(sqlT) ?? 0) + 1);
        if (col.provenance === "manual") part.manual_count++;
      }
      const built = Array.from(groups.values());
      // Carrier set = sources across every variant; the variants
      // partition this set, so concatenation is already unique.
      const carrierSources = built.flatMap((b) => b.sources);
      built.forEach((b, i) => {
        const items = Array.from(b.sql_types.entries());
        let summary: string;
        if (items.length === 1) {
          summary = items[0][0];
        } else {
          summary = items
            .sort((a, c) => c[1] - a[1])
            .map(([t, n]) => `${t} ×${n}`)
            .join(" / ");
        }
        out.push({
          name,
          variant_index: i,
          variant_count: built.length,
          sources: b.sources,
          carrier_sources: carrierSources,
          sample: b.sample,
          sql_type_summary: summary,
          manual_count: b.manual_count,
          missing_in_count: i === 0 ? missing : 0,
        });
      });
    }
    return out;
  });
</script>

<details class="group" class:no-register={group.register_id === null}>
  <!-- Group-level collapse: header always visible, contents (panels,
       column table, source list) hidden until expanded. Default closed
       so a fresh page load shows N register summaries instead of an
       N-table wall. The "Edit register…" button stops propagation so
       clicking it doesn't also toggle the details. -->
  <summary class="group-summary">
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
          · panel candidate ({group.panel_candidate.members.length})
        {/if}
      </p>
    </div>
    <button
      class="link"
      onclick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        editingRegister = true;
      }}
    >
      Edit register…
    </button>
  </summary>

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

  {#if store.groupColumnsByName}
    <!-- Grouped mode: one row per (column name, type, hint) partition. -->
    <table class="grouped-table">
      <thead>
        <tr>
          <th>Column</th>
          <th>SQL</th>
          <th>Type</th>
        </tr>
      </thead>
      <tbody>
        {#each partitions as p (p.name + "/" + p.variant_index)}
          {@const hint = hintSuffix(p.sample)}
          {@const regmeta = regmetaSuffix(p.sample)}
          {@const mismatch = isRegmetaMismatch(p.sample)}
          {@const split = p.variant_count > 1}
          <tr class:split>
            <td class="mono col-name" title={p.name}>
              {p.name}
              {#if split}
                <span
                  class="split-marker"
                  title={`${p.variant_count} type variants for this column in this register`}
                  aria-label="split column"
                >
                  ⇅ {p.variant_index + 1}/{p.variant_count}
                </span>
              {/if}
              {#if p.missing_in_count > 0}
                <span
                  class="missing-marker"
                  title={`${p.missing_in_count} source${p.missing_in_count === 1 ? "" : "s"} in this register do not carry this column`}
                >
                  · missing in {p.missing_in_count}
                </span>
              {/if}
            </td>
            <td class="mono dim">{p.sql_type_summary}</td>
            <td>
              <button
                class="type-pill type-{p.sample.current_type}"
                title={[p.sample.current_type, hint, regmeta]
                  .filter(Boolean)
                  .join(" · ") +
                  ` (${p.sources.length} source${p.sources.length === 1 ? "" : "s"}` +
                  (p.manual_count > 0
                    ? `, ${p.manual_count} manual`
                    : "") +
                  ")"}
                onclick={() =>
                  (editingColumn = {
                    sources: [...p.sources],
                    registerSourcesWithColumn: [...p.carrier_sources],
                    column: p.sample,
                  })}
              >
                <span class="type-name">{p.sample.current_type}</span>
                {#if hint}
                  <span class="type-suffix">· {hint}</span>
                {/if}
                {#if regmeta}
                  <span class="type-suffix regmeta">· {regmeta}</span>
                {/if}
                <span class="count-badge">× {p.sources.length}</span>
                {#if p.manual_count > 0}
                  <span class="manual-badge" title="manual overrides in this group"
                    >★{p.manual_count}</span
                  >
                {/if}
              </button>
              {#if mismatch}
                <span
                  class="mismatch-marker"
                  title={`regmeta implies '${p.sample.regmeta_implied_type}' — current is '${p.sample.current_type}'`}
                  aria-label="regmeta type mismatch">⚠</span
                >
              {:else if isUnmatchedCategorical(p.sample)}
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

    <!-- Per-source detail collapsed by default — preserves source-level
         visibility (year, source name) without dominating the table. -->
    <details class="sources-detail">
      <summary>
        Sources ({group.sources.length})
      </summary>
      <ul class="source-list">
        {#each group.sources as sn (sn)}
          <li>
            <span class="mono">{sn}</span>
            {#if sourceYears[sn] !== null && sourceYears[sn] !== undefined}
              <span class="stat-year" title="detected source year"
                >{sourceYears[sn]}</span
              >
            {/if}
          </li>
        {/each}
      </ul>
    </details>
  {:else}
    <!-- Per-source mode: original rendering (one <details> per source). -->
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
            <span class="stat-cols"
              >{stats.total} col{stats.total === 1 ? "" : "s"}</span
            >
            {#if stats.unmatched > 0}
              <span
                class="stat-unmatched"
                title="categoricals without regmeta classification or value codes"
                >● {stats.unmatched} unmatched</span
              >
            {/if}
            {#if stats.mismatch > 0}
              <span
                class="stat-mismatch"
                title="auto-classified type disagrees with regmeta-implied type"
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
              {@const provLabel =
                col.provenance === "manual"
                  ? "manual override"
                  : "auto-classified"}
              <tr>
                <td class="mono col-name" title={col.name}>{col.name}</td>
                <td class="mono dim">{col.sql_type ?? "—"}</td>
                <td>
                  <button
                    class="type-pill type-{col.current_type} prov-{col.provenance}"
                    title={[col.current_type, hint, regmeta]
                      .filter(Boolean)
                      .join(" · ") + ` (${provLabel})`}
                    onclick={() =>
                      (editingColumn = {
                        sources: [sourceName],
                        registerSourcesWithColumn: carriersForColumn(col.name),
                        column: col,
                      })}
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
  {/if}
</details>

{#if editingColumn}
  <ColumnTypeEditor
    sources={editingColumn.sources}
    registerSourcesWithColumn={editingColumn.registerSourcesWithColumn}
    registerName={group.register_name}
    column={editingColumn.column}
    onClose={() => (editingColumn = null)}
  />
{/if}

{#if editingRegister}
  <RegisterEditor {group} onClose={() => (editingRegister = false)} />
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
  .group-summary {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    flex-wrap: wrap;
    cursor: pointer;
    list-style: none;
    user-select: none;
  }
  .group[open] > .group-summary {
    margin-bottom: 0.5rem;
  }
  .group-summary::-webkit-details-marker {
    display: none;
  }
  .group-summary::before {
    content: "▸";
    color: #888;
    font-size: 0.85rem;
    margin-top: 0.2rem;
    transition: transform 0.12s ease;
    display: inline-block;
    flex: 0 0 auto;
    width: 0.9rem;
  }
  .group[open] > .group-summary::before {
    transform: rotate(90deg);
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
  .grouped-table {
    margin-top: 0.5rem;
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
  /* Visually link rows belonging to the same split column. The thin
     left-border is just enough to read the grouping at a glance without
     drawing attention away from the type pills themselves. */
  .grouped-table tr.split td:first-child {
    border-left: 2px solid #d6c5e6;
    padding-left: 0.5rem;
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
  .split-marker {
    color: #6f4ca0;
    font-size: 0.78rem;
    margin-left: 0.35rem;
    font-family: system-ui, sans-serif;
  }
  .missing-marker {
    color: #888;
    font-size: 0.78rem;
    margin-left: 0.35rem;
    font-family: system-ui, sans-serif;
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
    background: #f4f0e8;
    color: #5a523f;
    border-color: #d8d0bf;
  }
  .type-suffix {
    margin-left: 0.15rem;
    opacity: 0.65;
    font-size: 0.85em;
  }
  .count-badge {
    margin-left: 0.4rem;
    padding: 0 0.35rem;
    border-radius: 999px;
    background: rgba(0, 0, 0, 0.08);
    font-size: 0.78em;
    font-family: system-ui, sans-serif;
    font-weight: 600;
  }
  .manual-badge {
    margin-left: 0.25rem;
    color: #b34a00;
    font-size: 0.8em;
    font-family: system-ui, sans-serif;
  }
  .unmatched-marker {
    margin-left: 0.4rem;
    color: #b34a00;
    opacity: 0.55;
    font-size: 0.7rem;
    line-height: 1;
    vertical-align: middle;
  }
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
  .sources-detail {
    margin-top: 0.6rem;
    border-top: 1px solid #f0f0f0;
    padding-top: 0.4rem;
  }
  .sources-detail > summary {
    cursor: pointer;
    color: #666;
    font-size: 0.85rem;
    user-select: none;
    list-style: none;
  }
  .sources-detail > summary::-webkit-details-marker {
    display: none;
  }
  .sources-detail > summary::before {
    content: "▸";
    color: #888;
    font-size: 0.7rem;
    margin-right: 0.4rem;
    transition: transform 0.12s ease;
    display: inline-block;
  }
  .sources-detail[open] > summary::before {
    transform: rotate(90deg);
  }
  .source-list {
    list-style: none;
    padding: 0.4rem 0 0 1.2rem;
    margin: 0;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(14rem, 1fr));
    gap: 0.2rem 0.75rem;
    font-size: 0.85rem;
    color: #555;
  }
  .source-list li {
    display: flex;
    gap: 0.4rem;
    align-items: center;
  }
</style>
