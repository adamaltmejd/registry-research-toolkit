<script lang="ts">
  import type { ColumnInfo, Panel, RegisterGroupView } from "../types";
  import {
    columnIsManual,
    columnIsMismatch,
    columnIsUnmatchedCategorical,
    store,
  } from "../store.svelte";
  import ColumnTypeEditor from "./ColumnTypeEditor.svelte";
  import CoverageCell, { type CoverageEntry } from "./CoverageCell.svelte";
  import PanelEditor from "./PanelEditor.svelte";
  import RegisterEditor from "./RegisterEditor.svelte";
  import TypeCell from "./TypeCell.svelte";
  import ValueCodesModal from "./ValueCodesModal.svelte";

  interface Props {
    group: RegisterGroupView;
  }

  let { group }: Props = $props();

  // Modal target: list of sources to apply the column-type edit to. In
  // grouped mode this is a partition's source members; in per-source
  // mode it is a single-element list. `cellBySource` covers every
  // carrier source — its keys are the register-wide reconcile target
  // and its values feed the modal's manual-override count.
  let editingColumn: {
    sources: string[];
    cellBySource: Record<string, ColumnInfo>;
    column: ColumnInfo;
  } | null = $state(null);
  let editingRegister = $state(false);
  let editingPanel = $state(false);
  // Value-codes popover target. The register comes from this group;
  // server side handles the case where no register is set (returns
  // kind="none"), but we don't open the modal at all in that case
  // since there are no codes to look up.
  let viewingValuesFor: string | null = $state(null);

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
  // Single-panel attachment is the common case; the editor expects a
  // 1:1 group↔panel relationship for the "Edit panel" affordance. When
  // multiple panels overlap a group (rare; usually a hand-edit), the
  // first one wins for editing — the others are still listed below.
  let primaryPanelForGroup = $derived(
    panelsForGroup.length > 0 ? panelsForGroup[0] : null,
  );

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

  // Compact label for the regmeta evidence badge. Classification name
  // wins over the generic "value codes" because it carries more info
  // (e.g. "LKF2012" tells the user which version of the kommun coding).
  function regmetaBadge(col: ColumnInfo): string {
    const sig = col.regmeta_signal;
    if (!sig) return "";
    if (sig.classification_short_name) return sig.classification_short_name;
    if (sig.has_value_codes) return "vc";
    return "";
  }

  // Long-form text for the badge tooltip — same source data, expanded.
  function regmetaBadgeTitle(col: ColumnInfo): string {
    const sig = col.regmeta_signal;
    if (!sig) return "";
    if (sig.classification_short_name) {
      return `regmeta classification: ${sig.classification_short_name}`;
    }
    if (sig.has_value_codes) return "regmeta: value codes available";
    return "";
  }

  // Map carrier source → its cell, indexed by column name. Built once
  // per render of this group instead of per row: the per-source view
  // looks up every column under every source, and the grouped view
  // looks up every partition. Keys of `cellsByNameMap[colName]` double
  // as the carrier list for ColumnTypeEditor's register-wide reconcile
  // target (the server rejects calls that include sources missing the
  // column).
  let cellsByNameMap = $derived.by<Record<string, Record<string, ColumnInfo>>>(
    () => {
      const m: Record<string, Record<string, ColumnInfo>> = {};
      for (const sn of group.sources) {
        for (const c of group.columns_by_source[sn] ?? []) {
          (m[c.name] ??= {})[sn] = c;
        }
      }
      return m;
    },
  );

  function cellsByName(colName: string): Record<string, ColumnInfo> {
    return cellsByNameMap[colName] ?? {};
  }

  // Concern predicates live in the store so the FilterBar's chip counts
  // and the row-level markers below stay in lockstep with each other.
  // Local aliases keep the template readable.
  const isUnmatchedCategorical = columnIsUnmatchedCategorical;
  const isRegmetaMismatch = columnIsMismatch;

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
      if (columnIsManual(c)) manual++;
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
    sample: ColumnInfo;
    /** Every ColumnInfo aggregated into this partition, in source
     * order. Filter checks scan this rather than `sample` because
     * provenance / regmeta context can differ across cells with the
     * same type+hints (e.g. one source manually edited, siblings auto). */
    cells: ColumnInfo[];
    sql_type_summary: string;
    manual_count: number;
    /** Sources in the register that do not carry this column at all.
     * Counted only on variant_index === 0 to avoid double-attribution
     * when a column has multiple type variants. */
    missing_in_count: number;
  }

  // Open state lives in the store (localStorage-backed) so reload and
  // post-mutation re-renders preserve which cards the user has opened.
  // `forceOpen` reflects active filters: when filters are on, we open
  // every group/source so the user sees what survives the filter — but
  // we never force-close, leaving the persistent state untouched.
  let forceOpen = $derived(store.hasActiveFilters());
  let groupOpen = $derived(forceOpen || store.isGroupOpen(group.group_id));
  function onGroupToggle(event: Event): void {
    const el = event.currentTarget as HTMLDetailsElement;
    // While `forceOpen` is true we don't mutate persisted state — the
    // user can't really "close" a forced-open card; the toggle attempt
    // shouldn't leak into long-term state.
    if (forceOpen) return;
    store.setGroupOpen(group.group_id, el.open);
  }

  function isSourceOpen(sn: string): boolean {
    return forceOpen || store.isSourceOpen(group.group_id, sn);
  }
  function onSourceToggle(sn: string, event: Event): void {
    if (forceOpen) return;
    const el = event.currentTarget as HTMLDetailsElement;
    store.setSourceOpen(group.group_id, sn, el.open);
  }

  // Row-level click is a mouse-only enhancement: clicking anywhere on a
  // variable row opens the type editor. Inner buttons / clickable badges
  // stopPropagation so they keep their own intent. Keyboard users tab
  // straight to the inner type-pill button — we deliberately don't put
  // role="button"/tabindex on the <tr> because nesting interactive
  // children inside an exposed-as-button row is invalid AT semantics.
  function openEditorForPartition(p: ColumnPartition): void {
    editingColumn = {
      sources: [...p.sources],
      cellBySource: cellsByName(p.name),
      column: p.sample,
    };
  }
  function openEditorForCell(sn: string, col: ColumnInfo): void {
    editingColumn = {
      sources: [sn],
      cellBySource: cellsByName(col.name),
      column: col,
    };
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
      cells: ColumnInfo[];
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
            cells: [],
            sql_types: new Map(),
            manual_count: 0,
          };
          groups.set(key, part);
        }
        part.sources.push(sn);
        part.cells.push(col);
        const sqlT = col.sql_type ?? "—";
        part.sql_types.set(sqlT, (part.sql_types.get(sqlT) ?? 0) + 1);
        if (col.provenance === "manual") part.manual_count++;
      }
      const built = Array.from(groups.values());
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
          sample: b.sample,
          cells: b.cells,
          sql_type_summary: summary,
          manual_count: b.manual_count,
          missing_in_count: i === 0 ? missing : 0,
        });
      });
    }
    return out;
  });

  // Partitions filtered by the active filter chips / search. Hide the
  // partition rather than dim it: the user asked for a focused view,
  // dimmed rows would just be visual noise. The concern check scans
  // every cell in the partition, not just `sample` — otherwise a
  // partition whose only manual cell isn't the sample would vanish
  // from the "manual" filter.
  let visiblePartitions = $derived(
    store.hasActiveFilters()
      ? partitions.filter((p) => store.columnsMatchFilters(p.cells))
      : partitions,
  );

  // Per-source view: columns to render under each source, after filters.
  // Sources with no matching columns are dropped entirely from the
  // expanded card so the user doesn't scroll past empty source rows.
  interface FilteredSource {
    name: string;
    cols: ColumnInfo[];
  }
  let filteredSources = $derived.by<FilteredSource[]>(() => {
    if (store.groupColumnsByName) return [];
    const out: FilteredSource[] = [];
    for (const sn of group.sources) {
      const all = group.columns_by_source[sn] ?? [];
      const cols = store.hasActiveFilters()
        ? all.filter((c) => store.columnMatchesFilters(c))
        : all;
      if (cols.length === 0 && store.hasActiveFilters()) continue;
      out.push({ name: sn, cols });
    }
    return out;
  });

  function coverageForPartition(p: ColumnPartition): CoverageEntry[] {
    const inVariant = new Set(p.sources);
    const carriers = cellsByName(p.name);
    return group.sources.map((src) => {
      if (inVariant.has(src)) return { source: src, status: "present" };
      if (src in carriers) return { source: src, status: "variant" };
      return { source: src, status: "missing" };
    });
  }

  function coverageForSourceColumn(
    currentSource: string,
    colName: string,
  ): CoverageEntry[] {
    const carriers = cellsByName(colName);
    return group.sources.map((src) => {
      if (src === currentSource) return { source: src, status: "self" };
      if (src in carriers) return { source: src, status: "present" };
      return { source: src, status: "missing" };
    });
  }
</script>

<details
  class="group"
  class:no-register={group.register_id === null}
  title={group.group_id}
  open={groupOpen}
  ontoggle={onGroupToggle}
>
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
          <small class="group-id">· {group.group_id}</small>
        {/if}
      </h2>
      <p class="meta">
        <span class="conf conf-{group.confidence}"
          >{CONFIDENCE_LABEL[group.confidence]}</span
        >
        · {group.sources.length} source{group.sources.length === 1 ? "" : "s"}
        {#if group.schema_variants > 1}
          · {group.schema_variants} schemas
        {/if}
        {#if primaryPanelForGroup}
          · <span class="panel-tag" title="Panel attached to this group"
            >panel</span
          >
          <code class="panel-id">{primaryPanelForGroup.panel_id}</code>
          {@const range = panelPeriodRange(primaryPanelForGroup)}
          {#if range}
            ({range}, {primaryPanelForGroup.members.length} members)
          {:else}
            ({primaryPanelForGroup.members.length} members)
          {/if}
        {:else if group.panel_candidate}
          · <span class="panel-candidate-tag" title="Auto-detected panel candidate (not yet designated)"
            >panel candidate</span
          >
          ({group.panel_candidate.members.length})
        {/if}
      </p>
    </div>
    <div class="actions">
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
      {#if primaryPanelForGroup || group.panel_candidate}
        <button
          class="link"
          onclick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            editingPanel = true;
          }}
          title={primaryPanelForGroup
            ? "Edit attached panel"
            : "Designate this group as a panel"}
        >
          {primaryPanelForGroup ? "Edit panel…" : "Designate panel…"}
        </button>
      {/if}
    </div>
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
    <!-- "missing in N" inline marker only when the Coverage column is
         hidden — otherwise the per-source map carries the same info. -->
    {@const visCols = store.visibleColumns}
    <table class="grouped-table">
      <colgroup>
        <col class="col-name" />
        {#if visCols.sql}<col class="col-sql" />{/if}
        {#if visCols.type}<col class="col-type" />{/if}
        {#if visCols.coverage}<col class="col-coverage" />{/if}
      </colgroup>
      <thead>
        <tr>
          <th>Variable</th>
          {#if visCols.sql}<th>SQL</th>{/if}
          {#if visCols.type}<th>Type</th>{/if}
          {#if visCols.coverage}<th>Coverage</th>{/if}
        </tr>
      </thead>
      <tbody>
        {#each visiblePartitions as p (p.name + "/" + p.variant_index)}
          {@const hint = hintSuffix(p.sample)}
          {@const regmeta = regmetaBadge(p.sample)}
          {@const regmetaTitle = regmetaBadgeTitle(p.sample)}
          {@const split = p.variant_count > 1}
          {@const coverage = visCols.coverage ? coverageForPartition(p) : []}
          <tr
            class="clickable-row"
            class:split
            onclick={() => openEditorForPartition(p)}
          >
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
              {#if !visCols.coverage && p.missing_in_count > 0}
                <span
                  class="missing-marker"
                  title={`${p.missing_in_count} source${p.missing_in_count === 1 ? "" : "s"} in this register do not carry this column`}
                >
                  · missing in {p.missing_in_count}
                </span>
              {/if}
            </td>
            {#if visCols.sql}
              <td class="mono dim">{p.sql_type_summary}</td>
            {/if}
            {#if visCols.type}
              {@const pillTitle =
                [p.sample.current_type, hint].filter(Boolean).join(" · ") +
                ` (${p.sources.length} source${p.sources.length === 1 ? "" : "s"}` +
                (p.manual_count > 0 ? `, ${p.manual_count} manual` : "") +
                ")"}
              <TypeCell
                column={p.sample}
                {hint}
                {pillTitle}
                showProvIndicator={false}
                {regmeta}
                {regmetaTitle}
                manualCount={p.manual_count}
                onEditType={() => openEditorForPartition(p)}
                onShowValueCodes={() => (viewingValuesFor = p.name)}
              />
            {/if}
            {#if visCols.coverage}
              <CoverageCell cells={coverage} />
            {/if}
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
    <!-- Per-source mode: original rendering (one <details> per source).
         Sources with zero matching columns under active filters are
         already removed in `filteredSources`. -->
    {@const visCols = store.visibleColumns}
    {#each filteredSources as fs (fs.name)}
      {@const sourceName = fs.name}
      {@const cols = fs.cols}
      {@const stats = statsFor(cols)}
      {@const year = sourceYears[sourceName]}
      <details
        class="source"
        open={isSourceOpen(fs.name)}
        ontoggle={(event) => onSourceToggle(fs.name, event)}
      >
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
          <colgroup>
            <col class="col-name" />
            {#if visCols.sql}<col class="col-sql" />{/if}
            {#if visCols.type}<col class="col-type" />{/if}
            {#if visCols.coverage}<col class="col-coverage" />{/if}
          </colgroup>
          <thead>
            <tr>
              <th>Variable</th>
              {#if visCols.sql}<th>SQL</th>{/if}
              {#if visCols.type}<th>Type</th>{/if}
              {#if visCols.coverage}<th>Coverage</th>{/if}
            </tr>
          </thead>
          <tbody>
            {#each cols as col (col.name)}
              {@const hint = hintSuffix(col)}
              {@const regmeta = regmetaBadge(col)}
              {@const regmetaTitle = regmetaBadgeTitle(col)}
              {@const provLabel =
                col.provenance === "manual"
                  ? "manual override"
                  : "auto-classified"}
              {@const coverage = visCols.coverage
                ? coverageForSourceColumn(sourceName, col.name)
                : []}
              <tr
                class="clickable-row"
                onclick={() => openEditorForCell(sourceName, col)}
              >
                <td class="mono col-name" title={col.name}>{col.name}</td>
                {#if visCols.sql}
                  <td class="mono dim">{col.sql_type ?? "—"}</td>
                {/if}
                {#if visCols.type}
                  {@const pillTitle =
                    [col.current_type, hint].filter(Boolean).join(" · ") +
                    ` (${provLabel})`}
                  <TypeCell
                    column={col}
                    {hint}
                    {pillTitle}
                    showProvIndicator={true}
                    {regmeta}
                    {regmetaTitle}
                    onEditType={() => openEditorForCell(sourceName, col)}
                    onShowValueCodes={() => (viewingValuesFor = col.name)}
                  />
                {/if}
                {#if visCols.coverage}
                  <CoverageCell cells={coverage} />
                {/if}
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
    cellBySource={editingColumn.cellBySource}
    registerName={group.register_name}
    column={editingColumn.column}
    onClose={() => (editingColumn = null)}
  />
{/if}

{#if editingRegister}
  <RegisterEditor {group} onClose={() => (editingRegister = false)} />
{/if}

{#if editingPanel}
  <PanelEditor
    {group}
    existing={primaryPanelForGroup}
    onClose={() => (editingPanel = false)}
  />
{/if}

{#if viewingValuesFor !== null}
  <ValueCodesModal
    register={group.register_name}
    column={viewingValuesFor}
    onClose={() => (viewingValuesFor = null)}
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
    content: "▶";
    color: #888;
    font-size: 1rem;
    line-height: 1;
    margin-top: 0.2rem;
    transition: transform 0.12s ease;
    display: inline-block;
    flex: 0 0 auto;
    width: 1.1rem;
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
  /* group_id only renders on unassigned groups, where it doubles as
     the file/source identifier. Register groups carry it in `title`
     instead so the card title isn't crowded with reg-N noise. */
  .group-id {
    font-style: normal;
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
  .panel-candidate-tag {
    text-transform: uppercase;
    font-size: 0.7rem;
    letter-spacing: 0.06em;
    color: #7a5b00;
    margin-right: 0.25rem;
  }
  .panel-id {
    background: #eef2fb;
    color: #1a3b80;
    padding: 0.05rem 0.35rem;
    border-radius: 3px;
    font-family: ui-monospace, monospace;
    font-size: 0.85em;
  }
  .actions {
    display: inline-flex;
    align-items: center;
    gap: 0.75rem;
    flex: 0 0 auto;
    flex-wrap: wrap;
    justify-content: flex-end;
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
    content: "▶";
    color: #888;
    font-size: 0.85rem;
    line-height: 1;
    transition: transform 0.12s ease;
    display: inline-block;
    width: 0.95rem;
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
    /* Top-align: keeps the type pill anchored when Coverage wraps to
       multiple lines (vertical-align: middle would leave it floating). */
    vertical-align: top;
  }
  th {
    color: #777;
    font-weight: 500;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  /* The "name" column has no fixed width — it absorbs the slack. */
  col.col-sql {
    width: 8rem;
  }
  col.col-type {
    width: 14rem;
  }
  col.col-coverage {
    width: 12rem;
  }
  /* Visually link rows belonging to the same split column. The thin
     left-border is just enough to read the grouping at a glance without
     drawing attention away from the type pills themselves. */
  .grouped-table tr.split td:first-child {
    border-left: 2px solid #d6c5e6;
    padding-left: 0.5rem;
  }
  /* Whole-row click is a mouse-only enhancement; keyboard users tab
     straight to the inner type-pill button. Inner buttons stop
     propagation so clicks on the type pill / regmeta tag stay scoped
     to those controls. */
  tr.clickable-row {
    cursor: pointer;
  }
  tr.clickable-row:hover td {
    background: #f6f9ff;
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
    content: "▶";
    color: #888;
    font-size: 0.8rem;
    line-height: 1;
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
