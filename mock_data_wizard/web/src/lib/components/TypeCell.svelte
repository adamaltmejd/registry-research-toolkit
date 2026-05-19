<script lang="ts">
  import {
    columnIsMismatch,
    columnIsUnmatchedCategorical,
  } from "../store.svelte";
  import { TYPE_LABEL_SHORT, type ColumnInfo } from "../types";

  interface Props {
    column: ColumnInfo;
    hint: string;
    pillTitle: string;
    /** Apply the `prov-manual` left-border on the pill when this column
     * is a manual override. False in grouped mode — the row aggregates
     * multiple sources, so a single-cell indicator would be misleading;
     * the manual-count badge carries the same info there. True in
     * per-source mode. (Other provenance values have no styling.) */
    showManualOverrideBorder: boolean;
    reg_meta: string;
    reg_metaTitle: string;
    /** Render the reg_meta badge with the "varies" style when the column
     * maps to multiple classifications across years. */
    reg_metaVaries?: boolean;
    /** 0 hides the badge; only meaningful in grouped mode. */
    manualCount?: number;
    onEditType: () => void;
    onShowValueCodes: () => void;
  }

  let {
    column,
    hint,
    pillTitle,
    showManualOverrideBorder,
    reg_meta,
    reg_metaTitle,
    reg_metaVaries = false,
    manualCount = 0,
    onEditType,
    onShowValueCodes,
  }: Props = $props();

  let mismatch = $derived(columnIsMismatch(column));
  let unmatched = $derived(columnIsUnmatchedCategorical(column));

  function handlePillClick(e: MouseEvent): void {
    e.stopPropagation();
    onEditType();
  }
  function handleRegMetaClick(e: MouseEvent): void {
    e.stopPropagation();
    onShowValueCodes();
  }
</script>

<td class="type-cell">
  <div class="type-cell-inner">
    <button
      class="type-pill type-{column.current_type}"
      class:prov-manual={showManualOverrideBorder && column.provenance === "manual"}
      title={pillTitle}
      onclick={handlePillClick}
    >
      <span class="type-name">{TYPE_LABEL_SHORT[column.current_type]}</span>
      {#if hint}
        <span class="type-suffix">· {hint}</span>
      {/if}
    </button>
    {#if reg_meta}
      <button
        type="button"
        class="reg-meta-tag"
        class:varies={reg_metaVaries}
        title={`${reg_metaTitle} — click to load value codes`}
        onclick={handleRegMetaClick}>{reg_meta}</button
      >
    {/if}
    {#if manualCount > 0}
      <span class="manual-badge" title="manual overrides in this partition"
        >★{manualCount}</span
      >
    {/if}
    {#if mismatch}
      <span
        class="mismatch-marker"
        title={`reg_meta implies '${column.reg_meta_implied_type}' — current is '${column.current_type}'`}
        aria-label="reg_meta type mismatch">⚠</span
      >
    {:else if unmatched}
      <span
        class="unmatched-marker"
        title="categorical without reg_meta classification or value codes"
        aria-label="unmatched categorical">●</span
      >
    {/if}
  </div>
</td>

<style>
  .type-cell {
    padding: 0.3rem 0.4rem;
    border-bottom: 1px solid #f0f0f0;
    vertical-align: top;
  }
  /* Flex on the inner div — NOT on the td. `display: flex` on a <td>
     breaks the cell out of CSS table layout, so the row no longer keeps
     sibling cells at a shared height (visible as misaligned row borders
     when Coverage forces the row taller). */
  .type-cell-inner {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem;
    align-items: center;
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
    white-space: nowrap;
    flex: 0 0 auto;
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
  /* RegMeta evidence as a sibling tag rather than a pill suffix:
     classification short-name ("LKF2012") or "vc" for value codes,
     full text in the tooltip. Keeping it outside the pill lets the
     pill stay readable even when the cell is narrow. */
  .reg-meta-tag {
    padding: 0.05rem 0.35rem;
    border: 1px solid transparent;
    border-radius: 3px;
    background: #f0e8fa;
    color: #5d2b8c;
    font-size: 0.78em;
    font-family: ui-monospace, monospace;
    cursor: pointer;
    flex: 0 0 auto;
    line-height: 1.3;
  }
  .reg-meta-tag:hover {
    background: #e3d4f4;
    border-color: #c8b1e2;
  }
  .reg-meta-tag:focus-visible {
    outline: 2px solid #5d2b8c;
    outline-offset: 1px;
  }
  .reg-meta-tag.varies {
    background: #fff0d9;
    color: #7a4a00;
    border-color: #e8c184;
  }
  .reg-meta-tag.varies:hover {
    background: #fde2b6;
    border-color: #d6a85a;
  }
  .manual-badge {
    color: #b34a00;
    font-size: 0.8em;
    font-family: system-ui, sans-serif;
    flex: 0 0 auto;
  }
  .unmatched-marker {
    color: #b34a00;
    opacity: 0.55;
    font-size: 0.7rem;
    line-height: 1;
    flex: 0 0 auto;
  }
  .mismatch-marker {
    color: #b34a00;
    font-size: 0.85rem;
    line-height: 1;
    flex: 0 0 auto;
  }
</style>
