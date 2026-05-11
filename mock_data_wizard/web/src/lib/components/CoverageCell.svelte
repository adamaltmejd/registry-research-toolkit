<script module lang="ts">
  // "variant" only fires in grouped mode (source carries the column but
  // in a different type variant); "self" only fires in per-source mode
  // (the row's own source).
  export type CoverageStatus = "present" | "variant" | "missing" | "self";
  export interface CoverageEntry {
    source: string;
    status: CoverageStatus;
  }
</script>

<script lang="ts">
  interface Props {
    cells: CoverageEntry[];
  }

  let { cells }: Props = $props();

  function tooltip(cell: CoverageEntry): string {
    switch (cell.status) {
      case "present":
        return `${cell.source} — present`;
      case "variant":
        return `${cell.source} — different type variant`;
      case "missing":
        return `${cell.source} — missing`;
      case "self":
        return `${cell.source} — this row`;
    }
  }

  let ariaLabel = $derived.by(() => {
    const present = cells.filter(
      (c) => c.status === "present" || c.status === "self",
    ).length;
    return `${present}/${cells.length} sources present`;
  });
</script>

<td class="coverage-cell">
  <div class="coverage-grid" role="img" aria-label={ariaLabel}>
    {#each cells as cell (cell.source)}
      <span
        class="coverage-box coverage-{cell.status}"
        title={tooltip(cell)}
      ></span>
    {/each}
  </div>
</td>

<style>
  /* One box per source in group.sources order. Variant (amber) only
     applies in grouped mode. Capped to ~6 wrap-rows so registers with
     hundreds of sources can't balloon a single table row vertically;
     the aria-label carries exact counts for assistive tech. The flex
     container stays inside the <td> — display: flex on the cell itself
     would break the CSS table layout. */
  .coverage-cell {
    /* Lift baseline so the first wrap-row aligns with the type pill,
       which sits slightly higher than a 10px box. */
    padding: 0.45rem 0.4rem 0.3rem;
    border-bottom: 1px solid #f0f0f0;
    vertical-align: top;
  }
  .coverage-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 2px;
    max-height: 4.5rem;
    overflow: hidden;
  }
  .coverage-box {
    width: 10px;
    height: 10px;
    border-radius: 2px;
    box-sizing: border-box;
    border: 1px solid transparent;
    flex: 0 0 auto;
  }
  .coverage-present {
    background: #b8e0c2;
    border-color: #5ca06f;
  }
  .coverage-variant {
    /* Source carries the column but classified to a different type
       variant in this register — neither "present in this row" nor
       "missing entirely". Amber sits in between visually. */
    background: #f5d99b;
    border-color: #c08a30;
  }
  .coverage-missing {
    background: #f0c2c2;
    border-color: #b85050;
  }
  .coverage-self {
    /* High-contrast so the row's own source pops while scanning. */
    background: #4ca866;
    border-color: #1a661a;
  }
</style>
