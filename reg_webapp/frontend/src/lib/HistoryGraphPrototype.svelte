<script lang="ts">
import {
  type HistoryColumnSlice,
  type HistoryGraph,
  type HistoryGraphEdge,
  historyGraphYears,
} from "./history_graph";

let { graph }: { graph: HistoryGraph } = $props();

const width = 760;
const leftPad = 258;
const rightPad = 34;
const topPad = 44;
const rowHeight = 68;
const barHeight = 18;
const domain = $derived(historyGraphYears(graph));
const innerWidth = $derived(width - leftPad - rightPad);
const height = $derived(
  topPad + Math.max(graph.nodes.length, 1) * rowHeight + 26,
);
const span = $derived(domain.max - domain.min || 1);
const nodeIds = $derived(new Set(graph.nodes.map((node) => node.id)));
const edgeRows = $derived(
  graph.edges.filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to)),
);

function xForYear(year: number | null): number {
  const value = year ?? domain.max;
  return leftPad + ((value - domain.min) / span) * innerWidth;
}

function rowY(index: number): number {
  return topPad + index * rowHeight;
}

function displayTo(to: number | null): number {
  return to ?? domain.max;
}

function nodeIndex(id: string): number {
  return graph.nodes.findIndex((node) => node.id === id);
}

function nodeBar(node: { from: number | null; to: number | null }): {
  x: number;
  width: number;
} {
  const from = node.from ?? domain.min;
  const to = displayTo(node.to);
  const start = xForYear(from);
  const minimumWidth = node.from !== null && node.from === node.to ? 18 : 10;
  return { x: start, width: Math.max(xForYear(to) - start, minimumWidth) };
}

function columnBar(column: HistoryColumnSlice): { x: number; width: number } {
  const from = column.from ?? domain.min;
  const to = displayTo(column.to);
  const start = xForYear(from);
  return { x: start, width: Math.max(xForYear(to) - start, 6) };
}

function edgePath(edge: HistoryGraphEdge): string {
  const fromIndex = nodeIndex(edge.from);
  const toIndex = nodeIndex(edge.to);
  const fromY = rowY(fromIndex) + barHeight / 2;
  const toY = rowY(toIndex) + barHeight / 2;
  const year = edge.fromYear ?? edge.toYear;
  const x = xForYear(year);
  const fromX = year === null ? nodeBar(graph.nodes[fromIndex]).x : x;
  const toX = year === null ? nodeBar(graph.nodes[toIndex]).x : x;
  const midX = year === null ? Math.min(fromX, toX) - 16 : x;
  return `M ${fromX} ${fromY} C ${midX} ${fromY}, ${midX} ${toY}, ${toX} ${toY}`;
}

function shortLabel(label: string): string {
  return label.length > 29 ? `${label.slice(0, 28)}...` : label;
}
</script>

{#if graph.nodes.length > 0}
  <section class="history-graph" aria-labelledby={`${graph.mode}-history-graph-heading`}>
    <div class="heading-row">
      <h3 id={`${graph.mode}-history-graph-heading`}>{graph.title}</h3>
      <span class="mode">{graph.mode}</span>
    </div>

    <div class="graph-shell">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${graph.mode} history graph prototype`}
      >
        <g class="axis">
          <line
            x1={leftPad}
            y1="20"
            x2={width - rightPad}
            y2="20"
            vector-effect="non-scaling-stroke"
          />
          <text x={leftPad} y="13">{domain.min}</text>
          <text x={width - rightPad} y="13" text-anchor="end">{domain.max}</text>
        </g>

        <g class="edges">
          {#each edgeRows as edge (edge.id)}
            <path class={edge.kind} d={edgePath(edge)} />
          {/each}
        </g>

        <g class="nodes">
          {#each graph.nodes as node, i (node.id)}
            {@const bar = nodeBar(node)}
            {@const y = rowY(i)}
            {@const label = shortLabel(node.label)}
            <g class={`node ${node.kind}`} class:self={node.self} class:current={node.current}>
              {#if label !== node.label}
                <title>{node.label}</title>
              {/if}
              <text class="node-label" x="0" y={y + 13}>
                {label}
              </text>
              {#if node.detail}
                <text class="detail" x="0" y={y + 31}>{node.detail}</text>
              {/if}
              <rect
                class="bar"
                x={bar.x}
                y={y}
                width={bar.width}
                height={barHeight}
                rx="4"
              />
              {#if node.columns.length > 0}
                {#each node.columns as column, ci (column.id)}
                  {@const col = columnBar(column)}
                  <rect
                    class="column-slice"
                    x={col.x}
                    y={y + 24 + (ci % 3) * 9}
                    width={col.width}
                    height="6"
                    rx="2"
                  />
                {/each}
                {#if node.columns.length > 3}
                  <text class="column-count" x={leftPad} y={y + 58}>
                    {node.columns.length} columns
                  </text>
                {/if}
              {/if}
            </g>
          {/each}
        </g>
      </svg>
    </div>

    <div class="legend" aria-label="Graph edge legend">
      <span><i class="succession"></i>succession</span>
      <span><i class="related"></i>related</span>
      <span><i class="lineage"></i>lineage</span>
      <span><i class="member"></i>member</span>
    </div>

    {#if graph.warnings.length > 0}
      <details class="contract-gaps" open={edgeRows.length === 0}>
        <summary>Contract gaps</summary>
        <ul>
          {#each graph.warnings as warning (warning)}
            <li>{warning}</li>
          {/each}
        </ul>
      </details>
    {/if}
  </section>
{/if}

<style>
  .history-graph {
    margin-top: 1.5rem;
  }
  .heading-row {
    display: flex;
    align-items: baseline;
    gap: var(--space-2, 0.5rem);
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.25rem;
    margin-bottom: 0.75rem;
  }
  h3 {
    margin: 0;
  }
  .mode {
    color: var(--muted);
    font-size: var(--text-sm, 0.9rem);
  }
  .graph-shell {
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius, 8px);
    background: var(--surface, #fff);
  }
  svg {
    display: block;
    width: 100%;
    height: auto;
  }
  .axis line {
    stroke: var(--border);
  }
  .axis text,
  .detail,
  .column-count {
    fill: var(--muted);
    font-size: 12px;
  }
  .node-label {
    fill: currentColor;
    font-size: 14px;
    font-weight: 600;
  }
  .bar {
    fill: var(--surface-selected, #e6f0ff);
    stroke: var(--accent);
    stroke-width: 1.2;
  }
  .group .bar {
    fill: var(--surface-hover, #f5f5f5);
    stroke: var(--muted);
  }
  .classification .bar {
    fill: #f0efe8;
    stroke: #8a6f2a;
  }
  .classification.current .bar {
    fill: #eadfbd;
  }
  .self .bar {
    stroke-width: 2;
  }
  .current .bar {
    filter: saturate(1.2);
  }
  .column-slice {
    fill: var(--accent);
    opacity: 0.72;
  }
  .edges path {
    fill: none;
    stroke-width: 1.6;
    vector-effect: non-scaling-stroke;
  }
  .edges .succession {
    stroke: var(--accent);
  }
  .edges .related {
    stroke: #7a5ca7;
    stroke-dasharray: 5 4;
  }
  .edges .lineage {
    stroke: #386f6b;
    stroke-dasharray: 2 4;
  }
  .edges .member {
    stroke: var(--muted);
    stroke-dasharray: 3 4;
  }
  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem 1rem;
    margin-top: 0.5rem;
    color: var(--muted);
    font-size: var(--text-sm, 0.9rem);
  }
  .legend span {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }
  .legend i {
    width: 1.2rem;
    border-top: 2px solid var(--muted);
  }
  .legend .succession {
    border-color: var(--accent);
  }
  .legend .related {
    border-color: #7a5ca7;
    border-top-style: dashed;
  }
  .legend .lineage {
    border-color: #386f6b;
    border-top-style: dotted;
  }
  .legend .member {
    border-top-style: dashed;
  }
  .contract-gaps {
    margin-top: 0.5rem;
    color: var(--muted);
    font-size: var(--text-sm, 0.9rem);
  }
  .contract-gaps ul {
    margin: 0.4rem 0 0;
    padding-left: 1.25rem;
  }
</style>
