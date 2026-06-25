<script lang="ts">
import { Collapsible } from "bits-ui";
import { catalogHref } from "./catalog";
import {
  type HistoryColumnSlice,
  type HistoryGraph,
  type HistoryGraphEdge,
  type HistoryGraphEdgeKind,
  type HistoryGraphNode,
  hasRenderableHistoryGraph,
  historyGraphYears,
} from "./history_graph";

let { graph, vintageYear }: { graph: HistoryGraph; vintageYear?: number } =
  $props();

const width = 760;
const leftPad = 56;
const rightPad = 34;
const topPad = 44;
const rowHeight = 68;
const graphNodeWidth = 164;
const graphNodeHeight = 30;
const columnLaneHeight = 6;
const columnLaneGap = 3;
const columnLaneTop = 30;
const graphSidePad = 28;
const domain = $derived(historyGraphYears(graph, vintageYear));
const innerWidth = $derived(width - leftPad - rightPad);
const span = $derived(domain.max - domain.min || 1);
const nodeIds = $derived(new Set(graph.nodes.map((node) => node.id)));
const edgeRows = $derived(
  graph.edges.filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to)),
);
const hasTimeAxis = $derived(graph.mode !== "classification");
const edgeLegend = $derived.by(() => {
  const present = new Set(edgeRows.map((edge) => edge.kind));
  const entries: { kind: HistoryGraphEdgeKind; label: string }[] = [
    { kind: "succession", label: "succession" },
    { kind: "related", label: "related" },
    { kind: "lineage", label: "lineage" },
    { kind: "member", label: "member" },
  ];
  return entries.filter((entry) => present.has(entry.kind));
});
const equalSpacingLayout = $derived.by(() => {
  const depthById = new Map(graph.nodes.map((node) => [node.id, 0]));
  for (let pass = 0; pass < graph.nodes.length; pass += 1) {
    let changed = false;
    for (const edge of edgeRows) {
      const nextDepth = (depthById.get(edge.from) ?? 0) + 1;
      if (nextDepth > (depthById.get(edge.to) ?? 0)) {
        depthById.set(edge.to, nextDepth);
        changed = true;
      }
    }
    if (!changed) {
      break;
    }
  }
  const maxDepth = Math.max(
    ...graph.nodes.map((node) => depthById.get(node.id) ?? 0),
    0,
  );
  const nextRowByDepth = new Map<number, number>();
  const xSpan = width - graphSidePad * 2 - graphNodeWidth;
  return graph.nodes.map((node) => {
    const depth = depthById.get(node.id) ?? 0;
    const row = nextRowByDepth.get(depth) ?? 0;
    nextRowByDepth.set(depth, row + 1);
    return {
      id: node.id,
      depth,
      row,
      x: graphSidePad + (maxDepth === 0 ? 0 : (depth / maxDepth) * xSpan),
      y: topPad + row * rowHeight,
      width: graphNodeWidth,
      height: graphNodeHeight,
    };
  });
});
const relationRowCount = $derived(
  Math.max(...equalSpacingLayout.map((item) => item.row), 0) + 1,
);
const height = $derived(
  topPad +
    Math.max(hasTimeAxis ? graph.nodes.length : relationRowCount, 1) *
      rowHeight +
    26,
);
const hasGraphContent = $derived(hasRenderableHistoryGraph(graph));

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

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function labelMinWidth(label: string): number {
  return Math.min(graphNodeWidth, Math.max(58, label.length * 7 + 22));
}

function distinctColumnLabels(node: HistoryGraphNode): string[] {
  return [...new Set(node.columns.map((column) => column.label))];
}

function columnCaption(node: HistoryGraphNode): string | null {
  const labels = distinctColumnLabels(node);
  if (labels.length === 2) {
    return labels.join(" -> ");
  }
  if (labels.length === 3) {
    return labels.join(" · ");
  }
  if (labels.length > 3) {
    return `${labels.length} columns`;
  }
  const valueLabels = [
    ...new Set(
      node.columns
        .map((column) => column.valueSetLabel ?? null)
        .filter((label): label is string => label !== null),
    ),
  ];
  if (valueLabels.length === 2) {
    return valueLabels.join(" -> ");
  }
  if (valueLabels.length === 3) {
    return valueLabels.join(" · ");
  }
  if (valueLabels.length > 3) {
    return `${valueLabels.length} value sets`;
  }
  return null;
}

function nodeTextWidthLabel(node: HistoryGraphNode): string {
  return [shortLabel(node.label, 22), columnCaption(node) ?? ""].reduce(
    (longest, value) => (value.length > longest.length ? value : longest),
    "",
  );
}

function nodeIndex(id: string): number {
  return graph.nodes.findIndex((node) => node.id === id);
}

function nodeBar(
  node: HistoryGraphNode,
  index: number,
): {
  x: number;
  y: number;
  width: number;
  height: number;
} {
  if (!hasTimeAxis) {
    return (
      equalSpacingLayout[index] ?? {
        x: graphSidePad,
        y: rowY(index),
        width: graphNodeWidth,
        height: graphNodeHeight,
      }
    );
  }
  const from = node.from ?? domain.min;
  const to = displayTo(node.to);
  const start = xForYear(from);
  const end = xForYear(to);
  const timeWidth = Math.max(end - start, 10);
  const maxWidth = width - leftPad - rightPad;
  const displayWidth = Math.min(
    Math.max(timeWidth, labelMinWidth(nodeTextWidthLabel(node))),
    maxWidth,
  );
  const center = start + timeWidth / 2;
  const maxX = width - rightPad - displayWidth;
  return {
    x: clamp(center - displayWidth / 2, leftPad, Math.max(leftPad, maxX)),
    y: rowY(index),
    width: displayWidth,
    height: nodeHeight(node),
  };
}

function nodeHeight(node: HistoryGraphNode): number {
  if (!hasTimeAxis || node.columns.length === 0) {
    return graphNodeHeight;
  }
  const lanes = Math.min(node.columns.length, 3);
  return columnLaneTop + lanes * (columnLaneHeight + columnLaneGap) + 10;
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
  const fromNode = graph.nodes[fromIndex];
  const toNode = graph.nodes[toIndex];
  if (!fromNode || !toNode) {
    return "";
  }
  const fromBar = nodeBar(fromNode, fromIndex);
  const toBar = nodeBar(toNode, toIndex);
  const fromY = fromBar.y + fromBar.height / 2;
  const toY = toBar.y + toBar.height / 2;
  if (!hasTimeAxis) {
    const fromX = fromBar.x + fromBar.width;
    const toX = toBar.x;
    const midX = fromX + (toX - fromX) / 2;
    return `M ${fromX} ${fromY} C ${midX} ${fromY}, ${midX} ${toY}, ${toX} ${toY}`;
  }
  const year = edge.fromYear ?? edge.toYear;
  const yearX = year === null ? null : xForYear(year);
  const fromX =
    yearX === null
      ? fromBar.x + fromBar.width
      : clamp(yearX, fromBar.x, fromBar.x + fromBar.width);
  const toX =
    yearX === null ? toBar.x : clamp(yearX, toBar.x, toBar.x + toBar.width);
  const midX = yearX === null ? fromX + (toX - fromX) / 2 : yearX;
  return `M ${fromX} ${fromY} C ${midX} ${fromY}, ${midX} ${toY}, ${toX} ${toY}`;
}

function shortLabel(label: string, max = 29): string {
  return label.length > max ? `${label.slice(0, max - 1)}...` : label;
}
</script>

{#if hasGraphContent}
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
        {#if hasTimeAxis}
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
        {/if}

        <g class="edges">
          {#each edgeRows as edge (edge.id)}
            <path class={edge.kind} d={edgePath(edge)} />
          {/each}
        </g>

        <g class="nodes">
          {#each graph.nodes as node, i (node.id)}
            {@const bar = nodeBar(node, i)}
            {@const label = shortLabel(
              node.label,
              22,
            )}
            {@const caption = columnCaption(node)}
            {#snippet nodeGlyph()}
              <g class={`node ${node.kind}`} class:self={node.self} class:current={node.current}>
                {#if label !== node.label || !hasTimeAxis}
                  <title>{node.label}</title>
                {/if}
                <rect
                  class="bar"
                  x={bar.x}
                  y={bar.y}
                  width={bar.width}
                  height={bar.height}
                  rx="4"
                />
                <text
                  class="node-label in-bar"
                  x={bar.x + bar.width / 2}
                  y={bar.y + 18}
                  text-anchor="middle"
                >
                  {label}
                </text>
                {#if hasTimeAxis && node.columns.length > 0}
                  {#each node.columns as column, ci (column.id)}
                    {@const col = columnBar(column)}
                    <rect
                      class="column-slice"
                      x={col.x}
                      y={bar.y + columnLaneTop + (ci % 3) * (columnLaneHeight + columnLaneGap)}
                      width={col.width}
                      height={columnLaneHeight}
                      rx="2"
                    />
                  {/each}
                  {#if caption}
                    <text
                      class="column-count"
                      x={bar.x + bar.width - 8}
                      y={bar.y + bar.height - 4}
                      text-anchor="end"
                    >
                      {shortLabel(caption, 26)}
                    </text>
                  {/if}
                {/if}
              </g>
            {/snippet}
            {#if node.fqid}
              <a href={catalogHref(node.fqid)} class="node-link" aria-label={`Open ${node.label}`}>
                {@render nodeGlyph()}
              </a>
            {:else}
              {@render nodeGlyph()}
            {/if}
          {/each}
        </g>
      </svg>
    </div>

    {#if edgeLegend.length > 0}
      <div class="legend" aria-label="Graph edge legend">
        {#each edgeLegend as entry (entry.kind)}
          <span><i class={entry.kind}></i>{entry.label}</span>
        {/each}
      </div>
    {/if}

    {#if graph.warnings.length > 0}
      <Collapsible.Root class="contract-gaps">
        <Collapsible.Trigger class="disclosure-trigger">
          Contract gaps
        </Collapsible.Trigger>
        <Collapsible.Content>
          <ul class="contract-gap-list">
            {#each graph.warnings as warning (warning)}
              <li>{warning}</li>
            {/each}
          </ul>
        </Collapsible.Content>
      </Collapsible.Root>
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
  .node-label.in-bar {
    font-size: 12px;
    pointer-events: none;
  }
  .bar {
    fill: var(--surface-hover, #f5f5f5);
    stroke: var(--muted);
    stroke-width: 1.2;
  }
  .self .bar {
    fill: var(--surface-selected, #e6f0ff);
    stroke: var(--accent);
    stroke-width: 2;
  }
  .current .bar {
    filter: saturate(1.2);
  }
  .node-link {
    color: inherit;
    text-decoration: none;
  }
  .node-link:hover .bar,
  .node-link:focus-visible .bar {
    stroke-width: 2.4;
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
  .disclosure-trigger {
    appearance: none;
    border: 0;
    background: transparent;
    color: inherit;
    cursor: pointer;
    font: inherit;
    padding: 0;
    text-align: left;
  }
  .contract-gap-list {
    margin: 0.4rem 0 0;
    padding-left: 1.25rem;
  }
</style>
