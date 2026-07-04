<script lang="ts">
import type { ClassificationGraphNode, RelationshipGraph } from "./api";
import { catalogHref, leafSlug } from "./catalog";
import {
  type ClassificationDagEdge,
  type ClassificationDagNode,
  type ClassificationPoint,
  classificationDagLayout,
  clustersOf,
  type NodeCluster,
  resolveEdges,
} from "./picker_graph";

// The classification-edition picker surface (#906): a compact DAG ordered by
// succession topology, not by absolute distance on a year axis. It is read-only
// for now: editions are navigable catalog nodes, not add-to-project columns.
let { graph }: { graph: RelationshipGraph } = $props();

const NODE_W = 168;
const NODE_H = 58;
const COL_GAP = 48;
const ROW_GAP = 18;
const PAD = 14;
const EDGE_LABEL_BAND = 24;

interface DagCluster {
  cluster: NodeCluster;
  layout: ReturnType<typeof classificationDagLayout>;
  width: number;
  height: number;
}

function scrollActiveEdition(
  node: HTMLElement,
  active: boolean,
): { update: (next: boolean) => void; destroy: () => void } {
  let frame: number | null = null;
  const scroll = () => {
    frame = null;
    if (active) {
      node.scrollIntoView({ block: "nearest", inline: "center" });
    }
  };
  const queue = () => {
    if (frame != null) {
      cancelAnimationFrame(frame);
    }
    frame = requestAnimationFrame(scroll);
  };
  if (active) {
    queue();
  }
  return {
    update(next: boolean) {
      active = next;
      if (active) {
        queue();
      }
    },
    destroy() {
      if (frame != null) {
        cancelAnimationFrame(frame);
      }
    },
  };
}

const dagClusters = $derived.by((): DagCluster[] => {
  const resolved = resolveEdges(graph);
  const successionEdges = resolved.filter(
    (edge) =>
      edge.source.kind === "classification" &&
      edge.target.kind === "classification",
  );
  let renderIds = new Set<string>();
  if (graph.focus_id != null) {
    const neighbors = new Map<string, Set<string>>();
    for (const edge of successionEdges) {
      const sourceNeighbors = neighbors.get(edge.source.id) ?? new Set();
      sourceNeighbors.add(edge.target.id);
      neighbors.set(edge.source.id, sourceNeighbors);
      const targetNeighbors = neighbors.get(edge.target.id) ?? new Set();
      targetNeighbors.add(edge.source.id);
      neighbors.set(edge.target.id, targetNeighbors);
    }
    const queue = neighbors.has(graph.focus_id) ? [graph.focus_id] : [];
    while (queue.length > 0) {
      const id = queue.shift() as string;
      if (renderIds.has(id)) {
        continue;
      }
      renderIds.add(id);
      for (const next of neighbors.get(id) ?? []) {
        queue.push(next);
      }
    }
  } else {
    renderIds = new Set(
      successionEdges.flatMap((edge) => [edge.source.id, edge.target.id]),
    );
  }
  if (renderIds.size === 0) {
    return [];
  }
  return clustersOf(graph)
    .map((cluster): DagCluster | null => {
      const points = cluster.nodes
        .filter(
          (node): node is ClassificationPoint => node.kind === "classification",
        )
        .filter((point) => renderIds.has(point.node.id));
      const ids = new Set(points.map((point) => point.node.id));
      const edges = successionEdges.filter(
        (edge) => ids.has(edge.source.id) && ids.has(edge.target.id),
      );
      const layout = classificationDagLayout(points, edges);
      if (layout.nodes.length === 0 || layout.edges.length === 0) {
        return null;
      }
      return {
        cluster,
        layout,
        width:
          PAD * 2 + layout.columns * NODE_W + (layout.columns - 1) * COL_GAP,
        height:
          PAD * 2 +
          EDGE_LABEL_BAND +
          layout.rows * NODE_H +
          (layout.rows - 1) * ROW_GAP,
      };
    })
    .filter((cluster): cluster is DagCluster => cluster != null);
});

function nodeLeft(node: ClassificationDagNode): number {
  return PAD + node.column * (NODE_W + COL_GAP);
}

function nodeTop(node: ClassificationDagNode): number {
  return PAD + EDGE_LABEL_BAND + node.row * (NODE_H + ROW_GAP);
}

function edgePath(edge: ClassificationDagEdge): string {
  const sx = nodeLeft(edge.source) + NODE_W;
  const sy = nodeTop(edge.source) + NODE_H / 2;
  const tx = nodeLeft(edge.target);
  const ty = nodeTop(edge.target) + NODE_H / 2;
  const bend = Math.max(24, Math.abs(tx - sx) / 2);
  return `M${sx} ${sy} C${sx + bend} ${sy} ${tx - bend} ${ty} ${tx} ${ty}`;
}

function edgeYear(edge: ClassificationDagEdge): string | null {
  return edge.edge.effective_year != null
    ? String(edge.edge.effective_year)
    : null;
}

function edgeYearLeft(edge: ClassificationDagEdge): number {
  const sx = nodeLeft(edge.source) + NODE_W;
  const tx = nodeLeft(edge.target);
  return sx + (tx - sx) / 2 - 17;
}

function edgeYearTop(edge: ClassificationDagEdge): number {
  const sy = nodeTop(edge.source) + NODE_H / 2;
  const ty = nodeTop(edge.target) + NODE_H / 2;
  if (edge.source.row === edge.target.row) {
    return sy - 26;
  }
  return (sy + ty) / 2 - 7;
}

function editionYear(node: ClassificationGraphNode): string {
  return node.version_year != null ? String(node.version_year) : "undated";
}

function editionName(node: ClassificationGraphNode): string {
  return node.fqid != null ? leafSlug(node.fqid) : node.label;
}

function editionHref(node: ClassificationGraphNode): string | null {
  if (node.id === graph.focus_id || node.fqid == null) {
    return null;
  }
  return catalogHref(node.fqid);
}

function editionLabel(node: ClassificationGraphNode): string {
  return `${editionName(node)}, ${node.label}, ${editionYear(node)}${node.is_current ? ", current edition" : ""}`;
}
</script>

{#if dagClusters.length > 0}
  <section
    class="classification-editions"
    aria-labelledby="classification-editions-heading"
  >
    <h3 id="classification-editions-heading">Editions</h3>

    {#each dagClusters as { cluster, layout, width, height }, ci (cluster.groupKey ?? `u${ci}`)}
      <div class="edition-cluster">
        {#if cluster.label}
          <h4 class="edition-cluster-heading">{cluster.label}</h4>
        {/if}

        <div class="edition-scroll">
          <div
            class="edition-dag"
            role="group"
            aria-label={cluster.label
              ? `Edition succession for ${cluster.label}: ${layout.nodes.length} editions`
              : `Edition succession: ${layout.nodes.length} editions`}
            style={`width:${width}px; height:${height}px;`}
          >
            <svg
              class="edition-connectors"
              width={width}
              height={height}
              aria-hidden="true"
            >
              <defs>
                <marker
                  id={`edition-arrow-${ci}`}
                  viewBox="0 0 8 8"
                  refX="6"
                  refY="4"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto"
                >
                  <path d="M0,0 L8,4 L0,8 z" class="edition-arrow-head" />
                </marker>
              </defs>
              {#each layout.edges as edge (edge.edge.id)}
                <path
                  d={edgePath(edge)}
                  class="edition-edge"
                  marker-end={`url(#edition-arrow-${ci})`}
                />
              {/each}
            </svg>

            {#each layout.edges as edge (edge.edge.id)}
              {@const year = edgeYear(edge)}
              {#if year}
                <span
                  class="edition-edge-year"
                  aria-label={`Superseded in ${year}`}
                  style={`left:${edgeYearLeft(edge)}px; top:${edgeYearTop(edge)}px;`}
                >
                  {year}
                </span>
              {/if}
            {/each}

            {#each layout.nodes as item (item.point.node.id)}
              {@const node = item.point.node}
              {@const href = editionHref(node)}
              {@const focused = node.id === graph.focus_id}
              {@const active = focused || (graph.focus_id == null && node.is_current)}
              <div
                class="edition-node"
                class:focused
                class:current={node.is_current}
                use:scrollActiveEdition={active}
                style={`left:${nodeLeft(item)}px; top:${nodeTop(item)}px;`}
                aria-label={editionLabel(node)}
              >
                <span class="edition-main">
                  {#if href}
                    <a class="edition-name" {href} title={node.label}
                      >{editionName(node)}</a
                    >
                  {:else}
                    <span class="edition-name" title={node.label}
                      >{editionName(node)}</span
                    >
                  {/if}
                  <span class="edition-year">{editionYear(node)}</span>
                </span>
                <span class="edition-tags">
                  {#if focused}
                    <span class="edition-viewed">viewed</span>
                  {/if}
                  {#if node.is_current}
                    <span class="edition-current">current</span>
                  {/if}
                </span>
              </div>
            {/each}
          </div>
        </div>

        <ul class="visually-hidden">
          {#each layout.edges as edge (edge.edge.id)}
            <li>
              {edge.source.point.node.label} to {edge.target.point.node.label}
              {#if edge.edge.effective_year != null}
                in {edge.edge.effective_year}
              {/if}
            </li>
          {/each}
        </ul>
      </div>
    {/each}
  </section>
{/if}

<style>
  .classification-editions {
    margin: var(--space-4) 0;
  }
  h3 {
    margin: 0 0 var(--space-3);
    padding-bottom: var(--space-1);
    border-bottom: 1px solid var(--border);
    font-size: var(--text-h3);
  }
  .edition-cluster + .edition-cluster {
    margin-top: var(--space-4);
    padding-top: var(--space-4);
    border-top: 1px solid color-mix(in srgb, var(--border) 50%, transparent);
  }
  .edition-cluster-heading {
    margin: 0 0 var(--space-2);
    font-size: var(--text-sm);
    font-weight: 600;
    color: var(--text-muted);
  }
  .edition-scroll {
    overflow-x: auto;
    overflow-y: hidden;
    border: 1px solid color-mix(in srgb, var(--border) 70%, transparent);
    border-radius: var(--radius);
    background: var(--surface);
  }
  .edition-dag {
    position: relative;
    min-width: 100%;
  }
  .edition-connectors {
    position: absolute;
    inset: 0;
    z-index: 1;
    overflow: visible;
    pointer-events: none;
  }
  .edition-edge {
    fill: none;
    stroke: var(--viz-edge-succession);
    stroke-width: 1.5;
  }
  .edition-edge-year {
    position: absolute;
    z-index: 2;
    box-sizing: border-box;
    width: 34px;
    padding: 0 2px;
    border: 1px solid
      color-mix(in srgb, var(--viz-edge-succession) 35%, var(--border));
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    line-height: 1.25;
    text-align: center;
    pointer-events: none;
  }
  .edition-arrow-head {
    fill: var(--viz-edge-succession);
  }
  .edition-node {
    position: absolute;
    z-index: 2;
    box-sizing: border-box;
    width: 168px;
    height: 58px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-2);
    padding: var(--space-2);
    border: 1px solid var(--border);
    border-left: 3px solid var(--cat-class);
    border-radius: var(--radius-sm);
    background: color-mix(in srgb, var(--cat-class) 8%, var(--surface));
  }
  .edition-node.focused {
    background: var(--accent-bg);
    border-color: color-mix(in srgb, var(--accent) 55%, var(--border));
    border-left-color: var(--accent);
  }
  .edition-node.current:not(.focused) {
    border-color: color-mix(in srgb, var(--cat-class) 45%, var(--border));
  }
  .edition-main {
    display: flex;
    min-width: 0;
    flex-direction: column;
    gap: 1px;
  }
  .edition-name {
    color: var(--text);
    font-size: var(--text-sm);
    font-weight: 600;
    line-height: 1.2;
    text-decoration: none;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
  }
  a.edition-name:hover {
    color: var(--accent);
  }
  a.edition-name:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .edition-year {
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    font-variant-numeric: tabular-nums;
    color: var(--cat-class-ink);
  }
  .edition-tags {
    display: flex;
    flex: 0 0 auto;
    flex-direction: column;
    align-items: flex-end;
    gap: 2px;
  }
  .edition-viewed,
  .edition-current {
    padding: 0 4px;
    border-radius: var(--radius-sm);
    font-size: 0.58rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    line-height: 1.45;
    text-transform: uppercase;
  }
  .edition-viewed {
    background: color-mix(in srgb, var(--accent) 12%, transparent);
    color: var(--accent-ink);
  }
  .edition-current {
    background: color-mix(in srgb, var(--cat-class) 12%, transparent);
    color: var(--cat-class-ink);
  }
</style>
