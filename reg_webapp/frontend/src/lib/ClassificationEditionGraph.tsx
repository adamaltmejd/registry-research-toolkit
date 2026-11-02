import type { CSSProperties } from "react";
import type { ClassificationGraphNode, RelationshipGraph } from "./api";
import styles from "./ClassificationEditionGraph.module.css";
import { catalogHref } from "./catalog";
import {
  type ClassificationDagEdge,
  type ClassificationDagNode,
  type ClassificationPoint,
  classificationDagLayout,
  clustersOf,
  type NodeCluster,
  resolveEdges,
} from "./picker_graph";

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

/** The succession clusters to draw: with a focus, only the editions connected
 * to it; without one, every edition on a succession edge. */
function dagClusters(graph: RelationshipGraph): DagCluster[] {
  const successionEdges = resolveEdges(graph).filter(
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
      if (renderIds.has(id)) continue;
      renderIds.add(id);
      for (const next of neighbors.get(id) ?? []) queue.push(next);
    }
  } else {
    renderIds = new Set(
      successionEdges.flatMap((edge) => [edge.source.id, edge.target.id]),
    );
  }
  if (renderIds.size === 0) return [];
  return clustersOf(graph).flatMap((cluster): DagCluster[] => {
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
    if (layout.nodes.length === 0 || layout.edges.length === 0) return [];
    return [
      {
        cluster,
        layout,
        width:
          PAD * 2 + layout.columns * NODE_W + (layout.columns - 1) * COL_GAP,
        height:
          PAD * 2 +
          EDGE_LABEL_BAND +
          layout.rows * NODE_H +
          (layout.rows - 1) * ROW_GAP,
      },
    ];
  });
}

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

function edgeYearPosition(edge: ClassificationDagEdge): CSSProperties {
  const sx = nodeLeft(edge.source) + NODE_W;
  const tx = nodeLeft(edge.target);
  const sy = nodeTop(edge.source) + NODE_H / 2;
  const ty = nodeTop(edge.target) + NODE_H / 2;
  return {
    left: sx + (tx - sx) / 2 - 17,
    top: edge.source.row === edge.target.row ? sy - 26 : (sy + ty) / 2 - 7,
  };
}

function editionName(node: ClassificationGraphNode): string {
  return node.short_name ?? node.label;
}

/** What the drawing says about an edition beyond its name, for a screen
 * reader: its full name, year, and whether it is current or the one viewed. */
function editionDetail(node: ClassificationGraphNode): string {
  const name = editionName(node);
  const fullName = node.label !== name ? `, ${node.label}` : "";
  const year =
    node.version_year != null ? String(node.version_year) : "undated";
  return `${fullName}, ${year}${node.is_current ? ", current edition" : ""}`;
}

/** Brings the viewed (or current) edition into the scroll box once drawn. */
function scrollIntoReach(element: HTMLLIElement): () => void {
  const frame = requestAnimationFrame(() =>
    element.scrollIntoView({ block: "nearest", inline: "center" }),
  );
  return () => cancelAnimationFrame(frame);
}

/**
 * A classification's edition succession (#906): a compact DAG ordered by
 * succession, not by distance on a year axis. Read-only: editions are catalog
 * pages to open, not columns to add. Renders nothing for a graph without a
 * succession edge.
 *
 * simplify: a free-form node graph, which DESIGN.md → Time rules out; kept as
 * ported until the time-axis LaneGraph rebuild (bundle A1) can draw editions
 * too, then replace it with that.
 */
export function ClassificationEditionGraph({
  graph,
}: {
  graph: RelationshipGraph;
}) {
  const clusters = dagClusters(graph);
  if (clusters.length === 0) return null;
  return (
    <section className={styles.editions} aria-labelledby="editions-heading">
      <h2 id="editions-heading" className={styles.heading}>
        Editions
      </h2>
      {clusters.map(({ cluster, layout, width, height }, ci) => (
        <div key={cluster.groupKey ?? `u${ci}`} className={styles.cluster}>
          {cluster.label && (
            <h3 className={styles.clusterHeading}>{cluster.label}</h3>
          )}
          <div className={styles.scroll}>
            <div className={styles.dag} style={{ width, height }}>
              <svg
                className={styles.connectors}
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
                    <path d="M0,0 L8,4 L0,8 z" className={styles.arrowHead} />
                  </marker>
                </defs>
                {layout.edges.map((edge) => (
                  <path
                    key={edge.edge.id}
                    d={edgePath(edge)}
                    className={styles.edge}
                    markerEnd={`url(#edition-arrow-${ci})`}
                  />
                ))}
              </svg>
              {layout.edges.map(
                (edge) =>
                  edge.edge.effective_year != null && (
                    <span
                      key={edge.edge.id}
                      className={styles.edgeYear}
                      style={edgeYearPosition(edge)}
                      aria-hidden="true"
                    >
                      {edge.edge.effective_year}
                    </span>
                  ),
              )}
              <ul
                className={styles.nodes}
                aria-label={
                  cluster.label
                    ? `Editions of ${cluster.label}`
                    : "Editions in succession"
                }
              >
                {layout.nodes.map((item) => {
                  const node = item.point.node;
                  const focused = node.id === graph.focus_id;
                  const active =
                    focused || (graph.focus_id == null && node.is_current);
                  const href =
                    focused || node.fqid == null
                      ? null
                      : catalogHref(node.fqid);
                  return (
                    <li
                      key={node.id}
                      ref={active ? scrollIntoReach : undefined}
                      className={
                        focused
                          ? `${styles.node} ${styles.focused}`
                          : styles.node
                      }
                      style={{ left: nodeLeft(item), top: nodeTop(item) }}
                    >
                      {href ? (
                        <a
                          className={styles.name}
                          href={href}
                          title={node.label}
                        >
                          {editionName(node)}
                        </a>
                      ) : (
                        <span className={styles.name} title={node.label}>
                          {editionName(node)}
                        </span>
                      )}
                      <span className="visually-hidden">
                        {editionDetail(node)}
                      </span>
                      <span className={styles.tags} aria-hidden="true">
                        {focused && (
                          <span className={styles.viewed}>Viewed</span>
                        )}
                        {node.is_current && (
                          <span className={styles.currentTag}>Current</span>
                        )}
                      </span>
                    </li>
                  );
                })}
              </ul>
            </div>
          </div>
          <ul className="visually-hidden">
            {layout.edges.map((edge) => (
              <li key={edge.edge.id}>
                {edge.source.point.node.label} to {edge.target.point.node.label}
                {edge.edge.effective_year != null &&
                  ` in ${edge.edge.effective_year}`}
              </li>
            ))}
          </ul>
        </div>
      ))}
    </section>
  );
}
