<script lang="ts">
import type { RelationshipGraph } from "./api";
import { catalogHref, facetLabelJoin, leafSlug } from "./catalog";
import {
  axisTicks,
  type ClassificationPoint,
  clustersOf,
  type NodeCluster,
  type RenderNode,
  type ResolvedEdge,
  resolveEdges,
  type VariableLane,
  type YearScale,
  yearScaleOf,
} from "./history_graph";

// The unified catalog history-graph view (#678) over the relationship-graph
// contract (#761/#792). Replaces the four retired panels (Dimensions / Lineage /
// their classification twins) with ONE renderer that draws BOTH leaf kinds on a
// SHARED HORIZONTAL TIME AXIS (the #667/#678 headline):
//   - a VARIABLE node → a horizontal LANE: a left gutter (name + qualifier) plus a
//     timeline track of representation-run CELLS positioned at [x(from), x(to)] on
//     the year scale (one cell per `representation_run_id`), so cells across lanes
//     align in time. Open-ended cells fade toward the vintage ceiling; unknown
//     starts fade in from the floor.
//   - a CLASSIFICATION node → a POINT at its `version_year` on the same axis (an
//     edition is NOT a span and NOT dead after its successor), drawn as a labelled
//     stop on a thin succession line.
// Nodes sharing a `group_key` CLUSTER under their `group_label` heading (Fork B);
// `succession` (directed) + `related` (undirected) edges read as chronological
// flow / a subtle related affordance; `same_as` surfaces as a per-node "also
// delivered in {register}" chip (NOT an edge); the `focus_id` node is emphasized.
//
// SUBSTRATE: HTML/CSS lanes over an absolute-positioned, year-scaled track (not a
// monolithic SVG) — that gives a STICKY left gutter under horizontal scroll, a
// real type hierarchy, and native hover/tooltip affordances cohesive with the
// rest of the app. Accessibility: a parallel VISUALLY-HIDDEN structured list
// (`role="img"` container + sr-only list) mirrors every node/cell/edge so a
// screen reader reads the graph as text, never a pixel/scroll blob.
//
// An empty graph (`nodes.length === 0`) is the contract's "don't render" signal.
let { graph, vintageYear }: { graph: RelationshipGraph; vintageYear?: number } =
  $props();

// ── Track geometry ──────────────────────────────────────────────────────────
// The track is a year-scaled strip; the gutter is a fixed sticky column to its
// left. Cells/points are absolutely positioned by year → px. PX_PER_YEAR gives
// each year enough room that a multi-decade span reads without crushing; the
// track scrolls horizontally inside the 56rem column when it overflows.
const GUTTER_W = 188; // sticky left gutter (name + qualifier)
const PX_PER_YEAR = 19; // horizontal density of the year scale
const TRACK_MIN = 360; // floor track width so a 1–2 year graph still reads
const TRACK_PAD = 14; // inner left/right padding so end cells don't touch edges
const LANE_BASE_H = 56; // a single-row lane's height
const ROW_H = 44; // each extra packed sub-row adds this much
const CELL_H = 38; // a cell's height (centred within its sub-row)
const CELL_MIN_W = 64; // a single-year cell still shows its label

/** The shared year axis across the WHOLE graph (all clusters), or null when no
 * node is datable (→ the renderer omits the axis and just stacks lanes). */
const scale = $derived<YearScale | null>(yearScaleOf(graph, vintageYear));

/** Inner track width in px (the year span scaled), floored. */
const trackInnerW = $derived(
  scale
    ? Math.max(TRACK_MIN, (scale.maxYear - scale.minYear) * PX_PER_YEAR)
    : TRACK_MIN,
);
const trackW = $derived(trackInnerW + TRACK_PAD * 2);

/** Year → x (px from the track's left edge), inside the padded inner band. A
 * non-finite year (the no-scale degenerate graph, where nothing is datable) maps
 * to the pad — the renderer then sequences such cells by index instead. */
function x(year: number): number {
  if (!scale || !Number.isFinite(year)) {
    return TRACK_PAD;
  }
  const span = scale.maxYear - scale.minYear || 1;
  return TRACK_PAD + ((year - scale.minYear) / span) * trackInnerW;
}

const ticks = $derived(scale ? axisTicks(scale) : []);

/** A laid-out lane: its node, its `top`/`height` in the lane stack (height grows
 * with packed sub-rows), and its vertical centre (the connector anchor). */
interface LaneBox {
  rn: RenderNode;
  top: number;
  height: number;
  center: number;
}

interface RenderCluster {
  cluster: NodeCluster;
  edges: ResolvedEdge[];
  lanes: LaneBox[];
  byId: Map<string, LaneBox>;
  height: number; // total lane-stack height
}

/** A variable lane's height grows with its packed sub-row count; a classification
 * point (or single-row variable) is the base height. */
function laneHeightOf(rn: RenderNode): number {
  if (rn.kind === "variable" && rn.rowCount > 1) {
    return LANE_BASE_H + (rn.rowCount - 1) * ROW_H;
  }
  return LANE_BASE_H;
}

/** Cluster + lay out lanes (variable-height, stacked) + bucket each edge into its
 * single cluster ONCE per graph change. The scale is graph-wide, so
 * `clustersOf(graph, scale)` clamps + sub-row-packs every cell up front; the
 * template reads laid-out clusters and the cluster-local edges (both endpoints
 * share a cluster — `clustersOf` unions edge-connected nodes). A cross-cluster
 * edge (defensive) lands nowhere. */
const renderClusters = $derived.by((): RenderCluster[] => {
  const clusters = clustersOf(graph, scale);
  const out: RenderCluster[] = clusters.map((cluster) => {
    let top = 0;
    const lanes: LaneBox[] = cluster.nodes.map((rn) => {
      const height = laneHeightOf(rn);
      const box = { rn, top, height, center: top + height / 2 };
      top += height;
      return box;
    });
    return {
      cluster,
      edges: [],
      lanes,
      byId: new Map(lanes.map((l) => [l.rn.node.id, l])),
      height: top,
    };
  });
  const clusterOfNode = new Map<string, number>();
  out.forEach((rc, i) => {
    for (const rn of rc.cluster.nodes) {
      clusterOfNode.set(rn.node.id, i);
    }
  });
  for (const re of resolveEdges(graph)) {
    const si = clusterOfNode.get(re.source.id);
    const ti = clusterOfNode.get(re.target.id);
    if (si !== undefined && si === ti) {
      out[si].edges.push(re);
    }
  }
  return out;
});

function isFocus(rn: RenderNode): boolean {
  return rn.node.id === graph.focus_id;
}

/** A variable lane is a DEAD/renamed predecessor (a thin node) when it carries no
 * states — render it muted with a "(renamed)" hint, labelled by its leaf slug. */
function isRenamed(rn: VariableLane): boolean {
  return rn.node.states.length === 0;
}

/** The gutter label for a variable lane:
 *  - facets present → the faceted member label (Fork B in-cluster distinction);
 *  - grouped (a `group_label`) but facet-less → the LEAF SLUG (an edge-style
 *    concept group's members share one `group_label` AND `label`, so the slug is
 *    the only disambiguator);
 *  - a dead/renamed thin node (no states) → its leaf slug;
 *  - otherwise (ungrouped, live) → the concept `label`. */
function memberLabel(rn: VariableLane): string {
  if (rn.node.facets.length > 0) {
    return facetLabelJoin(rn.node.facets);
  }
  if ((rn.node.group_label != null || isRenamed(rn)) && rn.node.fqid) {
    return leafSlug(rn.node.fqid);
  }
  return rn.node.label;
}

/** Whether the gutter shows a secondary (mono slug) line under the primary name —
 * for a faceted/grouped member whose primary label isn't already the slug. The
 * slug disambiguates members that share a concept name. */
function gutterSlug(rn: VariableLane): string | null {
  if (rn.node.facets.length > 0 && rn.node.fqid) {
    return leafSlug(rn.node.fqid);
  }
  return null;
}

/** The display label for an edge, or null to render none. A `related` edge keeps
 * its `relation_kind` label. A `succession` edge shows its `label` ONLY between
 * VARIABLE nodes: a classification-edition succession carries an internal curation
 * provenance tag (the predecessor `note`) as its label, never a human reason — the
 * retired ClassificationLineagePanels showed no edition succession reason, so
 * suppress it for parity AND to avoid leaking the internal tag (SVG + fallback). */
function edgeLabelText(re: ResolvedEdge): string | null {
  if (!re.edge.label) {
    return null;
  }
  if (
    re.edge.kind === "succession" &&
    !(re.source.kind === "variable" && re.target.kind === "variable")
  ) {
    return null;
  }
  return re.edge.label;
}

/** A classification point's year tag (the version_year), or "" when unknown. */
function yearTag(p: ClassificationPoint): string {
  return p.node.version_year != null ? String(p.node.version_year) : "";
}

/** The x of a classification point on the axis — its version_year, or the track
 * centre when the year is unknown (so an undated edition still has a stop). */
function pointX(p: ClassificationPoint): number {
  if (p.node.version_year != null && scale) {
    return x(p.node.version_year);
  }
  return TRACK_PAD + trackInnerW / 2;
}

/** The top y of a cell's sub-row within its lane (the lane is `height` tall; row
 * 0 sits at the lane top with a small inset, each further row drops by ROW_H). */
function cellTop(laneHeight: number, row: number, rowCount: number): number {
  if (rowCount <= 1) {
    return (laneHeight - CELL_H) / 2; // vertically centred single row
  }
  const inset = (LANE_BASE_H - CELL_H) / 2;
  return inset + row * ROW_H;
}
</script>

{#if graph.nodes.length > 0}
  <section class="history-graph" aria-labelledby="history-graph-heading">
    <h3 id="history-graph-heading">History</h3>

    {#each renderClusters as { cluster, edges: cEdges, lanes, byId, height: stackH }, ci (cluster.groupKey ?? `u${ci}`)}
      <div class="cluster">
        {#if cluster.label}
          <h4 class="cluster-heading">{cluster.label}</h4>
        {/if}

        <!-- The drawn timeline: role="img" with a text summary; the structured
             fallback below is the real a11y surface (sr-only). The visual track is
             decorative to AT beyond the summary. -->
        <div
          class="timeline"
          role="img"
          aria-label={cluster.label
            ? `History timeline for ${cluster.label}: ${cluster.nodes.length} nodes`
            : `History timeline: ${cluster.nodes.length} nodes`}
        >
          <div class="grid" style={`--track-w:${trackW}px; --gutter-w:${GUTTER_W}px;`}>
            <!-- Axis header: a sticky row of year ticks; gridlines drop through
                 the lanes below so cells align in time. -->
            {#if scale}
              <div class="axis-row" aria-hidden="true">
                <div class="axis-gutter"></div>
                <div class="axis-track">
                  {#each ticks as tick (tick.year)}
                    <span class="tick" style={`left:${x(tick.year)}px`}
                      >{tick.label}</span
                    >
                  {/each}
                </div>
              </div>
            {/if}

            <!-- Lanes: gutter (sticky) + track. Succession/related connectors are
                 an absolutely-positioned overlay over the lane stack (drawn first,
                 behind the cells). -->
            <div class="lanes" style={`height:${stackH}px`}>
              <!-- Gridlines behind everything (through the whole lane stack). -->
              {#if scale}
                <div class="gridlines" aria-hidden="true">
                  {#each ticks as tick (tick.year)}
                    <span class="gridline" style={`left:${x(tick.year)}px`}></span>
                  {/each}
                </div>
              {/if}

              <!-- Connector overlay: succession is the chronological flow, related
                   a subtle dashed bow. Both anchor at the source/target lane
                   centres down the gutter rail. -->
              <svg
                class="connectors"
                width={trackW}
                height={stackH}
                aria-hidden="true"
              >
                <defs>
                  <marker
                    id={`arrow-${ci}`}
                    viewBox="0 0 8 8"
                    refX="6"
                    refY="4"
                    markerWidth="6"
                    markerHeight="6"
                    orient="auto"
                  >
                    <path d="M0,0 L8,4 L0,8 z" class="arrow-head" />
                  </marker>
                </defs>
                {#each cEdges as re, ei (re.edge.id)}
                  {@const s = byId.get(re.source.id)}
                  {@const t = byId.get(re.target.id)}
                  {#if s && t}
                    {#if re.edge.kind === "succession"}
                      <!-- A thin connector down the gutter rail (predecessor →
                           successor), arrowed at the successor. -->
                      <line
                        x1={GUTTER_W - 10}
                        y1={s.center}
                        x2={GUTTER_W - 10}
                        y2={t.center}
                        class="edge succession"
                        marker-end={`url(#arrow-${ci})`}
                      />
                    {:else}
                      {@const bow = GUTTER_W - 22 - ei * 7}
                      <path
                        d={`M ${GUTTER_W - 10} ${s.center} Q ${bow} ${(s.center + t.center) / 2} ${GUTTER_W - 10} ${t.center}`}
                        class="edge related"
                        fill="none"
                      />
                    {/if}
                  {/if}
                {/each}
              </svg>

              <!-- Succession reason annotations: a small truncated chip at the rail
                   midpoint between predecessor and successor (HTML so it ellipsizes
                   + carries a full-text title; the sr-only fallback has the full
                   reason). Only variable→variable successions carry a human reason
                   (edgeLabelText suppresses the internal classification tag). -->
              {#each cEdges as re (re.edge.id)}
                {@const s = byId.get(re.source.id)}
                {@const t = byId.get(re.target.id)}
                {#if re.edge.kind === "succession" && edgeLabelText(re) && s && t}
                  <div
                    class="reason"
                    style={`top:${(s.center + t.center) / 2}px; left:${GUTTER_W + 6}px`}
                    title={edgeLabelText(re)}
                  >
                    {edgeLabelText(re)}
                  </div>
                {/if}
              {/each}

              {#each lanes as { rn, top, height } (rn.node.id)}
                {@const renamed = rn.kind === "variable" && isRenamed(rn)}
                <div
                  class="lane"
                  class:focus={isFocus(rn)}
                  class:renamed
                  style={`top:${top}px; height:${height}px; animation-delay:${Math.min(top / LANE_BASE_H, 12) * 35}ms`}
                >
                  <!-- Gutter: primary name (+ qualifier/slug), sticky on scroll. -->
                  <div class="gutter">
                    <span class="marker" class:focus={isFocus(rn)}></span>
                    <div class="gutter-text">
                      {#if rn.kind === "variable"}
                        <span class="name" title={rn.node.label}
                          >{memberLabel(rn)}{#if renamed}<span class="hint"
                              > (renamed)</span
                            >{/if}</span
                        >
                        {#if gutterSlug(rn)}
                          <span class="slug">{gutterSlug(rn)}</span>
                        {/if}
                      {:else}
                        <span class="name" title={rn.node.label}
                          >{rn.node.label}</span
                        >
                        {#if yearTag(rn)}
                          <span class="slug">{yearTag(rn)}</span>
                        {/if}
                      {/if}
                      {#if isFocus(rn)}
                        <span class="viewed">viewed</span>
                      {/if}
                    </div>
                    {#if rn.kind === "variable" && rn.node.same_as.length > 0}
                      <div class="same-as">
                        <span class="sa-prefix">also in</span>
                        {#each rn.node.same_as as sa (sa.fqid)}
                          <a class="sa-chip" href={catalogHref(sa.fqid)}
                            >{sa.register}</a
                          >
                        {/each}
                      </div>
                    {/if}
                  </div>

                  <!-- Track: cells (variable) or one point (classification). -->
                  <div class="track" style={`width:${trackW}px`}>
                    {#if rn.kind === "variable"}
                      {#each rn.cells as cell, i (cell.runId)}
                        <!-- On the scale: position by year span. Without a scale
                             (no datable node) sequence cells left→right by index so
                             they don't collapse onto one x. -->
                        {@const left = scale
                          ? x(cell.fromYear)
                          : TRACK_PAD + i * (CELL_MIN_W + 8)}
                        {@const w = scale
                          ? Math.max(CELL_MIN_W, x(cell.toYear) - x(cell.fromYear))
                          : CELL_MIN_W}
                        <div
                          class="cell"
                          class:open-start={cell.openStart}
                          class:open-end={cell.openEnd}
                          style={`left:${left}px; width:${w}px; top:${cellTop(height, cell.row, rn.rowCount)}px`}
                          title={`${cell.label} · ${cell.window}`}
                        >
                          <span class="cell-label">{cell.label}</span>
                          <span class="cell-window"
                            >{cell.window}{#if rn.multiVariant}<span
                                class="variant-tag">{cell.variant}</span
                              >{/if}</span
                          >
                        </div>
                      {/each}
                    {:else}
                      <!-- A classification edition: a point stop, accented when
                           current. -->
                      <div
                        class="point"
                        class:current={rn.node.is_current}
                        style={`left:${pointX(rn)}px`}
                        title={`${rn.node.label}${yearTag(rn) ? ` (${yearTag(rn)})` : ""}`}
                      >
                        <span class="dot"></span>
                        <span class="point-label"
                          >{yearTag(rn) || "edition"}{#if rn.node.is_current}<span
                              class="current-tag">current</span
                            >{/if}</span
                        >
                      </div>
                    {/if}
                  </div>
                </div>
              {/each}
            </div>
          </div>
        </div>

        <!-- The structured, screen-reader-legible mirror of the same graph: every
             node (focus marker, member label, same_as affordances), its cells /
             edition, and the cluster's edges — as real text the visual can't be. -->
        <ul class="graph-fallback">
          {#each cluster.nodes as rn (rn.node.id)}
            <li class:focus={isFocus(rn)}>
              <span class="fb-marker" aria-hidden="true"
                >{isFocus(rn) ? "●" : "○"}</span
              >
              <span class="fb-node">
                {#if rn.kind === "variable"}
                  {#if rn.node.fqid}
                    <a href={catalogHref(rn.node.fqid)}>{memberLabel(rn)}</a>
                  {:else}
                    {memberLabel(rn)}
                  {/if}
                  {#if isRenamed(rn)}
                    <span class="muted tag">(renamed)</span>
                  {/if}
                  {#if isFocus(rn)}
                    <span class="muted tag">this variable</span>
                  {/if}
                  <ul class="fb-cells">
                    {#each rn.cells as cell (cell.runId)}
                      <li>
                        <span class="fb-cell-label">{cell.label}</span>
                        <span class="muted">{cell.window}</span>
                        {#if rn.multiVariant}
                          <span class="muted">· {cell.variant}</span>
                        {/if}
                      </li>
                    {/each}
                  </ul>
                  {#if rn.node.same_as.length > 0}
                    <p class="fb-same-as muted">
                      also delivered in
                      {#each rn.node.same_as as sa, i (sa.fqid)}
                        {#if i > 0}, {/if}<a href={catalogHref(sa.fqid)}
                          >{sa.register}</a
                        >
                      {/each}
                    </p>
                  {/if}
                {:else}
                  {#if rn.node.fqid}
                    <a href={catalogHref(rn.node.fqid)}>{rn.node.label}</a>
                  {:else}
                    {rn.node.label}
                  {/if}
                  {#if yearTag(rn)}
                    <span class="muted">({yearTag(rn)})</span>
                  {/if}
                  {#if rn.node.is_current}
                    <span class="muted tag">current edition</span>
                  {/if}
                  {#if isFocus(rn)}
                    <span class="muted tag">this edition</span>
                  {/if}
                {/if}
              </span>
            </li>
          {/each}

          {#each cEdges as re (re.edge.id)}
            <li class="fb-edge muted">
              <span class="fb-marker" aria-hidden="true"
                >{re.edge.kind === "succession" ? "▸" : "↔"}</span
              >
              {re.source.label}
              {re.edge.kind === "succession" ? "→" : "↔"}
              {re.target.label}
              {#if edgeLabelText(re)}({edgeLabelText(re)}){/if}
            </li>
          {/each}
        </ul>
      </div>
    {/each}
  </section>
{/if}

<style>
  .history-graph {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 var(--space-3, 0.75rem);
    padding-bottom: var(--space-1, 0.25rem);
    border-bottom: 1px solid var(--border);
    font-size: 1.1rem;
  }
  .cluster + .cluster {
    margin-top: var(--space-4, 1rem);
    padding-top: var(--space-4, 1rem);
    border-top: 1px solid color-mix(in srgb, var(--border) 50%, transparent);
  }
  .cluster-heading {
    margin: 0 0 var(--space-2, 0.5rem);
    font-size: 0.95rem;
    font-weight: 600;
    color: var(--muted);
  }

  /* The timeline scrolls horizontally inside the 56rem column; the gutter stays
     pinned (position: sticky on each lane's .gutter). */
  .timeline {
    overflow-x: auto;
    overflow-y: hidden;
    border: 1px solid color-mix(in srgb, var(--border) 60%, transparent);
    border-radius: var(--radius, 4px);
    background: var(--surface);
  }
  .grid {
    min-width: 100%;
    width: max-content;
  }

  /* ── Axis (sticky header of year ticks) ──────────────────────────────────── */
  .axis-row {
    position: sticky;
    top: 0;
    z-index: 3;
    display: flex;
    height: 26px;
    background: var(--surface);
    border-bottom: 1px solid color-mix(in srgb, var(--border) 70%, transparent);
  }
  .axis-gutter {
    flex: 0 0 var(--gutter-w);
    position: sticky;
    left: 0;
    z-index: 1;
    background: var(--surface);
    border-right: 1px solid color-mix(in srgb, var(--border) 60%, transparent);
  }
  .axis-track {
    position: relative;
    flex: 0 0 var(--track-w);
    width: var(--track-w);
  }
  .tick {
    position: absolute;
    top: 6px;
    transform: translateX(-50%);
    font-size: 0.68rem;
    font-variant-numeric: tabular-nums;
    color: var(--muted);
    white-space: nowrap;
  }

  /* ── Lanes ───────────────────────────────────────────────────────────────── */
  .lanes {
    position: relative;
  }
  .gridlines {
    position: absolute;
    inset: 0;
    left: var(--gutter-w);
    pointer-events: none;
  }
  .gridline {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 1px;
    background: color-mix(in srgb, var(--border) 35%, transparent);
  }
  .connectors {
    position: absolute;
    top: 0;
    left: 0;
    pointer-events: none;
    z-index: 2;
    overflow: visible;
  }

  .lane {
    position: absolute;
    left: 0;
    right: 0;
    /* height set inline (grows with packed sub-rows). */
    display: flex;
    align-items: stretch;
    /* One orchestrated load reveal — a gentle staggered fade/slide. */
    animation: lane-in 0.32s ease-out both;
  }
  @keyframes lane-in {
    from {
      opacity: 0;
      transform: translateY(4px);
    }
    to {
      opacity: 1;
      transform: translateY(0);
    }
  }
  @media (prefers-reduced-motion: reduce) {
    .lane {
      animation: none;
    }
  }
  .lane.focus {
    background: var(--accent-bg);
  }

  /* Gutter: sticky name column with a subtle right rule. A HARD fixed width
     (min/max + min-width:0) so a long name ellipsizes inside it instead of
     widening the flex item — which would shove the track right and break the
     cell/point ↔ axis alignment. */
  .gutter {
    position: sticky;
    left: 0;
    z-index: 1;
    flex: 0 0 var(--gutter-w);
    width: var(--gutter-w);
    min-width: 0;
    max-width: var(--gutter-w);
    box-sizing: border-box;
    padding: var(--space-2, 0.5rem) var(--space-3, 0.75rem)
      var(--space-2, 0.5rem) var(--space-3, 0.75rem);
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 2px;
    background: inherit;
    border-right: 1px solid color-mix(in srgb, var(--border) 60%, transparent);
  }
  .lane:not(.focus) .gutter {
    background: var(--surface);
  }
  .lane.focus .gutter {
    background: var(--accent-bg);
    border-left: 2px solid var(--accent);
    margin-left: -2px;
  }
  .marker {
    position: absolute;
    right: -5px;
    top: 50%;
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: var(--surface);
    border: 1.5px solid var(--muted);
    transform: translateY(-50%);
    z-index: 2;
  }
  .marker.focus {
    background: var(--accent);
    border-color: var(--accent);
  }
  .renamed .marker {
    border-style: dashed;
  }
  .gutter-text {
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
  }
  .name {
    font-size: 0.82rem;
    font-weight: 600;
    line-height: 1.2;
    color: #1a1a1a;
    /* Clamp to 2 lines so a long classification name keeps its distinguishing
       tail (…— Utbildningsnivå) instead of a 1-line ellipsis that makes every
       edition read identically. */
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
  .focus .name {
    color: var(--accent);
  }
  .renamed .name {
    color: var(--muted);
    font-weight: 500;
  }
  .hint {
    color: var(--muted);
    font-weight: 400;
    font-style: italic;
  }
  .slug {
    font-family: var(--mono, monospace);
    font-size: 0.68rem;
    color: var(--muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .viewed {
    align-self: flex-start;
    margin-top: 1px;
    font-size: 0.6rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    text-transform: uppercase;
    color: var(--accent);
    background: color-mix(in srgb, var(--accent) 12%, transparent);
    border-radius: 3px;
    padding: 0 4px;
  }
  .same-as {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 3px;
    margin-top: 2px;
  }
  .sa-prefix {
    font-size: 0.62rem;
    color: var(--muted);
  }
  .sa-chip {
    font-size: 0.62rem;
    font-family: var(--mono, monospace);
    color: var(--accent);
    background: var(--accent-bg);
    border-radius: 3px;
    padding: 0 4px;
    line-height: 1.4;
  }

  /* Track + cells (representation runs). */
  .track {
    position: relative;
    flex: 0 0 auto;
  }
  .cell {
    position: absolute;
    height: 38px;
    box-sizing: border-box;
    padding: 4px 8px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 1px;
    overflow: hidden;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius, 4px);
    transition:
      box-shadow 0.12s ease,
      border-color 0.12s ease,
      transform 0.12s ease;
  }
  .focus .cell {
    border-color: color-mix(in srgb, var(--accent) 45%, var(--border));
  }
  .cell:hover {
    z-index: 4;
    border-color: var(--accent);
    box-shadow: 0 2px 8px color-mix(in srgb, var(--accent) 18%, transparent);
    transform: translateY(-1px);
  }
  /* Open-ended runs fade toward the vintage ceiling rather than a hard wall;
     unknown starts fade in from the floor. */
  .cell.open-end {
    border-right-color: transparent;
    -webkit-mask-image: linear-gradient(
      to right,
      #000 calc(100% - 28px),
      transparent
    );
    mask-image: linear-gradient(to right, #000 calc(100% - 28px), transparent);
  }
  .cell.open-start {
    border-left-color: transparent;
    -webkit-mask-image: linear-gradient(to left, #000 calc(100% - 22px), transparent);
    mask-image: linear-gradient(to left, #000 calc(100% - 22px), transparent);
  }
  .cell-label {
    font-size: 0.72rem;
    font-weight: 600;
    line-height: 1.15;
    color: #1a1a1a;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .cell-window {
    font-size: 0.64rem;
    color: var(--muted);
    font-variant-numeric: tabular-nums;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .variant-tag {
    margin-left: 4px;
    padding: 0 3px;
    font-family: var(--mono, monospace);
    background: color-mix(in srgb, var(--border) 35%, transparent);
    border-radius: 2px;
  }

  /* Classification edition points — stops on the succession line. */
  .point {
    position: absolute;
    top: 50%;
    transform: translate(-50%, -50%);
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 3px;
  }
  .dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--surface);
    border: 1.5px solid var(--muted);
  }
  .point.current .dot {
    background: var(--accent);
    border-color: var(--accent);
  }
  .focus .dot {
    border-color: var(--accent);
  }
  .point-label {
    font-size: 0.7rem;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    color: #1a1a1a;
    white-space: nowrap;
  }
  .point.current .point-label {
    color: var(--accent);
  }
  .current-tag {
    margin-left: 4px;
    font-size: 0.58rem;
    font-weight: 600;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    color: var(--accent);
  }

  /* Connectors. */
  .edge.succession {
    stroke: color-mix(in srgb, var(--accent) 70%, var(--border));
    stroke-width: 1.5;
  }
  .edge.related {
    stroke: var(--muted);
    stroke-width: 1.2;
    stroke-dasharray: 4 3;
    opacity: 0.7;
  }
  .arrow-head {
    fill: color-mix(in srgb, var(--accent) 70%, var(--border));
  }
  /* Succession reason chip: small, muted, truncated; haloed so it reads over a
     gridline. Non-interactive (cell hover passes through). */
  .reason {
    position: absolute;
    z-index: 2;
    transform: translateY(-50%);
    max-width: 220px;
    padding: 0 5px;
    font-size: 0.62rem;
    line-height: 1.5;
    color: var(--muted);
    background: color-mix(in srgb, var(--surface) 88%, transparent);
    border-radius: 3px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    pointer-events: none;
  }

  /* The screen-reader / no-visual structured fallback — visually hidden but
     present in the accessibility tree. */
  .graph-fallback {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
    border: 0;
  }
  .muted {
    color: var(--muted);
  }
  .tag {
    font-size: 0.85em;
    font-style: italic;
  }
</style>
