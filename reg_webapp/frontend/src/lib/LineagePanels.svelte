<script lang="ts">
import {
  type BindingNodeData,
  getBindingLineageWarnings,
  getBindingPredecessors,
  type RelatedRefModel,
  type VariableRefModel,
} from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, formatWindow, windowTitle } from "./catalog";

// Stacked (always-visible, NOT tabbed) lineage sections for a binding leaf:
//
//   SUCCESSION — BOTH directions: `replaced_by` (outbound, embedded on the leaf)
//                AND `/predecessors` (inbound, fetched — the leaf embeds only the
//                outbound side).
//   RELATED    — split-sibling edges (`related_to`, embedded), each with its
//                `relation_kind`.
//   LINEAGE    — consumer/source edges (`lineage`, embedded) with source_fqid
//                links.
//   WARNINGS   — `/lineage_warnings` (fetched; the leaf does NOT embed these).
//
// The two fetched panels use `asyncResource` keyed on `fqidPath`. Refs link to
// their 3-seg `fqid` when present, else render the provider/register/variable
// triple as plain text (the shared `refItem` snippet below).
let { fqidPath, node }: { fqidPath: string; node: BindingNodeData } = $props();

const predecessors = asyncResource(() => getBindingPredecessors(fqidPath));
const warnings = asyncResource(() => getBindingLineageWarnings(fqidPath));

// Empty metadata sections are OMITTED, not rendered as "None." walls (a typical
// variable has no succession/related/lineage at all — five headed "None."
// sections drown the page). A section is shown when it has data OR is still
// loading OR errored (we never hide a section whose state is unknown). The two
// embedded arms (replaced_by / related_to / lineage) are known synchronously;
// the two fetched arms (predecessors / warnings) gate on their resource state.
const hasReplacedBy = $derived(node.replaced_by.length > 0);
const hasPredecessors = $derived(
  predecessors.loading ||
    !!predecessors.error ||
    (predecessors.data?.predecessors.length ?? 0) > 0,
);
// SUCCESSION wraps both directions — show the section if EITHER has content.
const showSuccession = $derived(hasReplacedBy || hasPredecessors);
const showRelated = $derived(node.related_to.length > 0);
const showLineage = $derived(node.lineage.length > 0);
const showWarnings = $derived(
  warnings.loading ||
    !!warnings.error ||
    (warnings.data?.lineage_warnings.length ?? 0) > 0,
);
// When every section is empty (the common case), render one compact line instead
// of a bare gap — keeps the leaf layout from orphaning below the states view.
const anySection = $derived(
  showSuccession || showRelated || showLineage || showWarnings,
);

/** The provider/register/variable triple as plain text (the fallback identity
 * when a ref has no populated `fqid`). */
function refTriple(ref: {
  provider: string;
  register: string;
  variable: string;
}): string {
  return `${ref.provider}/${ref.register}/${ref.variable}`;
}

// Succession refs carry optional `reason`/`effective_year` (#142) — show them
// as a parenthetical annotation when present (or `null` for no annotation).
function succAnnotation(ref: VariableRefModel): string | null {
  const bits: string[] = [];
  if (ref.effective_year !== null && ref.effective_year !== undefined) {
    bits.push(String(ref.effective_year));
  }
  if (ref.reason) {
    bits.push(ref.reason);
  }
  return bits.length > 0 ? `(${bits.join(" · ")})` : null;
}

// #489: render succession as ONE period-ordered chain — predecessors → THIS
// variable → successors — so a renamed measure (e.g. anninkf → anninkf04 →
// anninkf18) reads top-to-bottom as one thing over time, instead of two
// disconnected "replaces"/"replaced by" lists. `effective_year` is the temporal
// signal on the edges (succession edges only — NOT same_as/related_to, which are
// equivalence/sibling, not temporal); we sort ascending with nulls last (an
// undated edge has no place on the timeline, so it trails). Per-direction sorts
// are sufficient — the leaf only embeds its immediate neighbours, so the chain
// is predecessors (oldest→) then this node then (→newest) successors.
function byEffectiveYearAsc(a: VariableRefModel, b: VariableRefModel): number {
  const ay = a.effective_year ?? null;
  const by = b.effective_year ?? null;
  if (ay === by) {
    return 0;
  }
  if (ay === null) {
    return 1; // nulls last
  }
  if (by === null) {
    return -1;
  }
  return ay - by;
}

// The current variable's leaf slug (last FQID segment) for the in-place "current"
// chain node — derived from the leaf's own `fqid` (always the 3-seg binding).
const currentSlug = $derived(node.fqid.split("/").at(-1) ?? node.fqid);

// The chain's successor arm — `replaced_by` (outbound), sorted ascending.
const successorChain = $derived([...node.replaced_by].sort(byEffectiveYearAsc));
// The chain's predecessor arm — the fetched inbound `/predecessors`, sorted
// ascending (recomputed only when the fetch resolves).
const predecessorChain = $derived(
  [...(predecessors.data?.predecessors ?? [])].sort(byEffectiveYearAsc),
);
</script>

<!-- One ref row: a link to its 3-seg `fqid` (with the FQID as a code), or the
     provider/register/variable triple when the ref has no resolvable `fqid`;
     plus an optional trailing annotation. Shared by the succession + related
     sections (their only difference is the annotation). -->
{#snippet refItem(
  ref: VariableRefModel | RelatedRefModel,
  annotation: string | null,
)}
  <li>
    {#if ref.fqid}
      <a href={catalogHref(ref.fqid)}>{ref.variable}</a>
      <code class="ref-fqid">{ref.fqid}</code>
    {:else}
      <span>{ref.variable}</span>
      <code class="ref-fqid muted">{refTriple(ref)}</code>
    {/if}
    {#if annotation}
      <span class="muted ann">{annotation}</span>
    {/if}
  </li>
{/snippet}

<!-- One node of the #489 succession chain: a link to the neighbour's fqid (or
     the triple when absent) with a leading rail marker + the optional #142
     annotation. The `current` flag is unused here (the current node is rendered
     inline, not via this snippet) but kept in the signature so a future "mark a
     neighbour current" stays a one-line change. -->
{#snippet chainNode(
  ref: VariableRefModel,
  annotation: string | null,
  current: boolean,
)}
  <li class="chain-node" class:current>
    <span class="marker" aria-hidden="true">○</span>
    <span class="chain-ref">
      {#if ref.fqid}
        <a href={catalogHref(ref.fqid)}>{ref.variable}</a>
        <code class="ref-fqid">{ref.fqid}</code>
      {:else}
        <span>{ref.variable}</span>
        <code class="ref-fqid muted">{refTriple(ref)}</code>
      {/if}
      {#if annotation}
        <span class="muted ann">{annotation}</span>
      {/if}
    </span>
  </li>
{/snippet}

<div class="lineage-panels">
  {#if !anySection}
    <!-- Every section is empty (the common case) — one compact line instead of
         four "None." walls, so the leaf layout doesn't orphan below the states. -->
    <p class="muted no-links">No succession or lineage links.</p>
  {/if}

  <!-- SUCCESSION — ONE period-ordered chain (#489): predecessors → THIS variable
       → successors, so a renamed measure reads top-to-bottom as one thing over
       time. The whole section is omitted when NEITHER direction has links. The
       inbound (predecessors) arm is fetched, so its loading/error states gate the
       chain head; the current node + outbound (replaced_by) arm are synchronous. -->
  {#if showSuccession}
    <section aria-labelledby="succession-heading">
      <h3 id="succession-heading">Succession</h3>

      {#if predecessors.loading}
        <p class="muted" aria-busy="true">Loading predecessors…</p>
      {:else if predecessors.error}
        <p class="error" role="alert">
          Failed to load predecessors: {predecessors.error}
        </p>
      {/if}

      <ol class="chain">
        {#each predecessorChain as ref, i (ref.fqid ?? refTriple(ref) + i)}
          {@render chainNode(ref, succAnnotation(ref), false)}
        {/each}
        <!-- THIS variable, marked in place — a non-link "current" node. -->
        <li class="chain-node current" aria-current="true">
          <span class="marker" aria-hidden="true">●</span>
          <span class="this-var">
            <span class="var-name">{node.name ?? currentSlug}</span>
            <code class="ref-fqid">{node.fqid}</code>
            <span class="muted ann">(this variable)</span>
          </span>
        </li>
        {#each successorChain as ref, i (ref.fqid ?? refTriple(ref) + i)}
          {@render chainNode(ref, succAnnotation(ref), false)}
        {/each}
      </ol>
    </section>
  {/if}

  <!-- RELATED — split siblings -->
  {#if showRelated}
    <section aria-labelledby="related-heading">
      <h3 id="related-heading">Related (split siblings)</h3>
      <ul class="refs">
        {#each node.related_to as ref, i (ref.fqid ?? refTriple(ref) + i)}
          {@render refItem(ref, ref.relation_kind)}
        {/each}
      </ul>
    </section>
  {/if}

  <!-- LINEAGE — consumer/source edges -->
  {#if showLineage}
    <section aria-labelledby="lineage-heading">
      <h3 id="lineage-heading">Lineage</h3>
      <ul class="refs">
        {#each node.lineage as edge (edge.consumer_state_id + ":" + edge.source_state_id)}
          <li>
            <!-- #309: sentinel-free window display (raw ISO on the tooltip). -->
            <span class="muted edge-validity" title={windowTitle(edge.valid_from, edge.valid_to)}>
              {formatWindow(edge.valid_from, edge.valid_to)}
            </span>
            {#if edge.source_fqid}
              ← <a href={catalogHref(edge.source_fqid)}>{edge.source_fqid}</a>
            {:else}
              ← <span class="muted">source state #{edge.source_state_id}</span>
            {/if}
          </li>
        {/each}
      </ul>
    </section>
  {/if}

  <!-- LINEAGE WARNINGS — fetched (not embedded) -->
  {#if showWarnings}
    <section aria-labelledby="lineage-warnings-heading">
      <h3 id="lineage-warnings-heading">Lineage warnings</h3>
      {#if warnings.loading}
        <p class="muted" aria-busy="true">Loading…</p>
      {:else if warnings.error}
        <p class="error" role="alert">
          Failed to load lineage warnings: {warnings.error}
        </p>
      {:else if warnings.data}
        <ul class="warnings">
          {#each warnings.data.lineage_warnings as w (w.consumer_state_id + ":" + w.warning_kind)}
            <li>
              <code class="warn-kind">{w.warning_kind}</code>
              <span>{w.message}</span>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  {/if}
</div>

<style>
  .lineage-panels {
    margin-top: 1.5rem;
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
  }
  .no-links {
    margin: 0;
    font-size: 0.9rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .refs {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }
  .refs li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
  }
  .ref-fqid {
    font-size: 0.85em;
    color: var(--muted);
  }
  .ann {
    font-size: 0.85em;
  }
  /* #489: the succession chain — a vertical rail (left border on each node)
     with a marker per node, so predecessors → this var → successors reads as
     one continuous timeline. The current node's marker is filled + bold to mark
     it in place. */
  .chain {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
  }
  .chain-node {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.3rem 0 0.3rem 0.85rem;
    border-left: 2px solid var(--border);
    margin-left: 0.4rem;
  }
  .chain-node .marker {
    margin-left: -1.3rem;
    color: var(--muted);
    font-size: 0.7em;
  }
  .chain-node.current {
    border-left-color: var(--accent);
  }
  .chain-node.current .marker {
    color: var(--accent);
    font-size: 0.85em;
  }
  .chain-node.current .var-name {
    font-weight: 600;
  }
  .chain-ref,
  .this-var {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
  }
  .edge-validity {
    font-size: 0.85em;
  }
  .warnings {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .warnings li {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
  }
  .warn-kind {
    color: #92600a;
    font-size: 0.85em;
  }
</style>
