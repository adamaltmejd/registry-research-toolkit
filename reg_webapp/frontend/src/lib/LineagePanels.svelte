<script lang="ts">
import {
  type BindingNodeData,
  getBindingLineageWarnings,
  getBindingPredecessors,
  type RelatedRefModel,
  type VariableRefModel,
} from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref } from "./catalog";

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

<div class="lineage-panels">
  {#if !anySection}
    <!-- Every section is empty (the common case) — one compact line instead of
         four "None." walls, so the leaf layout doesn't orphan below the states. -->
    <p class="muted no-links">No succession or lineage links.</p>
  {/if}

  <!-- SUCCESSION — both directions; the whole section is omitted when NEITHER
       direction has links, and each sub-heading is omitted when its side is empty
       (so a variable that is only replaced_by doesn't show an empty "predecessors"). -->
  {#if showSuccession}
    <section aria-labelledby="succession-heading">
      <h3 id="succession-heading">Succession</h3>

      {#if hasReplacedBy}
        <h4>Replaced by</h4>
        <ul class="refs">
          {#each node.replaced_by as ref, i (ref.fqid ?? refTriple(ref) + i)}
            {@render refItem(ref, succAnnotation(ref))}
          {/each}
        </ul>
      {/if}

      {#if hasPredecessors}
        <h4>Replaces / predecessors</h4>
        {#if predecessors.loading}
          <p class="muted" aria-busy="true">Loading…</p>
        {:else if predecessors.error}
          <p class="error" role="alert">
            Failed to load predecessors: {predecessors.error}
          </p>
        {:else if predecessors.data}
          <ul class="refs">
            {#each predecessors.data.predecessors as ref, i (ref.fqid ?? refTriple(ref) + i)}
              {@render refItem(ref, succAnnotation(ref))}
            {/each}
          </ul>
        {/if}
      {/if}
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
            <span class="muted edge-validity">{edge.valid_from} – {edge.valid_to}</span>
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
  h4 {
    margin: 0.75rem 0 0.4rem;
    font-size: 0.95rem;
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
