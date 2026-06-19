<script lang="ts">
import {
  type BindingNodeData,
  getBindingLineageWarnings,
  type RelatedRefModel,
  type VariableEditionModel,
  type VariableRefModel,
} from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, formatWindow, windowTitle } from "./catalog";

// Stacked (always-visible, NOT tabbed) lineage sections for a binding leaf:
//
//   SUCCESSION — the FULL succession chain (#582), EMBEDDED on the leaf as
//                `succession_chain` (oldest first → terminal/current last). The
//                whole chain (predecessors → THIS variable → successors) is
//                rendered SYNCHRONOUSLY — no per-neighbour fetch.
//   RELATED    — split-sibling edges (`related_to`, embedded), each with its
//                `relation_kind`.
//   LINEAGE    — consumer/source edges (`lineage`, embedded) with source_fqid
//                links.
//   WARNINGS   — `/lineage_warnings` (fetched; the leaf does NOT embed these).
//
// The one fetched panel (warnings) uses `asyncResource` keyed on `fqidPath`. Refs
// link to their 3-seg `fqid` when present, else render the
// provider/register/variable triple as plain text (the shared `refItem` snippet
// below).
let { fqidPath, node }: { fqidPath: string; node: BindingNodeData } = $props();

const warnings = asyncResource(() => getBindingLineageWarnings(fqidPath));

// Empty metadata sections are OMITTED, not rendered as "None." walls (a typical
// variable has no succession/related/lineage at all — five headed "None."
// sections drown the page). A section is shown when it has data OR is still
// loading OR errored (we never hide a section whose state is unknown). The three
// embedded arms (succession_chain / related_to / lineage) are known synchronously;
// the one fetched arm (warnings) gates on its resource state.
//
// The embedded chain (already oldest→current ordered; tolerate the optional wire
// field's absence on a stale cache — degrade to empty rather than crash).
const chain = $derived(node.succession_chain ?? []);
// SUCCESSION renders ONLY for a real chain (>1 edition). A variable with no
// succession is a 1-element chain (just `is_self` + `is_current`) — no panel.
const showSuccession = $derived(chain.length > 1);
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

// Succession editions carry optional `reason`/`effective_year` (#142) — show them
// as a parenthetical annotation when present (or `null` for no annotation).
// (UNLIKE the classification chain, whose succession table has no reason column.)
function succAnnotation(edition: VariableEditionModel): string | null {
  const bits: string[] = [];
  if (edition.effective_year !== null && edition.effective_year !== undefined) {
    bits.push(String(edition.effective_year));
  }
  if (edition.reason) {
    bits.push(edition.reason);
  }
  return bits.length > 0 ? `(${bits.join(" · ")})` : null;
}

// #582: the embedded full chain already IS predecessors → THIS variable →
// successors, ordered oldest→current with `is_self` marking the viewed variable
// and `is_current` marking the terminal/latest edition. Two always-visible anchors
// split the chain into collapsible groups so a long chain doesn't render every
// edition by default (mirrors ClassificationLineagePanels' fold):
//   - the VIEWED edition (`is_self`) — "this variable";
//   - the CURRENT/terminal edition (`is_current`) — always last in the chain.
// When viewing the latest, ONE edition carries both flags. The bulk collapses into
// up to two <details> disclosures (the app's disclosure idiom): editions BEFORE
// the viewed one ("N earlier") and editions BETWEEN the viewed and current one
// ("N later"). The viewed + current editions stay visible without expanding.
const selfIndex = $derived(chain.findIndex((e) => e.is_self));
const currentIndex = $derived(chain.findIndex((e) => e.is_current));

// Everything strictly before the viewed edition is the collapsed "earlier" arm.
const earlier = $derived(selfIndex > 0 ? chain.slice(0, selfIndex) : []);
// Editions strictly between the viewed and the current one are the collapsed
// "later" arm. Empty when self IS current (it's the last edition) or when current
// is the immediate successor (nothing to collapse).
const later = $derived(
  selfIndex >= 0 && currentIndex > selfIndex + 1
    ? chain.slice(selfIndex + 1, currentIndex)
    : [],
);
// The viewed edition and (when distinct) the terminal edition — always visible.
const selfEdition = $derived(selfIndex >= 0 ? chain[selfIndex] : undefined);
const currentEdition = $derived(
  currentIndex > selfIndex ? chain[currentIndex] : undefined,
);
</script>

<!-- One ref row: a link to its 3-seg `fqid` (with the FQID as a code), or the
     provider/register/variable triple when the ref has no resolvable `fqid`;
     plus an optional trailing annotation. Shared by the related section. -->
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

<!-- One node of the #582 succession chain on the vertical rail. The viewed edition
     (`is_self`) renders in place as a non-link "this variable" node; every other
     edition links to its fqid.

     Dead/renamed predecessor (#355/#411): an edition with a valid `fqid` but
     `name === null` is a dead/renamed variable with no live row. We STILL link it
     (its fqid 301-redirects to the current edition) — UNLIKE the classification
     chain, which renders a null-name node as plain text. The link text falls back
     to the `variable` slug (there's no name), and a muted "(renamed)" hint reads
     it as historical. `fqid === null` (malformed, not expected) falls through to
     plain text. `marked` fills the marker + bolds the name for an anchor edition
     (the viewed and/or current edition). -->
{#snippet chainNode(edition: VariableEditionModel, marked: boolean)}
  {@const annotation = succAnnotation(edition)}
  {#if edition.is_self}
    <!-- THIS variable, marked in place — a non-link "this variable" node. It still
         shows its own transition `reason`/`effective_year` (#142) and, when the
         viewed edition IS also the terminal, the "(current edition)" tag. -->
    <li class="chain-node current" class:marked aria-current="true">
      <span class="marker" aria-hidden="true">●</span>
      <span class="chain-ref">
        <span class="var-name">{edition.name ?? edition.variable}</span>
        {#if edition.fqid}
          <code class="ref-fqid">{edition.fqid}</code>
        {/if}
        <span class="muted ann">(this variable)</span>
        {#if edition.is_current}
          <span class="muted ann">(current edition)</span>
        {/if}
        {#if annotation}
          <span class="muted ann">{annotation}</span>
        {/if}
      </span>
    </li>
  {:else}
    <li class="chain-node" class:marked aria-current={undefined}>
      <span class="marker" aria-hidden="true">{marked ? "●" : "○"}</span>
      <span class="chain-ref">
        {#if edition.fqid}
          <!-- name === null → dead/renamed predecessor (#355/#411): still a
               redirecting link, using the variable slug as text. -->
          <a href={catalogHref(edition.fqid)}>{edition.name ?? edition.variable}</a>
          <code class="ref-fqid">{edition.fqid}</code>
          {#if edition.name === null}
            <span class="muted ann">(renamed)</span>
          {/if}
        {:else}
          <span>{edition.name ?? edition.variable}</span>
          <code class="ref-fqid muted">{refTriple(edition)}</code>
        {/if}
        {#if edition.is_current}
          <span class="muted ann">(current edition)</span>
        {/if}
        {#if annotation}
          <span class="muted ann">{annotation}</span>
        {/if}
      </span>
    </li>
  {/if}
{/snippet}

<!-- A collapsed run of editions on the same rail — the <details> disclosure idiom
     (matches ClassificationLineagePanels + SearchView's concept-group fold). Hidden
     by default; the full ordered run reveals on expand. -->
{#snippet editionRun(label: string, editions: VariableEditionModel[])}
  <li class="chain-history">
    <details>
      <summary>
        <span class="marker" aria-hidden="true">○</span>
        {label}
      </summary>
      <ol class="chain">
        {#each editions as edition, i (edition.fqid ?? refTriple(edition) + i)}
          {@render chainNode(edition, false)}
        {/each}
      </ol>
    </details>
  </li>
{/snippet}

<div class="lineage-panels">
  {#if !anySection}
    <!-- Every section is empty (the common case) — one compact line instead of
         four "None." walls, so the leaf layout doesn't orphan below the states. -->
    <p class="muted no-links">No succession or lineage links.</p>
  {/if}

  <!-- SUCCESSION — the embedded FULL chain (#582): predecessors → THIS variable →
       successors, oldest→current, so a renamed measure reads top-to-bottom as one
       thing over time. The viewed + current editions stay visible; a long chain's
       bulk collapses into "N earlier"/"N later" disclosures. Omitted for a single
       (no-succession) edition. -->
  {#if showSuccession}
    <section aria-labelledby="succession-heading">
      <h3 id="succession-heading">Succession</h3>

      <ol class="chain">
        <!-- Earlier editions (before the viewed one) — collapsed. -->
        {#if earlier.length > 0}
          {@render editionRun(
            `${earlier.length} earlier edition${earlier.length === 1 ? "" : "s"}`,
            earlier,
          )}
        {/if}

        <!-- The VIEWED edition, marked in place. When it is ALSO the
             current/terminal edition it carries both tags (a single node). -->
        {#if selfEdition}
          {@render chainNode(selfEdition, true)}
        {/if}

        <!-- Later editions (between the viewed and the current one) — collapsed. -->
        {#if later.length > 0}
          {@render editionRun(
            `${later.length} later edition${later.length === 1 ? "" : "s"}`,
            later,
          )}
        {/if}

        <!-- The CURRENT/terminal edition, always visible — the path's head, the
             latest variable. Omitted when the viewed edition IS the current one. -->
        {#if currentEdition}
          {@render chainNode(currentEdition, true)}
        {/if}
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
  /* #582: the succession chain — a vertical rail (left border on each node) with a
     marker per node, so predecessors → this var → successors reads as one
     continuous timeline. An anchor edition's marker is filled + bold to mark it in
     place (the viewed and/or current edition). Mirrors ClassificationLineagePanels. */
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
  .chain-node.marked {
    border-left-color: var(--accent);
  }
  .chain-node.marked .marker {
    color: var(--accent);
    font-size: 0.85em;
  }
  .chain-node.marked .chain-ref a,
  .chain-node.marked .var-name {
    font-weight: 600;
  }
  .chain-ref {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
  }
  /* The collapsible edition runs — sit on the same rail. */
  .chain-history {
    border-left: 2px solid var(--border);
    margin-left: 0.4rem;
    padding-left: 0.85rem;
  }
  .chain-history summary {
    cursor: pointer;
    color: var(--muted);
    font-size: 0.9em;
  }
  .chain-history summary .marker {
    margin-left: -1.3rem;
    font-size: 0.7em;
  }
  /* The nested run indents under the disclosure. */
  .chain-history .chain {
    margin-top: 0.3rem;
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
