<script lang="ts">
import {
  type ClassificationNodeData,
  type ClassificationRefModel,
  getClassificationPredecessors,
} from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref } from "./catalog";

// The classification-leaf edition-succession panel (#571) — the classification
// analogue of the variable LineagePanels' succession chain (#489). A
// classification carries its EMBEDDED outbound `replaced_by` (the editions that
// replaced this one, toward the current edition); the inbound side (the editions
// THIS edition replaced) is FETCHED via `/classification_predecessors` (the leaf
// embeds only the outbound arm, mirroring the binding leaf).
//
// The two arms compose into ONE period-ordered chain — predecessors → THIS
// edition → successors — so a classification's vintages (e.g. sun1996 → sun2000
// → sun2020) read top-to-bottom as one thing over time. `effective_year` is the
// temporal signal on the edges; we sort ascending with nulls last (an undated
// edge has no place on the timeline, so it trails).
//
// This is a SEPARATE component from the variable LineagePanels (not a conditional
// arm): a classification ref is a single edition `slug` linking to `class/<slug>`
// (NOT a provider/register/variable triple), and there is no
// related/lineage/warnings surface here — so reusing the variable component's
// snippets would mean threading awkward triple/annotation conditionals.
let { fqidPath, node }: { fqidPath: string; node: ClassificationNodeData } =
  $props();

const predecessors = asyncResource(() =>
  getClassificationPredecessors(fqidPath),
);

// Ascending by `effective_year`, nulls last (an undated edge trails — it has no
// place on the timeline). Mirrors LineagePanels' `byEffectiveYearAsc`.
function byEffectiveYearAsc(
  a: ClassificationRefModel,
  b: ClassificationRefModel,
): number {
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

// The chain's two arms, each sorted ascending. `replaced_by` (embedded, outbound)
// is the successor arm — the editions newer than THIS one; the fetched
// `/classification_predecessors` is the older arm. (Tolerate the optional wire
// field's absence on a stale cache — degrade to an empty arm rather than crash.)
const successorChain = $derived(
  [...(node.replaced_by ?? [])].sort(byEffectiveYearAsc),
);
const predecessorChain = $derived(
  [...(predecessors.data?.predecessors ?? [])].sort(byEffectiveYearAsc),
);

// The panel renders ONLY when the chain has >1 edition — a standalone
// classification with no succession shows nothing extra. The embedded
// `replaced_by` is known synchronously; the inbound arm gates on its fetch state
// (we never hide a panel whose inbound state is still unknown / errored, which
// would read as a confirmed standalone). So show the panel when EITHER arm has
// (or might have) an edition beyond THIS one.
const hasSuccessors = $derived(successorChain.length > 0);
const hasPredecessors = $derived(
  predecessors.loading || !!predecessors.error || predecessorChain.length > 0,
);
const show = $derived(hasSuccessors || hasPredecessors);
</script>

<!-- One chain node: a link to the neighbour edition's `class/<slug>` (or the bare
     slug when the edition is dead / has no live row), showing the edition name +
     year. -->
{#snippet chainNode(ref: ClassificationRefModel)}
  <li class="chain-node">
    <span class="marker" aria-hidden="true">○</span>
    <span class="chain-ref">
      {#if ref.fqid}
        <a href={catalogHref(ref.fqid)}>{ref.slug}</a>
      {:else}
        <span class="dead-edition">{ref.slug}</span>
      {/if}
      {#if ref.effective_year != null}
        <span class="muted year">({ref.effective_year})</span>
      {/if}
    </span>
  </li>
{/snippet}

{#if show}
  <section aria-labelledby="cls-succession-heading" class="cls-succession">
    <h3 id="cls-succession-heading">Editions</h3>

    {#if predecessors.loading}
      <p class="muted" aria-busy="true">Loading earlier editions…</p>
    {:else if predecessors.error}
      <p class="error" role="alert">
        Failed to load earlier editions: {predecessors.error}
      </p>
    {/if}

    <ol class="chain">
      <!-- Earlier editions (the predecessor arm) — collapsible; the current
           edition + later editions stay always-visible. The <details> is the
           app's disclosure idiom (matches SearchView's concept-group fold). -->
      {#if predecessorChain.length > 0}
        <li class="chain-history">
          <details>
            <summary>
              <span class="marker" aria-hidden="true">○</span>
              {predecessorChain.length} earlier edition{predecessorChain.length ===
              1
                ? ""
                : "s"}
            </summary>
            <ol class="chain">
              {#each predecessorChain as ref, i (ref.fqid ?? ref.slug + i)}
                {@render chainNode(ref)}
              {/each}
            </ol>
          </details>
        </li>
      {/if}

      <!-- THIS edition, marked in place — a non-link "current edition" node. -->
      <li class="chain-node current" aria-current="true">
        <span class="marker" aria-hidden="true">●</span>
        <span class="this-edition">
          <span class="edition-name">{node.name}</span>
          <code class="ref-fqid">{node.fqid}</code>
          <span class="muted year">(current edition)</span>
        </span>
      </li>

      <!-- Later editions (the successor arm), always visible — they are the path
           toward the current standard. -->
      {#each successorChain as ref, i (ref.fqid ?? ref.slug + i)}
        {@render chainNode(ref)}
      {/each}
    </ol>
  </section>
{/if}

<style>
  .cls-succession {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .muted {
    color: var(--muted);
  }
  .year {
    font-size: 0.85em;
  }
  /* The succession chain — a vertical rail with a marker per node, mirroring the
     variable LineagePanels chain. The current edition's marker is filled + bold
     to mark it in place. */
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
  .chain-node.current .edition-name {
    font-weight: 600;
  }
  .chain-ref,
  .this-edition {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
  }
  .ref-fqid {
    font-size: 0.85em;
    color: var(--muted);
  }
  .dead-edition {
    color: var(--muted);
  }
  /* The collapsible "earlier editions" history — sits on the same rail. */
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
  /* The nested predecessor chain indents under the disclosure. */
  .chain-history .chain {
    margin-top: 0.3rem;
  }
</style>
