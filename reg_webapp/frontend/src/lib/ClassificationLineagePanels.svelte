<script lang="ts">
import type { ClassificationChainEdition, ClassificationNodeData } from "./api";
import { catalogHref } from "./catalog";

// The classification-leaf edition-succession panel (#571) — the classification
// analogue of the variable LineagePanels' succession chain (#489). The node EMBEDS
// the FULL edition chain (`edition_chain`, oldest first → terminal/current last),
// so the panel renders it SYNCHRONOUSLY — no per-neighbour fetch. Each edition
// carries `is_self` (the edition being viewed) and `is_current` (the terminal /
// latest edition). Every chain edition is a live classification row (the build
// validator guarantees succession editions are live), so `fqid` is null only when
// the slug is missing/unresolvable; such an edition renders as plain text rather
// than a link (a generic, type-justified null-guard on the optional wire field).
//
// This is a SEPARATE component from the variable LineagePanels (not a conditional
// arm): a classification edition is a single `slug` linking to `class/<slug>` (NOT
// a provider/register/variable triple), and there is no related/lineage/warnings
// surface here — so reusing the variable component's snippets would mean threading
// awkward triple/annotation conditionals.
let { node }: { node: ClassificationNodeData } = $props();

// The embedded chain (already oldest→current ordered; tolerate the optional wire
// field's absence on a stale cache — degrade to empty rather than crash).
const chain = $derived(node.edition_chain ?? []);

// The panel renders ONLY for a real succession (>1 edition). A standalone
// classification is a 1-element chain (just `is_self` + `is_current`) — it shows
// nothing extra.
const show = $derived(chain.length > 1);

// The two always-visible anchors split the chain into collapsible groups so a long
// chain (e.g. lkf = 47 editions) doesn't render 47 tall rows by default:
//   - the VIEWED edition (`is_self`) — "you are here";
//   - the CURRENT/terminal edition (`is_current`) — always last in the chain.
// When viewing the latest, ONE edition carries both flags. The bulk collapses into
// up to two <details> disclosures (the app's disclosure idiom — mirrors SearchView's
// concept-group fold): editions BEFORE the viewed one ("N earlier editions") and
// editions BETWEEN the viewed and the current one ("N later editions"). The viewed
// and current editions stay visible without expanding.
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

<!-- One chain node on the vertical rail: a link to the edition's `class/<slug>` (or
     plain text when `fqid` is missing/unresolvable — succession editions are live
     rows, so this is a generic null-guard), showing its name + effective year.
     `marked` fills the marker + bolds the name for an anchor edition (the viewed
     and/or current edition). -->
{#snippet chainNode(edition: ClassificationChainEdition, marked: boolean)}
  <li class="chain-node" class:marked aria-current={edition.is_self ? "true" : undefined}>
    <span class="marker" aria-hidden="true">{marked ? "●" : "○"}</span>
    <span class="chain-ref">
      {#if edition.fqid}
        <a href={catalogHref(edition.fqid)}>{edition.name ?? edition.slug}</a>
      {:else}
        <span class="no-link-edition">{edition.name ?? edition.slug}</span>
      {/if}
      {#if edition.effective_year != null}
        <span class="muted year">({edition.effective_year})</span>
      {/if}
      {#if edition.is_self}
        <span class="muted tag">you are here</span>
      {/if}
      {#if edition.is_current}
        <span class="muted tag">current edition</span>
      {/if}
    </span>
  </li>
{/snippet}

<!-- A collapsed run of editions on the same rail — the <details> disclosure idiom
     (matches SearchView's concept-group fold). Hidden by default; the full ordered
     run reveals on expand. -->
{#snippet editionRun(label: string, editions: ClassificationChainEdition[])}
  <li class="chain-history">
    <details>
      <summary>
        <span class="marker" aria-hidden="true">○</span>
        {label}
      </summary>
      <ol class="chain">
        {#each editions as edition, i (edition.fqid ?? edition.slug + i)}
          {@render chainNode(edition, false)}
        {/each}
      </ol>
    </details>
  </li>
{/snippet}

{#if show}
  <section aria-labelledby="cls-succession-heading" class="cls-succession">
    <h3 id="cls-succession-heading">Editions</h3>

    <ol class="chain">
      <!-- Earlier editions (before the viewed one) — collapsed. -->
      {#if earlier.length > 0}
        {@render editionRun(
          `${earlier.length} earlier edition${earlier.length === 1 ? "" : "s"}`,
          earlier,
        )}
      {/if}

      <!-- The VIEWED edition, marked in place. When it is ALSO the current/terminal
           edition it carries both tags (a single node). -->
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

      <!-- The CURRENT/terminal edition, always visible — the path's head, the latest
           standard. Omitted when the viewed edition IS the current one (one node). -->
      {#if currentEdition}
        {@render chainNode(currentEdition, true)}
      {/if}
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
  .tag {
    font-size: 0.85em;
    font-style: italic;
  }
  /* The succession chain — a vertical rail with a marker per node, mirroring the
     variable LineagePanels chain. An anchor edition's marker is filled + bold to
     mark it in place (the viewed and/or current edition). */
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
  .chain-node.marked .no-link-edition {
    font-weight: 600;
  }
  .chain-ref {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
  }
  .no-link-edition {
    color: var(--muted);
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
</style>
