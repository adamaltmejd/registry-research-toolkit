<script lang="ts">
import { type BindingNodeData, getBindingLineageWarnings } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, formatWindow, windowTitle } from "./catalog";

// The binding-leaf NON-graph lineage affordances (#678) — the two surfaces the
// relationship-graph payload (#761) does NOT carry, re-homed here off the retired
// LineagePanels so they survive on the binding leaf with no regression:
//
//   PROVENANCE — `node.lineage[]` (consumer/source edges, embedded on the leaf):
//                a validity window + source_fqid link per edge (fallback "source
//                state #N"). Plus the variable's `source_register` (a composite
//                register's underlying source), surfaced as a compact line when
//                present. A LIST, not a node-link graph.
//   WARNINGS   — fetched via `/lineage_warnings` (its OWN failure domain): each of
//                loading / error / empty handled; omitted when empty; an error
//                renders inline and NEVER blanks the leaf.
//
// Succession is NOT here — it is a graph EDGE now (HistoryGraph).
//
// Omit-when-empty (the LineagePanels ethos): each section is shown when it has
// data OR (warnings) is still loading / errored — we never hide a section whose
// state is unknown (that would read as a confirmed absence). When BOTH are empty,
// one compact line replaces the headed walls.
let { fqidPath, node }: { fqidPath: string; node: BindingNodeData } = $props();

const warnings = asyncResource(() => getBindingLineageWarnings(fqidPath));

// The composite-register provenance line: a curated `source_register_text` (the
// human-readable source register) when present; else null. `source_register_id`
// alone (no text) carries nothing to render, so the text is the gate.
const sourceRegister = $derived(node.source_register_text ?? null);

const showProvenance = $derived(
  node.lineage.length > 0 || sourceRegister != null,
);
const showWarnings = $derived(
  warnings.loading ||
    !!warnings.error ||
    (warnings.data?.lineage_warnings.length ?? 0) > 0,
);
const anySection = $derived(showProvenance || showWarnings);
</script>

<div class="lineage-details">
  {#if !anySection}
    <!-- Both empty (the common case) — one compact line instead of headed walls. -->
    <p class="muted no-links">No provenance or lineage warnings.</p>
  {/if}

  <!-- PROVENANCE — source register + consumer/source lineage edges -->
  {#if showProvenance}
    <section aria-labelledby="provenance-heading">
      <h3 id="provenance-heading">Provenance</h3>
      {#if sourceRegister}
        <p class="source-register">
          <span class="muted">Source register:</span>
          {sourceRegister}
        </p>
      {/if}
      {#if node.lineage.length > 0}
        <ul class="refs">
          {#each node.lineage as edge (edge.consumer_state_id + ":" + edge.source_state_id)}
            <li>
              <!-- #309: sentinel-free window display (raw ISO on the tooltip). -->
              <span
                class="muted edge-validity"
                title={windowTitle(edge.valid_from, edge.valid_to)}
              >
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
      {/if}
    </section>
  {/if}

  <!-- LINEAGE WARNINGS — fetched (its own failure domain) -->
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
  .lineage-details {
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
  .source-register {
    margin: 0 0 0.5rem;
    font-size: 0.9rem;
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
    color: var(--warn);
    font-size: 0.85em;
  }
  .error {
    color: var(--err);
  }
  .muted {
    color: var(--text-muted);
  }
</style>
