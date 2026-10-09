<script lang="ts">
import { getLineage, type VariableShow } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, formatWindow, windowTitle } from "./catalog";

// The binding-leaf NON-graph lineage affordances (#678) — the two surfaces the
// relationship-graph payload (#761) does NOT carry, re-homed here off the retired
// LineagePanels so they survive on the binding leaf with no regression:
//
//   PROVENANCE — the `lineage` read's consumer/source `edges`: a validity window
//                + source_fqid link per edge (fallback "source state #N"),
//                listed once the read lands. Plus the variable's
//                `source_register_text` (a composite register's underlying
//                source) from `node`, a compact line shown at once. A LIST, not
//                a node-link graph.
//   WARNINGS   — the same read's `warnings`. The read is ONE failure domain
//                (edges + warnings); its loading / error render in the warnings
//                section, inline, and NEVER blank the leaf.
//
// Succession is NOT here — it is a graph EDGE now (the picker graph mode).
//
// Omit-when-empty (the LineagePanels ethos): each section is shown when it has
// data OR (warnings) is still loading / errored — we never hide a section whose
// state is unknown (that would read as a confirmed absence). When BOTH are empty,
// render nothing.
let { node }: { node: VariableShow } = $props();

const lineage = asyncResource(() => getLineage(node.fqid));
const edges = $derived(lineage.data?.edges ?? []);

// The composite-register provenance line: a curated `source_register_text` (the
// human-readable source register) when present; else null.
const sourceRegister = $derived(node.source_register_text ?? null);

const showProvenance = $derived(edges.length > 0 || sourceRegister != null);
const showWarnings = $derived(
  lineage.loading ||
    !!lineage.error ||
    (lineage.data?.warnings.length ?? 0) > 0,
);
const anySection = $derived(showProvenance || showWarnings);
</script>

{#if anySection}
  <div class="lineage-details">
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
      {#if edges.length > 0}
        <ul class="refs">
          {#each edges as edge (edge.consumer_state_id + ":" + edge.source_state_id)}
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

  <!-- LINEAGE WARNINGS — and the lineage read's loading / error -->
  {#if showWarnings}
    <section aria-labelledby="lineage-warnings-heading">
      <h3 id="lineage-warnings-heading">Lineage warnings</h3>
      {#if lineage.loading}
        <p class="muted" aria-busy="true">Loading…</p>
      {:else if lineage.error}
        <p class="error" role="alert">
          Failed to load lineage: {lineage.error}
        </p>
      {:else if lineage.data}
        <ul class="warnings">
          {#each lineage.data.warnings as w (w.consumer_state_id + ":" + w.warning_kind)}
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
{/if}

<style>
  .lineage-details {
    margin-top: 1.5rem;
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
    font-weight: var(--heading-weight);
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
