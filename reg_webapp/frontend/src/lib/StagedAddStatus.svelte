<script lang="ts">
import { type StagedApplyOutcome, stagedDiffSummary } from "./staged_picker";

// The one line a catalog page says after an Apply: the REFUSAL when the batch was
// declined whole and nothing was authored, else the CONFIRMATION of what was
// committed. One component because all three authoring pages (binding leaf, concept
// group, register list) say it, and a third copy of the markup + tint is exactly the
// leaf-duplication CLAUDE.md names.

let {
  outcome = null,
  blocked = null,
}: {
  /** What the last Apply committed, or null (nothing applied yet / cleared). */
  outcome?: StagedApplyOutcome | null;
  /** The refusal copy when an Apply was declined whole and nothing was authored,
   * else null. The page passes its own wording: it knows which gate refused and
   * which of ITS controls the researcher can reach for. A refusal wins over a stale
   * confirmation. */
  blocked?: string | null;
} = $props();

const applied = $derived(outcome === null ? "" : stagedDiffSummary(outcome));
</script>

{#if blocked}
  <!-- The Apply was refused before the store was touched, so nothing was
       authored. A status row (frontend/DESIGN.md → Banners and status rows): the
       status tint as fill, a glyph first, plain copy. -->
  <p class="page-add">
    <!-- DESIGN.md status glyphs: ▲ warn. Every refusal a host can show is one the
         researcher's own next move retires, so it announces as the calmer
         `status` rather than an `alert`. -->
    <span class="add-blocked" role="status">
      <span aria-hidden="true">▲</span>
      {blocked}
    </span>
  </p>
{:else if outcome}
  <!-- A status row (frontend/DESIGN.md → Banners and status rows), same shape as
       the refusal above: the `--ok` tint as fill, a glyph first, plain copy. -->
  <p class="page-add">
    <span class="add-confirm" role="status">
      <!-- DESIGN.md status glyphs: ✓ ok. -->
      <span aria-hidden="true">✓</span>
      Applied {applied} — <a href="/project">view</a>
    </span>
  </p>
{/if}

<style>
  /* The add-confirmation line (the picker / the page's own bar owns the Add). */
  .page-add {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-3);
    margin: var(--space-4) 0;
  }
  .add-blocked {
    display: inline-flex;
    align-items: baseline;
    gap: var(--space-2);
    padding: var(--space-2) var(--space-3);
    border-radius: var(--radius-sm);
    font-size: var(--text-sm);
    background: var(--warn-bg);
    color: var(--warn);
  }
  .add-confirm {
    display: inline-flex;
    align-items: baseline;
    gap: var(--space-2);
    padding: var(--space-2) var(--space-3);
    border-radius: var(--radius-sm);
    font-size: var(--text-sm);
    background: var(--ok-bg);
    color: var(--ok);
  }
</style>
