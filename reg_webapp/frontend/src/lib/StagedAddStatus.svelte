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
  <!-- The Apply was refused before the store was touched, so nothing was authored.
       A status row (frontend/DESIGN.md → Banners and status rows): warn tint, glyph
       first, and copy naming what the researcher can do about it. -->
  <p class="page-add">
    <span class="add-blocked" role="alert">
      <span aria-hidden="true">▲</span>
      {blocked}
    </span>
  </p>
{:else if outcome}
  <p class="page-add">
    <span class="add-confirm" role="status">
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
    background: var(--warn-bg);
    color: var(--warn);
    font-size: var(--text-sm);
  }
  .add-confirm {
    font-size: var(--text-sm);
    color: var(--accent);
  }
</style>
