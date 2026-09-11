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
  blockedTone = "warn",
  note = null,
}: {
  /** What the last Apply committed, or null (nothing applied yet / cleared). */
  outcome?: StagedApplyOutcome | null;
  /** The refusal copy when an Apply was declined whole and nothing was authored,
   * else null. The page passes its own wording: it knows which gate refused and
   * which of ITS controls the researcher can reach for. A refusal wins over a stale
   * confirmation. */
  blocked?: string | null;
  /** `error` when `blocked` names a read that failed rather than a choice anyone
   * made — nobody refused anything, the batch just couldn't be evaluated — vs the
   * default `warn` for a refusal the researcher's own next move retires (DESIGN.md
   * status glyphs: ✕ error, ▲ warn). */
  blockedTone?: "warn" | "error";
  /** Rides beside a successful `outcome`: names a ticked column the batch could
   * NOT commit (the register list's exact-era drop, read only after the tick gate's
   * own aggregate pass), so a partial Add never reads as a complete one. Ignored
   * without `outcome`. */
  note?: string | null;
} = $props();

const applied = $derived(outcome === null ? "" : stagedDiffSummary(outcome));
// DESIGN.md status glyphs: ✕ error, ▲ warn. `error` announces `alert` (nothing
// to act on but retry); `warn` is the calmer `status`, a refusal the researcher's
// own next move retires.
const blockedGlyph = $derived(blockedTone === "error" ? "✕" : "▲");
const blockedRole = $derived(blockedTone === "error" ? "alert" : "status");
</script>

{#if blocked}
  <!-- The Apply was refused before the store was touched, so nothing was authored
       — or, for `error`, never even evaluated. A status row (frontend/DESIGN.md →
       Banners and status rows): the status tint as fill, a glyph first, plain
       copy. -->
  <p class="page-add">
    <span class="add-blocked tone-{blockedTone}" role={blockedRole}>
      <span aria-hidden="true">{blockedGlyph}</span>
      {blocked}
    </span>
  </p>
{:else if outcome}
  <p class="page-add">
    <span class="add-confirm" role="status">
      Applied {applied} — <a href="/project">view</a>
      {#if note}
        <span class="add-note">· {note}</span>
      {/if}
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
  }
  .add-blocked.tone-warn {
    background: var(--warn-bg);
    color: var(--warn);
  }
  .add-blocked.tone-error {
    background: var(--err-bg);
    color: var(--err);
  }
  .add-confirm {
    font-size: var(--text-sm);
    color: var(--accent);
  }
  /* The dropped-column note: a caveat beside a real confirmation, not a status of
     its own — muted like the app's other secondary asides (e.g. CatalogNodeView's
     `.column-years`), not a second tinted banner. */
  .add-note {
    color: var(--text-muted);
  }
</style>
