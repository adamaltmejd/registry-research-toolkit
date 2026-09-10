<script lang="ts">
import type { Snippet } from "svelte";

// The shared multi-select filter chip (Y-82). A checkbox faced as a selectable
// chip: the native input is visually hidden (the `.visually-hidden` utility) but
// kept in the DOM, so the control stays keyboard-operable, labelled and
// announced as a checkbox — the platform behavior, not a re-implementation.
// `selected` paints the brand accent (selection is interactive chrome, DESIGN.md
// → Color); `--radius-sm` is the control radius (nothing is a pill).
//
// One component for every "narrow this list by a value" strip: the picker's
// per-dimension filters, the concept-group navigator's per-axis filters, and the
// register page's variant chips — each of which had pasted its own copy.
// Content is a snippet, not a string: a call site pairs the value label with its
// own disambiguator (the picker's mono variant key).

let {
  selected = false,
  onToggle,
  children,
}: {
  selected?: boolean;
  /** Called when the chip is toggled; the caller owns the filter set. */
  onToggle: () => void;
  children: Snippet;
} = $props();
</script>

<label class="ui-chip" class:on={selected}>
  <input
    class="visually-hidden"
    type="checkbox"
    checked={selected}
    onchange={onToggle}
  />
  {@render children()}
</label>

<style>
  /* Neutral at rest — no `--cat-*` type palette (that sub-system tags result/node
     TYPE; reusing it here would read a filter value as a CODE/REG chip;
     DESIGN.md → Color). Dimension identity is carried by the strip's TEXT (its
     legend), never by hue. A host tints the chip through a `:global(.ui-chip)`
     descendant rule when its own strip carries a data encoding (the picker's
     facet axes). */
  .ui-chip {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    font-size: var(--text-sm);
    cursor: pointer;
    user-select: none;
    background: var(--surface);
    color: var(--text);
  }
  .ui-chip.on {
    background: var(--accent-bg);
    border-color: var(--accent);
    color: var(--accent-ink);
    font-weight: 600;
  }
  /* Keyboard focus ring on the (hidden) input projects onto its chip label. */
  .ui-chip:focus-within {
    box-shadow: var(--focus-ring);
  }
</style>
