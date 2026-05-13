<script lang="ts">
  import type { Snippet } from "svelte";

  interface Props {
    /** Lines rendered into the popover, one per row. Empty or all-empty
     *  arrays suppress the popover entirely and the trigger renders as
     *  inert text without the dotted-underline cue. */
    lines: string[];
    /** Trigger content. Inline-only — wrap with `<strong>` / `<span class="tag">`
     *  at the call site if you want extra styling on the visible text. */
    children: Snippet;
  }

  let { lines, children }: Props = $props();

  let hasPopover = $derived(lines.some((l) => l.length > 0));

  // Hover and focus are tracked separately so leaving the mouse while
  // the trigger is still tab-focused doesn't dismiss the popover.
  let hovered = $state(false);
  let focused = $state(false);
  // Click-to-toggle pins the popover open after a tap (mobile / touchpad
  // users who can't hover). The pin clears on blur so re-tabbing through
  // the page doesn't leave a trail of stuck tooltips.
  let pinned = $state(false);
  let open = $derived(hasPopover && (hovered || focused || pinned));

  function onKeydown(event: KeyboardEvent): void {
    if (event.key === "Escape") {
      pinned = false;
      (event.currentTarget as HTMLElement).blur();
    }
  }
  function onClick(event: MouseEvent): void {
    // Prevent the parent <details>/<summary>/<label> from interpreting
    // the click — pinning a tooltip shouldn't expand a card or focus
    // the input next to a form label.
    event.preventDefault();
    event.stopPropagation();
    pinned = !pinned;
  }
</script>

{#if hasPopover}
  <button
    type="button"
    class="hint-trigger has-popover"
    aria-haspopup="true"
    aria-expanded={open}
    onmouseenter={() => (hovered = true)}
    onmouseleave={() => (hovered = false)}
    onfocus={() => (focused = true)}
    onblur={() => {
      focused = false;
      pinned = false;
    }}
    onkeydown={onKeydown}
    onclick={onClick}
  >
    {@render children()}
    {#if open}
      <span class="hint-popover" role="tooltip">
        {#each lines as line, i (i)}
          <span class="hint-line">{line}</span>
        {/each}
      </span>
    {/if}
  </button>
{:else}
  <span class="hint-trigger">{@render children()}</span>
{/if}

<style>
  /* The trigger is a real <button> so it inherits keyboard activation,
     focus management, and AT semantics for free. Visual styling is
     stripped back so the trigger reads as inline text with a hover
     cue, not as a chrome-coloured button. */
  .hint-trigger {
    position: relative;
    display: inline;
    background: transparent;
    border: 0;
    padding: 0;
    margin: 0;
    color: inherit;
    font: inherit;
    text-align: inherit;
    cursor: inherit;
  }
  .hint-trigger.has-popover {
    cursor: help;
    text-decoration: underline dotted rgba(0, 0, 0, 0.3);
    text-underline-offset: 2px;
  }
  .hint-trigger.has-popover:focus-visible {
    outline: 2px solid #1656c0;
    outline-offset: 2px;
    border-radius: 2px;
  }
  .hint-popover {
    position: absolute;
    top: calc(100% + 0.3rem);
    left: 0;
    z-index: 30;
    background: #1f2430;
    color: #fff;
    padding: 0.45rem 0.6rem;
    border-radius: 4px;
    font-size: 0.78rem;
    line-height: 1.35;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    min-width: max-content;
    max-width: 32rem;
    text-align: left;
    text-decoration: none;
    text-transform: none;
    letter-spacing: normal;
    font-weight: normal;
    cursor: default;
    /* Long single lines (e.g. 30-source comma list) should wrap rather
       than overflow the viewport — anywhere lets unbroken strings break. */
    white-space: normal;
    overflow-wrap: anywhere;
  }
  .hint-line {
    display: block;
  }
</style>
