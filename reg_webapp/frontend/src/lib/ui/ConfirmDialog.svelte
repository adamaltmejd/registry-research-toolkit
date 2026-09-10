<script lang="ts">
import { AlertDialog } from "bits-ui";
import type { Snippet } from "svelte";

// The shared confirmation dialog (#804). Behavior + a11y come from Bits UI's
// headless `AlertDialog`: role="alertdialog", the title/description wiring, the
// focus trap and the return of focus to whatever was focused when it opened. We
// only own the chrome and the action row.
//
// `interactOutsideBehavior` is left at its "ignore" default, which is right for
// every question worth asking: a stray click on the scrim must not answer one.
// Escape and the caller's Cancel button are the ways out.
//
// Bits UI PORTALS the overlay and content to <body>, so their classes land outside
// this component's scoped CSS — they are styled through `:global`, namespaced
// `ui-dialog*` the way `Button.svelte` namespaces `.ui-btn`.

interface Props {
  /** Bindable for a caller that owns the flag; a caller whose truth lives
   *  elsewhere (a store) passes a one-way `open` plus `onOpenChange`. */
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  /** The question, as a heading. */
  title: string;
  /** What answering it costs. */
  description: Snippet;
  /** The answers, right-aligned — Cancel first, the destructive one last. */
  actions: Snippet;
}

let {
  open = $bindable(false),
  onOpenChange,
  title,
  description,
  actions,
}: Props = $props();
</script>

<AlertDialog.Root bind:open {onOpenChange}>
  <AlertDialog.Portal>
    <AlertDialog.Overlay class="ui-dialog-scrim" />
    <AlertDialog.Content class="ui-dialog">
      <AlertDialog.Title class="ui-dialog-title">{title}</AlertDialog.Title>
      <AlertDialog.Description class="ui-dialog-body">
        {@render description()}
      </AlertDialog.Description>
      <div class="ui-dialog-actions">{@render actions()}</div>
    </AlertDialog.Content>
  </AlertDialog.Portal>
</AlertDialog.Root>

<style>
  /* Above every layer the shell stacks (its drawer is 60, that drawer's own scrim
     55) — a modal the app can paint through is not a modal. Both are portalled to
     <body>, so they share the root stacking context with those. */
  :global(.ui-dialog-scrim) {
    position: fixed;
    inset: 0;
    z-index: 70;
    background: var(--scrim);
  }

  :global(.ui-dialog) {
    position: fixed;
    /* Centred on the wide canvas; at 375px the inset margin governs and the dialog
       fills the width minus that margin — where the actions wrap. 35rem is what
       fits the widest action row (three buttons) on one line from 768 up. A long
       project name in the description scrolls rather than pushing the actions past
       the viewport. */
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: min(35rem, calc(100vw - var(--space-4) * 2));
    max-height: calc(100vh - var(--space-4) * 2);
    overflow-y: auto;
    z-index: 71;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
    padding: var(--space-4);
    background: var(--surface-raised);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--elevation-raised);
  }

  :global(.ui-dialog:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }

  :global(.ui-dialog-title) {
    font-size: var(--text-h3);
    font-weight: var(--heading-weight);
  }

  :global(.ui-dialog-body) {
    color: var(--text-muted);
  }

  :global(.ui-dialog-actions) {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: var(--space-2);
  }
</style>
