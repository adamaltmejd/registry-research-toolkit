<script lang="ts">
  import { onMount, type Snippet } from "svelte";

  import { store } from "../store.svelte";

  interface Props {
    /** Used as `aria-labelledby`; the slot must render an element with
     * this id (typically the dialog heading). */
    headingId: string;
    onClose: () => void;
    children: Snippet;
  }

  let { headingId, onClose, children }: Props = $props();

  let dialogEl: HTMLDivElement | undefined = $state();
  let cardEl: HTMLDivElement | undefined = $state();

  // Auto-close on stale-state recovery: the modal's snapshot of the
  // column/group is no longer trustworthy. The first effect pass latches
  // a baseline; every subsequent pass means the store bumped the tick,
  // so we close. A boolean (rather than comparing to a sentinel value)
  // avoids coupling to whatever initial value the store happens to use.
  let observedTick = false;
  $effect(() => {
    void store.staleRecoveryTick;
    if (!observedTick) {
      observedTick = true;
      return;
    }
    onClose();
  });

  // Find focusable descendants of the card. The element-level `inert`
  // check below only catches elements directly marked inert; we rely on
  // the portal-to-body design to guarantee no ancestor of `cardEl`
  // (above the dialog node itself) is inerted. If that invariant ever
  // breaks — e.g. someone nests a Modal inside another inerted region
  // — the trap would silently leak focus, so the assumption is loud
  // here on purpose.
  function focusableTargets(): HTMLElement[] {
    if (!cardEl) return [];
    // `summary` is focusable when its parent `<details>` is the standard
    // disclosure widget — omitting it would let Tab jump out of the trap
    // when a `<details>` (e.g. ColumnTypeEditor's "show source names") is
    // the natural next focus target.
    return Array.from(
      cardEl.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])',
      ),
    ).filter((el) => !el.hasAttribute("inert"));
  }

  // Pull surrounding page content out of the tab order while the modal
  // is open. `inert` is the right tool: it removes the subtree from the
  // accessibility tree and blocks focus, so screen-reader and Tab users
  // both stay scoped to the dialog. To make the inert sweep tractable
  // (otherwise we'd have to walk every ancestor of the dialog and inert
  // their siblings), portal the dialog node to <body>; every body child
  // except the dialog itself can then be inerted in one pass.
  let inertTargets: Element[] = [];
  onMount(() => {
    const previouslyFocused = document.activeElement as HTMLElement | null;
    if (dialogEl && dialogEl.parentElement !== document.body) {
      document.body.appendChild(dialogEl);
    }
    // Skip non-interactive head-style siblings; setting `inert` on them
    // is a no-op but bloats the cleanup list and clutters the DOM during
    // inspection. Vite injects style tags into the body in dev mode, so
    // this matters in practice.
    const NON_INTERACTIVE = new Set(["SCRIPT", "STYLE", "LINK", "META"]);
    for (const sibling of Array.from(document.body.children)) {
      if (sibling === dialogEl) continue;
      if (NON_INTERACTIVE.has(sibling.tagName)) continue;
      if (!sibling.hasAttribute("inert")) {
        sibling.setAttribute("inert", "");
        inertTargets.push(sibling);
      }
    }
    // Initial focus: the dialog wrapper itself (tabindex=-1). Tab will
    // move to the first form control, Shift+Tab to the last, and Esc
    // closes immediately because the keydown lands on a node inside the
    // dialog. Avoiding an explicit focus on the first input keeps screen
    // readers from announcing "categorical, radio button, 2 of 5" before
    // the dialog title — they read the title first because focus is on
    // the dialog. Synchronous focus inside onMount: the node is already
    // attached to the body at this point, so no deferral is needed and
    // we close the gap where Tab/Esc pressed before a setTimeout fires
    // would land on the previously-focused element.
    dialogEl?.focus();
    return () => {
      for (const t of inertTargets) {
        t.removeAttribute("inert");
      }
      // Remove the portaled node — Svelte's destroy step expects to
      // clean up its rendered tree but won't traverse out of #app.
      if (dialogEl && dialogEl.parentElement === document.body) {
        dialogEl.remove();
      }
      // Only restore focus if the originating element is still in the
      // DOM. After a stale-state refresh the snapshot may have been
      // re-rendered and the trigger button replaced, in which case
      // focusing a detached node is a silent no-op at best and a
      // confusing accessibility regression at worst.
      if (previouslyFocused?.isConnected) {
        previouslyFocused.focus();
      }
    };
  });

  function onKeydown(event: KeyboardEvent): void {
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") return;
    const targets = focusableTargets();
    if (targets.length === 0) return;
    const first = targets[0];
    const last = targets[targets.length - 1];
    const active = document.activeElement as HTMLElement | null;
    if (event.shiftKey) {
      if (active === first || !cardEl?.contains(active)) {
        event.preventDefault();
        last.focus();
      }
    } else {
      if (active === last) {
        event.preventDefault();
        first.focus();
      }
    }
  }
</script>

<div
  class="overlay"
  role="dialog"
  aria-modal="true"
  aria-labelledby={headingId}
  tabindex="-1"
  bind:this={dialogEl}
  onclick={(e) => {
    if (e.target === e.currentTarget) onClose();
  }}
  onkeydown={onKeydown}
>
  <div class="card" bind:this={cardEl}>
    {@render children()}
  </div>
</div>

<style>
  .overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.35);
    display: grid;
    place-items: center;
    z-index: 100;
  }
  .card {
    background: #fff;
    border-radius: 6px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
    padding: 1rem 1.25rem;
    width: min(32rem, 92vw);
    /* Cap height and let the inner form region scroll. Consumers that
       structure their form as `<header> + <div class="modal-body"> +
       <footer>` get sticky action buttons for free. Without this cap,
       a PanelEditor against a register with 30+ sources pushes the
       footer below the viewport — and because surrounding content is
       inert, the user can't scroll the page to reach it either. */
    max-height: min(85vh, 100%);
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    overflow: hidden;
  }
</style>
