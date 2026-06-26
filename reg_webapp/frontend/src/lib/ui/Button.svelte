<script lang="ts">
import { Button } from "bits-ui";
import type { Snippet } from "svelte";
import type { ButtonSize, ButtonVariant } from "./types";

// The shared button (#804). Behavior + polymorphism come from Bits UI's
// headless `Button` (verified against node_modules/bits-ui/dist/bits/button:
// it renders a real <button>, switches to <a> when `href` is set, carries the
// disabled a11y, and forwards `class` + the rest of the HTML attributes). We
// only own the variant/size scoped CSS + the brand focus ring.
//
// `href` makes it a link (Bits UI's polymorphic branch); pass `onclick` for the
// button branch. The brand accent paints `primary` ONLY (DESIGN.md accent-vs-
// status: accent is interactive chrome, never status — there's no "success"
// button variant; `danger` is its own cool error stop).

interface Props {
  variant?: ButtonVariant;
  size?: ButtonSize;
  href?: string;
  type?: "button" | "submit" | "reset";
  disabled?: boolean;
  onclick?: (event: MouseEvent) => void;
  children: Snippet;
}

let {
  variant = "default",
  size = "md",
  href,
  type,
  disabled,
  onclick,
  children,
}: Props = $props();
</script>

<!-- Bits UI narrows on the presence of `href` (anchor) vs `type` (button); pass
     only the one that applies so its union type resolves. -->
{#if href}
  <Button.Root {href} {disabled} class="btn variant-{variant} size-{size}">
    {@render children()}
  </Button.Root>
{:else}
  <Button.Root
    type={type ?? "button"}
    {disabled}
    {onclick}
    class="btn variant-{variant} size-{size}"
  >
    {@render children()}
  </Button.Root>
{/if}

<style>
  /* Bits UI renders the element; `class` lands on it, so target globally but
     scope through the `.btn` hook this component owns. */
  :global(.btn) {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: var(--space-2);
    font-family: var(--font-ui);
    font-weight: 500;
    border-radius: var(--radius-sm);
    border: 1px solid transparent;
    cursor: pointer;
    text-decoration: none;
    transition: background var(--motion-fast), border-color var(--motion-fast);
  }
  :global(.btn:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  :global(.btn:disabled),
  :global(.btn[aria-disabled="true"]) {
    opacity: 0.5;
    cursor: not-allowed;
  }

  :global(.btn.size-md) {
    padding: var(--space-2) var(--space-3);
    font-size: var(--text-sm);
  }
  :global(.btn.size-sm) {
    padding: var(--space-1) var(--space-2);
    font-size: var(--text-micro);
  }

  :global(.btn.variant-primary) {
    background: var(--accent);
    color: var(--accent-fg);
  }
  :global(.btn.variant-primary:hover:not(:disabled)) {
    background: var(--accent-ink);
  }

  :global(.btn.variant-default) {
    background: var(--surface);
    color: var(--text);
    border-color: var(--border);
  }
  :global(.btn.variant-default:hover:not(:disabled)) {
    background: var(--surface-hover);
    border-color: var(--border-strong);
  }

  :global(.btn.variant-ghost) {
    background: transparent;
    color: var(--text);
  }
  :global(.btn.variant-ghost:hover:not(:disabled)) {
    background: var(--surface-hover);
  }

  :global(.btn.variant-danger) {
    background: var(--err);
    color: #ffffff;
  }
  :global(.btn.variant-danger:hover:not(:disabled)) {
    background: color-mix(in srgb, var(--err) 85%, black);
  }
</style>
