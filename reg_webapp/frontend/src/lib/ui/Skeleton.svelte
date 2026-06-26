<script lang="ts">
// Loading placeholder (#804) — replaces bare `aria-busy` text with a shimmer
// block. The visual is `aria-hidden`: the LOADING SEMANTICS belong to the
// container (the panel/region that swaps Skeleton for content sets its own
// `aria-busy`/live-region), so the placeholder itself is decorative and stays
// out of the a11y tree.
//
// Honors `prefers-reduced-motion`: the shimmer animation is gated behind a
// `no-preference` query, so reduced-motion users get a static block (the
// data-tool motion budget — DESIGN.md → motion).

interface Props {
  variant?: "line" | "block";
  /** CSS width (e.g. "12rem", "60%"). Defaults to full width. */
  width?: string;
  /** Repeat count — render N stacked placeholders. */
  count?: number;
}

let { variant = "line", width, count = 1 }: Props = $props();
</script>

<div class="skeleton-stack" aria-hidden="true">
  {#each { length: count } as _, i (i)}
    <div class="skeleton {variant}" style={width ? `width: ${width}` : undefined}></div>
  {/each}
</div>

<style>
  .skeleton-stack {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
  .skeleton {
    background: var(--surface-sunken);
    border-radius: var(--radius-sm);
  }
  .line {
    height: 0.85em;
  }
  .block {
    height: 4rem;
    border-radius: var(--radius);
  }
  @media (prefers-reduced-motion: no-preference) {
    .skeleton {
      background: linear-gradient(
        90deg,
        var(--surface-sunken) 25%,
        var(--surface-hover) 50%,
        var(--surface-sunken) 75%
      );
      background-size: 200% 100%;
      animation: shimmer 1.4s ease-in-out infinite;
    }
    @keyframes shimmer {
      0% {
        background-position: 200% 0;
      }
      100% {
        background-position: -200% 0;
      }
    }
  }
</style>
