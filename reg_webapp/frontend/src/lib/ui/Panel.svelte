<script lang="ts">
import type { Snippet } from "svelte";

// The unit of grouping (#804 / DESIGN.md → Panels): a header (micro-label
// title + optional meta/badge) over a body, on a raised surface card. The
// header title uses the tracked uppercase micro-label convention — the device
// that gives the dashboard look its hierarchy without heavy headings.
//
// `title` is a string OR a snippet (a title may carry a mono identifier). The
// optional `meta` snippet rides the header's trailing edge (a count Tag, an
// action). Omit-when-empty is the CONSUMER's concern — Panel always renders its
// chrome; a caller with nothing to show simply doesn't render the Panel.

interface Props {
  title: string | Snippet;
  meta?: Snippet;
  children: Snippet;
}

let { title, meta, children }: Props = $props();
</script>

<section class="panel">
  <header class="panel-header">
    <h2 class="panel-title">
      {#if typeof title === "string"}{title}{:else}{@render title()}{/if}
    </h2>
    {#if meta}<div class="panel-meta">{@render meta()}</div>{/if}
  </header>
  <div class="panel-body">{@render children()}</div>
</section>

<style>
  .panel {
    background: var(--surface-raised);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--elevation-raised);
    overflow: hidden;
  }
  .panel-header {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    padding: var(--space-3) var(--space-4);
    border-bottom: 1px solid var(--border);
  }
  .panel-title {
    margin: 0;
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    font-weight: 600;
    color: var(--text-muted);
  }
  .panel-meta {
    display: flex;
    align-items: center;
    gap: var(--space-2);
  }
  .panel-body {
    padding: var(--space-4);
  }
</style>
