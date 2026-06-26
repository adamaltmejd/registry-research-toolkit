<script lang="ts">
import type { Snippet } from "svelte";
import type { KeyValueRow } from "./types";

// Labelled metadata rows (#804) — the primitive the many ad-hoc "label: value"
// meta blocks converge on. A `<dl>` of term/description: the label is a muted
// micro-label, the value is --text (mono-faced when it's an identifier, via the
// row's `mono` flag).
//
// Two input modes (use one):
//  - `rows`: the common `{ label, value, mono? }[]` data form.
//  - `children`: a snippet, for values richer than a string (a Tag, a link) —
//    the caller composes its own `<div class="kv-row">`-shaped content. When a
//    snippet is passed, `rows` is ignored.

interface Props {
  rows?: KeyValueRow[];
  children?: Snippet;
}

let { rows = [], children }: Props = $props();
</script>

<dl class="key-value">
  {#if children}
    {@render children()}
  {:else}
    <!-- Keyed by index, not label: labels can repeat (e.g. two "Type" rows) and a
         duplicate keyed-each key throws. A static metadata list replaced wholesale
         makes index keys correct here (matches Breadcrumbs.svelte). -->
    {#each rows as row, i (i)}
      <div class="kv-row">
        <dt>{row.label}</dt>
        <dd class:mono={row.mono}>{row.value}</dd>
      </div>
    {/each}
  {/if}
</dl>

<style>
  .key-value {
    margin: 0;
    display: grid;
    gap: var(--space-2);
  }
  .kv-row {
    display: grid;
    grid-template-columns: minmax(8rem, max-content) 1fr;
    gap: var(--space-3);
    align-items: baseline;
  }
  dt {
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    font-weight: 600;
    color: var(--text-muted);
  }
  dd {
    margin: 0;
    color: var(--text);
    font-size: var(--text-sm);
  }
  dd.mono {
    font-family: var(--font-mono);
  }
</style>
