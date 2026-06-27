<script lang="ts">
import type { Snippet } from "svelte";
import type { KeyValueRow } from "./types";

// Labelled metadata rows (#804) — the primitive the many ad-hoc "label: value"
// meta blocks converge on. A `<dl>` of term/description: the label is a muted
// micro-label, the value is --text (mono-faced when it's an identifier, via the
// row's `mono` flag).
//
// `rows` is the common `{ label, value, mono? }[]` data form. For values richer
// than a string (a Tag, a link) pass the per-row `value` snippet: KeyValue still
// owns the `.kv-row`/`dt`/`dd` structure (so the row stays component-styled —
// caller-authored markup would fall outside this scoped CSS), and the snippet
// only supplies the `<dd>` content. A row omitting `value` (plain-string path)
// renders empty unless the snippet fills it.

interface Props {
  rows?: KeyValueRow[];
  /** Per-row rich-value renderer; default renders `row.value`. */
  value?: Snippet<[KeyValueRow]>;
}

let { rows = [], value }: Props = $props();
</script>

<dl class="key-value">
  <!-- Keyed by index, not label: labels can repeat (e.g. two "Type" rows) and a
       duplicate keyed-each key throws. A static metadata list replaced wholesale
       makes index keys correct here (matches Breadcrumbs.svelte). -->
  {#each rows as row, i (i)}
    <div class="kv-row">
      <dt class="micro-label">{row.label}</dt>
      <dd class:mono={row.mono}>
        {#if value}{@render value(row)}{:else}{row.value ?? ""}{/if}
      </dd>
    </div>
  {/each}
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
  dd {
    margin: 0;
    color: var(--text);
    font-size: var(--text-sm);
  }
  dd.mono {
    font-family: var(--font-mono);
  }
</style>
