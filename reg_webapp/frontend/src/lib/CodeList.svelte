<script lang="ts">
import { Collapsible } from "bits-ui";
import type { ClassificationExtensionMemberModel } from "./api";
import { formatWindow } from "./catalog";

// The UNIFIED value-set / code viewer (#638 PR3). A variable's value set and a
// classification's code list are the same thing — a code→label set (a value set
// often IS a classification) — so they render IDENTICALLY here: the
// classification-list style (a <ul> of code rows), used for BOTH.
//
// The caller (ValueSetCodes) filters and bounds the codes SERVER-side and hands
// over the pages it has read, so this renders them verbatim in a
// height-constrained scroll: no second filter to compete with the caller's, and no
// grouping — a partial page would group under parents it may not hold.

// A code→label set member. Covers BOTH shapes: classification codes and variable
// value-set members.
interface Code {
  code: string;
  label: string;
  level?: number | null;
  member_kind?: ClassificationExtensionMemberModel["member_kind"];
  sentinel_meaning?: ClassificationExtensionMemberModel["sentinel_meaning"];
  scoped_sentinels?: ClassificationExtensionMemberModel["scoped_sentinels"];
}

let { codes }: { codes: Code[] } = $props();

function codeLevel(code: Code): number | null {
  return typeof code.level === "number" && Number.isFinite(code.level)
    ? code.level
    : null;
}

// Depth counts from the shallowest level loaded, not from level 1: most
// classifications start at level 2 or deeper (ICD-10-SE is all level 2), and
// those must render flat. The first page is in code order, so it holds the top
// level.
const topLevel = $derived(
  Math.min(
    ...codes.map(codeLevel).filter((level): level is number => level != null),
  ),
);

function depthOf(code: Code): number | null {
  const level = codeLevel(code);
  return level == null ? null : level - topLevel + 1;
}
</script>

{#snippet codeRow(code: Code, depth: number | null)}
  <!-- `depth` is the row's level below the shallowest loaded one (1 at the top):
       a page cannot group under parents it may not hold, so each levelled row
       carries its depth itself — `aria-level` for the accessibility tree, and an
       indent step plus a hairline guide per level below the top. -->
  <li
    class="code-row"
    class:levelled={depth != null && depth > 1}
    aria-level={depth ?? undefined}
    style:--code-depth={depth != null ? depth - 1 : undefined}
  >
    <code class="code-key">{code.code}</code>
    <span class="code-label">{code.label}{#if code.member_kind === "sentinel"}
        {#if code.sentinel_meaning}<span class="sentinel-meaning">Special code: {code.sentinel_meaning}</span>{/if}
        {#each code.scoped_sentinels ?? [] as evidence}
          <span class="sentinel-meaning">Special code for <code>{evidence.delivery_column_name}</code>
            {formatWindow(evidence.valid_from ?? null, evidence.valid_to ?? null) || "unknown period"}:
            {evidence.members.filter(([member]) => member === code.code).map(([, meaning]) => meaning).join("; ")}
          </span>
          <Collapsible.Root><Collapsible.Trigger class="evidence-toggle">Source evidence</Collapsible.Trigger><Collapsible.Content><p>{evidence.provenance}</p></Collapsible.Content></Collapsible.Root>
        {/each}
      {/if}</span>
  </li>
{/snippet}

{#if codes.length > 0}
  <div class="code-scroll">
    <ul class="codes">
      {#each codes as code, i (i)}
        {@render codeRow(code, depthOf(code))}
      {/each}
    </ul>
  </div>
{/if}

<style>
  /* Height-constrained so large lists (LISA value sets run to hundreds of codes)
     stay bounded — the variable table's former `.value-set-scroll` idiom, now the
     shared scroll for both contexts. */
  .code-scroll {
    max-height: 18rem;
    overflow-y: auto;
  }
  .codes {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .code-row {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.2rem 0;
  }
  /* A row below the top loaded level: one indent step per level, with a hairline
     guide so the depth reads without the parent row on screen. */
  .code-row.levelled {
    margin-inline-start: calc(
      (var(--code-depth) - 1) * (0.45rem + var(--space-2))
    );
    padding-inline-start: calc(0.45rem + var(--space-2) - 1px);
    border-inline-start: 1px solid var(--border);
  }
  .code-key {
    flex: 0 0 auto;
    min-width: 3.5rem;
    /* A value-set code — a machine identifier, so mono-faced (DESIGN.md). */
    font-family: var(--font-mono);
    color: var(--text-muted);
    font-size: var(--text-mono);
  }
  .sentinel-meaning {
    display: block;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .code-label {
    flex: 1;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  :global(.evidence-toggle) {
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text);
    font: inherit;
    font-size: var(--text-sm);
    padding: var(--space-1) var(--space-2);
    cursor: pointer;
  }
  :global(.evidence-toggle:hover) {
    background: var(--surface-hover);
    border-color: var(--border-strong);
  }
  :global(.evidence-toggle:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }
</style>
