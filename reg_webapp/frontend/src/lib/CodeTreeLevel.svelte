<script lang="ts">
import type { SvelteSet } from "svelte/reactivity";
import { type ClassificationCodeModel, getValues } from "./api";
import { asyncResource } from "./async.svelte";
import CodeTreeLevel from "./CodeTreeLevel.svelte";
import { Button, Skeleton } from "./ui";

// One level of a classification's code tree (#1297): the roots (`parent` "") or
// one code's children, read through `values?parent=`. A row with children is a
// native disclosure whose subtree mounts only while it is open, so a closed node
// issues no request.
//
// A level is read once per panel: `levels` caches each level's read by
// classification and parent, so collapsing and reopening a node, or clearing a
// filter, never reads it again. A level longer than one page is read page by page
// within its node; there is no "load more". While it loads, the node reserves one
// skeleton row per child it announced (`expected`), so the list below does not
// jump when the children land (#1139).

/** A level's read, shared by every level of one panel. */
export type Levels = Map<string, Promise<ClassificationCodeModel[]>>;

interface Props {
  /** The classification FQID. */
  ref: string;
  /** "" for the roots, else the code whose children this level lists. */
  parent: string;
  /** How many rows to reserve while loading: the parent's `child_count`. */
  expected: number | null;
  levels: Levels;
  /** The open nodes (ref and code), kept for the panel's life so a filter
   * round-trip reopens the tree where the reader left it. */
  opened: SvelteSet<string>;
}

let { ref, parent, expected, levels, opened }: Props = $props();

// The server's ceiling, as the flat panel reads.
const PAGE_SIZE = 200;
// The most skeleton rows a node reserves: past this the shared scroll box (18rem)
// is full, so more rows reserve nothing visible.
const MAX_RESERVED = 20;

async function readLevel(): Promise<ClassificationCodeModel[]> {
  const rows: ClassificationCodeModel[] = [];
  let cursor: string | null = null;
  do {
    const page = await getValues(ref, { parent, cursor, limit: PAGE_SIZE });
    rows.push(...(page.items as ClassificationCodeModel[]));
    cursor = page.next_cursor ?? null;
  } while (cursor !== null);
  return rows;
}

// Bumped by Retry, and READ inside the fetch, so a failed level reads again.
let attempt = $state(0);

const resource = asyncResource(() => {
  void attempt;
  const key = `${ref}\u0000${parent}`;
  let read = levels.get(key);
  if (!read) {
    read = readLevel();
    // A failed read is not cached: Retry, or reopening the node, reads again.
    read.catch(() => levels.delete(key));
    levels.set(key, read);
  }
  return read;
});

const nodeKey = (code: string) => `${ref}\u0000${code}`;
function toggle(code: string, event: Event): void {
  if ((event.currentTarget as HTMLDetailsElement).open) {
    opened.add(nodeKey(code));
  } else {
    opened.delete(nodeKey(code));
  }
}
</script>

{#if resource.data}
  <ul class="tree-level" class:nested={parent !== ""}>
    {#each resource.data as code (code.code + "\u0000" + code.label)}
      <li>
        {#if code.child_count}
          <details open={opened.has(nodeKey(code.code))} ontoggle={(e) => toggle(code.code, e)}>
            <summary class="tree-row">
              <code class="code-key">{code.code}</code>
              <span class="code-label">{code.label} <span class="child-count">({code.child_count}<span class="visually-hidden"> codes</span>)</span></span>
            </summary>
            {#if opened.has(nodeKey(code.code))}
              <CodeTreeLevel {ref} parent={code.code} expected={code.child_count} {levels} {opened} />
            {/if}
          </details>
        {:else}
          <div class="tree-row leaf">
            <code class="code-key">{code.code}</code>
            <span class="code-label">{code.label}</span>
          </div>
        {/if}
      </li>
    {/each}
  </ul>
  {#if resource.data.length === 0}
    <p class="tree-empty" class:nested={parent !== ""}>No codes under {parent || "this classification"}.</p>
  {/if}
{:else if resource.error}
  <div class="tree-error" class:nested={parent !== ""} role="alert">
    <p class="error"><span aria-hidden="true">✕</span> Could not load codes: {resource.error}</p>
    <Button size="sm" onclick={() => (attempt += 1)}>Retry</Button>
  </div>
{:else}
  <div class="tree-loading" class:nested={parent !== ""} aria-busy="true">
    <span class="visually-hidden">Loading codes…</span>
    {#each { length: Math.min(expected ?? 3, MAX_RESERVED) } as _, i (i)}
      <div class="tree-row"><span class="reserve"><Skeleton width="60%" /></span></div>
    {/each}
  </div>
{/if}

<style>
  .tree-level {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  /* A level below the roots: one indent step with a hairline guide, as CodeList
     indents a levelled row. */
  .nested {
    margin-block-start: var(--space-1);
    margin-inline-start: calc(0.45rem + var(--space-2));
    padding-inline-start: calc(0.45rem + var(--space-2) - 1px);
    border-inline-start: 1px solid var(--border);
  }
  .tree-loading {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  /* A reserved row is as tall as the code row it stands in for. */
  .tree-loading .tree-row {
    box-sizing: content-box;
    min-height: 1lh;
    align-items: center;
  }
  .reserve {
    flex: 1;
  }
  /* CodeList's row, with its spacing so the two lists read alike: mono code,
     wrapping label. The disclosure glyph takes a fixed
     column so a leaf's code lines up under its open sibling's. */
  .tree-row {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.2rem 0;
  }
  .tree-row::before {
    flex: 0 0 var(--space-3);
    content: "";
    color: var(--text-muted);
  }
  summary.tree-row {
    list-style: none;
    cursor: pointer;
    border-radius: var(--radius-sm);
  }
  summary.tree-row::-webkit-details-marker {
    display: none;
  }
  summary.tree-row::before {
    content: "▸" / "";
  }
  details[open] > summary.tree-row::before {
    content: "▾" / "";
  }
  summary.tree-row:hover {
    background: var(--surface-hover);
  }
  summary.tree-row:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .code-key {
    flex: 0 0 auto;
    min-width: 3.5rem;
    /* A classification code — a machine identifier, so mono-faced (DESIGN.md). */
    font-family: var(--font-mono);
    color: var(--text-muted);
    font-size: var(--text-mono);
  }
  .code-label {
    flex: 1;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .child-count {
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: var(--text-sm);
  }
  .tree-empty {
    margin: 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .tree-error {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-3);
  }
  .tree-error p {
    margin: 0;
    font-size: var(--text-sm);
  }
</style>
