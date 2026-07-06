<script lang="ts">
import { Accordion, Collapsible } from "bits-ui";
import { matchesFilter } from "./catalog";
import FilterInput from "./FilterInput.svelte";

// The UNIFIED value-set / code viewer (#638 PR3). A variable's value set and a
// classification's code list are the same thing — a code→label set (a value set
// often IS a classification) — so they render IDENTICALLY here: the
// classification-list style (a <ul> of code rows), used for BOTH.
//
// Size-dependent filter (the maintainer's call): sizes vary wildly on both sides
// (tiny classifications, huge LISA value sets), so the search box appears only once
// a set is big enough to be worth filtering — hidden below the threshold where it'd
// be pointless. Large lists scroll in a height-constrained container so hundreds of
// codes stay bounded. Very large lists additionally collapse: levelled classifications
// become drillable groups, prefix-shaped sets group by their visible code prefix, and
// genuinely flat sets show a bounded preview with an explicit expand affordance.
//
// Defensive empty-guard only — callers already omit the surrounding section when
// the set is empty; this just never crashes on `[]`.

// A code→label set member. Covers BOTH shapes: classification codes and variable
// value-set members.
interface Code {
  code: string;
  label: string;
  is_valid?: boolean | null;
  level?: number | null;
}

interface CodeGroup {
  key: string;
  code: string;
  label: string;
  count: number;
  children: Code[];
}

type CodeLayout =
  | { kind: "flat"; codes: Code[] }
  | { kind: "grouped"; groups: CodeGroup[]; singles: Code[] }
  | { kind: "collapsed-flat"; preview: Code[]; remaining: Code[] };

let {
  codes,
  filterLabel = "Filter codes",
  filterPlaceholder = "Filter codes…",
}: {
  codes: Code[];
  filterLabel?: string;
  filterPlaceholder?: string;
} = $props();

// Below this many codes the filter box is hidden — per the maintainer: pointless
// for a handful of items (a small classification or short value set). At or above
// it, the FilterInput appears.
const CODE_FILTER_THRESHOLD = 5;
const COLLAPSE_THRESHOLD = 50;
const FLAT_PREVIEW_LIMIT = 50;
const MIN_GROUP_COUNT = 2;
const showFilter = $derived(codes.length >= CODE_FILTER_THRESHOLD);

// In-memory type-to-filter over code + label (matchesFilter folds diacritics and
// treats an empty needle as match-all — the unfiltered full list). Reset when the
// `codes` prop changes (navigation / state switch) so a new set opens unfiltered.
let filter = $state("");
let flatExpanded = $state(false);
let openGroups = $state<string[]>([]);
$effect(() => {
  void codes;
  filter = "";
  flatExpanded = false;
  openGroups = [];
});
const shown = $derived(
  codes.filter((c) => matchesFilter(filter, c.code, c.label)),
);
const filtering = $derived(filter.trim().length > 0);
const layout = $derived(layoutFor(shown, filtering));

function codeLevel(code: Code): number | null {
  return typeof code.level === "number" && Number.isFinite(code.level)
    ? code.level
    : null;
}

function layoutFor(list: Code[], isFiltering: boolean): CodeLayout {
  if (isFiltering || list.length < COLLAPSE_THRESHOLD) {
    return { kind: "flat", codes: list };
  }
  return (
    groupedByLevel(list) ??
    groupedByExplicitPrefix(list) ??
    groupedByBucketPrefix(list) ?? {
      kind: "collapsed-flat",
      preview: list.slice(0, FLAT_PREVIEW_LIMIT),
      remaining: list.slice(FLAT_PREVIEW_LIMIT),
    }
  );
}

function usefulGroupedLayout(
  groups: CodeGroup[],
  singles: Code[],
  total: number,
): CodeLayout | null {
  const groupedRows = groups.reduce((sum, group) => sum + group.count, 0);
  if (
    groups.length < MIN_GROUP_COUNT ||
    groupedRows < Math.ceil(total * 0.6) ||
    singles.length > Math.floor(total * 0.4)
  ) {
    return null;
  }
  return { kind: "grouped", groups, singles };
}

function groupedByLevel(list: Code[]): CodeLayout | null {
  const levels = list
    .map(codeLevel)
    .filter((level): level is number => level != null);
  if (levels.length < list.length || new Set(levels).size < 2) {
    return null;
  }
  const topLevel = Math.min(...levels);
  const groups: CodeGroup[] = [];
  const singles: Code[] = [];
  let current: CodeGroup | null = null;

  for (const [index, code] of list.entries()) {
    const level = codeLevel(code);
    if (level === topLevel) {
      current = {
        key: `level:${code.code}:${index}`,
        code: code.code,
        label: code.label,
        count: 1,
        children: [],
      };
      groups.push(current);
      continue;
    }
    if (current == null || level == null || level <= topLevel) {
      singles.push(code);
      continue;
    }
    current.children.push(code);
    current.count += 1;
  }

  const populated = groups.filter((group) => group.children.length > 0);
  singles.push(
    ...groups
      .filter((group) => group.children.length === 0)
      .map((group) => ({ code: group.code, label: group.label })),
  );
  return usefulGroupedLayout(populated, singles, list.length);
}

function normalizeCodeKey(value: string): string {
  return value
    .trim()
    .replace(/[\s./_-]+/g, "")
    .toLowerCase();
}

function groupedByExplicitPrefix(list: Code[]): CodeLayout | null {
  const normalized = list.map((code) => normalizeCodeKey(code.code));
  const claimed = new Set<number>();
  const groups: CodeGroup[] = [];

  for (const [index, code] of list.entries()) {
    if (claimed.has(index)) {
      continue;
    }
    const prefix = normalized[index];
    if (prefix.length === 0) {
      continue;
    }
    const children: Code[] = [];
    const childIndexes: number[] = [];
    for (
      let childIndex = index + 1;
      childIndex < list.length;
      childIndex += 1
    ) {
      if (claimed.has(childIndex)) {
        continue;
      }
      const candidate = normalized[childIndex];
      if (candidate.length > prefix.length && candidate.startsWith(prefix)) {
        children.push(list[childIndex]);
        childIndexes.push(childIndex);
      }
    }
    if (children.length === 0) {
      continue;
    }
    groups.push({
      key: `prefix-parent:${code.code}:${index}`,
      code: code.code,
      label: code.label,
      count: children.length + 1,
      children,
    });
    claimed.add(index);
    for (const childIndex of childIndexes) {
      claimed.add(childIndex);
    }
  }

  const singles = list.filter((_, index) => !claimed.has(index));
  return usefulGroupedLayout(groups, singles, list.length);
}

function bucketPrefix(code: string): string | null {
  const trimmed = code.trim();
  const letterPrefix = /^[A-Za-z]+/.exec(trimmed)?.[0];
  if (letterPrefix) {
    return letterPrefix.slice(0, 1).toUpperCase();
  }
  const digitPrefix = /^\d+/.exec(trimmed)?.[0];
  return digitPrefix ? digitPrefix.slice(0, 1) : null;
}

function groupedByBucketPrefix(list: Code[]): CodeLayout | null {
  const buckets = new Map<string, Code[]>();
  const singles: Code[] = [];
  for (const code of list) {
    const prefix = bucketPrefix(code.code);
    if (prefix == null) {
      singles.push(code);
      continue;
    }
    const bucket = buckets.get(prefix) ?? [];
    bucket.push(code);
    buckets.set(prefix, bucket);
  }
  const groups: CodeGroup[] = [];
  for (const [prefix, bucket] of buckets) {
    if (bucket.length === 1) {
      singles.push(bucket[0]);
      continue;
    }
    groups.push({
      key: `prefix-bucket:${prefix}`,
      code: prefix,
      label: `Codes starting with ${prefix}`,
      count: bucket.length,
      children: bucket,
    });
  }
  return usefulGroupedLayout(groups, singles, list.length);
}
</script>

{#snippet codeRow(code: Code)}
  <li class="code-row">
    <code class="code-key">{code.code}</code>
    <span class="code-label">{code.label}</span>
  </li>
{/snippet}

{#if codes.length > 0}
  {#if showFilter}
    <FilterInput
      bind:value={filter}
      total={codes.length}
      shown={shown.length}
      placeholder={filterPlaceholder}
      label={filterLabel}
    />
  {/if}

  {#if shown.length > 0}
    <div class="code-scroll">
      {#if layout.kind === "grouped"}
        <Accordion.Root type="multiple" bind:value={openGroups} class="code-groups">
          {#each layout.groups as group (group.key)}
            <Accordion.Item value={group.key} class="code-group">
              <Accordion.Header level={4} class="code-group-heading">
                <Accordion.Trigger class="code-group-trigger">
                  <span class="group-caret" aria-hidden="true"></span>
                  <span class="group-main">
                    <code class="code-key">{group.code}</code>
                    <span class="code-label">{group.label}</span>
                  </span>
                  <span class="group-count">{group.count} codes</span>
                </Accordion.Trigger>
              </Accordion.Header>
              <Accordion.Content class="code-group-content">
                {#if openGroups.includes(group.key)}
                  <ul class="codes nested-codes">
                    {#each group.children as code, i (`${group.key}:${i}`)}
                      {@render codeRow(code)}
                    {/each}
                  </ul>
                {/if}
              </Accordion.Content>
            </Accordion.Item>
          {/each}
        </Accordion.Root>
        {#if layout.singles.length > 0}
          <ul class="codes loose-codes" aria-label="Ungrouped codes">
            {#each layout.singles as code, i (i)}
              {@render codeRow(code)}
            {/each}
          </ul>
        {/if}
      {:else if layout.kind === "collapsed-flat"}
        <Collapsible.Root bind:open={flatExpanded} class="flat-collapse">
          <p class="summary-note">
            Showing first {layout.preview.length} of {shown.length} codes.
          </p>
          <ul class="codes">
            {#each layout.preview as code, i (i)}
              {@render codeRow(code)}
            {/each}
          </ul>
          <Collapsible.Content class="flat-extra">
            {#if flatExpanded}
              <ul class="codes">
                {#each layout.remaining as code, i (i)}
                  {@render codeRow(code)}
                {/each}
              </ul>
            {/if}
          </Collapsible.Content>
          <Collapsible.Trigger class="flat-toggle">
            {flatExpanded ? "Show fewer codes" : `Show all ${shown.length} codes`}
          </Collapsible.Trigger>
        </Collapsible.Root>
      {:else}
        <ul class="codes">
          {#each layout.codes as code, i (i)}
            {@render codeRow(code)}
          {/each}
        </ul>
      {/if}
    </div>
  {:else}
    <p class="muted">No codes match “{filter}”.</p>
  {/if}
{/if}

<style>
  .muted {
    color: var(--text-muted);
  }
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
  .code-key {
    flex: 0 0 auto;
    min-width: 3.5rem;
    /* A value-set code — a machine identifier, so mono-faced (DESIGN.md). */
    font-family: var(--font-mono);
    color: var(--text-muted);
    font-size: 0.9em;
  }
  .code-label {
    flex: 1;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  :global(.code-groups) {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  :global(.code-group) {
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
    overflow: hidden;
  }
  :global(.code-group-heading) {
    margin: 0;
  }
  :global(.code-group-trigger) {
    width: 100%;
    border: 0;
    background: transparent;
    color: var(--text);
    font: inherit;
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
    align-items: baseline;
    gap: var(--space-2);
    padding: var(--space-2);
    text-align: left;
    cursor: pointer;
  }
  :global(.code-group-trigger:hover) {
    background: var(--surface-hover);
  }
  :global(.code-group-trigger:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .group-caret {
    width: 0.45rem;
    height: 0.45rem;
    border-right: 1.5px solid var(--text-muted);
    border-bottom: 1.5px solid var(--text-muted);
    transform: rotate(-45deg);
    transition: transform var(--motion-fast);
  }
  :global(.code-group-trigger[data-state="open"] .group-caret) {
    transform: rotate(45deg);
  }
  .group-main {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    min-width: 0;
  }
  .group-count,
  .summary-note {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .group-count {
    justify-self: end;
    white-space: nowrap;
  }
  :global(.code-group-content) {
    border-top: 1px solid var(--border);
    padding: var(--space-2);
    background: var(--surface-raised);
  }
  .nested-codes {
    padding-left: calc(0.45rem + var(--space-2));
  }
  .loose-codes {
    margin-top: var(--space-2);
  }
  :global(.flat-collapse) {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
  .summary-note {
    margin: 0;
  }
  :global(.flat-toggle) {
    align-self: flex-start;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text);
    font: inherit;
    font-size: var(--text-sm);
    padding: var(--space-1) var(--space-2);
    cursor: pointer;
  }
  :global(.flat-toggle:hover) {
    background: var(--surface-hover);
    border-color: var(--border-strong);
  }
  :global(.flat-toggle:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }

  @media (max-width: 48rem) {
    :global(.code-group-trigger) {
      grid-template-columns: auto minmax(0, 1fr);
      align-items: start;
    }
    .group-count {
      grid-column: 2;
      justify-self: start;
    }
  }
</style>
