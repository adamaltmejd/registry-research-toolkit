<script lang="ts" module>
import type { GroupAxisModel, GroupFacetModel } from "./api";

/** The minimal member shape the navigator reads: the facet-grid fields plus the
 * components of a stable composite key (`fqid` + `delivery_column`). The host's
 * concrete member type (browse `ConceptGroupMember` or subject
 * `ConceptGroupNodeMember`) widens this, and the `member` snippet receives it
 * back at full type via the component's `M` generic. Declared in the module
 * script so the instance `generics="M extends NavigatorMember"` can reference
 * it. */
export interface NavigatorMember {
  fqid: string;
  name?: string | null;
  delivery_column?: string | null;
  facets: GroupFacetModel[];
}
</script>

<script lang="ts" generics="M extends NavigatorMember">
import type { Snippet } from "svelte";
import { axisValues, memberFacet, memberKey } from "./catalog";
import Tag from "./ui/Tag.svelte";

// ── The N-axis facet navigator (#819) ────────────────────────────────────────
// A 2D matrix only addresses TWO axes; a group with >2 axes (the iot
// disposable-income family: enhet × hushållsbegrepp × kapitalvinst) can't be a
// grid without collapsing members that share the first two coords and DROPPING
// the rest. So >2-axis groups render through THIS navigator instead: every
// member is a row carrying one neutral facet tag per axis, and per-axis filter
// controls narrow the visible set. The navigator drops NO member — the matrix's
// data-loss bug (members differing only on a 3rd axis vanished) is the reason it
// exists. The navigator is a CLIENT-SIDE LENS — a filter only narrows what's
// shown; it NEVER mutates the project/selection.
//
// SHARED between the group SUBJECT page (ConceptGroupView, read-only links +
// per-member coverage/greying) and the register-browse / picker row
// (ConceptGroupRow, browse links OR pick buttons). Both hosts feed the same
// `members` + `axes`; the per-member ACTION (a link, a pick button, plus any
// coverage decoration) differs, so the host supplies it as the `member` snippet
// rendered after the shared facet tags. The navigator owns the filter state +
// fieldsets + the full member list — the invariant (no member dropped, every
// visible member reachable, axis identity carried by TEXT) lives in one place.

let {
  members,
  axes,
  member: memberSnippet,
}: {
  members: readonly M[];
  /** The group's declared axes (#819): match on `axis.name`, display `axis.label`. */
  axes: readonly GroupAxisModel[];
  /** The per-member action element (link / pick button + any coverage/greying),
   * rendered after the shared facet tags inside each list row. */
  member: Snippet<[M]>;
} = $props();

// One filter group per axis: the distinct (value, label) pairs members carry on
// it (value-sorted, via `axisValues`). The navigator renders one filter fieldset
// per entry; axis identity is the fieldset's <legend> TEXT (never hue) — the
// curator-authored `axis.label`, keyed/filtered on the stable `axis.name`.
const axisFilters = $derived(
  axes.map((axis) => ({
    axis: axis.name,
    label: axis.label,
    values: axisValues({ members }, axis.name),
  })),
);

// The active filter selection: axis → set of selected facet VALUES. An axis with
// an empty/absent set imposes no constraint (shows all its values) — so the
// initial all-empty state shows every member. The parent keys this component on
// the group (`{#key}`), so navigating to a different group remounts it and the
// filter state resets — no manual member-signature reset needed.
let selected = $state<Record<string, Set<string>>>({});

/** Toggle a facet value in an axis's filter set (multi-select within an axis). */
function toggleFilter(axis: string, value: string): void {
  const current = selected[axis] ?? new Set<string>();
  const next = new Set(current);
  if (next.has(value)) {
    next.delete(value);
  } else {
    next.add(value);
  }
  // Reassign the whole record so the `$state` proxy tracks the change.
  selected = { ...selected, [axis]: next };
}

/** Clear every axis filter (back to "show all members"). */
function clearFilters(): void {
  selected = {};
}

const anyFilterActive = $derived(
  Object.values(selected).some((s) => s.size > 0),
);

/** Whether a facet value is currently selected in its axis's filter. */
function isSelected(axis: string, value: string): boolean {
  return selected[axis]?.has(value) ?? false;
}

// The visible member set under the active filters: a member passes when, for
// EVERY axis with a non-empty selection, it carries a facet on that axis whose
// value is selected (AND across axes, OR within an axis). An axis with no
// selection imposes no constraint. Filter-only: this narrows the rendered list,
// never the project — and it NEVER drops a member outside the active filters
// (the matrix-collapse data-loss bug the navigator fixes).
const visibleMembers = $derived(
  members.filter((m) =>
    axes.every((axis) => {
      const sel = selected[axis.name];
      if (!sel || sel.size === 0) {
        return true;
      }
      return m.facets.some((f) => f.axis === axis.name && sel.has(f.value));
    }),
  ),
);
</script>

<!-- Per-axis filter controls + the filtered member list. Filter-only — narrows
     the list, never the project. -->
<div class="facet-filters" role="group" aria-label="Filter members by facet">
  {#each axisFilters as { axis, label, values } (axis)}
    <fieldset class="axis-filter">
      <legend>{label}</legend>
      <div class="filter-options">
        {#each values as v (v.value)}
          <label class="filter-pill" class:on={isSelected(axis, v.value)}>
            <input
              type="checkbox"
              checked={isSelected(axis, v.value)}
              onchange={() => toggleFilter(axis, v.value)}
            />
            <span>{v.label}</span>
          </label>
        {/each}
      </div>
    </fieldset>
  {/each}
  {#if anyFilterActive}
    <button type="button" class="clear-filters" onclick={clearFilters}>
      Clear filters
    </button>
  {/if}
</div>

<p class="member-count muted" aria-live="polite">
  Showing {visibleMembers.length} of {members.length} members
</p>

{#if visibleMembers.length === 0}
  <p class="muted no-members">No members match the active filters.</p>
{:else}
  <ul class="members navigator">
    {#each visibleMembers as m (memberKey(m))}
      <li>
        <!-- A member's per-axis facet tags: one NEUTRAL Tag per axis carrying the
             member's value label, with the axis identity carried as visible TEXT
             (the micro-label before the value) — NOT by hue. So a multi-axis
             member reads as "enhet: Individ · kapitalvinst: Inkl." at a glance,
             and the axis is legible to an assistive-tech user (not color-only).
             An axis the member lacks a facet on is omitted (a partial family). -->
        <span class="facet-tags">
          {#each axes as axis (axis.name)}
            {@const facet = memberFacet(m, axis.name)}
            {#if facet}
              <Tag tone="neutral">
                <!-- #819: the authored axis LABEL (e.g. "Hushållsbegrepp"), not the
                     uppercased match key — keyed on the stable `axis.name`. -->
                <span class="axis-name">{axis.label}:</span>
                {facet.label}
              </Tag>
            {/if}
          {/each}
          <!-- #819 FIX B: two members sharing one fqid AND identical facet coords
               differ ONLY by `delivery_column` (e.g. `din8` = DIN83/DIN84/DIN86) —
               their facet tags render IDENTICALLY, so without the column a picker
               user can't tell which they're choosing. Show it as a subtle technical
               discriminator when present (whole-variable members carry none → it's
               omitted). Rendered HERE (the shared navigator) so it shows in BOTH
               browse and pick modes. -->
          {#if m.delivery_column}
            <code class="delivery-column">{m.delivery_column}</code>
          {/if}
        </span>
        {@render memberSnippet(m)}
      </li>
    {/each}
  </ul>
{/if}

<style>
  /* ── The N-axis facet navigator (#819) ──────────────────────────────────────
     Per-axis filter fieldsets + a filtered member list. NEUTRAL throughout — no
     `--cat-*` type palette (that sub-system tags result/node TYPE; reusing it
     here would read a facet value as a CODE/REG type chip; DESIGN.md → Color).
     Axis identity is carried by TEXT (the legend + each tag's micro-label), not
     by hue. */
  .facet-filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-3);
    align-items: flex-start;
    margin: var(--space-2) 0;
  }
  .axis-filter {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-2) var(--space-3) var(--space-3);
    margin: 0;
    min-inline-size: 0;
  }
  .axis-filter legend {
    color: var(--text-muted);
    font-weight: 600;
    font-size: var(--text-sm);
    padding: 0 var(--space-1);
    /* #819: render the curator-authored axis label as-authored (no capitalize). */
  }
  .filter-options {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-1);
  }
  /* A filter pill: a checkbox styled as a selectable neutral chip. The native
     input is visually hidden but kept in the DOM (a11y / keyboard / labelled),
     and the `.on` class paints the selected state with the brand accent (the
     selection cue, distinct from the neutral resting chip). */
  .filter-pill {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    padding: 0.1rem 0.55rem;
    border: 1px solid var(--border);
    border-radius: 1rem;
    font-size: var(--text-sm);
    cursor: pointer;
    user-select: none;
    background: var(--surface);
    color: var(--text);
  }
  .filter-pill input {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
    border: 0;
  }
  .filter-pill.on {
    background: var(--accent-bg);
    border-color: var(--accent);
    color: var(--accent-ink);
    font-weight: 600;
  }
  /* Keyboard focus ring on the (hidden) input projects onto its pill label. */
  .filter-pill:focus-within {
    box-shadow: var(--focus-ring);
  }
  .clear-filters {
    align-self: center;
    background: none;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    padding: 0.2rem 0.6rem;
    font: inherit;
    font-size: var(--text-sm);
    color: var(--text-muted);
    cursor: pointer;
  }
  .clear-filters:hover {
    color: var(--text);
    border-color: var(--border-strong);
  }
  .member-count {
    font-size: var(--text-sm);
    margin: var(--space-2) 0 var(--space-1);
  }
  .muted {
    color: var(--text-muted);
  }
  .no-members {
    margin: var(--space-2) 0;
  }
  .members {
    list-style: none;
    padding: 0;
    margin: 0.5rem 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  /* A navigator member row: its per-axis neutral tags, then the host's member
     action (a link, or a pick button + coverage/greying). */
  .members.navigator > li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.25rem 0.6rem;
  }
  .facet-tags {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-1);
  }
  /* #819 FIX B: the delivery-column discriminator on duplicate-coordinate members
     — a muted monospace code so it reads as a technical identifier, secondary to
     the facet tags (it only matters when two members would otherwise look alike). */
  .delivery-column {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  /* The per-axis micro-label inside a neutral tag: the curator-authored axis
     label (#819) — rendered AS AUTHORED (no uppercase transform, so "Hushållsbegrepp"
     keeps its casing), so the axis identity is read as TEXT (not color). Dimmed so
     the value label leads. */
  .axis-name {
    font-size: 0.8em;
    letter-spacing: 0.02em;
    opacity: 0.7;
  }
</style>
