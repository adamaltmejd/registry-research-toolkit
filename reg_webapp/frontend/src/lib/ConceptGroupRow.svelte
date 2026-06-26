<script lang="ts">
import type { ConceptGroup } from "./api";
import ConceptGroupNavigator from "./ConceptGroupNavigator.svelte";
import {
  axisValues,
  catalogHref,
  distinctMemberCount,
  leafSlug,
  memberAt,
  memberKey,
  membersHaveUniqueCoords,
} from "./catalog";
import { Tag } from "./ui";

// One folded concept-group row (#303): a <details> that expands to the facet
// picker — a value matrix for two axes (month × rank), chips for FACETED members
// (single-axis token groups AND axis-less classification umbrellas — each member
// carries a curated short label), and a plain member list for edge groups (split
// siblings, NO facets — names usually shared, so the slug is the signal).
//
// Two member-action modes (#322): browse (default) renders members as
// catalogHref links; pick mode (`onpick` set — the CatalogPicker's variable
// list) renders them as buttons that emit the member FQID instead of
// navigating. `disabled` greys the buttons while the picker resolves.
//
// `href` (#673/#756): in the REGISTER-arm browse AND the classification-umbrella
// arm, the row LINKS to the group's own subject page instead of expanding inline
// (register groups → `/catalog/group/<p>/<r>/<key>`, #673; classification
// umbrellas → `/catalog/group/class/<key>`, #756). When `href` is set AND `onpick`
// is NOT, the summary line renders as a link — no <details>, no matrix. Otherwise
// (no href, or pick mode) the inline <details> stays: pick mode keeps its
// member-pick buttons, and any caller that passes no href still folds inline.
let {
  group,
  noun = "variables",
  href,
  onpick,
  disabled = false,
}: {
  group: ConceptGroup;
  noun?: string;
  href?: string;
  onpick?: (fqid: string) => void;
  disabled?: boolean;
} = $props();

// Link mode only when a subject href is supplied AND we're not in pick mode
// (pick mode must keep the inline member-pick buttons). The link reuses the same
// summary content (label + count + group-key badge) the <summary> shows.
const asLink = $derived(href !== undefined && onpick === undefined);

const axes = $derived(group.axes);
// A 2D matrix only addresses TWO axes; a group with >2 axes (the iot
// disposable-income family) renders through the shared ConceptGroupNavigator
// instead — a matrix would collapse members that share the first two coords and
// DROP the rest (e.g. the kapitalvinst incl/excl pair on one variable), which
// also escape `ungridded` (they cover every declared axis). The navigator drops
// NO member, in BOTH browse (links) and pick (`onpick` buttons) modes. The
// matrix/chips/list path stays for ≤2 axes (no regression).
//
// #819 FIX C: >2 axes is not the only matrix-lossy shape. A 2-axis group can ALSO
// carry two members on the SAME (row, col) coordinate — representation members
// distinguished by `delivery_column` (e.g. `din8` = DIN83/DIN84/DIN86). The matrix
// renders only the FIRST per cell (`memberAt`) and DROPS the rest, and they escape
// `ungridded` (they cover every axis). So route through the navigator when a 2-axis
// group has NON-UNIQUE coordinates too. The coordinate test is gated to
// `axes.length === 2`: only the 2D matrix loses colliding members; a ≤1-axis group
// renders a chip/list (every member shown) and an axis-less umbrella collides
// trivially (all members map to the empty coord) but must keep its chips, not the
// navigator.
const useNavigator = $derived(
  axes.length > 2 ||
    (axes.length === 2 && !membersHaveUniqueCoords(group, axes)),
);
// Matrix orientation: first axis → rows, second axis → columns.
const matrixRows = $derived(
  axes.length > 0 ? axisValues(group, axes[0].name) : [],
);
const matrixCols = $derived(
  axes.length > 1 ? axisValues(group, axes[1].name) : [],
);
// `axes` is the UNION across members — a curated family can mix absorbed
// token-group members (month + rank) with single-variable members (rank
// only). A member missing a facet on any axis never matches a matrix cell, so
// it would silently vanish from the grid; render those below as a plain list
// (keeps the rendered set == the "N {noun}" summary count).
const ungridded = $derived(
  axes.length >= 2 && !useNavigator
    ? group.members.filter(
        (m) =>
          !axes.every((axis) => m.facets.some((f) => f.axis === axis.name)),
      )
    : [],
);
// Below the matrix threshold, render chips when the MEMBERS carry facets — true
// for single-axis token groups AND for axis-less classification umbrellas
// (`axes: []`, members each carry one curated `{axis: null, label}` facet, #516).
// Driving off member-facet presence (not `axes[0]`) keeps the curated umbrella
// labels instead of dropping to bare slugs. A member's chip label is its first
// facet's label: in a single-axis group that IS the axis facet; in an umbrella it
// is the curated short label. Truly facet-less members (edge groups — split
// siblings) fall through to the plain member list below.
const faceted = $derived(group.members.some((m) => m.facets.length > 0));
</script>

{#snippet memberItem(member: ConceptGroup["members"][number])}
  {#if onpick}
    <button
      type="button"
      class="member-pick"
      {disabled}
      onclick={() => onpick?.(member.fqid)}
    >
      <code class="member-slug">{leafSlug(member.fqid)}</code>
      {#if member.name && member.name !== group.label}
        <span class="member-name">{member.name}</span>
      {/if}
    </button>
  {:else}
    <a href={catalogHref(member.fqid)}>
      <code class="member-slug">{leafSlug(member.fqid)}</code>
      {#if member.name && member.name !== group.label}
        <span class="member-name">{member.name}</span>
      {/if}
    </a>
  {/if}
{/snippet}

{#snippet summaryLine()}
  <span class="label">{group.label}</span>
  <!-- #819: count DISTINCT member FQIDs (the variable identity), not raw member
       rows — a representation group carries several members on one variable (one
       `fqid`, distinct delivery columns), so `members.length` overstates the
       "N variables" readout. Distinct-by-fqid is a no-op for axis groups (each
       facet value is its own variable) and umbrellas (distinct classifications).
       The count is a neutral chrome pill (Tag tone="neutral"): a quantity, not a
       TYPE, so it must not borrow the categorical group hue (the group-key badge
       below owns that). -->
  <span class="count"><Tag tone="neutral">{distinctMemberCount(group.members)} {noun}</Tag></span>
  <!-- The group key is a presentation anchor (slug stem / min-member slug /
       curated key per ConceptGroupSummary), NOT an addressable variable
       (#498). Deliberately NOT a <code>/.child-fqid: monospace + that class
       read as a pickable leaf FQID and confused a maintainer. The categorical
       `group` Tag tone (moss — concept groups) is exactly this anchor's
       identity: it reads as a grouping label, non-monospace, distinct from the
       monospace leaf <code>s. Do NOT "restore" the code look. -->
  <span class="group-key"><Tag tone="group">{group.key}</Tag></span>
{/snippet}

{#if asLink}
  <!-- #673: the register-arm browse links to the group SUBJECT page instead of
       expanding inline. Same summary content as the <details> summary, just a
       link — one interactive element per row (no nested controls). -->
  <a class="group-link" {href}>{@render summaryLine()}</a>
{:else}
<details class="group">
  <summary>
    {@render summaryLine()}
  </summary>
  {#if useNavigator}
    <!-- #819: the >2-axis navigator (per-axis filters + a no-member-dropped list)
         replaces the 2D matrix, which would collapse + drop members differing only
         on a 3rd axis. The per-member action is `memberItem` (a browse link or a
         pick button), so every visible member stays reachable / pickable. Keyed on
         the group key so the navigator's filter state recreates per group row. -->
    {#key group.key}
      <ConceptGroupNavigator members={group.members} {axes}>
        {#snippet member(m)}
          {@render memberItem(m)}
        {/snippet}
      </ConceptGroupNavigator>
    {/key}
  {:else if axes.length >= 2}
    <table class="facet-matrix">
      <thead>
        <tr>
          <th></th>
          {#each matrixCols as col (col.value)}
            <th scope="col">{col.label}</th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each matrixRows as row (row.value)}
          <tr>
            <th scope="row">{row.label}</th>
            {#each matrixCols as col (col.value)}
              {@const member = memberAt(group, [
                { axis: axes[0].name, value: row.value },
                { axis: axes[1].name, value: col.value },
              ])}
              <td>
                {#if member && onpick}
                  <button
                    type="button"
                    class="member-pick"
                    {disabled}
                    title={member.name ?? member.fqid}
                    onclick={() => onpick?.(member.fqid)}
                  >
                    <code>{leafSlug(member.fqid)}</code>
                  </button>
                {:else if member}
                  <a href={catalogHref(member.fqid)} title={member.name ?? member.fqid}>
                    <code>{leafSlug(member.fqid)}</code>
                  </a>
                {:else}
                  <!-- partial family (e.g. a month SCB never delivered) -->
                  <span class="muted" aria-hidden="true">–</span>
                {/if}
              </td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
    {#if ungridded.length > 0}
      <ul class="members">
        {#each ungridded as member (memberKey(member))}
          <li>{@render memberItem(member)}</li>
        {/each}
      </ul>
    {/if}
  {:else if faceted}
    <ul class="facet-chips">
      {#each group.members as member (memberKey(member))}
        {@const facet = member.facets[0]}
        <li>
          {#if onpick}
            <button
              type="button"
              class="chip member-pick"
              {disabled}
              title={member.name ?? member.fqid}
              onclick={() => onpick?.(member.fqid)}
            >
              {facet?.label ?? leafSlug(member.fqid)}
            </button>
          {:else}
            <a
              class="chip"
              href={catalogHref(member.fqid)}
              title={member.name ?? member.fqid}
            >
              {facet?.label ?? leafSlug(member.fqid)}
            </a>
          {/if}
        </li>
      {/each}
    </ul>
  {:else}
    <ul class="members">
      {#each group.members as member (memberKey(member))}
        <li>{@render memberItem(member)}</li>
      {/each}
    </ul>
  {/if}
</details>
{/if}

<style>
  summary,
  .group-link {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    cursor: pointer;
  }
  /* Keyboard focus on the disclosure summary / the link variant: the shared
     --focus-ring (matching DataTable's selectable rows, the search owner/leaf
     rows, #808), replacing the hand-rolled / UA default outline. */
  summary:focus-visible,
  .group-link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  /* #673: the register-arm link variant carries the row's own accent color (it
     navigates), but the summary content styling (label/count/badge) is shared. */
  .group-link {
    color: var(--accent);
  }
  summary .label,
  .group-link .label {
    font-weight: 600;
  }
  /* The count/group-key Tags don't shrink the summary row on wrap. */
  summary .count,
  .group-link .count,
  summary .group-key,
  .group-link .group-key {
    white-space: nowrap;
  }
  .facet-matrix {
    margin: var(--space-2) 0 var(--space-2) var(--space-4);
    border-collapse: collapse;
    font-size: var(--text-sm);
  }
  .facet-matrix th,
  .facet-matrix td {
    padding: var(--space-1) var(--space-2);
    text-align: left;
  }
  .facet-matrix thead th {
    color: var(--text-muted);
    font-weight: 600;
  }
  .facet-matrix tbody th {
    color: var(--text-muted);
    font-weight: 400;
  }
  .facet-chips {
    list-style: none;
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-1);
    padding: 0;
    margin: var(--space-2) 0 var(--space-2) var(--space-4);
  }
  /* The facet/member chips are interactive (link/button) pills, not the static
     `Tag` primitive — but they borrow Tag's pill geometry (em-based padding +
     --radius-sm) so they sit visually with the count/group Tags above. */
  .chip {
    display: inline-block;
    padding: 0.1em 0.5em;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    font-size: var(--text-sm);
    text-decoration: none;
  }
  .members {
    list-style: none;
    padding: 0;
    margin: var(--space-2) 0 var(--space-2) var(--space-4);
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  .members a {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
  }
  /* Pick-mode members (#322): link-look buttons so the picker's group rows
     read like the browse ones, just emitting instead of navigating. */
  .member-pick {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: var(--accent);
    cursor: pointer;
  }
  .member-pick:disabled {
    color: var(--text-muted);
    cursor: default;
  }
  .members .member-pick {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
  }
  .member-pick.chip {
    border: 1px solid var(--border);
    padding: 0.1em 0.5em;
  }
  .member-name {
    color: var(--text-muted);
    font-size: 0.9em;
  }
  .muted {
    color: var(--text-muted);
  }
</style>
