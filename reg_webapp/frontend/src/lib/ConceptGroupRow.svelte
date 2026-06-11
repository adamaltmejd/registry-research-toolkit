<script lang="ts">
import type { ConceptGroup } from "./api";
import { axisValues, catalogHref, memberAt } from "./catalog";

// One folded concept-group row (#303): a <details> that expands to the facet
// picker — a value matrix for two axes (month × rank), chips for one axis
// (months / vintages), and a plain member list for edge groups (split
// siblings, no facets — names usually shared, so the slug is the signal).
let {
  group,
  noun = "variables",
}: {
  group: ConceptGroup;
  noun?: string;
} = $props();

const axes = $derived(group.axes);
// Matrix orientation: first axis → rows, second axis → columns.
const matrixRows = $derived(axes.length > 0 ? axisValues(group, axes[0]) : []);
const matrixCols = $derived(axes.length > 1 ? axisValues(group, axes[1]) : []);
// `axes` is the UNION across members — a curated family can mix absorbed
// token-group members (month + rank) with single-variable members (rank
// only). A member missing a facet on any axis never matches a matrix cell, so
// it would silently vanish from the grid; render those below as a plain list
// (keeps the rendered set == the "N {noun}" summary count).
const ungridded = $derived(
  axes.length >= 2
    ? group.members.filter(
        (m) => !axes.every((axis) => m.facets.some((f) => f.axis === axis)),
      )
    : [],
);

function leafSlug(fqid: string): string {
  return fqid.split("/").at(-1) ?? fqid;
}
</script>

<details class="group">
  <summary>
    <span class="label">{group.label}</span>
    <span class="count">{group.members.length} {noun}</span>
    <code class="child-fqid">{group.key}</code>
  </summary>
  {#if axes.length >= 2}
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
                { axis: axes[0], value: row.value },
                { axis: axes[1], value: col.value },
              ])}
              <td>
                {#if member}
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
        {#each ungridded as member (member.fqid)}
          <li>
            <a href={catalogHref(member.fqid)}>
              <code class="member-slug">{leafSlug(member.fqid)}</code>
              {#if member.name && member.name !== group.label}
                <span class="member-name">{member.name}</span>
              {/if}
            </a>
          </li>
        {/each}
      </ul>
    {/if}
  {:else if axes.length === 1}
    <ul class="facet-chips">
      {#each group.members as member (member.fqid)}
        {@const facet = member.facets.find((f) => f.axis === axes[0])}
        <li>
          <a
            class="chip"
            href={catalogHref(member.fqid)}
            title={member.name ?? member.fqid}
          >
            {facet?.label ?? leafSlug(member.fqid)}
          </a>
        </li>
      {/each}
    </ul>
  {:else}
    <ul class="members">
      {#each group.members as member (member.fqid)}
        <li>
          <a href={catalogHref(member.fqid)}>
            <code class="member-slug">{leafSlug(member.fqid)}</code>
            {#if member.name && member.name !== group.label}
              <span class="member-name">{member.name}</span>
            {/if}
          </a>
        </li>
      {/each}
    </ul>
  {/if}
</details>

<style>
  summary {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    cursor: pointer;
  }
  summary .label {
    font-weight: 600;
  }
  summary .count {
    color: var(--muted);
    font-size: 0.85em;
    white-space: nowrap;
  }
  .child-fqid {
    color: var(--muted);
    font-size: 0.85em;
  }
  .facet-matrix {
    margin: 0.5rem 0 0.5rem 1rem;
    border-collapse: collapse;
    font-size: 0.85em;
  }
  .facet-matrix th,
  .facet-matrix td {
    padding: 0.15rem 0.5rem;
    text-align: left;
  }
  .facet-matrix thead th {
    color: var(--muted);
    font-weight: 600;
  }
  .facet-matrix tbody th {
    color: var(--muted);
    font-weight: 400;
  }
  .facet-chips {
    list-style: none;
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
    padding: 0;
    margin: 0.5rem 0 0.5rem 1rem;
  }
  .chip {
    display: inline-block;
    padding: 0.1rem 0.55rem;
    border: 1px solid var(--muted);
    border-radius: 1rem;
    font-size: 0.85em;
    text-decoration: none;
  }
  .members {
    list-style: none;
    padding: 0;
    margin: 0.5rem 0 0.5rem 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }
  .members a {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .member-name {
    color: var(--muted);
    font-size: 0.9em;
  }
  .muted {
    color: var(--muted);
  }
</style>
