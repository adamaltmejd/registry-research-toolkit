<script lang="ts">
import { getRegisterVariants } from "./api";
import { asyncResource } from "./async.svelte";
import { variantsHref } from "./catalog";
import { type Column, DataTable } from "./ui";
import { groupVariants } from "./variants";

// The register page's COMPACT variants summary (Y-79): one row per variant
// family — its name, the concrete slugs it is delivered under, and the years it
// spans — over a link to the register's variants page, which carries the
// register-version prose. The version wall used to render here: 105 near-identical
// "Version" blocks below ~300 variables, which nobody scrolled to and which buried
// the one thing worth knowing (LISA's population changed frame in 2010).
//
// The non-`_default` variants (#673/M4): `_default` is NOT a user-facing variant
// — it's a STORED variant for some registers (LSS/BU/SOL) and the synthesized
// default for others. A register whose only "variant" is `_default` (or that has
// none) has no real variant axis, so the whole section is suppressed (no useless
// "Variants" heading, no link to a page with nothing on it). A register with ≥1
// real variant renders the FULL list — `_default` is NOT filtered out of a mixed
// list (out of scope).
const { registerFqid }: { registerFqid: string } = $props();

interface VariantRow {
  name: string;
  /** Not a column of its own: the slugs read as a second line under the name,
   * so the table stays at TWO columns and `DataTable` can stack it at 375
   * without breaking a slug mid-token. */
  slugs: string[];
  years: string;
}

const columns: Column<VariantRow>[] = [
  { key: "name", label: "Variant" },
  { key: "years", label: "Years", mono: true, align: "end" },
];

const variants = asyncResource(() => getRegisterVariants(registerFqid));
const hasRealVariant = $derived(
  variants.data?.variants.some((v) => v.slug !== "_default") ?? false,
);
const rows = $derived(
  groupVariants(variants.data?.variants ?? []).map((group) => ({
    name: group.label,
    slugs: group.segments.map((segment) => segment.variant.slug),
    years: group.span,
  })),
);
</script>

<!-- #673/M4: render the section ONLY when there's a real (non-`_default`)
     variant, or an error. While loading, render nothing (the variants are a
     secondary affordance — no "Loading variants…" flash); a register with no
     real variant (empty list OR `_default`-only) renders nothing at all (no
     section, no heading, no "No variants." text). -->
{#if variants.error || hasRealVariant}
  <section class="variants" aria-labelledby="variants-heading">
    <h3 id="variants-heading">Variants</h3>
    {#if variants.error}
      <p class="error" role="alert">Failed to load variants: {variants.error}</p>
    {:else}
      <DataTable framed {columns} {rows}>
        {#snippet cell(row, column)}
          {#if column.key === "name"}
            <span class="name">{row.name}</span>
            {#each row.slugs as slug (slug)}
              <code class="slug">{slug}</code>
            {/each}
          {:else}
            {row.years}
          {/if}
        {/snippet}
      </DataTable>
      <p class="details">
        <a href={variantsHref(registerFqid)}>All variant details</a>
      </p>
    {/if}
  </section>
{/if}

<style>
  .variants {
    margin-top: var(--space-4);
  }
  .variants h3 {
    margin: 0 0 var(--space-3);
  }
  .name {
    display: block;
  }
  .slug {
    display: block;
    font-family: var(--font-mono);
    color: var(--text-muted);
  }
  .details {
    margin: var(--space-3) 0 0;
    font-size: var(--text-sm);
  }
  .details a {
    font-weight: 600;
  }
  .details a:hover {
    text-decoration: underline;
  }
  .details a:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
</style>
