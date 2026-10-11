<script lang="ts">
// The global catalog's scope, and the way to the SWECOV catalog (#1461). A search
// here for a SWECOV-only source (Inera/1177, regional care, quality registers)
// finds nothing, and nothing else on the page says those holdings live in another
// catalog. Rendered on Home and /project only when this deployment serves the
// global catalog; a steward deployment's own name already says what it holds.
//
// simplify: the SWECOV address is a constant, because `/api/context` carries no
// related-catalog link (the `hostname` in reg_webapp/stewards/swecov/steward.json
// never reaches the SPA). Serve it from the context if a second steward catalog
// needs linking, or if the SWECOV deployment moves.
const SWECOV_CATALOG_URL = "https://data.swecov.se";

const { steward }: { steward: string | null | undefined } = $props();
</script>

{#if steward === "global"}
  <p class="scope">
    This catalog covers Swedish national registers. Data held by the SWECOV
    project, including regional, quality-register and commercial sources, is in
    the <a href={SWECOV_CATALOG_URL}>SWECOV catalog</a>.
  </p>
{/if}

<style>
  /* Default paragraph margins, like the page's other muted notes; a gap-laid-out
     parent (Home's hero) zeroes them. */
  .scope {
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
</style>
