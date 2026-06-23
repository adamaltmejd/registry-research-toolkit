<script lang="ts">
import { getStats, type StewardInfo } from "./api";
import { asyncResource } from "./async.svelte";

// The landing/home page (#675), served at `/` — split from the data browser
// (`/catalog`). It welcomes the visitor with the deployment's `long_name`, a
// one-line catalog stats summary, and three entry-point cards.
//
// `steward` is threaded from App (which already holds `/api/context`) so Home
// makes NO duplicate context fetch; it fetches only `/api/stats` itself. `null`
// until the context resolves (App passes `context?.steward ?? null`) — the page
// renders without the steward name in that brief window, corrected on load.
let { steward = null }: { steward?: StewardInfo | null } = $props();

// The catalog counts (#675). Fetched here (not blocking the whole page) so the
// cards render immediately; the stats line fills in / is omitted on error.
const stats = asyncResource(() => getStats());

// The entry points into the app. `/catalog` is the data browser, `/search` the
// results page (a no-`?q=` `/search` renders a "start typing" prompt), `/project`
// the authoring surface. Plain internal `<a>`s — the shell's `use:link` action
// intercepts the clicks for pushState navigation.
const entries = [
  {
    href: "/catalog",
    title: "Browse the data",
    blurb: "Explore providers, registers, and variables in the catalog.",
  },
  {
    href: "/search",
    title: "Search",
    blurb: "Find registers, variables, codes, and classifications by keyword.",
  },
  {
    href: "/project",
    title: "Start a project",
    blurb: "Assemble and validate a data request for your research.",
  },
];
</script>

<section class="home">
  <header class="hero">
    <h1>
      {#if steward}
        {steward.long_name}
      {:else}
        Register Research Catalog
      {/if}
    </h1>
    <p class="tagline">Browse and search Swedish register metadata.</p>
    {#if stats.data}
      <p class="stats muted">
        {stats.data.providers.toLocaleString()} providers ·
        {stats.data.registers.toLocaleString()} registers ·
        {stats.data.variables.toLocaleString()} variables
      </p>
    {:else if stats.loading}
      <!-- A subtle placeholder so the hero block doesn't reflow when the
           stats line arrives; an error simply omits the line. -->
      <p class="stats muted placeholder" aria-hidden="true">&nbsp;</p>
    {/if}
  </header>

  <ul class="entries">
    {#each entries as entry (entry.href)}
      <li>
        <a class="card" href={entry.href}>
          <span class="card-title">{entry.title}</span>
          <span class="card-blurb">{entry.blurb}</span>
        </a>
      </li>
    {/each}
  </ul>
</section>

<style>
  .home {
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
  }
  .hero {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
  .hero h1 {
    margin: 0;
    font-size: 1.6rem;
  }
  .tagline {
    margin: 0;
    font-size: 1.05rem;
  }
  .stats {
    margin: 0;
    font-size: var(--text-sm);
    font-family: ui-monospace, monospace;
  }
  .placeholder {
    /* Reserve the line's height without showing text while loading. */
    visibility: hidden;
  }
  .entries {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(14rem, 1fr));
    gap: var(--space-3);
  }
  .card {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    height: 100%;
    padding: var(--space-4);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--surface);
    color: inherit;
  }
  .card:hover {
    border-color: var(--accent);
    text-decoration: none;
  }
  .card-title {
    font-weight: 700;
    color: var(--accent);
  }
  .card-blurb {
    font-size: var(--text-sm);
    color: var(--muted);
  }
</style>
