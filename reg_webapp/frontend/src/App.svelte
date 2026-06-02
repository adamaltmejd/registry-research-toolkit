<script lang="ts">
import { onMount } from "svelte";
import { ApiError, type Context, getContext } from "./lib/api";
import CatalogNodeView from "./lib/CatalogNodeView.svelte";
import CatalogRoot from "./lib/CatalogRoot.svelte";
import { link, router } from "./lib/router.svelte";

// The shell: a header with steward branding (GET /api/context) + a catalog-drift
// banner when present, and a routed main area. Internal <a> clicks are
// intercepted at the shell so navigation is pushState (no full reload).
let context = $state<Context | null>(null);
let contextError = $state<string | null>(null);

// The deployment context is app-global and immutable for the session — fetch
// once at mount (not an $effect that could re-run).
onMount(() => {
  getContext()
    .then((resp) => {
      context = resp;
    })
    .catch((e) => {
      contextError = e instanceof ApiError ? e.message : String(e);
    });
});

const route = $derived(router.route);
const driftWarnings = $derived(context?.catalog_drift_warnings ?? []);
</script>

<!-- Click interception is delegated from the root container via the `link`
     action: any internal <a> inside bubbles here and is pushState-navigated. -->
<div class="app" use:link>
  <header>
    <div class="brand">
      <a href="/" class="home">
        {#if context}
          {context.steward.long_name}
        {:else}
          Register Research Catalog
        {/if}
      </a>
      {#if context}
        <span class="steward-id">{context.steward.id}</span>
      {/if}
    </div>
    {#if context}
      <div class="build muted" title="reg_meta DB build / installed versions">
        schema {context.reg_meta.schema_version} · webapp {context.webapp.version}
      </div>
    {/if}
  </header>

  {#if contextError}
    <p class="banner error" role="alert">
      Failed to load deployment context: {contextError}
    </p>
  {/if}

  {#if driftWarnings.length > 0}
    <div class="banner drift" role="status">
      <strong>Catalog drift:</strong>
      {driftWarnings.length} steward
      {driftWarnings.length === 1 ? "binding" : "bindings"} no longer resolve
      against this reg_meta build.
      <ul>
        {#each driftWarnings as warning (warning.path)}
          <li><code>{warning.code}</code> — {warning.message}</li>
        {/each}
      </ul>
    </div>
  {/if}

  <main>
    {#if route.name === "root"}
      <CatalogRoot />
    {:else if route.name === "catalog-node"}
      {#key route.fqidPath}
        <CatalogNodeView fqidPath={route.fqidPath} />
      {/key}
    {:else}
      <article>
        <h2>Not found</h2>
        <p>No page at <code>{route.path}</code>.</p>
        <p><a href="/catalog">Back to the catalog</a></p>
      </article>
    {/if}
  </main>
</div>

<style>
  :global(:root) {
    --border: #d4d4d4;
    --muted: #666;
    --accent: #2563eb;
    --accent-bg: #eff4ff;
    --surface: #fff;
  }
  :global(body) {
    margin: 0;
    font-family: system-ui, sans-serif;
    color: #1a1a1a;
    line-height: 1.5;
  }
  :global(a) {
    color: var(--accent);
    text-decoration: none;
  }
  :global(a:hover) {
    text-decoration: underline;
  }
  :global(.muted) {
    color: var(--muted);
  }
  :global(.error) {
    color: #b00020;
  }
  .app {
    max-width: 56rem;
    margin: 0 auto;
    padding: 0 1rem 3rem;
  }
  header {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.5rem 1rem;
    padding: 1rem 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.5rem;
  }
  .brand {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .home {
    font-size: 1.25rem;
    font-weight: 700;
    color: inherit;
  }
  .home:hover {
    text-decoration: none;
    color: var(--accent);
  }
  .steward-id {
    font-family: ui-monospace, monospace;
    font-size: 0.8rem;
    color: var(--muted);
  }
  .build {
    font-size: 0.8rem;
  }
  .banner {
    padding: 0.75rem 1rem;
    border-radius: 4px;
    margin-bottom: 1rem;
  }
  .banner.drift {
    background: #fff7ed;
    border: 1px solid #fdba74;
  }
  .banner.drift ul {
    margin: 0.5rem 0 0;
  }
  .banner.error {
    background: #fef2f2;
    border: 1px solid #fca5a5;
  }
</style>
