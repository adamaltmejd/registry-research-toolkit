<script lang="ts">
import { onMount } from "svelte";
import { type Context, errMessage, getContext } from "./lib/api";
import CatalogNodeView from "./lib/CatalogNodeView.svelte";
import CatalogRoot from "./lib/CatalogRoot.svelte";
import ConceptGroupView from "./lib/ConceptGroupView.svelte";
import DocView from "./lib/DocView.svelte";
import ProjectEditor from "./lib/ProjectEditor.svelte";
import { projectStore } from "./lib/project_store.svelte";
import { link, router } from "./lib/router.svelte";
import SearchOmnibox from "./lib/SearchOmnibox.svelte";
import SearchView from "./lib/SearchView.svelte";
import { windowStore } from "./lib/window.svelte";
import YearWindowSlider from "./lib/YearWindowSlider.svelte";

// The shell: a header with steward branding (GET /api/context) + a catalog-drift
// banner when present, and a routed main area. Internal <a> clicks are
// intercepted at the shell so navigation is pushState (no full reload).
let context = $state<Context | null>(null);
let contextError = $state<string | null>(null);

// The deployment context is app-global and immutable for the session — fetch
// once at mount (not an $effect that could re-run). Also wire the beforeunload
// warning (see reg_webapp/DESIGN.md → Browser storage + project-file persistence
// (the SPA store)): a tab/window close with a dirty draft prompts the
// browser's native "leave site?" dialog (autosaved-but-not-downloaded state is
// recoverable from IndexedDB in A5.4, but the file download is the durable copy).
onMount(() => {
  getContext()
    .then((resp) => {
      context = resp;
    })
    .catch((e) => {
      contextError = errMessage(e);
    });

  const onBeforeUnload = (event: BeforeUnloadEvent) => {
    if (projectStore.dirty) {
      event.preventDefault();
      // Legacy assignment kept for older browsers that gate the prompt on it.
      event.returnValue = "";
    }
  };
  window.addEventListener("beforeunload", onBeforeUnload);
  return () => window.removeEventListener("beforeunload", onBeforeUnload);
});

const route = $derived(router.route);
const driftWarnings = $derived(context?.catalog_drift_warnings ?? []);
// The deployment's bare reg_meta package version + its steward id, seeded into a
// new project's skeleton (ProjectEditor formats the version into a `reg_meta/v`
// release tag). Both fall back to the empty string until /api/context resolves (a
// new project before the context loads is an edge case; the seed is corrected on
// the next New).
const regMetaVersion = $derived(context?.webapp.reg_meta_version ?? "");
const steward = $derived(context?.steward.id ?? "");
// The catalog VINTAGE for the site-wide footer (#355 decision 2): the date the
// reg_meta DB was built. `import_date` is a UTC timestamp string
// (e.g. "2026-06-12T08:30:00Z"); show just the leading YYYY-MM-DD (split on "T").
const buildDate = $derived(context?.reg_meta.import_date.split("T")[0] ?? "");

// The header window slider's bounds (#611 → Period model). The floor is FIXED —
// a sensible earliest register year (Swedish registers start in the 1960s) — and
// the ceiling is the catalog vintage year (the `import_date` year, current year
// as fallback). `/api/context` exposes no catalog-wide variable year range, and
// adding one would need a new reg_meta aggregate accessor + backend field +
// openapi/types regen — heavier than this PR warrants; a catalog-derived
// refinement can be a later follow-up (#614 report).
const WINDOW_FLOOR_YEAR = 1960;
const windowMaxYear = $derived(
  Number(context?.reg_meta.import_date.slice(0, 4)) || new Date().getFullYear(),
);
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
    <nav class="nav">
      <a href="/catalog" class:active={route.name === "root" || route.name === "catalog-node"}>Catalog</a>
      <a href="/project" class:active={route.name === "project"}>
        Project
        {#if projectStore.dirty}<span class="nav-dirty" title="Unsaved changes">●</span>{/if}
      </a>
    </nav>
    <!-- The global project-window control (#611 → Period model): sets the window
         runtime layer, which hydrates from the active project (→ dirty → autosave)
         or falls back to localStorage when browsing without a project. The subject
         page's period picker will default to this window (#615). -->
    <YearWindowSlider
      min={WINDOW_FLOOR_YEAR}
      max={windowMaxYear}
      window={windowStore.value}
      onchange={(next) => windowStore.set(next)}
      onclear={() => windowStore.set(null)}
    />
    <!-- Full-width search row: the header wraps, so the omnibox sits on its own
         line below brand/nav and stretches across (#379). Inside the use:link
         div, so result-page links it routes to are interceptable. -->
    <SearchOmnibox />
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
        {#each driftWarnings as warning (`${warning.code}|${warning.path}`)}
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
        <CatalogNodeView
          fqidPath={route.fqidPath}
          {regMetaVersion}
          {steward}
          vintageYear={windowMaxYear}
        />
      {/key}
    {:else if route.name === "group"}
      <!-- #617: the concept-group SUBJECT page. Keyed on the (provider, register,
           key) triple so navigating between groups remounts; the `?member=` focus
           hint is read off the query INSIDE the view (no remount on refine). -->
      {#key `${route.provider}/${route.register}/${route.key}`}
        <ConceptGroupView
          provider={route.provider}
          register={route.register}
          key={route.key}
        />
      {/key}
    {:else if route.name === "project"}
      <ProjectEditor {regMetaVersion} {steward} />
    {:else if route.name === "search"}
      <SearchView />
    {:else if route.name === "doc"}
      {#key route.identifier}
        <DocView identifier={route.identifier} />
      {/key}
    {:else}
      <article>
        <h2>Not found</h2>
        <p>No page at <code>{route.path}</code>.</p>
        <p><a href="/catalog">Back to the catalog</a></p>
      </article>
    {/if}
  </main>

  <!-- Site-wide citation vintage (#355 decision 2): renders on every route so a
       reader citing any catalog node can see which reg_meta build it reflects.
       Guarded on `context` (rendered only once the deployment context has loaded). -->
  {#if context}
    <footer class="vintage muted">
      as of reg_meta v{regMetaVersion} · schema {context.reg_meta.schema_version} · built {buildDate}
    </footer>
  {/if}
</div>

<style>
  :global(:root) {
    --border: #d4d4d4;
    --muted: #666;
    --accent: #2563eb;
    --accent-bg: #eff4ff;
    --surface: #fff;
    /* Validation-level palette (error / warning / info), shared by every editor
       and the validation panels. Info reuses the accent. */
    --level-error: #b00020;
    --level-warning: #d97706;
    --level-info: var(--accent);
    /* The error-banner / error-badge fill + border pair. */
    --banner-error-bg: #fef2f2;
    --banner-error-border: #fca5a5;
  }
  :global(body) {
    margin: 0;
    font-family: system-ui, sans-serif;
    /* Explicit light theme: every palette token (borders, surfaces, text)
       assumes a light page, so pin the background — without it an OS dark-mode
       default paints dark behind the dark `#1a1a1a` text (unreadable). */
    background-color: #fff;
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
    color: var(--level-error);
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
  .nav {
    display: flex;
    gap: 1rem;
    align-items: baseline;
  }
  .nav a {
    color: var(--muted);
    font-weight: 600;
  }
  .nav a.active {
    color: var(--accent);
  }
  .nav-dirty {
    color: var(--level-warning);
    font-size: 0.7rem;
    vertical-align: super;
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
    background: var(--banner-error-bg);
    border: 1px solid var(--banner-error-border);
  }
  .vintage {
    font-size: 0.8rem;
    text-align: center;
    padding-top: 1.5rem;
    margin-top: 2rem;
    border-top: 1px solid var(--border);
  }
</style>
