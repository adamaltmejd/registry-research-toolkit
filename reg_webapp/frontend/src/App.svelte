<script lang="ts">
import { onMount } from "svelte";
import AppShell from "./lib/AppShell.svelte";
import { type Context, errMessage, getContext } from "./lib/api";
import CatalogNodeView from "./lib/CatalogNodeView.svelte";
import CatalogRoot from "./lib/CatalogRoot.svelte";
import ClassificationGroupView from "./lib/ClassificationGroupView.svelte";
import ConceptGroupView from "./lib/ConceptGroupView.svelte";
import { routeBreadcrumbs } from "./lib/catalog";
import DocView from "./lib/DocView.svelte";
import Home from "./lib/Home.svelte";
import ProjectEditor from "./lib/ProjectEditor.svelte";
import { projectStore } from "./lib/project_store.svelte";
import { link, router } from "./lib/router.svelte";
import SearchView from "./lib/SearchView.svelte";
import { windowStore } from "./lib/window.svelte";

// The app root: owns the deployment context (GET /api/context), the
// beforeunload guard, the drift/error banners, the routed <main> switch, and the
// citation footer. The chrome (left rail + topbar command bar) is delegated to
// AppShell (#803); App passes the context-derived props down and the routed
// content in as the shell's `children`. Internal <a> clicks are intercepted at
// the root container so navigation is pushState (no full reload).
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

// The header window slider's bounds (#611 → Period model). A steward deployment
// can expose a best-effort catalog-wide year span; use it so filtered catalogs do
// not show decades of empty track. Global/unparseable deployments fall back to
// the historical 1960 → catalog-vintage range.
const FALLBACK_WINDOW_FLOOR_YEAR = 1960;
const catalogVintageYear = $derived(
  Number(context?.reg_meta.import_date.slice(0, 4)) || new Date().getFullYear(),
);
const catalogPeriodSpan = $derived(
  context?.steward.catalog_period_span ?? null,
);
const enforcePeriodBounds = $derived(catalogPeriodSpan !== null);
const windowMinYear = $derived(
  catalogPeriodSpan?.from ?? FALLBACK_WINDOW_FLOOR_YEAR,
);
const windowMaxYear = $derived(catalogPeriodSpan?.to ?? catalogVintageYear);

$effect(() => {
  if (enforcePeriodBounds) {
    windowStore.clampTo(windowMinYear, windowMaxYear);
  }
});

// The route-derived topbar breadcrumb (#803). Structural only — raw slug labels;
// the routed page owns its rich, display-name header.
const breadcrumbItems = $derived(routeBreadcrumbs(route));
</script>

<!-- Click interception is delegated from the root container via the `link`
     action: any internal <a> inside bubbles here and is pushState-navigated. The
     chrome (rail + topbar) is the AppShell; App owns the banners (above the
     canvas), the routed content (the shell's children), and the footer. -->
<div class="app" use:link>
  <AppShell
    steward={context?.steward ?? null}
    windowMin={windowMinYear}
    windowMax={windowMaxYear}
    windowValue={windowStore.value}
    onWindowChange={(next) => windowStore.set(next)}
    onWindowClear={() => windowStore.set(null)}
    breadcrumbs={breadcrumbItems}
  >
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

    <!-- The routed content (AppShell's `<main>` canvas is the landmark wrapper;
         App emits the page bodies into it). -->
    <div class="routed">
      {#if route.name === "home"}
        <!-- #675: the landing page at `/`, split from the data browser. App holds
             the context, so pass the steward through — Home fetches only /api/stats. -->
        <Home steward={context?.steward ?? null} />
      {:else if route.name === "root"}
        <CatalogRoot />
      {:else if route.name === "catalog-node"}
        {#key route.fqidPath}
          <CatalogNodeView
            fqidPath={route.fqidPath}
            {regMetaVersion}
            {steward}
            {windowMinYear}
            {windowMaxYear}
            {enforcePeriodBounds}
            vintageYear={catalogVintageYear}
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
            {regMetaVersion}
            {steward}
            {windowMinYear}
            {windowMaxYear}
            {enforcePeriodBounds}
            vintageYear={catalogVintageYear}
          />
        {/key}
      {:else if route.name === "class-group"}
        <!-- #756: the classification-umbrella SUBJECT page. Keyed on the group key so
             navigating between umbrellas remounts. Catalog-global (no
             provider/register/period), so it takes only the key. -->
        {#key route.key}
          <ClassificationGroupView key={route.key} />
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
          <p><a href="/catalog">Back to the data browser</a></p>
        </article>
      {/if}
    </div>

    <!-- Site-wide citation vintage (#355 decision 2): renders on every route so a
         reader citing any catalog node can see which reg_meta build it reflects.
         Guarded on `context` (rendered only once the deployment context loaded). -->
    {#if context}
      <footer class="vintage muted">
        as of reg_meta v{regMetaVersion} · schema {context.reg_meta.schema_version} · built {buildDate}
      </footer>
    {/if}
  </AppShell>
</div>

<style>
  /* The design-token layer (palette, type, geometry, fonts) + base body
     typography live in src/tokens.css (imported in main.ts), NOT here. This
     block holds only App-shell layout. Token references below (--accent,
     --text-muted, --err, --border, --err-bg, …) resolve from tokens.css's
     canonical semantic roles. */
  :global(a) {
    color: var(--accent);
    text-decoration: none;
  }
  :global(a:hover) {
    text-decoration: underline;
  }
  :global(.muted) {
    color: var(--text-muted);
  }
  :global(.error) {
    color: var(--err);
  }
  /* The app root is a bare wrapper now — AppShell owns the rail/topbar/canvas
     layout (the old centered 56rem ribbon is gone, #803). */
  .banner {
    padding: 0.75rem 1rem;
    border-radius: 4px;
    margin-bottom: 1rem;
  }
  .banner.drift {
    background: var(--warn-bg);
    border: 1px solid var(--warn);
  }
  .banner.drift ul {
    margin: 0.5rem 0 0;
  }
  .banner.error {
    background: var(--err-bg);
    border: 1px solid var(--red-border);
  }
  .vintage {
    font-size: 0.8rem;
    text-align: center;
    padding-top: 1.5rem;
    margin-top: 2rem;
    border-top: 1px solid var(--border);
  }
</style>
