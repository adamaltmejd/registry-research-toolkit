<script lang="ts">
import type { Snippet } from "svelte";
import { getCatalogRoot } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, DATA_BROWSER_LABEL } from "./catalog";
import type { StudyWindow } from "./project_data";
import { projectStore, type ValidationStatus } from "./project_store.svelte";
import { type Route, router } from "./router.svelte";
import SearchOmnibox from "./SearchOmnibox.svelte";
import { type BreadcrumbItem, Breadcrumbs } from "./ui";
import YearWindowSlider from "./YearWindowSlider.svelte";

// The app shell (#803): a persistent left RAIL (brand + primary nav + the
// project-window slider + the full provider facet list) and a TOPBAR (breadcrumb
// + the promoted SearchOmnibox command bar), wrapping a wide content canvas. On
// mobile the rail collapses to an overlay DRAWER toggled from the topbar, so all
// providers — and the window slider — stay reachable without scrolling past the
// fold.
//
// The project-window slider is a GLOBAL control (like the provider facets and
// primary nav), so it lives in the rail, reachable on every route and inside the
// drawer on mobile. Keeping it OUT of the topbar matches the design-system spec
// (topbar = breadcrumb + command bar only) and removes the 375px overflow its
// track caused when crammed into the topbar row.
//
// App.svelte stays the owner of the deployment context, banners, footer, and the
// routed <main> switch; it passes the context-derived chrome props down and the
// routed content in as `children`. The shell fetches its OWN provider list (the
// same GET /api/catalog the CatalogRoot page uses) — it's contextual facet
// navigation, independent of the routed page.

interface Props {
  /** Steward branding for the brand block (long name + id), or null until the
   * deployment context resolves. */
  steward: { long_name: string; id: string } | null;
  /** The project-window slider bounds + wiring (App owns the window store). */
  windowMin: number;
  windowMax: number;
  windowValue: StudyWindow | null;
  onWindowChange: (next: StudyWindow) => void;
  onWindowClear: () => void;
  /** The route-derived topbar breadcrumb trail (App derives it from the route). */
  breadcrumbs: BreadcrumbItem[];
  /** The routed content (App's <main> switch). */
  children: Snippet;
}

let {
  steward,
  windowMin,
  windowMax,
  windowValue,
  onWindowChange,
  onWindowClear,
  breadcrumbs,
  children,
}: Props = $props();

const route = $derived(router.route);

// The contextual provider facets — every provider + the classification-root
// sentinel, the same catalog root the CatalogRoot page renders. Fetched here so
// the facets are reachable on EVERY route, not just `/catalog`.
const root = asyncResource(() => getCatalogRoot());
const providers = $derived(root.data?.children ?? []);

// The mobile drawer open state. Closed by default; the topbar hamburger toggles
// it, and any navigation closes it (an $effect on the route) so a facet click
// doesn't leave the overlay covering the freshly-routed page.
let drawerOpen = $state(false);
$effect(() => {
  // Re-run on route change; close the drawer after a navigation.
  route;
  drawerOpen = false;
});

// Whether the data-browser nav item is "active" — any catalog/group route. The
// active state is signalled by weight + a left-edge marker + aria-current, never
// hue alone (the design review's color-only-signalling flag).
const dataBrowserActive = $derived(
  route.name === "root" ||
    route.name === "catalog-node" ||
    route.name === "group" ||
    route.name === "class-group",
);

const projectDraft = $derived(projectStore.draft);
const projectSources = $derived(
  Array.isArray(projectDraft?.sources) ? projectDraft.sources : [],
);
const projectSourceCount = $derived(projectSources.length);
const projectColumnCount = $derived(
  projectSources.reduce(
    (total, source) =>
      total + (Array.isArray(source.bindings) ? source.bindings.length : 0),
    0,
  ),
);
const projectTitle = $derived.by(() => {
  if (projectDraft == null) {
    return "No project";
  }
  const name =
    typeof projectDraft.name === "string" ? projectDraft.name.trim() : "";
  return name.length > 0 ? name : "Untitled project";
});
const projectStatus = $derived(projectStore.validationStatus);

const STATUS_LABEL: Record<ValidationStatus, string> = {
  unchecked: "Unchecked",
  checking: "Checking",
  ok: "Valid",
  warnings: "Warnings",
  errors: "Errors",
};

function isCurrent(active: boolean): "page" | undefined {
  return active ? "page" : undefined;
}

// The provider facet's display label: providers carry an optional name, the
// classification-root a default one; both fall back to the FQID (the
// CatalogRoot labelling).
function facetLabel(child: { name?: string | null; fqid: string }): string {
  return child.name ?? child.fqid;
}

function plural(count: number, singular: string, pluralLabel: string): string {
  return `${count} ${count === 1 ? singular : pluralLabel}`;
}
</script>

<div class="shell" class:drawer-open={drawerOpen}>
  <!-- The rail. On desktop it's the persistent left column; on mobile it's an
       off-canvas drawer revealed by the topbar toggle (the `.drawer-open` class
       on the shell slides it in). -->
  <aside id="app-rail" class="rail" aria-label="Primary">
    <div class="brand">
      <a href="/" class="brand-home">
        {#if steward}
          {steward.long_name}
        {:else}
          Register Research Catalog
        {/if}
      </a>
      {#if steward}
        <span class="brand-id">{steward.id}</span>
      {/if}
    </div>

    <nav class="primary-nav" aria-label="Sections">
      <a
        href="/catalog"
        class="nav-item"
        class:active={dataBrowserActive}
        aria-current={isCurrent(dataBrowserActive)}>{DATA_BROWSER_LABEL}</a>
      <a
        href="/project"
        class="project-chip"
        class:active={route.name === "project"}
        class:empty={projectDraft == null}
        aria-current={isCurrent(route.name === "project")}
        aria-label={`Project: ${projectTitle}, ${plural(projectSourceCount, "source", "sources")} and ${plural(projectColumnCount, "column", "columns")}, ${STATUS_LABEL[projectStatus]}`}
      >
        <span class="project-chip-head">
          <span class="project-chip-label">Project</span>
          {#if projectStore.dirty}
            <span class="project-dirty">Unsaved</span>
          {/if}
        </span>
        <span class="project-chip-title">{projectTitle}</span>
        <span class="project-chip-meta">
          <span>
            {plural(projectSourceCount, "source", "sources")} · {plural(projectColumnCount, "column", "columns")}
          </span>
          <span class={`project-status ${projectStatus}`}>
            <span class="project-status-dot" aria-hidden="true"></span>
            {STATUS_LABEL[projectStatus]}
          </span>
        </span>
      </a>
    </nav>

    <!-- The project-window slider: a global control, rendered ONCE here in the
         rail (so it's reachable on every route, and inside the drawer on mobile).
         A `--micro-label` eyebrow matches the "Providers" facets-label style. -->
    <div class="rail-window">
      <p class="rail-window-label">Study window</p>
      <YearWindowSlider
        min={windowMin}
        max={windowMax}
        window={windowValue}
        onchange={onWindowChange}
        onclear={onWindowClear}
      />
    </div>

    <!-- Contextual facets: the full provider list, reachable on every route.
         Inside the shell's `use:link` ancestor (App's root), so these route via
         pushState like any internal link. -->
    <nav class="facets" aria-label="Providers">
      <p class="facets-label">Providers</p>
      {#if root.loading}
        <p class="facets-note" aria-busy="true">Loading…</p>
      {:else if root.error}
        <p class="facets-note error" role="alert">Failed to load providers.</p>
      {:else}
        <ul class="facet-list">
          {#each providers as child (child.fqid)}
            {@const active =
              route.name === "catalog-node" &&
              (route.fqidPath === child.fqid ||
                route.fqidPath.startsWith(`${child.fqid}/`))}
            <li>
              <a
                href={catalogHref(child.fqid)}
                class="facet"
                class:active
                title={child.fqid}
                aria-current={isCurrent(active)}>{facetLabel(child)}</a>
            </li>
          {/each}
        </ul>
      {/if}
    </nav>
  </aside>

  <!-- A click-catching scrim behind the open mobile drawer; closing it taps out.
       aria-hidden + a plain button label keep it out of the reading order while
       still keyboard-dismissible. -->
  {#if drawerOpen}
    <button
      type="button"
      class="scrim"
      aria-label="Close menu"
      onclick={() => (drawerOpen = false)}
    ></button>
  {/if}

  <div class="frame">
    <header class="topbar">
      <button
        type="button"
        class="menu-toggle"
        aria-label="Open menu"
        aria-controls="app-rail"
        aria-expanded={drawerOpen}
        onclick={() => (drawerOpen = !drawerOpen)}
      >
        <span class="menu-glyph" aria-hidden="true">☰</span>
      </button>

      <!-- Wrapper we control so we can hide the breadcrumb on mobile (it crowds
           the 375px topbar row; the rail + routed page header carry context
           there). Don't restyle the shared Breadcrumbs primitive for this. -->
      <div class="topbar-crumbs">
        <Breadcrumbs items={breadcrumbs} />
      </div>

      <div class="command">
        <SearchOmnibox />
      </div>
    </header>

    <main class="canvas">
      {@render children()}
    </main>
  </div>
</div>

<style>
  /* Two columns on desktop: a fixed-width rail + a fluid frame. The rail becomes
     an off-canvas drawer below the mobile breakpoint (see the media query). */
  .shell {
    display: grid;
    grid-template-columns: 16rem minmax(0, 1fr);
    min-height: 100vh;
  }

  /* ── Rail ──────────────────────────────────────────────────────────────── */
  .rail {
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
    min-width: 0;
    padding: var(--space-4) var(--space-3);
    border-right: 1px solid var(--border);
    background: var(--surface-sunken);
  }
  .brand {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  .brand-home {
    font-size: var(--text-h3);
    font-weight: 700;
    color: var(--text);
    text-decoration: none;
    line-height: 1.2;
  }
  .brand-home:hover {
    color: var(--accent-ink);
  }
  .brand-home:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .brand-id {
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    color: var(--text-muted);
  }

  .primary-nav {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  /* Nav + facet links share the active treatment: a left-edge accent bar +
     bolder weight (NOT hue alone — the design-review color-only flag). The bar is
     a transparent border that the accent fills when active, so the text doesn't
     shift position between states. */
  .nav-item,
  .facet {
    display: flex;
    align-items: baseline;
    gap: var(--space-1);
    padding: var(--space-1) var(--space-2);
    border-left: 2px solid transparent;
    color: var(--text-muted);
    text-decoration: none;
    border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
  }
  .nav-item {
    font-weight: 600;
  }
  .nav-item:hover,
  .facet:hover {
    color: var(--accent-ink);
    background: var(--surface-hover);
  }
  .nav-item:focus-visible,
  .facet:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .nav-item.active,
  .facet.active {
    color: var(--text);
    font-weight: 700;
    border-left-color: var(--accent);
    background: var(--accent-bg);
  }
  .project-chip {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    box-sizing: border-box;
    min-width: 0;
    padding: var(--space-2);
    border: 1px solid var(--border);
    border-left: 3px solid transparent;
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text);
    text-decoration: none;
  }
  .project-chip:hover {
    border-color: var(--border-strong);
    border-left-color: var(--accent);
    background: var(--surface-hover);
  }
  .project-chip:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .project-chip.active {
    border-color: color-mix(in srgb, var(--accent) 45%, var(--border));
    border-left-color: var(--accent);
    background: var(--accent-bg);
  }
  .project-chip.empty .project-chip-title {
    color: var(--text-muted);
  }
  .project-chip-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-2);
    min-width: 0;
  }
  .project-chip-label {
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    color: var(--text-faint);
  }
  .project-dirty {
    flex: 0 0 auto;
    font-size: var(--text-micro);
    font-weight: 700;
    color: var(--warn);
  }
  .project-chip-title {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-weight: 700;
    line-height: 1.2;
  }
  .project-chip-meta {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    color: var(--text-muted);
  }
  .project-status {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    font-family: var(--font-ui);
    font-weight: 700;
  }
  .project-status-dot {
    width: 0.5rem;
    height: 0.5rem;
    border-radius: 999px;
    background: currentColor;
  }
  .project-status.unchecked {
    color: var(--text-faint);
  }
  .project-status.checking {
    color: var(--info);
  }
  .project-status.ok {
    color: var(--ok);
  }
  .project-status.warnings {
    color: var(--warn);
  }
  .project-status.errors {
    color: var(--err);
  }

  /* The window-slider control block in the rail. The eyebrow reuses the
     facets-label micro-label treatment so the rail's two global controls (window
     + providers) read as a consistent pair. */
  .rail-window {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    min-width: 0;
  }
  .facets {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    min-width: 0;
  }
  .rail-window-label,
  .facets-label {
    margin: 0 0 var(--space-1);
    padding: 0 var(--space-2);
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    color: var(--text-faint);
  }
  .facets-note {
    margin: 0;
    padding: 0 var(--space-2);
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  .facets-note.error {
    color: var(--err);
  }
  .facet-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  /* The facet label may be a long provider name — clamp it so it never widens the
     rail (or, on mobile, the drawer) past its track. */
  .facet {
    font-size: var(--text-sm);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* ── Frame (topbar + canvas) ───────────────────────────────────────────── */
  .frame {
    display: flex;
    flex-direction: column;
    min-width: 0;
  }
  .topbar {
    display: flex;
    align-items: center;
    gap: var(--space-3);
    min-width: 0;
    padding: var(--space-2) var(--space-4);
    border-bottom: 1px solid var(--border);
    background: var(--surface);
  }
  /* Breadcrumb wrapper — shrinks before the command bar and is hidden on mobile
     (see the media query). `min-width: 0` lets it ellipsize rather than push the
     row wide. */
  .topbar-crumbs {
    min-width: 0;
  }
  .command {
    display: flex;
    flex: 1 1 auto;
    min-width: 0;
  }
  /* The hamburger is mobile-only (hidden on desktop where the rail is persistent). */
  .menu-toggle {
    display: none;
    align-items: center;
    justify-content: center;
    box-sizing: border-box;
    min-width: 36px;
    min-height: 36px;
    padding: var(--space-1);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text);
    font-size: var(--text-h3);
    line-height: 1;
    cursor: pointer;
  }
  .menu-toggle:hover {
    border-color: var(--accent);
    color: var(--accent-ink);
  }
  .menu-toggle:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }

  .canvas {
    /* A wide, comfortable measure — no 56rem cap — but not full-bleed: dense
       pages breathe while a max-width keeps line lengths legible. The 375px
       no-overflow fix (criterion #1) is the `box-sizing: border-box` + `width:
       100%` + `max-width` + `min-width: 0`: the canvas element itself never
       exceeds the viewport (border-box folds the padding INTO `width: 100%` —
       without it, there being no global border-box reset in this app, the canvas
       was 100% + 32px wide, overflowing by exactly its horizontal padding). With
       the canvas bounded, `overflow-x: auto` lets content WIDER than the canvas
       (ConceptGroupView's facet-matrix, dense tables) scroll horizontally INSIDE
       the canvas instead of being clipped (a `hidden` clip made the rightmost
       columns unreachable). Only the overflowing children scroll; the document
       does not — so this stays within criterion #1, it's a reachability fix, not
       a clip-bandaid. */
    box-sizing: border-box;
    width: 100%;
    max-width: 80rem;
    min-width: 0;
    margin: 0 auto;
    padding: var(--space-4);
    overflow-x: auto;
  }

  .scrim {
    display: none;
  }

  /* ── Mobile: rail → off-canvas drawer ──────────────────────────────────── */
  @media (max-width: 48rem) {
    .shell {
      /* Single column; the rail leaves the flow and overlays as a drawer. */
      grid-template-columns: minmax(0, 1fr);
    }
    .menu-toggle {
      display: inline-flex;
    }
    /* Hide the breadcrumb on mobile so the topbar is a clean [hamburger][command
       bar] row — the rail + routed page header already carry context here, and
       the crumb crowded the 375px row. */
    .topbar-crumbs {
      display: none;
    }
    .rail {
      position: fixed;
      inset: 0 auto 0 0;
      z-index: 60;
      width: min(20rem, 85vw);
      max-width: 85vw;
      overflow-y: auto;
      /* Off-canvas by default; `.drawer-open` slides it in. `visibility: hidden`
         on the closed rail removes its links from the tab order and the a11y
         tree (a translateX-only off-canvas element stays focusable, so a keyboard
         user would tab into invisible off-screen links). `visibility` still
         allows the transform transition — unlike `display: none`, which would
         kill the slide — so we transition it alongside the transform. */
      visibility: hidden;
      transform: translateX(-100%);
      transition:
        transform var(--motion-fast) ease-out,
        visibility var(--motion-fast) ease-out;
      box-shadow: var(--elevation-raised);
    }
    .drawer-open .rail {
      visibility: visible;
      transform: translateX(0);
    }
    .scrim {
      /* The click-out dimmer covers only the content EXPOSED beside the open
         drawer (its left edge starts at the rail's right edge), not the whole
         viewport. The opaque rail (z-60) already hides what's under it, so a
         full-bleed scrim there would only add a dead zone where the rail
         intercepts a click meant to dismiss — leaving the scrim's own hit-area
         (and its center) reliably on top of the exposed canvas. */
      display: block;
      position: fixed;
      inset: 0 0 0 min(20rem, 85vw);
      z-index: 55;
      border: 0;
      padding: 0;
      background: var(--scrim);
      cursor: pointer;
    }
  }
</style>
