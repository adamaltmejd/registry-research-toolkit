<script lang="ts">
import {
  type ConceptGroupNodeMember,
  getConceptGroup,
  getConceptGroupGraph,
  type VariableGraphNode,
} from "./api";
import { asyncResource } from "./async.svelte";
import {
  catalogHref,
  facetLabelJoin,
  pickerRepresentations,
  pickerWindowYears,
  registerPrefixOf,
} from "./catalog";
import PeriodPicker from "./PeriodPicker.svelte";
import type { PeriodGrain } from "./period";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store.svelte";
import RepresentationPicker, {
  type PickerBand,
  type PickerSelection,
} from "./RepresentationPicker.svelte";
import { router } from "./router.svelte";
import SubjectView from "./SubjectView.svelte";
import TechnicalDetails from "./TechnicalDetails.svelte";
import { windowStore } from "./window.svelte";

// The concept-group SUBJECT page (#617, #678 inc 2): fetches a group by (provider,
// register, key) — its members + facets — AND the group's relationship graph (the
// union of its members' variable nodes, each carrying that variable's states). It
// renders the members as a vertical stack of REPRESENTATION BANDS (one per member
// variable, each its own adaptive representation rows) under ONE shared selection
// basket and ONE "Add" footer — the group→variable→representation nesting (#678).
//
// Renders through the unified SubjectView shell (#638 PR1), same as the binding +
// classification leaves. A group fills two of the shell's sections — the
// `description` (a Technical details disclosure holding key/facets/source) and the
// `picker` (the nested RepresentationPicker + the PeriodPicker availability lens).
let {
  provider,
  register,
  key,
  // The deployment seed (App → here, mirroring CatalogNodeView/BindingLeafView): the
  // reg_meta package version + steward id stamped onto a created project. Empty
  // until /api/context resolves — an implicit project created with an empty seed is
  // never re-seeded, so Add stays disabled until both are present (sub-second).
  regMetaVersion,
  steward,
  // #631: the catalog VINTAGE year (App → here, mirroring CatalogNodeView), the
  // period picker's open-ended slider ceiling. undefined only before
  // /api/context resolves (the picker falls back to wall-clock then).
  vintageYear,
}: {
  provider: string;
  register: string;
  key: string;
  regMetaVersion: string;
  steward: string;
  vintageYear?: number;
} = $props();

// The `?member=` focus hint lives in the query (like `?period`), so refining it
// doesn't remount this view. Read it reactively and pass it to the fetch (the
// backend echoes it on the node only when it names a real member).
const memberHint = $derived(router.getQueryParam("member"));
const resource = asyncResource(() =>
  getConceptGroup(provider, register, key, memberHint ?? undefined),
);
const node = $derived(resource.data);

// The group's relationship graph (#761/#678) — the union of its member variables'
// nodes, each carrying that variable's states. Its OWN failure domain: a graph
// error / empty leaves a member's band with no rows (the band shows a quiet "no
// representations") rather than blanking the page. `focus_id` is null (a
// group-addressed call).
const graphResource = asyncResource(() =>
  getConceptGroupGraph(provider, register, key),
);
const graph = $derived(graphResource.data);

// The register the group lives under — the breadcrumb target and a member's
// shared ancestor (a group is always register-scoped).
const registerFqid = $derived(`${provider}/${register}`);

function leafSlug(fqid: string): string {
  return fqid.split("/").at(-1) ?? fqid;
}

// ── The variable nodes by fqid (the band → states match) ─────────────────────
// Each group member is a real leaf FQID; the graph carries one variable node per
// member variable (keyed by its own `fqid`). Index the variable nodes by fqid so a
// member's band pulls its states by FQID match. A representation-member group can
// carry several members on ONE fqid (distinct delivery columns) — they share the
// single graph node, whose states already span every column, so the band's rows
// enumerate them all; building bands per DISTINCT fqid avoids duplicating those rows.
const nodesByFqid = $derived.by(() => {
  const map = new Map<string, VariableGraphNode>();
  for (const n of graph?.nodes ?? []) {
    if (n.kind === "variable" && n.fqid != null) {
      map.set(n.fqid, n);
    }
  }
  return map;
});

// ── Shared concept definition / description (#678) ───────────────────────────
// The group page's missing descriptive context: each MEMBER variable node carries
// its own `definition`/`description` (the variable-level concept text). Most
// parallel-column siblings carry null — that's expected — but the canonical member
// usually carries the one definition the whole group shares. Collect the DISTINCT
// non-empty values across the MEMBER nodes (scoped to `node.members`, so a
// succession/related neighbour the graph union pulls in never leaks its text here):
// typically exactly one (rendered once at the group level); zero → render nothing;
// several distinct → render each (rare today).
function distinctMemberMeta(field: "definition" | "description"): string[] {
  if (!node) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const member of node.members) {
    const value = nodesByFqid.get(member.fqid)?.[field]?.trim();
    if (value && !seen.has(value)) {
      seen.add(value);
      out.push(value);
    }
  }
  return out;
}
const sharedDefinitions = $derived(distinctMemberMeta("definition"));
const sharedDescriptions = $derived(distinctMemberMeta("description"));

/** The in-this-group facet label for a member (e.g. "AGI · 2007 SNI edition"),
 * joined from its facets, or null when the member carries none (an ungrouped /
 * axis-less member — its name suffices). */
function memberFacetLabel(member: ConceptGroupNodeMember): string | null {
  return member.facets.length > 0 ? facetLabelJoin(member.facets) : null;
}

/** The picker bands — one per DISTINCT member variable (by fqid), in the group's
 * member order. Each band pulls its representation rows from the matching graph
 * node's states; a member with no graph node (graph error / partial union) still
 * renders a band with EMPTY rows (the band shows a quiet "no representations" rather
 * than dropping the member). The facet label distinguishes representation members
 * collapsed onto one fqid; the band's adaptive rows surface the delivery-column
 * split. */
const bands = $derived.by((): PickerBand[] => {
  if (!node) {
    return [];
  }
  const seen = new Set<string>();
  const out: PickerBand[] = [];
  for (const member of node.members) {
    if (seen.has(member.fqid)) {
      continue;
    }
    seen.add(member.fqid);
    const states = nodesByFqid.get(member.fqid)?.states ?? [];
    out.push({
      key: member.fqid,
      name: member.name ?? leafSlug(member.fqid),
      registerPrefix: registerPrefixOf(member.fqid),
      facetLabel: memberFacetLabel(member),
      // The member's own leaf page — the picker renders the identity as a nav link
      // (the binding leaf passes no href; it's already that page).
      href: catalogHref(member.fqid),
      rows: pickerRepresentations(states),
    });
  }
  return out;
});

// ── The time axis: `?period` (client-side lens, no refetch) ──────────────────
// The PeriodPicker drives per-row DIMMING only — `getConceptGroup` takes no period,
// so the value triggers NO refetch; it just narrows the dim window (mirrors the
// binding leaf's `pickerWindow`).
const period = $derived(router.getQueryParam("period"));

// Members are year-grain coverage, so the picker offers only the year grain.
const grains: PeriodGrain[] = ["year"];

// The picker's coverage track = the union span over all member variables' rows.
// Derived from the bands' representation spans (year-grain): the slider shows where
// the group as a whole has data. simplify: an inline min/max over the bands' rows is
// enough here — the union is presentational track context, not a gate.
const unionCoverage = $derived.by(() => {
  let from: number | null = null;
  let to: number | null = null;
  let openEnded = false;
  for (const band of bands) {
    for (const row of band.rows) {
      const lo = Number(row.from.slice(0, 4));
      if (Number.isFinite(lo) && row.from !== "0001-01-01") {
        from = from === null ? lo : Math.min(from, lo);
      }
      if (row.to === "9999-12-31") {
        openEnded = true;
      } else {
        const hi = Number(row.to.slice(0, 4));
        if (Number.isFinite(hi)) {
          to = to === null ? hi : Math.max(to, hi);
        }
      }
    }
  }
  if (from === null && to === null) {
    return null;
  }
  return { from, to: openEnded ? null : to };
});

/** The active period window to DIM against, as an inclusive year pair (a `?period`
 * wire wins, else the global study window), or null (no dimming). Mirrors the
 * binding leaf's `pickerWindow`. */
const pickerWindow = $derived(
  pickerWindowYears(period ?? null, windowStore.value),
);

/** Write `?period` to the group URL (preserving the pathname + any `?member=` focus
 * hint), which the reactive query picks up. A null period drops `?period`. NO
 * refetch — `getConceptGroup` takes no period; the value only drives the per-row
 * dimming. */
function writePeriod(next: string | null): void {
  const qs = new URLSearchParams();
  if (next) {
    qs.set("period", next);
  }
  if (memberHint) {
    qs.set("member", memberHint);
  }
  const query = qs.toString();
  router.navigate(window.location.pathname + (query ? `?${query}` : ""));
}

// ── Add to project ───────────────────────────────────────────────────────────
// The deployment seed is ready once /api/context has populated BOTH fields.
const seedReady = $derived(regMetaVersion !== "" && steward !== "");

/** The committed outcome (drives the inline confirmation): the sources bindings
 * landed in (created or found) and how many were already present. */
let addOutcome = $state<{
  added: { name: string; period: string | null }[];
  already: number;
} | null>(null);

// A fresh period / member refine or a group change clears the stale confirmation.
$effect(() => {
  void period;
  void memberHint;
  void key;
  addOutcome = null;
});

/** Commit each selected representation across all bands through the store, one
 * `addFromCatalog` per picked row. Each selection's `band.key` is the member fqid
 * (the variable); each row's own period span is the wire period for its source.
 * Aggregates into the existing `addOutcome` confirmation. */
function commitSelected(selected: PickerSelection[]): void {
  if (selected.length === 0) {
    return;
  }
  const added: { name: string; period: string | null }[] = [];
  let already = 0;
  for (const { band, row } of selected) {
    const result = projectStore.addFromCatalog(
      {
        registerVariant: `${registerPrefixOf(band.key)}/${row.variant}`,
        variable: band.key,
        representation: row.column,
        resolvedPeriod: row.wirePeriod,
      },
      { reg_meta_version: regMetaReleaseTag(regMetaVersion), steward },
    );
    if (result.status === "added") {
      added.push({ name: result.sourceName, period: row.wirePeriod });
    } else {
      already += 1;
    }
  }
  addOutcome = { added, already };
}
</script>

{#if resource.loading}
  <p class="muted" aria-busy="true">Loading…</p>
{:else if resource.error}
  <p class="error" role="alert">
    {#if resource.status === 404}
      Not found: concept group <code>{key}</code> in <code>{registerFqid}</code>
    {:else}
      {resource.error}
    {/if}
  </p>
{:else if node}
  {#snippet description()}
    <!-- #678: the SHARED concept definition/description, deduplicated across the
         member variable nodes (most siblings carry null; the canonical member
         carries the one the group shares). Rendered as a clean labelled block at the
         TOP of the page — mirroring the binding leaf's Definition/Description
         presentation — ABOVE the Technical details disclosure, giving the group its
         missing descriptive context. Hidden entirely when no member carries either. -->
    {#if sharedDefinitions.length > 0 || sharedDescriptions.length > 0}
      <dl class="meta">
        {#each sharedDefinitions as definition}
          <dt>Definition</dt>
          <dd>{definition}</dd>
        {/each}
        {#each sharedDescriptions as description}
          <dt>Description</dt>
          <dd>{description}</dd>
        {/each}
      </dl>
    {/if}

    <!-- The group's key, facets, and source are all build-derivation metadata, not
         researcher-facing — so all three are demoted together behind the "Technical
         details" disclosure. The page then leads with the title + member bands. -->
    <TechnicalDetails>
      <dl class="meta">
        <dt>Group</dt>
        <dd><code>{node.key}</code></dd>
        {#if node.axes.length > 0}
          <dt>Facets</dt>
          <dd>{node.axes.map((a) => a.label).join(", ")}</dd>
        {/if}
        <dt>Source</dt>
        <dd>{node.source}</dd>
      </dl>
    </TechnicalDetails>
  {/snippet}

  {#snippet picker()}
    <!-- The nested REPRESENTATION PICKER (#678 inc 2): one band per member variable,
         each with its own adaptive representation rows, under ONE shared selection
         basket and ONE "Add" footer. The period window only DIMS out-of-window rows
         (`pickerWindow`); `seedReady` gates the commit. -->
    {#if bands.length > 0}
      <RepresentationPicker
        {bands}
        window={pickerWindow}
        canAdd={seedReady}
        onadd={commitSelected}
      />
    {/if}

    {#if addOutcome}
      <p class="page-add">
        {#if addOutcome.added.length === 0}
          <span class="add-confirm already" role="status"
            >Already in project</span
          >
        {:else}
          <span class="add-confirm" role="status">
            {#if addOutcome.added.length === 1}
              Added to project ({addOutcome.added[0].name})
            {:else}
              Added {addOutcome.added.length} columns:
              {addOutcome.added
                .map((a) => (a.period ? `${a.name} (${a.period})` : a.name))
                .join(", ")}
            {/if}
            {#if addOutcome.already > 0}
              · {addOutcome.already} already in project
            {/if}
            — <a href="/project">view</a>
          </span>
        {/if}
      </p>
    {/if}

    <!-- The TIME axis: a client-side availability LENS (no refetch). Seeds from the
         project window, draws the members' union coverage span, and dims the rows
         whose span doesn't overlap the chosen window. Writes `?period` only (never
         the global window), mirroring the leaf; `getConceptGroup` ignores it. -->
    <PeriodPicker
      {period}
      {grains}
      window={windowStore.value}
      coverage={unionCoverage}
      {vintageYear}
      onsubmit={(p) => writePeriod(p)}
      onclear={() => writePeriod(null)}
    />
  {/snippet}

  <SubjectView title={node.label} {description} {picker} />
{/if}

<style>
  /* #638 PR4: row spacing standardized across the three subject kinds. */
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: var(--space-1) var(--space-4);
    margin: var(--space-4) 0;
  }
  .meta dt {
    font-weight: 600;
  }
  /* The add-confirmation line (the picker owns the Add button). */
  .page-add {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-3);
    margin: var(--space-4) 0;
  }
  .add-confirm {
    font-size: var(--text-sm);
    color: var(--accent);
  }
  .add-confirm.already {
    color: var(--text-muted);
  }
</style>
