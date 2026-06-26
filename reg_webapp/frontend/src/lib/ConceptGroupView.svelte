<script lang="ts">
import { type ConceptGroupNodeMember, getConceptGroup } from "./api";
import { asyncResource } from "./async.svelte";
import {
  axisHueVar,
  axisValues,
  catalogHref,
  DATA_BROWSER_LABEL,
  formatWindow,
  memberAt,
  memberCoverageUnion,
  memberKey,
  OPEN_ENDED_VALID_TO,
  YEARLESS_VALID_FROM,
} from "./catalog";
import PeriodPicker from "./PeriodPicker.svelte";
import {
  type Coverage,
  notDeliveredGaps,
  type PeriodGrain,
  yearWindowFromWire,
} from "./period";
import { router } from "./router.svelte";
import SubjectView from "./SubjectView.svelte";
import TechnicalDetails from "./TechnicalDetails.svelte";
import { windowStore } from "./window.svelte";

// The concept-group SUBJECT page (#617): fetches a group by (provider, register,
// key) and renders its members + facets + per-member coverage. A group's default
// selection is "all members", which a member FQID can't express — so the group
// has its own address (`/catalog/group/<p>/<r>/<key>`), distinct from the FQID
// browse path that CatalogNodeView serves.
//
// Renders through the unified SubjectView shell (#638 PR1), same as the binding +
// classification leaves. A group fills two of the shell's sections — the
// `description` (just a Technical details disclosure holding key/facets/source) and
// a `picker` (#638 PR2a, below). It has no single fqid (its key shows inside the
// disclosure), no value set, no docs, and (post
// PR2a) no `relationships` — its members live IN the picker's selector now — so
// those sections are omitted. The breadcrumbs + loading / error arms stay OUTSIDE
// the shell (the shell is the success-arm body only).
let {
  provider,
  register,
  key,
  // #631: the catalog VINTAGE year (App → here, mirroring CatalogNodeView), the
  // period picker's open-ended slider ceiling. undefined only before
  // /api/context resolves (the picker falls back to wall-clock then).
  vintageYear,
}: {
  provider: string;
  register: string;
  key: string;
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

// The register the group lives under — the breadcrumb target and a member's
// shared ancestor (a group is always register-scoped).
const registerFqid = $derived(`${provider}/${register}`);

function leafSlug(fqid: string): string {
  return fqid.split("/").at(-1) ?? fqid;
}

/** A minimal study-window line for a member's coverage (#351), or "" when the
 * member carries no finite bound (a stateless member, or one unbounded on both
 * sides). Delegates to `formatWindow` so the display matches the rest of the
 * catalog (year-collapsed bounds; the open-ended sentinel renders "since <year>",
 * never the raw 9999). */
function coverageText(member: ConceptGroupNodeMember): string {
  const cov = member.coverage;
  if (!cov) {
    return "";
  }
  // `_coverage_bounds` contract: a finite window carries a non-null `coverage_to`;
  // open-ended maps to the sentinel. A null `to` here is a stateless member —
  // nothing to show.
  const to = cov.open_ended ? OPEN_ENDED_VALID_TO : cov.coverage_to;
  if (to == null) {
    return "";
  }
  // The start may be unknown — null (no finite start) or the yearless-fallback
  // floor (`0001-01-01`). Pass the sentinel through so `formatWindow` renders the
  // one-sided "until <end>" form (#658) and a known end year is shown rather than
  // hidden. An unknown start with an open-ended end has no finite bound → "".
  const from = cov.coverage_from ?? YEARLESS_VALID_FROM;
  if (from === YEARLESS_VALID_FROM && to === OPEN_ENDED_VALID_TO) {
    return "";
  }
  return formatWindow(from, to);
}

// ── #638 PR2a: the picker — a slice axis (member selector) × a time axis ──────
// The picker section gives the group page two orthogonal controls:
//   • the MEMBER SELECTOR (slice axis) — the group's facet grid, rendered
//     expanded (a 2-axis matrix / 1-axis chips / 0-axis list), mirroring the
//     register-browse ConceptGroupRow's shapes but richer (per-member coverage +
//     availability greying). Each member is a link to its leaf FQID; focusing a
//     member in place would need its value set (out of scope), so we navigate.
//   • the PERIOD PICKER (time axis) — the reusable window/coverage control. It is
//     a CLIENT-SIDE AVAILABILITY LENS only: `getConceptGroup` takes no period, so
//     the period drives NO refetch — it only greys members whose coverage doesn't
//     span the active window.

const axes = $derived(node?.axes ?? []);
// A 2D matrix only addresses TWO axes; a group with >2 axes (the iot
// disposable-income family) renders through the facet navigator (below) instead,
// so the matrix/chips/list path and its `ungridded` fallback are gated off this.
const useNavigator = $derived(axes.length > 2);
// Matrix orientation: first axis → rows, second axis → columns (mirrors
// ConceptGroupRow). `node` is always present inside the success arm where the
// snippet renders, but guard so the top-level deriveds stay total.
const matrixRows = $derived(
  node && axes.length > 0 ? axisValues(node, axes[0]) : [],
);
const matrixCols = $derived(
  node && axes.length > 1 ? axisValues(node, axes[1]) : [],
);
// `axes` is the UNION across members; a member missing a facet on any axis never
// matches a matrix cell, so it would silently vanish from the grid — render those
// below as a plain list (mirrors ConceptGroupRow's `ungridded`).
const ungridded = $derived(
  node && axes.length >= 2 && !useNavigator
    ? node.members.filter(
        (m) => !axes.every((axis) => m.facets.some((f) => f.axis === axis)),
      )
    : [],
);

// ── The N-axis facet navigator (#819) ────────────────────────────────────────
// A 2D matrix only addresses TWO axes; a group with >2 axes (the iot
// disposable-income family: enhet × hushållsbegrepp × kapitalvinst) can't be a
// grid without collapsing members that share the first two coords and DROPPING
// the rest (they also escape `ungridded`, which only catches members MISSING an
// axis — a 3-axis member covers all declared axes). So >2-axis groups render
// through a facet navigator instead: every member is a row carrying one
// hue-tinted facet pill per axis, and per-axis filter controls narrow the visible
// set. The matrix path stays for ≤2 axes (no regression). The navigator is a
// CLIENT-SIDE LENS — like the period slider, a filter only narrows what's shown;
// it NEVER mutates the project/selection.

// One filter group per axis: the distinct (value, label) pairs members carry on
// it (value-sorted, via `axisValues`), each with the axis's display hue. The
// navigator renders one filter block per entry.
const axisFilters = $derived(
  node && useNavigator
    ? axes.map((axis, i) => ({
        axis,
        hue: axisHueVar(i),
        values: axisValues(node, axis),
      }))
    : [],
);

// The active filter selection: axis → set of selected facet VALUES. An axis with
// an empty/absent set imposes no constraint (shows all its values) — so the
// initial all-empty state shows every member. Cleared whenever the loaded group's
// member set changes (navigated to a different group) so filters don't carry
// across groups.
let selected = $state<Record<string, Set<string>>>({});
const memberSig = $derived(
  node ? node.members.map((m) => memberKey(m)).join("|") : "",
);
let lastSig = "";
$effect(() => {
  if (memberSig !== lastSig) {
    lastSig = memberSig;
    selected = {};
  }
});

/** Toggle a facet value in an axis's filter set (multi-select within an axis). */
function toggleFilter(axis: string, value: string): void {
  const current = selected[axis] ?? new Set<string>();
  const next = new Set(current);
  if (next.has(value)) {
    next.delete(value);
  } else {
    next.add(value);
  }
  // Reassign the whole record so the `$state` proxy tracks the change.
  selected = { ...selected, [axis]: next };
}

/** Clear every axis filter (back to "show all members"). */
function clearFilters(): void {
  selected = {};
}

const anyFilterActive = $derived(
  Object.values(selected).some((s) => s.size > 0),
);

// The visible member set under the active filters: a member passes when, for
// EVERY axis with a non-empty selection, it carries a facet on that axis whose
// value is selected (AND across axes, OR within an axis). An axis with no
// selection imposes no constraint. Filter-only: this narrows the rendered list,
// never the project.
const visibleMembers = $derived(
  node && useNavigator
    ? node.members.filter((m) =>
        axes.every((axis) => {
          const sel = selected[axis];
          if (!sel || sel.size === 0) {
            return true;
          }
          return m.facets.some((f) => f.axis === axis && sel.has(f.value));
        }),
      )
    : [],
);

/** Whether a facet value is currently selected in its axis's filter. */
function isSelected(axis: string, value: string): boolean {
  return selected[axis]?.has(value) ?? false;
}

/** A member's facet on `axis` — the (value, label) the navigator pill renders,
 * or undefined when the member carries no facet there (a partial family member;
 * the pill is then omitted for that axis). */
function memberFacet(
  member: ConceptGroupNodeMember,
  axis: string,
): { value: string; label: string } | undefined {
  return member.facets.find((f) => f.axis === axis);
}

// ── The time axis: `?period` (client-side lens, no refetch) ──────────────────
const period = $derived(router.getQueryParam("period"));

/** A member's leaf URL, carrying the group page's active `?period` (when set) so
 * the member's leaf page opens narrowed to the SAME window — continuity into the
 * leaf, including its add-to-project plan (which keys off `?period`). Without
 * this, narrowing the group's availability lens then clicking a member would drop
 * the period and open the leaf at full history. Carries ONLY `?period` — the
 * group-page-specific `?member` focus hint is not consumed by the binding leaf.
 * `catalogHref` returns a query-less `/catalog/<path>`, so appending is safe. */
function memberHref(fqid: string): string {
  const href = catalogHref(fqid);
  if (!period) {
    return href;
  }
  const qs = new URLSearchParams();
  qs.set("period", period);
  return `${href}?${qs.toString()}`;
}
// Members are year-grain coverage, so the picker offers only the year grain.
const grains: PeriodGrain[] = ["year"];
// The picker's coverage track = the UNION span over all members (open-ended on
// the end when any member is still delivered) — so the slider shows where the
// group as a whole has data.
const unionCoverage = $derived<Coverage | null>(
  node ? memberCoverageUnion(node.members.map((m) => m.coverage)) : null,
);

/** Write `?period` to the group URL (preserving the pathname + any `?member=`
 * focus hint), which the reactive query picks up. A null period drops `?period`.
 * NO refetch — `getConceptGroup` takes no period; the value only drives the
 * client-side availability lens below (mirrors BindingLeafView's `?period` write,
 * but with only `period` + `member` to merge). */
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

// ── The availability lens (greying) ──────────────────────────────────────────
// The active window for greying, by precedence:
//   • an explicit `?period` (ANY form) is authoritative — a year `?period` →
//     that window; a NON-year `?period` (e.g. `HT2020`, a comma list, a deep
//     link) → `yearWindowFromWire` null → no greying. The year-grain lens can't
//     represent a sub-annual selection, so it suppresses greying rather than
//     silently falling back to the project window and greying against a window
//     the user isn't actually on (mirrors PeriodPicker treating such values as
//     `subAnnualPeriod` and suppressing slider availability gaps).
//   • no `?period` → the project window.
//   • neither → none (browsing the full group is not a deviation).
const activeWindow = $derived(
  period != null ? yearWindowFromWire(period) : windowStore.value,
);

// The open-ended coverage ceiling (#631): an open-ended member end projects to
// the catalog vintage year for the gap computation (the catalog only knows
// delivery up to its vintage), mirroring PeriodPicker's `ceilingYear` fallback
// and PeriodWindowSlider's `effectiveCoverage` projection.
const ceilingYear = $derived(vintageYear ?? new Date().getFullYear());

/** A member's coverage as a year-grain `Coverage` (open-ended end → null = "still
 * delivered"), or null when the member is stateless — reused for the per-member
 * availability gap. */
function memberCoverage(member: ConceptGroupNodeMember): Coverage | null {
  return memberCoverageUnion([member.coverage]);
}

/** Whether a member is NOT fully delivered across the active window — the
 * availability deviation (same rule as PeriodWindowSlider: a not-delivered gap
 * exists when the window extends before a finite coverage start or after the
 * coverage end, where an open-ended end is first projected to the catalog
 * vintage). False when no window is active, or the member is stateless (nothing
 * to gap against — don't grey a member whose coverage is simply unknown). */
function notDelivered(member: ConceptGroupNodeMember): boolean {
  if (activeWindow === null) {
    return false;
  }
  const cov = memberCoverage(member);
  if (cov === null) {
    return false;
  }
  // Project an open-ended member end (`to === null` = "still delivered") to the
  // catalog vintage ceiling for the gap test — the catalog only knows delivery
  // up to its vintage, so a window beyond it reads as "not delivered after
  // <vintage>" (mirrors PeriodWindowSlider's `effectiveCoverage`). Without this,
  // a null `to` never trailing-gaps and the member is wrongly never greyed for a
  // window past the vintage. The DISPLAYED coverage text stays open-ended ("since
  // <year>") — this projection feeds only the gap/greying computation.
  const projected = cov.to === null ? { from: cov.from, to: ceilingYear } : cov;
  return notDeliveredGaps(activeWindow, projected).length > 0;
}

/** The "not delivered <window>" note for a greyed member, or "" when delivered /
 * no active window. */
function notDeliveredNote(member: ConceptGroupNodeMember): string {
  if (!notDelivered(member) || activeWindow === null) {
    return "";
  }
  return `not delivered ${formatWindow(`${activeWindow.from}-01-01`, `${activeWindow.to}-12-31`)}`;
}
</script>

<nav class="breadcrumbs" aria-label="Breadcrumb">
  <a href="/catalog">{DATA_BROWSER_LABEL}</a>
  <span class="sep" aria-hidden="true">/</span>
  <a href={catalogHref(provider)}>{provider}</a>
  <span class="sep" aria-hidden="true">/</span>
  <a href={catalogHref(registerFqid)}>{register}</a>
  <span class="sep" aria-hidden="true">/</span>
  <span class="current">group/{key}</span>
</nav>

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
    <!-- The group's key, facets, and source are all build-derivation metadata, not
         researcher-facing — so all three are demoted together behind the "Technical
         details" disclosure. The page then leads with the title + member selector
         (mirrors the variable page, whose Technical details also lives here). -->
    <TechnicalDetails>
      <dl class="meta">
        <dt>Group</dt>
        <dd><code>{node.key}</code></dd>
        {#if node.axes.length > 0}
          <dt>Facets</dt>
          <dd>{node.axes.join(", ")}</dd>
        {/if}
        <dt>Source</dt>
        <dd>{node.source}</dd>
      </dl>
    </TechnicalDetails>
  {/snippet}

  <!-- One snippet per member, used by every selector shape. Renders the member as
       a link to its leaf FQID, with the `?member=` focus accent, per-member
       coverage, and the availability-lens greying + note. -->
  {#snippet memberLink(member: ConceptGroupNodeMember, label: string)}
    {@const slug = leafSlug(member.fqid)}
    {@const focused = node?.member === slug}
    {@const muted = notDelivered(member)}
    <a
      href={memberHref(member.fqid)}
      class:focused
      class:not-delivered={muted}
      title={member.fqid}
    >
      <span class="label">{label}</span>
      <!-- Show the leaf slug as a muted secondary code ONLY when it disambiguates
           — i.e. the label isn't already the slug (the matrix shape passes the
           slug as the label). For edge groups all members share one name, so the
           slug is the only thing that tells two rows apart (#638 PR2a regression). -->
      {#if label !== slug}
        <code class="member-slug muted">{slug}</code>
      {/if}
      {#if coverageText(member)}
        <span class="coverage muted">{coverageText(member)}</span>
      {/if}
      {#if notDeliveredNote(member)}
        <span class="availability muted">{notDeliveredNote(member)}</span>
      {/if}
    </a>
  {/snippet}

  <!-- A member's per-axis facet pills (#819 navigator): one hue-tinted pill per
       axis carrying the member's value label, so a multi-axis member reads as
       "one value per axis" at a glance. The hue is set per-axis off the
       categorical palette (axisHueVar); the shared `.facet-pill` rule mixes its
       tint/border/ink off `--axis-hue` (mirrors the Tag primitive). An axis the
       member lacks a facet on is omitted (a partial family). -->
  {#snippet memberPills(member: ConceptGroupNodeMember)}
    <span class="facet-pills">
      {#each axisFilters as { axis, hue } (axis)}
        {@const facet = memberFacet(member, axis)}
        {#if facet}
          <span class="facet-pill" style="--axis-hue: {hue};" title={axis}>
            {facet.label}
          </span>
        {/if}
      {/each}
    </span>
  {/snippet}

  {#snippet picker()}
    <!-- The MEMBER SELECTOR (slice axis): the group's facet grid, expanded. -->
    <section class="member-selector" aria-labelledby="members-heading">
      <h3 id="members-heading">Members</h3>
      {#if useNavigator}
        <!-- The N-axis facet navigator (#819): per-axis filter controls + a
             filtered member list, each member carrying its per-axis hue pills.
             Filter-only — narrows the list, never the project. -->
        <div class="facet-filters" role="group" aria-label="Filter members by facet">
          {#each axisFilters as { axis, hue, values } (axis)}
            <fieldset class="axis-filter" style="--axis-hue: {hue};">
              <legend>{axis}</legend>
              <div class="filter-options">
                {#each values as v (v.value)}
                  <label class="filter-pill" class:on={isSelected(axis, v.value)}>
                    <input
                      type="checkbox"
                      checked={isSelected(axis, v.value)}
                      onchange={() => toggleFilter(axis, v.value)}
                    />
                    <span>{v.label}</span>
                  </label>
                {/each}
              </div>
            </fieldset>
          {/each}
          {#if anyFilterActive}
            <button type="button" class="clear-filters" onclick={clearFilters}>
              Clear filters
            </button>
          {/if}
        </div>
        <p class="member-count muted" aria-live="polite">
          Showing {visibleMembers.length} of {node.members.length} members
        </p>
        {#if visibleMembers.length === 0}
          <p class="muted no-members">No members match the active filters.</p>
        {:else}
          <ul class="members navigator">
            {#each visibleMembers as member (memberKey(member))}
              <li>
                {@render memberPills(member)}
                {@render memberLink(member, member.name ?? leafSlug(member.fqid))}
              </li>
            {/each}
          </ul>
        {/if}
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
                  {@const member = memberAt(node, [
                    { axis: axes[0], value: row.value },
                    { axis: axes[1], value: col.value },
                  ])}
                  <td>
                    {#if member}
                      {@render memberLink(member, leafSlug(member.fqid))}
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
              <li>
                {@render memberLink(
                  member,
                  member.name ?? leafSlug(member.fqid),
                )}
              </li>
            {/each}
          </ul>
        {/if}
      {:else if axes.length === 1}
        <ul class="facet-chips">
          {#each node.members as member (memberKey(member))}
            {@const facet = member.facets.find((f) => f.axis === axes[0])}
            <li>
              {@render memberLink(
                member,
                facet?.label ?? leafSlug(member.fqid),
              )}
            </li>
          {/each}
        </ul>
      {:else}
        <ul class="members">
          {#each node.members as member (memberKey(member))}
            <li>
              {@render memberLink(member, member.name ?? leafSlug(member.fqid))}
            </li>
          {/each}
        </ul>
      {/if}
    </section>

    <!-- The TIME axis: a client-side availability LENS (no refetch). Seeds from
         the project window, draws the members' union coverage span, and greys the
         members above whose coverage doesn't span the chosen window. The period
         writes to `?period` only (never the global window), mirroring the leaf
         page; `getConceptGroup` ignores it, so the lens is purely client-side. -->
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
  .breadcrumbs {
    font-size: 0.9rem;
    margin-bottom: 1rem;
  }
  .breadcrumbs .sep {
    color: var(--muted);
    margin: 0 0.25rem;
  }
  .breadcrumbs .current {
    color: var(--muted);
  }
  /* #638 PR4: row spacing standardized to 0.3rem across the three subject kinds. */
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.3rem 1rem;
    margin: 1rem 0;
  }
  .meta dt {
    font-weight: 600;
  }
  /* The member selector shapes mirror ConceptGroupRow's matrix / chips / list
     vocabulary (copied, not imported — scoped styles don't cross components),
     rendered expanded (no <details>) and richer (coverage + availability). */
  .members {
    list-style: none;
    padding: 0;
    margin: 0.5rem 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .facet-matrix {
    margin: 0.5rem 0;
    border-collapse: collapse;
    font-size: 0.9em;
  }
  .facet-matrix th,
  .facet-matrix td {
    padding: 0.25rem 0.6rem;
    text-align: left;
    vertical-align: baseline;
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
    gap: 0.5rem;
    padding: 0;
    margin: 0.5rem 0;
  }
  /* A member link: name/label + coverage + availability note. In the chips row
     each becomes a bordered pill; in the matrix/list it's an inline-flex link. */
  .member-selector a {
    display: inline-flex;
    align-items: baseline;
    flex-wrap: wrap;
    gap: 0.25rem 0.6rem;
    text-decoration: none;
  }
  .facet-chips a {
    border: 1px solid var(--muted);
    border-radius: 1rem;
    padding: 0.1rem 0.6rem;
  }
  .member-selector a .label {
    font-weight: 600;
  }
  /* The `?member=` focus hint: a faint left accent rule so a deep-linked member
     reads as highlighted without a heavy box. */
  .member-selector a.focused {
    border-left: 3px solid var(--accent);
    padding-left: 0.5rem;
  }
  /* The availability lens: a member whose coverage doesn't span the active
     window is muted + carries a "not delivered <window>" note. */
  .member-selector a.not-delivered {
    opacity: 0.5;
  }
  .coverage,
  .availability {
    font-size: 0.85em;
  }
  /* The disambiguating leaf slug (mirrors ConceptGroupRow's `.member-slug`): a
     muted monospace code that tells same-named edge-group members apart. */
  .member-slug {
    font-size: 0.85em;
  }

  /* ── The N-axis facet navigator (#819) ──────────────────────────────────────
     Per-axis filter blocks + a filtered member list. Each axis is tinted a
     distinct categorical hue (set per-fieldset / per-pill as `--axis-hue` off the
     `--cat-*` palette); the shared rules below mix that hue into the
     border/fill/ink (mirrors the Tag primitive's `--tone-hue` mechanism), so no
     new color tokens are introduced. */
  .facet-filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-3);
    align-items: flex-start;
    margin: var(--space-2) 0;
  }
  .axis-filter {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-2) var(--space-3) var(--space-3);
    margin: 0;
    min-inline-size: 0;
  }
  .axis-filter legend {
    color: color-mix(in srgb, var(--axis-hue) 85%, black);
    font-weight: 600;
    font-size: var(--text-sm);
    padding: 0 var(--space-1);
    text-transform: capitalize;
  }
  .filter-options {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-1);
  }
  /* A filter pill: a checkbox styled as a selectable hue chip. The native input
     is visually hidden but kept in the DOM (a11y / keyboard / labelled), and the
     `.on` class paints the selected state off `--axis-hue`. */
  .filter-pill {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    padding: 0.1rem 0.55rem;
    border: 1px solid color-mix(in srgb, var(--axis-hue) 35%, transparent);
    border-radius: 1rem;
    font-size: var(--text-sm);
    cursor: pointer;
    user-select: none;
    background: var(--surface);
    color: var(--text);
  }
  .filter-pill input {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
    border: 0;
  }
  .filter-pill.on {
    background: color-mix(in srgb, var(--axis-hue) 18%, var(--surface));
    border-color: color-mix(in srgb, var(--axis-hue) 55%, transparent);
    color: color-mix(in srgb, var(--axis-hue) 85%, black);
    font-weight: 600;
  }
  /* Keyboard focus ring on the (hidden) input projects onto its pill label. */
  .filter-pill:focus-within {
    box-shadow: var(--focus-ring);
  }
  .clear-filters {
    align-self: center;
    background: none;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    padding: 0.2rem 0.6rem;
    font: inherit;
    font-size: var(--text-sm);
    color: var(--text-muted);
    cursor: pointer;
  }
  .clear-filters:hover {
    color: var(--text);
    border-color: var(--border-strong);
  }
  .member-count {
    font-size: var(--text-sm);
    margin: var(--space-2) 0 var(--space-1);
  }
  .no-members {
    margin: var(--space-2) 0;
  }
  /* A navigator member row: its per-axis hue pills, then the member link. */
  .members.navigator > li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.25rem 0.6rem;
  }
  .facet-pills {
    display: inline-flex;
    flex-wrap: wrap;
    gap: var(--space-1);
  }
  /* A read-only member facet pill (one per axis): the value label tinted by the
     axis hue — same mix recipe as the Tag primitive's categorical tones. */
  .facet-pill {
    display: inline-flex;
    align-items: center;
    padding: 0.05rem 0.45rem;
    border-radius: var(--radius-sm);
    font-size: var(--text-sm);
    color: color-mix(in srgb, var(--axis-hue) 85%, black);
    border: 1px solid color-mix(in srgb, var(--axis-hue) 35%, transparent);
    background: color-mix(in srgb, var(--axis-hue) 10%, var(--surface));
    white-space: nowrap;
  }
</style>
