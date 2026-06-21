<script lang="ts">
import { type ConceptGroupNodeMember, getConceptGroup } from "./api";
import { asyncResource } from "./async.svelte";
import {
  axisValues,
  catalogHref,
  formatWindow,
  memberAt,
  memberCoverageUnion,
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
import { windowStore } from "./window.svelte";

// The concept-group SUBJECT page (#617): fetches a group by (provider, register,
// key) and renders its members + facets + per-member coverage. A group's default
// selection is "all members", which a member FQID can't express — so the group
// has its own address (`/catalog/group/<p>/<r>/<key>`), distinct from the FQID
// browse path that CatalogNodeView serves.
//
// Renders through the unified SubjectView shell (#638 PR1), same as the binding +
// classification leaves. A group fills two of the shell's sections — the meta
// `description` (key/source + facets) and a `picker` (#638 PR2a, below). It has no
// single fqid (its key shows inside the meta), no value set, no docs, and (post
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
 * member carries none (a stateless member). Delegates to `formatWindow` so the
 * display matches the rest of the catalog (year-collapsed bounds; the open-ended
 * sentinel renders "since <year>", never the raw 9999). */
function coverageText(member: ConceptGroupNodeMember): string {
  const cov = member.coverage;
  // The yearless-fallback floor (`0001-01-01`) is "start unknown", not year 1 —
  // treat it like a null start (mirrors `coverageFromStates`/`memberCoverageUnion`),
  // else `formatWindow` would render a bogus "1 – <to>" window.
  if (
    !cov ||
    cov.coverage_from == null ||
    cov.coverage_from === YEARLESS_VALID_FROM
  ) {
    return "";
  }
  // `_coverage_bounds` contract: a finite window carries a non-null `coverage_to`;
  // open-ended maps to the sentinel. The null guard is defensive (shouldn't fire).
  const to = cov.open_ended ? OPEN_ENDED_VALID_TO : cov.coverage_to;
  if (to == null) {
    return "";
  }
  return formatWindow(cov.coverage_from, to);
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
  node && axes.length >= 2
    ? node.members.filter(
        (m) => !axes.every((axis) => m.facets.some((f) => f.axis === axis)),
      )
    : [],
);

// ── The time axis: `?period` (client-side lens, no refetch) ──────────────────
const period = $derived(router.getQueryParam("period"));
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
  <a href="/catalog">catalog</a>
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
    <dl class="meta">
      <dt>Group</dt>
      <dd><code>{node.key}</code> · {node.source}</dd>
      {#if node.axes.length > 0}
        <dt>Facets</dt>
        <dd>{node.axes.join(", ")}</dd>
      {/if}
    </dl>
  {/snippet}

  <!-- One snippet per member, used by every selector shape. Renders the member as
       a link to its leaf FQID, with the `?member=` focus accent, per-member
       coverage, and the availability-lens greying + note. -->
  {#snippet memberLink(member: ConceptGroupNodeMember, label: string)}
    {@const slug = leafSlug(member.fqid)}
    {@const focused = node?.member === slug}
    {@const muted = notDelivered(member)}
    <a
      href={catalogHref(member.fqid)}
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

  {#snippet picker()}
    <!-- The MEMBER SELECTOR (slice axis): the group's facet grid, expanded. -->
    <section class="member-selector" aria-labelledby="members-heading">
      <h3 id="members-heading">Members</h3>
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
            {#each ungridded as member (member.fqid)}
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
          {#each node.members as member (member.fqid)}
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
          {#each node.members as member (member.fqid)}
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
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.35rem 1rem;
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
</style>
