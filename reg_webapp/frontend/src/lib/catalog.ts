/**
 * Display + FQID-path helpers for the catalog browse UI (`catalog.ts`):
 * a node's label, and the `/catalog/<fqid-path>` URL ↔ FQID-segment helpers
 * (the SPA routes mirror the API path; see reg_webapp/DESIGN.md → Catalog router
 * structure).
 */
import {
  type BindingChild,
  type BindingGroupRef,
  type CatalogNode,
  type ConceptGroup,
  classificationGroupPath,
  conceptGroupPath,
  encodeFqid,
  type GroupAxisModel,
  type GroupFacetModel,
  getCatalogNode,
  isCatalogNode,
  type StatesResponse,
  type VariableGraphNode,
  type VariableStateModel,
} from "./api";
import {
  type Coverage,
  grainOfToken,
  PERIOD_GRAINS,
  type PeriodGrain,
  periodRangeEndpoints,
  periodTokenBounds,
  periodTokenForBounds,
  periodWireBounds,
  VALUE_SET_VERSION_NONE,
} from "./period";
import type { Route } from "./router.svelte";
import type { BreadcrumbItem } from "./ui/types";

/** The user-facing label for the data-browser root (the `/catalog` URL is
 * unchanged — this is the LABEL only, #675). Shared by the App nav link and the
 * root breadcrumb crumb in `CatalogNodeView` / `ConceptGroupView` so the three
 * stay in sync. (The route path stays `/catalog`; only the displayed text reads
 * "Data browser".) */
export const DATA_BROWSER_LABEL = "Data browser";

/** Narrow the catch-all browse response to a browsable `CatalogNode`, or `null`
 * for a no-`kind` payload (a `?period` `StatesResponse` or a sub-endpoint
 * envelope) — the boundary every browse consumer narrows at before switching on
 * `kind`. */
export function narrowCatalogNode(
  data: CatalogNode | StatesResponse | null,
): CatalogNode | null {
  return data !== null && isCatalogNode(data) ? data : null;
}

/** The binding children of a register node, in order. `[]` for any other node
 * kind — a register's `children` mix binding entries with a `VariantsRef`, so
 * callers want only the `kind === "binding"` ones (the pickable / browsable
 * variable list). */
export function bindingChildren(node: CatalogNode): BindingChild[] {
  if (node.kind !== "register") {
    return [];
  }
  return node.children.filter((c): c is BindingChild => c.kind === "binding");
}

// ── Concept-group folding (#303) ────────────────────────────────────────────
// A register / classification-root node carries derived, PRESENTATION-ONLY
// concept `groups` alongside its complete flat `children`. The browse folds:
// grouped members render under one expandable group row; ungrouped children
// stay leaf rows. Members carry the real leaf FQIDs.

/** One browse row after concept-group folding: a group row or an ungrouped
 * leaf. */
export type GroupedRow<T> =
  | { kind: "group"; group: ConceptGroup }
  | { kind: "leaf"; item: T };

/** Fold `items` (a node's flat children) under `groups`, PRESERVING the
 * incoming item order (the API's — e.g. classifications arrive short_name-
 * ordered): ungrouped items stay leaf rows in place; a group row replaces its
 * FIRST member's position and the remaining members are hidden. A group with
 * no member in `items` (shouldn't happen — the flat children list is
 * complete) is appended so it can't silently vanish. */
export function foldGroupedRows<T extends { fqid: string }>(
  items: T[],
  groups: ConceptGroup[] | undefined,
): GroupedRow<T>[] {
  // `groups` is required on the wire TYPE, but tolerate its absence at
  // runtime: /api/catalog/* responses are cached `max-age=60` (since #499),
  // and an edge cache-generation predating a deploy (bounded by `__edge_v`
  // busting) or a browser copy within the 60s window can still be stale, so
  // right after a deploy that ADDS a field the new SPA can receive a stale
  // pre-`groups` payload. Degrade to the flat ungrouped list rather than
  // crash the browse (bit prod on the #303 rollout, 2026-06-11).
  const safeGroups = groups ?? [];
  const groupOf = new Map<string, ConceptGroup>();
  for (const g of safeGroups) {
    for (const m of g.members) {
      groupOf.set(m.fqid, g);
    }
  }
  const rows: GroupedRow<T>[] = [];
  const emitted = new Set<ConceptGroup>();
  for (const item of items) {
    const group = groupOf.get(item.fqid);
    if (!group) {
      rows.push({ kind: "leaf", item });
    } else if (!emitted.has(group)) {
      emitted.add(group);
      rows.push({ kind: "group", group });
    }
  }
  for (const group of safeGroups) {
    if (!emitted.has(group)) {
      rows.push({ kind: "group", group });
    }
  }
  return rows;
}

/** The DISTINCT-variable count a folded row list represents — group rows count
 * their distinct member FQIDs, leaves count 1. Keeps the "N variables" readout in
 * VARIABLE units after folding (a register whose 36 variables fold into one matrix
 * row still reports 36, not 1). #819: a representation group can carry several
 * members on ONE variable (one `fqid`, distinct `delivery_column`s — e.g. CDISP +
 * CDISP5), so the variable count dedups by `fqid` rather than counting raw member
 * rows (53 representation rows over ~36 variables must read as 36 variables). */
export function countFoldedMembers<T>(rows: GroupedRow<T>[]): number {
  return rows.reduce(
    (n, row) =>
      n + (row.kind === "group" ? distinctMemberCount(row.group.members) : 1),
    0,
  );
}

/** The number of DISTINCT variables a group's members address — its members
 * deduped by `fqid` (the variable identity). #819: representation members share
 * one `fqid` across delivery columns, so raw `members.length` overstates the
 * variable count; this is the variable-unit count for the "N variables" readouts. */
export function distinctMemberCount(
  members: readonly { fqid: string }[],
): number {
  return new Set(members.map((m) => m.fqid)).size;
}

/** The filterable text of a group row: its own label/key plus every member's
 * name/FQID/leaf slug — so filtering for a member (e.g. "maj") still surfaces
 * the group that folded it, AND a member-slug hunt (e.g. "inkjan" for
 * `scb/lisa/inkjan`) ranks the folding group at exact/prefix tier rather than as
 * an "other substring" — consistent with leaf rows ranking by their `leafSlug`
 * (#674; the member's leaf slug is a substring of its FQID, so the match SET is
 * unchanged, only the `rankFilter` tier improves). One source of truth for the
 * browse type-to-filter and the pickers' `rankFilter` keys over a group row
 * (#322). */
export function groupFilterKeys(
  group: ConceptGroup,
): (string | null | undefined)[] {
  return [
    group.label,
    group.key,
    ...group.members.flatMap((m) => [
      m.name,
      m.fqid,
      leafSlug(m.fqid),
      // #819 FIX D: representation members are distinguished by their delivery
      // column + facet labels/values (e.g. `CDISP5`, "Exkl. kapitalvinst"), not by
      // name/fqid (which they SHARE across the variable). Index those too so a
      // target-hunt for a column or a facet label surfaces the folding group.
      m.delivery_column,
      ...m.facets.flatMap((f) => [f.label, f.value]),
    ]),
  ];
}

// `axisValues`/`memberAt` are GENERIC over the member type (#638 PR2a): the
// register-browse `ConceptGroupRow` passes a `ConceptGroup` (members lack
// coverage), the group SUBJECT page passes a `ConceptGroupNodeData` (members ADD
// `coverage`). Both shapes carry `members: { facets: GroupFacetModel[] }[]`, and
// these helpers read ONLY `.facets`, so the minimal constraint is enough — and
// `memberAt` preserves the caller's member type so the subject page gets its
// `coverage` field back on the matched cell.

/** A group member as far as the facet-grid helpers care: just its facets. */
type FacetedMember = { facets: GroupFacetModel[] };

/** The stable `{#each}` key for a group member (#819). An FQID is NO LONGER
 * unique within a group: a multi-axis family can carry two members on ONE
 * variable (two delivery columns), so keying `{#each ... (m.fqid)}` would throw a
 * duplicate-key error / drop the second representation. The composite
 * `(fqid, delivery_column)` is unique — `delivery_column` is null for a
 * whole-variable member and the SCB column for a representation member, so the
 * pair distinguishes the two columns of one variable. The `::` separator can't
 * collide: an FQID is slash-separated and a delivery column is a bare SQL
 * identifier, neither contains `::`. */
export function memberKey(m: {
  fqid: string;
  delivery_column?: string | null;
}): string {
  return `${m.fqid}::${m.delivery_column ?? ""}`;
}

/** The distinct (value, label) pairs a group's members carry on `axis`,
 * value-sorted — the rows/columns of the facet picker. */
export function axisValues(
  group: { members: readonly FacetedMember[] },
  axis: string,
): { value: string; label: string }[] {
  const seen = new Map<string, string>();
  for (const m of group.members) {
    const facet = m.facets.find((f) => f.axis === axis);
    if (facet && !seen.has(facet.value)) {
      seen.set(facet.value, facet.label);
    }
  }
  return [...seen.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.value.localeCompare(b.value));
}

/** The member at a facet-coordinate (one value per axis, in `axes` order), or
 * undefined for an empty cell (partial families: a missing month/vintage).
 * Generic over `M` so the matched member keeps its concrete type (the subject
 * page's `coverage` survives). */
export function memberAt<M extends FacetedMember>(
  group: { members: readonly M[] },
  coords: { axis: string; value: string }[],
): M | undefined {
  return group.members.find((m) =>
    coords.every((c) =>
      m.facets.some((f) => f.axis === c.axis && f.value === c.value),
    ),
  );
}

/** A member's facet on `axis` — the (value, label) the N-axis navigator pill
 * renders (#819), or undefined when the member carries no facet there (a partial
 * family; the pill is then omitted for that axis). Hoisted here (out of
 * ConceptGroupView) so the shared ConceptGroupNavigator reuses it. */
export function memberFacet(
  member: FacetedMember,
  axis: string,
): GroupFacetModel | undefined {
  return member.facets.find((f) => f.axis === axis);
}

/** Whether every member of a group occupies a DISTINCT facet-coordinate tuple
 * (one value per axis, in `axes` order; a missing facet is its own slot). #819:
 * the 2D matrix renders only ONE member per (row, col) cell — `memberAt` returns
 * the first match — so two members sharing a full coordinate vector silently DROP
 * one (and they escape `ungridded`, which catches only members MISSING an axis,
 * not collisions on present ones). The schema permits representation members
 * (`delivery_column`-distinguished, same coords) for ANY axes length > 1, so a
 * 2-axis group can collide too. The host uses this to route a colliding group
 * through the no-member-dropped navigator instead of the matrix. Coordinate-only
 * (NOT delivery-column): the navigator lists every member regardless, so the test
 * is purely "would the matrix lose a member". */
export function membersHaveUniqueCoords(
  group: { members: readonly FacetedMember[] },
  axes: readonly GroupAxisModel[],
): boolean {
  const seen = new Set<string>();
  for (const m of group.members) {
    // Join the per-axis values with a control-char separator (and a distinct
    // control-char marker for an absent facet) so two DIFFERENT coordinate
    // vectors can never alias by concatenation — facet values are SCB
    // codes/tokens, never control characters.
    const coords = axes
      .map(
        (axis) => m.facets.find((f) => f.axis === axis.name)?.value ?? "\u0000",
      )
      .join("\u0001");
    if (seen.has(coords)) {
      return false;
    }
    seen.add(coords);
  }
  return true;
}

// ── Member-distinguishing qualifier (#670, graph-sourced #678) ───────────────
// A grouped binding leaf (`scb/lisa/agi1astsni2007g`) shares its concept
// `node.name` with ~31 siblings, so the header alone can't tell members apart.
// The #670 header qualifier + "member of ⟨group⟩" link now derive from the
// relationship-graph FOCUS node (the `VariableGraphNode` whose `id === focus_id`)
// instead of the retired `/dimensions` fetch (#678): the focus node already
// carries the member's `facets` (axis:value:label) and `group_label`, so the
// leaf's single graph fetch feeds both the renderer AND the header — no second
// request. Unlike the old `/dimensions` form there is no MULTI-group canonical
// ordering: the graph focus carries ONE group membership (the member's own
// register group), so the facets are unambiguous.

/** A member-distinguishing qualifier and whether it is the facet-label form or
 * the slug fallback — the discriminant the caller styles on (`facets` → a human
 * label `<span>`, `slug` → a technical-identifier `<code>`). */
export type MemberQualifier = { text: string; kind: "facets" | "slug" };

/** Join a faceted member's facet labels into one display string (" · "-
 * separated) — the single home for the separator/ordering, shared by the header
 * qualifier (`qualifierFromFocus`) and the HistoryGraph member-lane label. */
export function facetLabelJoin(facets: { label: string }[]): string {
  return facets.map((f) => f.label).join(" · ");
}

/** The member-distinguishing qualifier from the graph FOCUS node (#678): the
 * focus's facet labels joined with " · " (e.g. "AGI · 2007 SNI edition") when it
 * carries facets; else, for a GROUPED member with no facets (an edge group's
 * split siblings — `group_label` set, `facets: []`), the leaf slug fallback
 * (`kind: "slug"`), since the slug is the only differentiator between those
 * siblings; else `null` for an UNGROUPED variable (its `node.name` suffices).
 *
 * `fqid` is the LEAF's own fqid — only a fallback for the focus node's `fqid`,
 * which is itself the slug-fallback source. The focus node's `fqid` is the
 * member's CANONICAL identity (post-same_as), so the slug fallback prefers it
 * (`focus.fqid ?? fqid`) — the alias page and the canonical page then show the
 * SAME technical identifier (#670 Codex-P2 parity, preserved from the retired
 * `memberQualifier`). The discriminated `kind` lets the caller pick a
 * presentation (facet label vs. mono code) without re-scanning. */
export function qualifierFromFocus(
  focus: VariableGraphNode | null | undefined,
  fqid: string,
): MemberQualifier | null {
  if (!focus) {
    return null;
  }
  if (focus.facets.length > 0) {
    return { text: facetLabelJoin(focus.facets), kind: "facets" };
  }
  // Grouped (a group label) but facet-less → the CANONICAL leaf slug distinguishes
  // the edge-group split siblings. Prefer the focus node's own (canonical) fqid so
  // an alias page shows the canonical sibling slug, not its alias; fall back to the
  // leaf arg when the focus carries no fqid. Ungrouped (no group label) → null.
  return focus.group_label != null
    ? { text: leafSlug(focus.fqid ?? fqid), kind: "slug" }
    : null;
}

/** The "member of ⟨group label⟩" context link from the graph FOCUS node + the
 * leaf's `node.group` ref (#678). The label comes from the focus node's
 * `group_label`; the HREF still comes from the leaf `BindingGroupRef`
 * (provider/register/key) — that's the authoritative group-subject coordinate,
 * already resolved server-side. `null` when ungrouped (no `group_label`) or when
 * the leaf carries no group ref. */
export function groupLinkFromFocus(
  focus: VariableGraphNode | null | undefined,
  ref: BindingGroupRef | null | undefined,
): { label: string; href: string } | null {
  if (!focus || focus.group_label == null || !ref) {
    return null;
  }
  return {
    label: focus.group_label,
    href: groupHref(`${ref.provider}/${ref.register}`, ref.key),
  };
}

/** A node's display label — its `name` when present, else its FQID (providers
 * and registers carry an optional `name`; classifications carry a required
 * `name`; the classification-root carries a default `name`). The concept-group
 * SUBJECT (#617) is NOT a `CatalogNode` arm — it's served by the fixed
 * `/catalog/group/...` route and labelled directly off its `label` in
 * `ConceptGroupView`, so it never reaches this catch-all labeller. */
export function nodeLabel(node: CatalogNode): string {
  if (node.kind === "classification-root" || node.kind === "classification") {
    return node.name;
  }
  return node.name ?? node.fqid;
}

/** The plural noun for a concept group's member count, derived from its single
 * facet axis (e.g. "dimension" → "dimensions"). Variable groups still derive the
 * noun from their single axis; classification umbrella groups are axis-less (#516),
 * so they hit the no-axis fallback and render as "members". Naive +"s" is enough
 * for the axis vocabulary (dimension, month, …); a group with no axis falls back
 * to "members". */
export function axisNoun(axes: readonly GroupAxisModel[]): string {
  // The noun stem is the stable axis NAME ("dimension"/"month"), not the display
  // label — the +"s" plural rule is tuned to the name vocabulary (#819).
  const axis = axes[0]?.name;
  return axis ? `${axis}s` : "members";
}

// ── Type-to-filter (catalog browse + pickers) ───────────────────────────────
// The catalog/authoring lists render at scale (238 registers, 740 variables) —
// every list surface needs an in-memory substring filter. One shared matcher so
// all four surfaces fold identically: NFD-decompose + strip combining marks +
// lowercase, so "lon" matches both "Löne…" and "lön" (diacritic-blind), and the
// needle is matched against BOTH display name AND slug/FQID.

/** Fold a string for diacritic-blind, case-insensitive substring matching:
 * NFD-normalize then strip combining marks (U+0300–U+036F) and lowercase. */
export function foldText(s: string): string {
  return s
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "") // strip combining diacritical marks
    .toLowerCase();
}

/** Whether any of `haystacks` contains the folded `needle` (substring). An empty
 * (or whitespace-only) needle matches everything — the unfiltered full-list
 * behavior. `null`/`undefined` haystacks are skipped. */
export function matchesFilter(
  needle: string,
  ...haystacks: (string | null | undefined)[]
): boolean {
  const q = foldText(needle).trim();
  if (!q) {
    return true;
  }
  return haystacks.some((h) => h != null && foldText(h).includes(q));
}

/** Filter `items` by `matchesFilter` over `keys(item)`, THEN rank the survivors
 * for target-hunting: (1) folded-exact key match, (2) folded-prefix key match,
 * (3) other substring matches — each tier keeping the input order (a STABLE
 * sort, so the caller's existing alphabetical order survives within a tier).
 * Used by the PICKERS (where the user hunts a specific row: "kon" → Kön first)
 * AND by the browse children lists (registers + variables, #674): under an
 * active filter the browse ranks the same way (a slug-named target jumps above a
 * purpose-blurb-only match — see `leafSlug`), so typing the target's slug
 * surfaces it first. An empty needle returns the matched list unchanged (every
 * row is tier 3, stable), so the UNFILTERED browse list keeps its incoming
 * (alphabetical) order. */
export function rankFilter<T>(
  items: T[],
  needle: string,
  keys: (item: T) => (string | null | undefined)[],
): T[] {
  const q = foldText(needle).trim();
  const matched = items.filter((it) => matchesFilter(needle, ...keys(it)));
  if (!q) {
    return matched;
  }
  // 0 = exact, 1 = prefix, 2 = other. `Array.prototype.sort` is stable, so
  // equal-tier rows keep their incoming (alphabetical) order.
  const tier = (it: T): number => {
    const folded = keys(it).map((k) => (k == null ? null : foldText(k)));
    if (folded.some((f) => f === q)) {
      return 0;
    }
    if (folded.some((f) => f?.startsWith(q))) {
      return 1;
    }
    return 2;
  };
  return matched
    .map((it, i) => ({ it, i, t: tier(it) }))
    .sort((a, b) => a.t - b.t || a.i - b.i)
    .map((e) => e.it);
}

// ── FQID path helpers ───────────────────────────────────────────────────────
// The SPA routes mirror the API: `/catalog/<fqid-path>`. These split/join the
// path portion (after `/catalog`) into FQID segments.

/** The href for a catalog node, mirroring the API path. Segments are
 * percent-encoded (same as the API path) so a reserved/non-ASCII char in an
 * FQID can't produce a malformed URL — a no-op for today's ASCII slugs. `class`
 * and `class/<slug>` are valid FQID paths (the classification axis). */
export function catalogHref(fqidPath: string): string {
  return fqidPath ? `/catalog/${encodeFqid(fqidPath)}` : "/catalog";
}

/** The href for a concept-group SUBJECT page (#617): the fixed
 * `/catalog/group/<provider>/<register>/<key>` route, NOT the `/catalog/<fqid>`
 * browse path. `registerFqid` is the 2-seg `provider/register` of the browsing
 * register; `key` is the group's derivation key. Each segment is percent-encoded
 * the same way `catalogHref` encodes (per-segment `encodeURIComponent`), so a
 * reserved/non-ASCII char in a key can't produce a malformed URL — a no-op for
 * today's ASCII slugs/keys. The route is REGISTER-only (the backend validates
 * provider/register as a register FQID), so only the register-arm browse links
 * to it; classification-umbrella groups have no group page (#673). */
export function groupHref(registerFqid: string, key: string): string {
  const [provider, register] = fqidSegments(registerFqid);
  return conceptGroupPath(provider ?? "", register ?? "", key);
}

/** The href for a classification-umbrella SUBJECT page (#756): the fixed
 * `/catalog/group/class/<key>` route — the classification sibling of `groupHref`.
 * A classification umbrella is catalog-global (no provider/register), so it takes
 * only the group's derivation `key`. The classification-root browse arm links to
 * it so the umbrella group gets a first-class subject page like the register
 * groups do (#673 left it inline; #756 gives it the route). */
export function classGroupHref(key: string): string {
  return classificationGroupPath(key);
}

/** Segments of an FQID path (`scb/lisa/kon` → `["scb", "lisa", "kon"]`).
 * Empty string → `[]` (the root). */
export function fqidSegments(fqidPath: string): string[] {
  return fqidPath ? fqidPath.split("/") : [];
}

/** The leaf slug of an FQID — its last `/`-separated segment (`scb/rtb` →
 * `"rtb"`, a bare `"rtb"` → `"rtb"`). A ranking key for the browse type-to-filter
 * (`rankFilter`): the leaf slug exact-/prefix-matches the needle even though the
 * full FQID's provider prefix would block it (`foldText("scb/rtb")` doesn't start
 * with `"rtb"`), so a slug-named target (`scb/rtb` for "rtb") outranks a purpose-
 * blurb-only match (#674). */
export function leafSlug(fqid: string): string {
  return fqid.split("/").at(-1) ?? fqid;
}

/** Breadcrumb trail for an FQID path: each ancestor + the node itself, as
 * `{label, fqidPath}` pairs. `["scb","lisa","kon"]` →
 * `[{scb, "scb"}, {lisa, "scb/lisa"}, {kon, "scb/lisa/kon"}]`. The label is the
 * raw segment (the resolved `name` isn't known for ancestors without a fetch). */
export function breadcrumbs(
  fqidPath: string,
): { label: string; fqidPath: string }[] {
  const segs = fqidSegments(fqidPath);
  return segs.map((label, i) => ({
    label,
    fqidPath: segs.slice(0, i + 1).join("/"),
  }));
}

/** The topbar breadcrumb trail for a route (#803), as `Breadcrumbs` items (the
 * last item is the current page — no `href`). STRUCTURAL only: labels are the
 * raw slug segments (the routed page owns its rich, display-name header) and the
 * data-browser root leads every catalog trail. `home` is a single un-linked
 * "Home"; the non-catalog routes get a one- or two-crumb trail rooted where they
 * sit. A `catalog-node` splits its FQID path into cumulative `/catalog/...`
 * crumbs; the group routes prepend the browser root + a "Groups" hop so the trail
 * still descends from the data browser. */
export function routeBreadcrumbs(route: Route): BreadcrumbItem[] {
  const browserRoot: BreadcrumbItem = {
    label: DATA_BROWSER_LABEL,
    href: catalogHref(""),
  };
  switch (route.name) {
    case "home":
      return [{ label: "Home" }];
    case "root":
      // The data browser IS the current page — a single un-linked crumb.
      return [{ label: DATA_BROWSER_LABEL }];
    case "catalog-node": {
      // Each ancestor segment is a link to its cumulative `/catalog/...` path;
      // the leaf segment is the current page (no href).
      const trail = breadcrumbs(route.fqidPath);
      return [
        browserRoot,
        ...trail.map((c, i) => ({
          label: c.label,
          href: i < trail.length - 1 ? catalogHref(c.fqidPath) : undefined,
        })),
      ];
    }
    case "group":
      // Split provider and register into separate, individually-linked crumbs
      // (#887) — same per-segment idiom as `catalog-node`; the group key is the
      // current page (no href).
      return [
        browserRoot,
        { label: route.provider, href: catalogHref(route.provider) },
        {
          label: route.register,
          href: catalogHref(`${route.provider}/${route.register}`),
        },
        { label: route.key },
      ];
    case "class-group":
      return [
        browserRoot,
        { label: "class", href: catalogHref("class") },
        { label: route.key },
      ];
    case "search":
      return [{ label: "Search" }];
    case "project":
      return [{ label: "Project" }];
    case "doc":
      return [{ label: "Docs" }, { label: route.identifier }];
    default:
      // not-found
      return [browserRoot];
  }
}

// ── register_variant coordinate helpers ──────────────────────────────────────
// A Source.register_variant is a 3-seg coordinate `provider/register/variant`
// (see reg_schema/DESIGN.md → Two layers: models vs. validator). The binding
// variable picker is SCOPED to the provider/register prefix
// (enforcing the FQID-prefix coupling as UX); the resolve takes the variant.

/** The 2-seg `provider/register` prefix of a register_variant, or "" when it has
 * fewer than 2 segments. */
export function registerPrefixOf(registerVariant: string): string {
  const segs = fqidSegments(registerVariant);
  return segs.length >= 2 ? `${segs[0]}/${segs[1]}` : "";
}

/** The variant coordinate (3rd seg) of a 3-seg register_variant, or "" when it
 * isn't that shape (the picker omits the `?variant` modifier then). */
export function variantSeg(registerVariant: string): string {
  const segs = fqidSegments(registerVariant);
  return segs.length === 3 ? segs[2] : "";
}

// ── Variable-state derivation (the CatalogPicker derive-on-pick) ─────────────

// A LIGHT, advisory storage-token → ColumnType prefill for derive-on-pick
// (overridable; the backend is canonical — see reg_webapp/DESIGN.md → Pydantic
// boundary). Covers BOTH the SQL/storage
// spellings SCB delivers AND the Swedish `Datatyp` tokens SOS writes verbatim
// (reg_meta_build `sources/sos.py` `_norm_data_type` — lowercased "Heltal",
// "Datum", "Identifierare", …). Anything unrecognized → "opaque" for the user.
const NUMERIC_TOKENS = new Set([
  // SQL spellings (SCB)
  "tinyint",
  "smallint",
  "int",
  "integer",
  "bigint",
  "hugeint",
  "decimal",
  "numeric",
  "real",
  "float",
  "double",
  "money",
  "smallmoney",
  // Swedish Datatyp tokens (SOS)
  "heltal",
  "decimaltal",
  "numerisk",
]);
const DATE_TOKENS = new Set(["date", "datum"]);
const DATETIME_TOKENS = new Set([
  "datetime",
  "datetime2",
  "timestamp",
  "smalldatetime",
  // SOS "date and time" — a multi-word phrase, matched on the full string below
  // (the leading-token split would reduce it to "datum" → date).
  "datum och klockslag",
]);
const ID_TOKENS = new Set(["identifierare", "uniqueidentifier"]);

/** Advisory storage-type → ColumnType prefill for the binding type
 * derive-on-pick. A state carrying a value set is categorical; otherwise the
 * leading `data_type` token decides; unrecognized → "opaque". ALWAYS overridable
 * via the BindingEditor's `<select>` — the backend stays canonical. */
export function deriveType(state: VariableStateModel | undefined): string {
  if (!state) {
    return "opaque";
  }
  // reg_meta's curated, variable-grain `is_identifier` is the authoritative
  // semantic signal, so it wins over the storage-derived heuristics below — an
  // integer-stored panel key is an "id", not "numeric" (or "categorical").
  if (state.is_identifier) {
    return "id";
  }
  if (state.value_set_id != null || (state.value_set?.length ?? 0) > 0) {
    return "categorical";
  }
  const norm = (state.data_type ?? "").trim().toLowerCase();
  // The leading token (strip a trailing `(len)` or trailing words like "(text)").
  const token = norm.split(/[ (]/)[0];
  if (ID_TOKENS.has(token)) {
    return "id";
  }
  if (NUMERIC_TOKENS.has(token)) {
    return "numeric";
  }
  // Datetime before date: a multi-word datetime phrase ("datum och klockslag")
  // matches the full `norm`, while its leading token ("datum") would mis-hit DATE.
  if (DATETIME_TOKENS.has(token) || DATETIME_TOKENS.has(norm)) {
    return "datetime";
  }
  if (DATE_TOKENS.has(token)) {
    return "date";
  }
  return "opaque";
}

/** Display the storage `data_type` with its length parenthetical, DROPPING a
 * meaningless length. `data_length` arrives as a string (`str | None` on the
 * wire); many source rows carry `"0"` / `""` as a "no precision" sentinel
 * (rendered verbatim as the artifact "bigint(0)"), and 64 rows carry `"-1"` —
 * SQL Server's varchar(MAX) sentinel. A length is shown only when it parses to
 * a POSITIVE number (e.g. `char(25)`, `Decimaltal(4)`); zero/negative/empty/
 * non-numeric are suppressed. Returns "" when there is no data_type at all (the
 * caller already gates the "Data type" row on a present type). */
export function formatDataType(
  dataType: string | null | undefined,
  dataLength: string | null | undefined,
): string {
  const type = (dataType ?? "").trim();
  if (!type) {
    return "";
  }
  const len = (dataLength ?? "").trim();
  const n = Number(len);
  // Show the parenthetical only for a positive numeric length; "0"/"-1"/""/garbage drop.
  return len !== "" && Number.isFinite(n) && n > 0 ? `${type}(${len})` : type;
}

/** One co-existing REPRESENTATION of a concept at a period — a distinct delivery
 * column. `column` is the stable handle set on `binding.representation`; `label`
 * (the value-set version label, e.g. "5-års intervall"), `codeCount`, and
 * `classificationSlug` (the classification family, e.g. "lkf2007" — see
 * reg_meta/DESIGN.md → Classifications; null when
 * the representative state is code-less) are for display in the chooser.
 *
 * `validTo` is the representative state's `valid_to` (ISO `YYYY-MM-DD`,
 * `9999-12-31` for open-ended) — the latest-era ranking key (see
 * `representationsFromStates`). `codingKey` is a content hash of the value-set
 * (sorted `code|label` pairs + version label); two reps with the same
 * `codingKey` are coding-identical parallel deliveries (the UT0290/UT0280 case)
 * the chooser can COLLAPSE rather than present as a flat choice. */
export interface Representation {
  column: string;
  label: string;
  codeCount: number | null;
  classificationSlug: string | null;
  validTo: string;
  codingKey: string;
}

/** A stable content key for a state's coding — sorted `code|label` pairs plus the
 * value-set version label. Two coexisting columns with the same key carry the
 * IDENTICAL coding (same value-set content + label), so the chooser collapses
 * them (primary + reveal-alternates) instead of forcing a co-equal choice.
 * Code-less states key on `"<label>|no-codes"` so two code-less columns with the
 * same label also collapse. */
function codingKeyOf(s: VariableStateModel): string {
  const members = s.value_set
    ? s.value_set.map((m) => `${m.code}=${m.label}`).sort()
    : ["no-codes"];
  return `${s.value_set_version_label}|${members.join(",")}`;
}

/** The DISTINCT delivery columns among `states` that genuinely CO-EXIST — a column
 * whose validity window overlaps ANOTHER (different) column's window at some instant.
 * The single overlap-detection leaf shared by the editor's `representationsFromStates`
 * chooser (#266) AND the picker's `pickerRepresentations` sequential-rename collapse
 * (#902), so the two never re-derive (and can't drift apart on) the
 * coexist-vs-rename distinction. A column NOT in this set is, relative to its siblings,
 * a SEQUENTIAL RENAME (non-overlapping eras), not a parallel representation. Reads only
 * `delivery_column_name` / `valid_from` / `valid_to`; a null bound is normalized to the
 * `0001`/`9999` sentinel (an unbounded side overlaps freely), so both the leaf's
 * non-null `VariableStateModel` states and the picker's nullable `PickerStateInput`
 * states feed it. Inclusive overlap (`a.from <= b.to && b.from <= a.to`): two columns
 * whose windows merely touch at a shared instant are still co-existing. simplify: O(n^2)
 * pairwise scan, fine for a variable's handful of states. */
export function coexistingColumns(
  states: readonly {
    delivery_column_name: string | null;
    valid_from: string | null;
    valid_to: string | null;
  }[],
): Set<string> {
  const from = (s: { valid_from: string | null }): string =>
    s.valid_from ?? YEARLESS_VALID_FROM;
  const to = (s: { valid_to: string | null }): string =>
    s.valid_to ?? OPEN_ENDED_VALID_TO;
  const coexisting = new Set<string>();
  for (let i = 0; i < states.length; i++) {
    for (let j = i + 1; j < states.length; j++) {
      const a = states[i];
      const b = states[j];
      if (
        a.delivery_column_name &&
        b.delivery_column_name &&
        a.delivery_column_name !== b.delivery_column_name &&
        from(a) <= to(b) &&
        from(b) <= to(a)
      ) {
        coexisting.add(a.delivery_column_name);
        coexisting.add(b.delivery_column_name);
      }
    }
  }
  return coexisting;
}

/** The delivery-column representations a binding must choose between among a
 * resolve's states (the `StatesResponse` from a `?period` resolve), ranked
 * latest-era first so the PRIMARY (currently-active) column is `[0]` — the
 * chooser's default. >1 only when ≥2 columns CO-EXIST (overlapping validity
 * windows) — that is the genuine multi-representation case; this MIRRORS the
 * backend `_coexisting_columns` so a range crossing a sequential column RENAME
 * (distinct columns, non-overlapping) is treated as drift and does NOT open the
 * chooser. 0/1 means no choice is needed. Pure — unit-tested.
 *
 * Ranking key: `valid_to` DESC (open-ended `9999-12-31` sorts first; ISO strings
 * compare chronologically), ties broken by column name for determinism. This
 * reuses the build's latest-era canonicalization (reg_meta_build
 * `fqid_slugs.py`, `ORDER BY vs.valid_to DESC`) so the consumer's "primary"
 * matches the slug-derivation era rather than inventing a second policy
 * (issue #266). Policy is single-sourced HERE on the client: the states already
 * carry per-column `valid_to`, the backend `_coexisting_columns` is a pure
 * ambiguity validator (it ranks nothing), and the CLI `get_datacolumns` reads
 * the period-less `variable_alias` (no `valid_to` to rank by without a DB join,
 * which is out of scope) — so no shared API field would actually be shared. */
export function representationsFromStates(
  states: VariableStateModel[],
): Representation[] {
  const byColumn = new Map<string, VariableStateModel>();
  // A column's latest era = max(valid_to) over ALL its states; the first-seen
  // state supplies label/codeCount/slug/codingKey but NOT the ranking era (a
  // later state can extend the column past the representative's window).
  const maxValidTo = new Map<string, string>();
  for (const s of states) {
    const col = s.delivery_column_name;
    if (!col) {
      continue;
    }
    if (!byColumn.has(col)) {
      byColumn.set(col, s);
    }
    const prev = maxValidTo.get(col);
    if (prev === undefined || s.valid_to > prev) {
      maxValidTo.set(col, s.valid_to);
    }
  }
  // label / codeCount / classificationSlug / codingKey are sourced from the
  // representative (first-seen) state per column; validTo is the column's
  // latest era (max over its states) for ranking.
  const toRep = (s: VariableStateModel): Representation => ({
    column: s.delivery_column_name as string,
    label: s.value_set_version_label,
    codeCount: s.value_set?.length ?? null,
    classificationSlug: s.classification_slug ?? null,
    validTo: maxValidTo.get(s.delivery_column_name as string) as string,
    codingKey: codingKeyOf(s),
  });
  // Latest-era first: valid_to DESC, then column ASC for a stable order.
  const byLatestEra = (a: Representation, b: Representation): number =>
    b.validTo.localeCompare(a.validTo) || a.column.localeCompare(b.column);
  // Distinct columns valid at the SAME instant (overlapping windows) are parallel
  // representations; distinct columns in non-overlapping windows are a rename.
  const coexisting = coexistingColumns(states);
  if (coexisting.size >= 2) {
    return [...byColumn.values()]
      .filter((s) => coexisting.has(s.delivery_column_name as string))
      .map(toRep)
      .sort(byLatestEra);
  }
  // No genuine choice: a single column, or a sequential rename (drift). Report at
  // most the first column so the caller's `length > 1` chooser gate stays closed.
  const first = [...byColumn.values()][0];
  return first ? [toRep(first)] : [];
}

/** Whether a set of coexisting representations is CODING-IDENTICAL — every column
 * carries the same value-set content + version label (the UT0290/UT0280 case:
 * "Folkhögskola" delivered as two columns, both value_set 1197 / "Ja nej 1").
 * The chooser then COLLAPSES: it defaults to the primary (`reps[0]`, latest-era)
 * and offers the alternates as a reveal ("also delivered as …") rather than a
 * forced co-equal choice (issue #266). False for a genuine multi-coding choice
 * (SSYK 3/4/5-digit, age brackets) — those stay an explicit pick. A 0/1-length
 * list trivially collapses (no choice to make). */
export function representationsCollapse(reps: Representation[]): boolean {
  return reps.every((r) => r.codingKey === reps[0]?.codingKey);
}

// ── Direct representation picker (#678 redesign) ─────────────────────────────
// The redesigned add-to-project surface lists a variable's representations as
// selectable rows and commits the user's multi-selection directly — replacing
// the auto-planning `choose-variant` + post-click rep chooser. A "representation
// row" is one distinct `(variant, delivery_column_name)` over the leaf's FULL
// `node.states` history (NOT the period-narrowed subset — the period window only
// DIMS out-of-window rows). This is DISTINCT from `representationsFromStates`
// (which detects co-existing columns AT a period for the editor's chooser, a
// different shape): here every distinct column is its own selectable row, span
// is the column's full history, and there is no co-existence/collapse logic.

/** One selectable representation row in the picker — a distinct
 * `(variant, delivery_column_name)` over a variable's full state history.
 * `key` is the stable `{#each}` / selection identity `${variant}::${column}`
 * (a variant slug and a delivery column are both bare identifiers, neither
 * contains `::`, so the pair can't alias). `from`/`to` are the row's outer ISO
 * span (min `valid_from` … max `valid_to`, the open-ended `9999-12-31` /
 * unknown `0001-01-01` sentinels preserved for `formatWindow`); `period` is the
 * pre-formatted display span. `wirePeriod` is the span as a year-grain WIRE
 * period for the committed source (null when a bound is a sentinel — then the
 * source's period is left unset, the honest "covers the rep's whole span"
 * default, rather than emitting an out-of-grammar `0001`/`9999` token).
 * `valueSetLabel` is the value-set version label of the LATEST-era state (max
 * `valid_to`) — the row's representative coding. `codingsVary` is true when this one
 * column carried MORE THAN ONE distinct `value_set_id` across its states (a coding
 * change over time, e.g. yrkesreg's SUN2020Niva_Old going value-set 303 → 249); the
 * picker shows a quiet "codings vary" nudge then, pointing the user to the States /
 * value-set detail. Keyed on the reliable `value_set_id`, NOT the low-trust
 * per-delivery `value_set_version_label` (the same id is labelled inconsistently
 * across populations/years). */
export interface PickerRepresentation {
  key: string;
  variant: string;
  /** The variant's DISPLAY label — `register_variant.name` when present, else the
   * `variant` slug (a NULL-named variant falls back to the slug). The picker shows
   * THIS wherever the variant is the row identity; `variant` (the slug) stays the
   * selection key and the add coordinate. */
  variantLabel: string;
  column: string;
  from: string;
  to: string;
  /** The column's DISJOINT delivery windows (#678 finding: an interrupted series),
   * each an inclusive ISO span, in chronological order. A continuously-delivered
   * column has exactly one window spanning `from`..`to`; a column delivered in
   * separate eras (e.g. 2005–2010 then 2015–2020, a real gap between) has one window
   * per era. `rowWirePeriod`/`rowAddPeriod` emit the comma-union over these (the #307
   * interrupted-series wire form) so the committed source never covers the gap years
   * the representation wasn't delivered. */
  windows: { from: string; to: string }[];
  period: string;
  wirePeriod: string | null;
  valueSetLabel: string;
  codingsVary: boolean;
  /** The SUPERSEDED delivery-column names this row folds, when it is a collapsed
   * SEQUENTIAL RENAME (#902): one variable+variant's columns delivered over
   * NON-overlapping eras (`DINF` → `DINF83` → `DINF84` → `DINF86`) are ONE evolving
   * representation, not co-equal parallel columns, so they collapse into ONE row led by
   * the LATEST-era column (`column`/`key`) spanning the union of their windows. This
   * lists the EARLIER column names in chronological order (the latest, which leads, is
   * excluded) so the picker can surface the progression as a quiet sub-text hint
   * (`was DINF, DINF83, DINF84`) — NOT the full Gantt/time-band era view (#904).
   * Empty for an ordinary single-column row or a genuinely parallel (co-existing)
   * column, which stay their own rows. */
  renamedColumns: string[];
}

/** The WIRE segment for ONE inclusive ISO window, or null when a bound is a sentinel
 * (open-ended `9999-12-31` / unknown-start `0001-01-01`) — an unbounded side has no
 * in-grammar token. Emits the EXACT span, not a year-rounded one:
 * `periodTokenForBounds` renders the coarsest token that round-trips to these exact
 * bounds — a full year → the bare year, but a SUB-ANNUAL span
 * (`2020-01-01`..`2020-01-31` → `2020-01`, a quarter → `YYYY-Q[1-4]`, a single day →
 * the day) keeps its grain so the committed source covers only the column's real
 * window, not the whole year (which would pull in sibling columns for the rest of
 * the year — #678 fix). A window no single token covers is the explicit `lo..hi`
 * range. */
function windowWireSegment(from: string, to: string): string | null {
  if (from === YEARLESS_VALID_FROM || to === OPEN_ENDED_VALID_TO) {
    return null;
  }
  const token = periodTokenForBounds(from, to);
  // A multi-year span comes back as the explicit ISO range `lo..hi`; collapse
  // YEAR-ALIGNED endpoints to bare years (`2010-01-01..2020-12-31` → `2010..2020`)
  // so a whole-year multi-year span stays a clean year range. A sub-annual endpoint
  // (not Jan-1 / Dec-31 aligned) is preserved exactly — never widened to its year.
  const range = periodRangeEndpoints(token);
  if (range) {
    const [lo, hi] = range;
    const loYear = lo.endsWith("-01-01") ? lo.slice(0, 4) : lo;
    const hiYear = hi.endsWith("-12-31") ? hi.slice(0, 4) : hi;
    return `${loYear}..${hiYear}`;
  }
  return token;
}

/** The WIRE period for a representation row's DISJOINT delivery windows, or null
 * when any contributing window has a sentinel bound (an unbounded side has no
 * in-grammar token, so the source's period is left unset — covers the rep's whole
 * span — rather than leaking a sentinel). A single window emits one segment; several
 * DISJOINT windows emit the comma-union (`2005..2010,2015..2020`, the #307
 * interrupted-series wire form the catalog `?period=` resolves segment-wise since
 * #340) so the committed source covers only the eras the column was delivered, never
 * the gap years between them (#678 finding). */
function rowWirePeriod(
  windows: readonly { from: string; to: string }[],
): string | null {
  if (windows.length === 0) {
    return null;
  }
  const segments: string[] = [];
  for (const w of windows) {
    const seg = windowWireSegment(w.from, w.to);
    // A sentinel-bounded window has no in-grammar token; once ANY window is
    // unbounded the whole comma-union can't be expressed, so leave the period unset.
    if (seg === null) {
      return null;
    }
    segments.push(seg);
  }
  return segments.join(",");
}

/** Merge a column's states into DISJOINT inclusive ISO windows (#678 finding): order
 * by start, then fuse a state into the open window when it is contiguous/overlapping
 * with it (its start is at or before the day after the running end), else start a
 * NEW window (a real delivery gap). Open-ended windows (the `9999-12-31` ceiling)
 * swallow everything after them — like `collapseSpans`, but column-scoped and
 * returning only the spans (no technical-change notes). A continuously-delivered
 * column yields ONE window; an interrupted one yields a window per era. */
function deliveryWindows(
  bounds: readonly { from: string; to: string }[],
): { from: string; to: string }[] {
  const ordered = [...bounds].sort(
    (a, b) => a.from.localeCompare(b.from) || a.to.localeCompare(b.to),
  );
  const windows: { from: string; to: string }[] = [];
  for (const b of ordered) {
    const open = windows.at(-1);
    if (!open) {
      windows.push({ from: b.from, to: b.to });
      continue;
    }
    // An open-ended window already reaches "still delivered"; everything later is
    // contiguous with it (and `dayAfter("9999-12-31")` would overflow — see
    // `collapseSpans`). Contiguous/overlapping otherwise → extend; a real gap → split.
    if (open.to === OPEN_ENDED_VALID_TO || b.from <= dayAfter(open.to)) {
      if (b.to > open.to) {
        open.to = b.to;
      }
    } else {
      windows.push({ from: b.from, to: b.to });
    }
  }
  return windows;
}

/** The inclusive ISO bounds to clamp a picker row's span to on Add, given the active
 * `?period` wire and the year-grain dim window (#678). The `?period` wins at its REAL
 * grain (`periodWireBounds` — so a sub-annual `2020-Q1` stays `2020-01-01..2020-03-31`
 * rather than collapsing to the outer year, the #678 finding); else the year-grain
 * dim window expanded to `[lo-01-01, hi-12-31]`; else null (no clamp — full-span add).
 * Pure — unit-tested. */
export function addWindowBounds(
  period: string | null | undefined,
  window: [number, number] | null,
): { from: string; to: string } | null {
  if (period) {
    const bounds = periodWireBounds(period);
    if (bounds) {
      return bounds;
    }
  }
  return window
    ? { from: `${window[0]}-01-01`, to: `${window[1]}-12-31` }
    : null;
}

/** The wire period to COMMIT for a selected picker row, given the active period
 * window as inclusive ISO bounds (#678). The picker DIMS by the window but every row
 * stays selectable over its FULL span; on Add, though, the user means the period they
 * narrowed to — so intersect the row's DELIVERY WINDOWS with the active window and
 * commit THAT, not the row's whole history. Three failures this fixes (#678):
 *   - an OPEN-ENDED row (`wirePeriod` null) would otherwise add a period-UNSET
 *     source the derive leaves unresolved — clamped to the finite window, it
 *     resolves;
 *   - a FINITE multi-year row would otherwise widen the source to its whole history,
 *     ignoring the `?period` the user picked;
 *   - a SUB-ANNUAL `?period` (`2020-Q1`) is honored at its real grain — `window`
 *     carries the exact ISO bounds (see `addWindowBounds`), so the commit is the
 *     quarter, never the collapsed outer year.
 * `window` is the active inclusive ISO bounds (`addWindowBounds`), or null. With NO
 * window the row's own `wirePeriod` is committed unchanged (full-span add). Each of
 * the row's DISJOINT delivery windows is clamped into the active window (its sentinel
 * bounds treated as unbounded); the surviving windows render as the comma-union via
 * the same `rowWirePeriod` path (so an interrupted series stays interrupted, #678).
 * An empty intersection (the row lies wholly outside the window — only reachable for
 * an explicitly-selected dimmed row) falls back to the row's own `wirePeriod` so the
 * add is never dropped. Pure — unit-tested. */
export function rowAddPeriod(
  row: PickerRepresentation,
  window: { from: string; to: string } | null,
): string | null {
  if (!window) {
    return row.wirePeriod;
  }
  // Clamp each delivery window into the active window; a sentinel bound is unbounded
  // on that side, so the active-window edge wins there. Drop windows that fall wholly
  // outside (empty intersection) — only the surviving, in-window spans commit.
  const clamped: { from: string; to: string }[] = [];
  for (const w of row.windows) {
    const lo =
      w.from === YEARLESS_VALID_FROM
        ? window.from
        : maxIso(w.from, window.from);
    const hi =
      w.to === OPEN_ENDED_VALID_TO ? window.to : minIso(w.to, window.to);
    if (lo <= hi) {
      clamped.push({ from: lo, to: hi });
    }
  }
  // Wholly outside the window (no surviving window): keep the row's own span so an
  // explicitly-selected dimmed row still adds something sensible.
  if (clamped.length === 0) {
    return row.wirePeriod;
  }
  return rowWirePeriod(clamped);
}

/** The later / earlier of two ISO `YYYY-MM-DD` bounds (lexicographic order is
 * chronological for zero-padded ISO dates). */
function maxIso(a: string, b: string): string {
  return a > b ? a : b;
}
function minIso(a: string, b: string): string {
  return a < b ? a : b;
}

/** The state shape `pickerRepresentations` reads — the fields shared by the
 * binding leaf's `VariableStateModel` and the group graph's `GraphState` (#678 inc
 * 2). The leaf states carry non-null ISO bounds; the graph states carry `null` on
 * an unbounded/unknown side (so this widens both to `string | null`), normalized to
 * the `0001`/`9999` sentinels below so the rest of the pipeline is bound-uniform.
 * `VariableStateModel` is structurally assignable here (non-null is a subtype of
 * nullable), so the leaf call needs no cast — this widens the param, it doesn't
 * fork the function. */
export interface PickerStateInput {
  variant: string;
  /** The variant's curator DISPLAY name (`register_variant.name`), or null for a
   * NULL-named variant. Display-only — `variant` (the slug) stays the selection key /
   * add coordinate. Both sources carry it (`VariableStateModel.variant_label` / the
   * graph `GraphState.variant_label`). */
  variant_label: string | null;
  delivery_column_name: string | null;
  value_set_version_label: string;
  /** The RELIABLE value-set identity — the coding-change signal (`codingsVary`).
   * Both sources carry it (`VariableStateModel.value_set_id` / the graph
   * `GraphState.value_set_id`). NOT the low-trust `value_set_version_label`, which
   * SCB labels inconsistently for the same id across populations/years. `null` for a
   * code-less state — its own distinct value, so a null↔id transition counts as a
   * coding change. */
  value_set_id: number | null;
  valid_from: string | null;
  valid_to: string | null;
}

/** Enumerate a variable's representation rows from its states (#678): one row per
 * distinct `(variant, delivery_column_name)`, states with a null
 * `delivery_column_name` skipped (no column = nothing to deliver/select). For
 * each group the span is `formatWindow(min valid_from, max valid_to)` across its
 * states and the value-set label is the LATEST-era state's
 * `value_set_version_label`. First-seen order is preserved so the list is
 * stable. Accepts both the binding leaf's `VariableStateModel[]` AND the group
 * graph's `GraphState[]` (#678 inc 2) via the shared `PickerStateInput` shape — a
 * null bound (the graph's unbounded side) is normalized to the `0001`/`9999`
 * sentinel `formatWindow`/`rowWirePeriod` already understand, so a graph-sourced
 * row renders identically to a leaf-sourced one. Pure — unit-tested in
 * catalog.test.ts. */
export function pickerRepresentations(
  states: readonly PickerStateInput[],
): PickerRepresentation[] {
  // Group by VARIANT first, then by column within each variant: the
  // coexist-vs-rename distinction (#902) is per-variant (a rename is one
  // variable+variant's columns over non-overlapping eras), so coexistence must be
  // computed on a variant's own states, not across populations. First-seen variant
  // order, then first-seen column order within it, keep the output stable.
  const byVariant = new Map<string, Map<string, PickerStateInput[]>>();
  for (const s of states) {
    if (!s.delivery_column_name) {
      continue;
    }
    const cols =
      byVariant.get(s.variant) ?? new Map<string, PickerStateInput[]>();
    const group = cols.get(s.delivery_column_name);
    if (group) {
      group.push(s);
    } else {
      cols.set(s.delivery_column_name, [s]);
    }
    byVariant.set(s.variant, cols);
  }
  const out: PickerRepresentation[] = [];
  for (const [variant, cols] of byVariant) {
    // The variant's genuinely CO-EXISTING (overlapping) columns — parallel
    // representations that stay their OWN rows. Every other column is, relative to its
    // siblings, a sequential RENAME and folds into ONE row (when ≥2 of them). Reuses
    // the shared `coexistingColumns` leaf so the picker and the editor's chooser can't
    // drift on the distinction (CLAUDE.md: reuse the leaf, don't re-derive it).
    const variantStates: PickerStateInput[] = [];
    for (const g of cols.values()) {
      variantStates.push(...g);
    }
    const coexisting = coexistingColumns(variantStates);
    const renameGroups: PickerStateInput[][] = [];
    const renameStates: PickerStateInput[] = [];
    for (const [column, group] of cols) {
      if (coexisting.has(column)) {
        // A parallel (co-existing) column: its own standalone row, as before.
        renameGroups.push(group);
      } else {
        // A non-overlapping column: accumulate into the variant's single rename fold.
        renameStates.push(...group);
      }
    }
    // The non-coexisting columns collapse into ONE row only when there are ≥2 of them
    // (a genuine rename progression); a lone non-coexisting column is just an ordinary
    // single-column row (handled by the same fold of one column).
    if (renameStates.length > 0) {
      renameGroups.push(renameStates);
    }
    for (const group of renameGroups) {
      out.push(pickerRow(variant, group));
    }
  }
  return out;
}

/** Build ONE picker row from a column-group's states (#902): an ordinary single
 * column's states, OR several SEQUENTIAL-RENAME columns of one variant+variant folded
 * together (`pickerRepresentations` decides which). The row leads with the LATEST-era
 * column (max `valid_to`) — its `key`/`column`/coding — and spans the union of every
 * contributing column's delivery windows, so a folded rename reads as ONE evolving
 * representation over its full time span. When the group folds >1 distinct column, the
 * superseded (earlier) columns are listed chronologically in `renamedColumns` for the
 * picker's quiet progression hint (the latest, which leads, is excluded). */
function pickerRow(
  variant: string,
  group: PickerStateInput[],
): PickerRepresentation {
  // Normalize a state's nullable bounds to the catalog sentinels: a null start is
  // the yearless floor, a null end the open-ended ceiling — the same forms the leaf
  // states already carry, so the span/wire-period logic below stays bound-uniform.
  const validFrom = (s: PickerStateInput): string =>
    s.valid_from ?? YEARLESS_VALID_FROM;
  const validTo = (s: PickerStateInput): string =>
    s.valid_to ?? OPEN_ENDED_VALID_TO;
  const from = group.reduce(
    (m, s) => (validFrom(s) < m ? validFrom(s) : m),
    validFrom(group[0]),
  );
  const to = group.reduce(
    (m, s) => (validTo(s) > m ? validTo(s) : m),
    validTo(group[0]),
  );
  // The DISJOINT delivery windows across ALL the group's states (a folded rename's
  // several columns, or one column's interrupted series) — the wire period commits the
  // comma-union over these so a gap year is never covered (#678 finding).
  const windows = deliveryWindows(
    group.map((s) => ({ from: validFrom(s), to: validTo(s) })),
  );
  // The latest-era state (max valid_to) leads: its column is the row identity and its
  // value-set label the representative coding. For a folded rename this is the CURRENT
  // (surviving) column name — `DINF86`, not the retired `DINF`.
  const latest = group.reduce((a, b) => (validTo(b) > validTo(a) ? b : a));
  const column = latest.delivery_column_name as string;
  // The superseded columns of a folded rename, oldest-first (the leading latest column
  // excluded) — the picker's "was X, Y, Z" progression hint. Ordered by each column's
  // own earliest era so the hint reads chronologically. Empty for an ordinary single
  // column (one distinct column → nothing superseded).
  const eraByColumn = new Map<string, string>();
  for (const s of group) {
    const col = s.delivery_column_name as string;
    const prev = eraByColumn.get(col);
    if (prev === undefined || validFrom(s) < prev) {
      eraByColumn.set(col, validFrom(s));
    }
  }
  const renamedColumns = [...eraByColumn.entries()]
    .filter(([col]) => col !== column)
    .sort((a, b) => a[1].localeCompare(b[1]))
    .map(([col]) => col);
  // Coding change over time: >1 DISTINCT value_set_id across the group's states.
  // Keyed on the reliable id (not the label, which SCB labels inconsistently for
  // one id); `null` (code-less) is its own value, so a null↔id transition counts.
  const codingsVary = new Set(group.map((s) => s.value_set_id)).size > 1;
  // The variant display label — the curator `variant_label`, falling back to the
  // slug for a NULL-named variant. Display-only; the slug stays the key/coordinate.
  const variantLabel = group[0].variant_label ?? variant;
  return {
    key: `${variant}::${column}`,
    variant,
    variantLabel,
    column,
    from,
    to,
    windows,
    period: formatWindow(from, to),
    wirePeriod: rowWirePeriod(windows),
    valueSetLabel: latest.value_set_version_label,
    codingsVary,
    renamedColumns,
  };
}

/** Narrow `pickerRepresentations`' input states to the SAME subset an active
 * StatesView narrowing modifier scopes to (#678 finding): when the leaf is opened
 * with a `?variant` and/or `?value_set_version` modifier (the "Narrowed by" chip),
 * the picker rows must be built only from states consistent with that narrowing —
 * else "select all" would add rows for variants / value-set versions OUTSIDE the
 * active narrowing. Mirrors StatesView's `inScope` matching exactly:
 *   - `variant` matches `state.variant`;
 *   - `valueSetVersion` matches `state.value_set_version_label`, with the
 *     `_none` sentinel (`VALUE_SET_VERSION_NONE`) meaning the empty/default label
 *     (an empty `?value_set_version=` can't ride in the URL).
 * Either modifier `null` is a no-op on that axis; BOTH null returns the states
 * unchanged (the full-history default — behavior is unchanged when no modifier is
 * active). Pure — unit-tested in catalog.test.ts. Generic over the shared
 * `PickerStateInput` shape so it composes directly before `pickerRepresentations`. */
export function narrowStatesByModifier<
  S extends Pick<PickerStateInput, "variant" | "value_set_version_label">,
>(
  states: readonly S[],
  variant: string | null,
  valueSetVersion: string | null,
): readonly S[] {
  if (variant === null && valueSetVersion === null) {
    return states;
  }
  const version =
    valueSetVersion === VALUE_SET_VERSION_NONE ? "" : valueSetVersion;
  return states.filter(
    (s) =>
      (variant === null || s.variant === variant) &&
      (version === null || s.value_set_version_label === version),
  );
}

// ── Adaptive row labeling (#678 1b) ─────────────────────────────────────────
// A variable's representation rows differ along up to four dimensions: `column`
// (delivery column), `variant` (population), `valueSet` (value-set version
// label), and `period` (the row's span). A naive "show the column" row label
// fails two real shapes:
//   - fordonsreg/naringsgren: the column is CONSTANT ("SNI2002") across rows and
//     the POPULATION varies (lastbilar/bussar/…) → every row reads identically;
//   - yrkesreg/sun2020niva: rows visibly DUPLICATE because the distinguishing
//     population isn't shown.
// The fix is adaptive: classify each dimension as CONSTANT (1 distinct value) or
// VARYING (>1), HOIST the constants to the band header (rendered once as quiet
// context), and show only the VARYING dimensions on each row. Rows within one
// variable share the same dim SET (consistent); the shape may differ across
// variables (intended). Pure — unit-tested in catalog.test.ts. The selection key
// and commit payload are unchanged: this is DISPLAY only.

/** A row's adaptive display projection: the prominent `primary` label (mono when
 * it is the delivery column — the picker renders a mono primary as a COLUMN CHIP),
 * the muted `qualifiers` (the remaining varying dimensions), and `period` ONLY when
 * the span varies across rows (else it is hoisted to the header, so it is null here).
 * `key` is the row's selection key (unchanged — `${variant}::${column}`). */
export interface PickerRowLabel {
  key: string;
  primary: { text: string; mono: boolean };
  qualifiers: string[];
  period: string | null;
}

/** The adaptive labeling of a variable's rows (#678 1b): `column` is the hoisted
 * CONSTANT delivery column (the picker renders it as a prominent COLUMN CHIP), or
 * null when the column varies (it's then each row's primary). `headerContext` is the
 * remaining quiet context — the constant value-set label, OR (when value-set labels
 * vary but share a long leading stem, #678) that COMMON STEM, hoisted once so the
 * rows show only their suffix. The period is NEVER in `headerContext` — every row
 * renders its own `period` on the right (the picker's period column), so hoisting it
 * would double-show it. The variant, when constant, is NOT hoisted either — a
 * single-variant register's whole-population default is noise and is already in the
 * add coordinate; it only appears when it VARIES (as the row identity). */
export interface PickerLabeling {
  column: string | null;
  headerContext: string[];
  rows: PickerRowLabel[];
}

/** The distinct count of a dimension across rows (via a projector). */
function distinctCount(
  rows: readonly PickerRepresentation[],
  pick: (r: PickerRepresentation) => string,
): number {
  return new Set(rows.map(pick)).size;
}

/** The longest leading WORD-SEQUENCE (whitespace-split) shared by a MAJORITY
 * (> half) of `labels`, or "" when no such stem is SUBSTANTIAL (≥ 2 words AND ≥ 10
 * chars). The hoist target for repetitive value-set labels (#678): SCB labels like
 * "Svensk standard för näringsgrensindelning, SNI 92, Aktiviteter" share a long
 * leading stem and differ only in the suffix — hoisting the stem once and showing
 * each row's suffix kills the repetition. MAJORITY (not all) so a lone outlier
 * (a differently-worded label) doesn't defeat the stem; rows whose label doesn't
 * start with the stem keep their full label (the caller's job). Pure — unit-tested.
 *
 * Word boundary, not character: the stem ends on a whole word so a suffix never
 * starts mid-word. Length floor (chars over the joined stem) avoids hoisting a
 * trivial shared lead like "SNI 92". */
export function commonLabelStem(labels: readonly string[]): string {
  const wordLists = labels
    .filter((l) => l !== "")
    .map((l) => l.split(/\s+/).filter((w) => w !== ""));
  if (wordLists.length === 0) {
    return "";
  }
  const majority = Math.floor(wordLists.length / 2) + 1;
  // Grow the candidate stem word by word: at each length, the most common leading
  // word-sequence of that length must still be shared by ≥ majority lists. Stop when
  // no length-k prefix reaches the majority.
  let best: string[] = [];
  const maxLen = Math.max(...wordLists.map((w) => w.length));
  for (let k = 1; k <= maxLen; k++) {
    // Tally each list's first-k-word prefix (lists shorter than k can't contribute).
    const counts = new Map<string, number>();
    for (const words of wordLists) {
      if (words.length < k) {
        continue;
      }
      const prefix = words.slice(0, k).join(" ");
      counts.set(prefix, (counts.get(prefix) ?? 0) + 1);
    }
    // The best length-k prefix and whether it still clears the majority.
    let topPrefix = "";
    let topCount = 0;
    for (const [prefix, count] of counts) {
      if (count > topCount) {
        topCount = count;
        topPrefix = prefix;
      }
    }
    if (topCount >= majority && topPrefix !== "") {
      best = topPrefix.split(" ");
    } else {
      break;
    }
  }
  const stem = best.join(" ");
  // Substantial only: ≥ 2 words AND ≥ 10 chars, else the hoist isn't worth it.
  return best.length >= 2 && stem.length >= 10 ? stem : "";
}

/** A value-set label projected against a hoisted `stem`: the SUFFIX (the label with
 * the stem removed and leading separators/space trimmed) when the label starts with
 * the stem; the FULL label otherwise (an outlier that doesn't share the stem). An
 * empty stem (no hoist) returns the label unchanged. A label that IS exactly the stem
 * yields "" (the identical-all case — nothing left to show per row). */
export function labelSuffix(label: string, stem: string): string {
  if (stem === "" || !label.startsWith(stem)) {
    return label;
  }
  // Trim the stem, then any leading separator punctuation + whitespace ("," ":" "-"…).
  return label.slice(stem.length).replace(/^[\s,;:.\-–—/]+/, "");
}

/** Compute the adaptive labeling for a variable's representation rows (#678 1b):
 * hoist the dimensions that are CONSTANT across the rows to `headerContext`, and
 * project each row onto only its VARYING dimensions. Row label priority among the
 * varying dims is `column` (mono) → `variant` → `valueSet`; the first varying one
 * is the prominent `primary`, the rest are muted `qualifiers`; `period` rides on
 * the right only when the span varies. When NOTHING varies (a single
 * representation), the row falls back to the column (mono), then the variant,
 * then "—" — never a blank or all-constant row. */
export function pickerLabeling(
  rows: readonly PickerRepresentation[],
): PickerLabeling {
  const columnVaries = distinctCount(rows, (r) => r.column) > 1;
  const variantVaries = distinctCount(rows, (r) => r.variant) > 1;
  const periodVaries = distinctCount(rows, (r) => r.period) > 1;

  // Value-set distinctness is over NON-EMPTY labels only: a label that is constant
  // except on rows with no value set (e.g. fordonsreg — one population delivers no
  // value set) must read as CONSTANT, so the one real label hoists to the context
  // instead of showing per-row. ≤1 distinct non-empty label ⇒ constant; empty rows
  // simply contribute no value-set label.
  const nonEmptyLabels = rows
    .map((r) => r.valueSetLabel)
    .filter((l) => l !== "");
  const valueSetLabels = new Set(nonEmptyLabels);
  const valueSetVaries = valueSetLabels.size > 1;
  // The single constant label to hoist (the lone non-empty one), or "" when none.
  const constantValueSet =
    valueSetLabels.size === 1 ? [...valueSetLabels][0] : "";
  // When the labels VARY, hoist their longest majority WORD-STEM (#678): the long
  // repeated lead (e.g. "Svensk standard för näringsgrensindelning,") goes to the
  // context once and each row shows only its suffix. "" when no substantial stem.
  const stem = valueSetVaries ? commonLabelStem(nonEmptyLabels) : "";

  // The constant column hoists to the prominent COLUMN CHIP (the picker styles it);
  // a varying column is each row's primary instead. Empty rows → null/empty labeling.
  const sample = rows[0];
  const column =
    sample && !columnVaries && sample.column ? sample.column : null;
  const headerContext: string[] = [];
  // The value-set quiet context: the single constant label, else the hoisted stem.
  if (!valueSetVaries && constantValueSet) {
    headerContext.push(constantValueSet);
  } else if (stem) {
    headerContext.push(stem);
  }

  const labelRows = rows.map((r): PickerRowLabel => {
    // The varying dimensions, in display priority. Each is a candidate label.
    // Variance is keyed on the slug (`variant`, the identity), but the DISPLAYED
    // text is the variant's `variantLabel` (the curator name, slug-fallback).
    const varying: { text: string; mono: boolean }[] = [];
    if (columnVaries) {
      varying.push({ text: r.column, mono: true });
    }
    if (variantVaries) {
      varying.push({ text: r.variantLabel, mono: false });
    }
    if (valueSetVaries && r.valueSetLabel) {
      // With a hoisted stem, show only this row's SUFFIX (full label for an outlier
      // that doesn't share the stem; "" for a label that IS exactly the stem — then
      // it contributes no qualifier). No stem → the full label as before.
      const text = labelSuffix(r.valueSetLabel, stem);
      if (text !== "") {
        varying.push({ text, mono: false });
      }
    }
    // Fallback when nothing varies (a single representation, or rows that differ
    // only by period): show the column, then the variant label, then a dash — never
    // a blank row.
    const primary =
      varying[0] ??
      (r.column
        ? { text: r.column, mono: true }
        : r.variant
          ? { text: r.variantLabel, mono: false }
          : { text: "—", mono: false });
    return {
      key: r.key,
      primary,
      qualifiers: varying.slice(1).map((d) => d.text),
      period: periodVaries ? r.period : null,
    };
  });

  return { column, headerContext, rows: labelRows };
}

// ── Picker dimension marking + filtering (#908) ──────────────────────────────
// A multi-axis concept group (the #819 families — enhet × hushållsbegrepp ×
// kapitalvinst, or a coding/variant split) crowds the picker: the user can see a
// column chip + a quiet facet/qualifier line, but can't (a) tell at a glance what
// KIND of dimension distinguishes two rows, nor (b) narrow the list to one axis
// value ("only Hushåll", or one variant). These helpers make the dimensions
// first-class. A dimension is one of three KINDS:
//   - `facet`  — a #819 declared axis (`GroupAxis`), a per-MEMBER property (every
//     row of a band carries the band's facet on that axis);
//   - `variant` — the population (`register_variant`), a per-ROW property;
//   - `coding`  — the value-set version label, a per-ROW property.
// A dimension is only marked/filterable when it DISCRIMINATES — ≥2 distinct values
// across the picker's rows; a single-value axis is not a filter. Filtering is a
// CLIENT-SIDE LENS: it narrows which rows show, never the selection or the commit.

/** One filterable dimension of the picker (#908): its `kind` (drives the marker
 * styling + the filter ordering — facets first, then variant, then coding), the
 * stable `key` (NAMESPACED `facet:<axis>` for a facet; the literal
 * `"variant"`/`"coding"` for the row dimensions — the `facet:` prefix makes the key
 * UNIQUE even when a declared axis is literally named `variant`/`coding`, #908 C2),
 * the raw `axis` name (set ONLY for `kind: "facet"`, so the facet branch can look the
 * row's facet up by its un-prefixed axis name), the display `label`, and the distinct
 * `(value, label)` pairs the rows carry on it (value-sorted). Only dimensions with ≥2
 * distinct values are emitted. */
export interface PickerDimension {
  kind: "facet" | "variant" | "coding";
  key: string;
  /** The raw axis name a facet dimension matches on (`rowFacet(band, row, axis)`).
   * Set only for `kind: "facet"`; the namespaced `key` (`facet:<axis>`) keys the UI /
   * selection state, this is the value-lookup key. */
  axis?: string;
  label: string;
  values: { value: string; label: string }[];
}

/** The facet a ROW carries on `axis` — looked up PER-COLUMN first, then BAND-LEVEL
 * (#908 C1). A facet is normally a per-MEMBER = per-(fqid,column) property, so a
 * representation group that collapses several members onto ONE band/fqid carries
 * DISTINCT facets per delivery column (`facetsByColumn`, keyed by the row's own
 * column). But a WHOLE-VARIABLE faceted member has a null `delivery_column` (e.g. a
 * month-faceted group: one variable per month), so its facets can't key by column —
 * they're carried band-level (`band.facets`) and apply to ALL of the band's rows. The
 * per-column entry wins when present; the band-level facet is the fallback. Undefined
 * when neither carries a facet on `axis`. */
export function rowFacet(
  band: PickerBandFacets,
  row: PickerRepresentation,
  axis: string,
): GroupFacetModel | undefined {
  return (
    band.facetsByColumn?.[row.column]?.find((x) => x.axis === axis) ??
    band.facets?.find((x) => x.axis === axis)
  );
}

/** The minimal band shape the dimension helpers read: its rows (for variant /
 * coding), its per-delivery-column facets (for the #819 representation-member axes),
 * and its BAND-LEVEL facets (#908 C1 — a whole-variable faceted member's facets,
 * which have no delivery column to key by and so apply to every row of the band). The
 * picker's `PickerBand` widens this. */
export interface PickerBandFacets {
  rows: readonly PickerRepresentation[];
  facetsByColumn?: Record<string, GroupFacetModel[]>;
  facets?: GroupFacetModel[];
}

/** The filterable dimensions across the picker's bands (#908), in display order:
 * the declared facet axes first (curator-ordered via `axes`), then `variant`, then
 * `coding`. Each is emitted ONLY when it discriminates — ≥2 distinct values across
 * the bands' rows — so a single-population, single-coding, single-axis-value group
 * surfaces NO filter (the controls collapse). Pure — unit-tested in catalog.test.ts. */
export function pickerFilterDimensions(
  bands: readonly PickerBandFacets[],
  axes: readonly GroupAxisModel[],
): PickerDimension[] {
  const out: PickerDimension[] = [];

  // Facet axes (#819): a per-MEMBER (= per-(fqid,column)) dimension — collect the
  // distinct facet values across every ROW's column. A representation group collapses
  // several members onto one band, so the axis varies WITHIN the band, column to
  // column. Curator order preserved (the `axes` tuple is ordinal-sorted); values
  // value-sorted within the axis.
  for (const axis of axes) {
    const seen = new Map<string, string>();
    for (const band of bands) {
      for (const row of band.rows) {
        const f = rowFacet(band, row, axis.name);
        if (f && !seen.has(f.value)) {
          seen.set(f.value, f.label);
        }
      }
    }
    if (seen.size >= 2) {
      out.push({
        kind: "facet",
        // Namespace the key (#908 C2) so a declared axis literally named
        // `variant`/`coding` can't collide with the built-in row dimensions below;
        // `axis` carries the raw name for the per-row facet lookup.
        key: `facet:${axis.name}`,
        axis: axis.name,
        label: axis.label,
        values: [...seen.entries()]
          .map(([value, label]) => ({ value, label }))
          .sort((a, b) => a.value.localeCompare(b.value)),
      });
    }
  }

  // Variant (population) — a per-ROW dimension keyed on the variant slug, displayed
  // by its `variantLabel` (curator name, slug-fallback).
  const variantSeen = new Map<string, string>();
  for (const band of bands) {
    for (const row of band.rows) {
      if (!variantSeen.has(row.variant)) {
        variantSeen.set(row.variant, row.variantLabel);
      }
    }
  }
  if (variantSeen.size >= 2) {
    out.push({
      kind: "variant",
      key: "variant",
      label: "Population",
      values: [...variantSeen.entries()]
        .map(([value, label]) => ({ value, label }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    });
  }

  // Coding (value-set version) — a per-ROW dimension keyed on the label itself
  // (it's display-only; empty labels — a code-less population — contribute none).
  const codingSeen = new Set<string>();
  for (const band of bands) {
    for (const row of band.rows) {
      if (row.valueSetLabel !== "") {
        codingSeen.add(row.valueSetLabel);
      }
    }
  }
  if (codingSeen.size >= 2) {
    out.push({
      kind: "coding",
      key: "coding",
      label: "Coding",
      values: [...codingSeen]
        .sort((a, b) => a.localeCompare(b))
        .map((v) => ({ value: v, label: v })),
    });
  }

  return out;
}

/** Whether a picker ROW passes the active filter selection (#908): for EVERY
 * dimension with a non-empty selection the row must carry a matching value (OR
 * within a dimension, AND across) — the same logic the #819 navigator uses. A
 * dimension with no selection imposes no constraint. The row's facet value on an
 * axis comes from `rowFacet` (per-column, then band-level — #908 C1); a row lacking a
 * facet on a selected axis fails that axis (it isn't part of that facet's slice).
 * Pure. */
export function pickerRowPasses(
  row: PickerRepresentation,
  band: PickerBandFacets,
  dimensions: readonly PickerDimension[],
  selected: Readonly<Record<string, ReadonlySet<string>>>,
): boolean {
  for (const dim of dimensions) {
    const sel = selected[dim.key];
    if (!sel || sel.size === 0) {
      continue;
    }
    if (dim.kind === "variant") {
      if (!sel.has(row.variant)) {
        return false;
      }
    } else if (dim.kind === "coding") {
      if (!sel.has(row.valueSetLabel)) {
        return false;
      }
    } else {
      // facet: the row's facet value on this axis (per-column, then band-level). The
      // selection is keyed by the namespaced `dim.key` (`facet:<axis>`), but the facet
      // is looked up by the RAW axis name (#908 C2).
      const f = rowFacet(band, row, dim.axis ?? dim.key);
      if (!f || !sel.has(f.value)) {
        return false;
      }
    }
  }
  return true;
}

// ── Adaptive band IDENTITY labeling (#678 inc 2) ─────────────────────────────
// One level UP from `pickerLabeling` (which adapts ROWS within one variable): a
// group's member BANDS differ along up to four identity dimensions — the variable
// NAME, the register prefix, the member's FACET label, and a distinguishing column
// /slug. A naive "show the name" band header fails the common group shape: all 8
// members of a representation group are named "Näringsgren" / share `scb/moms`, so
// every header reads identically and the thing that tells them apart (`Ng0`/`Ng1`,
// or the facet `specialskola`) is buried in the row context. The fix mirrors the
// row adaptiveness: classify each identity dimension as CONSTANT (≤1 distinct
// value) or VARYING, HOIST the constants (the name is already the page <h2>; the
// prefix is in the breadcrumb), and lead each band with its DISTINGUISHING
// identity. Pure — unit-tested in catalog.test.ts.

/** The identity dimensions of ONE member band — the inputs to `bandLabeling`. The
 * `distinguisher` is the band's natural technical differentiator (a SINGLE-COLUMN
 * band's sole delivery column — its several rows are populations — else the member
 * leaf slug), rendered mono when it leads. `distinguisherIsColumn` says which of the
 * two it is: a real delivery column (→ the picker renders it as the column chip-LINK
 * identity) or the slug fallback (→ a plain mono code, for a genuinely multi-column
 * member). */
export interface BandIdentity {
  name: string;
  registerPrefix: string;
  facetLabel: string | null;
  distinguisher: string;
  distinguisherIsColumn: boolean;
}

/** One band's adaptive header projection: the leading `primary` identity (mono for
 * a column/slug, normal weight for a name/facet) and whether that primary IS the
 * band's delivery column (so the band renders it as the column chip-LINK identity
 * and suppresses repeating that column in its row context). */
export interface BandLabel {
  primary: { text: string; mono: boolean };
  /** True when the primary IS this band's distinguisher AND that distinguisher is a
   * real delivery column (a SINGLE-COLUMN member) — the band leads with the column
   * chip-link and drops the redundant "column …" from its context. False for the
   * slug fallback (a genuinely multi-column member leads with a plain mono slug). */
  primaryIsColumn: boolean;
  /** True when the primary IS this band's `facetLabel` (the facet-varies branch, or
   * the lone-band facet fallback) — the band leads with the facet, so a SINGLE-column
   * band drops the redundant repeat of that same facet from its row context. False for
   * every other primary (name / column / slug / "—"). */
  primaryIsFacet: boolean;
}

/** The adaptive labeling across a group's member bands (#678 inc 2): `showName` /
 * `showPrefix` say whether the (constant-hoisted) variable name / register prefix
 * should still render on each band — false when constant across all bands (already
 * the page title / breadcrumb), true when they genuinely vary. `bands` carries each
 * band's leading identity, in input order.
 *
 * Per-band primary priority — the first VARYING identity dimension:
 *   NAME (genuinely different concepts) → FACET label (a facet axis, e.g.
 *   `specialskola`) → distinguisher (delivery column / member slug, e.g. `Ng0`).
 * When NOTHING varies (a single band — the leaf) the primary falls back to the
 * name, then the facet, then the distinguisher — so a 1-band leaf still leads with
 * the variable name, never an empty/column primary. */
export function bandLabeling(bands: readonly BandIdentity[]): {
  showName: boolean;
  showPrefix: boolean;
  bands: BandLabel[];
} {
  const distinct = (pick: (b: BandIdentity) => string | null): number =>
    new Set(bands.map(pick)).size;
  const nameVaries = distinct((b) => b.name) > 1;
  const prefixVaries = distinct((b) => b.registerPrefix) > 1;
  const facetVaries = distinct((b) => b.facetLabel) > 1;

  const labels = bands.map((b): BandLabel => {
    // The leading identity: first varying dimension, then a single-band fallback
    // chain (name → facet → distinguisher) so the leaf leads with its name.
    if (nameVaries) {
      return {
        primary: { text: b.name, mono: false },
        primaryIsColumn: false,
        primaryIsFacet: false,
      };
    }
    if (facetVaries && b.facetLabel) {
      return {
        primary: { text: b.facetLabel, mono: false },
        primaryIsColumn: false,
        primaryIsFacet: true,
      };
    }
    // Name + facet constant across bands → lead with the distinguisher (the column
    // /slug that actually varies, e.g. `SNI2002`/`SNI2007_Ag`), rendered mono. It is
    // the column chip identity for a single-column member (`distinguisherIsColumn`) and
    // a plain mono slug for a multi-column one. A SINGLE-COLUMN member leads with its
    // column even when it is the lone band (the leaf): the variable name is already the
    // page <h2>, so a one-column leaf shows just its column, not a repeated name. A
    // multi-BAND group always leads with the distinguisher (column or slug).
    if (b.distinguisher && (b.distinguisherIsColumn || bands.length > 1)) {
      return {
        primary: { text: b.distinguisher, mono: true },
        primaryIsColumn: b.distinguisherIsColumn,
        primaryIsFacet: false,
      };
    }
    // A lone MULTI-column leaf has no single column to lead with → the variable name
    // (its columns are the rows beneath). Then facet, then the distinguisher fallback.
    if (b.name) {
      return {
        primary: { text: b.name, mono: false },
        primaryIsColumn: false,
        primaryIsFacet: false,
      };
    }
    if (b.facetLabel) {
      return {
        primary: { text: b.facetLabel, mono: false },
        primaryIsColumn: false,
        primaryIsFacet: true,
      };
    }
    return {
      primary: { text: b.distinguisher || "—", mono: true },
      primaryIsColumn: b.distinguisherIsColumn && !!b.distinguisher,
      primaryIsFacet: false,
    };
  });

  return {
    showName: nameVaries,
    showPrefix: prefixVaries,
    bands: labels,
  };
}

/** One name-cluster of member bands (#901): the bands sharing a `name`, plus the
 * per-cluster `bandLabeling` output for THOSE bands (so each leads with what varies
 * WITHIN the cluster — its facet → column/slug — the name now constant and hoisted
 * off). `bands` carries the original `T` items in first-appearance order; `labeling`
 * is index-aligned with them. */
export interface BandCluster<T> {
  name: string;
  bands: T[];
  labeling: ReturnType<typeof bandLabeling>;
}

/** Cluster member bands by `name` and label each cluster independently (#901).
 *
 * `bandLabeling` decides the leading identity GLOBALLY — `nameVaries` is true as soon
 * as TWO distinct names appear, which makes every band in a heterogeneous group lead
 * with its (often repeated) name and bury the real distinguisher (facet/column). The
 * fix is to group by name FIRST, then run the existing per-band labeling per cluster:
 * inside a cluster the name is constant, so `bandLabeling` falls through to leading
 * each band with its facet → column/slug exactly as it already does for a homogeneous
 * group — no parallel labeling path.
 *
 * `showClusterHeadings` is true iff there is more than one cluster: a single cluster
 * (every member shares the name — the homogeneous group, or the one-member leaf) keeps
 * today's chromeless rendering (the name is already the page title), while a
 * heterogeneous group renders each name ONCE as a group heading over its
 * distinguisher-led bands. A singleton cluster (a name with one band) still earns its
 * one heading, uniformly.
 *
 * Order is preserved on both axes: clusters appear in the order their name is first
 * seen, and bands within a cluster keep their input order. `name` is read as
 * `identityOf(band).name` (the same value `BandIdentity.name` carries) so the heading
 * text matches the hoisted-off identity.
 *
 * When headings ARE shown, a cluster holding a SINGLE multi-column band would have its
 * `bandLabeling([band])` fall through to the lone-band name fallback and lead that band
 * with `band.name` — which is ALSO the cluster heading, so the name would render twice
 * (#901 fix). In that case the band's label is re-led with its `distinguisher` (the
 * member slug, mono) so the name appears only in the heading — consistent with how a
 * multi-column member in a MULTI-band cluster already leads with its slug. */
export function clusterBands<T>(
  bands: readonly T[],
  identityOf: (band: T) => BandIdentity,
): { clusters: BandCluster<T>[]; showClusterHeadings: boolean } {
  const byName = new Map<string, T[]>();
  for (const band of bands) {
    const name = identityOf(band).name;
    const existing = byName.get(name);
    if (existing) {
      existing.push(band);
    } else {
      byName.set(name, [band]);
    }
  }
  const showClusterHeadings = byName.size > 1;
  const clusters = [...byName].map(([name, members]): BandCluster<T> => {
    const labeling = bandLabeling(members.map(identityOf));
    // With a heading shown, a lone multi-column band falls through to the name
    // fallback and leads with `band.name` — the SAME text as the cluster heading, so
    // it would render twice (#901). Re-lead that band with its distinguisher (the
    // member slug, mono) so the name shows only in the heading. Skip any band with no
    // distinguisher (no slug to lead with → keep the name fallback). A multi-band
    // cluster already leads with the distinguisher, so this only ever rewrites the
    // lone-band fallback case.
    if (showClusterHeadings) {
      labeling.bands = labeling.bands.map((label, i) => {
        if (label.primary.text !== name) {
          return label;
        }
        const identity = identityOf(members[i]);
        if (identity.distinguisher === "") {
          return label;
        }
        return {
          primary: { text: identity.distinguisher, mono: true },
          primaryIsColumn: identity.distinguisherIsColumn,
          primaryIsFacet: false,
        };
      });
    }
    return { name, bands: members, labeling };
  });
  return { clusters, showClusterHeadings };
}

/** The active period window the picker DIMS against, as an inclusive year pair
 * `[lo, hi]`, or `null` when there is no active window (every row reads as
 * in-window). Precedence mirrors the leaf's resolution: an active `?period` wire
 * wins (parsed to its outer year span — a single token via `periodTokenBounds`,
 * a `lo..hi` range via its endpoints, a `a,b` comma-union via the min/max of its
 * parts); else the global `StudyWindow` (already year ints). A `?period` that
 * doesn't parse to any bound (e.g. `_default`) falls back to the window, then to
 * null. simplify: year-grain overlap is deliberate — the dim is an at-a-glance
 * relevance cue, not the hard period gate (selection works on any row). */
export function pickerWindowYears(
  periodWire: string | null | undefined,
  window: { from: number; to: number } | null,
): [number, number] | null {
  if (periodWire) {
    let lo: number | null = null;
    let hi: number | null = null;
    for (const part of periodWire.split(",")) {
      const endpoints = periodRangeEndpoints(part) ?? [
        part.trim(),
        part.trim(),
      ];
      const loBounds = periodTokenBounds(endpoints[0]);
      const hiBounds = periodTokenBounds(endpoints[1]);
      const partLo = loBounds ? yearOf(loBounds.from) : null;
      const partHi = hiBounds ? yearOf(hiBounds.to) : null;
      if (partLo !== null) {
        lo = lo === null ? partLo : Math.min(lo, partLo);
      }
      if (partHi !== null) {
        hi = hi === null ? partHi : Math.max(hi, partHi);
      }
    }
    if (lo !== null && hi !== null) {
      return [lo, hi];
    }
  }
  return window ? [window.from, window.to] : null;
}

/** Whether a representation row's span overlaps an active year window (inclusive),
 * for the picker's out-of-window DIMMING. A null window (no active narrowing)
 * overlaps everything. The row's bounds carry the `9999-12-31` (open-ended) /
 * `0001-01-01` (unknown-start) sentinels — `yearOf` reads them as 9999 / 1, the
 * right unbounded ends for an overlap test (an open-ended row reaches past any
 * finite `hi`; an unknown-start row reaches before any finite `lo`). A row with
 * an unparseable bound (neither year resolves) is treated as in-window — never
 * dim a row we can't place. */
export function representationInWindow(
  row: { from: string; to: string },
  window: [number, number] | null,
): boolean {
  if (!window) {
    return true;
  }
  const rowFrom = yearOf(row.from);
  const rowTo = yearOf(row.to);
  if (rowFrom === null || rowTo === null) {
    return true;
  }
  return rowFrom <= window[1] && window[0] <= rowTo;
}

// ── Value-set-centric state fold (#668 — dogfooding M13/M18/M20) ──────────────
// A binding leaf's `variable_state` rows blow up by VINTAGE: `scb/rtb/kommun` has
// 415 states but only ~28 distinct value-set ids — which themselves collapse to
// ~21 classification editions. The leaf's multi-state view is value-SET-centric,
// not state-centric, and dedups at TWO levels (M13):
//   - a CLASSIFICATION value set (one with a `classification_slug`) dedups by
//     `classification_slug`, so the several `value_set_id`s SCB ships for one LKF
//     edition (lkf1980 ×2, lkf1995 ×3, …) collapse to ONE "= LKF 1980" row — they
//     are the same classification edition. It links out to the classification
//     instead of dumping its (huge) code list.
//   - a NON-classification value set (no slug) keeps per `value_set_id`: each id
//     is a genuinely distinct code list. When several share a `versionLabel`
//     (e.g. "Kommun historisk" ×22) the VIEW disambiguates the row with its
//     overall period span so the rows aren't indistinguishable.
// The usages of a collapsed classification row are the UNION of all states across
// its `value_set_id`s (the per-variant adjacent-year M20 collapse runs over that
// union). Pure projection, unit-tested in catalog.test.ts; the StatesView is
// presentational over the result.

/** A contiguous delivery-year run within one (value set, variant), collapsed
 * across ADJACENT years (#668 / dogfooding M20). The per-variable annual states
 * (AGI's design) merge into a range render-side — `from`/`to` are the run's outer
 * ISO bounds (the open-ended `9999-12-31` ceiling survives so `formatWindow`
 * renders "since …"). `changes` carries the #309 technical-schema transitions
 * that would otherwise disappear when adjacent states fold into one span. */
export interface ValueSetSpan {
  from: string;
  to: string;
  changes?: ValueSetTechnicalChange[];
}

/** A technical-schema transition inside one collapsed value-set span (#743). */
export interface ValueSetTechnicalChange {
  at: string;
  notes: string[];
}

/** One variant's usage of a distinct value set — the variant slug and its
 * adjacent-collapsed period spans. */
export interface ValueSetVariantUsage {
  variant: string;
  spans: ValueSetSpan[];
}

/** One distinct value set across a variable's states — the dedup unit of the
 * value-set-centric leaf view (#668). `key` is the STABLE dedup identity (a
 * `class/<slug>` for a classification edition, an `id/<value_set_id>` otherwise —
 * the `null` "no value set" bucket is `id/none`); the view keys its `{#each}` and
 * its local isolation on it (NOT a list index, which a FilterInput's filtered
 * slice would invalidate). `classificationSlug`, when set, makes this value set a
 * known classification (link out instead of dumping codes — and the dedup is by
 * slug, so several `value_set_id`s for one edition collapse here). `versionLabel`,
 * `valueSet`, `dataType`/`dataLength` come from the representative (first-seen)
 * state. `usages` lists which variants/spans use it (the UNION across a collapsed
 * classification's ids); `variants` is the flat distinct variant list (the
 * active-variant scope test). `overallSpan` is the entry's outer
 * min(valid_from)…max(valid_to) — the view's disambiguator when several
 * non-classification rows share a `versionLabel`. */
export interface DistinctValueSet {
  key: string;
  classificationSlug: string | null;
  versionLabel: string;
  valueSet: NonNullable<VariableStateModel["value_set"]> | null;
  dataType: string | null;
  dataLength: string | null;
  usages: ValueSetVariantUsage[];
  variants: string[];
  overallSpan: ValueSetSpan;
}

function displayTechnicalValue(value: string): string {
  const trimmed = value.trim();
  return trimmed === "" ? "(none)" : trimmed;
}

function technicalChangeNotes(
  prev: VariableStateModel,
  next: VariableStateModel,
): string[] {
  const notes: string[] = [];
  const prevType = formatDataType(prev.data_type, prev.data_length);
  const nextType = formatDataType(next.data_type, next.data_length);
  if (prevType !== nextType) {
    notes.push(
      `type ${displayTechnicalValue(prevType)} -> ${displayTechnicalValue(nextType)}`,
    );
  }
  const prevColumn = prev.delivery_column_name ?? "";
  const nextColumn = next.delivery_column_name ?? "";
  if (prevColumn !== nextColumn) {
    notes.push(
      `column ${displayTechnicalValue(prevColumn)} -> ${displayTechnicalValue(nextColumn)}`,
    );
  }
  return notes;
}

function appendTechnicalChanges(
  span: ValueSetSpan,
  prev: VariableStateModel,
  next: VariableStateModel,
): void {
  // Same-state expanded windows (#319) and overlapping co-delivered alternatives
  // are not before-after transitions; folding may still combine them for display,
  // but a "changed" hint would mis-describe them as succession.
  if (prev.state_id === next.state_id || prev.valid_to >= next.valid_from) {
    return;
  }
  const notes = technicalChangeNotes(prev, next);
  if (notes.length === 0) {
    return;
  }
  span.changes = [...(span.changes ?? []), { at: next.valid_from, notes }];
}

/** Collapse a variant's states (already filtered to one value set) into
 * adjacent-year spans (#668 / M20): order by `valid_from`, then merge a state into
 * the open span when it is contiguous with it — its `valid_from` is at or before
 * the day after the running `valid_to` (so back-to-back annual states `…-12-31`
 * → `…-01-01`, and any overlap, fuse; a real gap year starts a new span). ISO
 * `YYYY-MM-DD` strings compare chronologically. The caller groups by (value set,
 * variant) — NOT by delivery column (a merged monthly-family value set fuses
 * across its 12 month columns by design) — so this tests ONLY time-adjacency; the
 * #743 technical-field transitions are attached as notes instead of splitting the
 * value-set row. */
function collapseSpans(states: VariableStateModel[]): ValueSetSpan[] {
  const ordered = [...states].sort(
    (a, b) =>
      a.valid_from.localeCompare(b.valid_from) ||
      a.state_id - b.state_id ||
      a.valid_to.localeCompare(b.valid_to),
  );
  const spans: ValueSetSpan[] = [];
  let previous: VariableStateModel | null = null;
  let previousAmbiguous = false;
  for (const s of ordered) {
    const open = spans.at(-1);
    // An already-open OPEN-ENDED span swallows everything after it: its ceiling
    // is the `9999-12-31` sentinel ("still delivered"), so any later state of the
    // same (value set, variant) is contiguous with it by definition. Handled
    // explicitly because `dayAfter("9999-12-31")` would overflow into year 10000
    // (`Date.toISOString()`'s `±YYYYYY` expanded form sorts BELOW real dates),
    // which would wrongly split a second still-delivered state into its own span.
    if (open && open.to === OPEN_ENDED_VALID_TO) {
      if (previous && !previousAmbiguous) {
        appendTechnicalChanges(open, previous, s);
      }
      continue;
    }
    // Contiguous (or overlapping) with the open span → extend it. The day-after
    // test fuses back-to-back annual windows (`2019-12-31` then `2020-01-01`)
    // without merging across a skipped year (`2019-12-31` then `2021-01-01`).
    if (open && s.valid_from <= dayAfter(open.to)) {
      if (previous && !previousAmbiguous) {
        appendTechnicalChanges(open, previous, s);
      }
      if (s.valid_to > open.to) {
        open.to = s.valid_to;
        previous = s;
        previousAmbiguous = false;
      } else if (s.valid_to === open.to && s.valid_from <= open.to) {
        // Two co-delivered alternatives reach the same span end; a later
        // successor has no single predecessor for a technical-change hint.
        previousAmbiguous = true;
      }
    } else {
      spans.push({ from: s.valid_from, to: s.valid_to });
      previous = s;
      previousAmbiguous = false;
    }
  }
  return spans;
}

/** The ISO day after an `YYYY-MM-DD` date — the adjacency boundary for
 * `collapseSpans` (two annual states are adjacent iff the later starts on or
 * before this). Uses a UTC `Date` so month/year rollovers are correct. The
 * open-ended `9999-12-31` ceiling is NOT passed here (`collapseSpans` short-
 * circuits an open span before calling), so the +1-day year-10000 overflow it
 * would produce never reaches the adjacency test. A non-parseable bound returns
 * the input unchanged (defensive on a malformed/edge wire — it then only fuses
 * exact-equal bounds). */
function dayAfter(iso: string): string {
  const ms = Date.parse(`${iso}T00:00:00Z`);
  if (Number.isNaN(ms)) {
    return iso;
  }
  return new Date(ms + 86_400_000).toISOString().slice(0, 10);
}

/** The TWO-LEVEL dedup key for a state's value set (M13): a classification value
 * set keys by `class/<slug>` so an edition's several `value_set_id`s collapse to
 * one entry; a non-classification one keys by `id/<value_set_id>` (`id/none` for
 * the null "no value set" bucket) so each distinct code list stays its own row.
 * The `class/` vs `id/` prefixes keep the two namespaces from ever colliding. */
function valueSetDedupKey(s: VariableStateModel): string {
  return s.classification_slug
    ? `class/${s.classification_slug}`
    : `id/${s.value_set_id ?? "none"}`;
}

/** Project a variable's multi-state set into DISTINCT value sets (#668), deduped
 * at TWO levels: classification value sets by `classification_slug`, others by
 * `value_set_id` (see `valueSetDedupKey` and the section header). First-seen order
 * is preserved so the list is stable; within each entry the states group by
 * variant and collapse adjacent years (M20) over the UNION of all states in the
 * bucket (so a collapsed classification edition's usages span its ids). The
 * representative (first-seen) state supplies the version label / value set / data
 * type. Pure — unit-tested. */
export function distinctValueSets(
  states: VariableStateModel[],
): DistinctValueSet[] {
  const byKey = new Map<string, VariableStateModel[]>();
  for (const s of states) {
    const k = valueSetDedupKey(s);
    const group = byKey.get(k);
    if (group) {
      group.push(s);
    } else {
      byKey.set(k, [s]);
    }
  }
  return [...byKey.entries()].map(([key, group]) => {
    const rep = group[0];
    const byVariant = statesByVariant(group);
    const usages: ValueSetVariantUsage[] = [...byVariant.entries()].map(
      ([variant, ss]) => ({ variant, spans: collapseSpans(ss) }),
    );
    // The entry's outer window across ALL its states — the view's disambiguator
    // when several non-classification rows share a version label.
    const overallSpan: ValueSetSpan = {
      from: group.reduce(
        (m, s) => (s.valid_from < m ? s.valid_from : m),
        rep.valid_from,
      ),
      to: group.reduce(
        (m, s) => (s.valid_to > m ? s.valid_to : m),
        rep.valid_to,
      ),
    };
    return {
      key,
      classificationSlug: rep.classification_slug ?? null,
      versionLabel: rep.value_set_version_label,
      valueSet: rep.value_set,
      dataType: rep.data_type,
      dataLength: rep.data_length,
      usages,
      variants: usages.map((u) => u.variant),
      overallSpan,
    };
  });
}

/** Humanize a classification slug for the value-set label (#668): the clean
 * vintage form `<letters><4-digit-year>` (e.g. `lkf2007`) becomes
 * `"<LETTERS-UPPER> <year>"` (`"LKF 2007"`); anything else (suffixed/hyphenated
 * slugs like `sun2000-niva`, `icd-10-se`, or a non-vintage slug) falls back to
 * the raw slug verbatim — a stable identifier beats a mangled guess. */
export function humanizeClassificationSlug(slug: string): string {
  const m = /^([a-z]+)(\d{4})$/.exec(slug);
  return m ? `${m[1].toUpperCase()} ${m[2]}` : slug;
}

// ── State-window display (#309 + #321) ──────────────────────────────────────

/** The open-ended `variable_state.valid_to` sentinel (the reg_meta_build DDL
 * default) — never shown to the user (#309). */
export const OPEN_ENDED_VALID_TO = "9999-12-31";

/** The yearless-fallback `variable_state.valid_from` sentinel — the floor
 * reg_meta_build writes when a state's start year is unknown (`_VALID_FROM_UNKNOWN`
 * in `reg_meta_build/db.py`; the `0001` twin of the `9999` ceiling, per
 * `reg_meta.queries`). Like the ceiling it must NOT read as a literal year:
 * coverage `from` is unbounded/unknown for it, never year 1 (which would let the
 * slider emit out-of-grammar wires like `1..2026`). */
export const YEARLESS_VALID_FROM = "0001-01-01";

/** Group state rows by their variant slug, preserving input order — shared by
 * the add planner (#306) and the value-set-centric fold (#668). */
function statesByVariant(
  states: VariableStateModel[],
): Map<string, VariableStateModel[]> {
  const byVariant = new Map<string, VariableStateModel[]>();
  for (const s of states) {
    const group = byVariant.get(s.variant);
    if (group) {
      group.push(s);
    } else {
      byVariant.set(s.variant, [s]);
    }
  }
  return byVariant;
}

/**
 * The display form of a validity window (#309/#321):
 *  - open-ended (the `9999-12-31` sentinel) → `"since <from>"` (year-collapsed
 *    when Jan-1-aligned);
 *  - yearless-start (the `0001-01-01` sentinel, an unknown start) with a finite
 *    end → a one-sided `"until <to>"` (year-collapsed when Dec-31-aligned) — the
 *    mirror of `"since"`, so a known end year is shown instead of leaking the
 *    sentinel as `"0001 – <to>"` (#658);
 *  - a closed window with a SINGLE period token (the coarsest grain that
 *    exactly covers it — `"2009"`, `"VT2009"`, `"2009-Q3"`, `"2020-02"`) →
 *    that token, so two same-year sub-annual siblings never both read as the
 *    bare year (#271's display mandate);
 *  - otherwise (a multi-year window, whose token is the explicit `lo..hi`
 *    range; an edge window with no token; or a payload predating the
 *    `period_token` field — one edge-cache generation of skew tolerated, the
 *    #317 rule) → `"<from> – <to>"`, collapsed to bare years ONLY when BOTH
 *    bounds are year-aligned (`1992-01-01 – 2009-12-31` → `1992 – 2009`). A
 *    one-sided collapse would read backwards for a stale-cached sub-annual
 *    window (`2009-07-01 – 2009`), so a partially-aligned window keeps both
 *    exact dates.
 * The raw ISO window stays available to the UI via a `title` tooltip.
 */
export function formatWindow(
  validFrom: string,
  validTo: string,
  periodToken?: string | null,
): string {
  if (validTo === OPEN_ENDED_VALID_TO) {
    return `since ${validFrom.endsWith("-01-01") ? validFrom.slice(0, 4) : validFrom}`;
  }
  // Unknown start (the yearless floor) with a finite end → a one-sided
  // `"until <to>"`, the mirror of `"since"` above. Checked AFTER `since` so a
  // wholly-unbounded `0001..9999` window keeps the existing open-ended form
  // rather than reading `"until 9999"`; the closed-window branch below would
  // otherwise leak the sentinel as `"0001 – <to>"` (#658).
  if (validFrom === YEARLESS_VALID_FROM) {
    return `until ${validTo.endsWith("-12-31") ? validTo.slice(0, 4) : validTo}`;
  }
  if (
    typeof periodToken === "string" &&
    periodToken !== "" &&
    !periodToken.includes("..")
  ) {
    return periodToken;
  }
  if (validFrom.endsWith("-01-01") && validTo.endsWith("-12-31")) {
    const fromYear = validFrom.slice(0, 4);
    const toYear = validTo.slice(0, 4);
    return fromYear === toYear ? fromYear : `${fromYear} – ${toYear}`;
  }
  return `${validFrom} – ${validTo}`;
}

/** The "showing N of M" caption when the displayed slice is smaller than the full
 * match count (N = rendered rows, M = total before the server's per-request
 * limit), else null (don't caption a complete group). */
export function showingOf(shown: number, total: number): string | null {
  return shown < total ? `showing ${shown} of ${total}` : null;
}

/** `formatWindow` over a state row (its bounds + backend token). */
export function formatStateWindow(s: VariableStateModel): string {
  return formatWindow(s.valid_from, s.valid_to, s.period_token);
}

/** The exact-dates tooltip for a window — the sentinel reads "open-ended"
 * rather than leaking the raw 9999-12-31 (Codex P2 on #335). */
export function windowTitle(validFrom: string, validTo: string): string {
  return `${validFrom} – ${validTo === OPEN_ENDED_VALID_TO ? "open-ended" : validTo}`;
}

/** The period grains a variable's states actually exhibit (#308 option b) —
 * pre-narrows the range picker's grain select. Year is always offered (the
 * coarse query everyone understands); finer grains come from the states'
 * `period_token`s (the #321 coarsest-exact tokens — defensive on a stale
 * payload missing them, the #317 rule: degrade to year-only). Coarse → fine. */
export function grainsFromStates(states: VariableStateModel[]): PeriodGrain[] {
  const found = new Set<PeriodGrain>(["year"]);
  for (const s of states) {
    const token = s.period_token;
    if (typeof token === "string" && !token.includes("..")) {
      const grain = grainOfToken(token);
      if (grain) {
        found.add(grain);
      }
    }
  }
  return PERIOD_GRAINS.filter((g) => found.has(g));
}

/** The subject's data-availability span as a year-grain `Coverage` (#615),
 * derived from the EMBEDDED states: `from` = year of the earliest finite
 * `valid_from`, `to` = year of the latest finite `valid_to`. The two sides are
 * INDEPENDENT — each sentinel only unbounds ITS OWN side, never the whole span:
 *  - the open-ended sentinel (`9999-12-31`) means "still delivered" → an
 *    unbounded END (`to: null`), so the track never balloons past the slider
 *    (the picker treats null-to as "reaches the present");
 *  - the yearless-fallback floor (`0001-01-01`) means "start unknown" → an
 *    unbounded START (`from: null`), never year 1 (which would let the slider
 *    emit out-of-grammar wires like `1..2026`).
 * A `0001..2008` state therefore yields `{from: null, to: 2008}` — the finite
 * END is PRESERVED so a selection past it still flags "Not delivered after
 * 2008" (the round-1 regression: dropping the WHOLE span to null suppressed
 * that gap). Returns null only when BOTH bounds are unknown (a cold/empty node,
 * or one whose only bounds are both sentinels) — the picker then has no
 * availability track to draw and softens the deviation hint.
 *
 * This is the `coverage_from`/`coverage_to` of #611's Period model computed
 * client-side from already-embedded data (no backend field): the leaf node
 * never carries the per-register `RegisterCoverageModel`, and the spec is
 * explicit that leaf coverage is derived from the states. */
export function coverageFromStates(
  states: VariableStateModel[],
): Coverage | null {
  let from: number | null = null;
  let to: number | null = null;
  for (const s of states) {
    // The yearless-fallback floor (`0001-01-01`) is "start unknown", not year 1
    // — skip it so it never floors `from`; the side stays unbounded (null).
    const fromYear =
      s.valid_from === YEARLESS_VALID_FROM ? null : yearOf(s.valid_from);
    if (fromYear !== null && (from === null || fromYear < from)) {
      from = fromYear;
    }
    // The open-ended sentinel is "still delivered" → the END stays unbounded
    // (null), the picker projects it to the vintage ceiling for the track.
    const toYear =
      s.valid_to === OPEN_ENDED_VALID_TO ? null : yearOf(s.valid_to);
    if (toYear !== null && (to === null || toYear > to)) {
      to = toYear;
    }
  }
  // BOTH bounds unknown (e.g. a wholly open-ended-on-both-sides state) → no
  // coverage track to draw, like an empty/bound-less set. A single finite side
  // is enough to draw + gap against.
  if (from === null && to === null) {
    return null;
  }
  return { from, to };
}

/** The 4-digit year of an ISO `YYYY-MM-DD` bound as an int, or null when it
 * isn't a leading-4-digit string (a blank/edge bound on a stale payload). Shared
 * with `history_graph.ts`'s `orderKey` (one home for the leading-year regex). */
export function yearOf(iso: string): number | null {
  const m = /^(\d{4})/.exec(iso ?? "");
  return m ? Number.parseInt(m[1], 10) : null;
}

/** A per-member coverage span as it rides on the wire (`VariableCoverageModel`):
 * ISO `coverage_from`/`coverage_to` (`null` when unknown) + the open-ended flag.
 * Structural so callers needn't import the schema alias. */
export interface MemberCoverage {
  coverage_from?: string | null;
  coverage_to?: string | null;
  open_ended: boolean;
}

/** The UNION data-availability span (#638 PR2a) over a group's member coverages,
 * as a year-grain `Coverage` for the period picker's availability lens:
 *  - `from` = the earliest finite member `coverage_from` year (null when none
 *    has a finite start);
 *  - `to` = the latest finite member `coverage_to` year — UNLESS any member is
 *    open-ended (or carries a null `coverage_to`), which unbounds the END
 *    (`to: null` = "still delivered"), mirroring `coverageFromStates`'s
 *    open-ended sentinel handling so the slider projects it to the vintage.
 * Members with null coverage (stateless) are skipped. Returns null when no
 * member contributes a finite bound AND none is open-ended (nothing to draw or
 * gap against — the picker softens the deviation hint). */
export function memberCoverageUnion(
  coverages: readonly (MemberCoverage | null | undefined)[],
): Coverage | null {
  let from: number | null = null;
  let to: number | null = null;
  let openEnded = false;
  for (const cov of coverages) {
    if (!cov) {
      continue;
    }
    // A stateless member's payload is `{null, null, false}` (not null) — it
    // carries no span, so treat it like null coverage. WITHOUT this, its null
    // `coverage_to` would wrongly trip the open-ended branch below and unbound
    // the WHOLE union END (the union track then runs to the vintage even when
    // every finite member ends earlier). A finite `coverage_from` WITH a null
    // `coverage_to` is a GENUINE open-ended member and still falls through.
    if (
      cov.coverage_from == null &&
      cov.coverage_to == null &&
      !cov.open_ended
    ) {
      continue;
    }
    // The yearless-fallback floor (`0001-01-01`) is "start unknown", not year 1
    // — skip it so it never floors the union `from` (mirrors `coverageFromStates`).
    const fromYear =
      cov.coverage_from && cov.coverage_from !== YEARLESS_VALID_FROM
        ? yearOf(cov.coverage_from)
        : null;
    if (fromYear !== null && (from === null || fromYear < from)) {
      from = fromYear;
    }
    // An open-ended member (or one with no finite end) unbounds the union END.
    if (cov.open_ended || cov.coverage_to == null) {
      openEnded = true;
    } else {
      const toYear = yearOf(cov.coverage_to);
      if (toYear !== null && (to === null || toYear > to)) {
        to = toYear;
      }
    }
  }
  const unionTo = openEnded ? null : to;
  if (from === null && unionTo === null) {
    return null;
  }
  return { from, to: unionTo };
}

// ── Shared binding resolution (picker derive-on-pick + store re-derive) ──────
// ONE resolution path, used by BOTH the CatalogPicker's derive-on-pick AND the
// store's re-derive-on-(period/variant)-change (B2, UI audit). Keeping it here —
// not inlined in the picker — is what lets the store re-resolve every binding of a
// source through the IDENTICAL logic when the source's period/variant changes, so a
// picked binding never goes silently stale.

/** Why a binding could not be resolved to a real type at the source's
 * (period, variant). Drives the BindingEditor's "unresolved" marker (B2.3): an
 * honest "set the period" / "no data here" cue instead of dressing the opaque
 * fallback as a derived type. */
export type UnresolvedReason = "period-unset" | "no-states" | "not-a-leaf";

/** The outcome of resolving one binding's variable at a (period, variant).
 *  - `derived`: a single representation → type + display-name default ready to apply.
 *  - `ambiguous`: >1 co-existing delivery column → the author must pick a
 *    representation (only the picker's chooser can; the store can't auto-pick, so
 *    it surfaces a non-blocking hint and leaves the existing value).
 *  - `unresolved`: resolution impossible (no period / no covering state). */
export type BindingResolution =
  | {
      kind: "derived";
      type: string;
      displayNameDefault: string | null;
      representation: string | null;
    }
  | { kind: "ambiguous"; fqid: string; states: VariableStateModel[] }
  | { kind: "unresolved"; reason: UnresolvedReason };

/** The payload the CatalogPicker hands back on a pick (the BindingEditor applies it
 * through the store). It carries the ground-truth resolution `kind` from
 * `resolveBindingAt` so the consumer NEVER re-infers status from value tells (a
 * genuinely-derived `opaque` with a null delivery column would otherwise be
 * mislabeled "unresolved"). `unresolvedReason` rides along only when the resolve
 * could not produce a type (period unset / no covering state) — the opaque
 * fallback the picker wrote. */
export interface PickedVariable {
  variable: string;
  type: string;
  displayNameDefault: string | null;
  // The chosen REPRESENTATION (delivery column) when the concept has >1 at the
  // period; null/undefined when there is a single representation.
  representation?: string | null;
  /** Ground truth from `resolveBindingAt`: `derived` (a real type was resolved) or
   * `unresolved` (the opaque fallback). `ambiguous` never reaches a pick — the
   * picker's chooser resolves it to a concrete representation (`derived`) first. */
  resolution: "derived" | "unresolved";
  /** Why unresolved (only when `resolution === "unresolved"`). */
  unresolvedReason?: UnresolvedReason;
}

/**
 * Resolve `fqid` at the source's (`period`, `variant`) through the catalog
 * `?period` resolve — the SINGLE source of truth for derive-on-pick AND store
 * re-derive. A null/blank period is `unresolved` ("period-unset") WITHOUT a fetch
 * (the resolve needs a period). A leaf that yields no covering state is
 * `no-states`; a non-leaf payload is `not-a-leaf` (shouldn't happen with
 * `?period`). >1 co-existing representation is `ambiguous` (deferred to the picker
 * chooser); exactly one is `derived` with the prefill. A network/422 throws — the
 * caller owns the error surface (the picker shows `resolveError`; the store stores
 * a per-binding error hint). */
export async function resolveBindingAt(
  fqid: string,
  period: string | null,
  variant: string,
): Promise<BindingResolution> {
  if (!period) {
    return { kind: "unresolved", reason: "period-unset" };
  }
  const resolved = await getCatalogNode(fqid, {
    period,
    variant: variant || undefined,
  });
  if (isCatalogNode(resolved) || resolved.states.length === 0) {
    return {
      kind: "unresolved",
      reason: isCatalogNode(resolved) ? "not-a-leaf" : "no-states",
    };
  }
  const reps = representationsFromStates(resolved.states);
  if (reps.length > 1) {
    return { kind: "ambiguous", fqid, states: resolved.states };
  }
  const first = resolved.states[0];
  return {
    kind: "derived",
    type: deriveType(first),
    displayNameDefault: first.delivery_column_name ?? null,
    representation: null,
  };
}
