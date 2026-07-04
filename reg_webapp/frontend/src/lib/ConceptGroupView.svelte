<script lang="ts">
import {
  type ConceptGroupNodeMember,
  type GroupFacetModel,
  getConceptGroup,
  getConceptGroupGraph,
  type VariableGraphNode,
} from "./api";
import { asyncResource } from "./async.svelte";
import {
  addWindowBounds,
  type BindingResolution,
  bindingFieldsFromResolution,
  catalogHref,
  facetLabelJoin,
  type PickerRepresentation,
  pickerRepresentations,
  pickerWindowYears,
  registerPrefixOf,
  resolveBindingAt,
  rowAddPeriod,
  windowsAddPeriod,
  windowsOverlapWindow,
} from "./catalog";
import PeriodPicker from "./PeriodPicker.svelte";
import {
  clampYearPeriodWire,
  clampYearWindow,
  isStructurallyValidPeriodWire,
  periodFromWire,
  periodToWire,
} from "./period";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store.svelte";
import RepresentationPicker, {
  type PickerApplyPayload,
  type PickerBand,
  type PickerSelection,
} from "./RepresentationPicker.svelte";
import { router } from "./router.svelte";
import SubjectView from "./SubjectView.svelte";
import {
  committedPickerRows,
  finalSourcePeriodsForStagedAdds,
  periodChangesWithStagedAdds,
  rowRegisterVariantForVariant,
  sourcePeriodsFromDraft,
  stagedRemoveForCommitted,
} from "./staged_picker";
import TechnicalDetails from "./TechnicalDetails.svelte";
import { windowStore } from "./window.svelte";

// The concept-group SUBJECT page (#617, #678 inc 2): fetches a group by (provider,
// register, key) — its members + facets — AND the group's relationship graph (the
// union of its members' variable nodes, each carrying that variable's states). It
// renders the members as a vertical stack of REPRESENTATION BANDS (one per member
// variable, each its own adaptive representation rows) under ONE shared staged diff
// and ONE "Apply" footer — the group→variable→representation nesting (#678/#995).
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
  // #1037: steward-aware slider floor (App → here, mirroring CatalogNodeView).
  windowMinYear,
  // #631: the true catalog vintage year, used by open-ended graph timelines.
  vintageYear,
  // #1037: steward-aware period-control ceiling. Defaults to `vintageYear` for
  // direct component callers that are outside App's steward context.
  windowMaxYear = vintageYear,
  enforcePeriodBounds = false,
}: {
  provider: string;
  register: string;
  key: string;
  regMetaVersion: string;
  steward: string;
  windowMinYear: number;
  vintageYear?: number;
  windowMaxYear?: number;
  enforcePeriodBounds?: boolean;
} = $props();

const periodCeilingYear = $derived(windowMaxYear ?? new Date().getFullYear());

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
      for (const alias of n.same_as) {
        map.set(alias.fqid, n);
      }
    }
  }
  return map;
});

// ── Shared concept definition / description (#678, #900) ─────────────────────
// The group page's missing descriptive context: each MEMBER variable node carries
// its own `definition`/`description` (the variable-level concept text). Most
// parallel-column siblings carry null — that's expected — but the canonical member
// usually carries the one definition the whole group shares. Collect the DISTINCT
// non-empty values across the MEMBER nodes (scoped to `node.members`, so a
// succession neighbour the graph union pulls in never leaks its text here),
// then return the LONE shared value, or null.
//
// #900: a group-level definition is only honest when members genuinely AGREE — one
// shared value. Zero non-empty values → render nothing. MORE than one distinct value
// means the members DISAGREE (heterogeneous curated groups carry near-duplicate
// per-member text), so rendering each at the group level misrepresents member text as
// concept text — instead render NOTHING and rely on the per-member leaf pages (the
// picker bands link to them). Surfacing distinct per-column text on the bands is gated
// on build #892.
function sharedMemberMeta(field: "definition" | "description"): string | null {
  if (!node) {
    return null;
  }
  const seen = new Set<string>();
  for (const member of node.members) {
    const value = nodesByFqid.get(member.fqid)?.[field]?.trim();
    if (value) {
      seen.add(value);
    }
  }
  return seen.size === 1 ? [...seen][0] : null;
}
const sharedDefinition = $derived(sharedMemberMeta("definition"));
const sharedDescription = $derived(sharedMemberMeta("description"));

/** The in-this-group facet label for a member (e.g. "AGI · 2007 SNI edition"),
 * joined from its facets, or null when the member carries none (an ungrouped /
 * axis-less member — its name suffices). */
function memberFacetLabel(member: ConceptGroupNodeMember): string | null {
  return member.facets.length > 0 ? facetLabelJoin(member.facets) : null;
}

function hasZeroStateCoverage(member: ConceptGroupNodeMember): boolean {
  const coverage = member.coverage;
  return (
    member.delivery_column !== null &&
    member.delivery_column !== undefined &&
    coverage !== null &&
    coverage !== undefined &&
    coverage.state_count === 0 &&
    coverage.coverage_from === null &&
    coverage.coverage_to === null &&
    !coverage.open_ended
  );
}

function neverDeliveredRow(
  deliveryColumn: string,
  seed: PickerRepresentation | undefined,
): PickerRepresentation {
  const variant = seed?.variant ?? "_default";
  return {
    key: `${variant}::${deliveryColumn}`,
    variant,
    variantLabel: seed?.variantLabel ?? "default",
    column: deliveryColumn,
    representation: deliveryColumn,
    from: "9999-12-31",
    to: "0001-01-01",
    windows: [],
    period: "not delivered",
    wirePeriod: null,
    valueSetLabel: seed?.valueSetLabel ?? "",
    codingsVary: false,
    selectable: false,
    renamedColumns: [],
  };
}

/** The member's leaf-page href, carrying the active group `?period` when set (#678):
 * narrowing the group with the PeriodPicker then opening a member keeps that same
 * window on the leaf, rather than opening it at full history. A null period (no
 * narrowing) yields the bare catalog href. `period` is read reactively so the bands
 * recompute when the window changes. (A hoisted function so the `bands` derive, above
 * the `period` declaration, can call it — `period` is read at call time, not bind
 * time.) */
function memberHref(fqid: string): string {
  const base = catalogHref(fqid);
  return period ? `${base}?period=${encodeURIComponent(period)}` : base;
}

/** The DELIVERY-COLUMN filter per fqid (#678): the set of `delivery_column`s the
 * GROUP's members for that fqid actually address, or `null` (no filter — expose
 * every column) when ANY such member is a WHOLE-VARIABLE member
 * (`delivery_column == null`). The graph node now carries the variable's FULL column
 * set (every distinct delivery column — backend `_graph_states` fix), so a
 * representation group whose members are only a SUBSET of those columns would
 * otherwise expose NON-member columns as selectable rows — letting a user add a
 * column OUTSIDE the concept being browsed. Restricting the band's input states to
 * the group's member columns closes that. A whole-variable member means the concept
 * IS the whole variable, so all columns are legitimately selectable → no filter. In a
 * filtered-STEWARD catalog "all columns" is already the steward's HELD columns: the
 * group `/graph` route narrows each member variable's states to the held set (backend
 * `_narrow_graph_to_held`, #678), so a whole-variable member admitted only because the
 * steward holds SOME of its columns no longer leaks the non-held ones here. */
const memberColumnsByFqid = $derived.by(() => {
  const map = new Map<string, Set<string> | null>();
  for (const member of node?.members ?? []) {
    const existing = map.get(member.fqid);
    if (existing === null) {
      continue; // already whole-variable (no filter) — stays no-filter
    }
    if (member.delivery_column == null) {
      map.set(member.fqid, null); // whole-variable member → expose all columns
    } else {
      const set = existing ?? new Set<string>();
      set.add(member.delivery_column);
      map.set(member.fqid, set);
    }
  }
  return map;
});

const suppressRowDimensionFilters = $derived(
  (node?.axes.length ?? 0) === 0 || node?.source === "curated",
);

const foldSuccessionBands = $derived((node?.axes.length ?? 0) === 0);

/** The inter-variable SUCCESSION fold (#902): for AXIS-LESS groups, the group graph's
 * `succession` edges collapse predecessor→successor pairs whose BOTH endpoints are
 * members of this group, so a superseded edition is NOT a co-equal band — the chain
 * HEAD (the latest edition) leads and its predecessor(s) become quiet history on the
 * head band. Faceted groups still surface the same history, but keep predecessor
 * members selectable: their axes are the user's browse surface, and hiding an older
 * member can make period-specific columns unreachable.
 *
 * Edge direction (reg_meta `graph.py`): `source` is the PREDECESSOR (older), `target`
 * the SUCCESSOR (newer), and `effective_year` the year the source was replaced by the
 * target. So a member that is ANY in-group edge's `source` is SUPERSEDED; a member that
 * is only ever a `target` (never a source) is the chain head that leads. Chains of
 * length >2 (A→B→C) fold transitively: B and A are both superseded, C leads with both as
 * history. An edge endpoint OUTSIDE the group (a partial chain) is ignored — only pairs
 * where both endpoints are group members fold; a member with an out-of-group successor
 * stays a normal band.
 *
 * Yields `{ superseded, historyByHead }`: the set of member fqids to DROP as co-equal
 * bands, and per chain-head fqid the ordered (oldest-first) superseded predecessors to
 * surface as history. */
const successionFold = $derived.by(() => {
  const memberFqids = new Set((node?.members ?? []).map((m) => m.fqid));
  const edges = (graph?.edges ?? []).filter(
    (e) =>
      e.kind === "succession" &&
      memberFqids.has(e.source) &&
      memberFqids.has(e.target),
  );
  // successor → its predecessor edges (to walk a chain back from its head).
  // `effective_year` rides on the predecessor (the year it was replaced by the
  // successor).
  const predecessorsOf = new Map<
    string,
    { fqid: string; effectiveYear: number | null }[]
  >();
  const superseded = new Set<string>();
  for (const e of edges) {
    superseded.add(e.source); // a source (predecessor) is superseded
    const preds = predecessorsOf.get(e.target) ?? [];
    preds.push({ fqid: e.source, effectiveYear: e.effective_year ?? null });
    predecessorsOf.set(e.target, preds);
  }
  // Chain heads: members that participate in succession but are never superseded (only
  // ever a target). Walk each head back through its predecessors, depth-first, so the
  // history lists the whole chain (A→B→C ⇒ C's history is [A, B]); order oldest-first.
  const historyByHead = new Map<
    string,
    { fqid: string; effectiveYear: number | null }[]
  >();
  for (const fqid of memberFqids) {
    if (superseded.has(fqid) || !predecessorsOf.has(fqid)) {
      continue; // not a head (it's superseded, or has no in-group predecessor)
    }
    const chain: { fqid: string; effectiveYear: number | null }[] = [];
    const visited = new Set<string>([fqid]);
    const walk = (target: string): void => {
      for (const pred of predecessorsOf.get(target) ?? []) {
        if (visited.has(pred.fqid)) {
          continue; // defend against a cyclic edge set (shouldn't occur)
        }
        visited.add(pred.fqid);
        walk(pred.fqid); // older predecessors first
        chain.push(pred);
      }
    };
    walk(fqid);
    historyByHead.set(fqid, chain);
  }
  return { superseded, historyByHead };
});

/** The display name for a superseded predecessor fqid: its graph node label, else its
 * member name, else the leaf slug — so the history entry reads a human name. */
function predecessorName(fqid: string): string {
  const member = node?.members.find((m) => m.fqid === fqid);
  return nodesByFqid.get(fqid)?.label ?? member?.name ?? leafSlug(fqid);
}

/** The picker bands — one per DISTINCT member variable (by fqid), in the group's
 * member order. Each band pulls its representation rows from the matching graph
 * node's states, RESTRICTED to the group's member delivery columns for that fqid
 * (`memberColumnsByFqid`) so a column outside the browsed concept is never
 * selectable; a member with no graph node (graph error / partial union) still
 * renders a band with EMPTY rows (the band shows a quiet "no representations" rather
 * than dropping the member). The facet label distinguishes representation members
 * collapsed onto one fqid; the band's adaptive rows surface the delivery-column
 * split. A member superseded by an in-group succession edge (#902) is DROPPED as a
 * co-equal band — it rides as history on the chain head instead. */
const bands = $derived.by((): PickerBand[] => {
  if (!node) {
    return [];
  }
  // delivery_column → human facet label, across ALL members of each fqid (#678): a
  // representation group carries several members on one variable, each a distinct
  // delivery_column with its own facet (CDISP "Inkl. kapitalvinst" / CDISP5 "Exkl.
  // kapitalvinst"). The band is built per DISTINCT fqid (first member wins below), so
  // the later members' facet labels would otherwise never reach their rows — collect
  // them here so the picker can show the human facet per column.
  const facetByColumnByFqid = new Map<string, Record<string, string>>();
  // delivery_column → the member's STRUCTURED facets (#908): the per-axis (axis,
  // value, label) tuples the picker's dimension marking + per-axis filter read.
  // Same per-(fqid, column) keying as the joined-label map above — first member
  // wins — but carries the raw facets so the picker can group/filter by axis.
  const facetsByColumnByFqid = new Map<
    string,
    Record<string, GroupFacetModel[]>
  >();
  // fqid → the BAND-LEVEL facets of a WHOLE-VARIABLE faceted member (#908 C1): a
  // member whose `delivery_column` is null carries facets that can't key by column
  // (e.g. a month-faceted group — one variable per month, each carrying a `month`-axis
  // facet on the whole variable). Those facets apply to ALL the band's rows. First
  // such member per fqid wins. Distinct from the per-column path below, which is keyed
  // by delivery_column and so only sees members WITH a column.
  const bandFacetsByFqid = new Map<string, GroupFacetModel[]>();
  const membersByFqid = new Map<string, ConceptGroupNodeMember[]>();
  for (const member of node.members) {
    const membersForFqid = membersByFqid.get(member.fqid) ?? [];
    membersForFqid.push(member);
    membersByFqid.set(member.fqid, membersForFqid);
    // A whole-variable (null delivery_column) faceted member contributes its facets
    // band-level, then is skipped for the per-COLUMN maps (which key by column).
    if (member.delivery_column == null) {
      if (member.facets.length > 0 && !bandFacetsByFqid.has(member.fqid)) {
        bandFacetsByFqid.set(member.fqid, member.facets);
      }
      continue;
    }
    if (member.facets.length > 0) {
      const fmap = facetsByColumnByFqid.get(member.fqid) ?? {};
      fmap[member.delivery_column] ??= member.facets;
      facetsByColumnByFqid.set(member.fqid, fmap);
    }
    const facet = memberFacetLabel(member);
    if (facet == null) {
      continue;
    }
    const map = facetByColumnByFqid.get(member.fqid) ?? {};
    // First non-null facet per (fqid, column) wins — a column's facet is stable across
    // the group's members; later duplicates (shouldn't occur) don't override.
    map[member.delivery_column] ??= facet;
    facetByColumnByFqid.set(member.fqid, map);
  }
  const { superseded, historyByHead } = successionFold;
  const seen = new Set<string>();
  const out: PickerBand[] = [];
  for (const member of node.members) {
    if (seen.has(member.fqid)) {
      continue;
    }
    seen.add(member.fqid);
    // In axis-less groups, a member superseded by an in-group succession edge (#902)
    // is not its own band — it rides as history on its chain head. Faceted groups
    // keep predecessors selectable while still showing the successor's history
    // disclosure.
    if (foldSuccessionBands && superseded.has(member.fqid)) {
      continue;
    }
    const allStates = nodesByFqid.get(member.fqid)?.states ?? [];
    // null filter = whole-variable member → every column; a Set = only the group's
    // member columns for this variable (a state with no column is always dropped by
    // pickerRepresentations, so the filter never needs to admit null).
    const cols = memberColumnsByFqid.get(member.fqid) ?? null;
    const states =
      cols === null
        ? allStates
        : allStates.filter(
            (s) =>
              s.delivery_column_name != null &&
              cols.has(s.delivery_column_name),
          );
    const explicitMemberColumns = new Set(
      (membersByFqid.get(member.fqid) ?? [])
        .map((m) => m.delivery_column)
        .filter((col): col is string => col != null),
    );
    const baseRows = pickerRepresentations(states);
    const explicitSelectableColumns = new Set(
      baseRows
        .filter(
          (row) =>
            row.representation != null && explicitMemberColumns.has(row.column),
        )
        .map((row) => row.column),
    );
    const rows = baseRows.map((row) =>
      row.representation != null &&
      explicitSelectableColumns.size > 1 &&
      explicitSelectableColumns.has(row.column)
        ? { ...row, pinRepresentation: true }
        : row,
    );
    const rowColumns = new Set(rows.map((r) => r.column));
    const rowSeed = rows[0];
    for (const groupMember of membersByFqid.get(member.fqid) ?? []) {
      const deliveryColumn = groupMember.delivery_column;
      if (
        deliveryColumn === null ||
        deliveryColumn === undefined ||
        rowColumns.has(deliveryColumn) ||
        !hasZeroStateCoverage(groupMember)
      ) {
        continue;
      }
      rows.push(neverDeliveredRow(deliveryColumn, rowSeed));
      rowColumns.add(deliveryColumn);
    }
    out.push({
      key: member.fqid,
      name: member.name ?? leafSlug(member.fqid),
      registerPrefix: registerPrefixOf(member.fqid),
      facetLabel: memberFacetLabel(member),
      // Per-column facet labels across ALL the fqid's members (#678) — so a
      // multi-member-on-one-fqid representation group shows each column's human facet.
      facetByColumn: facetByColumnByFqid.get(member.fqid),
      // The structured per-column facets (#908) — the picker's dimension marking
      // + per-axis filter read these to group/filter rows by axis.
      facetsByColumn: facetsByColumnByFqid.get(member.fqid),
      // Band-level facets (#908 C1) — a whole-variable faceted member's facets, which
      // have no delivery column to key by and so apply to every row of the band. The
      // picker's `rowFacet` falls back to these after the per-column lookup.
      facets: bandFacetsByFqid.get(member.fqid),
      // The member's OPERATIONAL DEFINITION (#892/#932): the per-(split-)variable
      // distinguishing text, carried on its graph node. Surfaced per band so parallel
      // members whose name/definition coincide (owner / previous-owner näringsgren) are
      // told apart inline — the consumer half of #892, replacing the #900 deferral that
      // hid distinct per-member text rather than misrepresent it as concept text. Most
      // siblings carry null; the picker renders the line only when present.
      operationalDefinition: nodesByFqid.get(member.fqid)
        ?.operational_definition,
      // The member's own leaf page — the picker renders the identity as a nav link
      // (the binding leaf passes no href; it's already that page). Carry the active
      // group `?period` onto the link (#678) so opening a member from the picker
      // keeps the window the user narrowed the group to, instead of resetting the
      // leaf to full history.
      href: memberHref(member.fqid),
      rows,
      // The chain head carries its superseded predecessor editions as history (#902):
      // oldest-first, each a leaf-page link, so they stay reachable without being
      // co-equal selectable bands. Undefined for a member that heads no in-group chain.
      supersedes: historyByHead.get(member.fqid)?.map((p) => ({
        name: predecessorName(p.fqid),
        href: memberHref(p.fqid),
        effectiveYear: p.effectiveYear,
      })),
    });
  }
  return out;
});
/** The `band.key` (member fqid) the `?member=` focus hint names, for the picker's
 * deep-link highlight (#678). The backend echoes the VALIDATED member slug on
 * `node.member` (null when absent / unrecognized); the band key is the member fqid,
 * whose leaf slug is that slug — so match a band by `leafSlug(key) === node.member`.
 * Null when there's no (valid) hint, so the picker marks nothing. */
const focusKey = $derived.by((): string | null => {
  const hint = node?.member;
  if (hint == null) {
    return null;
  }
  const band = bands.find((b) => leafSlug(b.key) === hint);
  return band?.key ?? null;
});

// ── The time axis: `?period` (client-side lens, no refetch) ──────────────────
// The PeriodPicker drives per-row DIMMING only — `getConceptGroup` takes no period,
// so the value triggers NO refetch; it just narrows the dim window (mirrors the
// binding leaf's `pickerWindow`).
const period = $derived(router.getQueryParam("period"));
const boundedPickerPeriod = $derived(
  enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, periodCeilingYear)
    : period,
);
const boundedProjectWindow = $derived(
  windowStore.value === null || !enforcePeriodBounds
    ? windowStore.value
    : clampYearWindow(windowStore.value, windowMinYear, periodCeilingYear),
);
const activePickerPeriod = $derived(
  boundedPickerPeriod && isStructurallyValidPeriodWire(boundedPickerPeriod)
    ? boundedPickerPeriod
    : null,
);
const graphMemberHrefs = $derived.by((): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const member of node?.members ?? []) {
    out[member.fqid] = memberHref(member.fqid);
  }
  return out;
});

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
      if (row.selectable === false) {
        continue;
      }
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
  pickerWindowYears(activePickerPeriod, boundedProjectWindow),
);
const committedRows = $derived(
  committedPickerRows(projectStore.draft, bands, {
    period: activePickerPeriod,
    window: pickerWindow,
  }),
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

// ── Staged add/remove to project ─────────────────────────────────────────────
// The deployment seed is ready once /api/context has populated BOTH fields.
const seedReady = $derived(regMetaVersion !== "" && steward !== "");

/** The applied outcome (drives the inline confirmation). */
let applyOutcome = $state<{
  added: number;
  removed: number;
  periodChanged: number;
} | null>(null);

// A fresh period / member refine or a group change clears the stale confirmation.
$effect(() => {
  void period;
  void memberHint;
  void key;
  applyOutcome = null;
});

function stagedAddCandidates(selection: PickerSelection) {
  const { band, row } = selection;
  const addWindow = addWindowBounds(activePickerPeriod, pickerWindow);
  const segments =
    row.variantSegments && row.variantSegments.length > 0
      ? row.variantSegments
      : [{ variant: row.variant, windows: row.windows }];
  const overlappingSegments =
    segments.length === 1
      ? segments
      : segments.filter((segment) =>
          windowsOverlapWindow(segment.windows, addWindow),
        );
  const selectedSegments =
    overlappingSegments.length > 0 ? overlappingSegments : segments;
  return selectedSegments.map((segment) => {
    const addPeriod =
      segments.length === 1
        ? rowAddPeriod(row, addWindow)
        : windowsAddPeriod(
            segment.windows,
            overlappingSegments.length > 0 ? addWindow : null,
            false,
          );
    return {
      selection,
      variant: segment.variant,
      registerVariant: rowRegisterVariantForVariant(band, segment.variant),
      periodWire: addPeriod,
      period: periodFromWire(addPeriod),
    };
  });
}

type StagedAddCandidate = ReturnType<typeof stagedAddCandidates>[number];

async function stagedAdd(
  candidate: StagedAddCandidate,
  resolvePeriodWire: string | null,
) {
  const { band, row } = candidate.selection;
  let resolution: BindingResolution;
  try {
    resolution = await resolveBindingAt(
      band.key,
      resolvePeriodWire,
      candidate.variant,
    );
  } catch {
    resolution = { kind: "unresolved" as const, reason: "no-states" as const };
  }
  return {
    registerVariant: candidate.registerVariant,
    period: candidate.period,
    binding: bindingFieldsFromResolution(
      band.key,
      resolution,
      row.representation,
      { pinRepresentation: row.pinRepresentation === true },
    ),
  };
}

/** Apply the staged diff through ONE synchronous store mutation. */
async function applyStaged(payload: PickerApplyPayload): Promise<boolean> {
  if (
    payload.adds.length === 0 &&
    payload.removes.length === 0 &&
    payload.periodChanges.length === 0
  ) {
    return true;
  }
  if (projectStore.draft === null && payload.adds.length > 0) {
    projectStore.newProject({
      reg_meta_version: regMetaReleaseTag(regMetaVersion),
      steward,
    });
  }
  const target = projectStore.draft;
  const candidates = payload.adds.flatMap(stagedAddCandidates);
  const finalPeriods = finalSourcePeriodsForStagedAdds(
    sourcePeriodsFromDraft(target),
    payload.periodChanges,
    candidates,
  );
  const adds = await Promise.all(
    candidates.map((candidate) =>
      stagedAdd(
        candidate,
        periodToWire(
          finalPeriods.get(candidate.registerVariant) ?? candidate.period,
        ),
      ),
    ),
  );
  if (projectStore.draft !== target) {
    return false;
  }
  const removes = payload.removes.flatMap((r) =>
    stagedRemoveForCommitted(r.committed),
  );
  projectStore.applyStagedDiff({
    adds,
    removes,
    periodChange: periodChangesWithStagedAdds(
      payload.periodChanges,
      candidates,
    ),
  });
  applyOutcome = {
    added: payload.adds.length,
    removed: removes.length,
    periodChanged: payload.periodChanges.length,
  };
  return true;
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
    <!-- #678/#900: the SHARED concept definition/description — rendered ONLY when the
         members genuinely AGREE on a single value (most siblings carry null; the
         canonical member carries the one the group shares). Rendered as a clean
         labelled block at the TOP of the page — mirroring the binding leaf's
         Definition/Description presentation — ABOVE the Technical details disclosure,
         giving the group its missing descriptive context. When members DISAGREE (more
         than one distinct value) or none carry text, the block (or the row) is hidden
         and the per-member text stays reachable on each member's leaf page (#900). -->
    {#if sharedDefinition || sharedDescription}
      <dl class="meta">
        {#if sharedDefinition}
          <dt>Definition</dt>
          <dd>{sharedDefinition}</dd>
        {/if}
        {#if sharedDescription}
          <dt>Description</dt>
          <dd>{sharedDescription}</dd>
        {/if}
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
    <!-- The period availability lens sits ABOVE the column picker (both group + leaf):
         pick the period first, then the columns. Seeds from the project window, draws
         the members' union coverage span, and dims rows whose span doesn't overlap;
         writes `?period` only (never the global window); `getConceptGroup` ignores it. -->
    <PeriodPicker
      period={boundedPickerPeriod}
      window={boundedProjectWindow}
      coverage={unionCoverage}
      {windowMinYear}
      vintageYear={periodCeilingYear}
      {enforcePeriodBounds}
      onsubmit={(p) => writePeriod(p)}
      onclear={() => writePeriod(null)}
    />

    <!-- The nested REPRESENTATION PICKER (#678 inc 2): one band per member variable,
         each with its own adaptive representation rows, under ONE shared staged diff
         and ONE "Apply" footer. The period window only DIMS out-of-window rows
         (`pickerWindow`); `seedReady` gates the commit. -->
    {#if bands.length > 0}
      <RepresentationPicker
        {bands}
        axes={node.axes}
        includeRowDimensionFilters={!suppressRowDimensionFilters}
        window={pickerWindow}
        canAdd={seedReady}
        {committedRows}
        activePeriod={activePickerPeriod}
        {focusKey}
        {graph}
        {graphMemberHrefs}
        {vintageYear}
        onapply={applyStaged}
        onstagechange={(hasDiff) => {
          if (hasDiff) {
            applyOutcome = null;
          }
        }}
      />
    {/if}

    {#if applyOutcome}
      <p class="page-add">
        <span class="add-confirm" role="status">
          Applied
          {[
            applyOutcome.added > 0
              ? `+${applyOutcome.added} ${applyOutcome.added === 1 ? "column" : "columns"}`
              : null,
            applyOutcome.removed > 0
              ? `-${applyOutcome.removed} ${applyOutcome.removed === 1 ? "column" : "columns"}`
              : null,
            applyOutcome.periodChanged > 0
              ? `${applyOutcome.periodChanged} ${
                  applyOutcome.periodChanged === 1
                    ? "period change"
                    : "period changes"
                }`
              : null,
          ]
            .filter(Boolean)
            .join(" · ")}
          — <a href="/project">view</a>
        </span>
      </p>
    {/if}
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
</style>
