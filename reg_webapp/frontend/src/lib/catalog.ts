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
  encodeFqid,
  type GroupFacetModel,
  getCatalogNode,
  isCatalogNode,
  type StatesResponse,
  type VariableStateModel,
} from "./api";
import {
  type Coverage,
  grainOfToken,
  PERIOD_GRAINS,
  type PeriodGrain,
  periodRangeEndpoints,
  periodTokenBounds,
} from "./period";

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

/** The member count a folded row list represents — group rows count their
 * members, leaves count 1. Keeps the "N variables" readout in VARIABLE units
 * after folding (a register whose 36 variables fold into one matrix row still
 * reports 36, not 1). */
export function countFoldedMembers<T>(rows: GroupedRow<T>[]): number {
  return rows.reduce(
    (n, row) => n + (row.kind === "group" ? row.group.members.length : 1),
    0,
  );
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
    ...group.members.flatMap((m) => [m.name, m.fqid, leafSlug(m.fqid)]),
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

// ── Member-distinguishing qualifier (#670) ──────────────────────────────────
// A grouped binding leaf (`scb/lisa/agi1astsni2007g`) shares its concept
// `node.name` with ~31 siblings, so the header alone can't tell members apart.
// The /dimensions groups carry per-member `facets` (axis:value:label) that
// LOCATE each member in its group — the qualifier is THIS member's facet labels
// (e.g. "AGI · 2007 SNI edition"). Derived from the SAME /dimensions data
// DimensionsPanel renders (lifted into BindingLeafView; no extra fetch).

/** A member-distinguishing qualifier and whether it is the facet-label form or
 * the slug fallback — the discriminant the caller styles on (`facets` → a human
 * label `<span>`, `slug` → a technical-identifier `<code>`). */
export type MemberQualifier = { text: string; kind: "facets" | "slug" };

/** The member-distinguishing qualifier for `fqid` across its dimension `groups`
 * — the facet labels (axis:value pairs) that locate THIS member in its group,
 * joined with " · ". A variable can be in MULTIPLE groups; the CANONICAL group
 * (`canonicalKey` = `BindingNode.group.key`, the member's own register group)
 * leads, so its facets win when several groups carry the member.
 *
 * A GROUPED member ALWAYS gets a qualifier: when no group yields facets (an edge
 * group's split siblings — e.g. `agi1astsni2007g` vs `ku1astsni`, `axes: []`,
 * empty facets), the member's leaf slug is the fallback (`kind: "slug"`), since
 * the slug is the only thing that distinguishes those siblings (it encodes the
 * edition/vintage/variant) — without it the sibling pages render IDENTICAL
 * headers (the shared concept name; #670 / dogfooding M10). "Grouped" = a
 * canonical `canonicalKey`, or the member appears in some group. Returns `null`
 * ONLY for an UNGROUPED member (a normal variable whose `node.name` already
 * suffices). The discriminated `kind` lets the caller pick a presentation
 * (facet label vs. mono code) without re-scanning the groups. */
export function memberQualifier(
  groups: readonly ConceptGroup[],
  fqid: string,
  canonicalKey?: string | null,
): MemberQualifier | null {
  // Canonical group first so its facets lead when the member is in several
  // groups; the rest follow in their incoming order. /dimensions can return
  // MULTIPLE dimension groups for one member (level / population / rank
  // memberships — DimensionsPanel/#489), hence canonical-first.
  const ordered =
    canonicalKey == null
      ? groups
      : [...groups].sort((a, b) => {
          const aCanon = a.key === canonicalKey ? 0 : 1;
          const bCanon = b.key === canonicalKey ? 0 : 1;
          return aCanon - bCanon;
        });
  let grouped = canonicalKey != null;
  for (const group of ordered) {
    const member = group.members.find((m) => m.fqid === fqid);
    if (member) {
      grouped = true;
    }
    const facets = member?.facets ?? [];
    if (facets.length > 0) {
      return { text: facets.map((f) => f.label).join(" · "), kind: "facets" };
    }
  }
  // Grouped but no group yields facets → fall back to the member's leaf slug so
  // the edge-group split siblings still get a distinguishing header element. An
  // ungrouped member returns null (its `node.name` already distinguishes it).
  return grouped ? { text: leafSlug(fqid), kind: "slug" } : null;
}

/** The group a binding leaf links back to (#670): the dimension group matching
 * the member's canonical `node.group.key`, else (defensively) the first
 * dimension group containing the member — its `{ label, href }` for the
 * "member of ⟨label⟩" context link. `null` when ungrouped, or when no fetched
 * group matches (loading / error / a stale skew between `node.group` and
 * /dimensions). The href targets the group SUBJECT route via `groupHref`. */
export function memberGroupLink(
  groups: readonly ConceptGroup[],
  ref: BindingGroupRef | null | undefined,
  fqid: string,
): { label: string; href: string } | null {
  if (!ref) {
    return null;
  }
  const byKey = groups.find((g) => g.key === ref.key);
  const containing =
    byKey ?? groups.find((g) => g.members.some((m) => m.fqid === fqid));
  if (!containing) {
    return null;
  }
  return {
    label: containing.label,
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
 * facet axis (e.g. "dimension" → "dimensions"). Classification umbrella groups
 * are single-axis (#516); we derive the noun from that axis rather than
 * hardcoding "vintages" (succession editions aren't groups, #571). Naive +"s"
 * is enough for the axis vocabulary (dimension, month, …); a group with no axis
 * falls back to "members". */
export function axisNoun(axes: string[]): string {
  const axis = axes[0];
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
  return `/catalog/group/${enc(provider)}/${enc(register)}/${enc(key)}`;
}

/** Per-segment percent-encode (the encoding `catalogHref`/`encodeFqid` apply),
 * tolerating an absent segment as "". */
function enc(segment: string | undefined): string {
  return encodeURIComponent(segment ?? "");
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
  const coexisting = new Set<string>();
  for (let i = 0; i < states.length; i++) {
    for (let j = i + 1; j < states.length; j++) {
      const a = states[i];
      const b = states[j];
      if (
        a.delivery_column_name &&
        b.delivery_column_name &&
        a.delivery_column_name !== b.delivery_column_name &&
        a.valid_from <= b.valid_to &&
        b.valid_from <= a.valid_to
      ) {
        coexisting.add(a.delivery_column_name);
        coexisting.add(b.delivery_column_name);
      }
    }
  }
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

// ── One-click add plan (#306) ────────────────────────────────────────────────
// The variable page's page-level "Add to project" auto-picks everything that
// isn't a GENUINE choice: time-sequential register-variant succession within
// the chosen range auto-splits into one source per variant segment (succession
// is not a user choice — decided in #306), and coding-identical parallel
// columns auto-pick the primary (#266 latest-era ranking). Only CO-EXISTING
// variants (a population choice) and genuinely distinct codings prompt. Pure —
// unit-tested in catalog.test.ts; the page (BindingLeafView) renders prompts
// and commits segments through `addFromCatalog`.

/** One register variant's overall validity window across a state set. */
export interface VariantWindow {
  variant: string;
  from: string;
  to: string;
}

/** One per-variant add: the (clipped) wire period for the source and the
 * representation outcome for the binding. `representation` is pre-set to the
 * PRIMARY column whenever >1 column co-exists (the #266 default — final when
 * the columns are coding-identical, the prompt's preselect when
 * `needsRepChoice`); null for a single/absent column (matching the
 * pin-only-when-genuinely-multi rule the store's single-rep derive expects). */
export interface AddSegment {
  variant: string;
  /** Wire period for this segment's source (the user's period, or the clipped
   * sub-range for a succession segment); null when no period is chosen. */
  period: string | null;
  reps: Representation[];
  /** True when the segment's columns are a GENUINE coding choice (>1
   * co-existing, not coding-identical) — the page must prompt. */
  needsRepChoice: boolean;
  representation: string | null;
}

/** The page-level add decision:
 *  - `segments`: proceed (after any rep prompts) — 1 segment, or several for a
 *    range spanning a variant succession.
 *  - `choose-variant`: ≥2 variants CO-EXIST inside the chosen period (or the
 *    period is point/absent/unparseable with ≥2 variants present) — a genuine
 *    population choice the user must make. */
export type AddPlan =
  | { kind: "segments"; segments: AddSegment[] }
  | { kind: "choose-variant"; options: VariantWindow[] };

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
 * the add planner (#306) and the change-hint differ (#309). */
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
 * isn't a leading-4-digit string (a blank/edge bound on a stale payload). */
function yearOf(iso: string): number | null {
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

/** Per-state stable key — NOT `state_id` alone. A merged monthly-family
 * variable (#319) expands ONE annual `variable_state` row into up to 12
 * same-`state_id` per-month windows (they SHARE the annual state's `state_id`
 * and `value_set_version_label`; only `delivery_column_name` + the validity
 * bounds are overridden per window — see reg_meta `catalog.py`
 * `_expand_state_windows`). So `state_id` is no longer unique in the list — the
 * compound `(state_id, delivery_column_name, valid_from)` is. Single source of
 * truth for the `StatesView` `#each` key, its #310 inline-expansion map, and the
 * `stateChangeHints` Map key (#384). */
export function stateKey(s: VariableStateModel): string {
  return `${s.state_id}:${s.delivery_column_name ?? ""}:${s.valid_from}`;
}

/**
 * Per-state "what changed" hints (#309): for each variant's states in
 * `valid_from` order, diff every state against its predecessor and report the
 * fields that actually changed — data type (formatted), delivery column, and
 * value set (`value_set_id` is the CONTENT key: the coding can change even
 * when the version label doesn't). Keyed by the LATER state's compound
 * `stateKey` — `state_id` alone collides across the same-`state_id` windows a
 * merged monthly family (#319) expands into, collapsing all 12 month rows onto
 * one Map entry (#384). Cross-variant transitions are never hinted (the variant
 * is its own visible column). Two states differing in none of these render
 * identically without a hint — exactly the int→bigint invisibility this fixes,
 * so every diffed field must also be VISIBLE in the row.
 */
export function stateChangeHints(
  states: VariableStateModel[],
): Map<string, string[]> {
  const byVariant = statesByVariant(states);
  const hints = new Map<string, string[]>();
  for (const group of byVariant.values()) {
    const ordered = [...group].sort(
      (a, b) =>
        a.valid_from.localeCompare(b.valid_from) || a.state_id - b.state_id,
    );
    for (let i = 1; i < ordered.length; i++) {
      const prev = ordered[i - 1];
      const cur = ordered[i];
      // The windows of a merged monthly family (#319) SHARE one annual
      // `state_id`: they are 12 representations of a SINGLE claim, not a
      // before→after succession. They are non-overlapping consecutive months,
      // so the overlap guard below does NOT catch them — skip them explicitly
      // (a spurious "column LonFinkJan → LonFinkFeb" hint otherwise, #384).
      if (prev.state_id === cur.state_id) {
        continue;
      }
      // Only a genuine SUCCESSION is a transition: overlapping same-variant
      // states (co-delivered vintages/columns at one period) are parallel
      // ALTERNATIVES — diffing them as before→after would be misleading
      // (Codex P2 on #335).
      if (prev.valid_to >= cur.valid_from) {
        continue;
      }
      const changes: string[] = [];
      const prevType = formatDataType(prev.data_type, prev.data_length);
      const curType = formatDataType(cur.data_type, cur.data_length);
      if (prevType !== curType) {
        changes.push(`type ${prevType || "—"} → ${curType || "—"}`);
      }
      if (prev.delivery_column_name !== cur.delivery_column_name) {
        changes.push(
          `column ${prev.delivery_column_name ?? "—"} → ${cur.delivery_column_name ?? "—"}`,
        );
      }
      if (prev.value_set_id !== cur.value_set_id) {
        changes.push(
          prev.value_set_version_label !== cur.value_set_version_label
            ? `value set ${prev.value_set_version_label || "(no version)"} → ${cur.value_set_version_label || "(no version)"}`
            : "value set changed",
        );
      }
      if (changes.length > 0) {
        hints.set(stateKey(cur), changes);
      }
    }
  }
  return hints;
}

/** Render a clipped ISO bound as a period-range ENDPOINT token, collapsing
 * year-aligned bounds to the bare year (`2010-01-01` → `2010` as a start,
 * `2009-12-31` → `2009` as an end) so the common year-grain succession yields
 * year ranges; a mid-year bound stays an exact date token (valid grammar). */
function boundToken(iso: string, edge: "from" | "to"): string {
  if (edge === "from" && iso.endsWith("-01-01")) {
    return iso.slice(0, 4);
  }
  if (edge === "to" && iso.endsWith("-12-31")) {
    return iso.slice(0, 4);
  }
  return iso;
}

function segmentFor(
  variant: string,
  states: VariableStateModel[],
  period: string | null,
): AddSegment {
  const reps = representationsFromStates(states);
  const multi = reps.length > 1;
  return {
    variant,
    period,
    reps,
    needsRepChoice: multi && !representationsCollapse(reps),
    representation: multi ? reps[0].column : null,
  };
}

/**
 * Build the add plan for the VISIBLE states (the `?period`-narrowed subset when
 * a period is active — already only the states overlapping it — else the full
 * history) at the page's wire period. Splitting needs a parseable RANGE: a
 * point/absent/unparseable period with ≥2 variants is a `choose-variant` (at a
 * single window ≥2 remaining variants co-exist; with no time bound "all of
 * them" isn't well-defined), which also covers the rare mid-year succession a
 * point period straddles — one extra question, never a wrong auto-pick.
 */
export function buildAddPlan(
  states: VariableStateModel[],
  periodWire: string | null,
): AddPlan {
  const byVariant = statesByVariant(states);
  const windows: VariantWindow[] = [...byVariant.entries()].map(
    ([variant, ss]) => ({
      variant,
      from: ss.reduce(
        (m, s) => (s.valid_from < m ? s.valid_from : m),
        ss[0].valid_from,
      ),
      to: ss.reduce(
        (m, s) => (s.valid_to > m ? s.valid_to : m),
        ss[0].valid_to,
      ),
    }),
  );
  if (windows.length === 0) {
    return { kind: "segments", segments: [] };
  }
  if (windows.length === 1) {
    const w = windows[0];
    return {
      kind: "segments",
      segments: [
        segmentFor(w.variant, byVariant.get(w.variant) ?? [], periodWire),
      ],
    };
  }

  // ≥2 variants: only a parseable token range can prove succession.
  const endpoints = periodWire ? periodRangeEndpoints(periodWire) : null;
  const loBounds = endpoints ? periodTokenBounds(endpoints[0]) : null;
  const hiBounds = endpoints ? periodTokenBounds(endpoints[1]) : null;
  const byFrom = [...windows].sort(
    (a, b) =>
      a.from.localeCompare(b.from) || a.variant.localeCompare(b.variant),
  );
  if (!endpoints || !loBounds || !hiBounds) {
    return { kind: "choose-variant", options: byFrom };
  }

  // Clip each variant window to the range (ISO strings compare chronologically),
  // then test pairwise overlap INSIDE the range: any overlap → co-existing.
  const lo = loBounds.from;
  const hi = hiBounds.to;
  // The visible states are period-narrowed, so every window intersects the
  // range; the inverted-clip filter is defensive (a non-covering variant must
  // never yield an inverted segment period).
  const clipped = byFrom
    .map((w) => ({
      ...w,
      from: w.from > lo ? w.from : lo,
      to: w.to < hi ? w.to : hi,
    }))
    .filter((w) => w.from <= w.to);
  for (let i = 0; i < clipped.length; i++) {
    for (let j = i + 1; j < clipped.length; j++) {
      if (
        clipped[i].from <= clipped[j].to &&
        clipped[j].from <= clipped[i].to
      ) {
        return { kind: "choose-variant", options: byFrom };
      }
    }
  }

  // Pure succession: one segment per variant, period clipped to its window —
  // the user's own endpoint tokens survive verbatim at the range edges.
  const segments = clipped.map((w, i) => {
    const fromTok = i === 0 ? endpoints[0] : boundToken(w.from, "from");
    const toTok =
      i === clipped.length - 1 ? endpoints[1] : boundToken(w.to, "to");
    const period = fromTok === toTok ? fromTok : `${fromTok}..${toTok}`;
    return segmentFor(w.variant, byVariant.get(w.variant) ?? [], period);
  });
  return { kind: "segments", segments };
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
