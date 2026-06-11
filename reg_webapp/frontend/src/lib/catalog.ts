/**
 * Display + FQID-path helpers for the catalog browse UI (`catalog.ts`):
 * a node's label, and the `/catalog/<fqid-path>` URL ↔ FQID-segment helpers
 * (the SPA routes mirror the API path; see reg_webapp/DESIGN.md → Catalog router
 * structure).
 */
import {
  type BindingChild,
  type CatalogNode,
  type ConceptGroup,
  type ConceptGroupMember,
  encodeFqid,
  getCatalogNode,
  isCatalogNode,
  type StatesResponse,
  type VariableStateModel,
} from "./api";

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
  groups: ConceptGroup[],
): GroupedRow<T>[] {
  const groupOf = new Map<string, ConceptGroup>();
  for (const g of groups) {
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
  for (const group of groups) {
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

/** Whether a group row survives the type-to-filter: match on the group's own
 * label/key OR any member's name/FQID — so filtering for a member (e.g. "maj")
 * still surfaces the group that folded it. */
export function groupMatchesFilter(
  needle: string,
  group: ConceptGroup,
): boolean {
  return (
    matchesFilter(needle, group.label, group.key) ||
    group.members.some((m) => matchesFilter(needle, m.name, m.fqid))
  );
}

/** The distinct (value, label) pairs a group's members carry on `axis`,
 * value-sorted — the rows/columns of the facet picker. */
export function axisValues(
  group: ConceptGroup,
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
 * undefined for an empty cell (partial families: a missing month/vintage). */
export function memberAt(
  group: ConceptGroup,
  coords: { axis: string; value: string }[],
): ConceptGroupMember | undefined {
  return group.members.find((m) =>
    coords.every((c) =>
      m.facets.some((f) => f.axis === c.axis && f.value === c.value),
    ),
  );
}

/** A node's display label — its `name` when present, else its FQID (providers
 * and registers carry an optional `name`; classifications carry a required
 * `name`; the classification-root carries a default `name`). */
export function nodeLabel(node: CatalogNode): string {
  if (node.kind === "classification-root" || node.kind === "classification") {
    return node.name;
  }
  return node.name ?? node.fqid;
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
 * Used by the PICKERS (where the user hunts a specific row: "kon" → Kön first),
 * NOT the browse pages (which keep plain alphabetical order — the filter only
 * narrows). An empty needle returns the matched list unchanged (every row is
 * tier 3, stable). */
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

/** Segments of an FQID path (`scb/lisa/kon` → `["scb", "lisa", "kon"]`).
 * Empty string → `[]` (the root). */
export function fqidSegments(fqidPath: string): string[] {
  return fqidPath ? fqidPath.split("/") : [];
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
