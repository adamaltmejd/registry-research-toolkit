/**
 * Display + FQID-path helpers for the catalog browse UI (`catalog.test.ts`):
 * a node's label, and the `/catalog/<fqid-path>` URL ↔ FQID-segment helpers
 * (the SPA routes mirror the API path, §9.5).
 */
import {
  type BindingChild,
  type CatalogNode,
  encodeFqid,
  isCatalogNode,
  type StatesResponse,
  type VariableStateModel,
} from "./api";

/** Narrow the catch-all browse response to a browsable `CatalogNode`, or `null`
 * for a no-`kind` payload (a `?period` `StatesResponse` or a sub-endpoint
 * envelope) — the boundary every browse consumer narrows at before switching on
 * `kind` (§9.5). */
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

/** A node's display label — its `name` when present, else its FQID (providers
 * and registers carry an optional `name`; classifications carry a required
 * `name`; the classification-root carries a default `name`). */
export function nodeLabel(node: CatalogNode): string {
  if (node.kind === "classification-root" || node.kind === "classification") {
    return node.name;
  }
  return node.name ?? node.fqid;
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
// (§6.2). The binding variable picker is SCOPED to the provider/register prefix
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
// (overridable; the backend is canonical, §9.6). Covers BOTH the SQL/storage
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
]);
const ID_TOKENS = new Set(["identifierare", "uniqueidentifier"]);

/** Advisory storage-type → ColumnType prefill (§6.3) for the binding type
 * derive-on-pick. A state carrying a value set is categorical; otherwise the
 * leading `data_type` token decides; unrecognized → "opaque". ALWAYS overridable
 * via the BindingEditor's `<select>` — the backend stays canonical (§9.6). */
export function deriveType(state: VariableStateModel | undefined): string {
  if (!state) {
    return "opaque";
  }
  if (state.value_set_id != null || (state.value_set?.length ?? 0) > 0) {
    return "categorical";
  }
  // The leading token (strip a trailing `(len)` or trailing words like "(text)").
  const token = (state.data_type ?? "").trim().toLowerCase().split(/[ (]/)[0];
  if (ID_TOKENS.has(token)) {
    return "id";
  }
  if (NUMERIC_TOKENS.has(token)) {
    return "numeric";
  }
  if (DATE_TOKENS.has(token)) {
    return "date";
  }
  if (DATETIME_TOKENS.has(token)) {
    return "datetime";
  }
  return "opaque";
}

/** One co-existing REPRESENTATION of a concept at a period — a distinct delivery
 * column. `column` is the stable handle set on `binding.representation`; `label`
 * (the value-set version label, e.g. "5-års intervall") + `codeCount` are for
 * display in the chooser. */
export interface Representation {
  column: string;
  label: string;
  codeCount: number | null;
}

/** The delivery-column representations a binding must choose between among a
 * resolve's states (the `StatesResponse` from a `?period` resolve), first-seen
 * order. >1 only when ≥2 columns CO-EXIST (overlapping validity windows) — that
 * is the genuine multi-representation case; this MIRRORS the backend
 * `_coexisting_columns` so a range crossing a sequential column RENAME (distinct
 * columns, non-overlapping) is treated as drift and does NOT open the chooser.
 * 0/1 means no choice is needed. Pure — unit-tested. */
export function representationsFromStates(
  states: VariableStateModel[],
): Representation[] {
  const byColumn = new Map<string, VariableStateModel>();
  for (const s of states) {
    if (s.delivery_column_name && !byColumn.has(s.delivery_column_name)) {
      byColumn.set(s.delivery_column_name, s);
    }
  }
  const toRep = (s: VariableStateModel): Representation => ({
    column: s.delivery_column_name as string,
    label: s.value_set_version_label,
    codeCount: s.value_set?.length ?? null,
  });
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
      .map(toRep);
  }
  // No genuine choice: a single column, or a sequential rename (drift). Report at
  // most the first column so the caller's `length > 1` chooser gate stays closed.
  const first = [...byColumn.values()][0];
  return first ? [toRep(first)] : [];
}
