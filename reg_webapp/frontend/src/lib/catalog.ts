/**
 * Display + FQID-path helpers for the catalog browse UI (`catalog.test.ts`):
 * a node's label, and the `/catalog/<fqid-path>` URL ↔ FQID-segment helpers
 * (the SPA routes mirror the API path, §9.5).
 */
import { type CatalogNode, encodeFqid, type VariableStateModel } from "./api";

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
