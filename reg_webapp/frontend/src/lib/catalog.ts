/**
 * Narrowing helpers over the `kind`-discriminated catalog union (§9.5). The
 * backend's catch-all returns a Pydantic discriminated union; `openapi-typescript`
 * emits it as a tagged TS union keyed on `kind`. These helpers narrow a node at
 * the fetch boundary so components can switch on a known arm — and are the unit
 * under `catalog.test.ts`.
 */
import type { CatalogNode } from "./api";

/** The `kind` discriminator of any catalog node the catch-all returns (A5.3a:
 * no `?period`, so no `StatesResponse` — that arm has no `kind`). */
export type CatalogNodeKind = CatalogNode["kind"];

type NodeOfKind<K extends CatalogNodeKind> = Extract<CatalogNode, { kind: K }>;

/** Narrow a catalog node to a specific `kind`, or `null` if it's a different
 * arm. Keeps the `kind` switch in one tested place rather than scattered casts. */
export function narrowNode<K extends CatalogNodeKind>(
  node: CatalogNode,
  kind: K,
): NodeOfKind<K> | null {
  return node.kind === kind ? (node as NodeOfKind<K>) : null;
}

/** True when the node is a binding leaf (the embedded longitudinal record). */
export function isBinding(node: CatalogNode): node is NodeOfKind<"binding"> {
  return node.kind === "binding";
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

/** The href for a catalog node, mirroring the API path. `class` and
 * `class/<slug>` are valid FQID paths (the classification axis). */
export function catalogHref(fqidPath: string): string {
  return fqidPath ? `/catalog/${fqidPath}` : "/catalog";
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
