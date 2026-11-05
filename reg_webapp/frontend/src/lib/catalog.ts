/**
 * Display + FQID-path helpers for the catalog browse UI (`catalog.ts`):
 * a node's label, and the `/catalog/<fqid-path>` URL ↔ FQID-segment helpers
 * (the SPA routes mirror the API path; see reg_webapp/DESIGN.md → Catalog router
 * structure).
 */
import { encodeFqid } from "./api";
import type { Route } from "./router";
import type { BreadcrumbItem } from "./ui/types";

/** The label of the breadcrumb's first crumb, the data-browser root at
 * `/catalog` (#675). Below the narrow breakpoint it is the way back to the
 * register list, so every page's trail leads with it; above it the list is on
 * screen and the crumb is hidden (DESIGN.md → Layout). */
const BROWSE_REGISTERS_LABEL = "Browse registers";

/** The same crumb while the search holds text: the matches are in that list. */
export const SHOW_MATCHING_REGISTERS_LABEL = "Show matching registers";

/** Swedish labels sort with å, ä and ö after z, case- and accent-blind
 * otherwise (DESIGN.md → Swedish text). */
export const svCollator = new Intl.Collator("sv", { sensitivity: "base" });

/** A provider's registers in the one order every list of them uses: the
 * register column, the provider page and its variables column (maintainer
 * 2026-11-05), by name in Swedish collation. The server lists them by FQID. */
export function sortedRegisters<
  R extends { fqid: string; name?: string | null },
>(registers: readonly R[]): R[] {
  return [...registers].sort((a, b) =>
    svCollator.compare(a.name ?? a.fqid, b.name ?? b.fqid),
  );
}

// ── Type-to-filter (catalog browse + pickers) ───────────────────────────────
// The catalog/authoring lists render at scale (238 registers, 740 variables) —
// every list surface needs an in-memory substring filter. One shared matcher so
// all four surfaces fold identically (`foldSearch`): "lon" matches both "Löne…"
// and "lön" (diacritic-blind), and the needle is matched against BOTH display
// name AND slug/FQID.

/** The browse filter's fold: NFKD, lower case, combining marks dropped, one
 * space between words. It is close to the server search's fold, not the same
 * (maintainer 2026-11-04): "ss" does not match "ß", since JavaScript lower-cases
 * "ß" to itself where full case folding gives "ss". */
export function foldSearch(text: string): string {
  return text
    .normalize("NFKD")
    .toLowerCase()
    .replace(/\p{M}/gu, "")
    .replace(/\s+/gu, " ")
    .trim();
}

/** Whether any of `haystacks` contains the folded `needle` (substring). An empty
 * (or whitespace-only) needle matches everything — the unfiltered full-list
 * behavior. `null`/`undefined` haystacks are skipped. */
export function matchesFilter(
  needle: string,
  ...haystacks: (string | null | undefined)[]
): boolean {
  const q = foldSearch(needle);
  if (!q) {
    return true;
  }
  return haystacks.some((h) => h != null && foldSearch(h).includes(q));
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
  const q = foldSearch(needle);
  const matched = items.filter((it) => matchesFilter(needle, ...keys(it)));
  if (!q) {
    return matched;
  }
  // 0 = exact, 1 = prefix, 2 = other. `Array.prototype.sort` is stable, so
  // equal-tier rows keep their incoming (alphabetical) order.
  const tier = (it: T): number => {
    const folded = keys(it).map((k) => (k == null ? null : foldSearch(k)));
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
function fqidSegments(fqidPath: string): string[] {
  return fqidPath ? fqidPath.split("/") : [];
}

/** The catalog ancestry without the group namespace, including partial paths. */
export function unqualifiedCatalogPath(fqidPath: string): string {
  return fqidPath.startsWith("set/") ? fqidPath.slice("set/".length) : fqidPath;
}

/** The owning register for a catalog path, including namespaced groups. */
export function registerOfPath(fqidPath: string): string | null {
  const segments = fqidSegments(unqualifiedCatalogPath(fqidPath));
  if (segments[0] === "class") return null;
  return segments.length >= 2 ? segments.slice(0, 2).join("/") : null;
}

/** The leaf slug of an FQID — its last `/`-separated segment (`scb/rtb` →
 * `"rtb"`, a bare `"rtb"` → `"rtb"`). A ranking key for the browse type-to-filter
 * (`rankFilter`): the leaf slug exact-/prefix-matches the needle even though the
 * full FQID's provider prefix would block it (`foldSearch("scb/rtb")` doesn't start
 * with `"rtb"`), so a slug-named target (`scb/rtb` for "rtb") outranks a purpose-
 * blurb-only match (#674). */
export function leafSlug(fqid: string): string {
  return fqid.split("/").at(-1) ?? fqid;
}

/** The catalog pages above a route's page, provider then register: the
 * ancestors its trail names. The group namespace (`set/`) and the
 * classification axis have no pages of their own, so they add none. */
export function trailAncestors(route: Route): string[] {
  if (route.name !== "catalog-node") return [];
  if (route.fqidPath === "class" || route.fqidPath.startsWith("class/"))
    return [];
  const segments = fqidSegments(unqualifiedCatalogPath(route.fqidPath));
  return segments
    .slice(0, Math.min(segments.length - 1, 2))
    .map((_, i) => segments.slice(0, i + 1).join("/"));
}

/** The detail pane's breadcrumb trail for a route (#803): the ancestors' names,
 * ending at the page's parent, since the page's `h1` names the page itself
 * (maintainer Q5, 2026-11-04). Every trail but the register list's own leads
 * with "Browse registers", shown only below the narrow breakpoint, where it is
 * the way back to that list. An ancestor whose name has not arrived is left
 * out, never shown as its slug. */
export function routeBreadcrumbs(
  route: Route,
  names: ReadonlyMap<string, string>,
): BreadcrumbItem[] {
  if (route.name === "root") return [];
  const browserRoot: BreadcrumbItem = {
    label: BROWSE_REGISTERS_LABEL,
    href: catalogHref(""),
    narrowOnly: true,
  };
  return [
    browserRoot,
    ...trailAncestors(route).flatMap((fqid) => {
      const label = names.get(fqid);
      return label ? [{ label, href: catalogHref(fqid) }] : [];
    }),
  ];
}

const classificationSuffixes: Record<string, string> = {
  niva: "nivå",
  inr: "inriktning",
};

/** Vintage classification names share the same spelling in lists and pins. */
export function humanizeClassificationSlug(slug: string): string {
  const m = /^([a-z]+)(\d{4})(?:-(.+))?$/.exec(slug);
  if (!m) return slug;
  const suffix = m[3];
  return `${m[1].toUpperCase()} ${m[2]}${suffix ? ` (${classificationSuffixes[suffix] ?? suffix.replaceAll("-", " ")})` : ""}`;
}
