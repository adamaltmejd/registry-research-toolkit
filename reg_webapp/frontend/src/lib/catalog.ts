/**
 * Display + FQID-path helpers for the catalog browse UI (`catalog.ts`):
 * a node's label, and the `/catalog/<fqid-path>` URL ↔ FQID-segment helpers
 * (the SPA routes mirror the API path; see reg_webapp/DESIGN.md → Catalog router
 * structure).
 */
import { encodeFqid } from "./api";
import { foldSearch } from "./reg_core";
import type { Route } from "./router";
import type { BreadcrumbItem } from "./ui/types";

/** The label of the breadcrumb's first crumb, the data-browser root at
 * `/catalog` (#675). Below the narrow breakpoint it is the way back to the
 * register list, so every page's trail leads with it; above it the list is on
 * screen and the crumb is hidden (DESIGN.md → Layout). */
export const BROWSE_REGISTERS_LABEL = "Browse registers";

/** The same crumb while the search holds text: the matches are in that list. */
export const SHOW_MATCHING_REGISTERS_LABEL = "Show matching registers";

/** Swedish labels sort with å, ä and ö after z, case- and accent-blind
 * otherwise (DESIGN.md → Swedish text). */
export const svCollator = new Intl.Collator("sv", { sensitivity: "base" });

// ── Type-to-filter (catalog browse + pickers) ───────────────────────────────
// The catalog/authoring lists render at scale (238 registers, 740 variables) —
// every list surface needs an in-memory substring filter. One shared matcher so
// all four surfaces fold identically, and identically to the server's search
// (`fold_search`, crates/DESIGN.md "Text folds"): "lon" matches both "Löne…"
// and "lön" (diacritic-blind), "strasse" matches "Straße", and the needle is
// matched against BOTH display name AND slug/FQID. The fold is reg-core's own
// (`foldSearch`, through WASM), not a copy.

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
export function fqidSegments(fqidPath: string): string[] {
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

/** The detail pane's breadcrumb trail for a route (#803), as `Breadcrumbs`
 * items (the last item is the current page — no `href`). STRUCTURAL only: labels
 * are the raw slug segments (the routed page owns its rich, display-name header)
 * and the data-browser root leads every trail, catalog or not, because below the
 * narrow breakpoint it is the way back to the register list. Catalog-node
 * ancestors link to their pages; the set namespace has no breadcrumb. */
export function routeBreadcrumbs(route: Route): BreadcrumbItem[] {
  const browserRoot: BreadcrumbItem = {
    label: BROWSE_REGISTERS_LABEL,
    href: catalogHref(""),
    narrowOnly: true,
  };
  switch (route.name) {
    case "home":
      // Above the breakpoint the home page needs no trail: nothing is above it.
      return [browserRoot, { label: "Home", word: true, narrowOnly: true }];
    case "root":
      // The data browser IS the current page — a single un-linked crumb.
      return [{ label: BROWSE_REGISTERS_LABEL, narrowOnly: true }];
    case "catalog-node": {
      // Each ancestor segment is a link to its cumulative `/catalog/...` path;
      // the leaf segment is the current page (no href). Classification families
      // and their root have no pages, so their ancestors stay plain text.
      const trail = breadcrumbs(unqualifiedCatalogPath(route.fqidPath));
      const classification =
        route.fqidPath === "class" || route.fqidPath.startsWith("class/");
      return [
        browserRoot,
        ...trail.map((c, i) => ({
          label: c.label,
          href:
            !classification && i < trail.length - 1
              ? catalogHref(c.fqidPath)
              : undefined,
        })),
      ];
    }
    case "project":
      return [browserRoot, { label: "Project", word: true }];
    default:
      // not-found
      return [browserRoot];
  }
}

// ── register_variant coordinate helpers ──────────────────────────────────────
// A Source.register_variant is a 3-seg coordinate `provider/register/variant`
// (see crates/reg-core/DESIGN.md → Scope). The binding
// variable picker is SCOPED to the provider/register prefix
// (enforcing the FQID-prefix coupling as UX); the resolve takes the variant.

/** The 2-seg `provider/register` prefix of a register_variant, or "" when it has
 * fewer than 2 segments. */
export function registerPrefixOf(registerVariant: string): string {
  const segs = fqidSegments(registerVariant);
  return segs.length >= 2 ? `${segs[0]}/${segs[1]}` : "";
}

/** The HEADING of a source card in the cart: the register the source delivers
 * from, in the CATALOG's own word — `"LISA"`, `"MiDAS"`, never the slug
 * uppercased, and never the source's `name`, which is a generated join key
 * (`LISA_2`) rather than the thing the researcher picked.
 *
 * The rest of the title — the variant that names the population, and the provider
 * where the deployment serves more than one — qualifies this heading as its own
 * element beside it. The card composes them in MARKUP, not into one joined string:
 * frontend/DESIGN.md rules out middle-dot meta strings, and separate lines survive
 * a 375px width and read as separate things to a screen reader.
 *
 * Falls back to `coordinate` (the raw `register_variant`) whenever the register has
 * no name: while the catalog read is in flight, when it failed, and for a
 * coordinate outside this deployment's catalog. A machine coordinate is honest
 * there; an invented word would not be. */
export function sourceCardHeading(
  register: string | null | undefined,
  coordinate: string,
): string {
  return register?.trim() || coordinate;
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

/** The DISTINCT delivery columns among `states` that genuinely CO-EXIST — a column
 * whose validity window overlaps ANOTHER (different) column's window at some instant.
 * The single overlap-detection leaf shared by `resolveBindingAt`'s ambiguity gate
 * (#266) AND the picker's `pickerRepresentations` sequential-rename collapse
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
    period_scope?: string;
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
        ((a.period_scope === "year_independent" &&
          b.period_scope === "year_independent") ||
          (a.period_scope !== "year_independent" &&
            b.period_scope !== "year_independent" &&
            from(a) <= to(b) &&
            from(b) <= to(a)))
      ) {
        coexisting.add(a.delivery_column_name);
        coexisting.add(b.delivery_column_name);
      }
    }
  }
  return coexisting;
}

/** The display TEXT that TWO OR MORE distinct identities in `pairs` carry — the text
 * that therefore identifies nothing on its own. A curator variant name is not unique,
 * so two variant families can share one, and a picker row (or a variant filter value)
 * showing only that name would stand for either. The picker puts the concrete identity
 * back on exactly these — `pickerFilterDimensions`' variant values — and leaves every
 * other label alone. */
export function collidingLabels(
  pairs: Iterable<readonly [identity: string, label: string]>,
): Set<string> {
  const byLabel = new Map<string, Set<string>>();
  for (const [identity, label] of pairs) {
    const identities = byLabel.get(label) ?? new Set<string>();
    identities.add(identity);
    byLabel.set(label, identities);
  }
  const shared = new Set<string>();
  for (const [label, identities] of byLabel) {
    if (identities.size > 1) {
      shared.add(label);
    }
  }
  return shared;
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

// ── State-window display (#309 + #321) ──────────────────────────────────────

/** The open-ended `variable_state.valid_to` sentinel (the reg_meta_build DDL
 * default) — never shown to the user (#309). */
export const OPEN_ENDED_VALID_TO = "9999-12-31";

/** The yearless-fallback `variable_state.valid_from` sentinel — the floor
 * reg_meta_build writes when a state's start year is unknown (`_VALID_FROM_UNKNOWN`
 * in `reg_meta_build/db.py`; the `0001` twin of the `9999` ceiling). Like the ceiling it must NOT read as a literal year:
 * coverage `from` is unbounded/unknown for it, never year 1 (which would let the
 * slider emit out-of-grammar wires like `1..2026`). */
export const YEARLESS_VALID_FROM = "0001-01-01";

/**
 * The display form of a validity window (#309/#321):
 *  - wholly unknown (`0001-01-01` → `9999-12-31`) → `null`, so callers omit the
 *    display line instead of leaking a sentinel year;
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
  validFrom: string | null,
  validTo: string | null,
  periodToken?: string | null,
): string | null {
  if (validFrom === null || validTo === null)
    return validFrom === null && validTo === null ? "Year-independent" : null;
  if (validFrom === YEARLESS_VALID_FROM && validTo === OPEN_ENDED_VALID_TO) {
    return null;
  }
  if (validTo === OPEN_ENDED_VALID_TO) {
    return `since ${validFrom.endsWith("-01-01") ? validFrom.slice(0, 4) : validFrom}`;
  }
  // Unknown start (the yearless floor) with a finite end → a one-sided
  // `"until <to>"`, the mirror of `"since"` above. Wholly unknown windows
  // returned null above, so this arm only handles known finite ends (#658).
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

/** A window's first and last school term (term index = year × 2, +1 for the
 * autumn term) when it reads as terms, else null. The bounds are reg-core's
 * period grammar: VT (spring) is January–June, HT (autumn) July–December. A
 * window reads as terms when it is one term, or when it starts or ends mid-year
 * on a term bound (merged consecutive terms, HT2012–VT2013); a whole-year
 * window stays a year. */
function termSpan(
  from: string | null,
  to: string | null,
): [number, number] | null {
  if (from === null || to === null) return null;
  const start = from.endsWith("-01-01")
    ? 0
    : from.endsWith("-07-01")
      ? 1
      : null;
  const end = to.endsWith("-06-30") ? 0 : to.endsWith("-12-31") ? 1 : null;
  if (start === null || end === null) return null;
  const first = Number(from.slice(0, 4)) * 2 + start;
  const last = Number(to.slice(0, 4)) * 2 + end;
  if (last < first) return null;
  if (first !== last && start === 0 && end === 1) return null;
  return [first, last];
}

const termLabel = (index: number) =>
  `${index % 2 === 0 ? "VT" : "HT"}${Math.floor(index / 2)}`;

/** A list of windows as display labels, school terms written compactly
 * (maintainer ruling 2026-10-22): each term is `VT2013` / `HT2013`, a run of
 * consecutive terms is one range (`HT2012–VT2019`), and the same term in
 * consecutive years, the shape of a once-a-year school delivery, is the term
 * over a year range (`VT2013–2019`). A repeated term adds nothing. Any other
 * window reads as `label` says. Windows are taken in chronological order. */
export function windowRunLabels<
  W extends { from: string | null; to: string | null },
>(windows: readonly W[], label: (window: W) => string | null): string[] {
  // Runs of terms, each a first and last term index and its step: 1 for
  // consecutive terms, 2 for the same term a year apart (null while one long).
  type Run = { first: number; last: number; step: 1 | 2 | null };
  const runText = ({ first, last, step }: Run) =>
    first === last
      ? termLabel(first)
      : step === 2
        ? `${termLabel(first)}–${Math.floor(last / 2)}`
        : `${termLabel(first)}–${termLabel(last)}`;
  const out: string[] = [];
  let run: Run | null = null;
  const ordered = [...windows].sort((a, b) =>
    (a.from ?? "").localeCompare(b.from ?? ""),
  );
  for (const window of ordered) {
    const span = termSpan(window.from, window.to);
    if (span === null) {
      if (run !== null) out.push(runText(run));
      run = null;
      const text = label(window);
      if (text !== null) out.push(text);
      continue;
    }
    const [first, last] = span;
    if (run !== null) {
      // A term the run already covers (a repeat) adds nothing.
      if (last <= run.last) continue;
      const gap: number = first - run.last;
      // Adjacent or overlapping terms extend a consecutive run; one term a
      // year on extends a same-term run.
      const step: 1 | 2 | null =
        gap <= 1 ? 1 : gap === 2 && first === last ? 2 : null;
      if (step !== null && (run.step === null || run.step === step)) {
        run = { first: run.first, last, step };
        continue;
      }
      out.push(runText(run));
    }
    run = { first, last, step: first === last ? null : 1 };
  }
  if (run !== null) out.push(runText(run));
  return out;
}

/** The calendar years a set of ISO spans delivers: the Timeline's delivered
 * runs and edition ticks. An open end (`9999-12-31`) runs to
 * `ceiling`, the catalog vintage; a yearless start (`0001-01-01`) and a
 * year-independent span (no bounds) count nothing. */
export function deliveredYears(
  spans: Iterable<{ from: string | null; to: string | null }>,
  ceiling: number,
): number[] {
  const years = new Set<number>();
  for (const span of spans) {
    if (span.from === null || span.from === YEARLESS_VALID_FROM) continue;
    const lo = yearOf(span.from);
    const hi = span.to === OPEN_ENDED_VALID_TO ? ceiling : yearOf(span.to);
    if (lo === null || hi === null) continue;
    for (let y = lo; y <= Math.min(hi, ceiling); y += 1) years.add(y);
  }
  return [...years].sort((a, b) => a - b);
}

/** The 4-digit year of an ISO `YYYY-MM-DD` bound as an int, or null when it
 * isn't a leading-4-digit string (a blank/edge bound on a stale payload). */
export function yearOf(iso: string | null): number | null {
  const m = /^(\d{4})/.exec(iso ?? "");
  return m ? Number.parseInt(m[1], 10) : null;
}
