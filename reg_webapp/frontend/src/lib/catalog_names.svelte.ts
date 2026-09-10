/**
 * The catalog DISPLAY-NAME cache — one module-singleton read per catalog fact the
 * app needs in order to NAME something it only holds a coordinate for
 * (`.svelte.ts` so the compiler processes the runes).
 *
 * The #991 cart stores coordinates and nothing else: `project_data.json` carries a
 * `register_variant` and a variable FQID, and an ordinary pick writes no column
 * name at all (`representation` stays null when the variable resolves to one
 * column — issue #992). The words a researcher recognises — "LISA", "Individer, 16
 * år och äldre", "Kon" — live only in the catalog, so the cart READS them here.
 * Nothing read here is ever written back into the draft.
 *
 * Every read is cached under its own key for the session, so a cart of 100 columns
 * issues each request ONCE — never again on a re-render — and every source on one
 * provider shares the single read that names its registers. The root read is the
 * one the shell's rail facets already make, so the cart's provider word costs no
 * extra request.
 *
 * A read that has not landed, or that failed, reads as `null`/`[]`: the caller
 * shows the coordinate it already holds. Failure is deliberately NOT an error
 * surface — a coordinate outside this steward's catalog, or an unreachable backend,
 * must leave the cart readable, and a machine coordinate is honest where an
 * invented name would not be.
 */

import {
  getCatalogNode,
  getCatalogRoot,
  getRegisterVariants,
  isCatalogNode,
  type RootResponse,
} from "./api";
import {
  deliveryColumnNamesFromStates,
  fqidSegments,
  registerPrefixOf,
  variantCardName,
} from "./catalog";

/** One cached catalog read. `value` is null until it lands and STAYS null when it
 * failed; `loading` tells those two apart (the caller's `aria-busy`, and a settled
 * null is a read that had nothing to give). Each entry is REPLACED, never mutated,
 * so a caller holding one can trust its identity: a `$derived` over an unchanged
 * read stops there instead of re-running everything downstream of it. */
export interface Read<T> {
  readonly value: T | null;
  readonly loading: boolean;
}

/** Every read this session, by key, held in ONE source.
 *
 * Not a `$state` per entry: entries are created lazily, inside the very `$derived`
 * that asks for them, and Svelte deliberately does not make a source a dependency
 * of the reaction that CREATED it — a per-entry source would never wake its first
 * reader. This one exists before any reaction runs, so every reader depends on it.
 *
 * `.raw`, so the two mutations differ: ADDING an in-flight entry writes into the
 * current object and notifies nobody, which is what makes starting a read from a
 * `$derived` safe; a read that SETTLES replaces the object, which wakes every
 * reader — cheap, they are all lookups. */
let reads = $state.raw<Record<string, Read<unknown>>>({});

/** The shared in-flight entry: a read that has not landed says the same thing
 * whatever it is reading. */
const PENDING: Read<never> = { value: null, loading: true };

/** The shared never-started entry — what a caller with nothing to ask about gets,
 * so it too holds a stable identity. */
export const UNASKED: Read<never> = { value: null, loading: false };

/** Which CACHE the in-flight reads belong to — bumped by every reset, so a read
 * started before one cannot write into the cache that replaced it. */
let generation = 0;

/** The cached read for `key`, STARTING it on first ask. Repeat asks (a re-render,
 * a second card on the same register) get the same entry and issue no request. */
function read<T>(key: string, fetcher: () => Promise<T>): Read<T> {
  const hit = reads[key] as Read<T> | undefined;
  if (hit) {
    return hit;
  }
  reads[key] = PENDING;
  const startedIn = generation;
  const settle = (value: T | null) => {
    // A read the reset abandoned settles into nothing: repopulating the fresh
    // cache with the previous one's answer would hand a case the response the
    // case before it stubbed.
    if (startedIn === generation) {
      reads = { ...reads, [key]: { value, loading: false } };
    }
  };
  // A failed name read is not an error surface — see the module comment.
  fetcher().then(settle, () => settle(null));
  return PENDING;
}

/** Drop every cached read, in-flight ones included. The test seam: the cache is a
 * session singleton, so a case that stubs a DIFFERENT catalog response for the
 * same coordinate must start from an empty one. */
export function resetCatalogNames(): void {
  generation += 1;
  reads = {};
}

/** The catalog root's children — the deployment's providers (plus the
 * classification root), for the shell's facet rail and for `sourceNames`'
 * provider word. */
export function catalogRootChildren(): Read<RootResponse["children"]> {
  return read("root", async () => (await getCatalogRoot()).children ?? []);
}

/** Whether this deployment serves more than one PROVIDER (false until the root read
 * lands) — which is what makes a bare register name ambiguous, and so what qualifies
 * a source card's title with its provider. The threshold lives here, read once at
 * the app's root and threaded down like `steward`.
 *
 * Counted off the catalog ROOT, not `/api/stats.providers`, because the shell's
 * rail already pays for that read — this is the canonical answer for "does a
 * register name need its provider", and `/api/stats` remains the deployment's own
 * headline count on `/`. */
export function providerQualified(): boolean {
  return providersOf(catalogRootChildren()).length > 1;
}

/** The PROVIDER nodes of a root read — the root also carries the classification
 * sentinel, which owns no register and qualifies no name. One spelling, so the
 * count above and the gate in `sourceNames` cannot drift apart. */
function providersOf(
  root: Read<RootResponse["children"]>,
): RootResponse["children"] {
  return (root.value ?? []).filter((child) => child.kind === "provider");
}

/** The display names of one source's `register_variant` coordinate — all null until
 * EVERY read behind them has landed with an answer, and null for good when any of
 * them cannot be answered. All-or-nothing on purpose: the caller shows the
 * coordinate until it can say the whole name, rather than settling in two steps or
 * saying a half-name that reads like a different source. `variant` alone is null
 * once named for `_default`, which names no population (#673) — the register alone
 * is then the whole title. */
export interface SourceNames {
  provider: string | null;
  register: string | null;
  variant: string | null;
  loading: boolean;
}

/** Resolve a `provider/register/variant` coordinate to the catalog's own words.
 * A coordinate with fewer than 3 segments names no variant to look up and issues
 * no request. */
export function sourceNames(registerVariant: string): SourceNames {
  const [provider, register, variant] = fqidSegments(registerVariant);
  if (!provider || !register || !variant) {
    return { provider: null, register: null, variant: null, loading: false };
  }
  const registerFqid = registerPrefixOf(registerVariant);
  const root = catalogRootChildren();
  // The PROVIDER's node names every register it owns, in one small payload. The
  // register's OWN node would name just this one and carry every variable in the
  // register along with it — so a cart on several registers of one provider reads
  // once here, and reads less.
  const registersRead = read(`registers:${provider}`, async () => {
    const node = await getCatalogNode(provider);
    return isCatalogNode(node) && node.kind === "provider" ? node.children : [];
  });
  const variantsRead = read(`variants:${registerFqid}`, async () => {
    const response = await getRegisterVariants(registerFqid);
    return response.variants ?? [];
  });
  const loading = root.loading || registersRead.loading || variantsRead.loading;
  // Trimmed to null, so "no name" is ONE condition: the heading falls back to the
  // coordinate on a blank register name, and the gate below has to agree with it
  // or the card shows a coordinate heading with a variant line beside it.
  const registerName =
    (registersRead.value ?? [])
      .find((r) => r.fqid === registerFqid)
      ?.name?.trim() || null;
  const variantName = variantCardName(variantsRead.value ?? [], variant);
  const providers = providersOf(root);
  const providerName =
    providers.find((child) => child.fqid === provider)?.name ?? null;
  // The ROOT is part of the all-or-nothing: where the deployment serves more than
  // one provider the title CARRIES the provider, so a root that failed — or that
  // does not list this coordinate's provider — leaves the name incomplete, and an
  // unqualified "MiDAS" would read as the unambiguous register of a
  // single-provider deployment. `loading` already covers the root still being in
  // flight; `root.value === null` covers it having settled with nothing.
  const named =
    !loading &&
    root.value !== null &&
    registerName !== null &&
    variantName !== null &&
    (providers.length < 2 || providerName !== null);
  return {
    provider: named ? providerName : null,
    register: named ? registerName : null,
    // `_default` is named, and names no population — it contributes no word (#673).
    variant: named ? variantName || null : null,
    loading,
  };
}

/** The delivery column NAMES a binding's variable resolves to at its source's
 * `(register_variant, period)`, newest era first (see
 * `deliveryColumnNamesFromStates`) — null while the read is in flight, when it
 * failed, and when nothing there names one column.
 *
 * The same `?period`+`?variant` leaf resolve the picker runs on a pick
 * (`resolveBindingAt`), cached per `(fqid, period, variant)` so two columns of one
 * source, and every re-render of either, share the one request. A source with no
 * period cannot be resolved at all (the resolve needs one) and issues none — nor
 * can one with no VARIANT (a `register_variant` of fewer than 3 segments, which
 * only a malformed draft carries): resolving the variable across every variant
 * would name a column this source does not deliver. */
export function columnNames(
  fqid: string,
  period: string | null,
  variant: string,
): Read<string[]> {
  if (!fqid || !period || !variant) {
    return UNASKED;
  }
  return read(`resolve:${fqid}?${period}&${variant}`, async () => {
    const resolved = await getCatalogNode(fqid, { period, variant });
    return isCatalogNode(resolved)
      ? []
      : deliveryColumnNamesFromStates(resolved.states ?? []);
  });
}
