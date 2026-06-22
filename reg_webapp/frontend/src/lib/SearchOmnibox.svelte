<script lang="ts">
import { Combobox } from "bits-ui";
import { untrack } from "svelte";
import { SEARCH_MIN_QUERY_LENGTH, type SearchResponse, search } from "./api";
import { catalogHref } from "./catalog";
import { router } from "./router.svelte";

// The global header search box (#379), rebuilt as an accessible COMBOBOX on Bits
// UI's headless `Combobox` primitive (UI-foundation spike Arm A, #689). It keeps
// the original routing behavior unchanged AND adds live suggestions:
//
//   • Routing (unchanged): a live, debounced query ROUTES to the shareable
//     `/search?q=<query>` results page (SearchView) — the URL is the single
//     source of truth, mirroring the `?period` pattern: deep-linkable, shareable,
//     back/forward-correct. Enter / the debounced commit / Escape all behave as
//     before (see `commit` + the two sync effects below).
//   • Suggestions (new): the same debounced query also fetches GET /api/search
//     (limit 8) and flattens the top hits across the four typed groups into a
//     popup listbox; selecting a suggestion NAVIGATES straight to its catalog
//     node (skipping the results page). Bits UI owns the listbox ARIA, keyboard
//     nav, focus, and dismissal; we own the data + the routing fallback.
//
// Bidirectional URL↔box sync (the one subtle bit): typing writes the URL
// (debounced) and an $effect mirrors the URL's `?q=` back into the box (so a
// deep-link / back-forward updates the input). Both sides assign `query` only
// when the value actually DIFFERS, so they can't ping-pong: typing → URL changes
// → the effect reads the SAME value it just wrote → no-op; back/forward → URL
// changes → the effect writes the box, but the debounced router write is a no-op
// (already at that URL). Convergent in one tick.

// 300ms (not 200) to further cut request volume: the omnibox is the first SPA
// consumer of GET /api/search, whose codes/value sub-query is currently slow in
// prod (a backend index fix is in flight), so a longer settle keeps superseded
// queries from piling up server-side. Shared by the routing commit AND the
// suggestion fetch (one debounce, one settled query drives both).
const DEBOUNCE_MS = 300;

// Top-N suggestions across all four groups (the popup is a quick-jump shortcut,
// not the full results page — that's what routing to /search is for).
const SUGGESTION_LIMIT = 8;

// Seed from `?q=` so a cold deep-link to `/search?q=foo` populates the box.
let query = $state(router.getQueryParam("q") ?? "");
// Whether the suggestion popup is open (bound to the Combobox). We own this flag
// rather than letting Bits UI manage it freely: a reconcile effect drives it to
// track `suggestions.length > 0 && !dismissed` (see below), so the popup is open
// EXACTLY when there are suggestions for the CURRENT query that the user hasn't
// dismissed — no stale-query options, no first-batch-hidden race.
let open = $state(false);
// The Combobox's selected item value (its href). Bound so we can RESET it to ""
// after handling a selection — otherwise Bits UI keeps the chosen href selected,
// and re-selecting the same option later is a no-op (no `onValueChange`), so that
// suggestion would silently stop navigating (#689 review #3).
let selectedValue = $state("");
// User dismissed the popup for the CURRENT query (Escape on an open popup). Gates
// the reconcile effect so a dismiss isn't immediately undone by suggestions still
// being present; cleared whenever the query text changes (a new query reopens).
let dismissed = $state(false);

/** One popup option: the suggestion's display label + the catalog route to
 * navigate to on selection. `value` is the Combobox item value — we use the href
 * (a stable, unique key per option) so `onValueChange` can route directly. */
interface Suggestion {
  value: string;
  label: string;
  /** A short type/context tag shown muted after the label (e.g. the register). */
  context: string | null;
}

let suggestions = $state<Suggestion[]>([]);

/** Flatten a SearchResponse's four ordered groups into a capped, deduped option
 * list. Each leaf hit with a resolvable `fqid` (codes use the owning entity's
 * fqid) becomes one option labelled by its display name; non-addressable folded
 * rows (concept groups) are skipped here — the popup is a quick-jump to concrete
 * nodes, and the full results page renders the folded families. */
function flatten(resp: SearchResponse): Suggestion[] {
  const out: Suggestion[] = [];
  const seen = new Set<string>();
  const push = (
    fqid: string | null | undefined,
    label: string | null | undefined,
    context: string | null,
  ) => {
    if (!fqid || seen.has(fqid) || out.length >= SUGGESTION_LIMIT) {
      return;
    }
    seen.add(fqid);
    out.push({ value: catalogHref(fqid), label: label ?? fqid, context });
  };
  for (const group of resp.groups) {
    if (group.group === "registers") {
      for (const r of group.results) {
        push(r.fqid, r.name, "register");
      }
    } else if (group.group === "variables") {
      for (const r of group.results) {
        if (r.type === "variable") {
          push(r.fqid, r.name, r.register ?? "variable");
        }
      }
    } else if (group.group === "classifications") {
      for (const r of group.results) {
        if (r.type === "classification") {
          push(r.fqid, r.short_name ?? r.name, "classification");
        } else if (r.type === "classification_succession") {
          push(r.fqid, r.short_name ?? r.name, "classification");
        }
      }
    } else if (group.group === "codes") {
      for (const r of group.results) {
        // A code's actionable target is its OWNING entity (variable, then
        // classification) — the bare (code,label) isn't FQID-addressable. Owner
        // `fqid`s are nullable (a malformed/unresolvable slug → null), so pick the
        // FIRST owner with a non-null fqid, scanning variables before
        // classifications; only skip the code if NONE is addressable. (Taking
        // `variables[0]` unconditionally would drop a quick-jump whenever the first
        // variable owner happened to be unaddressable — #689 review #6.) The two
        // owner shapes differ (variable has `name`; classification has
        // `short_name`/`name`), so label each on its own branch.
        const fallback = `${r.code} ${r.label}`;
        const variable = r.variables.find((v) => v.fqid);
        if (variable) {
          push(variable.fqid, variable.name ?? fallback, `${r.code}`);
          continue;
        }
        const classification = r.classifications.find((c) => c.fqid);
        if (classification) {
          push(
            classification.fqid,
            classification.short_name ?? classification.name ?? fallback,
            `${r.code}`,
          );
        }
      }
    }
  }
  return out;
}

/** Route to / refine the search page for a trimmed query. ENTERING search
 * (not already on the route) is a pushState entry; refining in place is a
 * replaceState (no back-stack spam). A blank trimmed query is a no-op here —
 * an empty box doesn't navigate away on its own (the user clears, then types). */
function commit(raw: string): void {
  const q = raw.trim();
  if (!q) {
    return;
  }
  // Preserve an active `?type=` scope (#393 item 1) so typing more into the box
  // doesn't silently reset the SearchView toggle back to "all". `all` is the
  // server default (the SearchView omits it from the URL), so a present `?type=`
  // is always a non-`all` scope worth carrying.
  const type = router.getQueryParam("type");
  const url = `/search?q=${encodeURIComponent(q)}${type ? `&type=${type}` : ""}`;
  if (router.route.name !== "search") {
    router.navigate(url);
  } else {
    router.replace(url);
  }
}

// Debounced commit on input: a burst of keystrokes routes once. Reading `query`
// registers it as the effect dependency; the teardown clears the pending timer.
// Skip the commit when the trimmed box already matches the URL's `q` — that
// covers the mount-time seed AND a value just adopted FROM the URL (deep-link /
// back-forward), so neither re-commits and re-enters the navigate path. It also
// closes the one race the bidirectional sync could have: a stale debounce from an
// adopted value firing after fresh typing.
$effect(() => {
  const raw = query;
  const timer = setTimeout(() => {
    if (raw.trim() === (router.getQueryParam("q") ?? "").trim()) {
      return;
    }
    commit(raw);
  }, DEBOUNCE_MS);
  return () => clearTimeout(timer);
});

// Debounced suggestion fetch (separate timer from the routing commit so a fetch
// failure can NEVER affect routing). Reads `query` for dependency tracking;
// short-circuits a too-short query to an empty list WITHOUT a network call.
// Resilience: a fetch error/abort (e.g. no backend in the test env, or a
// superseded query) just yields NO suggestions — it must not break the search
// box. This isolation is what keeps the existing no-mock browser test green.
$effect(() => {
  const raw = query.trim();
  // The query text changed: drop the PRIOR query's suggestions IMMEDIATELY
  // (before the debounced fetch resolves) so a slow `/api/search` can never leave
  // options for an old query rendered/selectable under the new text — clicking one
  // would navigate to an unrelated node (#689 review #1). The brief empty-popup
  // flicker during the 300ms debounce is the accepted cost; correctness wins.
  // Also clear the user-dismiss flag: a new query is a fresh chance to suggest.
  suggestions = [];
  dismissed = false;
  if (raw.length < SEARCH_MIN_QUERY_LENGTH) {
    return;
  }
  const controller = new AbortController();
  const timer = setTimeout(() => {
    // Last-request-wins: a slow older fetch can resolve AFTER a newer query has
    // re-rendered (abort doesn't guarantee the in-flight promise rejects before
    // it resolves), so only mutate `suggestions` if this request is still the
    // current query. `query.trim()` is read at settle time (untracked — this is a
    // guard, not a dependency; the effect already re-runs on `query`).
    const isCurrent = () => untrack(() => query.trim()) === raw;
    search(raw, { limit: SUGGESTION_LIMIT, signal: controller.signal })
      .then((resp) => {
        if (isCurrent()) {
          suggestions = flatten(resp);
        }
      })
      .catch(() => {
        // Network error / timeout / supersede-abort — degrade to no suggestions,
        // but only for the CURRENT query (a superseded request's failure must not
        // wipe a newer query's already-rendered suggestions).
        if (isCurrent()) {
          suggestions = [];
        }
      });
  }, DEBOUNCE_MS);
  return () => {
    clearTimeout(timer);
    controller.abort();
  };
});

// THE open-state contract: the popup is open EXACTLY when there are suggestions
// for the current query AND the user hasn't dismissed them. This single effect
// owns `open` so it stays honest in both directions:
//   • OPENS when an async fetch returns a non-empty list (typing opens Bits UI's
//     combobox before the fetch resolves; without this nothing would reopen it
//     once the first batch arrives — #689 review #2);
//   • CLOSES when the list goes empty (too-short query, fetch miss, or a stale
//     list just cleared on edit) so `open`/`aria-expanded`/the Escape branch never
//     lie about a popup that isn't rendered (the Content is gated on the same
//     `suggestions.length > 0`).
// Loop-safe: writing `open` doesn't feed back into `suggestions` or `dismissed`,
// so it converges in one pass; the `open !== shouldOpen` guard avoids a redundant
// write (and fighting Bits UI's own focus/blur toggles per keystroke).
$effect(() => {
  const shouldOpen = suggestions.length > 0 && !dismissed;
  if (open !== shouldOpen) {
    open = shouldOpen;
  }
});

// URL → box: when on the search route and the URL's `q` differs from the box,
// adopt it (deep-link / back-forward). This effect must fire ONLY on a URL change
// — NOT when the box changes — or it would revert mid-typing (box="kon" while the
// URL is still "?q=ko" → it would clobber "kon" back to "ko"). So track only the
// router (`route`/`search`, read via getQueryParam) and read/assign `query` under
// `untrack`. The box→URL effect remains the sole writer driven by typing; this one
// is the sole writer driven by navigation. That one-way-each split is what makes
// the bidirectional sync converge instead of ping-pong.
$effect(() => {
  if (router.route.name !== "search") {
    return;
  }
  const urlQ = router.getQueryParam("q") ?? "";
  untrack(() => {
    if (urlQ !== query) {
      query = urlQ;
    }
  });
});

/** Selecting a suggestion (`onValueChange` fires the chosen item's `value`, which
 * is its catalog href) navigates straight to that node — skipping the results
 * page. The shell's `use:link` interception is for <a> clicks; this is a
 * programmatic jump, so it routes via the router directly. */
function onSelect(href: string): void {
  if (href) {
    router.navigate(href);
    // Clearing `query` does three things at once: (1) empties `suggestions` (the
    // query-change effect), which the reconcile-effect turns into `open = false`,
    // closing the popup; (2) neutralizes the pending routing-commit timer — the
    // commit $effect keys on `query`, so re-running it reschedules a BLANK commit
    // (a no-op via `commit`'s blank-trimmed early return), so the stale timer
    // can't fire ~300ms later and clobber the catalog node we just navigated to;
    // (3) re-runs the suggestion $effect, whose teardown aborts any in-flight
    // fetch. The URL→box adopt-effect returns early here (we're now on the
    // catalog-node route, not /search), so clearing the box can't fight that sync.
    query = "";
    // Reset the Combobox's selected value. Bits UI keeps the chosen href selected
    // otherwise, so if the SAME href surfaces again later, re-selecting it is
    // treated as re-selecting the already-selected item — `onValueChange` does NOT
    // fire and the suggestion silently stops navigating (#689 review #3). Clearing
    // to "" lets the next selection of any href (including this one) fire again.
    selectedValue = "";
  }
}

/** Whether a suggestion was highlighted at the START of the current keydown
 * (captured in the composed handler BEFORE Bits UI runs, since Bits UI's Enter
 * handler selects + closes, clearing the highlight). Lets the Enter fallback
 * distinguish "select the highlighted option" (Bits UI handled it) from "route
 * the typed query to /search" (nothing highlighted). */
let hadHighlightOnKeydown = false;

function onKeydown(event: KeyboardEvent, wasOpen: boolean): void {
  // Enter: route to /search (the original behavior) ONLY when no suggestion was
  // highlighted. When one was, Bits UI already selected it (→ onValueChange →
  // onSelect navigates to the node); committing here too would double-navigate.
  if (event.key === "Enter") {
    if (!hadHighlightOnKeydown) {
      // Flush immediately (don't wait for the debounce). Bits UI preventDefaults
      // the input's Enter, so there's no native form submit to fight.
      commit(query);
    }
    return;
  }
  if (event.key === "Escape") {
    // Branch on the popup state SNAPSHOT taken BEFORE Bits UI's handler ran (the
    // caller captures it): Bits UI's Escape synchronously closes the popup via
    // `bind:open`, so reading the live `open` here would always see `false` and
    // wrongly clear the text even when Escape only meant "dismiss the popup".
    //   • popup was OPEN → dismiss it, PRESERVE the text. Set `dismissed` so the
    //     reconcile effect keeps it closed (suggestions are still present) until
    //     the query changes — otherwise it would reopen on the next tick.
    //   • popup was CLOSED → clear the box (the original empty-box Escape).
    if (wasOpen) {
      dismissed = true;
    } else {
      query = "";
    }
  }
}
</script>

<form class="omnibox" role="search" onsubmit={(e) => { e.preventDefault(); commit(query); }}>
  <!-- `inputValue={query}` is the WRITE-DOWN side: it drives Bits UI's displayed
       input text, so the URL↔box sync (deep-link / back-forward writing `query`)
       updates the box. The READ-UP side is the composed `oninput` below, which
       copies the typed text back into `query` (Bits UI's `inputValue` prop is not
       `$bindable`, so this is the supported round-trip). `onValueChange` fires the
       SELECTED option's value (its href) → navigate. -->
  <Combobox.Root
    type="single"
    bind:open
    bind:value={selectedValue}
    inputValue={query}
    onValueChange={onSelect}
    items={suggestions}
  >
    <!-- The `child` snippet renders our OWN <input> so we can compose our routing
         handlers onto Bits UI's. We KEEP Bits UI's default `role="combobox"` (from
         `{...props}`) — that accessible combobox semantics (expandable suggestions)
         is the whole point of the migration; overriding to `role="searchbox"` would
         make screen readers announce a plain search field and hide the listbox. We
         pin `type="text"` so the native `search` input type can't re-impose the
         searchbox role.
         CRUCIAL: a plain `{...props}` spread + an explicit `onkeydown`/`oninput`
         would REPLACE Bits UI's handlers (Svelte doesn't compose spread handlers),
         silently killing the popup's open/keyboard/highlight behavior — so we
         COMPOSE explicitly: call Bits UI's handler first, then ours. `value` is
         left to flow from `props` (= `inputValue` = `query`); overriding it would
         desync the displayed text from `query`. -->
    <Combobox.Input>
      {#snippet child({ props })}
        {@const bitsKeydown = props.onkeydown as
          | ((e: KeyboardEvent) => void)
          | undefined}
        {@const bitsInput = props.oninput as
          | ((e: Event) => void)
          | undefined}
        <input
          {...props}
          type="text"
          aria-label="Search the catalog"
          autocomplete="off"
          placeholder="Search registers, variables, codes…"
          oninput={(e) => {
            // Bits UI's oninput first (updates its internal inputValue + opens /
            // highlights), then mirror the typed text into our `query` (the
            // single source for the debounced routing + suggestion effects).
            bitsInput?.(e);
            query = (e.currentTarget as HTMLInputElement).value;
          }}
          onkeydown={(e) => {
            // Capture the highlight BEFORE Bits UI's handler runs (its Enter
            // selects + closes, clearing aria-activedescendant).
            hadHighlightOnKeydown =
              !!(e.currentTarget as HTMLElement).getAttribute(
                "aria-activedescendant",
              );
            // Snapshot the popup-open state BEFORE Bits UI's handler — Escape
            // synchronously closes it via `bind:open`, so the Escape branch must
            // see the pre-close value to distinguish dismiss-popup from clear-box.
            const wasOpen = open;
            bitsKeydown?.(e);
            onKeydown(e, wasOpen);
          }}
        />
      {/snippet}
    </Combobox.Input>

    {#if suggestions.length > 0}
      <Combobox.Portal>
        <Combobox.Content class="omnibox-popup" sideOffset={4}>
          <Combobox.Viewport>
            {#each suggestions as suggestion (suggestion.value)}
              <Combobox.Item
                value={suggestion.value}
                label={suggestion.label}
                class="omnibox-option"
              >
                <span class="opt-label">{suggestion.label}</span>
                {#if suggestion.context}
                  <span class="opt-context">{suggestion.context}</span>
                {/if}
              </Combobox.Item>
            {/each}
          </Combobox.Viewport>
        </Combobox.Content>
      </Combobox.Portal>
    {/if}
  </Combobox.Root>
</form>

<style>
  .omnibox {
    /* Own row in the wrapping header (flex-basis 100% forces the wrap). */
    flex: 1 1 100%;
    margin: 0;
  }
  .omnibox :global(input) {
    width: 100%;
    box-sizing: border-box;
    padding: var(--space-2) var(--space-3);
    font: inherit;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--surface);
  }
  .omnibox :global(input:focus) {
    outline: none;
    border-color: var(--accent);
    box-shadow: var(--focus-ring);
  }

  /* The popup listbox. Bits UI renders Content into a Portal (outside this
     component's DOM subtree), so these rules are :global — scoped by the
     `omnibox-popup` / `omnibox-option` classes we pass through. */
  :global(.omnibox-popup) {
    z-index: 50;
    width: var(--bits-combobox-anchor-width);
    max-height: 18rem;
    overflow-y: auto;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: 0 4px 12px rgb(0 0 0 / 0.12);
  }
  :global(.omnibox-option) {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    padding: var(--space-2) var(--space-3);
    font-size: var(--text-base);
    cursor: pointer;
    /* Bits UI exposes the keyboard-highlighted option via data-highlighted and
       the chosen one via data-selected — style both off the token surfaces. */
  }
  :global(.omnibox-option:hover) {
    background: var(--surface-hover);
  }
  :global(.omnibox-option[data-highlighted]) {
    background: var(--surface-selected);
  }
  :global(.omnibox-option .opt-label) {
    font-weight: 600;
  }
  :global(.omnibox-option .opt-context) {
    margin-left: auto;
    color: var(--muted);
    font-size: var(--text-sm);
  }
</style>
