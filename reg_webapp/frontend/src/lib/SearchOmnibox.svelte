<script lang="ts">
import { untrack } from "svelte";
import { router } from "./router.svelte";

// The global header search box (#379). A live, debounced query that ROUTES to the
// shareable `/search?q=<query>` results page (SearchView) — the URL is the single
// source of truth, mirroring the `?period` pattern: deep-linkable, shareable,
// back/forward-correct.
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
// queries from piling up server-side.
const DEBOUNCE_MS = 300;

// Seed from `?q=` so a cold deep-link to `/search?q=foo` populates the box.
let query = $state(router.getQueryParam("q") ?? "");

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

function onSubmit(event: SubmitEvent): void {
  // Flush immediately on Enter (don't wait for the debounce) and prevent the
  // form's default full-page reload.
  event.preventDefault();
  commit(query);
}

function onKeydown(event: KeyboardEvent): void {
  if (event.key === "Escape") {
    query = "";
  }
}
</script>

<form class="omnibox" role="search" onsubmit={onSubmit}>
  <input
    type="search"
    aria-label="Search the catalog"
    autocomplete="off"
    placeholder="Search registers, variables, codes…"
    bind:value={query}
    onkeydown={onKeydown}
  />
</form>

<style>
  .omnibox {
    /* Own row in the wrapping header (flex-basis 100% forces the wrap). */
    flex: 1 1 100%;
    margin: 0;
  }
  .omnibox input {
    width: 100%;
    box-sizing: border-box;
    padding: 0.4rem 0.6rem;
    font: inherit;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
  }
  .omnibox input:focus {
    outline: none;
    border-color: var(--accent);
  }
</style>
