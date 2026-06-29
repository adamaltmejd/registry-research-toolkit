<script lang="ts">
import { onMount, untrack } from "svelte";
import { commandShortcutHint, isMacPlatform } from "./platform";
import { router } from "./router.svelte";

// The global header search box (#379): a plain routing input. The routing rule
// splits on whether you're already on the results page (SearchView):
//   - From any OTHER route, typing does NOT navigate — you stay put and the box
//     just holds text. Only Enter / form-submit routes you to the shareable
//     `/search?q=<query>` page. (A plain search box shouldn't yank you onto the
//     results page mid-word.)
//   - When ALREADY on `/search`, typing LIVE-REFINES in place: a debounced
//     replaceState updates `?q=` (and preserves `?type=`) without a new history
//     entry. You're already on the results page, so this isn't "being taken"
//     anywhere — it's the live-search we keep.
// The URL is the single source of truth for the results page, mirroring the
// `?period` pattern: deep-linkable, shareable, back/forward-correct. SearchView is
// the single search surface; the live-suggestion popup (the #689 Arm A Combobox
// spike) was removed — there is no in-box dropdown.
//
// Bidirectional URL↔box sync (the one subtle bit, on the `/search` route): the
// debounced effect writes the URL and an $effect mirrors the URL's `?q=` back into
// the box (so a deep-link / back-forward updates the input). Both sides assign
// `query` only when the value actually DIFFERS, so they can't ping-pong: typing →
// URL changes → the effect reads the SAME value it just wrote → no-op;
// back/forward → URL changes → the effect writes the box, but the debounced router
// write is a no-op (already at that URL). Convergent in one tick.

// 300ms (not 200) to cut request volume on the prod-slow GET /api/search that the
// routed results page fires: a longer settle keeps superseded queries from piling
// up server-side while the user is still typing.
const DEBOUNCE_MS = 300;

// Seed from `?q=` so a cold deep-link to `/search?q=foo` populates the box.
let query = $state(router.getQueryParam("q") ?? "");

// The command-bar shortcut (#803): Cmd+K on macOS, Ctrl+K elsewhere, FOCUSES
// this input from anywhere. Platform is read once (it doesn't change mid-session)
// so the badge + the modifier check agree; `isMacPlatform()` reads navigator
// (UA-Client-Hints first, see platform.ts). The badge glyph is platform-adaptive.
const mac = isMacPlatform();
const shortcutHint = commandShortcutHint(mac);
// The live <input>; bound so the global shortcut can focus + select it without
// reaching across components.
let inputEl = $state<HTMLInputElement | null>(null);
// Whether the <input> currently holds focus — used only to hide the redundant ⌘K
// hint badge while the box is focused.
let focused = $state(false);
const showEnterHint = $derived(
  focused && query.trim() !== "" && router.route.name !== "search",
);

// Register the global shortcut on the window: the listener lives in onMount so it
// tears down on unmount (the omnibox is persistent in the topbar, but a clean
// teardown keeps it test-safe). `(meta && mac) || (ctrl && !mac)` so the badge,
// the docs, and the actual chord all use the same platform decision — never
// Mac-only. preventDefault stops the browser's own Ctrl/Cmd+K (search-bar focus
// in some browsers) from also firing.
onMount(() => {
  const onKey = (e: KeyboardEvent) => {
    // Case-insensitive on the key: with CapsLock on, `e.key` is "K", so an exact
    // "k" compare would silently drop the shortcut. Shift+⌘K must still NOT
    // trigger (it's a distinct chord), so the altKey/shiftKey early-return stays.
    if (e.key.toLowerCase() !== "k" || e.altKey || e.shiftKey) {
      return;
    }
    if ((e.metaKey && mac) || (e.ctrlKey && !mac)) {
      e.preventDefault();
      inputEl?.focus();
      inputEl?.select();
    }
  };
  window.addEventListener("keydown", onKey);
  return () => window.removeEventListener("keydown", onKey);
});

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

// Debounced LIVE-REFINE on input — ONLY while already on `/search`. A burst of
// keystrokes refines once; off the search route the debounce does nothing, so the
// box never navigates on its own (Enter is the sole path to /search from
// elsewhere). Reading `query` AND `router.route.name` registers both as effect
// dependencies, so it re-evaluates on type and on route change. The teardown
// clears the pending timer. Skip the commit when the trimmed box already matches
// the URL's `q` — that covers the mount-time seed AND a value just adopted FROM
// the URL (deep-link / back-forward), so neither re-commits and re-enters the
// navigate path. It also closes the one race the bidirectional sync could have: a
// stale debounce from an adopted value firing after fresh typing.
$effect(() => {
  const raw = query;
  const onSearch = router.route.name === "search";
  if (!onSearch) {
    return;
  }
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

function onKeydown(event: KeyboardEvent): void {
  if (event.key === "Enter") {
    // Flush immediately (don't wait for the debounce); the form's onsubmit also
    // preventDefaults the native reload.
    event.preventDefault();
    commit(query);
  } else if (event.key === "Escape") {
    query = "";
  }
}
</script>

<form
  class="omnibox"
  class:enter-hint={showEnterHint}
  role="search"
  onsubmit={(e) => { e.preventDefault(); commit(query); }}
>
  <input
    bind:this={inputEl}
    bind:value={query}
    type="text"
    aria-label="Search the catalog"
    aria-describedby={showEnterHint ? "omnibox-enter-hint" : undefined}
    autocomplete="off"
    placeholder="Search registers, variables, codes…"
    onfocus={() => { focused = true; }}
    onblur={() => { focused = false; }}
    onkeydown={onKeydown}
  />

  <!-- Platform-adaptive shortcut hint (#803) while unfocused; Enter hint while a
       non-search route has uncommitted focused text. The shortcut badge is
       aria-hidden because the input's accessible name already names the control;
       the Enter hint is referenced through aria-describedby. -->
  {#if showEnterHint}
    <kbd id="omnibox-enter-hint" class="omnibox-hint">Enter</kbd>
  {:else if !focused}
    <kbd class="omnibox-hint" aria-hidden="true">{shortcutHint}</kbd>
  {/if}
</form>

<style>
  .omnibox {
    /* The command bar in the topbar (#803): grows to fill the available row,
       capped so it doesn't sprawl on a wide canvas. `min-width: 0` lets it shrink
       inside the topbar flex row without forcing horizontal overflow at 375px.
       (It previously forced its own header row with `flex: 1 1 100%`; the topbar
       no longer wraps it onto a separate line.) */
    position: relative;
    flex: 1 1 auto;
    min-width: 0;
    max-width: 32rem;
    margin: 0;
  }
  .omnibox input {
    width: 100%;
    box-sizing: border-box;
    /* Right padding leaves room for the absolutely-positioned shortcut badge. */
    padding: var(--space-2) var(--space-3);
    padding-right: 3.5rem;
    font: inherit;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
  }
  .omnibox input:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: var(--focus-ring);
  }

  /* The ⌘K / Ctrl+K hint badge, pinned to the right inside the input. Hidden
     while focused (rendered only when `!focused`). Pointer-events off so a click
     on the glyph still lands on the input beneath. */
  .omnibox-hint {
    position: absolute;
    top: 50%;
    right: var(--space-2);
    transform: translateY(-50%);
    pointer-events: none;
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    color: var(--text-faint);
    background: var(--surface-sunken);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    padding: 0.1rem 0.35rem;
    line-height: 1;
    white-space: nowrap;
  }

  /* Mobile (matches the AppShell drawer breakpoint): hide the ⌘K / Ctrl+K hint
     badge — it's misleading on a touch device with no physical keyboard, and it
     overlapped the placeholder when the bar was narrow. The global Cmd/Ctrl+K
     listener stays (harmless); only the visible badge is hidden. With the badge
     gone, drop the badge-clearing right padding so the placeholder uses the full
     width. */
  @media (max-width: 48rem) {
    .omnibox-hint[aria-hidden="true"] {
      display: none;
    }
    .omnibox:not(.enter-hint) input {
      padding-right: var(--space-3);
    }
  }
</style>
