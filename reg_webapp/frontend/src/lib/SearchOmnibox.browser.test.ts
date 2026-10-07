import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { isMacPlatform } from "./platform";
import { router } from "./router.svelte";
import SearchOmnibox from "./SearchOmnibox.svelte";

// The omnibox is a plain routing search box: typing ROUTES the (debounced) query
// to `/search?q=…` (the SearchView results page is the single search surface — the
// #689 live-suggestion popup was removed). These tests exercise the URL↔box sync,
// Escape-clear, ?type= preservation, and the ⌘K focus shortcut. The box is a plain
// `<input aria-label="Search the catalog">` — queried by its textbox role.

// The box is queried by its accessible name (a plain textbox, no longer a
// combobox).
function box() {
  return page.getByRole("textbox", { name: "Search the catalog" });
}

// Typing routes through a ~300 ms debounce. Fake the timer while typing and run
// it out, so a case observes the settled result without waiting it out.
async function typeAndSettle(text: string): Promise<void> {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    await box().fill(text);
    await vi.runOnlyPendingTimersAsync();
  } finally {
    vi.useRealTimers();
  }
}

function press(input: HTMLInputElement, key: string): void {
  input.focus();
  input.dispatchEvent(
    new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true }),
  );
}

function setUrl(path: string): void {
  // Reset to a SENTINEL URL distinct from `path` first, so the subsequent
  // `navigate(path)` isn't a no-op (the guard compares the full URL) and actually
  // re-syncs the singleton's reactive route/search regardless of where a prior
  // test left it. A bare `pushState(path)` + `navigate(path)` would no-op and
  // leave the singleton's `search` stale (the prior test's `?q=` would leak).
  window.history.pushState({}, "", "/__reset__");
  router.navigate(path);
}

beforeEach(() => {
  setUrl("/");
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("SearchOmnibox — URL↔box sync (#379)", () => {
  it("does NOT navigate when typing from another route — only Enter routes to /search", async () => {
    // New behavior (maintainer direction): a plain search box must not yank you
    // onto /search mid-word. From a non-search route the debounce is inert; typing
    // leaves you put, and Enter is the sole path to the results page.
    await render(SearchOmnibox);
    // Run out the debounce, then assert we're STILL on the home route — typing did
    // not navigate.
    await typeAndSettle("kon");
    expect(router.route.name).toBe("home");
    expect(router.getQueryParam("q")).toBeNull();

    // Enter (form submit) commits immediately — NOW we route to /search?q=kon via
    // pushState. (This assertion still fails if Enter-navigation breaks.)
    const form = (box().element() as HTMLInputElement).form;
    form?.requestSubmit();

    await expect.poll(() => router.route.name).toBe("search");
    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
  });

  it("shows an Enter hint while a non-search route has uncommitted text", async () => {
    await render(SearchOmnibox);

    await box().fill("kon");

    await expect.element(page.getByText("Enter")).toBeVisible();
    await expect.element(box()).toHaveAccessibleDescription(/Enter/);
  });

  it("refines in place (replaceState) while already on /search — no back-stack spam", async () => {
    setUrl("/search?q=ko");
    await render(SearchOmnibox);
    const lenBefore = window.history.length;
    await typeAndSettle("kon");

    expect(router.getQueryParam("q")).toBe("kon");
    // Refinement replaces the current entry — the history stack doesn't grow.
    expect(window.history.length).toBe(lenBefore);
  });

  it("does not navigate when Enter follows an Escape-cleared box (blank commit is a no-op)", async () => {
    // From a non-search route: type, clear via Escape, then submit. `commit`'s
    // blank-trimmed early return must keep us off /search (no stray pushState).
    await render(SearchOmnibox);
    await box().fill("kon");
    press(box().element() as HTMLInputElement, "Escape");
    await expect.element(box()).toHaveValue("");

    const form = (box().element() as HTMLInputElement).form;
    form?.requestSubmit();

    // The route stays at home (the beforeEach base, #675) — a blank query never
    // enters /search. Poll so a (hypothetical) async navigation would still be
    // caught before asserting.
    await expect.poll(() => router.route.name).toBe("home");
  });

  it("preserves an active ?type= scope when refining the query (#393 item 1)", async () => {
    // On a scoped /search URL, typing more into the box must NOT reset the scope
    // back to "all" — the committed URL carries the existing ?type= forward.
    setUrl("/search?q=ko&type=value");
    await render(SearchOmnibox);
    await typeAndSettle("kon");

    expect(router.getQueryParam("q")).toBe("kon");
    expect(router.getQueryParam("type")).toBe("value");
  });

  it("does not add a ?type= when none is present (default scope stays clean)", async () => {
    setUrl("/search?q=ko");
    await render(SearchOmnibox);
    await typeAndSettle("kon");

    expect(router.getQueryParam("q")).toBe("kon");
    expect(router.getQueryParam("type")).toBeNull();
  });

  it("adopts the URL's ?q on a back/forward (popstate) without ping-pong", async () => {
    setUrl("/search?q=first");
    await render(SearchOmnibox);
    await expect.element(box()).toHaveValue("first");

    // Simulate a back/forward landing on a different ?q (jsdom-style: change the
    // location, then fire popstate so the singleton re-reads it).
    window.history.pushState({}, "", "/search?q=second");
    window.dispatchEvent(new PopStateEvent("popstate"));

    await expect.element(box()).toHaveValue("second");
    // And it stays put — the URL→box effect doesn't fight the box→URL effect.
    await expect.poll(() => router.getQueryParam("q")).toBe("second");
  });
});

describe("⌘K shortcut (#803)", () => {
  it("focuses the omnibox input on the platform chord", async () => {
    // The global keydown listener (onMount) focuses + selects the box. The
    // component keys the chord off the SAME `isMacPlatform()` decision the badge
    // does (Meta+K on mac, Ctrl+K elsewhere), so dispatch the modifier the live
    // host resolves to — the test-runner host (real Chromium) reports macOS here,
    // so the live branch is Meta+K, not Ctrl. The mac/non-mac DECISION itself is
    // unit-tested in platform.test.ts; this exercises the focus wiring on one
    // (host-correct) path.
    const mac = isMacPlatform();
    await render(SearchOmnibox);
    const input = box().element() as HTMLInputElement;

    // Start unfocused so the focus is observably the shortcut's doing.
    input.blur();
    expect(document.activeElement).not.toBe(input);

    window.dispatchEvent(
      new KeyboardEvent("keydown", {
        key: "k",
        ctrlKey: !mac,
        metaKey: mac,
        bubbles: true,
      }),
    );

    await vi.waitFor(() => expect(document.activeElement).toBe(input));
  });
});
