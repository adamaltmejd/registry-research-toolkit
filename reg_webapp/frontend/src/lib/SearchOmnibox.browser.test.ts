import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { router } from "./router.svelte";
import SearchOmnibox from "./SearchOmnibox.svelte";

// The omnibox routes through the real `router` singleton (no API mock — it only
// touches the URL). Each case resets the URL and re-syncs the singleton before
// rendering. The debounce is ~200ms; the assertions poll, so they ride past it.
//
// Typing uses the locator's atomic `.fill()` (the sibling browser tests' idiom —
// one `input` event, not per-keystroke), so the debounce sees a single value.
// Enter/Escape are dispatched as real KeyboardEvents on the focused input (the
// component's handlers key off `event.key`).

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
  it("seeds the box from ?q on a deep-link to /search", async () => {
    setUrl("/search?q=lisa");
    await render(SearchOmnibox);

    await expect.element(page.getByRole("searchbox")).toHaveValue("lisa");
  });

  it("routes to /search?q=… (pushState) when typing from another route", async () => {
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    // The debounced commit enters the search route via pushState (one entry).
    await expect.poll(() => router.route.name).toBe("search");
    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
  });

  it("refines in place (replaceState) while already on /search — no back-stack spam", async () => {
    setUrl("/search?q=ko");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    const lenBefore = window.history.length;
    await box.fill("kon");

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    // Refinement replaces the current entry — the history stack doesn't grow.
    expect(window.history.length).toBe(lenBefore);
  });

  it("commits immediately on Enter (does not full-reload the page)", async () => {
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");
    // Enter submits the single-input form; the handler preventDefaults the reload
    // and flushes the commit. `requestSubmit()` runs the form's onsubmit (a bare
    // dispatch of a non-cancelable submit wouldn't).
    const form = (box.element() as HTMLInputElement).form;
    form?.requestSubmit();

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
  });

  it("clears the box on Escape", async () => {
    setUrl("/search?q=kon");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await expect.element(box).toHaveValue("kon");

    press(box.element() as HTMLInputElement, "Escape");
    await expect.element(box).toHaveValue("");
  });

  it("does not navigate when Enter follows an Escape-cleared box (blank commit is a no-op)", async () => {
    // From a non-search route: type, clear via Escape, then submit. `commit`'s
    // blank-trimmed early return must keep us off /search (no stray pushState).
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");
    press(box.element() as HTMLInputElement, "Escape");
    await expect.element(box).toHaveValue("");

    const form = (box.element() as HTMLInputElement).form;
    form?.requestSubmit();

    // The route stays at root — a blank query never enters /search. Poll so a
    // (hypothetical) async navigation would still be caught before asserting.
    await expect.poll(() => router.route.name).toBe("root");
  });

  it("preserves an active ?type= scope when refining the query (#393 item 1)", async () => {
    // On a scoped /search URL, typing more into the box must NOT reset the scope
    // back to "all" — the committed URL carries the existing ?type= forward.
    setUrl("/search?q=ko&type=value");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    await expect.poll(() => router.getQueryParam("type")).toBe("value");
  });

  it("does not add a ?type= when none is present (default scope stays clean)", async () => {
    setUrl("/search?q=ko");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    await expect.poll(() => router.getQueryParam("type")).toBeNull();
  });

  it("adopts the URL's ?q on a back/forward (popstate) without ping-pong", async () => {
    setUrl("/search?q=first");
    await render(SearchOmnibox);
    await expect.element(page.getByRole("searchbox")).toHaveValue("first");

    // Simulate a back/forward landing on a different ?q (jsdom-style: change the
    // location, then fire popstate so the singleton re-reads it).
    window.history.pushState({}, "", "/search?q=second");
    window.dispatchEvent(new PopStateEvent("popstate"));

    await expect.element(page.getByRole("searchbox")).toHaveValue("second");
    // And it stays put — the URL→box effect doesn't fight the box→URL effect.
    await expect.poll(() => router.getQueryParam("q")).toBe("second");
  });
});
