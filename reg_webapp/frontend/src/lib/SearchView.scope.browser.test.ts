import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { SearchPage } from "./api";
import { search } from "./api";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";
import { mockSearch, type Pages, setQuery } from "./search-view-test-helpers";

// Split from SearchView.browser.test.ts by contract surface:
// scoped-search ?type= toggle. Siblings: SearchView.*.browser.test.ts.

// Stub the search GET the view drives; keep the rest of api.ts real (the type
// exports). SearchView reads `?q=` off the `router` singleton, so each case sets
// the URL (and re-syncs the singleton's reactive `search`) before rendering.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    search: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(search).mockReset();
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

// The (type, limit) of every `search` call so far, in call order.
function calls(): { type: string | undefined; limit: number | undefined }[] {
  return vi.mocked(search).mock.calls.map(([, options]) => ({
    type: options?.type,
    limit: options?.limit,
  }));
}

describe("SearchView — scoped-search ?type= toggle (#393 item 1)", () => {
  const ONE_REGISTER: Pages = {
    register: [
      { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
    ],
  };

  it("renders the toggle whenever there's a query and marks 'All' active by default", async () => {
    mockSearch(ONE_REGISTER);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
    // No ?type= → "All" is the active (aria-pressed) button.
    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "false");
  });

  it("fetches only the URL's ?type= arm", async () => {
    // Fails if a scoped view still makes the untyped top-results call or asks
    // for another arm.
    mockSearch(ONE_REGISTER);
    // Deep-link straight to a scoped URL.
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);

    await expect
      .element(page.getByRole("link", { name: /^LISA/ }))
      .toBeVisible();
    expect(calls()).toEqual([{ type: "register", limit: 3 }]);
    // The scoped button is the active one.
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "true");
  });

  it("clicking 'Codes / values' routes ?type=value and fetches both code arms", async () => {
    // `value` is the SPA's name for two server arms; fails if the click does not
    // refetch, or sends `value` (not a server type) instead of the two arms.
    mockSearch({});
    setQuery("kon");
    await render(SearchView);
    await expect.poll(() => calls().length).toBe(6);

    await page.getByRole("button", { name: "Codes" }).click();

    // The URL gains ?type=value (the "Codes" toggle value).
    await expect.poll(() => router.getQueryParam("type")).toBe("value");
    await expect
      .poll(() => calls().slice(6))
      .toEqual([
        { type: "classification_code", limit: 3 },
        { type: "register_value", limit: 3 },
      ]);
  });

  it("clicking 'All' OMITS ?type= from the URL (clean canonical URL)", async () => {
    mockSearch(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);
    await expect.poll(() => router.getQueryParam("type")).toBe("register");

    await page.getByRole("button", { name: "All" }).click();

    // Back to the default scope: no ?type= in the URL.
    await expect.poll(() => router.getQueryParam("type")).toBeNull();
  });

  it("degrades an unknown ?type= to 'all': one untyped page and one page per arm", async () => {
    // Decision 17: the top-results strip is one untyped call (5 hits); every arm
    // has its own call (3 hits). Fails if the view sends the unknown type, drops
    // an arm, or changes a page size.
    mockSearch(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=bogus");
    await render(SearchView);

    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect.poll(() => calls().length).toBe(6);
    expect(calls()).toEqual([
      { type: undefined, limit: 5 },
      { type: "register", limit: 3 },
      { type: "variable", limit: 3 },
      { type: "classification", limit: 3 },
      { type: "classification_code", limit: 3 },
      { type: "register_value", limit: 3 },
    ]);
  });

  it("renders the toggle while loading (so the user can switch scope mid-search)", async () => {
    // A never-resolving search keeps the view in the loading state.
    vi.mocked(search).mockReturnValue(new Promise<SearchPage>(() => {}));
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("Searching…")).toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
  });
});
