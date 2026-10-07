import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { SearchResponse } from "./api";
import { search } from "./api";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";
import { setQuery } from "./search-view-test-helpers";

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

describe("SearchView — scoped-search ?type= toggle (#393 item 1)", () => {
  const ONE_REGISTER: SearchResponse = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        has_more: false,
        next_cursor: null,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
    ],
  } as unknown as SearchResponse;

  it("renders the toggle whenever there's a query and marks 'All' active by default", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
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

  it("fetches the scoped result set for the URL's ?type=", async () => {
    // The stub answers only the register-scoped request, so LISA renders only if
    // the view forwards the URL's ?type=.
    vi.mocked(search).mockImplementation(async (_q, opts) =>
      opts?.type === "register"
        ? ONE_REGISTER
        : ({
            kind: "search",
            query: "kon",
            groups: [],
          } as unknown as SearchResponse),
    );
    // Deep-link straight to a scoped URL.
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);

    await expect
      .element(page.getByRole("link", { name: /^LISA/ }))
      .toBeVisible();
    // The scoped button is the active one.
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "true");
  });

  it("clicking a scope button routes ?type= and refetches scoped", async () => {
    // The stub answers only the Codes-scoped request, so LISA renders only once
    // the click refetched with that scope.
    vi.mocked(search).mockImplementation(async (_q, opts) =>
      opts?.type === "value"
        ? ONE_REGISTER
        : ({
            kind: "search",
            query: "kon",
            groups: [],
          } as unknown as SearchResponse),
    );
    setQuery("kon");
    await render(SearchView);

    await page.getByRole("button", { name: "Codes" }).click();

    // The URL gains ?type=value (the "Codes" toggle value).
    await expect.poll(() => router.getQueryParam("type")).toBe("value");
    // …and search refetches with that scope.
    await expect
      .element(page.getByRole("link", { name: /^LISA/ }))
      .toBeVisible();
  });

  it("clicking 'All' OMITS ?type= from the URL (clean canonical URL)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);
    await expect.poll(() => router.getQueryParam("type")).toBe("register");

    await page.getByRole("button", { name: "All" }).click();

    // Back to the default scope: no ?type= in the URL.
    await expect.poll(() => router.getQueryParam("type")).toBeNull();
  });

  it("degrades an unknown ?type= to 'all' (no crash, All active)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=bogus");
    await render(SearchView);

    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    // An unknown scope is sent to the server as "all" (omitted by the api layer).
    await expect
      .poll(() =>
        vi.mocked(search).mock.calls.some(([, opts]) => opts?.type === "all"),
      )
      .toBe(true);
  });

  it("renders the toggle while loading (so the user can switch scope mid-search)", async () => {
    // A never-resolving search keeps the view in the loading state.
    vi.mocked(search).mockReturnValue(new Promise<SearchResponse>(() => {}));
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("Searching…")).toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
  });
});
