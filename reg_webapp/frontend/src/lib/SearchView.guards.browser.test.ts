import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";

// Robustness guards (PR B) on top of #379's rendering. These exercise the REAL
// `search`/`apiGet` (no `./api` mock — unlike the sibling rendering suite) by
// stubbing `window.fetch`, so the AbortController/timeout/min-length wiring
// actually runs end to end against a controllable fetch. Each case sets `?q=` on
// the real router (the view's single input) before rendering.

const EMPTY_SEARCH_BODY = { kind: "search", query: "", groups: [] };

function setQuery(q: string): void {
  // Reset to a sentinel URL so `navigate` isn't a no-op (its guard compares the
  // full URL), then route to the target — mirrors the rendering suite's helper.
  window.history.pushState({}, "", "/__reset__");
  router.navigate(`/search?q=${encodeURIComponent(q)}`);
}

afterEach(() => {
  window.history.pushState({}, "", "/");
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("SearchView — request cancellation (supersede)", () => {
  it("aborts the prior in-flight request when the query changes, with no error", async () => {
    // First query's fetch never resolves on its own — it can only END by abort.
    // Capture the signal it received so we can assert the supersede aborts it.
    let firstSignal: AbortSignal | undefined;
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      if (firstSignal === undefined) {
        firstSignal = init?.signal ?? undefined;
        // A never-settling promise that rejects only when the signal aborts (the
        // shape a real aborted fetch takes), so asyncResource's catch arm runs.
        return new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(init.signal?.reason),
          );
        });
      }
      // The superseding query resolves cleanly with one register hit.
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({
          kind: "search",
          query: "kon",
          groups: [
            {
              group: "registers",
              total_count: 1,
              results: [
                {
                  type: "register",
                  fqid: "scb/lisa",
                  name: "LISA",
                  purpose: null,
                },
              ],
            },
          ],
        }),
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    setQuery("ko");
    await render(SearchView);
    // The first request is in flight (signal captured, not yet aborted).
    await vi.waitFor(() => expect(firstSignal).toBeDefined());
    expect(firstSignal?.aborted).toBe(false);

    // Supersede: change the query. asyncResource's teardown aborts run 1's signal.
    setQuery("kon");
    await vi.waitFor(() => expect(firstSignal?.aborted).toBe(true));

    // The fresh query renders; the aborted one must NOT surface as an error.
    await expect
      .element(page.getByRole("link", { name: /LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByText(/Search failed/))
      .not.toBeInTheDocument();
    await expect.element(page.getByText(/timed out/)).not.toBeInTheDocument();
  });

  it("deleting back below the min length aborts the in-flight request and shows keep-typing (no error/no-matches flash)", async () => {
    // Symmetric to the supersede case but the new query is TOO SHORT: the "ko"
    // fetch is in flight; deleting to "k" hits the min-length short-circuit (no
    // new fetch), and the effect re-run's teardown must still abort the "ko"
    // request — without flashing a spurious "Search failed" / "No matches".
    let inFlightSignal: AbortSignal | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((_url: string, init?: RequestInit) => {
        inFlightSignal = init?.signal ?? undefined;
        return new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(init.signal?.reason),
          );
        });
      }),
    );

    setQuery("ko");
    await render(SearchView);
    await vi.waitFor(() => expect(inFlightSignal).toBeDefined());
    expect(inFlightSignal?.aborted).toBe(false);

    // Delete a character: "k" is below the min length → short-circuit, no fetch.
    setQuery("k");
    await vi.waitFor(() => expect(inFlightSignal?.aborted).toBe(true));

    await expect
      .element(page.getByText("Keep typing to search…"))
      .toBeVisible();
    await expect
      .element(page.getByText(/Search failed/))
      .not.toBeInTheDocument();
    await expect.element(page.getByText(/No matches/)).not.toBeInTheDocument();
  });
});

describe("SearchView — classification_succession routing (#571)", () => {
  it("routes a mixed classifications group (leaf + succession) through the real guards", async () => {
    // Exercise the REAL search/apiGet path (this suite stubs window.fetch, no
    // ./api mock) so the `isClassificationSuccession` guard actually runs over the
    // mixed classifications union: a plain leaf hit AND a folded succession row.
    // Both must render their own shape (leaf link + succession disclosure), with
    // no each_key_duplicate crash.
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            kind: "search",
            query: "sun",
            groups: [
              {
                group: "classifications",
                total_count: 2,
                results: [
                  {
                    type: "classification",
                    fqid: "class/lkf2020",
                    short_name: "LKF",
                    name: "Län/kommun/församling",
                  },
                  {
                    type: "classification_succession",
                    fqid: "class/sun2020",
                    short_name: "SUN",
                    name: "SUN 2020",
                    matched_count: 2,
                    editions: [
                      {
                        slug: "sun2020",
                        fqid: "class/sun2020",
                        name: "SUN 2020",
                        effective_year: 2020,
                      },
                      {
                        slug: "sun1996",
                        fqid: "class/sun1996",
                        name: "SUN 1996",
                        effective_year: 1996,
                      },
                    ],
                  },
                ],
              },
            ],
          }),
        }),
      ),
    );

    setQuery("sun");
    await render(SearchView);

    // The plain leaf renders as a direct link…
    await expect
      .element(page.getByRole("link", { name: /LKF/ }))
      .toHaveAttribute("href", "/catalog/class/lkf2020");
    // …and the succession row renders its folded editions hint (the guard routed
    // it to the succession snippet, not the leaf snippet).
    await expect
      .element(page.getByText("matched 2 of 2 editions"))
      .toBeVisible();
    // No render crash — the search did not wedge on "Searching…".
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });
});

describe("SearchView — min query length", () => {
  beforeEach(() => {
    // Any fetch here would be a bug — a 1-char query must NOT hit the network.
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve({
          ok: true,
          status: 200,
          json: async () => EMPTY_SEARCH_BODY,
        }),
      ),
    );
  });

  it("shows the keep-typing hint and fires NO fetch for a single character", async () => {
    setQuery("k");
    await render(SearchView);

    await expect
      .element(page.getByText("Keep typing to search…"))
      .toBeVisible();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("fetches once the query reaches two characters", async () => {
    setQuery("ko");
    await render(SearchView);

    await vi.waitFor(() => expect(fetch).toHaveBeenCalled());
    const url = vi.mocked(fetch).mock.calls[0][0] as string;
    expect(url).toContain("/api/search?q=ko");
  });
});

describe("SearchView — timeout", () => {
  it("shows the friendly 'timed out' message when the request times out", async () => {
    // A fetch that never settles EXCEPT via the timeout abort. `search` layers
    // AbortSignal.timeout, so the combined signal aborts with a TimeoutError —
    // asyncResource (NOT cancelled) surfaces it, and SearchView maps the
    // name-prefixed error to the timeout copy. Stub the floor low so the test
    // doesn't wait the real 12s.
    const realTimeout = AbortSignal.timeout.bind(AbortSignal);
    vi.spyOn(AbortSignal, "timeout").mockImplementation(() => realTimeout(50));
    vi.stubGlobal(
      "fetch",
      vi.fn((_url: string, init?: RequestInit) => {
        return new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(init.signal?.reason),
          );
        });
      }),
    );

    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByText("Search timed out — try a more specific term."))
      .toBeVisible();
    // It is NOT the generic failure banner.
    await expect
      .element(page.getByText(/Search failed/))
      .not.toBeInTheDocument();
  });

  it("a timeout that fires AFTER a successful resolution does not flip the good view to an error", async () => {
    // The timeout signal still fires (50ms) after fetch already RESOLVED — but on
    // a settled promise the abort is a no-op, so the rendered results must stay
    // put (no "timed out" copy). Guards against a stray timeout clobbering a good
    // view. Stub the floor low so the post-resolution wait is short.
    const realTimeout = AbortSignal.timeout.bind(AbortSignal);
    vi.spyOn(AbortSignal, "timeout").mockImplementation(() => realTimeout(50));
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            kind: "search",
            query: "kon",
            groups: [
              {
                group: "registers",
                total_count: 1,
                results: [
                  {
                    type: "register",
                    fqid: "scb/lisa",
                    name: "LISA",
                    purpose: null,
                  },
                ],
              },
            ],
          }),
        }),
      ),
    );

    setQuery("kon");
    await render(SearchView);

    // Results render first (poll before sleeping past the stub timeout to avoid
    // racing the resolution).
    await expect
      .element(page.getByRole("link", { name: /LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");

    // Wait past the 50ms stub timeout, then confirm the view is unchanged.
    await new Promise((r) => setTimeout(r, 100));
    await expect
      .element(page.getByRole("link", { name: /LISA/ }))
      .toBeVisible();
    await expect.element(page.getByText(/timed out/)).not.toBeInTheDocument();
    await expect
      .element(page.getByText(/Search failed/))
      .not.toBeInTheDocument();
  });
});
