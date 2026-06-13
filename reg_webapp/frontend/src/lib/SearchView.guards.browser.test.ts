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
});
