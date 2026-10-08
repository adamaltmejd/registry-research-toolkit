// Shared helpers for the SearchView.*.browser.test.ts suites (browser-only:
// drives `window.history` and the `router` singleton).
import { vi } from "vitest";
import type { SearchHit, SearchPage, SearchType } from "./api";
import { search } from "./api";
import { router } from "./router.svelte";

export function setQuery(q: string): void {
  // Reset to a SENTINEL URL distinct from the target first so `navigate` isn't a
  // no-op (its guard compares the full URL), then route to the target — this
  // re-syncs the singleton's reactive route/search regardless of where a prior
  // test left it. (A bare `pushState` to the same path would no-op the navigate
  // and leak the prior test's `?q=`.)
  window.history.pushState({}, "", "/__reset__");
  router.navigate(`/search?q=${encodeURIComponent(q)}`);
}

export function pillLabel(element: Element | null | undefined): string | null {
  return element?.childNodes[0]?.textContent?.trim() ?? null;
}

/** The first page of each call SearchView makes: `top` is the untyped call, the
 * other keys the arm a typed call names. A bare array is a last page. */
export type Pages = Partial<
  Record<SearchType | "top", readonly SearchHit[] | SearchPage>
>;

function asPage(entry: Pages[keyof Pages]): SearchPage {
  if (entry === undefined) return { items: [], next_cursor: null };
  return "items" in entry ? entry : { items: [...entry], next_cursor: null };
}

/** Answer each `search` call (the test file mocks `./api`) with its page. */
export function mockSearch(pages: Pages): void {
  vi.mocked(search).mockImplementation(async (_q, options) =>
    asPage(pages[options?.type ?? "top"]),
  );
}
