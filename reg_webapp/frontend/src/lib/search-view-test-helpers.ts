// Shared helpers for the SearchView.*.browser.test.ts suites (browser-only:
// drives `window.history` and the `router` singleton).
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
