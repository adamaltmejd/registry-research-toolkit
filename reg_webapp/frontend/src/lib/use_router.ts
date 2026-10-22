import { useSyncExternalStore } from "react";
import { type Route, router } from "./router";

const readRoute = (): Route => router.route;

/** The current route. Keyed on the pathname only: a query-only navigation hands
 * back the same object, so nothing derived from it re-renders (Y-65). */
export function useRoute(): Route {
  return useSyncExternalStore(router.subscribe, readRoute);
}

/** One query parameter (`router.getQueryParam`: null when absent or empty).
 * A string snapshot, so a navigation that leaves it alone re-renders nothing. */
export function useQueryParam(name: string): string | null {
  return useSyncExternalStore(router.subscribe, () =>
    router.getQueryParam(name),
  );
}
