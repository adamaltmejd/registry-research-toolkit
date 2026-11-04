import { useSyncExternalStore } from "react";
import { type Route, router } from "./router";

const readRoute = (): Route => router.route;

/** The current route. Keyed on the pathname only: a query-only navigation hands
 * back the same object, so nothing derived from it re-renders (Y-65). */
export function useRoute(): Route {
  return useSyncExternalStore(router.subscribe, readRoute);
}
