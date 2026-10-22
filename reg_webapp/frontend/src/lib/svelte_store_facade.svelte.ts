// TEMPORARY (React port, REACT_PORT.md): the unported Svelte views (and the
// `useRune` readers in svelte_bridge) read the plain stores through these
// façades, which make every read reactive. Deleted in port PR 9 with the last
// Svelte view.
import { createSubscriber } from "svelte/reactivity";

/** `store` with every property read tracked as a Svelte dependency on its
 * `subscribe`. Methods are bound to the store itself, so a class store's private
 * fields stay reachable through the proxy. */
export function reactiveFacade<T extends object>(
  store: T & { subscribe: (listener: () => void) => () => void },
): T {
  const track = createSubscriber((update) => store.subscribe(update));
  return new Proxy(store, {
    get(target, key) {
      track();
      const value: unknown = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
