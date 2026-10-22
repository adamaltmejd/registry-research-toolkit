// TEMPORARY (React port, REACT_PORT.md): the unported Svelte views read the vanilla
// router (`router.ts`) through this façade, which makes every read reactive. Deleted
// in port PR 9 with the last Svelte view.
import { createSubscriber } from "svelte/reactivity";
import { router as store } from "./router";

const track = createSubscriber((update) => store.subscribe(update));

export const router = {
  get route() {
    track();
    return store.route;
  },
  get search() {
    track();
    return store.search;
  },
  getQueryParam(name: string): string | null {
    track();
    return store.getQueryParam(name);
  },
  navigate: store.navigate,
};
