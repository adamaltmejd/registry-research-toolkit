// TEMPORARY (React port, REACT_PORT.md): the reactive façade over the plain
// window store (`window.ts`) for the unported Svelte views. Deleted in port PR 9
// with the last Svelte view.
import { reactiveFacade } from "./svelte_store_facade.svelte";
import { windowStore as store } from "./window";

export const windowStore = reactiveFacade(store);
