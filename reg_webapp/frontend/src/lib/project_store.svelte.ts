// TEMPORARY (React port, REACT_PORT.md): the reactive façade over the plain
// project store (`project_store.ts`) for the unported Svelte views. Deleted in
// port PR 9 with the last Svelte view.
import { projectStore as store } from "./project_store";
import { reactiveFacade } from "./svelte_store_facade.svelte";

export * from "./project_store";

export const projectStore = reactiveFacade(store);
