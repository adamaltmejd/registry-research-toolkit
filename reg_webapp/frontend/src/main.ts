import "./tokens.css";
import "./lib/ui/utilities.css";
import { mount } from "svelte";
import App from "./App.svelte";
import { IndexedDBPersistence } from "./lib/indexeddb_persistence";
import {
  AUTOSAVE_KEY,
  setPersistence,
  storeSchemaVersion,
} from "./lib/project_store.svelte";
import { initRegCore } from "./lib/reg_core";

// A5.4 production persistence wiring (the store default stays InMemoryPersistence
// for tests; this swaps in the IndexedDB drop-in before mount).
setPersistence(new IndexedDBPersistence(AUTOSAVE_KEY, storeSchemaVersion));

const target = document.getElementById("app");
if (!target) {
  throw new Error("#app mount point not found");
}

// reg-core (WASM) checks every draft synchronously, so it loads before the app
// mounts. Without it the app cannot author a project: say so in place of the app
// rather than mount one that throws on the first edit.
initRegCore().then(
  () => mount(App, { target }),
  (error: unknown) => {
    const alert = document.createElement("p");
    alert.setAttribute("role", "alert");
    alert.textContent =
      "The app could not load one of its parts. Reload the page; if this keeps happening, try again later.";
    target.replaceChildren(alert);
    throw error;
  },
);
