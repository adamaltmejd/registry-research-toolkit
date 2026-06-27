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

// A5.4 production persistence wiring (the store default stays InMemoryPersistence
// for tests; this swaps in the IndexedDB drop-in before mount).
setPersistence(new IndexedDBPersistence(AUTOSAVE_KEY, storeSchemaVersion));

const target = document.getElementById("app");
if (!target) {
  throw new Error("#app mount point not found");
}

const app = mount(App, { target });

export default app;
