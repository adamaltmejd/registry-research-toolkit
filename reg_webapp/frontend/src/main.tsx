import "./tokens.css";
import "./lib/ui/utilities.css";
import "./lib/ui/motion.css";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { IndexedDBPersistence } from "./lib/indexeddb_persistence";
import {
  AUTOSAVE_KEY,
  setPersistence,
  startDraftLifecycle,
  storeSchemaVersion,
} from "./lib/project_store";
import { initRegCore } from "./lib/reg_core";

// A5.4 production persistence wiring (the store default stays InMemoryPersistence
// for tests; this swaps in the IndexedDB drop-in before the lifecycle starts).
setPersistence(new IndexedDBPersistence(AUTOSAVE_KEY, storeSchemaVersion));

const target = document.getElementById("app");
if (!target) {
  throw new Error("#app mount point not found");
}

// reg-core (WASM) checks every draft synchronously, so it loads before the app
// renders. Without it the app cannot author a project: say so in place of the app
// rather than render one that throws on the first edit.
initRegCore().then(
  () => {
    startDraftLifecycle();
    createRoot(target).render(
      <StrictMode>
        <App />
      </StrictMode>,
    );
  },
  (error: unknown) => {
    const alert = document.createElement("p");
    alert.setAttribute("role", "alert");
    alert.textContent =
      "The app could not load one of its parts. Reload the page; if this keeps happening, try again later.";
    target.replaceChildren(alert);
    throw error;
  },
);
