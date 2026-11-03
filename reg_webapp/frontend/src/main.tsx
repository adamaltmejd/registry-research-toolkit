import "./tokens.css";
import "./lib/ui/utilities.css";
import "./lib/ui/motion.css";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { startDraftLifecycle } from "./lib/project_store";
import { initRegCore } from "./lib/reg_core";

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
