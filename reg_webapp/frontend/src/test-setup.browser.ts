// Vitest `browser` project setup: the `*.browser.test.ts` suite renders
// components directly via vitest-browser-svelte and does NOT evaluate main.ts,
// so the design-system tokens + fonts would be absent otherwise. Importing the
// global stylesheet here loads it into the real-Chromium test document so
// token-dependent component styling matches the app (DESIGN.md → Token
// architecture). Listed in vite.config.ts's browser `setupFiles` AFTER
// vitest-browser-svelte (which injects render/cleanup). The `.micro-label`
// utility (#836) is a sibling global stylesheet, imported the same way so a
// component's label header (DataTable th, Panel title, KeyValue dt) renders
// with its styling under test just as it does in the app.
import { commands } from "vitest/browser";
import "./tokens.css";
import "./lib/ui/utilities.css";
import { initRegCore } from "./lib/reg_core";

declare module "vitest/browser" {
  interface BrowserCommands {
    keepRequestInterception: () => Promise<{ path: string; marker: string }>;
  }
}

// Before the test file's `vi.mock` routes, make request interception live for
// this iframe, or a mocked module can load unmocked — see
// `keepRequestInterception` in vite.config.ts. Only the route answers the
// probe with the marker; Vite would not.
const probe = await commands.keepRequestInterception();
const deadline = performance.now() + 2000;
for (let attempt = 0; ; attempt++) {
  const body = await fetch(`${probe.path}?${attempt}`).then(
    (response) => response.text(),
    () => "",
  );
  if (body === probe.marker) break;
  if (performance.now() > deadline) {
    throw new Error(
      "Playwright request interception did not reach this test iframe within 2s; vi.mock routes would be bypassed.",
    );
  }
}

// reg-core (WASM) checks every draft synchronously; main.ts loads it before mount,
// and this suite never evaluates main.ts.
await initRegCore();
