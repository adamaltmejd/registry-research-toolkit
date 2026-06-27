// Vitest `browser` project setup: the `*.browser.test.ts` suite renders
// components directly via vitest-browser-svelte and does NOT evaluate main.ts,
// so the design-system tokens + fonts would be absent otherwise. Importing the
// global stylesheet here loads it into the real-Chromium test document so
// token-dependent component styling matches the app (DESIGN.md → Token
// architecture). Listed in vite.config.ts's browser `setupFiles` AFTER
// vitest-browser-svelte (which injects render/cleanup). The `.micro-label`
// utility (#836) is a sibling global stylesheet, imported the same way so a
// component's eyebrow header (DataTable th, Panel title, KeyValue dt) renders
// with its styling under test just as it does in the app.
import "./tokens.css";
import "./lib/ui/utilities.css";
