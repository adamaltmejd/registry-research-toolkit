// Vitest `browser` project setup: the `*.browser.test.ts` suite renders
// components directly via vitest-browser-svelte and does NOT evaluate main.ts,
// so the design-system tokens + fonts would be absent otherwise. Importing the
// global stylesheet here loads it into the real-Chromium test document so
// token-dependent component styling matches the app (DESIGN.md → Token
// architecture). Listed in vite.config.ts's browser `setupFiles` AFTER
// vitest-browser-svelte (which injects render/cleanup).
import "./tokens.css";
