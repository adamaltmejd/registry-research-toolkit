/// <reference types="vitest/config" />

import { svelte } from "@sveltejs/vite-plugin-svelte";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [svelte()],
  server: {
    // Dev proxy: the SPA fetches /api/* from the FastAPI backend on :8000.
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  // Two Vitest projects share this one Vite pipeline (so `.svelte` / `.svelte.ts`
  // resolve and compile identically via the svelte() plugin above):
  //   • `unit`    — jsdom, for pure logic + rune-MODULE tests (`*.test.ts`).
  //   • `browser` — real Chromium via Playwright, for `.svelte` COMPONENT tests
  //                 (`*.browser.test.ts`). jsdom can't faithfully run Svelte 5
  //                 runes reactivity, so component wiring is tested in a real
  //                 browser (#201). `bun run test` (= `vitest run`) runs both.
  test: {
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          environment: "jsdom",
          include: ["src/**/*.test.ts"],
          // Component tests belong to the `browser` project below — exclude them
          // here (their `.browser.test.ts` suffix also matches `*.test.ts`).
          exclude: ["src/**/*.browser.test.ts"],
        },
      },
      {
        extends: true,
        test: {
          name: "browser",
          include: ["src/**/*.browser.test.ts"],
          // vitest-browser-svelte injects `render`/`cleanup` (it auto-cleans
          // BEFORE each test) and its locator types via this setup entry.
          setupFiles: ["vitest-browser-svelte"],
          browser: {
            enabled: true,
            // Vitest 4.1 takes a provider FACTORY, not the old "playwright" string.
            provider: playwright(),
            headless: true,
            instances: [{ browser: "chromium" }],
          },
        },
      },
    ],
  },
});
