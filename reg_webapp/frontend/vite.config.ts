/// <reference types="vitest/config" />

import { svelte } from "@sveltejs/vite-plugin-svelte";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vite";
import { configDefaults } from "vitest/config";

// vite.config.ts runs under Node; read the env via globalThis so we don't pull a
// @types/node dep for one lookup. REG_WEBAPP_BACKEND_URL repoints the dev /api proxy
// so concurrent instances (parallel worktrees / PR lanes) can each target their own
// backend port — see reg_webapp/.claude/skills/run-reg-webapp "Parallel instances".
const backendUrl =
  (globalThis as { process?: { env?: Record<string, string | undefined> } })
    .process?.env?.REG_WEBAPP_BACKEND_URL ?? "http://localhost:8000";

export default defineConfig({
  plugins: [svelte()],
  server: {
    // Dev proxy: the SPA fetches /api/* from the FastAPI backend (default :8000).
    proxy: {
      "/api": {
        target: backendUrl,
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
          // here (their `.browser.test.ts` suffix also matches `*.test.ts`). A
          // custom `exclude` REPLACES Vitest's default (node_modules, .git), so
          // spread the defaults back in or the unit run loses that guard.
          exclude: [...configDefaults.exclude, "src/**/*.browser.test.ts"],
        },
      },
      {
        extends: true,
        test: {
          name: "browser",
          include: ["src/**/*.browser.test.ts"],
          // vitest-browser-svelte injects `render`/`cleanup` (it auto-cleans
          // BEFORE each test) and its locator types via this setup entry. The
          // second entry loads the global design tokens + fonts into the test
          // document (this suite never evaluates main.ts) so component styling
          // matches the app — see src/test-setup.browser.ts.
          setupFiles: ["vitest-browser-svelte", "./src/test-setup.browser.ts"],
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
