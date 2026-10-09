import { svelte } from "@sveltejs/vite-plugin-svelte";
import { playwright } from "@vitest/browser-playwright";
// defineConfig from vitest/config (not vite): it natively types the `test` block.
// Vitest 4 dropped the module augmentation that let a `/// <reference types="vitest/config" />`
// add `test` to vite's own UserConfig, so import the vitest-aware defineConfig directly.
import { configDefaults, defineConfig } from "vitest/config";
import type { BrowserCommand } from "vitest/node";

// vite.config.ts runs under Node; read the env via globalThis so we don't pull a
// @types/node dep for one lookup. REG_WEBAPP_BACKEND_URL repoints the dev /api proxy
// so concurrent instances (parallel worktrees / PR lanes) can each target their own
// backend port — see reg_webapp/.claude/skills/run-reg-webapp "Parallel instances".
// REG_META_SERVER_URL does the same for the Rust server (`reg-meta serve`), which
// answers the routes ported to it.
const runtimeProcess = (
  globalThis as {
    process?: { env?: Record<string, string | undefined>; platform?: string };
  }
).process;
const backendUrl =
  runtimeProcess?.env?.REG_WEBAPP_BACKEND_URL ?? "http://localhost:8000";
const rustServerUrl =
  runtimeProcess?.env?.REG_META_SERVER_URL ?? "http://127.0.0.1:8001";
const isCodexSeatbeltSandbox =
  runtimeProcess?.env?.CODEX_SANDBOX === "seatbelt";
const isMacOS = runtimeProcess?.platform === "darwin";
const needsSingleProcessChromium = isMacOS && isCodexSeatbeltSandbox;

// Playwright 1.61's Chromium build registers a Mach rendezvous port on macOS. The
// Codex seatbelt sandbox denies that registration; single-process Chromium avoids
// the blocked multi-process bootstrap. Normal local runs and Linux CI keep the
// standard browser path.
const chromiumLaunchArgs = needsSingleProcessChromium
  ? ["--single-process"]
  : [];

// The Playwright provider implements `vi.mock` as a `context.route()` per test
// file and removes it when the file ends, so request interception switches off
// between files and back on at the next file's `vi.mock`. Chromium applies the
// re-enable to the already-loaded test iframe a few ms after `route()` resolves:
// an import issued in that window reaches Vite unintercepted and the test runs
// against the real module (`vi.mocked(...).mockReset is not a function`, ~5% of
// Linux CI runs). This route stays for the whole context, so interception never
// switches off; test-setup.browser.ts arms it and waits until its iframe's
// requests are intercepted before any test file loads.
// simplify: every browser-test request now pauses in Playwright's routing
// (~5% on the browser project); drop once the provider keeps interception live
// across files, or Playwright's `route()` resolves only when it is in effect.
const ROUTE_PROBE_PATH = "/__route_probe__";
const ROUTE_PROBE_MARKER = "intercepted";
const armedContexts = new WeakSet<object>();
const keepRequestInterception: BrowserCommand<[]> = async ({ context }) => {
  if (!armedContexts.has(context)) {
    armedContexts.add(context);
    await context
      .route(
        (url) => url.pathname === ROUTE_PROBE_PATH,
        (route) => route.fulfill({ body: ROUTE_PROBE_MARKER }),
      )
      .catch((error: unknown) => {
        // Let the next file retry and surface this error, not a probe timeout.
        armedContexts.delete(context);
        throw error;
      });
  }
  return { path: ROUTE_PROBE_PATH, marker: ROUTE_PROBE_MARKER };
};

export default defineConfig({
  plugins: [svelte()],
  server: {
    // Dev proxy: the routes ported to the Rust server go there (default :8001),
    // the rest of /api/* to the FastAPI backend (default :8000). Vite takes the
    // first key the path starts with, so ported routes come before "/api".
    proxy: {
      "/api/context": {
        target: rustServerUrl,
        changeOrigin: true,
      },
      "/api/search": {
        target: rustServerUrl,
        changeOrigin: true,
      },
      "/api/docs": {
        target: rustServerUrl,
        changeOrigin: true,
      },
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
        // Unit tests read two cross-package oracles as text (`?raw`), which Vite
        // refuses outside the allowed roots (default: this package): the server
        // fold's corpus (catalog.fold.test.ts) and reg_schema's version
        // (project_data.test.ts). Allow those directories, not the whole repo. A
        // single-file entry does not work here: Vite compares it against the id
        // with its `?raw` query attached.
        server: {
          fs: {
            allow: [".", "../../conformance/cases/folds", "../../reg_schema"],
          },
        },
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
          fileParallelism: !needsSingleProcessChromium,
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
            provider: playwright({
              launchOptions: { args: chromiumLaunchArgs },
            }),
            headless: true,
            // Vitest 5 flipped `locators.exact` to true. This suite locates by
            // the sentence a user reads, inside elements that also carry
            // decoration, and passes `exact: true` per call where it means it.
            locators: { exact: false },
            instances: [{ browser: "chromium" }],
            commands: { keepRequestInterception },
          },
        },
      },
    ],
  },
});
