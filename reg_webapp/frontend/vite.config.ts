import optimizeLocales from "@react-aria/optimize-locales-plugin";
import react from "@vitejs/plugin-react";
// defineConfig from vitest/config (not vite): it natively types the `test` block.
// Vitest 4 dropped the module augmentation that let a `/// <reference types="vitest/config" />`
// add `test` to vite's own UserConfig, so import the vitest-aware defineConfig directly.
import { defineConfig } from "vitest/config";

// vite.config.ts runs under Node; read the env via globalThis so we don't pull a
// @types/node dep for one lookup. REG_META_SERVER_URL repoints the dev /api proxy at
// the Rust server (`reg-meta serve`) so concurrent instances (parallel worktrees /
// PR lanes) can each target their own port — see
// .agents/skills/run-reg-webapp "Parallel instances".
const runtimeProcess = (
  globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }
).process;
const rustServerUrl =
  runtimeProcess?.env?.REG_META_SERVER_URL ?? "http://127.0.0.1:8001";

export default defineConfig({
  plugins: [
    react(),
    // React Aria ships its strings for ~35 locales; keep the UI language and the
    // data's. `enforce: "pre"` runs it before Vite's own transforms (a CJS plugin,
    // so the Vite flavour is `.vite`).
    {
      ...optimizeLocales.vite({ locales: ["en-US", "sv-SE"] }),
      enforce: "pre",
    },
  ],
  server: {
    // Dev proxy: the Rust server answers every /api route.
    proxy: {
      "/api": {
        target: rustServerUrl,
        changeOrigin: true,
      },
    },
    // Unit tests read cross-package oracles, which Vite refuses outside the
    // allowed roots (default: this package): the server fold's corpus
    // (reg_core.test.ts), reg-core's structural corpus (reg_core.test.ts,
    // validation.test.ts), the server's unsupported-version case, the period
    // grammar corpora and the interval algebra's golden cases
    // (reg_core.test.ts). Allow those directories, not the whole repo. A
    // single-file entry does not work here: Vite compares it against the id
    // with its `?raw` query attached.
    fs: {
      allow: [
        ".",
        "../../conformance/cases/folds",
        "../../conformance/cases/api/validate-unsupported-schema-version",
        "../../conformance/cases/grammar",
        "../../crates/reg-core/tests/project/corpus",
        "../../crates/reg-core/tests/interval",
      ],
    },
  },
  // Pure-logic tests (`*.test.ts`) in jsdom. Component behavior is exercised by
  // the `flows` driver (reg_webapp/dev/driver.mjs), not here.
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
    setupFiles: ["./src/test-setup.unit.ts"],
  },
});
