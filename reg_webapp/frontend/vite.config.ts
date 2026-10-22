import { svelte } from "@sveltejs/vite-plugin-svelte";
import { playwright } from "vite-plus/test/browser-playwright";
// Vite+'s defineConfig types the `test`, `lint` and `fmt` blocks below.
import { configDefaults, defineConfig } from "vite-plus";
import type { BrowserCommand } from "vite-plus/test/node";
import {
  chromiumLaunchArgs,
  needsSingleProcessChromium,
} from "./scripts/chromium";

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

// Generated or vendored output the formatter and linter never touch.
const generated = [
  "dist/**",
  "src/lib/reg-core-wasm/**",
  "src/lib/api-types-rust.ts",
];

export default defineConfig({
  plugins: [svelte()],
  // `vp fmt` (Oxfmt). `svelte: true` formats the whole `.svelte` file (template,
  // script and style), which Biome never did. Markdown belongs to panache
  // (scripts/gate.py lint) and the lockfile to bun, so neither is formatted here.
  fmt: {
    printWidth: 80,
    svelte: true,
    sortImports: { newlinesBetween: false },
    sortPackageJson: false,
    ignorePatterns: [...generated, "**/*.md", "bun.lock"],
  },
  // `vp lint` (Oxlint). Oxlint lints `<script>` in `.svelte`, not the template;
  // svelte-check (`check`) owns templates, types and a11y. On top of Oxlint's
  // default `correctness` category these are the Oxlint ports of the rules
  // Biome's `recommended` preset enforced. CSS lint stays on Biome (biome.json):
  // Oxlint has no CSS rules.
  lint: {
    ignorePatterns: generated,
    // Oxlint reports its default category as warnings, which do not fail a run.
    categories: { correctness: "error" },
    rules: {
      "array-callback-return": "error",
      "default-case-last": "error",
      // Biome's noDoubleEquals allowed `== null`.
      eqeqeq: ["error", "always", { null: "ignore" }],
      "no-array-constructor": "error",
      "no-case-declarations": "error",
      "no-constructor-return": "error",
      "no-extra-label": "error",
      "no-fallthrough": "error",
      "no-inner-declarations": "error",
      "no-label-var": "error",
      "no-labels": "error",
      "no-lone-blocks": "error",
      "no-proto": "error",
      "no-prototype-builtins": "error",
      "no-redeclare": "error",
      "no-regex-spaces": "error",
      "no-script-url": "error",
      "no-self-compare": "error",
      "no-sequences": "error",
      "no-template-curly-in-string": "error",
      "no-unneeded-ternary": "error",
      // Biome's noInvalidUseBeforeDeclaration: only a same-scope use that would
      // throw, not a hoisted function or a reference from a later-called closure.
      "no-use-before-define": [
        "error",
        { functions: false, classes: false, variables: false },
      ],
      "no-useless-computed-key": "error",
      "no-useless-constructor": "error",
      "prefer-arrow-callback": "error",
      "prefer-const": "error",
      "prefer-exponentiation-operator": "error",
      "prefer-numeric-literals": "error",
      "prefer-regex-literals": "error",
      "prefer-rest-params": "error",
      "prefer-template": "error",
      radix: "error",
      "oxc/approx-constant": "error",
      "oxc/misrefactored-assign-op": "error",
      "oxc/no-accumulating-spread": "error",
      "oxc/no-const-enum": "error",
      "typescript/adjacent-overload-signatures": "error",
      "typescript/ban-ts-comment": "error",
      "typescript/ban-types": "error",
      "typescript/consistent-type-imports": [
        "error",
        { disallowTypeAnnotations: false },
      ],
      "typescript/no-empty-interface": "error",
      "typescript/no-explicit-any": "error",
      "typescript/no-extraneous-class": "error",
      "typescript/no-non-null-assertion": "error",
      "typescript/no-unnecessary-type-constraint": "error",
      "typescript/prefer-function-type": "error",
      "typescript/prefer-literal-enum-member": "error",
      "unicorn/new-for-builtins": "error",
      "unicorn/no-document-cookie": "error",
      "unicorn/no-instanceof-array": "error",
      "unicorn/no-useless-switch-case": "error",
      "unicorn/prefer-array-flat-map": "error",
      "unicorn/prefer-array-index-of": "error",
      "unicorn/prefer-date-now": "error",
      "unicorn/prefer-node-protocol": "error",
      "unicorn/prefer-number-properties": "error",
    },
    overrides: [
      {
        // A bare read inside `$effect` is how a rune subscribes to a value.
        files: ["**/*.svelte", "**/*.svelte.ts"],
        rules: { "no-unused-expressions": "off" },
      },
    ],
  },
  server: {
    // Dev proxy: the Rust server answers every /api route.
    proxy: {
      "/api": {
        target: rustServerUrl,
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
        // Unit tests read cross-package oracles, which Vite refuses outside the
        // allowed roots (default: this package): the server fold's corpus
        // (reg_core.test.ts), reg-core's structural corpus (reg_core.test.ts,
        // validation.test.ts), the server's unsupported-version case, the period
        // grammar corpora and the interval algebra's golden cases
        // (reg_core.test.ts). Allow those directories, not the whole repo. A
        // single-file entry does not work here: Vite compares it against the id
        // with its `?raw` query attached.
        server: {
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
        test: {
          name: "unit",
          environment: "jsdom",
          include: ["src/**/*.test.ts"],
          setupFiles: ["./src/test-setup.unit.ts"],
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
