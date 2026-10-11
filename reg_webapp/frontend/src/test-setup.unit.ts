// Vitest `unit` project setup: instantiate reg-core (WASM) from its bytes before
// any test file runs, as main.ts does before mount. jsdom cannot fetch a file URL,
// so the bytes come from disk. Read through `getBuiltinModule` (via globalThis, as
// vite.config.ts reads the env) so the suite needs no @types/node.
import { initRegCoreSync } from "./lib/reg_core";

interface NodeFs {
  readFileSync(path: string): Uint8Array<ArrayBuffer>;
}
const { process } = globalThis as unknown as {
  process: { getBuiltinModule(id: "node:fs"): NodeFs };
};

// `import.meta.url` is the jsdom page's URL here, not the file's; Vitest sets
// `dirname` to this file's directory.
initRegCoreSync(
  process
    .getBuiltinModule("node:fs")
    .readFileSync(
      `${(import.meta as ImportMeta & { dirname: string }).dirname}/lib/reg-core-wasm/reg_core_wasm_bg.wasm`,
    ),
);
