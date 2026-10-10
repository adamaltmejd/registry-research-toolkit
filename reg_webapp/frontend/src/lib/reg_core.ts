/**
 * `reg-core` in the browser: the ONLY importer of the `reg-core-wasm` module
 * (`crates/reg-core-wasm`, built into `./reg-core-wasm/` by `bun run gen:wasm`).
 *
 * The module loads once, before the app mounts (`main.ts` awaits `initRegCore`;
 * the test setups initialise it too). No top-level await: the wrappers below are
 * synchronous and throw when called before `initRegCore` settles, so a missed
 * initialisation fails loudly instead of skipping a check. JSON text crosses the
 * boundary; every export is total, so no input can abort the module.
 */

import type { ValidationResultModel } from "./api";
import init, {
  check_project,
  initSync,
  project_schema_version,
} from "./reg-core-wasm/reg_core_wasm";

let ready = false;

/** Fetch and instantiate the module (the browser path). Idempotent. */
export async function initRegCore(): Promise<void> {
  if (!ready) {
    await init();
    ready = true;
  }
}

/** Instantiate the module from its bytes (the jsdom test setup, which has no
 * `fetch` of a file URL). Idempotent. */
export function initRegCoreSync(bytes: BufferSource): void {
  if (!ready) {
    initSync({ module: bytes });
    ready = true;
  }
}

function requireReady(): void {
  if (!ready) {
    throw new Error("reg-core is not initialised: await initRegCore() first");
  }
}

/** The `project_data.json` `schema_version` this build reads; a new draft takes it. */
export function projectSchemaVersion(): string {
  requireReady();
  return project_schema_version();
}

/** The server's structural door (`reg_core::project::check`) over `json`: the one
 * `unsupported_schema_version` issue, every structural issue, or `ok` with none.
 * Any JSON value is checked; text serde_json cannot read (not JSON, or nested
 * past its 128-level limit) is one `invalid_json` issue. */
export function checkProject(json: string): ValidationResultModel {
  requireReady();
  return JSON.parse(check_project(json)) as ValidationResultModel;
}
