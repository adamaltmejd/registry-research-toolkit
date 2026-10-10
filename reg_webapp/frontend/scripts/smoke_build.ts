// The production build's bootstrap, before merge (`scripts/gate.py frontend`, after
// `bun run build`): Vite's preview server serves dist/, and Chromium checks that
//   1. the app mounts (its "Sections" navigation renders),
//   2. the reg-core .wasm answers 200 with `application/wasm`,
//   3. with the .wasm request aborted, the bootstrap alert renders in #app.
// It prints the mount time (navigation start to the mounted app) for the record.
// The API is aborted throughout: the bootstrap needs no server.
//
//   bun scripts/smoke_build.ts
import { chromium, type Page } from "playwright";
import { preview } from "vite";
import { chromiumLaunchArgs } from "./chromium";

const BOOTSTRAP_ALERT = "The app could not load one of its parts";

function check(ok: boolean, what: string): void {
  if (!ok) throw new Error(`smoke: ${what}`);
  console.log(`smoke: ok — ${what}`);
}

async function open(page: Page, base: string): Promise<void> {
  await page.route("**/api/**", (route) => route.abort());
  await page.goto(base);
}

// In process, not a child process: closing it in `finally` leaves nothing running.
const server = await preview({
  logLevel: "silent",
  preview: { host: "127.0.0.1", port: 0, strictPort: true, open: false },
});
const base = server.resolvedUrls?.local[0];
if (!base) {
  await server.close();
  throw new Error("smoke: vite preview reported no local URL");
}
const browser = await chromium.launch({ args: chromiumLaunchArgs });
try {
  // One context for both pages: under `--single-process` (the seatbelt sandbox) a
  // second browser context crashes Chromium. Routes below are page-scoped, and
  // routed requests bypass the HTTP cache, so the first page's .wasm cannot
  // satisfy the second page's aborted load.
  const context = await browser.newContext();
  const page = await context.newPage();
  const wasm = page.waitForResponse((r) => r.url().endsWith(".wasm"));
  await open(page, base);
  const response = await wasm;
  check(
    response.status() === 200 &&
      response.headers()["content-type"] === "application/wasm",
    `the .wasm answers 200 application/wasm (${response.status()} ${response.headers()["content-type"]})`,
  );
  await page
    .getByRole("navigation", { name: "Sections" })
    .waitFor({ timeout: 10_000 });
  const mountedMs = await page.evaluate(() => performance.now());
  check(
    true,
    `the app mounts (${mountedMs.toFixed(0)} ms after navigation start)`,
  );

  const broken = await context.newPage();
  await broken.route("**/*.wasm", (route) => route.abort());
  await open(broken, base);
  await broken
    .locator("#app")
    .getByRole("alert")
    .filter({ hasText: BOOTSTRAP_ALERT })
    .waitFor({ timeout: 10_000 });
  check(
    (await broken.getByRole("navigation", { name: "Sections" }).count()) === 0,
    "with the .wasm aborted, the bootstrap alert renders and the app does not mount",
  );
} finally {
  await browser.close();
  await server.close();
}
