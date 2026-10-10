// The production build's bootstrap, before merge (`scripts/gate.py frontend`, after
// `bun run build`): `vite preview` serves dist/, and Chromium checks that
//   1. the app mounts (its "Sections" navigation renders),
//   2. the reg-core .wasm answers 200 with `application/wasm`,
//   3. with the .wasm request aborted, the bootstrap alert renders in #app.
// It prints the mount time (navigation start to the mounted app) for the record.
// The API is aborted throughout: the bootstrap needs no server.
//
//   bun scripts/smoke_build.ts
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { chromium, type Page } from "playwright";

const BOOTSTRAP_ALERT = "The app could not load one of its parts";

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() =>
        typeof address === "object" && address
          ? resolve(address.port)
          : reject(new Error("no port")),
      );
    });
  });
}

async function waitForServer(url: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // not listening yet
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`vite preview did not answer at ${url}`);
}

function check(ok: boolean, what: string): void {
  if (!ok) throw new Error(`smoke: ${what}`);
  console.log(`smoke: ok — ${what}`);
}

async function open(page: Page, base: string): Promise<void> {
  await page.route("**/api/**", (route) => route.abort());
  await page.goto(base);
}

const port = await freePort();
const base = `http://127.0.0.1:${port}/`;
const preview = spawn(
  "bunx",
  [
    "vite",
    "preview",
    "--host",
    "127.0.0.1",
    "--port",
    `${port}`,
    "--strictPort",
  ],
  { stdio: "ignore" },
);
const browser = await chromium.launch();
try {
  await waitForServer(base);

  const page = await browser.newPage();
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

  const broken = await browser.newPage();
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
  preview.kill();
}
