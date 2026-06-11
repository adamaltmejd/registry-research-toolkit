// Playwright driver for the reg_webapp SPA dev setup. Reuses the frontend's
// own playwright devDep (vitest-browser already requires it + its Chromium),
// so there is nothing extra to install when frontend deps are present.
//
// Run from reg_webapp/frontend/ (so the playwright import resolves):
//
//   bun ../.claude/skills/run-reg-webapp/driver.mjs [command ...]
//
// Commands (default: `smoke`):
//   smoke               root loads, catalog tree renders, drill into the first
//                       provider → register → variable, screenshot each step
//   shot <url-path>     open a path (e.g. /catalog/scb/lisa) and screenshot it
//   eval <url-path> <js> open a path, evaluate JS in the page, print the result
//
// Screenshots land in /tmp/reg-webapp-shots/. Servers must already be running
// (backend :8000 + vite :5173) — see SKILL.md.
import { mkdirSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

// Resolve playwright from the CWD (reg_webapp/frontend), not from this file's
// directory — bun/node resolve imports relative to the importing file, and the
// dep lives in the frontend's node_modules.
const require = createRequire(join(process.cwd(), "package.json"));
const { chromium } = require("playwright");

const BASE = process.env.REG_WEBAPP_DEV_URL ?? "http://localhost:5173";
const SHOTS = "/tmp/reg-webapp-shots";
mkdirSync(SHOTS, { recursive: true });

const [cmd = "smoke", ...rest] = process.argv.slice(2);
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
page.on("console", (m) => {
  if (m.type() === "error") console.log(`[console.error] ${m.text()}`);
});
page.on("pageerror", (e) => console.log(`[pageerror] ${e.message}`));

async function shot(name) {
  const file = `${SHOTS}/${name}.png`;
  await page.screenshot({ path: file });
  console.log(`shot: ${file}`);
}

// `networkidle` is NOT enough: Svelte swaps in fetched data after the network
// settles, so a screenshot right after navigation captures the "Loading…"
// placeholder. Wait for it to clear before asserting/screenshotting.
async function settled() {
  await page.waitForLoadState("networkidle");
  await page.waitForFunction(
    () => !document.body.innerText.includes("Loading"),
    { timeout: 10_000 },
  );
}

async function open(path) {
  const resp = await page.goto(BASE + path, { waitUntil: "networkidle" });
  console.log(`GET ${path} → ${resp.status()}`);
}

if (cmd === "shot") {
  await open(rest[0] ?? "/");
  await settled();
  await shot((rest[0] ?? "root").replaceAll("/", "_") || "root");
} else if (cmd === "eval") {
  await open(rest[0] ?? "/");
  console.log(JSON.stringify(await page.evaluate(rest[1] ?? "null"), null, 2));
} else if (cmd === "smoke") {
  await open("/catalog");
  await shot("01-root");
  // Drill three levels (provider → register → variable) by clicking the first
  // link STRICTLY DEEPER than the current path each round — `a[href^="/catalog"]`
  // alone matches the header nav link first and goes nowhere. The SPA's `link`
  // action intercepts these anchors (pushState, no full reload).
  for (const name of ["02-provider", "03-register", "04-variable"]) {
    const here = new URL(page.url()).pathname.replace(/\/$/, "");
    const link = page.locator(`a[href^="${here}/"]`).first();
    const href = await link.getAttribute("href");
    await link.click();
    await settled();
    console.log(`clicked → ${href} (now at ${new URL(page.url()).pathname})`);
    await shot(name);
  }
  // Real form interaction: the binding page's Period → Resolve flow. Assert
  // the narrowing actually happened — settled() alone would pass on a silent
  // no-op resolve.
  await page.locator("input").first().fill("2022");
  await page.getByRole("button", { name: "Resolve" }).click();
  await settled();
  await page.waitForFunction(
    () => document.body.innerText.includes("narrowed to 2022"),
    { timeout: 10_000 },
  );
  await shot("04b-period-resolved");
  // Deep-link reload: a cold load of the current nested path must render the
  // same view (vite's SPA fallback in dev; the edge worker in production).
  const deep = new URL(page.url()).pathname;
  await open(deep);
  await settled();
  const body = await page.locator("body").innerText();
  if (body.trim().length < 40) throw new Error(`deep link ${deep} rendered ~empty body`);
  await shot("05-deep-link-reload");
  console.log("smoke: OK");
} else {
  throw new Error(`unknown command: ${cmd}`);
}

await browser.close();
