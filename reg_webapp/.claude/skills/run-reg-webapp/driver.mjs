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

// Screenshot viewport. Default `desktop` (the historical 1280x900). Override via
// REG_WEBAPP_VIEWPORT: a named preset (mobile/tablet/desktop) or raw "WxH" (e.g.
// "414x896"). dev.sh's `shot --mobile/--tablet/--all/--viewport` sets this per run
// so the free-port path can capture responsive breakpoints without the fixed-port
// preview server. The label is suffixed onto non-desktop screenshot names so a
// multi-viewport run doesn't clobber the desktop shot.
const VIEWPORTS = {
  mobile: { width: 375, height: 812 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 900 },
};
function resolveViewport(spec) {
  if (!spec) return { ...VIEWPORTS.desktop, label: "desktop" };
  if (spec in VIEWPORTS) return { ...VIEWPORTS[spec], label: spec };
  const m = /^(\d+)x(\d+)$/.exec(spec);
  if (!m) {
    throw new Error(
      `bad REG_WEBAPP_VIEWPORT "${spec}" — use mobile|tablet|desktop or WxH`,
    );
  }
  return { width: Number(m[1]), height: Number(m[2]), label: spec };
}
const viewport = resolveViewport(process.env.REG_WEBAPP_VIEWPORT);

const [cmd = "smoke", ...rest] = process.argv.slice(2);
const browser = await chromium.launch();
const page = await browser.newPage({
  viewport: { width: viewport.width, height: viewport.height },
});
page.on("console", (m) => {
  if (m.type() === "error") console.log(`[console.error] ${m.text()}`);
});
page.on("pageerror", (e) => console.log(`[pageerror] ${e.message}`));

async function shot(name) {
  // Desktop keeps the historical bare filename (SKILL.md references `01-root` …
  // `05-deep-link-reload`); other viewports get a `-<label>` suffix so a
  // multi-viewport run captures each breakpoint instead of overwriting.
  const suffix = viewport.label === "desktop" ? "" : `-${viewport.label}`;
  const file = `${SHOTS}/${name}${suffix}.png`;
  await page.screenshot({ path: file });
  console.log(`shot: ${file}`);
}

// `networkidle` is NOT enough: Svelte swaps in fetched data after the network
// settles, so a screenshot right after navigation captures the loading
// placeholder. Every loading placeholder carries aria-busy="true" (the
// components' contract with this driver — don't key on UI copy, which changes
// and can collide with catalog content). Wait for the last one to clear.
async function settled() {
  await page.waitForLoadState("networkidle");
  await page.waitForFunction(
    () => !document.querySelector('[aria-busy="true"]'),
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
  await page.getByRole("button", { name: "Apply" }).click();
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
