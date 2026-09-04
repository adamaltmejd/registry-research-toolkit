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
//   flows <out-dir>     the /project error+retry gate: three scenarios × four
//                       viewports, each in a fresh context, PNGs → <out-dir>
//
// smoke/shot/eval screenshots land in /tmp/reg-webapp-shots/; `flows` writes
// only into its explicit <out-dir> (the gate passes $YARD_ARTIFACT_DIR). Servers
// must already be running (backend :8000 + vite :5173) — see SKILL.md.
import { mkdirSync, readFileSync } from "node:fs";
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

// smoke/shot/eval keep the fixed /tmp shots dir; `flows` writes ONLY into the
// directory it is handed (the gate hands it $YARD_ARTIFACT_DIR, whose exact
// filenames are a declared contract), so an absent argument is an error rather
// than a silent fallback.
const outDir = cmd === "flows" ? rest[0] : null;
if (cmd === "flows" && !outDir) {
  throw new Error("flows: needs an output directory, e.g. flows $YARD_ARTIFACT_DIR");
}

// Two chromium.launch failure classes are worth a retry before we give up (issue #1049):
//   1. A TRANSIENT MachPortRendezvous name collision — a crashed prior attempt plus PID
//      reuse leaves a stale rendezvous name, so the very next multi-process launch fails
//      but an immediate retry at FULL fidelity heals it. Retry once in normal mode first.
//   2. A sandboxed agent shell (codex `-s workspace-write` seatbelt, Claude Code's
//      sandboxed Bash — same seatbelt profile design) denies multi-process Chromium's
//      Mach `bootstrap_check_in … (1100)`: those seatbelts have no mach-register grant, so
//      the renderer/GPU child process rendezvous can't complete. `--single-process` skips
//      the Mach rendezvous entirely (one process, no children to register) and is
//      empirically proven to launch under the real codex seatbelt. This is a hard,
//      deterministic block, not a transient one, so it needs the arg change, not a plain
//      retry. Caveat: `--single-process` is unsupported/best-effort per Chromium — revisit
//      on Playwright bumps in case the flag's behavior changes.
const SINGLE_PROCESS_ARGS = [
  "--single-process",
  "--no-sandbox",
  "--disable-gpu",
  "--disable-crash-reporter",
];
async function launchBrowser() {
  try {
    return await chromium.launch();
  } catch {
    // Stage 1: retry once in normal (multi-process) mode to heal a transient rendezvous
    // collision at full fidelity.
    console.error(
      "driver: normal relaunch after failed launch (transient Mach-port collision?)",
    );
    try {
      return await chromium.launch();
    } catch {
      // Stage 2: retry once in single-process mode for the sandboxed-shell seatbelt denial.
      console.error(
        "driver: single-process fallback engaged (sandboxed shell, issue #1049)",
      );
      return await chromium.launch({ args: SINGLE_PROCESS_ARGS });
    }
  }
}
const browser = await launchBrowser();

// Print the page's console errors and uncaught exceptions. The returned array
// is the ledger of exception messages, which `flows` asserts is empty per case;
// smoke/shot/eval only want the printing and ignore it.
function logPageOutput(page) {
  const pageErrors = [];
  page.on("console", (m) => {
    if (m.type() === "error") console.log(`[console.error] ${m.text()}`);
  });
  page.on("pageerror", (e) => {
    console.log(`[pageerror] ${e.message}`);
    pageErrors.push(e.message);
  });
  return pageErrors;
}

// The single page smoke/shot/eval drive. `flows` opens a FRESH context per case
// instead (isolated storage + its own page-error ledger), so it never uses this
// one — and doesn't pay for it either.
const page =
  cmd === "flows"
    ? null
    : await browser.newPage({
        viewport: { width: viewport.width, height: viewport.height },
      });
if (page) logPageOutput(page);

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
async function settled(page) {
  await page.waitForLoadState("networkidle");
  await page.waitForFunction(
    () => !document.querySelector('[aria-busy="true"]'),
    { timeout: 10_000 },
  );
}

async function open(page, path) {
  const resp = await page.goto(BASE + path, { waitUntil: "networkidle" });
  console.log(`GET ${path} → ${resp.status()}`);
}

// ── `flows`: the /project error + retry gate ────────────────────────────────
//
// Three scenarios × four viewports = 12 cases, each in a FRESH context against
// the REAL backend (the caller points it at a synthetic catalog DB through
// REG_META_DB — see catalog_fixture_db.py). Only the failing request of each
// case is injected: everything else is the actual app answering. The 16 PNGs
// these write are the artifact contract declared in .yard/config.toml, so their
// names are spelled here once and nowhere else.
const FLOW_VIEWPORTS = [
  { width: 375, height: 812 },
  { width: 768, height: 1024 },
  { width: 1280, height: 900 },
  { width: 1920, height: 1080 },
];
const VALIDATE_PATH = "/api/project/validate";
const ORDER_PATH = "/api/project/order";

function check(condition, message) {
  if (!condition) throw new Error(message);
}

/** Is this a POST of `path`? (Requests, and a response's own request.) */
function posts(request, path) {
  return request.method() === "POST" && new URL(request.url()).pathname === path;
}

/** Wait for the app's NEXT successful validation response — armed BEFORE the
 * action that triggers it, so the automatic (debounced) POST cannot be missed. */
function validated(page) {
  return page.waitForResponse(
    (r) => posts(r.request(), VALIDATE_PATH) && r.status() === 200,
  );
}

/** Fail every request to `path` in transport (counting each injection), and
 * hand back the restore that puts the real route back. page.route globs match
 * the whole URL, hence the `**` prefix on the pathname `posts()` compares
 * against; unroute removes a route by exactly the pattern+handler pair that was
 * installed, so the closure holds both. */
async function failInTransport(page, path, counts) {
  const pattern = `**${path}`;
  const handler = async (route) => {
    counts.injected += 1;
    await route.abort("failed");
  };
  await page.route(pattern, handler);
  return () => page.unroute(pattern, handler);
}

/** Load `project` through the editor's own file input (the researcher's open
 * path), rather than reaching into the store. */
function openProjectFile(page, project) {
  return page.locator('input[type="file"]').setInputFiles({
    name: "project_data.json",
    mimeType: "application/json",
    buffer: Buffer.from(`${JSON.stringify(project, null, 2)}\n`, "utf8"),
  });
}

/** Tab until `locator` holds focus — a REAL keyboard focus, so the shot shows
 * the :focus-visible ring (a programmatic .focus() does not paint one). */
async function tabTo(page, locator, what) {
  for (let i = 0; i < 80; i += 1) {
    if (await locator.evaluate((el) => el === document.activeElement)) return;
    await page.keyboard.press("Tab");
  }
  throw new Error(`flows: ${what} never took keyboard focus`);
}

/** One artifact: assert the layout fits the viewport, then write the PNG into
 * `outDir` (module scope, as `shot()` reads `SHOTS`). */
async function capture(page, name) {
  const width = await page.evaluate(() => ({
    scroll: document.documentElement.scrollWidth,
    client: document.documentElement.clientWidth,
  }));
  check(
    width.scroll <= width.client,
    `${name}: horizontal overflow (scrollWidth ${width.scroll} > clientWidth ${width.client})`,
  );
  const file = join(outDir, `${name}.png`);
  await page.screenshot({ path: file, fullPage: true });
  console.log(`flows: shot ${file}`);
}

// The two summaries ValidationPanel.svelte renders for a clean result: "Valid —
// no errors." and "Valid with warnings — no errors." (a non-blocking notes count
// may follow). Anchored on the whole phrase, so neither the failing branch
// ("Not valid — N errors.") nor the in-flight one ("Checking the current
// project…") can slip through the way a bare "Valid" prefix test allows.
const VALID_SUMMARY = /^Valid( with warnings)?\s+—\s+no errors\./;

/** Assert the panel is showing a clean verdict, quoting what it showed instead. */
async function checkValid(ui, why) {
  const text = (await ui.summary.innerText()).trim();
  check(VALID_SUMMARY.test(text), `${why} — summary read ${JSON.stringify(text)}`);
}

/** The panel/toolbar handles every scenario drives. */
function projectUi(page) {
  const panel = page.locator('section[aria-label="Validation results"]');
  return {
    panel,
    banner: panel.getByRole("alert"),
    summary: panel.getByRole("status"),
    // The CTA's accessible name IS "Downloading…" while the order POST is in
    // flight, so this locator resolving is itself the cleared-busy wait.
    download: page.getByRole("button", { name: "Download order.json" }),
    retryDownload: panel.getByRole("button", { name: "Retry download" }),
    retryValidation: panel.getByRole("button", { name: "Retry validation" }),
  };
}

/** Scenario 1 — a structurally fine but EMPTY project: validation passes, the
 * order is blocked by the backend, and no retry can change that verdict. */
async function blockedOrderCase(page, counts, shoot) {
  const ui = projectUi(page);
  const green = validated(page);
  await page.getByRole("button", { name: "New project" }).click();
  await green;
  await settled(page);
  await checkValid(
    ui,
    "the empty draft must validate clean before the order is attempted",
  );

  const blocked = page.waitForResponse((r) => posts(r.request(), ORDER_PATH));
  await ui.download.click();
  const response = await blocked;
  check(response.status() === 422, `expected 422 from /order, got ${response.status()}`);
  const body = await response.json();
  check(
    body.findings.some((f) => f.code === "project_empty"),
    `expected a project_empty finding, got ${JSON.stringify(body.findings)}`,
  );

  const findings = ui.panel.getByRole("group", { name: /^Blocking findings/ });
  await findings.waitFor();
  check(
    (await ui.banner.innerText()).includes("Order blocked"),
    "a blocked order must be stated as a verdict, not a request failure",
  );
  check(
    (await findings.innerText()).includes("project_empty"),
    "the blocking finding must be rendered, not just flattened into the banner",
  );
  await ui.download.waitFor();
  check(await ui.download.isDisabled(), "the download must stay closed on a block");
  check(
    (await ui.retryValidation.count()) === 0 && (await ui.retryDownload.count()) === 0,
    "a blocked order is a verdict on the draft — neither retry may be offered",
  );
  check(counts.order === 1, `expected one order POST, saw ${counts.order}`);
  await settled(page);
  await shoot("project-blocked");
}

/** Scenario 2 — one order POST fails in transport; the retry re-POSTs it, the
 * real manifest downloads, and the error clears. */
async function orderRetryCase(page, counts, shoot, project, expected) {
  const ui = projectUi(page);
  const green = validated(page);
  await openProjectFile(page, project);
  await green;
  await settled(page);
  await checkValid(
    ui,
    "the synthetic project must validate clean before the order is attempted",
  );

  const restoreOrder = await failInTransport(page, ORDER_PATH, counts);
  await ui.download.click();
  await ui.banner.waitFor();
  check(counts.injected === 1, `injected ${counts.injected} order failures, wanted 1`);
  check(
    (await ui.banner.innerText()).includes("Request failed"),
    "an unanswered order request must read as a request failure",
  );
  check(
    (await ui.retryDownload.count()) === 1 && (await ui.retryValidation.count()) === 0,
    "the failed ORDER request must offer Retry download and nothing else",
  );
  // Settle BEFORE walking the tab order: the shot is the evidence of the failed
  // order, so the panel must have finished re-rendering (and every aria-busy
  // marker cleared) before the focus ring lands and the PNG is taken.
  await settled(page);
  await tabTo(page, ui.retryDownload, "Retry download");
  await shoot("order-error");

  // Restore the real route, then retry WITHOUT touching the draft.
  await restoreOrder();
  const validateBefore = counts.validate;
  const [download, response] = await Promise.all([
    page.waitForEvent("download"),
    page.waitForResponse((r) => posts(r.request(), ORDER_PATH)),
    ui.retryDownload.click(),
  ]);
  check(response.status() === 200, `retried order answered ${response.status()}`);
  check(
    download.suggestedFilename() === "order.json",
    `downloaded ${download.suggestedFilename()}, wanted order.json`,
  );
  const manifest = JSON.parse(readFileSync(await download.path(), "utf8"));
  check(
    manifest.provenance.mode === expected.mode &&
      manifest.provenance.steward === expected.steward &&
      manifest.provenance.catalog_import_date === expected.importDate,
    `manifest provenance ${JSON.stringify(manifest.provenance)}`,
  );
  check(manifest.entries.length === 1, `manifest has ${manifest.entries.length} entries`);
  const [entry] = manifest.entries;
  check(
    entry.source === "lisa-2018" &&
      entry.logical.variable === "scb/lisa/kon" &&
      entry.requested_period === "2018" &&
      entry.physical.table === "" &&
      entry.physical.column === "Kon" &&
      entry.physical.edition === "2018",
    `unexpected synthetic manifest entry: ${JSON.stringify(entry)}`,
  );

  await ui.banner.waitFor({ state: "hidden" });
  await ui.download.waitFor();
  check(!(await ui.download.isDisabled()), "recovery must reopen the download");
  check(counts.order === 2, `expected a second order POST, saw ${counts.order}`);
  check(counts.orderOk === 1, `expected one 200 order response, saw ${counts.orderOk}`);
  check(
    counts.validate === validateBefore,
    `the order retry re-POSTed /validate (${validateBefore} → ${counts.validate})`,
  );
  await settled(page);
  await shoot("order-recovered");
}

/** Scenario 3 — the AUTOMATIC validation request fails (a transport error, not
 * a 200 carrying findings); the retry re-runs it and clears the banner. */
async function validationRetryCase(page, counts, shoot, project) {
  const ui = projectUi(page);
  // Installed BEFORE the action that triggers the automatic validation.
  const restoreValidate = await failInTransport(page, VALIDATE_PATH, counts);
  await openProjectFile(page, project);
  await ui.banner.waitFor();
  check(counts.injected === 1, `injected ${counts.injected} validation failures, wanted 1`);
  check(
    (await ui.banner.innerText()).includes("Request failed"),
    "an unanswered validation request must read as a request failure",
  );
  check(
    (await ui.retryValidation.count()) === 1 && (await ui.retryDownload.count()) === 0,
    "the failed VALIDATION request must offer Retry validation and nothing else",
  );
  await settled(page);
  await shoot("validation-error");

  await restoreValidate();
  await Promise.all([validated(page), ui.retryValidation.click()]);
  await ui.banner.waitFor({ state: "hidden" });
  await settled(page);
  await checkValid(
    ui,
    "a successful re-validation must replace the banner with the verdict",
  );
  check(
    counts.validate === 2,
    `expected the aborted validation plus exactly one retry, saw ${counts.validate}`,
  );
  check(counts.order === 0, `the validation retry POSTed /order ${counts.order} time(s)`);
}

/** Run one case in its own context: fresh storage, its own request ledger, its
 * own page-error ledger — and close the context whatever happens. */
async function runCase(viewport, name, body) {
  const label = `${viewport.width}x${viewport.height}`;
  const context = await browser.newContext({ viewport });
  const page = await context.newPage();
  const pageErrors = logPageOutput(page);
  const counts = { validate: 0, order: 0, orderOk: 0, injected: 0 };
  page.on("request", (r) => {
    if (posts(r, VALIDATE_PATH)) counts.validate += 1;
    else if (posts(r, ORDER_PATH)) counts.order += 1;
  });
  page.on("response", (r) => {
    if (posts(r.request(), ORDER_PATH) && r.status() === 200) counts.orderOk += 1;
  });
  try {
    await open(page, "/project");
    await settled(page);
    await body(page, counts, (shot) => capture(page, `${shot}-${label}`));
    check(
      pageErrors.length === 0,
      `${name} ${label}: page errors: ${pageErrors.join(" | ")}`,
    );
    console.log(
      `flows: OK ${name} ${label} route=/project ` +
        `validate=${counts.validate} order=${counts.order} ` +
        `order200=${counts.orderOk} injected-failures=${counts.injected}`,
    );
  } catch (e) {
    console.log(`flows: FAIL ${name} ${label} route=/project — ${e.message}`);
    throw e;
  } finally {
    await context.close();
  }
}

// One try/finally around the whole dispatch: every command closes the browser
// on the way out, including the failure path a thrown assertion takes.
try {
  if (cmd === "flows") {
    mkdirSync(outDir, { recursive: true });
    // The project the two recovery scenarios open, and the manifest it must
    // produce, are DERIVED from the deployment the flows run against — never
    // pinned to a stale capture. Shapes: reg_webapp/backend/tests/
    // test_project_order.py (a valid global-fallback project + its entry).
    // Playwright's APIRequestContext, not bun's global fetch. This is still an
    // out-of-page HTTP client (it does not run in the renderer), but it resolves
    // `localhost` the way the browser does; bun picks ::1 with no fallback while
    // the dev server binds 127.0.0.1, so global fetch dies on ConnectionRefused.
    const probe = await browser.newContext();
    const contextResp = await probe.request.get(`${BASE}/api/context`);
    const contextBody = await contextResp.text();
    await probe.close();
    check(
      contextResp.ok(),
      `GET /api/context answered ${contextResp.status()}: ${contextBody.slice(0, 300)}`,
    );
    const deployment = JSON.parse(contextBody);
    const project = {
      schema_version: "2.0.0",
      steward: deployment.steward.id,
      reg_meta_version: `reg_meta/v${deployment.webapp.reg_meta_version}`,
      name: "Synthetic order flow",
      sources: [
        {
          name: "lisa-2018",
          register_variant: "scb/lisa/individer-15plus",
          period: 2018,
          bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
        },
      ],
    };
    const expected = {
      mode: "global_fallback",
      steward: deployment.steward.id,
      importDate: deployment.reg_meta.import_date,
    };
    console.log(
      `flows: ${BASE} steward=${deployment.steward.id} ` +
        `reg_meta=${deployment.webapp.reg_meta_version} ` +
        `catalog=${deployment.reg_meta.schema_version}@${deployment.reg_meta.import_date}`,
    );
    for (const viewport of FLOW_VIEWPORTS) {
      // blockedOrderCase already has runCase's `body` signature; the other two
      // bind the shared project (and its expected manifest) into theirs.
      await runCase(viewport, "blocked-order", blockedOrderCase);
      await runCase(viewport, "order-retry", (p, c, shoot) =>
        orderRetryCase(p, c, shoot, project, expected),
      );
      await runCase(viewport, "validation-retry", (p, c, shoot) =>
        validationRetryCase(p, c, shoot, project),
      );
    }
    console.log("flows: OK — 12 cases, 16 shots");
  } else if (cmd === "shot") {
    await open(page, rest[0] ?? "/");
    await settled(page);
    await shot((rest[0] ?? "root").replaceAll("/", "_") || "root");
  } else if (cmd === "eval") {
    await open(page, rest[0] ?? "/");
    console.log(JSON.stringify(await page.evaluate(rest[1] ?? "null"), null, 2));
  } else if (cmd === "smoke") {
    await open(page, "/catalog");
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
      await settled(page);
      console.log(`clicked → ${href} (now at ${new URL(page.url()).pathname})`);
      await shot(name);
    }
    // Real form interaction: the binding page's Period → Resolve flow. Assert
    // the narrowing actually happened — settled() alone would pass on a silent
    // no-op resolve.
    await page.locator("input").first().fill("2022");
    await page.getByRole("button", { name: "Apply" }).click();
    await settled(page);
    await page.waitForFunction(
      () => document.body.innerText.includes("narrowed to 2022"),
      { timeout: 10_000 },
    );
    await shot("04b-period-resolved");
    // Deep-link reload: a cold load of the current nested path must render the
    // same view (vite's SPA fallback in dev; the edge worker in production).
    const deep = new URL(page.url()).pathname;
    await open(page, deep);
    await settled(page);
    const body = await page.locator("body").innerText();
    if (body.trim().length < 40) throw new Error(`deep link ${deep} rendered ~empty body`);
    await shot("05-deep-link-reload");
    console.log("smoke: OK");
  } else {
    throw new Error(`unknown command: ${cmd}`);
  }
} finally {
  await browser.close();
}
