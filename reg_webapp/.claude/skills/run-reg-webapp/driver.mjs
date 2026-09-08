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
//                       provider → register → variable, narrow the period
//                       slider, screenshot each step
//   shot <url-path>     open a path (e.g. /catalog/scb/lisa) and screenshot it
//   eval <url-path> <js> open a path, evaluate JS in the page, print the result
//   flows <out-dir> [scenario...]
//                       the project gates: four scenarios (three /project
//                       error+retry, one catalog-authored draft) × four
//                       viewports, each in a fresh context, PNGs → <out-dir>.
//                       Named scenarios run just those; no names runs all four.
//
// smoke/shot screenshots land in $REG_WEBAPP_SHOTS — dev.sh sets it to the one
// directory that invocation owns, and a direct run gets a fresh one under /tmp;
// `flows` writes only into its explicit <out-dir> (the gate passes
// $YARD_ARTIFACT_DIR). The servers must already be running on whichever free
// ports dev.sh picked, with REG_WEBAPP_DEV_URL pointing here — see SKILL.md.
import { existsSync, mkdirSync, mkdtempSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Where playwright looks for its browsers. An explicit PLAYWRIGHT_BROWSERS_PATH
// always wins; inside the lane image Chromium is baked at /opt/pw-browsers,
// outside any HOME the run may have been given, so default to it when it exists.
// Set BEFORE requiring playwright — the registry reads this at import time.
if (!process.env.PLAYWRIGHT_BROWSERS_PATH && existsSync("/opt/pw-browsers")) {
  process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/pw-browsers";
}

// Resolve playwright from the CWD (reg_webapp/frontend), not from this file's
// directory — bun/node resolve imports relative to the importing file, and the
// dep lives in the frontend's node_modules.
const require = createRequire(join(process.cwd(), "package.json"));
const { chromium } = require("playwright");

const BASE = process.env.REG_WEBAPP_DEV_URL ?? "http://localhost:5173";

// Screenshot viewport. Default `desktop` (the historical 1280x900). Override via
// REG_WEBAPP_VIEWPORT: a named preset (mobile/tablet/desktop/wide) or raw "WxH"
// (e.g. "414x896"). dev.sh's `shot --mobile/--wide/--all/--viewport` sets this per
// run so the free-port path can capture responsive breakpoints without the
// fixed-port preview server. The label is suffixed onto non-desktop screenshot
// names so a multi-viewport run doesn't clobber the desktop shot.
const VIEWPORTS = {
  mobile: { width: 375, height: 812 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 900 },
  wide: { width: 1920, height: 1080 },
};
function resolveViewport(spec) {
  if (!spec) return { ...VIEWPORTS.desktop, label: "desktop" };
  if (spec in VIEWPORTS) return { ...VIEWPORTS[spec], label: spec };
  const m = /^(\d+)x(\d+)$/.exec(spec);
  if (!m) {
    throw new Error(
      `bad REG_WEBAPP_VIEWPORT "${spec}" — use mobile|tablet|desktop|wide or WxH`,
    );
  }
  return { width: Number(m[1]), height: Number(m[2]), label: spec };
}
const viewport = resolveViewport(process.env.REG_WEBAPP_VIEWPORT);

const [cmd = "smoke", ...rest] = process.argv.slice(2);

// Where the images go. `flows` writes ONLY into the directory it is handed (the
// gate hands it $YARD_ARTIFACT_DIR, whose exact filenames are a declared
// contract), so an absent argument is an error rather than a silent fallback.
const outDir = cmd === "flows" ? rest[0] : null;
if (cmd === "flows" && !outDir) {
  throw new Error("flows: needs an output directory, e.g. flows $YARD_ARTIFACT_DIR");
}
// Scenario names after the out-dir, empty for "all of them" (see the dispatch).
const selection = cmd === "flows" ? rest.slice(1) : [];
// Only the shooting commands mint a directory (see the header): `flows` has its
// <out-dir> and `eval` captures nothing, so neither leaves an empty one behind.
const SHOTS =
  cmd === "smoke" || cmd === "shot"
    ? (process.env.REG_WEBAPP_SHOTS ?? mkdtempSync(join(tmpdir(), "reg-webapp-shots.")))
    : null;
if (SHOTS) mkdirSync(SHOTS, { recursive: true });

// A LADDER, not a retry: each rung gives up something the one above it keeps, so
// every rung is tried once and the first that starts wins. `chromiumSandbox` is
// stated EXPLICITLY on every rung — playwright pushes `--no-sandbox` itself unless
// the option is exactly `true`, so an unstated rung is an unsandboxed one and the
// top two would be the same launch under two names.
//   1. sandboxed — an ordinary multi-process launch with Chromium's own sandbox on:
//      what a real browser does, so it is what we try first.
//   2. no-sandbox — a Linux CONTAINER (the Yard lane image) runs as a uid with no
//      user-namespace grant, so the sandbox helper cannot start; multi-process
//      Chromium is otherwise healthy there, so ONLY the sandbox is dropped.
//   3. single-process — a sandboxed agent SHELL (codex `-s workspace-write`
//      seatbelt, Claude Code's sandboxed Bash) has no mach-register grant, so
//      multi-process Chromium's `bootstrap_check_in … (1100)` rendezvous is denied and
//      the renderer/GPU children never attach. One process has no children to
//      register. Unsupported/best-effort per Chromium — revisit on Playwright bumps
//      (issue #1049).
// Each rung's OWN error is kept: when the ladder runs out, reporting only the last
// failure would hide why the earlier, higher-fidelity rungs were rejected.
const LAUNCH_LADDER = [
  { stage: "sandboxed", options: { chromiumSandbox: true } },
  { stage: "no-sandbox", options: { chromiumSandbox: false } },
  {
    stage: "single-process",
    options: {
      chromiumSandbox: false,
      args: ["--single-process", "--disable-gpu", "--disable-crash-reporter"],
    },
  },
];
async function launchBrowser() {
  const failures = [];
  for (const { stage, options } of LAUNCH_LADDER) {
    try {
      const launched = await chromium.launch(options);
      console.error(`driver: chromium launched (${stage})`);
      return launched;
    } catch (e) {
      failures.push(`${stage}: ${e.message}`);
    }
  }
  throw new Error(`driver: chromium would not launch\n${failures.join("\n\n")}`);
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
  // `null` is the page-function ARGUMENT slot: waitForFunction takes
  // (fn, arg, options), so options passed second are silently the argument and
  // the timeout never applies.
  await page.waitForFunction(() => !document.querySelector('[aria-busy="true"]'), null, {
    timeout: 10_000,
  });
}

async function open(page, path) {
  const resp = await page.goto(BASE + path, { waitUntil: "networkidle" });
  console.log(`GET ${path} → ${resp.status()}`);
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

// ── `flows`: the project error/retry + catalog gates ────────────────────────
//
// Five scenarios × four viewports = 20 cases, each in a FRESH context against
// the REAL backend (the caller points it at a synthetic catalog DB through
// REG_META_DB — see catalog_fixture_db.py). The three error scenarios inject
// exactly one failing request and the two catalog scenarios inject none:
// everything else is the actual app answering — including the browser's own
// IndexedDB, which the catalog cases read back.
//
// The 32 PNGs these write are an artifact contract declared in .yard/config.toml,
// split across TWO gates because a yard gate declares at most 16 filenames:
// `project-flows` names the three /project scenarios (16 PNGs) and
// `catalog-flows` the two catalog ones (16). That is what the scenario argument
// in the dispatch below is for — a bare `flows <out-dir>` still runs all five,
// which is the local verification invocation.
//
// The filenames carry the size — so the sizes are the `shot` presets above,
// spelled once (frontend/DESIGN.md designs for exactly these four widths). Named
// one by one, not Object.values: a fifth preset must not silently turn 16 cases
// into 20 and invalidate the declared artifact lists.
const FLOW_VIEWPORTS = [
  VIEWPORTS.mobile,
  VIEWPORTS.tablet,
  VIEWPORTS.desktop,
  VIEWPORTS.wide,
];
const VALIDATE_PATH = "/api/project/validate";
const ORDER_PATH = "/api/project/order";

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

/** Stage a delivery column in the catalog picker and commit it to the project —
 * the researcher's only add path (both picker shapes wrap their checkbox in the
 * row/cell label, so the column name is its accessible name). */
async function addColumn(page, column) {
  await page.getByRole("checkbox", { name: new RegExp(`^${column}\\b`) }).check();
  await page.getByRole("button", { name: "Add to project" }).click();
}

/** The autosaved draft as IndexedDB actually holds it (`null` until the debounced
 * write lands), read in the page: the catalog flow's whole contract is that the
 * draft is durable BEFORE /project is ever opened, and the UI cannot show that. */
async function autosavedDraft() {
  const done = (request) =>
    new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error("IndexedDB read failed"));
    });
  const db = await done(indexedDB.open("reg_webapp_projects"));
  try {
    if (!db.objectStoreNames.contains("drafts")) return null;
    const store = db.transaction("drafts", "readonly").objectStore("drafts");
    return (await done(store.get("current")))?.draft ?? null;
  } finally {
    db.close();
  }
}

/** Wait until the autosave has written exactly `want` (register variants, in
 * order). POLLED, not slept: an Apply is several draft mutations (the project is
 * created, then the picks commit once their periods resolve) and the write lands
 * ~500ms after the LAST of them, so the first record on disk may be a skeleton.
 * Its own loop rather than waitForFunction so a timeout reports what the draft
 * actually held — the line an operator triages a red gate from. */
async function draftSaved(page, want) {
  const wanted = JSON.stringify(want);
  const deadline = Date.now() + 15_000;
  let held = "null";
  do {
    const stored = await page.evaluate(autosavedDraft);
    held = JSON.stringify((stored?.sources ?? []).map((s) => s.register_variant));
    if (held === wanted) return;
    await page.waitForTimeout(250);
  } while (Date.now() < deadline);
  throw new Error(`autosaved draft holds ${held}, wanted ${wanted}`);
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

/** Scenario 4 — the CATALOG-authored draft. The project lifecycle belongs to the
 * app, not to /project: a column picked from a catalog leaf must autosave, survive
 * a reload, and be EXTENDED (never forked) by a further pick made on a cold
 * catalog entry. Drives the real IndexedDB — the store is empty on every cold
 * load here, so nothing but the restore can produce these results. */
async function catalogDraftCase(page, counts, shoot) {
  // (1) A catalog leaf at a chosen period, on a fresh browser context, with
  //     /project never opened.
  const green = validated(page);
  await addColumn(page, "Kon");
  await green;
  await settled(page);
  // The leaf's own confirmation, rather than the rail's project chip: the rail is
  // an off-canvas drawer at the narrow viewports this case also runs at.
  await page.getByRole("status").filter({ hasText: "Applied +1 column" }).waitFor();
  await shoot("catalog-pick");

  // (2) The autosave is the recovery contract, and it must not wait for /project.
  await draftSaved(page, ["scb/lisa/individer-15plus"]);

  // (3) Navigate away (pushState, no reload) — the draft belongs to the app, so
  // leaving the authoring route neither drops it nor stops the lifecycle.
  await page.locator('a[href="/catalog/scb/rams/syss"]').first().click();
  await settled(page);

  // (4) RELOAD: a cold /project recovers the same project.
  await open(page, "/project");
  await settled(page);
  await page.getByRole("heading", { name: "Sources (1)" }).waitFor();
  // Substring, first match: the cart labels a known variant ("Individer (…)")
  // around the coordinate, and the same coordinate also names the source card.
  await page.getByText("scb/lisa/individer-15plus").first().waitFor();

  // (5) A COLD catalog entry with a saved project: the restored draft is what the
  // automatic validation runs on (away from /project), and the next pick EXTENDS
  // it instead of creating a second project over the saved one.
  const restoredGreen = validated(page);
  await open(page, "/catalog/scb/rams/syss?period=2019");
  await restoredGreen;
  await settled(page);
  const picked = validated(page);
  await addColumn(page, "Syss");
  await picked;
  await settled(page);
  // Extended, not forked: the saved source is still first, the new pick after it.
  await draftSaved(page, ["scb/lisa/individer-15plus", "scb/rams/standard"]);

  // (6) The recovered project, reloaded once more, carries both picks — and is
  //     the same VALID project the researcher authored, not a salvaged fragment.
  await open(page, "/project");
  await settled(page);
  await page.getByRole("heading", { name: "Sources (2)" }).waitFor();
  await page.getByText("scb/rams/standard").first().waitFor();
  await checkValid(projectUi(page), "the recovered project must validate clean");
  check(counts.order === 0, `the draft flow POSTed /order ${counts.order} time(s)`);
  await shoot("project-restored");
}

/** Scenario 5 — the pick with NO period. Ordinary browsing reaches a leaf without a
 * query string, and `scb/lisa/kon` is delivered open-ended, so a column added there
 * has no finite period to author: the source would carry `period: ""` and a binding
 * `type: ""` the resolve could not derive — autosaved before /project is ever opened
 * and rejected by the API. The Add must refuse, keep the draft untouched, say what
 * the researcher has to choose, and stay recoverable: choosing 2018 in the leaf's own
 * Period control and adding again authors the real source. */
async function catalogPeriodRequiredCase(page, counts, shoot) {
  // (1) Add with no period chosen. Settle on EITHER outcome — the refusal notice or
  //     the applied confirmation — so the shot is the state the app actually reached
  //     and the checks below are what make it the right one.
  await addColumn(page, "Kon");
  await page.waitForFunction(
    () => /Apply a period|Applied \+/.test(document.body.innerText),
    null,
    { timeout: 10_000 },
  );
  await settled(page);
  await shoot("catalog-no-period");
  await page.getByRole("alert").filter({ hasText: "Apply a period" }).waitFor();
  check(
    (await page.getByRole("status").filter({ hasText: "Applied" }).count()) === 0,
    "the refused Add reported a successful apply",
  );

  // (2) The draft is untouched: no project was minted, so nothing validates and
  //     nothing reaches IndexedDB (the autosave debounce is ~500ms).
  await page.waitForTimeout(1500);
  const stored = await page.evaluate(autosavedDraft);
  check(
    stored === null && counts.validate === 0,
    `the refused Add autosaved ${JSON.stringify(stored)} and POSTed ` +
      `${counts.validate} validation(s)`,
  );

  // (3) Recover through the control the notice names: 2018, then pick and add
  //     again. Applying a period re-resolves the leaf, which clears the picker's
  //     staging — so this really is a fresh pick, which is why the notice says to
  //     select again rather than just retry.
  const periodSlider = page.getByRole("group", { name: "Period window (years)" });
  await periodSlider.getByLabel("To year").fill("2018");
  await periodSlider.getByLabel("From year").fill("2018");
  await page.getByRole("button", { name: "Apply period" }).click();
  await page.waitForFunction(
    () => new URLSearchParams(location.search).get("period") === "2018",
    null,
    { timeout: 10_000 },
  );
  await settled(page);
  const green = validated(page);
  await addColumn(page, "Kon");
  await green;
  await settled(page);
  await page.getByRole("status").filter({ hasText: "Applied +1 column" }).waitFor();
  await shoot("catalog-period-recovered");

  // (4) The metadata the refused Add could not resolve, as the autosave holds it.
  await draftSaved(page, ["scb/lisa/individer-15plus"]);
  const [source] = (await page.evaluate(autosavedDraft)).sources;
  check(
    source.period === 2018 && source.bindings[0]?.type === "categorical",
    `the recovered source authored ${JSON.stringify(source)}`,
  );
}

/** Run one case in its own context: fresh storage, its own request ledger, its
 * own page-error ledger — and close the context whatever happens. `route` is
 * where the case starts (the /project scenarios' default; the catalog-authored
 * draft starts on a catalog leaf, which is the whole point of it). */
async function runCase(viewport, name, body, route = "/project") {
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
    await open(page, route);
    await settled(page);
    await body(page, counts, (shot) => capture(page, `${shot}-${label}`));
    check(
      pageErrors.length === 0,
      `${name} ${label}: page errors: ${pageErrors.join(" | ")}`,
    );
    console.log(
      `flows: OK ${name} ${label} route=${route} ` +
        `validate=${counts.validate} order=${counts.order} ` +
        `order200=${counts.orderOk} injected-failures=${counts.injected}`,
    );
  } catch (e) {
    console.log(`flows: FAIL ${name} ${label} route=${route} — ${e.message}`);
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
      schema_version: "3.0.0",
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
    // The scenarios by name, with the `route` each starts on (default /project)
    // and the number of PNGs it writes per viewport. `blockedOrderCase` already
    // has runCase's `body` signature; the two recovery cases bind the shared
    // project (and its expected manifest) into theirs.
    const scenarios = {
      "blocked-order": { shots: 1, run: blockedOrderCase },
      "order-retry": {
        shots: 2,
        run: (p, c, shoot) => orderRetryCase(p, c, shoot, project, expected),
      },
      "validation-retry": {
        shots: 1,
        run: (p, c, shoot) => validationRetryCase(p, c, shoot, project),
      },
      // The one case that does NOT start at /project: the draft is authored from
      // a catalog leaf, which is exactly the lifecycle the /project cases cannot
      // reach.
      "catalog-draft": {
        shots: 2,
        route: "/catalog/scb/lisa/kon?period=2018",
        run: catalogDraftCase,
      },
      // The same leaf as ordinary browsing reaches it — NO query string, so no
      // period is chosen and the pick has none to author under.
      "catalog-period-required": {
        shots: 2,
        route: "/catalog/scb/lisa/kon",
        run: catalogPeriodRequiredCase,
      },
    };
    for (const name of selection) {
      check(
        name in scenarios,
        `flows: unknown scenario "${name}" — pick from ${Object.keys(scenarios).join(", ")}`,
      );
    }
    // No names = every scenario, which is the local verification invocation. The
    // two yard gates each name their own subset instead: a gate declares at most
    // 16 artifact filenames, and all five scenarios write 32.
    const names = selection.length > 0 ? selection : Object.keys(scenarios);
    for (const viewport of FLOW_VIEWPORTS) {
      for (const name of names) {
        const { run, route } = scenarios[name];
        await runCase(viewport, name, run, route);
      }
    }
    const shots = names.reduce((n, name) => n + scenarios[name].shots, 0);
    console.log(
      `flows: OK — ${names.length * FLOW_VIEWPORTS.length} cases, ` +
        `${shots * FLOW_VIEWPORTS.length} shots`,
    );
  } else if (cmd === "shot") {
    await open(page, rest[0] ?? "/");
    await settled(page);
    await shot((rest[0] ?? "root").replaceAll("/", "_") || "root");
  } else if (cmd === "eval") {
    await open(page, rest[0] ?? "/");
    console.log(JSON.stringify(await page.evaluate(rest[1] ?? "null"), null, 2));
  } else if (cmd === "smoke") {
    await open(page, "/catalog");
    await settled(page); // the FIRST capture is a settled view too, not a placeholder
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
    // Real form interaction: the leaf's Period control — a labelled dual-thumb
    // year slider. Drive it the way a keyboard researcher does (one ArrowRight
    // tick on "From year", which the thumbs hard-clamp to the subject's own
    // coverage, so the result is always a range the catalog actually delivers),
    // then Apply and assert BOTH what the app wrote to the URL and what it shows.
    // Scoped to the leaf's slider group: the header's project-window slider
    // carries the same two thumb labels.
    const periodSlider = page.getByRole("group", { name: "Period window (years)" });
    const fromYear = periodSlider.getByLabel("From year");
    const seededFrom = Number(await fromYear.inputValue());
    const to = Number(await periodSlider.getByLabel("To year").inputValue());
    check(
      seededFrom < to,
      `period slider seeded a single year (${seededFrom}) — no range to narrow`,
    );
    await fromYear.press("ArrowRight");
    const from = Number(await fromYear.inputValue());
    check(from === seededFrom + 1, `From year did not step: ${seededFrom} → ${from}`);
    const period = `${from}..${to}`;
    await page.getByRole("button", { name: "Apply period" }).click();
    await page.waitForFunction(
      (wire) => new URLSearchParams(location.search).get("period") === wire,
      period,
      { timeout: 10_000 },
    );
    await settled(page);
    await page.waitForFunction(
      (wire) => document.body.innerText.includes(`narrowed to ${wire}`),
      period,
      { timeout: 10_000 },
    );
    console.log(`period ${seededFrom}..${to} → ${period} at ${page.url()}`);
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
