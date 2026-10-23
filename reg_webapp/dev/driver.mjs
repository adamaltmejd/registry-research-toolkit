// The reg_webapp project flows, in Playwright: eleven scenarios (three /project
// error+retry, two /project period, five catalog, one deliberate replacement) × four viewports, each
// in a fresh context against the real servers. Playwright, not agent-browser
// (which drives `dev.sh smoke`/`shot`): the cases inject request failures, hold
// requests open and count them per case, and that routing surface is
// Playwright's. Reuses the frontend's own playwright devDep (vitest-browser
// already requires it + its Chromium).
//
// Run from reg_webapp/frontend/ (so the playwright import resolves), with the
// servers already up and REG_WEBAPP_DEV_URL pointing at them — `dev.sh flows`
// does both:
//
//   bun ../dev/driver.mjs <out-dir> [scenario...]
//
// The PNGs land in <out-dir>. Named scenarios run just those; no names runs all
// eleven.
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

// Where playwright looks for its browsers. An explicit PLAYWRIGHT_BROWSERS_PATH
// always wins; inside a prepared container Chromium is baked at /opt/pw-browsers,
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

// The four widths frontend/DESIGN.md designs for; dev.sh's `shot` presets spell
// the same four.
const VIEWPORTS = {
  mobile: { width: 375, height: 812 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 900 },
  wide: { width: 1920, height: 1080 },
};

// An explicit output directory, so screenshots have a known home; scenario names
// after it, empty for "all of them" (see the dispatch).
const [outDir, ...selection] = process.argv.slice(2);
if (!outDir) {
  throw new Error("driver: needs an output directory, e.g. /tmp/project-flows");
}

// A LADDER, not a retry: each rung gives up something the one above it keeps, so
// every rung is tried once and the first that starts wins. `chromiumSandbox` is
// stated EXPLICITLY on every rung — playwright pushes `--no-sandbox` itself unless
// the option is exactly `true`, so an unstated rung is an unsandboxed one and the
// top two would be the same launch under two names.
//   1. sandboxed — an ordinary multi-process launch with Chromium's own sandbox on:
//      what a real browser does, so it is what we try first.
//   2. no-sandbox — a Linux CONTAINER runs as a uid with no
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
// is the ledger of exception messages, which each case asserts is empty.
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

// `networkidle` is NOT enough: React swaps in fetched data after the network
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

/** Wait until the /project cart shows exactly `n` source cards (each a region
 * named "Source N"). The cart's heading carries no count (DESIGN.md → No
 * counters), so the cards themselves are counted. */
async function sourceCards(page, n) {
  await page.waitForFunction(
    (want) =>
      [...document.querySelectorAll("section[aria-label]")].filter((el) =>
        /^Source \d+$/.test(el.getAttribute("aria-label")),
      ).length === want,
    n,
    { timeout: 10_000 },
  );
}

async function open(page, path) {
  const resp = await page.goto(BASE + path, { waitUntil: "networkidle" });
  console.log(`GET ${path} → ${resp.status()}`);
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

// ── `flows`: the project error/retry + catalog flows ────────────────────────
//
// Eleven scenarios × four viewports = 44 cases, each in a FRESH context against
// the REAL backend (the caller points it at a synthetic catalog DB through
// REG_META_DB — see dev.sh --fixture-db). The three error scenarios inject
// exactly one failing request and the other eight inject none: everything else
// is the actual app answering — including the browser's own IndexedDB, which the
// catalog cases read back.
//
// These write 72 PNGs. A bare `<out-dir>` runs all eleven scenarios;
// scenario arguments select a subset for focused verification.
//
// The filenames carry the size — so the sizes are the VIEWPORTS above,
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
const ORDER_PATH = "/api/project/order/manifest";

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

/** Wait until the URL's applied `?period` reads `wire` — how every Apply path
 * here knows the router has written the navigation the control triggered. */
function periodApplied(page, wire) {
  return page.waitForFunction(
    (want) => new URLSearchParams(location.search).get("period") === want,
    wire,
    { timeout: 10_000 },
  );
}

/** Hold every `?period` resolve open until the returned `release()` lets it
 * through. Nothing is faked — the real request is forwarded, just late — so a
 * case can act WHILE one is genuinely in flight; against a localhost server that
 * answers in single-digit milliseconds there is otherwise no in-flight moment to
 * act in. Gated on a promise rather than a timer: the overlap is then a fact
 * rather than a race the clock usually wins, and it costs no wall time. */
async function holdPeriodResolve(page) {
  // The binding page's `?period` subset is the Rust server's `states` facet.
  const pattern = /\/api\/states\/.*period=/;
  let open;
  const held = new Promise((resolve) => {
    open = resolve;
  });
  // `release()` waits for the requests it let go before removing the route:
  // unrouting one that a handler is still holding hands it to the fallback, and
  // the handler's own `continue()` then loses the race ("Route is already
  // handled!") — noise on stderr for work the case is about to assert on.
  const forwarded = new Set();
  const handler = (route) => {
    const done = held.then(() => route.continue());
    forwarded.add(done);
    return done;
  };
  await page.route(pattern, handler);
  return async () => {
    open();
    await Promise.all(forwarded);
    await page.unroute(pattern, handler);
  };
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

/** The catalog picker's checkbox for `column` — both picker shapes wrap it in the
 * row/cell label, so the column name is its accessible name. */
const columnCheckbox = (page, column) =>
  page.getByRole("checkbox", { name: new RegExp(`^${column}\\b`) });

/** Stage a delivery column in the catalog picker and commit it to the project —
 * the researcher's only add path. */
async function addColumn(page, column) {
  await columnCheckbox(page, column).check();
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

/** Wait until the autosave has written exactly `want` — the draft's sources under
 * `shape`, in order (register variants by default; the source-period case shapes
 * name + period + bindings, which is what a period edit moves and what its two
 * same-variant sources cannot be told apart by). POLLED, not slept: an Apply is
 * several draft mutations (the project is created, then the picks commit once
 * their periods resolve) and the write lands ~500ms after the LAST of them, so the
 * first record on disk may be a skeleton. Its own loop rather than waitForFunction
 * so a timeout reports what the draft actually held — the line an operator triages
 * a red gate from. Returns the matching record, so a caller that needs more of it
 * than the shape does not read IndexedDB a second time. */
async function draftSaved(page, want, shape = (s) => s.register_variant) {
  const wanted = JSON.stringify(want);
  const deadline = Date.now() + 15_000;
  let held = "null";
  do {
    const stored = await page.evaluate(autosavedDraft);
    held = JSON.stringify((stored?.sources ?? []).map(shape));
    if (held === wanted) return stored;
    await page.waitForTimeout(250);
  } while (Date.now() < deadline);
  throw new Error(`autosaved draft holds ${held}, wanted ${wanted}`);
}

/** Whether `locator` currently holds the document focus. */
const focused = (locator) =>
  locator.evaluate((el) => el === document.activeElement);

/** Tab until `locator` holds focus — a REAL keyboard focus, so the shot shows
 * the :focus-visible ring (a programmatic .focus() does not paint one). `key` is
 * "Shift+Tab" to walk backwards, for a control the focus has already passed. */
async function tabTo(page, locator, what, key = "Tab") {
  for (let i = 0; i < 80; i += 1) {
    if (await focused(locator)) return;
    await page.keyboard.press(key);
  }
  throw new Error(`flows: ${what} never took keyboard focus`);
}

/** What the document is focusing, and whether that focus is VISIBLE: the element
 * itself and its `:focus-visible` state. Read off `document.activeElement` rather
 * than a locator, because the question after an apply is where focus WENT —
 * naming an element up front would assume it. */
function focusState(page) {
  return page.evaluate(() => {
    // `<body>` is reported like any other element (it is where a lost focus
    // lands), so every caller reads the same shape.
    const el = document.activeElement ?? document.body;
    return {
      at: `${el.tagName.toLowerCase()}${el.id ? `#${el.id}` : ""}`,
      name: el.getAttribute("aria-label") ?? el.labels?.[0]?.textContent ?? "",
      inPeriodCard: el.closest(".period-picker") !== null,
      visible: el.matches(":focus-visible"),
    };
  });
}

/** One artifact: assert the layout fits the viewport, then write the PNG into
 * `outDir` (module scope). */
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

// The verdict ValidationPanel renders for a clean result: "Draft valid",
// alone when the result carries no issues (Y-75 — with nothing to report the
// verdict is the whole line) and followed by "with warnings" + the checked clause
// when it carries some. Anchored on the VERDICT only, so the clause's copy can
// change without breaking every scenario. The failing branch ("Draft not valid —
// N errors.") and the in-flight one ("Checking the current project…") match neither.
const VALID_SUMMARY = /^Draft valid\b/;

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
    // The verdict's sentence: the StatusRow's text after its aria-hidden glyph,
    // which innerText would otherwise lead with.
    summary: panel
      .getByRole("status")
      .locator(':scope > span:not([aria-hidden="true"])'),
    // The CTA's accessible name IS "Downloading…" while the order POST is in
    // flight, so this locator resolving is itself the cleared-busy wait.
    download: page.getByRole("button", { name: "Download order" }),
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

  // A new project has no name, and an order manifest needs one
  // (`project_unnamed`): the download stays closed and says why, visibly and as
  // the button's description, before any order POST. Fails if the gate ignores
  // the name or the reason lives only in a tooltip.
  check(await ui.download.isDisabled(), "an unnamed project must not offer the download");
  const reason = page.getByText("Name the project to download the order.");
  check(await reason.isVisible(), "the unnamed-project reason must be visible text");
  const describedBy = await ui.download.getAttribute("aria-describedby");
  check(
    describedBy != null &&
      (await page.locator(`[id="${describedBy}"]`).innerText()).includes(
        "Name the project",
      ),
    `the download must be described by the reason, got ${describedBy}`,
  );
  const named = validated(page);
  await page.getByRole("textbox", { name: "Name" }).fill("Draft");
  await named;
  await settled(page);
  check((await reason.count()) === 0, "naming the project must clear the reason");

  const blocked = page.waitForResponse((r) => posts(r.request(), ORDER_PATH));
  await ui.download.click();
  const response = await blocked;
  check(response.status() === 422, `expected 422 from /order, got ${response.status()}`);
  const { error } = await response.json();
  check(
    error.code === "order_blocked" &&
      error.fields.findings.some((f) => f.code === "project_empty"),
    `expected order_blocked with a project_empty finding, got ${JSON.stringify(error)}`,
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

  // Y-98: an Apply with nothing to change says so rather than doing nothing
  // silently. Captured here because this scenario already opens `lisa-2018`, the
  // one project-flows fixture with a SourceEditor card in frame — the notice
  // outlives the rest of this scenario, so it is still on screen for the shots
  // below.
  const card = page.getByRole("region", { name: "Source 1" });
  await card.getByRole("button", { name: /^Apply period/ }).click();
  await card.getByText("Period unchanged.").waitFor();

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
    /^order_[a-z0-9-]+_\d{4}-\d{2}-\d{2}\.json$/.test(download.suggestedFilename()),
    `downloaded ${download.suggestedFilename()}, wanted order_<slug>_<date>.json`,
  );
  const manifest = JSON.parse(readFileSync(await download.path(), "utf8"));
  check(
    manifest.provenance.mode === expected.mode &&
      manifest.provenance.steward === expected.steward &&
      manifest.provenance.catalog_generation_id === expected.generationId &&
      manifest.provenance.catalog_schema_version === expected.schemaVersion,
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
 * catalog entry — or ticked straight off the REGISTER LIST (Y-83), the other
 * surface that authors through the same staged add. Drives the real IndexedDB —
 * the store is empty on every cold load here, so nothing but the restore can
 * produce these results. */
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
  await sourceCards(page, 1);
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

  // (6) The REGISTER LIST as an authoring surface (Y-83). A researcher who knows
  //     LISA by its delivery columns ticks one straight from the list — no
  //     variable page opened — and it lands through the same staged add the leaf
  //     uses: the LISA source it already authored is EXTENDED (its period widening
  //     to cover the column's own span), never forked into a second one.
  const listed = validated(page);
  await open(page, "/catalog/scb/lisa");
  await listed;
  await settled(page);
  const fromList = validated(page);
  await columnCheckbox(page, "Forsamling").check();
  // The register list's add bar names its count (DESIGN.md → VariableRow).
  await page.getByRole("button", { name: "Add 1 column to project" }).click();
  await fromList;
  await settled(page);
  await page.getByRole("status").filter({ hasText: "Applied +1 column" }).waitFor();
  // Shaped by BINDING COUNT, not just the coordinate: the register-list add lands
  // in the source the leaf already authored, so a coordinate-only wait would pass
  // on the PREVIOUS autosave and race the debounced write of this one.
  await draftSaved(
    page,
    ["scb/lisa/individer-15plus:2", "scb/rams/standard:1"],
    (s) => `${s.register_variant}:${s.bindings.length}`,
  );

  // (7) The recovered project, reloaded once more, carries every pick — and is
  //     the same VALID project the researcher authored, not a salvaged fragment.
  await open(page, "/project");
  await settled(page);
  await sourceCards(page, 2);
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
    () => /Choose a period|Applied \+/.test(document.body.innerText),
    null,
    { timeout: 10_000 },
  );
  await settled(page);
  await shoot("catalog-no-period");
  // StagedAddStatus announces a refusal as `status`, not `alert`: every refusal a
  // host can show is one the researcher's own next move retires (this one: choose a
  // period), so it is a state to read, not an emergency to interrupt for.
  await page.getByRole("status").filter({ hasText: "Choose a period" }).waitFor();
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

  // (3) Recover through the control the notice names: the To thumb sent to
  //     its floor, which is the From thumb on 2018 (Kon opens there), so the
  //     period is 2018; then pick and add again. Applying a period re-resolves
  //     the leaf, which clears the picker's staging — so this really is a fresh
  //     pick, which is why the notice says to select again rather than just retry.
  const periodCard = page.locator(".period-picker");
  await periodCard.getByRole("slider", { name: "To year" }).focus();
  await page.keyboard.press("Home");
  await periodApplied(page, "2018");
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

  // (5) The browse tree stays reachable by keyboard after its rows fold away.
  await browseTreeStaysTabbable(page);
}

/** After a row is clicked and "Collapse all" hides it, Tab still reaches a tree
 * row. React Aria keeps only the focused row in the Tab order, so a focused key
 * left on a hidden row drops the whole tree out of it (#1782 review, twice).
 * Only where the browse column shows beside the detail pane (above 48rem). */
async function browseTreeStaysTabbable(page) {
  const collapseAll = page.getByRole("button", { name: "Collapse all" });
  if (!(await collapseAll.isVisible())) return;
  await page.getByRole("row", { name: "RAMS" }).click();
  await settled(page);
  await collapseAll.click();
  await page.getByRole("button", { name: "Expand all" }).focus();
  await page.keyboard.press("Tab");
  const landed = await page.evaluate(() => {
    const el = document.activeElement;
    return el?.getAttribute("role") === "row" && el.closest('[role="treegrid"]') !== null
      ? el.textContent.trim()
      : `${el?.tagName.toLowerCase()} ${el?.getAttribute("role") ?? ""}`;
  });
  check(
    landed.startsWith("Statistiska"),
    `Tab after "Collapse all" must reach the collapsed group's row, landed on ${landed}`,
  );
}

/** The browse search (DESIGN.md → Search filters in place). On LISA's page, a
 * search for another register ("midas") narrows the register column to the
 * provider that holds it, while the open page keeps every LISA variable: the
 * search filters the columns, never the page (#1829, #1830). Escape pressed
 * with focus on the page, not in the field, clears it and brings the hidden
 * providers back (#1827). Below the breakpoint the columns stack, and the
 * "Browse registers" button at the head of the trail opens the register list
 * (#1824). Each check fails on the behavior it names: a page filtered with the
 * columns loses Forsamling, a kept non-matching provider leaves its row, and an
 * Escape heard only by the field leaves the text in place. */
async function browseSearchCase(page, _counts, shoot) {
  const search = page.getByRole("searchbox", { name: /^Search registers/ });
  const scb = page.getByRole("row", { name: "Statistiska Centralbyrån", exact: true });
  const treeShown = await page.getByRole("button", { name: "Collapse all" }).isVisible();

  await search.fill("midas");
  if (treeShown) {
    await page.getByRole("row", { name: "MiDAS", exact: true }).waitFor();
    await scb.waitFor({ state: "detached" });
  }
  await settled(page);
  check(
    (await columnCheckbox(page, "Forsamling").count()) === 1,
    "the search for another register filtered LISA's own page",
  );
  await shoot("browse-search");
  if (!treeShown) {
    // The list the search filters is off screen: the way back says so.
    check(
      (await page.getByRole("link", { name: "Show matching registers" }).count()) === 1,
      "below the breakpoint the search left no way to its matches",
    );
  }

  // Focus off the field: the page's heading, then Escape.
  await page.getByRole("heading", { level: 1, name: "LISA" }).click();
  await page.keyboard.press("Escape");
  check(
    (await search.inputValue()) === "",
    "Escape with focus on the page left the search text in place",
  );
  if (treeShown) {
    await scb.waitFor();
  } else {
    await page.getByRole("link", { name: "Browse registers" }).click();
    await page.getByRole("row", { name: "LISA", exact: true }).waitFor();
  }
}

/** Scenario 6 — the source's PERIOD, edited on its /project card (Y-81), one
 * From/To row per segment for a #307 list period (Y-101). A researcher whose
 * study window is 2018..2020 wants ONE source to reach back to 2015, without
 * deleting and rebuilding it. The project opened here carries two differently
 * named sources on the SAME register variant — the shape the catalog's add path
 * cannot author (it finds-or-creates by variant), and the one only a source NAME
 * tells apart — so the edit must move exactly the card it was made on, keeping
 * that name and every column, Forsamling included. `lisa-lonfink`'s period is a
 * two-segment list (`2015..2017,2019..2020`) purely to give the first shot a
 * two-row card to render alongside the single-range card being edited — nothing
 * here edits it. The card is where this lives because it is the only surface that
 * shows a source WHOLE, which is what a source-wide rewrite has to be looked at
 * against. Rendered because the card, its refusal and the deviation it then
 * carries are the surface the operator judges.
 */
async function sourcePeriodCase(page, counts, shoot, project) {
  // (1) Open the two-source project, and let the autosave hold it.
  const green = validated(page);
  await openProjectFile(page, project);
  await green;
  await settled(page);
  await page.getByRole("heading", { name: project.name }).waitFor();
  const variant = "scb/lisa/individer-15plus";
  await draftSaved(page, [variant, variant]);

  // The card of the source named `lisa-core`. The two sources share a register, a
  // variant and therefore a heading, so the card's own region is what tells them
  // apart — the same thing the source name does in the draft.
  const card = page.getByRole("region", { name: "Source 1" });
  const from = card.getByRole("textbox", { name: "From" });
  const to = card.getByRole("textbox", { name: "To" });
  const apply = card.getByRole("button", { name: /^Apply period/ });

  // (2) The card shows the source WHOLE: its name, its stored period armed in the
  //     year fields, and every column on it — Forsamling included, which no single
  //     catalog page lists beside Kon.
  await card.getByText("lisa-core").waitFor();
  //     The column list names a variable, not its FQID (#1839): the row is
  //     found by the link to its catalog page.
  await card.locator('a[href="/catalog/scb/lisa/forsamling"]').waitFor();
  const armed = `${await from.inputValue()}..${await to.inputValue()}`;
  check(armed === "2018..2020", `the card armed its year fields at ${armed}`);
  // The sibling card (Y-101): a two-segment list period renders one row per
  // segment rather than the read-only note Y-81 shipped it with.
  const sibling = page.getByRole("region", { name: "Source 2" });
  const siblingFrom = sibling.getByRole("textbox", { name: "From" });
  check(
    (await siblingFrom.count()) === 2,
    `the two-segment sibling card rendered ${await siblingFrom.count()} From row(s)`,
  );
  await shoot("project-source-period-card");

  // (3) Years that name no range are refused, with the field at fault marked and
  //     nothing written. Apply stays LIVE so the click explains itself, rather than
  //     leaving a dead button and no reason.
  await from.fill("2030");
  await apply.click();
  await card.getByText(/From 2030 is after To 2020/).waitFor();
  check(await apply.isEnabled(), "the refused entry left Apply dead");
  await shoot("project-source-period-refused");

  // (4) The edit: ONE period-only diff, applied explicitly, revalidated at once.
  const revalidated = validated(page);
  await from.fill("2015");
  await apply.click();
  await revalidated;
  await settled(page);
  // The source now reaches past the study window, and the card says so in a ▲
  // StatusRow: the line is a heads-up, not a refusal, and it does not block the
  // order (a source that deliberately covers more is an ordinary order).
  await card.getByText("Differs from study window 2018–2020").waitFor();
  await shoot("project-source-period-applied");

  // (5) What the browser's own IndexedDB holds: the edited source moved, keeping
  //     its name and BOTH bindings, and the sibling on the same variant is exactly
  //     as it was opened.
  await draftSaved(
    page,
    [
      [
        "lisa-core",
        { from: 2015, to: 2020 },
        ["scb/lisa/kon", "scb/lisa/forsamling"],
      ],
      [
        "lisa-lonfink",
        [
          { from: 2015, to: 2017 },
          { from: 2019, to: 2020 },
        ],
        ["scb/lisa/lonfink"],
      ],
    ],
    (s) => [s.name, s.period, s.bindings.map((b) => b.variable)],
  );
  check(
    counts.validate > 0 && counts.order === 0,
    `the period edit POSTed ${counts.validate} validation(s) and ` +
      `${counts.order} order(s)`,
  );
}

/** Scenario 9 — a variable whose availability is NARROWER than the project
 * window (maintainer ruling 2026-10-22). The project window is 2015..2020 and
 * `scb/lisa/kon` is delivered from 2018, so the variable's own period card seeds
 * at 2018..2020 and says what the window asks for that the data lacks in an "i"
 * note. That narrowing is the data's, not the researcher's: applying the seed
 * must not raise the ▲ "Differs from the project window" row, which it did when
 * the comparison ignored availability. A period the researcher narrows past the
 * data (2019..2020) is a real deviation and must still raise it. */
async function periodAvailabilityCase(page, _counts, shoot, project) {
  // (1) A project with the wide window, opened through the editor and autosaved,
  //     so the cold leaf below restores it.
  const green = validated(page);
  await openProjectFile(page, project);
  await green;
  await settled(page);
  await draftSaved(page, ["scb/lisa/individer-15plus"]);
  await open(page, "/catalog/scb/lisa/kon");
  await settled(page);

  const card = page.locator(".period-picker");
  const differs = card.getByText(/Differs from the project window/);
  // (2) The seed: availability narrows the window, said as information.
  await card.getByText("Not covered 2015–2017").waitFor();
  check((await differs.count()) === 0, "the unapplied seed raised the ▲ row");

  // (3) Narrowing past the data is the researcher's choice: the ▲ row shows.
  const fromThumb = card.getByRole("slider", { name: "From year" });
  await fromThumb.focus();
  await page.keyboard.press("ArrowRight");
  await periodApplied(page, "2019..2020");
  await differs.waitFor();

  // (4) Moving back onto the seed applies it, and that is still no deviation.
  await page.keyboard.press("ArrowLeft");
  await periodApplied(page, "2018..2020");
  await settled(page);
  check(
    (await differs.count()) === 0,
    "applying the availability-narrowed seed raised the ▲ row",
  );
  // The band is still the project window, so the "i" note still names the gap.
  await card.getByText("Not covered 2015–2017").waitFor();
  await shoot("catalog-period-availability");
  await page.keyboard.press("ArrowRight");
  await periodApplied(page, "2019..2020");
  await differs.waitFor();

  // (5) Reset writes the window as Kon can hold it (2018..2020, not the raw
  //     2015..2020), and the ▲ row goes.
  await card.getByRole("button", { name: "Reset" }).click();
  await periodApplied(page, "2018..2020");
  await settled(page);
  check((await differs.count()) === 0, "Reset left the ▲ row up");
}

/** Scenario 10 — the /project cart judges a source's period against the study
 * window inside the years its columns are delivered in (period ruling
 * 2026-10-22), the rule the variable page's card uses. Under the window
 * 2015..2020 both sources hold 2018..2020. `kon-only` carries Kon, delivered
 * from 2018: its period is exactly what an add writes (window ∩ availability),
 * so its card raises no ▲ row. `kon-forsamling` also carries Forsamling,
 * delivered since 1952, so its columns could fill 2015..2017 and its period is a
 * real deviation. Fails on the pre-fix code, which raised the ▲ row on both
 * cards and counted "2 sources differ". */
async function projectPeriodAvailabilityCase(page, _counts, shoot, project) {
  const green = validated(page);
  await openProjectFile(page, project);
  await green;
  await settled(page);
  await page.getByRole("heading", { name: project.name }).waitFor();

  const differs = (card) =>
    card.getByText("Differs from study window 2015–2020");
  const konOnly = page.getByRole("region", { name: "Source 1" });
  const both = page.getByRole("region", { name: "Source 2" });
  // The deviation shows first, so the register read both cards are judged by
  // has landed before the absence below is asserted.
  await differs(both).waitFor();
  await page.getByText("1 source differs from it.").waitFor();
  check(
    (await differs(konOnly).count()) === 0,
    "a source holding window ∩ availability raised the ▲ row",
  );
  await shoot("project-period-availability");
}

/** Scenario 8 — the period card KEEPS keyboard focus while each key press on
 * a thumb applies (Y-65). A keyboard researcher adjusting the catalog period
 * used to lose focus to the document body on every apply, because the applied
 * `?period` moved the router's route object and remounted the article the card
 * lives in (reg_webapp/DESIGN.md → SPA routing). Both thumbs are driven here, and
 * the shots are the evidence that the focus ring is still on screen afterwards.
 * The thumb's ring is declared on the knob pseudo-element, which no browser
 * reports a computed style for, so the check is that the thumb still holds a
 * visible keyboard focus and the shot is the ring's evidence. The draft the leaf
 * authored first is the guard on the other side: browsing the period must not
 * rewrite it. */
async function periodFocusCase(page, counts, shoot) {
  // Scoped to the card: the desktop widths also render the project-window
  // slider in the header.
  const card = page.locator(".period-picker");
  const fromThumb = card.getByRole("slider", { name: "From year" });
  const toThumb = card.getByRole("slider", { name: "To year" });
  const thumbHeld = async (name, why) => {
    const state = await focusState(page);
    check(
      state.inPeriodCard && state.visible && state.name === name,
      `${why} — focus is ${JSON.stringify(state)}`,
    );
  };

  // (1) Arriving at the leaf focuses nothing — a period control that grabbed the
  //     focus on navigation would be a worse bug than the one being fixed.
  const arrival = await focusState(page);
  check(
    arrival.at === "body",
    `the leaf focused ${JSON.stringify(arrival)} on arrival`,
  );

  //     Then a pick, so the browse-period edits below have a draft to leave alone.
  const green = validated(page);
  await addColumn(page, "Kon");
  await green;
  await settled(page);
  const draft = await draftSaved(page, ["scb/lisa/individer-15plus"]);
  const authored = JSON.stringify(draft.sources);
  const validations = counts.validate;

  // (2) The From thumb, tabbed to (never .focus()ed — a programmatic focus
  //     paints no ring) and arrowed: the key press applies, focus stays.
  await tabTo(page, fromThumb, "the From year thumb");
  await page.keyboard.press("ArrowLeft");
  await periodApplied(page, "2018..2020");
  await settled(page);
  await thumbHeld("From year", "an arrowed From thumb lost the period card's focus");
  await page.getByText("narrowed to 2018..2020").waitFor();
  await shoot("catalog-period-focus-from");

  // (3) Tab reaches the To thumb, and its key press keeps focus too.
  await page.keyboard.press("Tab");
  check(await focused(toThumb), "Tab from the From thumb did not reach the To thumb");
  await page.keyboard.press("ArrowRight");
  await periodApplied(page, "2018..2021");
  await settled(page);
  await thumbHeld("To year", "an arrowed To thumb lost the period card's focus");
  await page.getByText("narrowed to 2018..2021").waitFor();
  await shoot("catalog-period-focus-to");

  // (5) A researcher who moves on DURING the request is not pulled back. The
  //     resolve is held until they have moved, so the overlap is a fact and not a
  //     race, and the column row is where they moved to — a control the leaf has
  //     at every width.
  const release = await holdPeriodResolve(page);
  const elsewhere = columnCheckbox(page, "Kon");
  const resolved = page.waitForResponse((r) => r.url().includes("period=2018..2020"));
  await page.keyboard.press("ArrowLeft");
  await elsewhere.focus();
  await release();
  await resolved;
  await settled(page);
  // The column is inside the period card since the Timeline and the columns
  // are one drawing (#1833), so the check is that focus is still on it.
  const moved = await focusState(page);
  check(
    await focused(elsewhere),
    `focus was pulled back to ${JSON.stringify(moved)} after the researcher left`,
  );

  // (6) What the browse-period edits authored: nothing. The draft is the one the
  //     pick made, and no further validation was asked for. `settled` already
  //     waited out the network; this is the autosave debounce (~500ms) on top.
  await page.waitForTimeout(600);
  const held = JSON.stringify((await page.evaluate(autosavedDraft)).sources);
  check(
    held === authored && counts.validate === validations,
    `browsing the period rewrote the draft to ${held} (was ${authored}) and ` +
      `POSTed ${counts.validate - validations} extra validation(s)`,
  );
}

/** Scenario 7 — replacing an EDITED draft is deliberate. New and a successful
 * Open both destroy the loaded project, and the browser keeps ONE recovery copy
 * under one autosave key, so both ask the same question first; a cancel leaves
 * the draft (and that copy) exactly as they were. Rendered because the
 * confirmation is the surface the operator judges. */
async function replaceConfirmCase(page, shoot, project) {
  const ui = projectUi(page);
  const dialog = page.getByRole("alertdialog", {
    name: "Replace the current project?",
  });
  const cancel = dialog.getByRole("button", { name: "Cancel" });
  const replace = dialog.getByRole("button", { name: "Replace without downloading" });
  const newProject = page.getByRole("button", { name: "New", exact: true });
  const edited = page.getByRole("heading", { name: /In progress/ });

  // An edited draft — created here, then named — i.e. work worth losing.
  let green = validated(page);
  await page.getByRole("button", { name: "New project" }).click();
  await green;
  green = validated(page);
  await page.getByRole("textbox", { name: "Name" }).fill("In progress");
  await green;
  await settled(page);
  await checkValid(ui, "the edited draft must validate clean before it is replaced");

  // (1) New asks first, naming what is at stake.
  await newProject.click();
  await dialog.waitFor();
  check(
    (await dialog.getAttribute("aria-modal")) === "true",
    "the replacement confirmation must be a modal alert dialog",
  );
  check(
    (await dialog.innerText()).includes("In progress"),
    "the confirmation must name the project it would replace",
  );
  // Tab to Cancel: a REAL keyboard focus inside the trap, so the shot carries the
  // focus ring as well as the dialog.
  await tabTo(page, cancel, "Cancel");
  await settled(page);
  // The dialog and its scrim fade in over --duration-slow: shoot the settled
  // dialog, not its entrance. Finite animations only (a skeleton shimmer loops).
  await page.evaluate(() =>
    Promise.all(
      document
        .getAnimations()
        .filter((a) => a.effect?.getTiming().iterations !== Infinity)
        .map((a) => a.finished),
    ),
  );
  await shoot("replace-confirm");

  // (2) Cancelled: the edited draft is still loaded, and focus comes back.
  await cancel.click();
  await dialog.waitFor({ state: "detached" });
  await edited.waitFor();
  check(
    await focused(newProject),
    "focus must return to the control that opened the confirmation",
  );

  // (3) A successful Open asks the SAME question — after the file parsed, and
  // before anything of it is loaded.
  green = validated(page);
  await openProjectFile(page, project);
  await dialog.waitFor();
  await edited.waitFor();
  check(
    (await page.getByRole("heading", { name: project.name }).count()) === 0,
    "the picked file must not load while the confirmation still stands",
  );

  // (4) Confirmed: the file is loaded, and the project it replaced is gone.
  await replace.click();
  await green;
  await dialog.waitFor({ state: "detached" });
  await page.getByRole("heading", { name: project.name }).waitFor();
  await settled(page);
  await checkValid(ui, "the opened project must validate clean once it is loaded");
  await shoot("replace-opened");
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

// One try/finally around the whole run: the browser closes on the way out,
// including the failure path a thrown assertion takes.
try {
  mkdirSync(outDir, { recursive: true });
  // The project the two recovery scenarios open, and the manifest it must
  // produce, are DERIVED from the deployment the flows run against — never
  // pinned to a stale capture. Shapes: conformance/cases/api/order-* (a valid
  // global-fallback project + its entry).
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
  // The Rust server's `context`: `{data, meta}`, the generation in `meta`.
  const { data: deployment, meta } = JSON.parse(contextBody);
  const project = {
    schema_version: "3.0.0",
    steward: deployment.steward.id,
    reg_meta_version: `reg_meta/v${deployment.reg_meta_version}`,
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
  // The source-period edit's project (Y-81): TWO differently named sources on
  // ONE register variant — a shape the catalog's add path cannot author, and the
  // reason the edit is keyed by source name. `lisa-core` carries Kon plus
  // Forsamling, which no single catalog page lists beside it. The study window is
  // what the edited period then deviates from.
  const twoSourceProject = {
    ...project,
    name: "Synthetic source period",
    window: { from: 2018, to: 2020 },
    sources: [
      {
        name: "lisa-core",
        register_variant: "scb/lisa/individer-15plus",
        period: { from: 2018, to: 2020 },
        bindings: [
          { variable: "scb/lisa/kon", type: "categorical", representation: "Kon" },
          {
            variable: "scb/lisa/forsamling",
            type: "categorical",
            representation: "Forsamling",
          },
        ],
      },
      {
        name: "lisa-lonfink",
        register_variant: "scb/lisa/individer-15plus",
        // A #307 two-segment list (Y-101) — untouched by this scenario, present
        // so the shots show a multi-row card alongside the one being edited.
        period: [
          { from: 2015, to: 2017 },
          { from: 2019, to: 2020 },
        ],
        bindings: [
          {
            variable: "scb/lisa/lonfink",
            type: "numeric",
            representation: "LonFinkJan",
          },
        ],
      },
    ],
  };
  // The manifest names the catalog it was materialized against by the SAME
  // identity `/api/context` reports for the served artifact (reg_meta
  // `OrderProvenance`): its steward, generation and schema — not its import
  // date. The steward also fixes the artifact kind (`global` is the catalog
  // artifact, an admission invariant), so the kind needs no check of its own.
  const expected = {
    mode: "global_fallback",
    steward: deployment.steward.id,
    generationId: meta.generation,
    schemaVersion: deployment.schema_version,
  };
  console.log(
    `flows: ${BASE} steward=${deployment.steward.id} ` +
      `reg_meta=${deployment.reg_meta_version} ` +
      `catalog=${deployment.schema_version}@${deployment.import_date}`,
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
    // The browse search on a register page, and the narrow way back to the
    // register list.
    "browse-search": {
      shots: 1,
      route: "/catalog/scb/lisa",
      run: browseSearchCase,
    },
    // Opens the SAME synthetic project as the recovery cases — the file whose
    // arrival has to wait for the researcher's answer.
    "replace-confirm": {
      shots: 2,
      run: (p, _c, shoot) => replaceConfirmCase(p, shoot, project),
    },
    // Opens the TWO-SOURCE project and edits one named source's period on its
    // own /project card — the only surface that shows a source whole.
    "project-source-period": {
      shots: 3,
      run: (p, c, shoot) => sourcePeriodCase(p, c, shoot, twoSourceProject),
    },
    // The SAME leaf the reported focus loss was found on, entered the way a
    // shared link reaches it — with a period already applied, so every key press
    // below is a period CHANGE.
    "catalog-period-focus": {
      shots: 2,
      route: "/catalog/scb/lisa/kon?period=2019..2020",
      run: periodFocusCase,
    },
    // Opens the one-source project under a window wider than Kon's
    // availability, then judges Kon's own period card against it.
    "catalog-period-availability": {
      shots: 1,
      run: (p, c, shoot) =>
        periodAvailabilityCase(p, c, shoot, {
          ...project,
          name: "Synthetic wide window",
          window: { from: 2015, to: 2020 },
        }),
    },
    // Two sources on one variant under that wide window: one whose columns
    // start in 2018, one whose columns reach back past the window.
    "project-period-availability": {
      shots: 1,
      run: (p, c, shoot) =>
        projectPeriodAvailabilityCase(p, c, shoot, {
          ...project,
          name: "Synthetic period availability",
          window: { from: 2015, to: 2020 },
          sources: [
            {
              name: "kon-only",
              register_variant: "scb/lisa/individer-15plus",
              period: { from: 2018, to: 2020 },
              bindings: [
                {
                  variable: "scb/lisa/kon",
                  type: "categorical",
                  representation: "Kon",
                },
              ],
            },
            {
              name: "kon-forsamling",
              register_variant: "scb/lisa/individer-15plus",
              period: { from: 2018, to: 2020 },
              bindings: [
                {
                  variable: "scb/lisa/kon",
                  type: "categorical",
                  representation: "Kon",
                },
                {
                  variable: "scb/lisa/forsamling",
                  type: "categorical",
                  representation: "Forsamling",
                },
              ],
            },
          ],
        }),
    },
  };
  for (const name of selection) {
    check(
      name in scenarios,
      `flows: unknown scenario "${name}" — pick from ${Object.keys(scenarios).join(", ")}`,
    );
  }
  // No names = every scenario; explicit names select a focused subset.
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
} finally {
  await browser.close();
}
