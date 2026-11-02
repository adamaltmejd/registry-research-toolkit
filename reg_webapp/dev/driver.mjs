// Shell and browse-search flows × four viewports, against the source-built
// webapp-dev fixture. Run from reg_webapp/frontend via dev.sh flows; screenshots
// land in the required <out-dir>.
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

async function open(page, path) {
  const resp = await page.goto(BASE + path, { waitUntil: "networkidle" });
  console.log(`GET ${path} → ${resp.status()}`);
}

function check(condition, message) {
  if (!condition) throw new Error(message);
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

const FLOW_VIEWPORTS = [
  VIEWPORTS.mobile,
  VIEWPORTS.tablet,
  VIEWPORTS.desktop,
  VIEWPORTS.wide,
];

// The retained shell boundary case fails if filtering leaves an unrelated provider,
// palette focus/closing breaks, or a chosen action leaks its query into the filter.
// Provider/register names come from fixtures/webapp-dev/curation/inputs.toml and
// inputs/frame/webapp-dev.jsonl; no entity detail or project behavior is asserted.
async function browseSearchCase(page, shoot) {
  const search = page.getByRole("searchbox", { name: /^Search registers/ });
  const otherProvider = page.getByRole("row", { name: "Demoleverantören", exact: true });
  const treeShown = await page.getByRole("button", { name: "Collapse all" }).isVisible();

  if (!treeShown) {
    // Below the breakpoint the field is folded behind a "Search" button, and
    // pressing it puts focus in the field: typing with no fill() goes nowhere
    // if focus does not move.
    await page.getByRole("button", { name: /^Search registers/ }).click();
    await page.keyboard.type("andra");
  } else {
    await search.fill("andra");
  }
  if (treeShown) {
    await page.getByRole("row", { name: "Andra registret", exact: true }).waitFor();
    await otherProvider.waitFor({ state: "detached" });
  }
  await settled(page);
  await shoot("browse-search");
  if (!treeShown) {
    // The list the search filters is off screen: the way back says so.
    check(
      (await page.getByRole("link", { name: "Show matching registers" }).count()) === 1,
      "below the breakpoint the search left no way to its matches",
    );
  }

  // Enter opens the palette under the field, which stays its one input: a
  // palette with a field of its own shows the text twice (the SWECOV report,
  // 2026-10-24), and its popover, open, must still fit the viewport (the shot).
  // Escape closes it and leaves the text.
  await search.press("Enter");
  await page.getByRole("menu").waitFor();
  check(
    (await page.getByRole("searchbox").count()) === 1,
    "the open palette renders a search field of its own",
  );
  // Past the popover's entry fade, so the shot shows it opaque.
  await page.waitForFunction(() => !document.querySelector("[data-entering]"), null, {
    timeout: 5000,
  });
  await shoot("browse-search-palette");
  await search.press("Escape");
  await page.getByRole("menu").waitFor({ state: "detached" });
  check(
    (await search.inputValue()) === "andra",
    "Escape with the palette open cleared the text instead of closing it",
  );

  // Tab out of the field with the palette open: it closes. Left open, focus
  // walks the page under a popover that still claims the field (#1871 review).
  await search.press("Enter");
  await page.getByRole("menu").waitFor();
  await page.keyboard.press("Tab");
  check(
    await menuGone(page),
    "Tab out of the field left the palette open under the page",
  );

  // Focus is now off the field (the Tab above), so Escape on the page clears
  // the text (#1827) and, below the breakpoint, folds the narrow field.
  await page.keyboard.press("Escape");
  // Below the breakpoint the emptied field is folded away (display: none).
  const searchAnyState = page.getByRole("searchbox", {
    name: /^Search registers/,
    includeHidden: true,
  });
  check(
    (await searchAnyState.inputValue()) === "",
    "Escape with focus on the page left the search text in place",
  );
  if (treeShown) {
    await otherProvider.waitFor();
  } else {
    // The emptied field folds back behind its button: left unfolded, the
    // header keeps an empty field and loses the logo and the button.
    check(
      await page.getByRole("button", { name: /^Search registers/ }).isVisible(),
      "Escape on the page cleared the search but left the narrow field unfolded",
    );
    await page.getByRole("link", { name: "Browse registers" }).click();
    await page.getByRole("row", { name: "Basregistret", exact: true }).waitFor();
  }

  // Choosing a palette row gives the browse columns back the filter they had
  // when the palette opened (Operator 2026-10-24): text typed only to find the
  // row does not stay behind as a filter. Fails if the palette's text leaks.
  if (treeShown) {
    await search.fill("andra");
  } else {
    await page.getByRole("button", { name: /^Search registers/ }).click();
    await page.keyboard.type("andra");
  }
  await search.press("Enter");
  await page.getByRole("menu").waitFor();
  await search.fill("project");
  await page.getByRole("menuitem", { name: /^Go to the project/ }).click();
  await page.waitForURL("**/project");
  const left = await searchAnyState.inputValue();
  check(
    left === "andra",
    `a palette row left the browse filter at ${JSON.stringify(left)}, not "andra"`,
  );
}

// Source: webapp-dev's hush/fam releases and its 2021 release notice. Fails if
// register/table dispatch, table links, collapsed releases or grouped findings
// are lost. The older shell/search case never opened these entity surfaces.
async function browsePagesCase(page, shoot) {
  const main = page.getByRole("main");
  await main.getByRole("link", { name: "demo", exact: true }).click();
  await page.waitForURL("**/catalog/demo");
  await main.getByRole("link", { name: "Hushållsregistret", exact: true }).click();
  await page.waitForURL("**/catalog/demo/hush");
  await settled(page);
  const tables = main.locator("section").filter({ has: page.getByRole("heading", { name: "Tables", exact: true }) });
  await tables.getByRole("link", { name: "Individer", exact: true }).waitFor();
  await shoot("browse-register");
  await tables.getByRole("link", { name: "Familjer", exact: true }).click();
  await page.waitForURL("**/catalog/demo/hush/fam");
  await settled(page);
  await main.getByRole("heading", { name: "Familjer", exact: true }).waitFor();
  // Stacked cells include their CSS label prefix in the accessible name.
  // Match the value at the end while retaining the table-cell boundary.
  const releases = main.getByRole("button", { name: "Releases", exact: true });
  check((await releases.getAttribute("aria-expanded")) === "false", "releases must start collapsed");
  check((await main.getByRole("cell", { name: /fam 2021$/ }).count()) === 0, "collapsed releases mounted their entries");
  const findings = main.locator("section").filter({ has: page.getByRole("heading", { name: "Findings", exact: true }) });
  const code = findings.getByRole("button", { name: /^Notice:/ });
  check((await code.getAttribute("aria-expanded")) === "false", "finding codes must start collapsed");
  await releases.click();
  await main.getByRole("cell", { name: /fam 2021$/ }).waitFor();
  await main.getByRole("cell", { name: /^(?:Primary )?Yes$/ }).first().waitFor();
  await code.click();
  await findings.getByText("Familjebegreppet ändrades 2021.", { exact: true }).waitFor();
  // This release notice has no subject.fqid. Rendering only fqid loses its
  // release and table, even though the message remains visible.
  const notice = findings.getByRole("listitem").filter({ hasText: "Familjebegreppet ändrades 2021." });
  await notice.getByText("Release: fam 2021", { exact: true }).waitFor();
  check(
    (await notice.getByRole("link", { name: "Familjer", exact: true }).getAttribute("href")) === "/catalog/demo/hush/fam",
    "the release finding lost its table page link",
  );
  await shoot("browse-table");
  await code.focus();
  await code.press("Enter");
  check((await code.getAttribute("aria-expanded")) === "false", "keyboard activation did not collapse the finding code");
}

// The project + artifact → logical order boundary, through the loaded-project UI.
// Fails if opening a file does not reach catalog validation, ordering loses a
// binding, the banner loses save/verdict state, reload loses the draft, a blocked
// order loses its named findings, or a structurally invalid file is accepted.
// Names and spellings are from webapp-dev's readable JSONL; the invalid type and
// located refusal code come from reg-core's binding_type_null corpus case. No
// existing browser case exercises project files or the order page.
async function projectPagesCase(page, shoot) {
  const main = page.getByRole("main");
  const projectFile = new URL(
    "../../conformance/cases/fixtures/webapp-dev/project_data.json",
    import.meta.url,
  );
  const invalidProject = JSON.parse(readFileSync(projectFile, "utf8"));
  invalidProject.sources[0].bindings[0].type = null;
  const invalidRaw = JSON.stringify(invalidProject);
  const banner = page.getByRole("region", { name: "Project", exact: true });
  async function openProjectFile(file, replacing = false) {
    const chooser = page.waitForEvent("filechooser");
    await main.getByRole("button", { name: "Open project file", exact: true }).click();
    await (await chooser).setFiles(file);
    if (replacing) {
      const replacement = page.getByRole("alertdialog", { name: "Replace the working project?", exact: true });
      await replacement.getByRole("button", { name: "Replace project", exact: true }).click();
    }
  }
  await openProjectFile(projectFile.pathname);
  await main.getByRole("button", { name: "Check catalog", exact: true }).click();
  await main.getByText("Project is valid against this catalog.", { exact: true }).waitFor();
  check(/1 source,\s*2 columns/.test(await banner.innerText()), "the banner lost the loaded project's source/column summary");
  await main.getByRole("textbox", { name: "Project name", exact: true }).fill("Baseline study revised");
  await banner.getByText("unsaved", { exact: true }).waitFor();
  const savedProject = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download project_data.json", exact: true }).click();
  check(JSON.parse(readFileSync(await (await savedProject).path(), "utf8")).name === "Baseline study revised",
    "the project download lost the edited project name");
  check((await banner.getByText("unsaved", { exact: true }).count()) === 0, "downloading did not clear the banner's unsaved state");
  await settled(page);
  await shoot("project-valid");

  // The draft is restored from local storage across a complete document load,
  // rather than surviving only because routing leaves the JS store alive.
  await page.reload();
  await settled(page);
  await main.getByRole("link", { name: "Order", exact: true }).click();
  await page.waitForURL("**/project?tab=order");
  await settled(page);
  const entries = main.getByRole("region", { name: "Entries", exact: true });
  for (const { variable, display, spelling } of [
    { variable: "Kön", display: "sex", spelling: "KON" },
    { variable: "Ålder", display: "age", spelling: "Alder" },
  ]) {
    const row = entries.getByRole("row").filter({
      has: page.getByRole("link", { name: variable, exact: true }),
    });
    await row.waitFor();
    check((await row.count()) === 1, `order did not list one ${variable} entry`);
    await row.getByText(spelling, { exact: true }).waitFor();
    check((await row.innerText()).includes("2020"), `${variable} order entry lost its period`);
    await row.getByRole("button", { name: `Details for ${variable}, 2020`, exact: true }).click();
    await page.getByRole("dialog").getByText(display, { exact: true }).waitFor();
    await page.keyboard.press("Escape");
    await page.getByRole("dialog").waitFor({ state: "hidden" });
  }
  await shoot("project-order");

  await main.getByRole("navigation", { name: "Project pages" }).getByRole("link", { name: "Project", exact: true }).click();
  // Bransch occurs only in Hushållsregistret's Individer table in webapp-dev.
  // Keeping the same register passes WASM, then pins the catalog's located
  // variable_not_in_table refusal (order/window.rs), including table:null refs.
  const blockedProject = JSON.parse(readFileSync(projectFile, "utf8"));
  blockedProject.sources[0] = { ...blockedProject.sources[0], table: "demo/hush/fam", period: 2021,
    bindings: [{ variable: "demo/hush/bransch", type: "categorical", display_name: "industry" }] };
  await openProjectFile({ name: "blocked-project.json", mimeType: "application/json", buffer: Buffer.from(JSON.stringify(blockedProject)) }, true);
  await main.getByRole("link", { name: "Order", exact: true }).click();
  await main.getByText("The order is blocked. Review the findings and update the project.", { exact: true }).waitFor();
  const findings = main.locator("section").filter({ has: page.getByRole("heading", { name: "Findings", exact: true }) });
  const group = findings.getByRole("button", { name: /Needs review: catalog finding/ }).first();
  await group.hover();
  await page.getByRole("tooltip").getByText("variable_not_in_table", { exact: true }).waitFor();
  await group.click();
  await findings.getByRole("link", { name: "Bransch", exact: true }).first().waitFor();
  await findings.getByRole("link", { name: "Familjer", exact: true }).first().waitFor();
  check((await findings.innerText()).includes("baseline"), "the blocked order lost its source location");
  check(!/demo\//.test(await findings.innerText()), "the blocked order exposed an internal catalog ref");
  await banner.getByRole("link", { name: "Order blocked", exact: true }).waitFor();

  await main.getByRole("navigation", { name: "Project pages" }).getByRole("link", { name: "Project", exact: true }).click();
  await openProjectFile({ name: "invalid-project.json", mimeType: "application/json", buffer: Buffer.from(invalidRaw) }, true);
  const issues = main.locator("section").filter({
    has: page.getByRole("heading", { name: "Draft check", exact: true }),
  });
  await issues.getByText("Source: baseline", { exact: true }).waitFor();
  await issues.getByText("Variable: Kön", { exact: true }).waitFor();
  await issues.getByText("Period: 2020", { exact: true }).waitFor();
  // Rejected documents survive reload and download byte-for-byte; accepting or
  // normalizing one during persistence would lose the original invalid input.
  await page.reload();
  await settled(page);
  await issues.getByText("Source: baseline", { exact: true }).waitFor();
  await issues.getByText("Variable: Kön", { exact: true }).waitFor();
  await issues.getByText("Period: 2020", { exact: true }).waitFor();
  const issueTitle = issues.getByRole("button").first();
  await issueTitle.focus();
  await issueTitle.press("Tab");
  await page.keyboard.press("Shift+Tab");
  await page.getByRole("tooltip").getByText("invalid_field_type", { exact: true }).waitFor();
  await page.keyboard.press("Escape");
  await page.getByRole("tooltip").waitFor({ state: "hidden" });
  const downloaded = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download project_data.json", exact: true }).click();
  check(readFileSync(await (await downloaded).path(), "utf8") === invalidRaw,
    "the rejected project changed during persistence or download");
  check((await banner.getByText("unsaved", { exact: true }).count()) === 0, "downloading the rejected file did not clear its unsaved state");
  await shoot("project-invalid");
}

// #2056, browser boundary over webapp-dev's Antal occurrences, provider text
// and variable notice. Fails if variable dispatch or cross-table segments are
// lost, named table links/storage are lost in the compact layout, evidence is
// fetched eagerly, or reopening refetches it. Existing cases
// only browse register/table pages and never request variable evidence.
async function browseVariableCase(page, shoot) {
  const evidenceRequests = [];
  page.on("request", (request) => {
    if (new URL(request.url()).searchParams.get("evidence") === "true") {
      evidenceRequests.push(request.url());
    }
  });
  const main = page.getByRole("main");
  await main.locator('a[href="/catalog/demo/hush/antal"]').click();
  await page.waitForURL("**/catalog/demo/hush/antal");
  await settled(page);
  const segments = main.locator("section").filter({ has: page.getByRole("heading", { name: "Segments", exact: true }) });
  await segments.getByRole("link", { name: "Familjer", exact: true }).waitFor();
  await segments.getByRole("link", { name: "Individer", exact: true }).waitFor();
  await segments.getByRole("cell", { name: /^(?:Storage )?int$/ }).first().waitFor();
  check(
    (await segments.getByRole("row").count()) >= 3,
    "the variable lost its segments across the family and individual tables",
  );
  const source = main.getByRole("button", { name: "Source and provenance", exact: true });
  check((await source.getAttribute("aria-expanded")) === "false", "source and provenance must start collapsed");
  check(evidenceRequests.length === 0, "variable evidence was fetched before opening its disclosure");
  await shoot("browse-variable");
  await source.click();
  await settled(page);
  check(evidenceRequests.length === 1, "opening source and provenance must issue exactly one evidence request");
  const sourcePanel = main.locator(`[id="${await source.getAttribute("aria-controls")}"]`);
  await sourcePanel.getByRole("button", { name: /^Provider wording/ }).first().click();
  await sourcePanel.getByText("Antal personer i hushållet vid årets slut.", { exact: true }).waitFor();
  const findings = main.locator("section").filter({ has: page.getByRole("heading", { name: "Findings", exact: true }) });
  await findings.getByRole("button", { name: /^Notice: source notice/ }).click();
  await findings.getByText("Antal redovisas på både individ- och familjetabellen.", { exact: true }).waitFor();
  await shoot("browse-variable-evidence");
  await source.click();
  await source.click();
  await settled(page);
  check(evidenceRequests.length === 1, "reopening source and provenance refetched cached evidence");
}

/** Whether no palette menu is mounted, given a moment to leave. */
async function menuGone(page) {
  try {
    await page.locator('[role="menu"]').waitFor({ state: "detached", timeout: 2000 });
    return true;
  } catch {
    return false;
  }
}

// Each viewport gets fresh storage and its own page-error ledger.
async function runCase(viewport, name, body, route) {
  const label = `${viewport.width}x${viewport.height}`;
  // Single-process Chromium cannot survive closing its last context. Give each
  // case its own process as well as its own storage (#1049).
  const browser = await launchBrowser();
  const context = await browser.newContext({ viewport });
  const page = await context.newPage();
  const pageErrors = logPageOutput(page);
  try {
    await open(page, route);
    await settled(page);
    await body(page, (shot) => capture(page, `${shot}-${label}`));
    check(
      pageErrors.length === 0,
      `${name} ${label}: page errors: ${pageErrors.join(" | ")}`,
    );
    console.log(`flows: OK ${name} ${label} route=${route}`);
  } catch (e) {
    console.log(`flows: FAIL ${name} ${label} route=${route} — ${e.message}`);
    throw e;
  } finally {
    await context.close();
    await browser.close();
  }
}

mkdirSync(outDir, { recursive: true });
const scenarios = {
  "browse-variable": {
    shots: 2,
    route: "/catalog/demo/hush",
    run: browseVariableCase,
  },
  "project-pages": {
    shots: 3,
    route: "/project",
    run: projectPagesCase,
  },
  "browse-pages": {
    shots: 2,
    route: "/catalog/demo/hush",
    run: browsePagesCase,
  },
  "browse-search": {
    shots: 2,
    route: "/catalog/demo/bas",
    run: browseSearchCase,
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
