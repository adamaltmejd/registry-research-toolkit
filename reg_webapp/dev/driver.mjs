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
    // Detail panels have their own filters; the shell and portalled palette
    // still share exactly one field. Fails if the palette adds a second one.
    (await page.getByRole("searchbox").evaluateAll((fields) => fields.filter((field) => !field.closest("main")).length)) === 1,
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
  // The tables are chips over the variable list (maintainer 2026-11-03), not
  // links: selecting one narrows the list, never the page.
  const tables = main.getByRole("group", { name: "Tables", exact: true });
  await tables.getByRole("checkbox", { name: /^Individer \d/ }).waitFor();
  const variables = main.getByRole("list", { name: "Variables", exact: true }).getByRole("listitem");
  const all = await variables.count();
  // RAC's input is visually hidden inside the chip's label: press the label.
  const familyName = { name: /^Familjer \d/ };
  const family = tables.getByRole("checkbox", familyName);
  await tables.locator("label").filter({ has: page.getByRole("checkbox", familyName) }).click();
  check(await family.isChecked(), "the table chip did not select");
  await main.getByText(new RegExp(`^\\d+ of ${all} shown$`)).waitFor();
  check((await variables.count()) < all, "a table chip did not narrow the variable list");
  await shoot("browse-register");
  await open(page, "/catalog/demo/hush/fam");
  await settled(page);
  await main.getByRole("heading", { name: "Familjer", exact: true }).waitFor();
  // Stacked cells include their CSS label prefix in the accessible name.
  // Match the value at the end while retaining the table-cell boundary.
  // One "Releases" heading: the panel's own, with the entries always shown.
  check((await main.getByRole("heading", { name: "Releases", exact: true }).count()) === 1, "Releases heading is duplicated");
  const findings = main.locator("section").filter({ has: page.getByRole("heading", { name: "Findings", exact: true }) });
  const code = findings.getByRole("button", { name: /^Notice:/ });
  check((await code.getAttribute("aria-expanded")) === "false", "finding codes must start collapsed");
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

  // A saved window wider than the catalog opens clamped to its span (#1037); the
  // download is the project the user would hand on. Fails if open stops clamping.
  const span = (await page.evaluate(() => fetch("/api/context").then((r) => r.json()))).data.period_span;
  const spanEnd = new Date(`${span.end}T00:00:00Z`);
  spanEnd.setUTCDate(spanEnd.getUTCDate() - 1);
  const spanYears = [Number(span.start.slice(0, 4)), spanEnd.getUTCFullYear()];
  const wide = JSON.parse(readFileSync(projectFile, "utf8"));
  wide.window = { from: 1900, to: 2200 };
  await openProjectFile(
    { name: "wide.json", mimeType: "application/json", buffer: Buffer.from(JSON.stringify(wide)) },
    true,
  );
  const wideDownload = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download project_data.json", exact: true }).click();
  const clamped = JSON.parse(readFileSync(await (await wideDownload).path(), "utf8")).window;
  check(clamped?.from === spanYears[0] && clamped.to === spanYears[1],
    `the opened project's window ${JSON.stringify(clamped)} was not clamped to the catalog span ${spanYears}`);
}

// Browser boundary: group links, member lanes and member navigation.
// Fails if groups lose their namespaced route, members leave API (authored)
// order, a lane title stops joining its varying facets in curated axis order
// (unit, then measure), facet labels become raw values, the member name stops
// telling lanes apart, or the synthetic set ancestor becomes a link.
// Expected titles and order come from webapp-dev's authored set and member
// facets (an unlabelled unit shows its value) in the member order the
// show-set conformance case pins; existing browse cases do not open a group.
async function browseGroupCase(page, shoot) {
  const main = page.getByRole("main");
  await main.getByRole("link", { name: "Inkomstmått", exact: true }).click();
  await page.waitForURL("**/catalog/set/demo/bas/inkomst");
  await settled(page);
  await main.getByRole("heading", { name: "Inkomstmått", level: 1, exact: true }).waitFor();
  await main.getByText("Inkomst före och efter skatt för personer och hushåll.", { exact: true }).waitFor();
  const lanes = main.getByRole("list", { name: "Group members", exact: true });
  await lanes.getByRole("link").first().waitFor();
  const titles = await lanes.getByRole("link").allTextContents();
  check(JSON.stringify(titles) === JSON.stringify([
    "household, Före skatt",
    "household, Efter skatt",
    "person, Före skatt",
    "person, Efter skatt",
  ]),
    `group lanes lost authored order or facet titles: ${JSON.stringify(titles)}`);
  await lanes.getByText("Disponibel inkomst, hushåll", { exact: true }).waitFor();
  check(await main.locator('nav a[href="/catalog/set"]').count() === 0, "group breadcrumb links to a synthetic set ancestor");
  await shoot("browse-group");
  await lanes.locator('a[href="/catalog/demo/bas/net-household"]').click();
  await page.waitForURL("**/catalog/demo/bas/net-household");
}

// Project + catalog → saved project boundary. Fails if changing a binding's
// type keeps its old subtype, a keyboard period edit is not saved, or removing
// a lane loses focus or removes the wrong binding. Existing cases open files
// and add columns; none edits a source card. Names and initial values come from
// webapp-dev/project_data.json; the expected year-int range follows one ArrowRight
// on the To year thumb of its 2020 period. The fixture has no classification refs.
async function sourceCardsCase(page, shoot) {
  const main = page.getByRole("main");
  const projectFile = new URL(
    "../../conformance/cases/fixtures/webapp-dev/project_data.json",
    import.meta.url,
  );
  const chooser = page.waitForEvent("filechooser");
  await main.getByRole("button", { name: "Open project file", exact: true }).click();
  await (await chooser).setFiles(projectFile.pathname);
  await settled(page);

  await main.getByRole("button", { name: /Type for Ålder in baseline/ }).click();
  await page.getByRole("option", { name: "Opaque", exact: true }).click();
  const period = main.getByRole("group", { name: "Period for baseline (years)", exact: true });
  const toYear = period.getByRole("slider", { name: /^To year/ });
  await toYear.focus();
  await toYear.press("ArrowRight");
  await settled(page);
  await shoot("source-cards-edited");

  const sources = main.getByRole("region", { name: "Project sources", exact: true });
  await sources.getByRole("button", { name: "Remove KON from baseline", exact: true }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Remove column", exact: true }).click();
  await page.getByRole("alertdialog").waitFor({ state: "hidden" });
  await settled(page);
  check(await sources.getByRole("link", { name: "Kön", exact: true }).count() === 0,
    "removing Kön left its lane in the source");
  await sources.getByRole("link", { name: "Ålder", exact: true }).waitFor();
  check(await sources.evaluate((node) => node.contains(document.activeElement)),
    "removing the lane lost focus outside the remaining source");

  const downloaded = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download project_data.json", exact: true }).click();
  const saved = JSON.parse(readFileSync(await (await downloaded).path(), "utf8"));
  const source = saved.sources[0];
  check(saved.sources.length === 1 && source.bindings.length === 1 &&
    source.bindings[0].variable === "demo/bas/alder" && source.bindings[0].type === "opaque" &&
    !("numeric_subtype" in source.bindings[0]),
  "the downloaded source lost Ålder's type edit, kept its numeric subtype, or removed the wrong binding");
  check(source.period !== null && !Array.isArray(source.period) &&
    typeof source.period === "object" && source.period.from === 2020 && source.period.to === 2021,
  "the keyboard period edit did not save the year-int range 2020–2021");
  await shoot("source-cards-removed");
}

// Open webapp-dev's fixture project with the 2021–2022 study window from the
// Project page. The app reads the chosen file asynchronously, so a navigation
// straight after setFiles can unload the page before the draft is stored. The
// catalog page then has no project: the add window still renders but no column
// is selectable (#2120). Wait for the opened project's sources before returning.
async function openWindowedFixture(page, name) {
  const main = page.getByRole("main");
  const fixture = JSON.parse(readFileSync(new URL(
    "../../conformance/cases/fixtures/webapp-dev/project_data.json",
    import.meta.url,
  ), "utf8"));
  fixture.window = { from: 2021, to: 2022 };
  const chooser = page.waitForEvent("filechooser");
  await main.getByRole("button", { name: "Open project file", exact: true }).click();
  await (await chooser).setFiles({
    name,
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(fixture)),
  });
  await main.getByRole("region", { name: "Project sources", exact: true }).waitFor();
  return fixture;
}

// Project + catalog → saved project boundary. Fails if a variable-page add
// confuses identical spellings in different tables, loses the binding/type or
// add-window years, retains a tick outside the moved window,
// leaves an already-bound column selectable, or strands a failed register read
// without a working Retry. The older
// project case opens complete files; none adds a column from the catalog.
// Names, tables and numeric type are from webapp-dev.jsonl; the window is this
// readable fixture project's explicit study window and the stated thumb move.
async function addVariableColumnsCase(page, shoot) {
  const main = page.getByRole("main");
  const fixture = await openWindowedFixture(page, "study-window.json");
  let failRegister = true;
  let registerRequests = 0;
  const registerRoute = "**/api/catalog/demo/hush";
  const readRegister = async (route) => {
    registerRequests += 1;
    if (failRegister) await route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({error:{message:"Register unavailable"}}) });
    else await route.continue();
  };
  await page.route(registerRoute, readRegister);
  await open(page, "/catalog/demo/hush/antal");
  const registerFailure = main.getByRole("alert").filter({hasText:"Could not load the register's tables."});
  await registerFailure.waitFor();
  check(await main.getByRole("checkbox").count() === 0, "the add path remained exposed without table metadata");
  failRegister = false;
  await registerFailure.getByRole("button", {name:"Retry", exact:true}).click();
  await settled(page);
  await main.getByRole("group", {name:"Add window (years)",exact:true}).waitFor();
  check(registerRequests >= 2, "Retry reused the failed register request");
  await page.unroute(registerRoute, readRegister);
  const familyColumn = main.getByRole("checkbox", { name: /Antal.*Familjer/ });
  const individualColumn = main.getByRole("checkbox", { name: /Antal.*Individer/ });
  const addWindow = main.getByRole("group", { name: "Add window (years)", exact: true });
  const fromYear = addWindow.getByRole("slider", { name: /^From year/ });
  await individualColumn.focus();
  await individualColumn.press("Space");
  await fromYear.press("ArrowRight");
  await individualColumn.waitFor({ state: "detached" });
  check((await main.getByRole("button", { name: /^Add \d+ columns? to project$/ }).count()) === 0,
    "moving the add window kept a column with no overlapping delivery staged");
  await fromYear.press("ArrowLeft");
  check(!(await individualColumn.isChecked()), "returning to a column's delivery years restored its dropped tick");
  await fromYear.press("ArrowRight");
  await familyColumn.focus();
  await familyColumn.press("Space");
  check(await familyColumn.isChecked(), "the column did not tick from the keyboard");
  check((await individualColumn.count()) === 0, "the column outside the add window remained selectable");
  await shoot("add-variable-selected");
  await main.getByRole("button", { name: "Add 1 column to project", exact: true }).click();
  await familyColumn.waitFor({ state: "detached" });
  await main.getByText(/Antal.*Familjer.*in project/i).waitFor();
  await main.getByText(/Antal.*Individer.*outside the add window/i).waitFor();
  check((await main.getByRole("button", { name: /^Add \d+ columns? to project$/ }).count()) === 0,
    "the add bar kept the committed selection staged");

  await open(page, "/project");
  await settled(page);
  const sources = main.getByRole("region", { name: "Project sources", exact: true });
  const source = sources.locator("section").filter({ has: page.getByRole("heading", { name: "fam", exact: true }) });
  await source.getByRole("link", { name: "Familjer", exact: true }).waitFor();
  await source.getByRole("link", { name: "Antal personer", exact: true }).waitFor();
  await source.getByText("Period: 2022", { exact: true }).waitFor();
  const downloaded = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download project_data.json", exact: true }).click();
  const saved = JSON.parse(readFileSync(await (await downloaded).path(), "utf8"));
  const added = saved.sources.filter((item) => item.table === "demo/hush/fam");
  check(added.length === 1 && added[0].bindings.length === 1 &&
    added[0].bindings[0].variable === "demo/hush/antal" && added[0].bindings[0].type === "numeric",
  "the downloaded project lost the selected table, binding or numeric type");
  check(saved.sources.length === fixture.sources.length + 1,
    "adding a column changed unrelated project sources");
  await shoot("add-variable-project");


}

// #2056, browser boundary over webapp-dev's Antal occurrences, provider text
// and variable notice. Fails if variable dispatch or the timeline's lane per
// table is lost, the storage fact is lost in the compact layout, evidence is
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
  const timeline = main.getByRole("list", { name: "Variable columns", exact: true });
  await timeline.getByText("Familjer", { exact: true }).waitFor();
  await timeline.getByText("Individer", { exact: true }).waitFor();
  await main.locator("header dd").filter({ hasText: /^int$/ }).first().waitFor();
  const source = main.getByRole("button", { name: "Source and provenance", exact: true });
  check((await source.getAttribute("aria-expanded")) === "false", "source and provenance must start collapsed");
  check(evidenceRequests.length === 0, "variable evidence was fetched before opening its disclosure");
  await shoot("browse-variable");
  await source.click();
  await settled(page);
  check(evidenceRequests.length === 1, "opening source and provenance must issue exactly one evidence request");
  const sourcePanel = main.locator(`[id="${await source.getAttribute("aria-controls")}"]`);
  await sourcePanel.getByRole("button", { name: /^Definition for/ }).first().click();
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

// Browser boundary over webapp-dev's undated Antal succession and its caveat.
// Fails if the graph filter is lost, succession lanes/legend disappear, or a
// neighbour becomes selectable, or a cross-register lane loses its register name.
// Existing browse cases do not check succession.
// The two focus columns and neighbour href come from webapp-dev.jsonl; the
// project window keeps both focus columns available, and names come from the page.
async function variableSuccessionCase(page, shoot) {
  const main = page.getByRole("main");
  await open(page, "/project");
  await settled(page);
  await openWindowedFixture(page, "succession-window.json");
  await open(page, "/catalog/demo/hush");
  await settled(page);
  const graphRequests = [];
  page.on("requestfinished", async (request) => {
    if (new URL(request.url()).pathname.startsWith("/api/graph/")) {
      const response = await request.response();
      if (response?.ok()) graphRequests.push(request.url());
    }
  });
  await main.locator('a[href="/catalog/demo/hush/antal"]').click();
  await page.waitForURL("**/catalog/demo/hush/antal");
  await settled(page);
  check(graphRequests.length === 1, "the variable must complete exactly one successful graph request");
  check(new URL(graphRequests[0]).searchParams.get("kind") === "continues",
    "the variable graph must request only succession relations");
  const lanes = main.getByRole("list", { name: "Variable columns", exact: true });
  const neighbour = lanes.locator('a[href="/catalog/demo/hush/antalgammal"]');
  await neighbour.waitFor();
  check((await neighbour.innerText()).trim().length > 0, "the neighbour lane lost its variable name");
  const crossRegisterLane = lanes.getByRole("listitem").filter({
    has: page.locator('a[href="/catalog/demo/bas/antalbas"]'),
  });
  await crossRegisterLane.getByText("Basregistret", { exact: true }).waitFor();
  await main.getByText("Succeeded by (definition change)", { exact: true }).waitFor();
  check(await lanes.getByRole("checkbox").count() === 2,
    "only the focus variable's two columns may be selectable");
  await shoot("variable-succession");
}

// Browser boundary over webapp-dev's kon and bransch value sets. Fails if
// variable disclosures lose their codes, q stops reaching the server, search
// leaves unrelated codes visible, or hierarchy expansion/flat search loses its
// children, levels or expansion after clearing search. Existing browse cases do
// not open value sets. Expected labels and hierarchy come from
// inputs/frame/webapp-dev.jsonl.
async function browseValuesCase(page, shoot) {
  const main = page.getByRole("main");
  await main.locator('a[href="/catalog/demo/bas/kon"]').click();
  await page.waitForURL("**/catalog/demo/bas/kon");
  await settled(page);
  const values = main.locator("section").filter({ has: page.getByRole("heading", { name: "Value sets", exact: true }) });
  await values.getByRole("button", { name: /^kon/ }).click();
  await settled(page);
  await values.getByText("Man", { exact: true }).waitFor();
  await values.getByText("Kvinna", { exact: true }).waitFor();
  await shoot("browse-values");
  const flatSearch = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname === "/api/values" && url.searchParams.get("q") === "Kvinna" && response.ok();
  });
  await values.getByRole("searchbox", { name: "Search codes", exact: true }).fill("Kvinna");
  await flatSearch;
  await values.getByText("Man", { exact: true }).waitFor({ state: "detached" });
  await values.getByText("Kvinna", { exact: true }).waitFor();
  await settled(page);
  // Fails if a successful server search omits its matching count.
  await values.getByText("1 of 3", { exact: true }).waitFor();
  await shoot("browse-values-search");

  await open(page, "/catalog/demo/hush/bransch");
  await settled(page);
  await values.getByRole("button", { name: /^bransch/ }).click();
  await settled(page);
  await values.getByRole("button").filter({ has: page.getByText("Jordbruk", { exact: true }) }).click();
  await values.getByText("Jordbruk, grupp 1", { exact: true }).waitFor();
  await settled(page);
  await shoot("browse-values-hierarchy");
  const hierarchySearch = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname === "/api/values" && url.searchParams.get("q") === "Jordbruk, grupp 1a" && response.ok();
  });
  await values.getByRole("searchbox", { name: "Search codes", exact: true }).fill("Jordbruk, grupp 1a");
  await hierarchySearch;
  await values.getByText("Jordbruk, grupp 1a", { exact: true }).waitFor();
  await values.getByText("Jordbruk, grupp 1", { exact: true }).waitFor({ state: "hidden" });
  await values.getByText(/level 3/i).waitFor();
  await settled(page);
  await shoot("browse-values-hierarchy-search");
  await values.getByRole("searchbox", { name: "Search codes", exact: true }).press("Escape");
  await values.getByText("Jordbruk, grupp 1", { exact: true }).waitFor();
  await settled(page);
}


// Fails if linked documents disappear, parsed text cannot expand inline, or a PDF
// routes through the deleted hash page. Expected text was read from the fixture
// server's /api/docs/doc endpoint; titles and source presence are fixture data.
async function linkedDocumentsCase(page, shoot) {
  const panel = page.getByRole("heading", { name: "Source documents", exact: true }).locator("..");
  // Fails if filtering leaves a matching section hidden in a collapsed container.
  // The expected title comes from the webapp-dev source frame.
  await panel.getByRole("searchbox", { name: "Filter source documents" }).fill("Beskrivning");
  const section = panel.getByRole("button", { name: "Beskrivning av Basregistret", exact: true });
  await section.waitFor({ state: "visible" });
  await section.click();
  await panel.getByText("Basregistret innehåller en post per person och år.", { exact: true }).waitFor();
  await settled(page);
  await shoot("linked-documents-parsed");
  await open(page, "/catalog/demo/hush");
  await settled(page);
  const pdfPanel = page.getByRole("heading", { name: "Source documents", exact: true }).locator("..");
  const pdf = pdfPanel.getByRole("link", { name: "Bilaga", exact: true });
  check(/^\/api\/docs\/file\/[0-9a-f]{64}$/.test(await pdf.getAttribute("href")), "PDF must link to its inline file endpoint");
  check((await pdfPanel.getByRole("link").count()) === 1, "PDF without a source URL must not invent a source link");
  await shoot("linked-documents-pdf");
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
// Paired catalog → browse/coverage/order UI boundary. Fails if pairing is ignored,
// held-only search drops supplemental facets, filtering hides their variables, the
// timeline stops drawing the held file, column and years, or received row keys
// disappear. Names, spellings and periods are from
// cases/holdings readable inputs and order-key-requirements.json; global cases
// cannot exercise this boundary because their artifact has no paired catalog.
async function stewardHeldCase(page, shoot) {
  const brand = page.locator("header").getByRole("link", { name: "Synthetic steward", exact: true });
  await brand.waitFor();
  check(await page.title() === "Synthetic steward", "paired title did not use steward branding");
  check(await page.locator('link[rel="icon"]').getAttribute("href")?.then((href) => href.startsWith("data:image/svg+xml,")), "paired favicon is missing");
  const narrow = !(await page.getByRole("button", { name: "Collapse all", exact: true }).isVisible());
  if (narrow) await page.getByRole("link", { name: "Browse registers", exact: true }).click();
  const search = page.getByRole("searchbox", { name: /^Search registers/ });
  if (narrow) await page.getByRole("button", { name: /^Search registers/ }).click();
  await search.fill("position");
  const tree = page.getByRole("treegrid", { name: "Registers", exact: true });
  const slots = tree.getByRole("row", { name: /^Delivery slots/ });
  await slots.waitFor();
  if (narrow) await slots.click();
  if (narrow) await page.getByRole("main").getByRole("link", { name: "Second position", exact: true }).waitFor();
  else await page.getByRole("option", { name: /Second position/ }).waitFor();
  await settled(page);
  await shoot("steward-filter");
  await page.keyboard.press("Escape");
  if (narrow) await page.getByRole("button", { name: /^Search registers/ }).click();
  // Debouncing has no network request or aria-busy region yet. Wait for both
  // current facet answers rather than capturing the previous query's matches.
  const lateFacets = [undefined, "6"].map((limit) => page.waitForResponse((response) => {
    const url = new URL(response.url());
    return response.ok() && url.pathname === "/api/search/facets" &&
      url.searchParams.get("q") === "Late" && !url.searchParams.has("register") &&
      url.searchParams.get("limit") === (limit ?? null);
  }));
  await search.fill("Late");
  await Promise.all(lateFacets);
  if (narrow) await page.getByRole("link", { name: "Show matching registers", exact: true }).click();
  const held = tree.getByRole("row", { name: /^Held reference metadata/ });
  await held.waitFor();
  if (narrow) await held.click();
  if (narrow) await page.getByRole("main").getByRole("link", { name: "Late measure", exact: true }).click();
  else await page.getByRole("option", { name: /Late measure/ }).click();
  await page.waitForURL("**/catalog/demo/held/late");
  await page.getByRole("main").getByText(/^Late, .*, Pool, 2019–2020,/).first().waitFor({ state: "attached" });
  await page.locator('[role="option"][data-key="steward/slots/slot2"]').waitFor({ state: "detached" });
  await settled(page);
  await shoot("steward-held");
  await open(page, "/project");
  const fixture = JSON.parse(readFileSync(new URL("../../conformance/cases/holdings/order-key-requirements.json", import.meta.url), "utf8"));
  const chooser = page.waitForEvent("filechooser");
  await page.getByRole("main").getByRole("button", { name: "Open project file", exact: true }).click();
  await (await chooser).setFiles({ name: "paired-project.json", mimeType: "application/json", buffer: Buffer.from(JSON.stringify(fixture.project)) });
  await page.getByRole("main").getByRole("link", { name: "Order", exact: true }).click();
  const rowKeys = page.locator("section").filter({ has: page.getByRole("heading", { name: "Row keys", exact: true }) });
  await rowKeys.getByText("Received_Id", { exact: true }).waitFor();
  await settled(page);
  await shoot("steward-order");
  // Supplemental-only variables order through the composed steward view (#2068),
  // so a project can select them like reference variables.
  await open(page, "/catalog/steward/slots/slot2");
  const slotColumn = page.getByRole("main").getByRole("checkbox").first();
  await slotColumn.waitFor();
  await slotColumn.focus();
  await slotColumn.press("Space");
  check(await slotColumn.isChecked(), "supplemental variable offers no project selection");
  await page.getByRole("main").getByRole("button", { name: /^Add \d+ columns? to project$/ }).waitFor();
  await settled(page);
  await shoot("steward-selectable");
  await open(page, "/catalog/demo/held/late");
  await page.getByRole("main").getByRole("checkbox").first().waitFor();
}

const scenarios = {
  "steward-held": {
    fixture: "paired",
    shots: 4,
    route: "/catalog/demo/held",
    run: stewardHeldCase,
  },
  "browse-group": {
    fixture: "webapp-dev",
    shots: 1,
    route: "/catalog/demo/bas/inkomst",
    run: browseGroupCase,
  },
  "variable-succession": {
    fixture: "webapp-dev",
    shots: 1,
    route: "/catalog/demo/hush",
    run: variableSuccessionCase,
  },
  "linked-documents": {
    fixture: "webapp-dev",
    shots: 2,
    route: "/catalog/demo/bas",
    run: linkedDocumentsCase,
  },
  "browse-values": {
    fixture: "webapp-dev",
    shots: 4,
    route: "/catalog/demo/bas",
    run: browseValuesCase,
  },
  "browse-variable": {
    fixture: "webapp-dev",
    shots: 2,
    route: "/catalog/demo/hush",
    run: browseVariableCase,
  },
  "project-pages": {
    fixture: "webapp-dev",
    shots: 3,
    route: "/project",
    run: projectPagesCase,
  },
  "add-variable-columns": {
    fixture: "webapp-dev",
    shots: 2,
    route: "/project",
    run: addVariableColumnsCase,
  },
  "source-cards": {
    fixture: "webapp-dev",
    shots: 2,
    route: "/project",
    run: sourceCardsCase,
  },
  "browse-pages": {
    fixture: "webapp-dev",
    shots: 2,
    route: "/catalog/demo/hush",
    run: browsePagesCase,
  },
  "browse-search": {
    fixture: "webapp-dev",
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
// No names = the selected fixture's scenarios; explicit names select a subset.
const fixture = process.env.REG_WEBAPP_FIXTURE ?? "webapp-dev";
const names = selection.length > 0 ? selection : Object.keys(scenarios).filter((name) => scenarios[name].fixture === fixture);
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
