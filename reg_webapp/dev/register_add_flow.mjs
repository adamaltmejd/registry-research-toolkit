import { readFileSync } from "node:fs";

// Project + catalog → saved project boundary. Fails if register browsing fetches
// every row's segments, a row tick adds only one of its delivery tables, an
// outside-window or already-bound variable stays selectable, or adding strands
// focus on a removed control. The variable-page case selects individual columns;
// this case pins a whole row's cross-table selection. Names, years and numeric
// types come from webapp-dev.jsonl; the study window is set explicitly below.
export async function addRegisterVariablesCase(page, shoot, { open, settled, check }) {
  const main = page.getByRole("main");
  const fixture = JSON.parse(readFileSync(new URL(
    "../../conformance/cases/fixtures/webapp-dev/project_data.json",
    import.meta.url,
  ), "utf8"));
  fixture.window = { from: 2021, to: 2022 };
  const chooser = page.waitForEvent("filechooser");
  await main.getByRole("button", { name: "Open project file", exact: true }).click();
  await (await chooser).setFiles({
    name: "register-study-window.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(fixture)),
  });

  const segmentRequests = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname.startsWith("/api/segments/"))
      segmentRequests.push(request.url());
  });
  await open(page, "/catalog/demo/hush");
  await settled(page);
  const variables = main.locator("section").filter({
    has: page.getByRole("heading", { name: "Variables", exact: true }),
  });
  const older = variables.getByRole("listitem").filter({
    has: page.getByRole("link", { name: "Antal personer, äldre", exact: true }),
  });
  await older.waitFor();
  check(await older.getByRole("checkbox").count() === 0,
    "the 2016–2017 variable remained selectable in the 2021–2022 study window");
  check(segmentRequests.length === 0, "register browsing fetched segments before a row was ticked");

  const antal = variables.getByRole("listitem").filter({
    has: page.getByRole("link", { name: "Antal personer", exact: true }),
  });
  const tick = antal.getByRole("checkbox", { name: "Add Antal personer", exact: true });
  await tick.focus();
  await tick.press("Space");
  check(await tick.isChecked(), "the variable row did not tick from the keyboard");
  const add = main.getByRole("button", { name: "Add 2 columns to project", exact: true });
  await add.waitFor();
  await settled(page);
  check(segmentRequests.length === 1, "one row tick did not make exactly one segment request");
  check(await add.isEnabled(), "the loaded variable columns cannot be added");
  await shoot("add-register-selected");
  await add.click();
  await tick.waitFor({ state: "detached" });
  await antal.getByText(", in the project", { exact: true }).waitFor();
  check(await main.evaluate((element) => element.contains(document.activeElement)),
    "adding the variable left focus outside the detail pane");
  check(await main.getByRole("button", { name: /^Add \d+ columns? to project$/ }).count() === 0,
    "the register add bar retained the committed selection");

  await open(page, "/project");
  await settled(page);
  const downloaded = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download project_data.json", exact: true }).click();
  const saved = JSON.parse(readFileSync(await (await downloaded).path(), "utf8"));
  const expected = [
    { table: "demo/hush/fam", period: { from: 2021, to: 2022 } },
    { table: "demo/hush/ind", period: 2021 },
  ];
  for (const { table, period } of expected) {
    const sources = saved.sources.filter((source) => source.table === table);
    check(sources.length === 1 && sources[0].bindings.length === 1 &&
      sources[0].bindings[0].variable === "demo/hush/antal" &&
      sources[0].bindings[0].type === "numeric" &&
      JSON.stringify(sources[0].period) === JSON.stringify(period),
    `the register add lost ${table}'s sole binding, numeric type or delivery period`);
  }
  const unrelated = saved.sources.filter((source) =>
    !expected.some(({ table }) => source.table === table));
  check(JSON.stringify(unrelated) === JSON.stringify(fixture.sources),
    "the register add changed unrelated sources");
  await shoot("add-register-project");
}
