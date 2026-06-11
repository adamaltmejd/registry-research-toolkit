import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import PeriodEditor from "./PeriodEditor.svelte";
import type { Period } from "./project_data";

// Scenario 3 (#201, updated by #200 and the #308 range-first rework): the real
// bugs this covers all reached merge once — a period defaulting to free text, a
// cross-source radio-group collision, and (the #188 symptom) stale state after
// a middle-source remove, now fixed at the root by stable each-block keys
// (#200) rather than the removed re-seed workaround. PeriodEditor is
// self-contained (props in, `onchange` out), so these drive it directly.
describe("PeriodEditor", () => {
  it("switches modes and emits the right Period shape per mode", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    // A fresh source seeds `period: ""` → RANGE mode by default (range-first,
    // #308) at the year grain.
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });

    await expect
      .element(screen.getByRole("radio", { name: "Range" }))
      .toBeChecked();
    await expect
      .element(screen.getByRole("spinbutton", { name: "From" }))
      .toBeVisible();

    // → Token: emits through periodFromWire (empty → the unset ""), swaps in
    // the token field.
    await screen.getByRole("radio", { name: "Token" }).click();
    await expect.element(screen.getByRole("textbox")).toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("");

    // → Default: emits the "_default" sentinel.
    await screen.getByRole("radio", { name: "Default" }).click();
    await expect.element(screen.getByText(/snapshot sentinel/)).toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("_default");

    // → Range: an incomplete selection emits the UNSET "" (the amber
    // incomplete hint shows; never a malformed {from:"",to:""}).
    await screen.getByRole("radio", { name: "Range" }).click();
    await expect
      .element(screen.getByRole("spinbutton", { name: "To" }))
      .toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("");
    await expect.element(screen.getByText(/complete the period/)).toBeVisible();
  });

  it("emits a bare year int from a single From (To falls back to From)", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });
    await screen.getByRole("spinbutton", { name: "From" }).fill("2015");
    expect(onchange).toHaveBeenLastCalledWith(2015);
    // Filling To produces the {from,to} OBJECT (the only schema-valid range
    // shape — never a raw "a..b" string).
    await screen.getByRole("spinbutton", { name: "To" }).fill("2020");
    expect(onchange).toHaveBeenLastCalledWith({ from: 2015, to: 2020 });
  });

  it("the term grain emits VT/HT tokens; a term range emits the token-endpoint object", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });
    await screen
      .getByRole("combobox", { name: "Granularity" })
      .selectOptions("Term (VT/HT)");
    await screen.getByRole("spinbutton", { name: "From" }).fill("2018");
    expect(onchange).toHaveBeenLastCalledWith("VT2018");
    await screen
      .getByRole("combobox", { name: "From term" })
      .selectOptions("HT (autumn)");
    expect(onchange).toHaveBeenLastCalledWith("HT2018");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2019");
    expect(onchange).toHaveBeenLastCalledWith({ from: "HT2018", to: "VT2019" });
  });

  it("a stored token opens in Range mode at its grain; a mixed-grain range opens in Token", async () => {
    // "HT2018" is range-representable → Range mode, term grain preselected.
    const screen = await render(PeriodEditor, {
      period: "HT2018",
      issues: [],
      onchange: vi.fn(),
    });
    await expect
      .element(screen.getByRole("radio", { name: "Range" }))
      .toBeChecked();
    await expect
      .element(screen.getByRole("combobox", { name: "Granularity" }))
      .toHaveValue("term");

    // A #306 succession clip ({from: 1992, to: "2009-06-30"}) is NOT
    // uniform-grain → Token mode, displayed as its wire text (never blanked).
    const screen2 = await render(PeriodEditor, {
      period: { from: 1992, to: "2009-06-30" },
      issues: [],
      onchange: vi.fn(),
    });
    await expect
      .element(screen2.getByRole("radio", { name: "Token" }).nth(1))
      .toBeChecked();
    await expect
      .element(screen2.getByRole("textbox"))
      .toHaveValue("1992..2009-06-30");
  });

  it("Token mode emits SCHEMA-VALID shapes through periodFromWire (a typed range is the object, not a raw string)", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });
    await screen.getByRole("radio", { name: "Token" }).click();
    await screen.getByRole("textbox").fill("2010..2020");
    expect(onchange).toHaveBeenLastCalledWith({ from: 2010, to: 2020 });
    await screen.getByRole("textbox").fill("HT2018");
    expect(onchange).toHaveBeenLastCalledWith("HT2018");
  });

  it("isolates radio groups across multiple instances (no cross-source collision)", async () => {
    // Two editors on one page. A shared radio `name` would make them ONE document
    // group, so selecting a mode in one would uncheck the other — the bug the
    // per-instance group name fixes. (locators query the whole page, so target the
    // two instances positionally with .nth().)
    await render(PeriodEditor, { period: "", issues: [], onchange: vi.fn() });
    await render(PeriodEditor, { period: 2020, issues: [], onchange: vi.fn() });

    // The mechanism: each instance's radio group has a DISTINCT `name`.
    const rangeRadios = page.getByRole("radio", { name: "Range" }).elements();
    expect(rangeRadios).toHaveLength(2);
    const names = rangeRadios.map((el) => el.getAttribute("name"));
    expect(names[0]).toBeTruthy();
    expect(names[0]).not.toBe(names[1]);

    // The behavior: switching instance A → Token leaves instance B's Range checked.
    await expect
      .element(page.getByRole("radio", { name: "Range" }).nth(1))
      .toBeChecked();
    await page.getByRole("radio", { name: "Token" }).nth(0).click();
    await expect
      .element(page.getByRole("radio", { name: "Range" }).nth(1))
      .toBeChecked();
  });

  it("seeds local state ONCE at mount and never snaps back on an unrelated prop change (#188/#200)", async () => {
    // #200 removed the #188 re-seed workaround: with stable each-block keys a
    // different source REMOUNTS a fresh PeriodEditor, so this instance never gets
    // REUSED for another source. The seed is therefore one-time at mount and a later
    // `period` prop change (an unrelated edit re-running the parent render with the
    // SAME stable key) must NOT snap local UI state back — the exact #188 symptom.
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: 2015,
      issues: [],
      onchange,
    });
    await expect
      .element(screen.getByRole("radio", { name: "Range" }))
      .toBeChecked();

    // The user switches THIS source to Token mode (local-only until they type).
    await screen.getByRole("radio", { name: "Token" }).click();
    await expect.element(screen.getByRole("textbox")).toBeVisible();

    // An unrelated edit elsewhere re-renders the parent; this instance survives on
    // its stable key and receives its OWN period prop again. The one-time seed must
    // NOT re-run — the mode stays Token (no snap-back to Range).
    await screen.rerender({ period: 2015, issues: [], onchange });
    await expect
      .element(screen.getByRole("radio", { name: "Token" }))
      .toBeChecked();
  });
});
