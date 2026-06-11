import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import PeriodEditor from "./PeriodEditor.svelte";
import type { Period } from "./project_data";

// Scenario 3 (#201, updated by #200): the real bugs this covers all reached merge
// once — a period defaulting to free text, a cross-source radio-group collision, and
// (the #188 symptom) stale state after a middle-source remove, now fixed at the root
// by stable each-block keys (#200) rather than the removed re-seed workaround.
// PeriodEditor is self-contained (props in, `onchange` out), so these drive it
// directly.
describe("PeriodEditor", () => {
  it("switches modes and emits the right Period shape per mode", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    // A fresh source seeds `period: ""` → YEARS mode by default (year-range-first).
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });

    await expect
      .element(screen.getByRole("radio", { name: "Years" }))
      .toBeChecked();
    await expect
      .element(screen.getByRole("spinbutton", { name: "From" }))
      .toBeVisible();

    // → Token: emits the trimmed text (empty here), swaps in the token field.
    await screen.getByRole("radio", { name: "Token" }).click();
    await expect.element(screen.getByRole("textbox")).toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("");

    // → Default: emits the "_default" sentinel.
    await screen.getByRole("radio", { name: "Default" }).click();
    await expect.element(screen.getByText(/snapshot sentinel/)).toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("_default");

    // → Years: blank spinners emit a {from,to} of raw strings (backend flags them).
    await screen.getByRole("radio", { name: "Years" }).click();
    await expect
      .element(screen.getByRole("spinbutton", { name: "To" }))
      .toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith({ from: "", to: "" });
  });

  it("emits a bare year int when From === To", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });
    await screen.getByRole("spinbutton", { name: "From" }).fill("2015");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2015");
    expect(onchange).toHaveBeenLastCalledWith(2015);
  });

  it("isolates radio groups across multiple instances (no cross-source collision)", async () => {
    // Two editors on one page. A shared radio `name` would make them ONE document
    // group, so selecting a mode in one would uncheck the other — the bug the
    // per-instance group name fixes. (locators query the whole page, so target the
    // two instances positionally with .nth().)
    await render(PeriodEditor, { period: "", issues: [], onchange: vi.fn() });
    await render(PeriodEditor, { period: 2020, issues: [], onchange: vi.fn() });

    // The mechanism: each instance's radio group has a DISTINCT `name`.
    const yearsRadios = page.getByRole("radio", { name: "Years" }).elements();
    expect(yearsRadios).toHaveLength(2);
    const names = yearsRadios.map((el) => el.getAttribute("name"));
    expect(names[0]).toBeTruthy();
    expect(names[0]).not.toBe(names[1]);

    // The behavior: switching instance A → Token leaves instance B's Years checked.
    await expect
      .element(page.getByRole("radio", { name: "Years" }).nth(1))
      .toBeChecked();
    await page.getByRole("radio", { name: "Token" }).nth(0).click();
    await expect
      .element(page.getByRole("radio", { name: "Years" }).nth(1))
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
      .element(screen.getByRole("radio", { name: "Years" }))
      .toBeChecked();

    // The user switches THIS source to Token mode (local-only until they type).
    await screen.getByRole("radio", { name: "Token" }).click();
    await expect.element(screen.getByRole("textbox")).toBeVisible();

    // An unrelated edit elsewhere re-renders the parent; this instance survives on
    // its stable key and receives its OWN period prop again. The one-time seed must
    // NOT re-run — the mode stays Token (no snap-back to Years).
    await screen.rerender({ period: 2015, issues: [], onchange });
    await expect
      .element(screen.getByRole("radio", { name: "Token" }))
      .toBeChecked();
  });

  it("opens a #307 list period in Token mode showing the comma wire, and comma text emits the list", async () => {
    // The MINIMAL interrupted-series affordance (#307): a list period must be
    // VISIBLE and editable as comma-joined token text (not a silently blanked
    // field), and comma text typed in Token mode must emit the segment LIST —
    // the dedicated picker mode is #308.
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: [
        { from: 2005, to: 2010 },
        { from: 2015, to: 2020 },
      ],
      issues: [],
      onchange,
    });

    await expect
      .element(screen.getByRole("radio", { name: "Token" }))
      .toBeChecked();
    await expect
      .element(screen.getByRole("textbox"))
      .toHaveValue("2005..2010,2015..2020");

    await screen.getByRole("textbox").fill("2005..2010,2013");
    expect(onchange).toHaveBeenLastCalledWith([{ from: 2005, to: 2010 }, 2013]);
  });
});
