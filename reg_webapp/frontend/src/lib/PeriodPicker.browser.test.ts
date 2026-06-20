import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import type { Coverage } from "./period";
import type { StudyWindow } from "./project_data";

// The catalog PeriodPicker. The DEFAULT control is the #615 year-window
// availability slider (seeded from the project window, over the subject's
// coverage track); the rich grammar (#308 range-first, #338/#340 Segments,
// Text) moves behind a "More options" expander. Self-contained (props in,
// onsubmit/onclear out) — BindingLeafView owns the URL write.
describe("PeriodPicker — more-options modes (range / list / text)", () => {
  it("Picker mode submits the picked wire (behind the expander)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "More options" }).click();
    await expect
      .element(screen.getByRole("button", { name: "Picker" }))
      .toHaveAttribute("aria-pressed", "true");
    await screen.getByRole("spinbutton", { name: "From" }).fill("2018");
    // The expander's Apply (plain "Apply"); the slider's is "Apply period".
    await screen.getByRole("button", { name: "Apply", exact: true }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2018");
  });

  it("Segments mode builds the #307 comma wire; Apply submits the union query (#340)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "More options" }).click();
    await screen.getByRole("button", { name: "Segments" }).click();
    await screen.getByRole("spinbutton", { name: "From" }).fill("2005");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2010");
    await screen.getByRole("button", { name: "Add segment" }).click();
    await screen.getByRole("spinbutton", { name: "From" }).fill("2015");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2020");
    await screen.getByRole("button", { name: "Add segment" }).click();
    await screen.getByRole("button", { name: "Apply", exact: true }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2005..2010,2015..2020");
  });

  it("an active comma ?period opens the expander in Segments with its chips; a removal applies the rest", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "2005..2010,2015..2020",
      onsubmit,
      onclear: vi.fn(),
    });
    // A comma list is not year-representable → the expander opens by default in
    // Segments with the chips visible.
    await expect
      .element(screen.getByRole("button", { name: "Segments" }))
      .toHaveAttribute("aria-pressed", "true");
    // Exact-match the chip — the sub-annual cue near the slider also renders the
    // full comma wire (`2005..2010,2015..2020`) as a <code>, so a substring
    // match would now be ambiguous.
    await expect
      .element(screen.getByText("2005..2010", { exact: true }))
      .toBeVisible();
    await screen.getByRole("button", { name: "Remove 2005..2010" }).click();
    await screen.getByRole("button", { name: "Apply", exact: true }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2015..2020");
  });

  it("switching to Picker from an unrepresentable active period blanks the buffer — Apply no-ops, never re-submits the invisible value", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "2005..2010,2015..2020",
      onsubmit,
      onclear: vi.fn(),
    });
    // The expander is already open (comma list); switch to Picker → blank buffer.
    await screen.getByRole("button", { name: "Picker" }).click();
    // Blank controls + a null buffer: Apply must NOT submit the stale comma
    // value hiding behind them (the #347/#349 stale-buffer class).
    await screen.getByRole("button", { name: "Apply", exact: true }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("an unrepresentable active period (_default) opens the expander in Text, visible and editable", async () => {
    const screen = await render(PeriodPicker, {
      period: "_default",
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("button", { name: "Text" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect.element(screen.getByRole("textbox")).toHaveValue("_default");
  });
});

// The #615 year-window slider — the picker's DEFAULT control. Precedence
// (?period > window > full history), a local change writes ?period only (the
// window is never touched), and the two deviation states.
describe("PeriodPicker — window slider (#615)", () => {
  const WINDOW: StudyWindow = { from: 2000, to: 2010 };
  const COVERAGE: Coverage = { from: 1995, to: 2008 };

  it("seeds the slider thumbs from the project window when no ?period is set", async () => {
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // Precedence: no ?period → the window seeds the thumbs.
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");
  });

  it("an explicit ?period overrides the window (precedence ?period > window)", async () => {
    const screen = await render(PeriodPicker, {
      period: "2004..2006",
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2004");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2006");
  });

  it("Apply on the seeded window default (no ?period, no thumb moved) submits the window wire, not a no-op", async () => {
    // Codex P2: the slider is visibly seeded from the window but `sliderWire`
    // stays null until a thumb moves — clicking the default Apply must still
    // submit the SHOWN window, else BindingLeafView (narrows only on ?period)
    // leaves the user on full history despite "accepting" the displayed window.
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2010");
  });

  it("Apply with NO window and no ?period stays a no-op (full history — nothing to apply)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: COVERAGE,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("a slider change submits a ?period wire and never mutates the window", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit,
      onclear: vi.fn(),
    });
    // Move a thumb, then Apply → a wire is emitted (a single ?period write; the
    // picker has no window-write path at all — it only ever calls onsubmit /
    // onclear, both ?period operations).
    await screen.getByRole("slider", { name: "From year" }).fill("2002");
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2002..2010");
  });

  it("user deviation: ?period ≠ window shows the hint; reset NARROWS to the window wire (Fix B)", async () => {
    // Fix B: "reset to project window" must NARROW back like Apply — submit the
    // window's wire so BindingLeafView (narrows only on ?period) lands on the
    // window and the deviation clears. The OLD behavior (onclear → drop ?period →
    // full history) under-narrowed.
    const onsubmit = vi.fn<(period: string) => void>();
    const onclear = vi.fn<() => void>();
    const screen = await render(PeriodPicker, {
      period: "2003..2007",
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit,
      onclear,
    });
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .toBeVisible();
    await screen
      .getByRole("button", { name: "reset to project window" })
      .click();
    expect(onsubmit).toHaveBeenCalledOnce();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2010");
    expect(onclear).not.toHaveBeenCalled();
  });

  it("the standalone Clear still drops ?period entirely (full history, Fix B)", async () => {
    // The separate "Clear" affordance keeps the explicit full-history path — it
    // calls onclear (NOT a window narrow), distinct from "reset to project
    // window".
    const onsubmit = vi.fn<(period: string) => void>();
    const onclear = vi.fn<() => void>();
    const screen = await render(PeriodPicker, {
      period: "2003..2007",
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit,
      onclear,
    });
    await screen.getByRole("button", { name: "Clear" }).click();
    expect(onclear).toHaveBeenCalledOnce();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("no deviation hint when ?period equals the window", async () => {
    const screen = await render(PeriodPicker, {
      period: "2000..2010",
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
  });

  it("availability deviation: a selection past coverage greys the not-delivered span", async () => {
    // Window 2000–2010, coverage 1995–2008 → 2009–2010 is not delivered.
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered after 2008/))
      .toBeVisible();
  });

  it("sub-annual ?period with a window: the slider shows the sub-annual cue (not a misleading no-deviation), the expander opens to the real value", async () => {
    // HT2020 is not year-representable → activeYearSelection falls back to the
    // window, so the slider seeds to the window. Without the cue that would read
    // as "no deviation" (window vs window) and silently hide that the active
    // value is really HT2020. The picker must instead flag it and open the
    // expander to the real value.
    const screen = await render(PeriodPicker, {
      period: "HT2020",
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The honest cue near the slider names the sub-annual value (exact-match the
    // <code> — the cue sentence also contains it).
    await expect.element(screen.getByText(/Active period/)).toBeVisible();
    await expect
      .element(screen.getByText("HT2020", { exact: true }))
      .toBeVisible();
    // …and NOT the misleading deviation hint nor its absence-as-match reading.
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
    // The expander auto-opened on the real value: HT2020 is a single term-grain
    // token the range UI CAN hold, so it lands in Picker mode (still NOT
    // year-grain — that is exactly why the slider can't represent it).
    await expect
      .element(screen.getByRole("button", { name: "Fewer options" }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("button", { name: "Picker" }))
      .toHaveAttribute("aria-pressed", "true");
  });

  it("a window/seed change clears a stale dragged buffer → Apply submits the new window (Fix C)", async () => {
    // No ?period: the slider seeds from the window. The user drags a thumb (sets
    // sliderWire), then the GLOBAL window changes (header) or a project opens →
    // the slider re-seeds. The stale buffer must clear so the next Apply submits
    // the NOW-DISPLAYED window, not the old dragged value (Codex P2).
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: COVERAGE,
      onsubmit,
      onclear: vi.fn(),
    });
    // Drag the From thumb → buffer holds 2005..2010.
    await screen.getByRole("slider", { name: "From year" }).fill("2005");
    // The global window changes underneath (no ?period) → re-seed.
    await screen.rerender({
      period: null,
      window: { from: 2012, to: 2018 },
      coverage: COVERAGE,
      onsubmit,
      onclear: vi.fn(),
    });
    // Apply now submits the NEW window, not the stale 2005..2010 drag.
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2012..2018");
  });

  it("a drag-then-Apply with NO intervening seed change still submits the dragged value (Fix C guard)", async () => {
    // The legitimate path must survive: a drag sets the buffer; Apply right after
    // (no seed/URL change between) submits it — the buffer only clears on a
    // re-seed, never on the drag itself.
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: COVERAGE,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("slider", { name: "From year" }).fill("2003");
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2003..2010");
  });

  it("no window set: the availability note softens (no amber deviation) + a 'set a window' hint shows", async () => {
    const screen = await render(PeriodPicker, {
      period: "2005..2012",
      window: null,
      coverage: COVERAGE, // 1995–2008 → 2009–2012 not delivered
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The not-delivered note still appears (availability is relative to the
    // active selection)…
    await expect
      .element(screen.getByText(/Not delivered after 2008/))
      .toBeVisible();
    // …and the no-window hint nudges the user toward the header window.
    await expect
      .element(screen.getByText(/No project window set/))
      .toBeVisible();
    // No user-deviation hint (nothing to deviate from).
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
  });
});
