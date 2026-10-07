import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import PeriodWindowSlider from "./PeriodWindowSlider.svelte";
import type { Coverage } from "./period";
import type { StudyWindow } from "./project_data";

// The #615 availability-aware local period slider (the subject page's default
// period control). Self-contained: bounds + selection + window + coverage in,
// `onchange`/`onreset` out. The PeriodPicker owns the wire seam; this verifies
// the control's own behavior (two thumbs, coverage readout, the two deviation
// states).
describe("PeriodWindowSlider", () => {
  const base = {
    min: 1990,
    max: 2020,
    coverage: { from: 1995, to: 2015 } as Coverage,
    // The default: the shown span IS the active selection (year-grain), not a
    // sub-annual projection — the sub-annual-cue tests override this.
    subAnnualPeriod: null as string | null,
    // A real selection/window is set in these tests (not the no-op full-history
    // default), so the availability gap is live (#639).
    hasSelection: true,
    // The user has chosen the shown selection (an explicit ?period or a drag),
    // so the USER-deviation hint is live (Fix B); the default-seed-suppression
    // case is its own test below.
    userChosen: true,
  };

  it("unbounded-start coverage (from: null) STILL fires the finite-end gap (Fix A)", async () => {
    // coverage {null..2008}: unknown start, KNOWN end. A 2010–2015 selection is
    // entirely after the finite end → "Not delivered after 2008" must fire (the
    // round-1 regression dropped the whole span to null and suppressed it). The
    // open start reads as an ellipsis, never year 1.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      coverage: { from: null, to: 2008 },
      selection: { from: 2010, to: 2015 },
      window: { from: 2010, to: 2015 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered after 2008/))
      .toBeVisible();
    await expect.element(screen.getByText("data …–2008")).toBeVisible();
  });

  it("open-ended coverage projects to the VINTAGE, not the track edge (#631)", async () => {
    // coverage {1995..null} with vintageYear 2021 on a track that runs to 2026
    // (a stale window widened the bounds): the open end projects ONLY to the
    // vintage, NOT to `max`. So a 2000–2026 selection gaps 2022–2026 as "Not
    // delivered after 2021" — the cap holds even though the slider reaches 2026.
    // The readout still shows the open end as an ellipsis (raw coverage).
    const screen = await render(PeriodWindowSlider, {
      ...base,
      min: 1990,
      max: 2026,
      coverage: { from: 1995, to: null },
      vintageYear: 2021,
      selection: { from: 2000, to: 2026 },
      window: { from: 2000, to: 2026 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered after 2021/))
      .toBeVisible();
    await expect.element(screen.getByText("data 1995–…")).toBeVisible();
    // The greyed gap cell exists (the band stops at the vintage, the selection
    // overruns it).
    expect(screen.container.querySelectorAll(".gap").length).toBe(1);
  });

  it("no coverage → no availability note and no coverage readout", async () => {
    const screen = await render(PeriodWindowSlider, {
      min: 1990,
      max: 2020,
      coverage: null,
      subAnnualPeriod: null,
      hasSelection: true,
      userChosen: true,
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    await expect.element(screen.getByText(/^data /)).not.toBeInTheDocument();
  });

  it("hasSelection:false suppresses the leading not-delivered gap (no-op full-history default, #639)", async () => {
    // The no-op full-history default: no ?period, no window → the thumbs sit at
    // the full bounds [1960, 2008] the user never chose. The leading 1960–1994
    // span below coverage (1995–2008) would otherwise gap as "Not delivered
    // before 1995" — but `hasSelection:false` suppresses it (the user never chose
    // that span). No gap cells, no note.
    const screen = await render(PeriodWindowSlider, {
      min: 1960,
      max: 2008,
      coverage: { from: 1995, to: 2008 } as Coverage,
      subAnnualPeriod: null,
      hasSelection: false,
      userChosen: false,
      selection: { from: 1960, to: 2008 },
      window: null,
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    expect(screen.container.querySelectorAll(".gap").length).toBe(0);
  });

  it("hasSelection:true keeps the leading not-delivered gap (conditional, not removed; #639)", async () => {
    // Complementary guard: identical props but `hasSelection:true` (a real
    // selection/window is set) → the leading 1960–1994 gap below coverage fires.
    // Locks the suppression as conditional on the flag, NOT an unconditional
    // feature removal.
    const screen = await render(PeriodWindowSlider, {
      min: 1960,
      max: 2008,
      coverage: { from: 1995, to: 2008 } as Coverage,
      subAnnualPeriod: null,
      hasSelection: true,
      userChosen: true,
      selection: { from: 1960, to: 2008 },
      window: null,
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered before 1995/))
      .toBeVisible();
    expect(screen.container.querySelectorAll(".gap").length).toBe(1);
  });

  it("a REJECTED move keeps the thumbs, the readout and the emitted range in agreement", async () => {
    // The synthetic catalog's finite 2018 leaf: coverage 2018–2018 on a
    // 1960–2020 track, both thumbs already on the single delivered year. Home on
    // From and End on To are rejected by the coverage clamp — so the pending
    // range stays 2018–2018, and the thumbs (what a screen reader announces, and
    // what Apply submits) must say the same.
    const onchange = vi.fn<(next: StudyWindow) => void>();
    const screen = await render(PeriodWindowSlider, {
      ...base,
      min: 1960,
      max: 2020,
      coverage: { from: 2018, to: 2018 } as Coverage,
      selection: { from: 2018, to: 2018 },
      window: { from: 2018, to: 2018 },
      onchange,
      onreset: vi.fn(),
    });
    await screen.getByRole("slider", { name: "From year" }).fill("1960");
    await screen.getByRole("slider", { name: "To year" }).fill("2020");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2018");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2018");
    // `exact` so the readout is matched, not the "data 2018–2018" coverage span.
    await expect
      .element(screen.getByText("2018–2018", { exact: true }))
      .toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith({ from: 2018, to: 2018 });
  });

  it("finite coverage: no redundant 'coverage through' note (the readout already names the end, M21)", async () => {
    const screen = await render(PeriodWindowSlider, {
      ...base, // coverage 1995–2015 (finite)
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect.element(screen.getByText("data 1995–2015")).toBeVisible();
    await expect
      .element(screen.getByText(/coverage through/))
      .not.toBeInTheDocument();
  });

  it("INVERTED coverage: no 'coverage through' note (the band/seed discard it, Fix 7)", async () => {
    // Inverted coverage {from:2025, to:null} on a 2024 vintage → effective
    // 2025..2024 (open end projected to the vintage), which `bandEdges` nulls as
    // unusable (Fix D: no band, no clamp). The "coverage through 2024" note would
    // contradict the "data 2025–…" readout, so Fix 7 gates it on `bandEdges` — the
    // note must NOT render. The raw readout still shows the open end as an ellipsis.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      min: 1990,
      max: 2024,
      coverage: { from: 2025, to: null },
      vintageYear: 2024,
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect.element(screen.getByText("data 2025–…")).toBeVisible();
    await expect
      .element(screen.getByText(/coverage through/))
      .not.toBeInTheDocument();
  });

  it("sub-annual ?period: availability gaps are suppressed (the projection isn't the real selection)", async () => {
    // selection 2000–2020 vs coverage 1995–2015 WOULD gap 2016–2020 — but the
    // shown span is the window PROJECTION, not the real (sub-annual) value, so the
    // gap is meaningless: no "Not delivered" note, no hatched gap cells. The
    // sub-annual cue names the real value without implying the slider represents it.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2000, to: 2020 },
      window: { from: 2000, to: 2020 },
      subAnnualPeriod: "HT2020",
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect.element(screen.getByText(/Active period/)).toBeVisible();
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    expect(screen.container.querySelectorAll(".gap").length).toBe(0);
  });
});
