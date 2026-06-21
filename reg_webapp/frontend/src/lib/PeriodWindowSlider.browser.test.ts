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
  };

  it("seeds two thumbs + the readout from the selection, shows the coverage span", async () => {
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");
    await expect.element(screen.getByText("2000–2010")).toBeVisible();
    await expect.element(screen.getByText("data 1995–2015")).toBeVisible();
  });

  it("moving the From thumb emits the new window (never crossing To)", async () => {
    const onchange = vi.fn<(next: StudyWindow) => void>();
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      onchange,
      onreset: vi.fn(),
    });
    await screen.getByRole("slider", { name: "From year" }).fill("2005");
    expect(onchange).toHaveBeenLastCalledWith({ from: 2005, to: 2010 });
  });

  it("user deviation: selection ≠ window shows the hint; reset fires onreset", async () => {
    const onreset = vi.fn<() => void>();
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2002, to: 2008 },
      window: { from: 2000, to: 2010 },
      onchange: vi.fn(),
      onreset,
    });
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .toBeVisible();
    await screen
      .getByRole("button", { name: "reset to project window" })
      .click();
    expect(onreset).toHaveBeenCalledOnce();
  });

  it("no user-deviation hint when the selection matches the window", async () => {
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
  });

  it("availability deviation: a selection beyond coverage shows the not-delivered note", async () => {
    // coverage 1995–2015; select 2000–2020 → 2016–2020 not delivered.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2000, to: 2020 },
      window: { from: 2000, to: 2020 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered after 2015/))
      .toBeVisible();
  });

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

  it("unbounded-end coverage (to: null) fires no 'after' gap (still delivered)", async () => {
    // coverage {1990..null}: a 2000–2030 selection sits entirely inside the open
    // end → no "Not delivered after" note; the end reads as an ellipsis.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      min: 1985,
      max: 2030,
      coverage: { from: 1990, to: null },
      selection: { from: 2000, to: 2030 },
      window: { from: 2000, to: 2030 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    await expect.element(screen.getByText("data 1990–…")).toBeVisible();
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

  it("finite coverage is NOT re-projected by vintageYear (#631)", async () => {
    // coverage {1990..2008} with vintageYear 2021: the finite end is the gap
    // boundary, NOT the vintage. A 2000–2015 selection gaps "after 2008", never
    // "after 2021" — vintageYear caps only the OPEN end.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      min: 1985,
      max: 2026,
      coverage: { from: 1990, to: 2008 },
      vintageYear: 2021,
      selection: { from: 2000, to: 2015 },
      window: { from: 2000, to: 2015 },
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered after 2008/))
      .toBeVisible();
    await expect.element(screen.getByText("data 1990–2008")).toBeVisible();
  });

  it("no coverage → no availability note and no coverage readout", async () => {
    const screen = await render(PeriodWindowSlider, {
      min: 1990,
      max: 2020,
      coverage: null,
      subAnnualPeriod: null,
      hasSelection: true,
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

  it("sub-annual ?period: the shown span is the window projection → show the cue, suppress the misleading no-deviation reading", async () => {
    // selection === window (the projected fallback) would normally read as "no
    // deviation"; but the active value is sub-annual, so the slider must say so
    // rather than imply the window is the active selection.
    const screen = await render(PeriodWindowSlider, {
      ...base,
      selection: { from: 2000, to: 2010 },
      window: { from: 2000, to: 2010 },
      subAnnualPeriod: "HT2020",
      onchange: vi.fn(),
      onreset: vi.fn(),
    });
    await expect.element(screen.getByText(/Active period/)).toBeVisible();
    await expect
      .element(screen.getByText("HT2020", { exact: true }))
      .toBeVisible();
    // No user-deviation hint (it would be a window-vs-window artefact here).
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
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

  it("sub-annual ?period: availability gaps are suppressed (the projection isn't the real selection)", async () => {
    // selection 2000–2020 vs coverage 1995–2015 WOULD gap 2016–2020 — but the
    // shown span is the window PROJECTION, not the real (sub-annual) value, so the
    // gap is meaningless: no "Not delivered" note, no hatched gap cells. The
    // sub-annual cue already points at the real value in More options (Codex P2).
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
