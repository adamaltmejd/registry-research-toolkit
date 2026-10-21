import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import PeriodWindowSlider from "./PeriodWindowSlider.svelte";
import type { Coverage } from "./period";
import type { ProjectStudyWindow } from "./project_data";

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

  it("a REJECTED move keeps the thumbs, the readout and the emitted range in agreement", async () => {
    // The synthetic catalog's finite 2018 leaf: coverage 2018–2018 on a
    // 1960–2020 track, both thumbs already on the single delivered year. Home on
    // From and End on To are rejected by the coverage clamp — so the pending
    // range stays 2018–2018, and the thumbs (what a screen reader announces, and
    // what Apply submits) must say the same.
    const onchange = vi.fn<(next: ProjectStudyWindow) => void>();
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
});
