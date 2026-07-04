import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import type { Coverage } from "./period";
import type { StudyWindow } from "./project_data";

// The catalog PeriodPicker. The control is the #615 year-window availability
// slider (seeded from the project window, over the subject's coverage track).
// Rich `?period` wire values still resolve server-side, but the picker no longer
// authors range/list/text periods.
describe("PeriodPicker — slider-only UI", () => {
  it("does not render advanced authoring controls or the no-window hint", async () => {
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("button", { name: "Apply period" }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("button", { name: "More options" }))
      .not.toBeInTheDocument();
    expect(screen.container.querySelector("input[type='text']")).toBeNull();
    await expect
      .element(screen.getByText(/No project window set/))
      .not.toBeInTheDocument();
  });

  it("keeps an active token ?period visible and clearable without rewriting it on untouched Apply", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const onclear = vi.fn<() => void>();
    const screen = await render(PeriodPicker, {
      period: "HT2020",
      window: { from: 2000, to: 2010 },
      onsubmit,
      onclear,
    });
    await expect.element(screen.getByText(/Active period/)).toBeVisible();
    await expect
      .element(screen.getByText("HT2020", { exact: true }))
      .toBeVisible();
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
    await screen.getByRole("button", { name: "Clear" }).click();
    expect(onclear).toHaveBeenCalledOnce();
  });

  it("keeps an active comma-list ?period visible and clearable without rewriting it on untouched Apply", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const onclear = vi.fn<() => void>();
    const screen = await render(PeriodPicker, {
      period: "2005..2010,2015..2020",
      window: { from: 2000, to: 2020 },
      onsubmit,
      onclear,
    });
    await expect
      .element(screen.getByText("2005..2010,2015..2020", { exact: true }))
      .toBeVisible();
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
    await screen.getByRole("button", { name: "Clear" }).click();
    expect(onclear).toHaveBeenCalledOnce();
  });
});

// The #615 year-window slider — the picker's DEFAULT control. Precedence
// (?period > window > full history), a local change writes ?period only (the
// window is never touched), and the two deviation states.
describe("PeriodPicker — window slider (#615)", () => {
  const WINDOW: StudyWindow = { from: 2000, to: 2010 };
  const COVERAGE: Coverage = { from: 1995, to: 2008 };

  it("seeds the slider thumbs from window∩coverage when no ?period is set (#671)", async () => {
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: COVERAGE, // 1995–2008
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // #671: no ?period → the thumbs seed at the window narrowed to coverage
    // (2000–2008), so the variable's real coverage shows up front instead of the
    // window's 2009–2010 tail reading as available.
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2008");
  });

  it("seeds the thumbs at window∩coverage when a window extends past coverage (#671)", async () => {
    // #671 seed precedence: no ?period, window 2000–2010 but coverage only
    // 1995–2008 → the thumbs seed at the intersection 2000–2008 (the window
    // narrowed to where data exists), not the bare window 2000–2010.
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 2000, to: 2010 },
      coverage: { from: 1995, to: 2008 } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2008");
  });

  it("seeds the thumbs at the coverage span when no window is set (#671)", async () => {
    // No ?period, no window, coverage 1995–2008 → the thumbs seed at the coverage
    // span (the variable's true coverage shown up front), not the full bounds.
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1995, to: 2008 } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("1995");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2008");
  });

  it("open-start coverage + a pre-1960 window seeds From at the window start, not the 1960 floor (Fix 5)", async () => {
    // Fix 5: window 1950–2005 starts before the SLIDER_FLOOR_YEAR (1960), so the
    // bounds widen below the floor. An OPEN-start coverage ({null..2008}) must
    // extend to that rendered track start — the From thumb seeds at 1950, NOT 1960
    // (which would have silently dropped the covered 1950–1959 years).
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 1950, to: 2005 } as StudyWindow,
      coverage: { from: null, to: 2008 } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("1950");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2005");
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

  it("Apply on the seeded window∩coverage default (no thumb moved) submits the shown wire, not a no-op", async () => {
    // Codex P2 invariant under #671: the slider is visibly seeded from the
    // window narrowed to coverage (2000–2008), and Apply on the untouched seed
    // must submit the SHOWN span — else BindingLeafView (narrows only on ?period)
    // leaves the user on full history despite "accepting" the displayed default.
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: COVERAGE, // 1995–2008 → seed 2000–2008
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2008");
  });

  it("Apply on the up-front coverage seed (no window, no ?period) submits the coverage span (#671)", async () => {
    // #671: with no window/?period but a finite coverage, the thumbs seed at the
    // coverage span (shown up front), so Apply on the untouched seed narrows to
    // it — accepting the displayed default applies it (the Codex P2 invariant),
    // now over coverage rather than no-opping on the old full-history default.
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: COVERAGE, // 1995–2008
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("1995..2008");
  });

  it("Apply with NO window, no ?period AND no coverage stays a no-op (nothing to narrow to)", async () => {
    // Only the truly-empty case (no selection AND no coverage) keeps the no-op:
    // the seed is the full bounds, nothing meaningful was chosen.
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: null,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("uses enforced steward-derived bounds when no selected period reaches outside them (#1037)", async () => {
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1996, to: null },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");
  });

  it("enforced steward bounds clamp stale project windows before deriving bounds or Apply output (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 1960, to: 2026 },
      coverage: { from: 1996, to: null },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2010");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2010");
  });

  it("enforced steward bounds do not report reset against the raw stale project window (#1037)", async () => {
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 2012, to: 2020 },
      coverage: { from: 1996, to: null },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2010");
    expect(document.body.textContent).not.toContain(
      "Deviates from project window (2012–2020)",
    );
  });

  it("enforced steward bounds ignore wholly post-ceiling coverage when deriving slider bounds (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 2027, to: 2030 },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2010");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("enforced steward bounds ignore wholly pre-floor coverage when deriving slider bounds (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1990, to: 1995 },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2010");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("enforced steward bounds ignore open-ended post-ceiling coverage when deriving slider bounds (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 2027, to: null },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2010");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("enforced steward bounds ignore open-start pre-floor coverage when deriving slider bounds (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: null, to: 1995 },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2010");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("enforced steward bounds clamp stale active year periods before deriving bounds or Apply output (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "1960..2026",
      window: { from: 2000, to: 2010 },
      coverage: { from: 1996, to: null },
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2010");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2010");
  });

  it("global fallback floor does not erase real pre-1960 coverage (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1950, to: 1965 },
      windowMinYear: 1960,
      vintageYear: 2026,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveAttribute("min", "1950");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("1950");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("1950..1965");
  });

  it("global fallback ceiling does not erase finite post-vintage coverage (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 2027, to: 2030 },
      windowMinYear: 1960,
      vintageYear: 2026,
      onsubmit,
      onclear: vi.fn(),
    });

    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2030");
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2027");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2030");

    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2027..2030");
  });

  it("Apply with an INVERTED (discarded) coverage and no window stays a no-op (Fix 4)", async () => {
    // Fix 4: an inverted effective coverage (e.g. {from:2025, to:null} on a 2024
    // vintage → effective 2025..2024) is treated as NO coverage by both
    // `intersectCoverageWindow` and the slider's `bandEdges` (Fix D), so with no
    // window the seed falls back to the FULL bounds (1960..2024) — a span containing
    // no data. The old gate (`coverage !== null`) still fired here (raw coverage is
    // non-null) and Apply submitted that full span. The refined gate keys on a
    // USABLE coverage, so this follows the no-op path instead.
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 2025, to: null } as Coverage, // inverted vs the vintage
      vintageYear: 2024,
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
    // onclear, both ?period operations). The To thumb seeds at the coverage end
    // (2008) under #671, so dragging only From yields 2002..2008.
    await screen.getByRole("slider", { name: "From year" }).fill("2002");
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2002..2008");
  });

  it("no spurious deviation on the coverage-clamped default seed (window 2000–2010 ∩ coverage 1995–2008, no ?period) (Fix B)", async () => {
    // Fix B: the default seed clamps to 2000–2008 (window ∩ coverage), which ≠ the
    // bare window 2000–2010 — but the user chose nothing, the data constrained it.
    // The amber "Deviates from project window" hint must NOT fire on this default
    // render (else its reset would submit the bare window and render the gap #671
    // avoids).
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: COVERAGE, // 1995–2008 → seed 2000–2008
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The seed is the clamped 2000–2008…
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2008");
    // …yet no deviation hint fires (the user hasn't chosen anything).
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
  });

  it("DISJOINT window/coverage: the default seed snaps OUTSIDE the window → the deviation hint DOES fire (Fix 3)", async () => {
    // Fix 3 refines Fix B's suppression: when the window and coverage do NOT
    // overlap (window 2012–2018, coverage 1995–2008, no ?period),
    // `intersectCoverageWindow` snaps the seed to the nearest coverage edge (2008),
    // which lands OUTSIDE the project window. That mismatch IS worth reporting — so
    // the "Deviates from project window" hint must fire even on the untouched
    // default seed (unlike the within-window narrowing case, which stays silent).
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 2012, to: 2018 } as StudyWindow,
      coverage: { from: 1995, to: 2008 } as Coverage, // disjoint → seed snaps to 2008
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The seed snaps to the coverage edge (2008), outside the 2012–2018 window…
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2008");
    // …so the deviation hint fires without any user action (the snap is reportable).
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .toBeVisible();
  });

  it("after the user drags a thumb to a value ≠ window, the deviation hint fires (Fix B)", async () => {
    // Once the user actually moves a thumb (userChosen via the live sliderWire),
    // the deviation hint becomes meaningful again — the suppression is only for the
    // untouched default seed.
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: COVERAGE, // 1995–2008
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // Drag the From thumb to 2003 (≠ window start) → a user-chosen selection.
    await screen.getByRole("slider", { name: "From year" }).fill("2003");
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .toBeVisible();
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

  it("availability deviation: an explicit ?period past coverage greys the not-delivered span", async () => {
    // An explicit ?period 2000–2010 (the URL's source of truth) past coverage
    // 1995–2008 → 2009–2010 is not delivered. Unlike the #671 default seed (which
    // is clipped INTO coverage and clamped), an explicit out-of-coverage ?period
    // renders honestly with its gap.
    const screen = await render(PeriodPicker, {
      period: "2000..2010",
      window: WINDOW,
      coverage: COVERAGE,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered after 2008/))
      .toBeVisible();
  });

  it("sub-annual ?period with a window: the slider shows the sub-annual cue without advanced modes", async () => {
    // HT2020 is not year-representable → activeYearSelection falls back to the
    // window, so the slider seeds to the window. Without the cue that would read
    // as "no deviation" (window vs window) and silently hide that the active
    // value is really HT2020. The picker must instead flag it without exposing
    // the removed range/list/text authoring modes.
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
    await expect
      .element(screen.getByRole("button", { name: "More options" }))
      .not.toBeInTheDocument();
    await expect
      .element(screen.getByRole("button", { name: "Clear" }))
      .toBeVisible();
  });

  it("a window/seed change clears a stale dragged buffer → Apply submits the new window (Fix C)", async () => {
    // No ?period: the slider seeds from the window. The user drags a thumb (sets
    // sliderWire), then the GLOBAL window changes (header) or a project opens →
    // the slider re-seeds. The stale buffer must clear so the next Apply submits
    // the NOW-DISPLAYED window, not the old dragged value (Codex P2).
    const onsubmit = vi.fn<(period: string) => void>();
    // Coverage spans both windows here so the #671 seed = the window verbatim
    // (window ⊆ coverage), isolating the Fix-C re-seed behavior from the
    // intersection-clip (covered by its own tests above).
    const WIDE: Coverage = { from: 1990, to: 2020 };
    const screen = await render(PeriodPicker, {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: WIDE,
      onsubmit,
      onclear: vi.fn(),
    });
    // Drag the From thumb → buffer holds 2005..2010.
    await screen.getByRole("slider", { name: "From year" }).fill("2005");
    // The global window changes underneath (no ?period) → re-seed.
    await screen.rerender({
      period: null,
      window: { from: 2012, to: 2018 },
      coverage: WIDE,
      onsubmit,
      onclear: vi.fn(),
    });
    // Apply now submits the NEW window, not the stale 2005..2010 drag.
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2012..2018");
  });

  it("a COVERAGE change (same ?period + window) clears a stale dragged buffer → Apply submits the new coverage-clamped seed (Fix C, #671 coverage seed)", async () => {
    // #671 made the thumb seed depend on `coverage`, but the reset effect tracked
    // only period/activeYearSelection/ceiling. Navigating between leaves that share
    // the URL (?period null) AND the window but differ in COVERAGE re-seeds the
    // thumbs without moving any of those — so a stale drag survived and Apply
    // submitted the PRIOR leaf's dragged span. The effect now tracks
    // `seededSelection`, so the coverage change clears the buffer.
    const onsubmit = vi.fn<(period: string) => void>();
    const props = {
      period: null,
      window: WINDOW, // 2000–2010
      coverage: { from: 1995, to: 2008 } as Coverage, // leaf A → seed 2000–2008
      onsubmit,
      onclear: vi.fn(),
    };
    const screen = await render(PeriodPicker, props);
    // Drag the From thumb on leaf A → buffer holds 2005..2008.
    await screen.getByRole("slider", { name: "From year" }).fill("2005");
    // Navigate to leaf B: SAME ?period (null) and window, DIFFERENT coverage.
    await screen.rerender({
      ...props,
      coverage: { from: 1995, to: 2004 } as Coverage, // leaf B → seed 2000–2004
    });
    // Apply submits leaf B's coverage-clamped seed (2000..2004), NOT the stale
    // leaf-A drag (2005..2008).
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2004");
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
    // To seeds at the coverage end (2008) under #671 → 2003..2008.
    expect(onsubmit).toHaveBeenLastCalledWith("2003..2008");
  });

  it("OPEN-ended coverage: a stale window past the vintage seeds INTO the vintage-capped coverage (#631 + #671)", async () => {
    // The #631 cap (an open-ended coverage projects only to the vintage, not the
    // track edge) under the #671 coverage-aware seed: a stale window 2000–2026 on
    // a 2021 catalog with open-ended coverage from 1995 → the seed is
    // window∩effective-coverage = 2000–2021 (clipped to the vintage), and the
    // thumbs are clamped to 2021. So the 2022–2026 span beyond the vintage is the
    // up-front non-selectable `unavailable` band, NOT a selected not-delivered gap
    // — the cap holds without the default seed ever overrunning it.
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 2000, to: 2026 } as StudyWindow, // stale, past the vintage
      coverage: { from: 1995, to: null } as Coverage, // open-ended
      vintageYear: 2021,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The bounds WIDEN to fit the real window value (never clip the track)…
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2026");
    // …but the seed To clamps to the vintage (the open-end cap), so the default
    // span tops out at 2021 with no not-delivered gap, and the beyond-vintage
    // 2022–2026 span is the unavailable backdrop. The open end still reads as an
    // ellipsis, with the "coverage through 2021" note naming the ceiling.
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2021");
    await expect.element(screen.getByText("data 1995–…")).toBeVisible();
    await expect
      .element(screen.getByText("coverage through 2021"))
      .toBeVisible();
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    expect(
      screen.container.querySelectorAll(".unavailable").length,
    ).toBeGreaterThan(0);
  });

  it("FINITE coverage is NOT extended to the vintage: default span reflects the real end (#631)", async () => {
    // The model: the vintage caps an OPEN-ended coverage; it must NOT floor the
    // bounds for a FINITE one. Coverage 1995–2008 on a 2021 catalog, no window /
    // no ?period → the bounds must stop at the real end (2008), NOT jump to 2021,
    // so the default full-span selection doesn't spuriously report 2009–2021 as
    // not-delivered. The old `Math.max(ceilingYear, ...)` forced max to 2021 and
    // the default [1960, 2021] span reported 2009–2021 as a gap (Codex P2 #2).
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1995, to: 2008 } as Coverage, // finite
      vintageYear: 2021,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The bounds end at the real coverage end (2008), not the vintage (2021)…
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2008");
    // …and the thumbs seed at the coverage span (#671: coverage shown up front,
    // no window → the coverage span), so the readout is 1995–2008, NOT the old
    // full-bounds 1960–2008. There is NO spurious "not delivered after" gap for
    // the 2009–2021 years the old forced bounds invented.
    await expect
      .element(screen.getByText("1995–2008", { exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByText(/Not delivered after/))
      .not.toBeInTheDocument();
  });

  it("FINITE coverage: a ?period past the finite end still flags 'not delivered' (#631)", async () => {
    // Finite coverage 1995–2008; an explicit ?period to 2026 WIDENS the bounds
    // (the thumb shows the real 2026) but reads as beyond the finite end — the
    // not-delivered gap fires at 2008, independent of the vintage / wall-clock.
    const screen = await render(PeriodPicker, {
      period: "2000..2026",
      window: WINDOW,
      coverage: { from: 1995, to: 2008 } as Coverage,
      vintageYear: 2021,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", "2026");
    await expect
      .element(screen.getByText(/Not delivered after 2008/))
      .toBeVisible();
  });

  it("before /api/context resolves (no vintageYear) the slider falls back to wall-clock (#631)", async () => {
    // The pre-context fallback (mirroring App's `|| new Date().getFullYear()`):
    // a leaf rendered before context loads has no vintageYear → the open-ended
    // ceiling falls back to wall-clock so the slider still works (corrected once
    // context resolves and the prop threads down). The bounds max (open-ended
    // coverage end ?? ceiling) is then the current year.
    const thisYear = new Date().getFullYear();
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 2000, to: 2010 } as StudyWindow,
      coverage: { from: 1995, to: null } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveAttribute("max", String(thisYear));
  });

  it("a ceiling change clears a stale drag buffer → Apply submits the corrected value (#631)", async () => {
    // Codex P2 #3: a leaf renders pre-context (ceiling = wall-clock), the user
    // drags a thumb past the (later) vintage, THEN context resolves and threads
    // vintageYear down → the ceiling flips. The display clamps to the new max,
    // but a stale `sliderWire` from the drag would let Apply submit the old
    // beyond-vintage wire. The reset effect now tracks the ceiling, so the buffer
    // clears and Apply submits the corrected (re-seeded) selection.
    const onsubmit = vi.fn<(period: string) => void>();
    const props = {
      period: null,
      window: { from: 2000, to: 2010 } as StudyWindow,
      coverage: { from: 1995, to: null } as Coverage,
      onsubmit,
      onclear: vi.fn(),
    };
    // Pre-context: no vintageYear (wall-clock ceiling, so 2026 is in bounds).
    const screen = await render(PeriodPicker, props);
    await screen.getByRole("slider", { name: "To year" }).fill("2026");
    // Context resolves: the vintage (2021) threads down → the ceiling flips.
    await screen.rerender({ ...props, vintageYear: 2021 });
    // The stale 2000..2026 drag is cleared; Apply submits the re-seeded window.
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2010");
  });

  it("LEADING gap is suppressed at the picker level (no ?period, no window → hasSelection:false, #639)", async () => {
    // The #639 entry condition: period null + window null + finite coverage
    // 1995–2008. With no selection, activeYearSelection is null → hasSelection
    // is false, so the leading 1960–1994 span below coverage must NOT gap as
    // "Not delivered before 1995" (the user never chose the full-history span).
    // No note, no gap cells.
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1995, to: 2008 } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    expect(screen.container.querySelectorAll(".gap").length).toBe(0);
  });

  it("dragging a thumb into the not-delivered region is hard-clamped to coverage (#671)", async () => {
    // #671 supersedes the #639 follow-up: rather than firing an availability gap
    // once a drag enters not-delivered years, the thumbs are HARD-CLAMPED to
    // coverage so they can't get there at all. The variable's true coverage shows
    // up front (the seed sits at [1995, 2008] and the out-of-coverage track is a
    // non-selectable greyed band), so the alarming "you dragged into bad data"
    // state never arises (the #639 intent, satisfied a different way).
    const screen = await render(PeriodPicker, {
      period: null,
      window: null,
      coverage: { from: 1995, to: 2008 } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    // The seed sits at the coverage span (shown up front), and the up-front
    // unavailable band renders without any interaction.
    await expect
      .element(screen.getByText("1995–2008", { exact: true }))
      .toBeVisible();
    expect(
      screen.container.querySelectorAll(".unavailable").length,
    ).toBeGreaterThan(0);
    // Drag the To thumb back to 1980 → clamped UP to the coverage start (1995);
    // it cannot enter the pre-coverage region, so no not-delivered gap fires.
    await screen.getByRole("slider", { name: "To year" }).fill("1980");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("1995");
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    expect(screen.container.querySelectorAll(".gap").length).toBe(0);
  });

  it("a window wider than coverage seeds INTO coverage — the pre-coverage span is the unavailable band, not a gap (#671)", async () => {
    // Under #671 the window 1960–2008 is intersected with coverage 1995–2008, so
    // the thumbs seed at 1995–2008 (the pre-coverage 1960–1994 span is no longer
    // SELECTED). That span reads as the up-front non-selectable `unavailable` band
    // — NOT a "Not delivered before" gap (which #671 supersedes for the default
    // seed; the alarming selection-gap only fires for an explicit out-of-coverage
    // ?period). This supersedes the old #639 "window set → leading gap fires" lock.
    const screen = await render(PeriodPicker, {
      period: null,
      window: { from: 1960, to: 2008 } as StudyWindow,
      coverage: { from: 1995, to: 2008 } as Coverage,
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("1995");
    await expect
      .element(screen.getByText(/Not delivered/))
      .not.toBeInTheDocument();
    expect(
      screen.container.querySelectorAll(".unavailable").length,
    ).toBeGreaterThan(0);
  });

  it("no window set: the availability note softens without rendering a helper hint", async () => {
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
    // …but no generic no-window hint renders on the page.
    await expect
      .element(screen.getByText(/No project window set/))
      .not.toBeInTheDocument();
    // No user-deviation hint (nothing to deviate from).
    await expect
      .element(screen.getByText(/Deviates from project window/))
      .not.toBeInTheDocument();
  });
});
