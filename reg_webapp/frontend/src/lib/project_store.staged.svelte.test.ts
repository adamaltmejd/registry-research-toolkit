import { afterEach, describe, expect, it, vi } from "vitest";
import { bindingFieldsFromResolution } from "./catalog";
import { sourceSnapshot } from "./project_data";
import {
  initDraftLifecycle,
  projectStore,
  type StagedAdd,
  setPersistence,
} from "./project_store.svelte";
import {
  add,
  openFile,
  SEED,
  storedProject,
} from "./project-store-test-helpers";

// Split from project_store.svelte.test.ts by contract surface: the cart's commit
// paths (applyStagedDiff, applySourcePeriodEdit). Sibling: project_store.svelte.test.ts.

// The store is a MODULE SINGLETON — each test must establish the state it needs
// (via newProject / openFile) rather than assume a fresh store.

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("applyStagedDiff (#992 — one atomic commit path)", () => {
  it("adds find-or-create by register_variant ALONE (a second add of the same variant appends, not a new source)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", 2018),
        add("scb/lisa/v1", "scb/lisa/alder", 2018, { type: "numeric" }),
      ],
    });
    // Both bindings land on ONE source (keyed on the variant), not two.
    expect(storedProject()?.sources).toHaveLength(1);
    expect(storedProject()?.sources[0].register_variant).toBe("scb/lisa/v1");
    expect(storedProject()?.sources[0].bindings.map((b) => b.variable)).toEqual(
      ["scb/lisa/kon", "scb/lisa/alder"],
    );
    // The #312 name prefill fired on the created source.
    expect(storedProject()?.sources[0].name).toBe("LISA");
  });

  it("commits two disjoint-era picks of ONE physical column with nothing to collide (Y-76)", () => {
    // `scb/lisa/forvink-ers-aktiv` (ForvErs, 1990..2021) and `scb/lisa/forvink-ers`
    // (ForvErs, 2022..2023) are the same LISA column across editions, added to ONE
    // source. Both bindings come from the real pick-time derivation, so neither
    // carries the `display_name` that reg-core's period-blind per-source
    // `display_name_collision` would have collided.
    projectStore.newProject(SEED);
    const pick = (variable: string, period: StagedAdd["period"]): StagedAdd =>
      add(
        "scb/lisa/individer",
        variable,
        period,
        bindingFieldsFromResolution(
          variable,
          { kind: "derived", type: "numeric" },
          "ForvErs",
        ),
      );
    projectStore.applyStagedDiff({
      adds: [
        pick("scb/lisa/forvink-ers-aktiv", { from: 1990, to: 2021 }),
        pick("scb/lisa/forvink-ers", { from: 2022, to: 2023 }),
      ],
    });

    expect(storedProject()?.sources).toHaveLength(1);
    expect(storedProject()?.sources[0].bindings).toEqual([
      {
        variable: "scb/lisa/forvink-ers-aktiv",
        type: "numeric",
        representation: null,
      },
      {
        variable: "scb/lisa/forvink-ers",
        type: "numeric",
        representation: null,
      },
    ]);
  });

  it("preserves token-grammar add windows as source-period coverage", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [add("scb/hst/v1", "scb/hst/kon", "HT2020")],
    });
    // A second add with a DIFFERENT token extends the source period so the saved
    // source still covers every binding that was resolved for it.
    projectStore.applyStagedDiff({
      adds: [add("scb/hst/v1", "scb/hst/alder", "VT2021")],
    });
    expect(storedProject()?.sources[0].period).toEqual(["HT2020", "VT2021"]);
  });

  it("preserves every same-batch token add window in the committed source period", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/hst/v1", "scb/hst/kon", "2020-Q1"),
        add("scb/hst/v1", "scb/hst/alder", "2020-Q2"),
        add("scb/hst/v1", "scb/hst/inkomst", "2020-Q3", {
          type: "numeric",
        }),
      ],
    });

    expect(storedProject()?.sources).toHaveLength(1);
    expect(storedProject()?.sources[0].period).toEqual([
      "2020-Q1",
      "2020-Q2",
      "2020-Q3",
    ]);
    expect(storedProject()?.sources[0].bindings.map((b) => b.variable)).toEqual(
      ["scb/hst/kon", "scb/hst/alder", "scb/hst/inkomst"],
    );
  });

  it("a finite add REPLACES an imported `_default` source period", async () => {
    // The retired sentinel is not a project period, so the coverage union has no
    // full-history branch to absorb into: staging a finite window onto an
    // imported `_default` source commits the finite window, not `_default`.
    openFile(
      JSON.stringify({
        schema_version: "2.0.0",
        steward: "global",
        reg_meta_version: "reg_meta/v1.0.0",
        name: "imported",
        sources: [
          {
            name: "LISA",
            register_variant: "scb/lisa/v1",
            period: "_default",
            bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
          },
        ],
      }),
    );

    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/alder", { from: 2010, to: 2015 })],
    });

    expect(storedProject()?.sources).toHaveLength(1);
    expect(storedProject()?.sources[0].period).toEqual({
      from: 2010,
      to: 2015,
    });
  });

  it("the add-path duplicate guard makes a re-add of the SAME (variant, variable, representation) a no-op (one binding, not two)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    // A SECOND commit of the identical add sees the first commit's binding as
    // `existing` → the guard drops it (bindingMatches' exact-column compare).
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    expect(storedProject()?.sources).toHaveLength(1);
    // Exactly ONE binding for the variable — the second add collapsed onto it.
    expect(storedProject()?.sources[0].bindings.map((b) => b.variable)).toEqual(
      ["scb/lisa/ssyk"],
    );
  });

  it("the add-path duplicate guard treats a null-STORED binding as a duplicate of ANY payload representation (null-either-side, no field overwrite)", () => {
    projectStore.newProject(SEED);
    // Store a binding with NO representation (the stored-null side).
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/ssyk", 2018)],
    });
    // Re-add the SAME variable with a CONCRETE representation. Per bindingMatches'
    // null-either-side rule (a stored null matches ANY payload rep), this is a
    // duplicate → a true no-op.
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    const bindings = storedProject()?.sources[0].bindings ?? [];
    expect(bindings).toHaveLength(1);
    expect(bindings[0].variable).toBe("scb/lisa/ssyk");
    // The guard is a no-op, not a replace: the stored binding keeps its original
    // (absent) representation rather than adopting the payload's Ssyk3.
    expect(bindings[0].representation ?? null).toBeNull();
  });

  it("the add-path duplicate guard treats a null PAYLOAD representation as a duplicate of a non-null stored binding (mirror null-either-side)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    // Re-add the SAME variable with NO representation (the payload-null side) →
    // the rule matches the stored Ssyk3 binding → no-op, still ONE binding that
    // retains its original Ssyk3.
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/ssyk", 2018)],
    });
    const bindings = storedProject()?.sources[0].bindings ?? [];
    expect(bindings).toHaveLength(1);
    expect(bindings[0].representation).toBe("Ssyk3");
  });

  it("two distinct non-null representations of the SAME variable coexist as separate bindings (bindingMatches' both-non-null branch), and re-adding one IS a duplicate", () => {
    projectStore.newProject(SEED);
    // Add the 3-digit extraction of SSYK.
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    // Add the 4-digit extraction of the SAME variable with NO intervening remove.
    // bindingMatches compares both non-null reps exactly (Ssyk4 !== Ssyk3), so this
    // is a distinct extraction — NOT a duplicate — and lands a SECOND binding.
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk4" }),
      ],
    });
    expect(storedProject()?.sources).toHaveLength(1);
    // BOTH extractions coexist on the one source, in add order.
    expect(
      storedProject()?.sources[0].bindings.map((b) => b.representation),
    ).toEqual(["Ssyk3", "Ssyk4"]);

    // Re-adding one of the now-coexisting reps (Ssyk3) DOES match its existing
    // binding → the guard drops it (no third binding), even though its sibling
    // Ssyk4 is a distinct live representation of the same variable.
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    expect(
      storedProject()?.sources[0].bindings.map((b) => b.representation),
    ).toEqual(["Ssyk3", "Ssyk4"]);
  });

  it("remove+add of the SAME register_variant in one batch preserves the source (name + merged period, not a fresh source) — review Fix 2", () => {
    projectStore.newProject(SEED);
    // Seed a source with a single binding at 2015..2020, then give it a user-set name
    // (so we can prove the source object survives the swap, not just its coordinate).
    projectStore.applyStagedDiff({
      adds: [
        add(
          "scb/lisa/v1",
          "scb/lisa/ssyk",
          { from: 2015, to: 2020 },
          {
            representation: "Ssyk3",
          },
        ),
      ],
    });
    // Set a user name via the sources mutator (the store's public edit path).
    const named = (storedProject()?.sources ?? []).map((s, i) =>
      i === 0 ? { ...s, name: "My cohort" } : s,
    );
    projectStore.updateField("sources", named);
    expect(storedProject()?.sources[0].name).toBe("My cohort");

    // ONE batch that removes the source's ONLY binding AND adds a binding for the
    // SAME register_variant (a representation swap). Pre-fix the removes phase would
    // prune the emptied source immediately, so the add would mint a FRESH source and
    // LOSE the user's name + the source's existing period. The deferred prune keeps
    // the source: the add refills it.
    projectStore.applyStagedDiff({
      removes: [
        {
          registerVariant: "scb/lisa/v1",
          variable: "scb/lisa/ssyk",
          representation: "Ssyk3",
        },
      ],
      adds: [
        add(
          "scb/lisa/v1",
          "scb/lisa/ssyk",
          { from: 2005, to: 2010 },
          {
            representation: "Ssyk4",
          },
        ),
      ],
    });

    // Still ONE source, and it is the SAME source (user name preserved).
    expect(storedProject()?.sources).toHaveLength(1);
    expect(storedProject()?.sources[0].name).toBe("My cohort");
    // Its period MERGED the add's disjoint window into the pre-existing one (the
    // find-or-create found the still-present source), rather than resetting to just
    // the add's window (which a fresh newSource would have done).
    expect(storedProject()?.sources[0].period).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
    // The old binding is gone, the new one landed.
    expect(
      storedProject()?.sources[0].bindings.map((b) => b.representation),
    ).toEqual(["Ssyk4"]);
  });

  it("a null-representation remove matches the variable's binding regardless of stored column", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    // The remove carries NO representation → the null-either-side rule matches the
    // stored Ssyk3 binding and drops it (pruning the emptied source).
    projectStore.applyStagedDiff({
      removes: [{ registerVariant: "scb/lisa/v1", variable: "scb/lisa/ssyk" }],
    });
    expect(storedProject()?.sources).toHaveLength(0);
  });

  it("commits the whole batch in ONE mutation (id mirror rebuilt once, autosave fires once)", async () => {
    vi.useFakeTimers();
    const saves: unknown[] = [];
    setPersistence({
      save: (_k, d) => {
        saves.push(d);
        return Promise.resolve();
      },
      load: () => Promise.resolve(null),
    });
    projectStore.newProject(SEED);
    const stop = $effect.root(() => {
      initDraftLifecycle();
    });
    await vi.advanceTimersByTimeAsync(600);
    saves.length = 0; // ignore the newProject autosave

    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", 2018),
        add("scb/rtb/v1", "scb/rtb/fodelsear", 2018),
      ],
    });
    // Both sources exist with fresh, distinct stable ids (the mirror rebuilt once).
    expect(projectStore.sourceId(0)).toMatch(/^c\d+$/);
    expect(projectStore.sourceId(1)).toMatch(/^c\d+$/);
    expect(projectStore.sourceId(0)).not.toBe(projectStore.sourceId(1));
    expect(projectStore.bindingId(0, 0)).toBeTruthy();

    // The debounced autosave collapses the single batch mutation into ONE write.
    await vi.advanceTimersByTimeAsync(600);
    expect(saves).toHaveLength(1);
    vi.useRealTimers();
    stop();
  });
});

describe("applySourcePeriodEdit (Y-81 — the cart card's period-only rewrite)", () => {
  /** A draft with TWO differently named sources on ONE register variant, the
   * shape only an imported spec produces (the catalog's add path finds-or-creates
   * by variant). `LISA` carries two columns; `LISA_2` is the sibling that must
   * not move. */
  function twoSourceDraft(): void {
    projectStore.newProject(SEED);
    projectStore.updateField("sources", [
      {
        name: "LISA",
        register_variant: "scb/lisa/individer",
        period: { from: 2010, to: 2015 },
        bindings: [
          {
            variable: "scb/lisa/kon",
            type: "categorical",
            representation: "Kon",
          },
          { variable: "scb/lisa/alder", type: "numeric" },
        ],
      },
      {
        name: "LISA_2",
        register_variant: "scb/lisa/individer",
        period: 2009,
        bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
      },
    ]);
  }

  /** An edit of `sourceName` started from its current value, proposing `period`. */
  function edit(sourceName: string, period: StagedAdd["period"]) {
    const source = (storedProject()?.sources ?? []).find(
      (s) => s.name === sourceName,
    );
    return {
      sourceName,
      registerVariant: "scb/lisa/individer",
      period,
      snapshot: sourceSnapshot($state.snapshot(source)),
      replacementGeneration: projectStore.replacementGeneration,
    };
  }

  it("refuses an edit from a project that has since been replaced", () => {
    twoSourceDraft();
    const stale = edit("LISA", { from: 2012, to: 2014 });
    // A New/Open bumps `replacementGeneration`; the edited source could well
    // exist identically in the replacement, and this edit is still not its.
    projectStore.newProject(SEED);
    projectStore.updateField("sources", [
      {
        name: "LISA",
        register_variant: "scb/lisa/individer",
        period: { from: 2010, to: 2015 },
        bindings: [
          {
            variable: "scb/lisa/kon",
            type: "categorical",
            representation: "Kon",
          },
          { variable: "scb/lisa/alder", type: "numeric" },
        ],
      },
    ]);

    expect(projectStore.applySourcePeriodEdit(stale)).toBe(false);
    expect(storedProject()?.sources[0].period).toEqual({
      from: 2010,
      to: 2015,
    });
  });
});
