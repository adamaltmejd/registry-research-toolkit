import { describe, expect, it } from "vitest";
import {
  addBinding,
  addSource,
  defaultSourceName,
  isPrefilledSourceName,
  MODEL_A_SCHEMA_VERSION,
  newProjectData,
  type ProjectData,
  regMetaReleaseTag,
  removeBinding,
  removeSource,
  type Source,
  serializeProjectData,
  uniqueSourceName,
  updateBinding,
  updateField,
  updateSource,
} from "./project_data";

const SEED = { reg_meta_version: "reg_meta/v1.0.0", steward: "global" };

describe("newProjectData", () => {
  it("seeds the Model A skeleton from the seed", () => {
    const draft = newProjectData(SEED);
    expect(draft).toEqual({
      schema_version: MODEL_A_SCHEMA_VERSION,
      steward: "global",
      reg_meta_version: "reg_meta/v1.0.0",
      name: "",
      sources: [],
    });
    expect(MODEL_A_SCHEMA_VERSION).toBe("2.0.0");
  });
});

describe("regMetaReleaseTag", () => {
  it("prefixes a bare package version into the canonical reg_meta/v release tag", () => {
    // A new project must carry a `reg_meta/v1.x.y` release tag (see
    // reg_meta/DESIGN.md → Release tags and distribution), derived
    // from the deployment's bare `context.webapp.reg_meta_version`.
    expect(regMetaReleaseTag("1.0.0")).toBe("reg_meta/v1.0.0");
    expect(regMetaReleaseTag("1.9.4")).toBe("reg_meta/v1.9.4");
  });

  it("maps an empty version to empty (context not yet resolved)", () => {
    expect(regMetaReleaseTag("")).toBe("");
  });
});

describe("immutable top-level edits", () => {
  it("updateField returns a new object with the field replaced", () => {
    const draft = newProjectData(SEED);
    const next = updateField(draft, "name", "My project");
    expect(next.name).toBe("My project");
    expect(draft.name).toBe(""); // original untouched
    expect(next).not.toBe(draft);
  });
});

describe("immutable source edits", () => {
  it("addSource appends an empty source skeleton", () => {
    const draft = addSource(newProjectData(SEED));
    expect(draft.sources).toHaveLength(1);
    expect(draft.sources[0]).toEqual({
      name: "",
      register_variant: "",
      period: "",
      bindings: [],
    });
  });

  it("coerces a malformed non-array `sources` to [] (review #280 — no char-spread, no throw)", () => {
    // An opened spec can carry a malformed `sources: "not-an-array"` (kept verbatim
    // for serialize/validate). A structural edit must match the editors' coercion
    // doctrine: start from [] rather than spreading the string into char "sources"
    // (the old `[...draft.sources, …]` bug) or throwing on `.map`/`.filter`.
    const malformed = {
      ...newProjectData(SEED),
      sources: "not-an-array" as unknown as ProjectData["sources"],
    };
    const added = addSource(malformed);
    expect(added.sources).toHaveLength(1);
    expect(added.sources[0].name).toBe("");
    // The array-reading mutators no longer throw on the malformed input either.
    expect(() => removeSource(malformed, 0)).not.toThrow();
    expect(removeSource(malformed, 0).sources).toEqual([]);
    expect(() => addBinding(malformed, 0)).not.toThrow();
    expect(() => removeBinding(malformed, 0, 0)).not.toThrow();
  });

  it("removeSource drops the source at the index", () => {
    let draft = newProjectData(SEED);
    draft = addSource(draft);
    draft = updateSource(draft, 0, { name: "first" });
    draft = addSource(draft);
    draft = updateSource(draft, 1, { name: "second" });
    const next = removeSource(draft, 0);
    expect(next.sources).toHaveLength(1);
    expect(next.sources[0].name).toBe("second");
  });

  it("updateSource shallow-merges the patch, preserving bindings + unmapped keys", () => {
    let draft = newProjectData(SEED);
    draft = addSource(draft);
    // Seed an unmapped key + a binding on the source.
    draft = updateSource(draft, 0, {
      name: "s1",
      register_variant: "scb/lisa/individer",
      extra_key: "kept",
    } as Partial<ProjectData["sources"][number]>);
    draft = addBinding(draft, 0);
    const next = updateSource(draft, 0, { period: 2020 });
    expect(next.sources[0].period).toBe(2020);
    expect(next.sources[0].name).toBe("s1"); // preserved
    expect(next.sources[0].bindings).toHaveLength(1); // preserved
    expect((next.sources[0] as Record<string, unknown>).extra_key).toBe("kept"); // unmapped key preserved
  });
});

describe("source-name prefill helpers (#312)", () => {
  const src = (name: string): Source => ({
    name,
    register_variant: "",
    period: "",
    bindings: [],
  });

  it("defaultSourceName uppercases the register slug (segment 2)", () => {
    expect(defaultSourceName("scb/lisa/v1")).toBe("LISA");
    expect(defaultSourceName("scb/rtb/v2")).toBe("RTB");
    // A 2-seg prefix already carries the register slug.
    expect(defaultSourceName("scb/lisa")).toBe("LISA");
  });

  it("defaultSourceName is empty without a register segment", () => {
    expect(defaultSourceName("")).toBe("");
    expect(defaultSourceName("scb")).toBe("");
  });

  it("isPrefilledSourceName accepts empty, the default, and a suffixed default", () => {
    expect(isPrefilledSourceName("", "scb/lisa/v1")).toBe(true);
    expect(isPrefilledSourceName("LISA", "scb/lisa/v1")).toBe(true);
    expect(isPrefilledSourceName("LISA_2", "scb/lisa/v1")).toBe(true);
    expect(isPrefilledSourceName("LISA_10", "scb/lisa/v1")).toBe(true);
  });

  it("isPrefilledSourceName rejects user-entered names", () => {
    expect(isPrefilledSourceName("my source", "scb/lisa/v1")).toBe(false);
    expect(isPrefilledSourceName("lisa", "scb/lisa/v1")).toBe(false); // case-sensitive
    expect(isPrefilledSourceName("LISA_x", "scb/lisa/v1")).toBe(false);
    expect(isPrefilledSourceName("LISAx", "scb/lisa/v1")).toBe(false);
    // No register segment → only the empty name counts as prefilled.
    expect(isPrefilledSourceName("LISA", "scb")).toBe(false);
  });

  it("uniqueSourceName suffixes _2, _3 … on collision (case-sensitive)", () => {
    expect(uniqueSourceName([src("RTB")], "LISA", 1)).toBe("LISA");
    expect(uniqueSourceName([src("LISA")], "LISA", 1)).toBe("LISA_2");
    expect(uniqueSourceName([src("LISA"), src("LISA_2")], "LISA", 2)).toBe(
      "LISA_3",
    );
    // Case differs → no collision (the schema compares case-sensitively).
    expect(uniqueSourceName([src("lisa")], "LISA", 1)).toBe("LISA");
  });

  it("uniqueSourceName ignores the source being named itself", () => {
    expect(uniqueSourceName([src("LISA")], "LISA", 0)).toBe("LISA");
  });
});

describe("immutable binding edits", () => {
  function draftWithSource(): ProjectData {
    return addSource(newProjectData(SEED));
  }

  it("addBinding appends an empty binding to the source", () => {
    const draft = addBinding(draftWithSource(), 0);
    expect(draft.sources[0].bindings).toHaveLength(1);
    expect(draft.sources[0].bindings[0]).toEqual({ variable: "", type: "" });
  });

  it("removeBinding drops the binding at the index", () => {
    let draft = draftWithSource();
    draft = addBinding(draft, 0);
    draft = updateBinding(draft, 0, 0, { variable: "scb/lisa/kon" });
    draft = addBinding(draft, 0);
    draft = updateBinding(draft, 0, 1, { variable: "scb/lisa/alder" });
    const next = removeBinding(draft, 0, 0);
    expect(next.sources[0].bindings).toHaveLength(1);
    expect(next.sources[0].bindings[0].variable).toBe("scb/lisa/alder");
  });

  it("updateBinding shallow-merges, preserving unmapped binding keys", () => {
    let draft = draftWithSource();
    draft = addBinding(draft, 0);
    draft = updateBinding(draft, 0, 0, {
      variable: "scb/lisa/kon",
      type: "categorical",
      id_subtype: "string",
    });
    const next = updateBinding(draft, 0, 0, { type: "id" });
    expect(next.sources[0].bindings[0].type).toBe("id");
    expect(next.sources[0].bindings[0].variable).toBe("scb/lisa/kon"); // preserved
    expect(next.sources[0].bindings[0].id_subtype).toBe("string"); // unmapped key
  });
});

describe("serializeProjectData", () => {
  it("preserves unmapped top-level keys (panels, namespaced blocks) verbatim", () => {
    const draft = {
      ...newProjectData(SEED),
      name: "p",
      panels: [{ panel_id: "panel1", members: [{ source: "s1" }] }],
      reg_monabundle: {
        binding_options: { "scb/lisa/kon": { suppress_k: 5 } },
      },
      swecov: { foo: "bar" },
    } as unknown as ProjectData;
    const text = serializeProjectData(draft);
    const roundTripped = JSON.parse(text);
    expect(roundTripped.panels).toEqual(draft.panels);
    expect(roundTripped.reg_monabundle).toEqual(draft.reg_monabundle);
    expect(roundTripped.swecov).toEqual({ foo: "bar" });
  });

  it("is stable (pretty 2-space, insertion order preserved)", () => {
    const draft = updateField(newProjectData(SEED), "name", "p");
    const text = serializeProjectData(draft);
    expect(text).toBe(JSON.stringify(draft, null, 2));
    // Re-serializing the same draft is byte-identical (the dirty baseline relies
    // on this).
    expect(serializeProjectData(draft)).toBe(text);
  });
});
