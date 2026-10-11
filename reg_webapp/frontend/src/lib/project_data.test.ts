import { describe, expect, it } from "vitest";
import {
  newProjectData,
  type ProjectSource,
  regMetaReleaseTag,
  uniqueSourceName,
} from "./project_data";
import { checkProject } from "./reg_core";

const SEED = { reg_meta_version: "reg_meta/v1.0.0", steward: "global" };

/** A well-formed source fixture (the pure edits no longer include an `addSource`
 * skeleton constructor — the store's catalog-add path builds sources now). */
function source(over: Partial<ProjectSource> = {}): ProjectSource {
  return { name: "", register_variant: "", period: "", bindings: [], ...over };
}

describe("newProjectData", () => {
  // Fails when a new draft is seeded at a version the server's version decision
  // rejects (a hard-coded version left behind by a schema bump).
  it("seeds a skeleton the server's version decision accepts", () => {
    const draft = newProjectData(SEED);
    expect(draft).toEqual({
      schema_version: expect.any(String),
      steward: "global",
      reg_meta_version: "reg_meta/v1.0.0",
      name: "",
      sources: [],
    });
    expect(
      checkProject(JSON.stringify(draft)).issues.map((i) => i.code),
    ).not.toContain("unsupported_schema_version");
  });
});

describe("regMetaReleaseTag", () => {
  it("prefixes a bare package version into the canonical reg_meta/v release tag", () => {
    // A new project carries the deployment's reg_meta release tag (see
    // crates/DESIGN.md → Versioning and determinism), derived from the
    // bare `context.webapp.reg_meta_version`.
    expect(regMetaReleaseTag("1.0.0")).toBe("reg_meta/v1.0.0");
    expect(regMetaReleaseTag("1.9.4")).toBe("reg_meta/v1.9.4");
  });
});

describe("source-name prefill helpers (#312)", () => {
  const src = (name: string): ProjectSource => source({ name });

  it("uniqueSourceName suffixes _2, _3 … on collision (case-sensitive)", () => {
    expect(uniqueSourceName([src("RTB")], "LISA", 1)).toBe("LISA");
    expect(uniqueSourceName([src("LISA")], "LISA", 1)).toBe("LISA_2");
    expect(uniqueSourceName([src("LISA"), src("LISA_2")], "LISA", 2)).toBe(
      "LISA_3",
    );
    // Case differs → no collision (the schema compares case-sensitively).
    expect(uniqueSourceName([src("lisa")], "LISA", 1)).toBe("LISA");
  });
});
