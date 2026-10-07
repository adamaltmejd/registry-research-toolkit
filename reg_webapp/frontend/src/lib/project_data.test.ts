import { describe, expect, it } from "vitest";
// reg_schema's package version IS the project schema_version it validates, so a new
// draft must carry exactly that version — read from the package, not re-typed here.
import regSchemaPyproject from "../../../../reg_schema/pyproject.toml?raw";
import {
  newProjectData,
  regMetaReleaseTag,
  type Source,
  uniqueSourceName,
} from "./project_data";

const SEED = { reg_meta_version: "reg_meta/v1.0.0", steward: "global" };

/** A well-formed source fixture (the pure edits no longer include an `addSource`
 * skeleton constructor — the store's catalog-add path builds sources now). */
function source(over: Partial<Source> = {}): Source {
  return { name: "", register_variant: "", period: "", bindings: [], ...over };
}

describe("newProjectData", () => {
  it("seeds the skeleton at reg_schema's schema version", () => {
    const regSchemaVersion = /^version = "([^"]+)"$/m.exec(
      regSchemaPyproject,
    )?.[1];
    expect(regSchemaVersion).toBeDefined();
    expect(newProjectData(SEED)).toEqual({
      schema_version: regSchemaVersion,
      steward: "global",
      reg_meta_version: "reg_meta/v1.0.0",
      name: "",
      sources: [],
    });
  });
});

describe("regMetaReleaseTag", () => {
  it("prefixes a bare package version into the canonical reg_meta/v release tag", () => {
    // A new project carries the deployment's reg_meta release tag (see
    // reg_meta/DESIGN.md → Release tags and distribution), derived from the
    // bare `context.webapp.reg_meta_version`.
    expect(regMetaReleaseTag("1.0.0")).toBe("reg_meta/v1.0.0");
    expect(regMetaReleaseTag("1.9.4")).toBe("reg_meta/v1.9.4");
  });
});

describe("source-name prefill helpers (#312)", () => {
  const src = (name: string): Source => source({ name });

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
