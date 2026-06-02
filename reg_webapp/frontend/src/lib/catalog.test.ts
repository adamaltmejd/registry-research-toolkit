import { describe, expect, it } from "vitest";
import type { CatalogNode } from "./api";
import {
  breadcrumbs,
  fqidSegments,
  isBinding,
  narrowNode,
  nodeLabel,
} from "./catalog";

// Minimal fixtures for each `kind` arm — only the fields the helpers read.
const provider = {
  kind: "provider",
  fqid: "scb",
  name: "Statistics Sweden",
} as CatalogNode;
const register = {
  kind: "register",
  fqid: "scb/lisa",
  name: null,
  purpose: null,
  children: [],
} as unknown as CatalogNode;
const binding = {
  kind: "binding",
  fqid: "scb/lisa/kon",
  name: "Sex",
} as unknown as CatalogNode;
const classification = {
  kind: "classification",
  fqid: "class/sun2020",
  name: "Education",
  short_name: "SUN",
} as unknown as CatalogNode;

describe("narrowNode", () => {
  it("returns the node when the kind matches", () => {
    const narrowed = narrowNode(binding, "binding");
    expect(narrowed).not.toBeNull();
    expect(narrowed?.fqid).toBe("scb/lisa/kon");
  });

  it("returns null when the kind differs", () => {
    expect(narrowNode(provider, "binding")).toBeNull();
    expect(narrowNode(binding, "provider")).toBeNull();
  });
});

describe("isBinding", () => {
  it("is true only for a binding leaf", () => {
    expect(isBinding(binding)).toBe(true);
    expect(isBinding(provider)).toBe(false);
    expect(isBinding(register)).toBe(false);
  });
});

describe("nodeLabel", () => {
  it("uses name when present, else falls back to fqid", () => {
    expect(nodeLabel(provider)).toBe("Statistics Sweden");
    expect(nodeLabel(register)).toBe("scb/lisa"); // name is null → fqid
    expect(nodeLabel(classification)).toBe("Education");
  });
});

describe("fqidSegments / breadcrumbs", () => {
  it("splits an fqid path into segments, [] for the root", () => {
    expect(fqidSegments("scb/lisa/kon")).toEqual(["scb", "lisa", "kon"]);
    expect(fqidSegments("")).toEqual([]);
  });

  it("builds a cumulative breadcrumb trail", () => {
    expect(breadcrumbs("scb/lisa/kon")).toEqual([
      { label: "scb", fqidPath: "scb" },
      { label: "lisa", fqidPath: "scb/lisa" },
      { label: "kon", fqidPath: "scb/lisa/kon" },
    ]);
  });
});
