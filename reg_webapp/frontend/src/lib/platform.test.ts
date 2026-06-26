import { describe, expect, it } from "vitest";
import { commandShortcutHint, isMacPlatform, platformIsMac } from "./platform";

describe("platformIsMac", () => {
  it("matches macOS platform strings (case-insensitively)", () => {
    expect(platformIsMac("macOS")).toBe(true);
    expect(platformIsMac("MacIntel")).toBe(true);
  });

  it("is false for non-mac / empty / nullish strings", () => {
    expect(platformIsMac("Win32")).toBe(false);
    expect(platformIsMac("")).toBe(false);
    expect(platformIsMac(null)).toBe(false);
    expect(platformIsMac(undefined)).toBe(false);
  });
});

describe("isMacPlatform (injected navigator stub)", () => {
  it("reads UA-Client-Hints `userAgentData.platform` first", () => {
    expect(isMacPlatform({ userAgentData: { platform: "macOS" } })).toBe(true);
    expect(isMacPlatform({ userAgentData: { platform: "Win32" } })).toBe(false);
  });

  it("falls back to the legacy `navigator.platform` when UA-CH is absent", () => {
    expect(isMacPlatform({ platform: "MacIntel" })).toBe(true);
    expect(isMacPlatform({ platform: "Linux x86_64" })).toBe(false);
  });

  it("is false for an empty stub or no navigator at all", () => {
    expect(isMacPlatform({})).toBe(false);
    expect(isMacPlatform(undefined)).toBe(false);
  });
});

describe("commandShortcutHint", () => {
  it("renders the ⌘ badge on mac and the Ctrl badge elsewhere", () => {
    expect(commandShortcutHint(true)).toBe("⌘K");
    expect(commandShortcutHint(false)).toBe("Ctrl+K");
  });
});
