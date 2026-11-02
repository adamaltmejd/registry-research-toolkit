import { describe, expect, it } from "vitest";
import { isMacPlatform } from "./platform";

describe("isMacPlatform (injected navigator stub)", () => {
  it("reads UA-Client-Hints `userAgentData.platform` first", () => {
    expect(isMacPlatform({ userAgentData: { platform: "macOS" } })).toBe(true);
    expect(isMacPlatform({ userAgentData: { platform: "Win32" } })).toBe(false);
  });

  it("falls back to the legacy `navigator.platform` when UA-CH is absent", () => {
    expect(isMacPlatform({ platform: "MacIntel" })).toBe(true);
    expect(isMacPlatform({ platform: "Linux x86_64" })).toBe(false);
  });
});
