/**
 * Platform detection for the command-bar shortcut (#803): macOS uses ⌘ (Meta),
 * everything else uses Ctrl. Kept as a tiny pure helper so both branches are
 * testable — a test injects the platform string instead of stubbing `navigator`.
 *
 * Prefer the modern `navigator.userAgentData.platform` (a plain platform string,
 * not the deprecated, increasingly-frozen `navigator.platform` / userAgent UA
 * sniff) and fall back to `navigator.platform` where UA-Client-Hints aren't
 * available (Safari/Firefox today). Both are matched case-insensitively against
 * `mac` (covers "macOS", "MacIntel", …).
 */

/** A structural view of the bits of `navigator` we read — so a test can pass a
 * stub for either branch without a full `Navigator`. */
export interface PlatformNavigator {
  userAgentData?: { platform?: string } | null;
  platform?: string;
}

/** Whether the given platform string names macOS. Pure + injectable: the source
 * of the string (real navigator vs. a test stub) is the caller's concern. */
export function platformIsMac(platform: string | null | undefined): boolean {
  return /mac/i.test(platform ?? "");
}

/** Whether the running browser is on macOS. Reads UA-Client-Hints first, then
 * the legacy `navigator.platform`. Defaults to the global `navigator`; a test
 * passes a stub `nav` to drive either branch. */
export function isMacPlatform(
  nav: PlatformNavigator | undefined = typeof navigator === "undefined"
    ? undefined
    : navigator,
): boolean {
  const platform = nav?.userAgentData?.platform ?? nav?.platform;
  return platformIsMac(platform);
}

/** The command-bar shortcut hint badge for the current (or given) platform:
 * `⌘K` on macOS, `Ctrl+K` elsewhere. */
export function commandShortcutHint(mac: boolean): string {
  return mac ? "⌘K" : "Ctrl+K";
}
