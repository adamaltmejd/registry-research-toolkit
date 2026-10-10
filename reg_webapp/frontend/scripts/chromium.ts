// How this machine must launch Playwright's Chromium: shared by the browser test
// project (vite.config.ts) and the build smoke (scripts/smoke_build.ts).
//
// Playwright 1.61's Chromium build registers a Mach rendezvous port on macOS. The
// Codex seatbelt sandbox denies that registration; single-process Chromium avoids
// the blocked multi-process bootstrap. Normal local runs and Linux CI keep the
// standard browser path. The env is read via globalThis so the frontend needs no
// @types/node for one lookup.
const runtimeProcess = (
  globalThis as {
    process?: { env?: Record<string, string | undefined>; platform?: string };
  }
).process;

export const needsSingleProcessChromium =
  runtimeProcess?.platform === "darwin" &&
  runtimeProcess?.env?.CODEX_SANDBOX === "seatbelt";

export const chromiumLaunchArgs = needsSingleProcessChromium
  ? ["--single-process"]
  : [];
