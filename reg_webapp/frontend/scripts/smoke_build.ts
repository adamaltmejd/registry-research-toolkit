// The production build's bootstrap, before merge (`scripts/gate.sh flows`, after
// `bun run build`): Vite's preview server serves dist/, and Chrome, driven by
// agent-browser, checks that
//   1. the app mounts (its "Catalog" navigation, the browse tree, renders),
//   2. the reg-core .wasm answers 200 with `application/wasm`,
//   3. with the .wasm request aborted, the bootstrap alert renders in #app,
// after checking that dist/_headers stamps /assets/* immutable and that every file
// there is named by its content hash, which is what makes that rule safe (#1454).
// It prints the mount time (navigation start to the mounted app) for the record.
// The API is aborted throughout: the bootstrap needs no server.
//
//   bun scripts/smoke_build.ts
import { preview } from "vite";

const BOOTSTRAP_ALERT = "The app could not load one of its parts";
const CATALOG_NAV = 'nav[aria-label="Catalog"]';
const AGENT_BROWSER = `${import.meta.dir}/../node_modules/agent-browser/bin/agent-browser.js`;

function check(ok: boolean, what: string): void {
  if (!ok) throw new Error(`smoke: ${what}`);
  console.log(`smoke: ok — ${what}`);
}

/** One agent-browser command in `session`; its stdout, or a thrown error. Async: the
 * preview server runs on this process's event loop, which a sync spawn would block. */
async function ab(session: string, ...args: string[]): Promise<string> {
  const run = Bun.spawn(
    [
      "bun",
      AGENT_BROWSER,
      "--session",
      session,
      "--idle-timeout",
      "2m",
      ...args,
    ],
    { stdout: "pipe", stderr: "pipe" },
  );
  const [out, err, code] = await Promise.all([
    new Response(run.stdout).text(),
    new Response(run.stderr).text(),
    run.exited,
  ]);
  if (code !== 0)
    throw new Error(`smoke: agent-browser ${args.join(" ")}: ${out}${err}`);
  return out;
}

/** A fresh session that aborts the API (and `alsoAbort`) from the first navigation. */
async function open(
  session: string,
  base: string,
  alsoAbort?: string,
): Promise<void> {
  await ab(session, "open");
  await ab(session, "network", "route", "**/api/**", "--abort");
  if (alsoAbort) await ab(session, "network", "route", alsoAbort, "--abort");
  await ab(session, "open", base);
}

const DIST = `${import.meta.dir}/../dist`;
const IMMUTABLE_ASSETS =
  "/assets/*\n  Cache-Control: public, max-age=31536000, immutable";
check(
  (await Bun.file(`${DIST}/_headers`).text()).includes(IMMUTABLE_ASSETS),
  "dist/_headers stamps /assets/* immutable",
);
// Vite names an emitted file `<name>-<8-character content hash>.<ext>`.
const unhashed = [...new Bun.Glob("*").scanSync(`${DIST}/assets`)].filter(
  (name) => !/-[\w-]{8}\.\w+$/.test(name),
);
check(
  unhashed.length === 0,
  `every dist/assets file is content-hashed${unhashed.length ? ` (not: ${unhashed.join(", ")})` : ""}`,
);

// In process, not a child process: closing it in `finally` leaves nothing running.
const server = await preview({
  logLevel: "silent",
  preview: { host: "127.0.0.1", port: 0, strictPort: true, open: false },
});
const base = server.resolvedUrls?.local[0];
if (!base) {
  await server.close();
  throw new Error("smoke: vite preview reported no local URL");
}
// One session per page: each is its own browser, so the first page's cached .wasm
// cannot satisfy the second page's aborted load.
const sessions = [`smoke-${process.pid}`, `smoke-${process.pid}-broken`];
try {
  const [session, broken] = sessions;
  await open(session, base);
  await ab(session, "wait", CATALOG_NAV);
  const mountedMs = Number(await ab(session, "eval", "performance.now()"));
  const requests: {
    url: string;
    status: number;
    responseHeaders?: Record<string, string>;
  }[] = JSON.parse(
    await ab(session, "network", "requests", "--filter", ".wasm", "--json"),
  ).data.requests;
  const wasm = requests.find((r) => r.url.endsWith(".wasm"));
  const type = wasm?.responseHeaders?.["Content-Type"];
  check(
    wasm?.status === 200 && type === "application/wasm",
    `the .wasm answers 200 application/wasm (${wasm?.status} ${type})`,
  );
  check(
    true,
    `the app mounts (${mountedMs.toFixed(0)} ms after navigation start)`,
  );

  await open(broken, base, "**/*.wasm");
  await ab(broken, "wait", "--text", BOOTSTRAP_ALERT);
  check(
    (await ab(broken, "get", "count", `#app [role="alert"]`)).trim() !== "0" &&
      (await ab(broken, "get", "count", CATALOG_NAV)).trim() === "0",
    "with the .wasm aborted, the bootstrap alert renders and the app does not mount",
  );
} finally {
  for (const session of sessions) {
    await Bun.spawn(["bun", AGENT_BROWSER, "--session", session, "close"])
      .exited;
  }
  await server.close();
}
