#!/usr/bin/env python3
"""Deterministic pr-pipeline launcher for the chief-of-staff `auto` dispatch mode.

On a `dispatch:` wake (cos_watch.py emits it once a freed slot leaves the pipeline
ledger below --max-slots), the chief-of-staff in `auto` mode runs THIS script once per
lane it decides to dispatch, instead of only recommending commands. The script is the
side-effecting half of that decision: it claims a slot, materializes a worktree, and
launches a detached pr-pipeline agent (codex or claude) on the given issues.

It is deliberately dumb about WHICH lane to run — the agent (or /plan-lanes) picks the
issues; this script just performs the launch atomically and records the claim. Order of
operations is chosen so a failure never leaks a slot: the slot file is written LAST,
only after the agent process is spawned. A failure after the worktree is created leaks
only the worktree, which the error names for adjudication.

Steps (fail-fast between each):
  1. kill switch  — <state-root>/auto-dispatch.off present ⇒ refuse (exit 3).
  2. budget       — busy slots >= --max-slots ⇒ refuse (exit 4).
  3. collision    — slot file OR worktree dir already exists ⇒ refuse (exit 2).
  4. worktree     — `git fetch origin main` then `git worktree add -b wt/<slug> …
                    origin/main`, run in the canonical checkout. `wt/<slug>` is only the
                    placeholder branch git requires; the pipeline skill creates its real
                    `s/<slug>` branch from inside the worktree.
  5. launch       — spawn the agent DETACHED (start_new_session=True, stdin closed,
                    stdout+stderr appended to <state-root>/dispatch-logs/<slug>.log). We
                    do not wait; the process re-parents when this session exits.
  6. session id   — claude: the uuid4 we pre-generated. codex: parsed from the JSONL log
                    (first event carrying a thread/session id) with a bounded poll; on
                    timeout, session=null (the chief's fuzzy-thread-search fallback
                    covers it) plus a stderr warning.
  7. slot file    — atomic write of <state-root>/pipeline-slots/<slug>.json. The `slot`
                    field MUST equal the filename stem or scan_slots treats it as absent.

Exit codes: 0 launched (or --dry-run OK); 2 usage/collision/tool error; 3 kill switch;
4 no free slot budget.

Stores match the sibling cos_* scripts: --state-root defaults to the parent of
cos_preflight.default_gate_root() ($XDG_STATE_HOME/registry-research-toolkit), so the
pipeline-slots ledger, merge-gates, and dispatch logs all sit together and a --state-root
override moves them as one. Like cos_preflight, the script must run from the canonical
main checkout (reuse require_canonical, with the same --canonical/--no-canonical-check
escape for tests).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

DEFAULT_SURFACE = "codex"
# Bounded poll for the codex thread id in its JSONL log. 30s ceiling; a null session is
# a tolerated outcome (the chief falls back to fuzzy thread search), so this never blocks
# the dispatch. Injectable via dispatch() args so the timeout test doesn't sleep 30s.
DEFAULT_CODEX_ID_TIMEOUT = 30.0
DEFAULT_CODEX_ID_POLL = 0.25


def _load_cos_preflight() -> ModuleType:
    # Spec-load the sibling like cos_watch.py does, so we reuse its store-root and slot
    # leaf helpers (default_gate_root, default_slots_root, scan_slots, DEFAULT_MAX_SLOTS,
    # require_canonical) instead of re-pasting them. Note: default_slots_root / scan_slots
    # / DEFAULT_MAX_SLOTS are hoisted into cos_preflight by the sibling cos_watch work;
    # this reference resolves at call time once that lands.
    spec = importlib.util.spec_from_file_location(
        "cos_preflight", Path(__file__).with_name("cos_preflight.py")
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_cos_preflight = _load_cos_preflight()


def default_state_root() -> Path:
    # Parent of the merge-gate root: $XDG_STATE_HOME/registry-research-toolkit. The slot
    # ledger, gate store, and dispatch logs all live under it, so a --state-root override
    # keeps them together (mirrors cos_watch's slots-dir = gate-dir.parent default).
    return _cos_preflight.default_gate_root().parent


def parse_issues(raw: str) -> list[int]:
    """Comma-separated issue numbers → sorted-input-order list of ints (fail-fast)."""
    issues: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            issues.append(int(token))
        except ValueError:
            raise SystemExit(f"invalid issue number {token!r} in --issues {raw!r}")
    if not issues:
        raise SystemExit("--issues must name at least one issue number")
    return issues


def default_slug(surface: str, issues: list[int]) -> str:
    return f"auto-{surface}-issue-{issues[0]}"


def validate_slug(slug: str) -> None:
    # The slot filename is <slug>.json and scan_slots requires slot==stem, so the slug
    # must be a clean single-path-component stem — no separators, no traversal.
    if not slug or "/" in slug or slug in {".", ".."} or slug != Path(slug).name:
        raise SystemExit(f"invalid --slug {slug!r}: must be a valid filename stem")


def build_launch_argv(
    surface: str,
    worktree: Path,
    issues: list[int],
    state_root: Path,
    session_id: str | None,
) -> list[str]:
    """The exact detached launch argv for the chosen surface.

    The pr-pipeline prompt is ONE string argument — `$pr-pipeline 1011 1012` (codex) /
    `/pr-pipeline 1011 1012` (claude) — with issues space-separated.
    """
    issues_arg = " ".join(str(n) for n in issues)
    if surface == "codex":
        return [
            "codex",
            "exec",
            "-C",
            str(worktree),
            "-s",
            "workspace-write",
            "-c",
            "approval_policy=never",
            "--add-dir",
            str(state_root),
            "--json",
            f"$pr-pipeline {issues_arg}",
        ]
    # claude: the session id is pre-generated so we know it before launch.
    assert session_id is not None
    return [
        "claude",
        "--session-id",
        session_id,
        "-p",
        f"/pr-pipeline {issues_arg}",
        "--dangerously-skip-permissions",
    ]


# Observed codex 0.142.5 JSONL (from `codex exec --json --ephemeral -s read-only`):
#   {"type":"thread.started","thread_id":"019f2334-4455-70a1-bc1b-2e86d5ecfccf"}
#   {"type":"turn.started"}
#   {"type":"item.completed","item":{...}}
#   {"type":"turn.completed","usage":{...}}
# The session/thread id is `thread_id` on the FIRST event. We take the id from the first
# event that carries one (thread_id preferred, session_id/id tolerated) and ignore
# unknown event types, so a future rename of the leading event can't strand the parser.
_ID_KEYS = ("thread_id", "session_id", "id")


def _extract_session_id(line: str) -> str | None:
    try:
        event = json.loads(line)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    for key in _ID_KEYS:
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def poll_codex_session_id(
    log_path: Path, timeout: float, poll_interval: float
) -> str | None:
    """Tail the JSONL dispatch log for the codex thread id, bounded by timeout.

    Returns the id, or None if none appeared within `timeout` (the caller warns and
    records session=null; the chief's fuzzy thread search covers a null session).
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            text = ""
        for line in text.splitlines():
            session_id = _extract_session_id(line)
            if session_id is not None:
                return session_id
        if time.monotonic() >= deadline:
            return None
        time.sleep(poll_interval)


def run_git(cwd: Path, args: list[str]) -> None:
    """Run a git command in the canonical checkout, failing fast (exit 2) with stderr.

    cwd is passed explicitly (not relied on as the ambient process cwd) so the fetch and
    worktree-add always act on the canonical repo — never on whatever directory the
    caller happens to be in, which under --no-canonical-check would corrupt an unrelated
    checkout.
    """
    proc = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)
    if proc.returncode != 0:
        tail = (
            proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed"
        )
        raise SystemExit(f"git {' '.join(args)} failed: {tail}")


def require_git_checkout(canonical: Path) -> None:
    """Refuse (exit 2) unless `canonical` is itself a git worktree root.

    Belt-and-braces before the mutating fetch/worktree-add: git's `-C <dir>` walks UP to
    the nearest enclosing repo, so a `canonical` that is NOT a checkout (an empty dir, a
    stray path) would silently target whatever repo sits above it — the exact class of
    accident that let a test's throwaway path resolve to the real checkout. Requiring the
    resolved `show-toplevel` to EQUAL `canonical` (true for a main checkout and for a
    linked worktree root, both valid dispatch origins) rejects that: an ambient parent
    repo has a different toplevel, and a non-repo dir makes rev-parse fail outright. This
    does not weaken the production contract — the real canonical checkout satisfies it.
    """
    proc = subprocess.run(
        ["git", "-C", str(canonical), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
    )
    toplevel = proc.stdout.strip()
    if proc.returncode != 0 or not toplevel:
        raise SystemExit(
            f"canonical checkout {canonical} is not a git worktree "
            f"({proc.stderr.strip() or 'rev-parse failed'})"
        )
    if Path(toplevel).resolve() != canonical.resolve():
        raise SystemExit(
            f"canonical checkout {canonical} is not a worktree root; git resolved it to "
            f"the enclosing repo {toplevel} — refusing to operate on an unintended repo"
        )


def write_slot_file(
    slot_path: Path,
    slug: str,
    issues: list[int],
    surface: str,
    session_id: str | None,
    pid: int,
) -> None:
    # Atomic write (temp file in the same dir + rename), mirroring cos_preflight.write_state
    # so scan_slots never sees a torn read. `slot` MUST equal the stem or readers drop it.
    slot_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "slot": slug,
        "issues": issues,
        "prs": [],
        "surface": surface,
        "session": session_id,
        "pid": pid,
        "dispatched": datetime.now(UTC).isoformat(),
    }
    with NamedTemporaryFile(
        "w",
        dir=slot_path.parent,
        encoding="utf-8",
        prefix=f"{slot_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as fh:
        tmp = Path(fh.name)
        fh.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    try:
        tmp.replace(slot_path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def launch_detached(argv: list[str], worktree: Path, log_path: Path) -> int:
    """Spawn the agent detached; return its pid. Never waits for completion.

    start_new_session=True detaches it into its own process group so it survives this
    session ending (it re-parents to init on our exit). stdin is closed (codex exec
    otherwise blocks reading stdin); stdout+stderr append to the per-slug log.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("a", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(worktree),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        raise SystemExit(f"failed to launch {argv[0]!r}: {exc}") from exc
    finally:
        # The child inherits its own dup of the fd; closing our handle is safe and avoids
        # leaking it into this process (also runs on the OSError path — no double close).
        log.close()
    return proc.pid


def dispatch(
    args: argparse.Namespace,
    *,
    codex_id_timeout: float = DEFAULT_CODEX_ID_TIMEOUT,
    codex_id_poll: float = DEFAULT_CODEX_ID_POLL,
) -> int:
    canonical = None if args.no_canonical_check else args.canonical
    _cos_preflight.require_canonical(canonical)

    issues = parse_issues(args.issues)
    surface = args.surface
    slug = args.slug or default_slug(surface, issues)
    validate_slug(slug)

    state_root: Path = args.state_root
    slots_root = state_root / "pipeline-slots"
    slot_path = slots_root / f"{slug}.json"
    worktree_root: Path = args.worktree_root or (
        args.canonical / ".claude" / "worktrees"
    )
    worktree = worktree_root / slug
    log_path = state_root / "dispatch-logs" / f"{slug}.log"

    # 1. Kill switch.
    kill_switch = state_root / "auto-dispatch.off"
    if kill_switch.exists():
        print(f"auto-dispatch disabled: kill switch present at {kill_switch}")
        return 3

    # 2. Slot budget.
    busy = len(_cos_preflight.scan_slots(slots_root)) if slots_root.is_dir() else 0
    if busy >= args.max_slots:
        print(f"no free slot budget: busy {busy}/{args.max_slots}")
        return 4

    # 3. Collision — an existing slot file or worktree dir means a concurrent claim.
    if slot_path.exists():
        raise SystemExit(f"slot collision: {slot_path} already exists")
    if worktree.exists():
        raise SystemExit(f"worktree collision: {worktree} already exists")

    # A pre-generated uuid is the claude session id (known before launch); codex's is
    # parsed from its JSONL log after launch.
    pre_session = str(uuid.uuid4()) if surface == "claude" else None
    argv = build_launch_argv(surface, worktree, issues, state_root, pre_session)

    if args.dry_run:
        # Checks 1–3 only; no worktree, no launch, no slot file, no log.
        print(
            json.dumps(
                {"launch_argv": argv, "slot_path": str(slot_path)},
                indent=2,
            )
        )
        return 0

    # 4. Worktree — placeholder wt/<slug> branch off a freshly fetched origin/main.
    canonical_repo: Path = args.canonical
    # Guard the mutating git ops: confirm canonical is a worktree ROOT so `git -C` can't
    # walk up into an unintended enclosing repo (see require_git_checkout).
    require_git_checkout(canonical_repo)
    run_git(canonical_repo, ["fetch", "origin", "main"])
    run_git(
        canonical_repo,
        ["worktree", "add", "-b", f"wt/{slug}", str(worktree), "origin/main"],
    )

    # 5. Launch detached. A launch failure after the worktree exists leaks only the
    # worktree (named in the error for adjudication); NO slot file is written.
    try:
        pid = launch_detached(argv, worktree, log_path)
    except SystemExit as exc:
        raise SystemExit(
            f"{exc.code} (worktree left at {worktree} for adjudication)"
        ) from exc

    # 6. Session id.
    if surface == "claude":
        session_id = pre_session
    else:
        session_id = poll_codex_session_id(log_path, codex_id_timeout, codex_id_poll)
        if session_id is None:
            print(
                f"warning: no codex session id in {log_path} after "
                f"{int(codex_id_timeout)}s; recording session=null "
                "(fuzzy thread search will recover it)",
                file=sys.stderr,
            )

    # 7. Slot file — written LAST, only after a successful launch, so a failed launch
    # never leaks a slot.
    write_slot_file(slot_path, slug, issues, surface, session_id, pid)

    print(
        json.dumps(
            {
                "slot": slug,
                "worktree": str(worktree),
                "surface": surface,
                "session": session_id,
                "pid": pid,
                "log": str(log_path),
            },
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--issues",
        required=True,
        help="comma-separated issue numbers for the pr-pipeline lane, e.g. 1011,1012",
    )
    ap.add_argument(
        "--surface",
        choices=("codex", "claude"),
        default=DEFAULT_SURFACE,
        help="dispatch surface (default: codex, the maintainer's default)",
    )
    ap.add_argument(
        "--slug",
        default=None,
        help="slot/worktree slug; defaults to auto-<surface>-issue-<first issue>",
    )
    ap.add_argument(
        "--worktree-root",
        type=Path,
        default=None,
        help="parent dir for the created worktree; defaults to <canonical>/.claude/worktrees",
    )
    ap.add_argument(
        "--state-root",
        type=Path,
        default=default_state_root(),
        help="state store root holding pipeline-slots/, dispatch-logs/, and the kill "
        "switch; defaults to $XDG_STATE_HOME/registry-research-toolkit",
    )
    ap.add_argument("--max-slots", type=int, default=_cos_preflight.DEFAULT_MAX_SLOTS)
    ap.add_argument(
        "--canonical",
        type=Path,
        default=_cos_preflight.DEFAULT_CANONICAL,
        help="required canonical checkout path (git commands run here)",
    )
    ap.add_argument(
        "--no-canonical-check",
        action="store_true",
        help="skip the canonical-checkout guard, for tests or local runs",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="run checks 1–3 only, print the launch argv + intended slot path as JSON, "
        "and exit 0 with zero side effects",
    )
    args = ap.parse_args(argv)
    try:
        return dispatch(args)
    except SystemExit as exc:
        message = exc.code if isinstance(exc.code, str) else ""
        if message:
            print(message, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
