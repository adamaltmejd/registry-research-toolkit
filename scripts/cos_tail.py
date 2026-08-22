#!/usr/bin/env python3
"""Live read-only viewer for dispatched pr-pipeline lanes (issue #1043).

`cos_dispatch.py` launches each lane detached with its full activity stream appended to
`<state-root>/dispatch-logs/<slug>.log` — `codex exec --json` JSONL for codex lanes,
plain `claude -p` text for claude lanes. This script renders those logs for a human;
it never writes to the ledger, the logs, or any other COS state.

  cos_tail.py status [--watch]     one-shot (or refreshing) table of live slots
  cos_tail.py follow <slug>        render one lane's dispatch log, live
  cos_tail.py follow --all         every live lane, prefix-multiplexed into one stream
  cos_tail.py tmux                 create/attach tmux session `cos`: a status/manager
                                   pane plus one auto-managed pane per live lane

Rendering policy (the noise contract): `agent_message` text is printed in full — it is
the narration spine. `command_execution` collapses to one dim line, with the output
tail printed only on a nonzero exit (`-v` always prints it). `file_change` is one line
of paths. Unknown item types render as a dim one-line headline so a future codex event
(e.g. reasoning items) degrades to a hint, never a flood. Non-JSON lines — child
stderr, claude-surface text — pass through verbatim.

Watching is deliberately log-based: resuming a live codex thread (`codex resume`)
could inject into or fork the running turn, while the dispatch log carries the same
content with zero interference.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Slot-ledger read rules live in cos_preflight (default root, tolerant JSON reads,
# stem-must-match-slot validity); reuse them so this viewer can never disagree with
# the watcher/probe about which lanes are live.
_CP_SPEC = importlib.util.spec_from_file_location(
    "cos_preflight", Path(__file__).with_name("cos_preflight.py")
)
assert _CP_SPEC and _CP_SPEC.loader
_cos_preflight = importlib.util.module_from_spec(_CP_SPEC)
sys.modules[_CP_SPEC.name] = _cos_preflight
_CP_SPEC.loader.exec_module(_cos_preflight)

DEFAULT_INTERVAL = 2.0
FAIL_TAIL_LINES = 20
MAX_COMMAND_CHARS = 160
TMUX_SESSION = "cos"
LANE_TITLE_PREFIX = "lane:"
DONE_TITLE_PREFIX = "done:"

_DIM = "\x1b[2m"
_BOLD = "\x1b[1m"
_RED = "\x1b[31m"
_CYAN = "\x1b[36m"
_RESET = "\x1b[0m"


# ---------------------------------------------------------------------------
# State-store roots


def resolve_roots(state_root: Path | None) -> tuple[Path, Path]:
    """(slots_root, logs_root) for a --state-root override or the ambient default.

    --state-root points at the `.../registry-research-toolkit` directory, matching
    cos_dispatch.py's flag of the same name.
    """
    if state_root is not None:
        return state_root / "pipeline-slots", state_root / "dispatch-logs"
    slots_root = _cos_preflight.default_slots_root()
    return slots_root, slots_root.parent / "dispatch-logs"


@dataclass(frozen=True)
class Slot:
    slug: str
    surface: str
    tier: str
    issues: tuple[int, ...]
    prs: tuple[int, ...]
    session: str | None
    dispatched: str


def list_slots(slots_root: Path) -> list[Slot]:
    """Every valid live slot, enriched for display (scan_slots gives only the slugs)."""
    slots = []
    for slug in sorted(_cos_preflight.scan_slots(slots_root)):
        loaded = _cos_preflight._read_json_tolerant(
            slots_root / f"{slug}.json", "pipeline-slot file"
        )
        if loaded is None:  # released between scan and read
            continue
        data = loaded[1]
        slots.append(
            Slot(
                slug=slug,
                surface=str(data.get("surface") or "?"),
                tier=str(data.get("tier") or "?"),
                issues=_int_tuple(data.get("issues")),
                prs=_int_tuple(data.get("prs")),
                session=data.get("session"),
                dispatched=str(data.get("dispatched") or "?"),
            )
        )
    return slots


def _int_tuple(value: Any) -> tuple[int, ...]:
    # Slot files come from multiple agents and manual adjudication; a malformed
    # optional field (e.g. `"issues": 1011`) must degrade one display cell, never
    # crash the viewer for the whole ledger.
    if not isinstance(value, list):
        return ()
    return tuple(n for n in value if isinstance(n, int))


def log_age(log_path: Path, now: float) -> str:
    """Human age of the last log write — the 'is it still moving?' signal."""
    try:
        delta = max(0.0, now - log_path.stat().st_mtime)
    except OSError:
        return "no log"
    if delta < 90:
        return f"{int(delta)}s"
    if delta < 5400:
        return f"{int(delta / 60)}m"
    return f"{delta / 3600:.1f}h"


def status_table(slots: list[Slot], logs_root: Path, now: float) -> str:
    if not slots:
        return "no live pipeline slots"
    rows = [("SLOT", "SURFACE", "TIER", "ISSUES", "PRS", "LAST EVENT")]
    for slot in slots:
        rows.append(
            (
                slot.slug,
                slot.surface,
                slot.tier,
                ",".join(f"#{n}" for n in slot.issues) or "-",
                ",".join(f"#{n}" for n in slot.prs) or "-",
                log_age(logs_root / f"{slot.slug}.log", now),
            )
        )
    widths = [max(len(row[col]) for row in rows) for col in range(len(rows[0]))]
    return "\n".join(
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in rows
    )


# ---------------------------------------------------------------------------
# Event rendering (pure: JSONL line in, display text out)

_SHELL_WRAPPER = re.compile(r"^\S*/(?:zsh|bash|sh)\s+(?:-l\s+)?-l?c\s+(.*)$", re.DOTALL)


def strip_shell_wrapper(command: str) -> str:
    """Unwrap `/bin/zsh -lc '<cmd>'` to `<cmd>` for one-line display."""
    match = _SHELL_WRAPPER.match(command.strip())
    if not match:
        return command.strip()
    inner = match.group(1).strip()
    if len(inner) >= 2 and inner[0] == inner[-1] and inner[0] in "'\"":
        inner = inner[1:-1]
    return inner.strip()


_WORKTREE_SEG = re.compile(r"^.*/worktrees/[^/]+/")


def shorten_path(path: str) -> str:
    """Strip the worktree prefix so file_change lines read repo-relative."""
    return _WORKTREE_SEG.sub("", path)


def short_slug(slug: str) -> str:
    return slug.removeprefix("auto-codex-").removeprefix("auto-claude-")


class Renderer:
    """Stateless line renderer; `color` picked once at construction."""

    def __init__(self, *, verbose: bool = False, color: bool = True) -> None:
        self.verbose = verbose
        self.color = color

    def _c(self, code: str, text: str) -> str:
        return f"{code}{text}{_RESET}" if self.color else text

    def render(self, line: str) -> str | None:
        """Display text for one log line, or None when the line is display-noise."""
        line = line.rstrip("\n")
        if not line.strip():
            return None
        try:
            event = json.loads(line)
        except ValueError:
            # Child stderr or claude-surface plain text: pass through, dimmed.
            return self._c(_DIM, line)
        if not isinstance(event, dict):
            return self._c(_DIM, line)
        etype = event.get("type")
        if etype in ("item.started", "item.updated"):
            return None
        if etype == "item.completed":
            item = event.get("item")
            return self._render_item(item) if isinstance(item, dict) else None
        if etype == "thread.started":
            return self._c(_DIM, f"— thread {event.get('thread_id', '?')}")
        if etype == "turn.started":
            return self._c(_DIM, "— turn started")
        if etype == "turn.completed":
            usage = event.get("usage") or {}
            return self._c(
                _DIM,
                "— turn completed"
                f" (in {usage.get('input_tokens', '?')},"
                f" out {usage.get('output_tokens', '?')})",
            )
        if etype in ("turn.failed", "error"):
            return self._c(_RED, f"✗ {etype}: {json.dumps(event, ensure_ascii=False)}")
        return self._c(_DIM, f"— {etype}") if isinstance(etype, str) else None

    def _render_item(self, item: dict[str, Any]) -> str | None:
        itype = item.get("type")
        if itype == "agent_message":
            return self._c(_BOLD, str(item.get("text") or "").rstrip())
        if itype == "command_execution":
            return self._render_command(item)
        if itype == "file_change":
            changes = item.get("changes") or []
            parts = [
                f"{change.get('kind', '?')} {shorten_path(str(change.get('path', '?')))}"
                for change in changes
                if isinstance(change, dict)
            ]
            return self._c(_CYAN, f"✎ {', '.join(parts) or '?'}")
        if itype in ("mcp_tool_call", "collab_tool_call"):
            name = item.get("tool") or item.get("server") or "?"
            return self._c(_DIM, f"⚙ {itype}: {name}")
        if itype == "todo_list":
            return None
        # Unknown/future item type (e.g. reasoning): dim headline, never a flood.
        headline = str(item.get("text") or item.get("summary") or "").strip()
        headline = headline.splitlines()[0] if headline else ""
        label = itype if isinstance(itype, str) else "item"
        return self._c(_DIM, f"— {label}{': ' + headline if headline else ''}")

    def _render_command(self, item: dict[str, Any]) -> str:
        command = strip_shell_wrapper(str(item.get("command") or "?"))
        # Heredocs/multi-line snippets must not break the one-line contract: collapse
        # to a single bounded headline; the full text is in the log (--raw).
        command = " ".join(part.strip() for part in command.split("\n") if part.strip())
        if len(command) > MAX_COMMAND_CHARS:
            command = command[: MAX_COMMAND_CHARS - 2] + " …"
        exit_code = item.get("exit_code")
        rc = "?" if exit_code is None else str(exit_code)
        head = self._c(_DIM, f"$ {command} → rc={rc}")
        failed = isinstance(exit_code, int) and exit_code != 0
        if not (failed or self.verbose):
            return head
        output = str(item.get("aggregated_output") or "").rstrip()
        if not output:
            return head
        tail = output.splitlines()[-FAIL_TAIL_LINES:]
        body = "\n".join(f"  {out_line}" for out_line in tail)
        return f"{head}\n{self._c(_RED if failed else _DIM, body)}"


# ---------------------------------------------------------------------------
# Incremental log following


@dataclass
class LogFollower:
    """Byte-offset tail of one dispatch log; yields only complete lines.

    The child appends unbuffered JSON lines; a poll racing a partial write must not
    hand the renderer a torn line, so bytes after the last newline stay buffered.
    A shrunken file (log rotated/removed and re-created) resets to offset 0.
    """

    path: Path
    pos: int = 0
    _buf: bytes = field(default=b"", repr=False)
    # True when a hot-join landed mid-line: the next newline ends a line whose head
    # was discarded, so its suffix must be dropped, not rendered as a fragment.
    _torn: bool = field(default=False, repr=False)

    def poll(self) -> list[str]:
        try:
            size = self.path.stat().st_size
            if size < self.pos:
                self.pos, self._buf, self._torn = 0, b"", False
            with self.path.open("rb") as fh:
                fh.seek(self.pos)
                chunk = fh.read()
                self.pos = fh.tell()
        except OSError:
            return []
        data = self._buf + chunk
        if not data:
            return []
        lines = data.split(b"\n")
        self._buf = lines.pop()
        if self._torn and lines:
            lines, self._torn = lines[1:], False
        return [raw.decode("utf-8", errors="replace") for raw in lines if raw.strip()]

    def skip_to_end(self) -> None:
        """Advance past existing content without yielding it (hot-join a live lane)."""
        self.poll()
        # A non-empty buffer means we joined mid-line; its suffix is still coming.
        self._torn = bool(self._buf)
        self._buf = b""

    def drain_tail(self) -> str | None:
        """The buffered unterminated final line, once the writer is known to be done.

        A child that exits after a write with no trailing newline leaves its last
        line in the buffer; poll() rightly withholds it while the writer might still
        be mid-line, so the lane-ended path must flush it explicitly. A torn buffer
        (suffix of a hot-joined line) is discarded, never rendered.
        """
        tail, self._buf = self._buf, b""
        if self._torn:
            self._torn = False
            return None
        return tail.decode("utf-8", errors="replace") if tail.strip() else None


COS_RUN_MARKER = b'{"type":"cos.run.started"'
CODEX_RUN_MARKER = b'{"type":"thread.started"'


def current_run_offset(log_path: Path) -> int:
    """Byte offset where the append-only per-slug log's CURRENT run begins.

    A retried dispatch reuses the slug and appends a whole new run, so replaying
    from byte 0 would show a prior run's commands/failures as if they were current.
    Every new dispatch now opens with a cos.run.started event, including plain
    claude-surface runs. Older codex logs only have thread.started, so keep that
    fallback. Logs without either marker (fresh files) start at 0.
    """
    try:
        data = log_path.read_bytes()
    except OSError:
        return 0
    # Unanchored on purpose: a retry can append its marker directly after a prior
    # run's unterminated final byte, so requiring a preceding newline would miss the
    # new run and replay the old one. The raw marker bytes cannot occur inside a
    # JSON string value (its quotes would be escaped), so a hit is a real event.
    cos_marker = data.rfind(COS_RUN_MARKER)
    if cos_marker >= 0:
        return cos_marker
    return max(data.rfind(CODEX_RUN_MARKER), 0)


def lane_is_plain(surface: str | None, log_path: Path) -> bool:
    """True when the lane's log should pass through verbatim instead of rendering.

    codex lanes emit `codex exec --json` events; claude lanes emit plain `claude -p`
    text, and running it through the event renderer would eat any output line that
    happens to be valid JSON (an object without a codex `type` renders as nothing).
    An unknown surface (done lane, malformed slot) is sniffed from the log's head.
    """
    if surface == "codex":
        return False
    if surface == "claude":
        return True
    try:
        offset = current_run_offset(log_path)
        with log_path.open("rb") as fh:
            if offset:
                fh.seek(offset)
            head = fh.read(16384)
    except OSError:
        return False
    # stdout+stderr share the dispatch log, so startup warnings can precede the
    # first JSON event — classify on codex events in the head, but ignore the
    # cross-surface cos.run.started sentinel that can also lead a plain claude log.
    for line in head.splitlines():
        stripped = line.lstrip()
        if not stripped.startswith(b'{"type":'):
            continue
        if stripped.startswith(COS_RUN_MARKER):
            try:
                event = json.loads(stripped.decode("utf-8", errors="replace"))
            except ValueError:
                continue
            if event.get("surface") == "claude":
                return True
            if event.get("surface") == "codex":
                return False
            continue
        return False
    return True


def follow_one(
    slug: str,
    slots_root: Path,
    logs_root: Path,
    renderer: Renderer,
    *,
    raw: bool,
    start_mode: str,
    interval: float,
) -> int:
    log_path = logs_root / f"{slug}.log"
    if not log_path.exists() and slug not in _cos_preflight.scan_slots(slots_root):
        known = sorted(path.stem for path in logs_root.glob("*.log"))
        print(
            f"error: no dispatch log or live slot for {slug!r}"
            + (f"; known logs: {', '.join(known)}" if known else ""),
            file=sys.stderr,
        )
        return 2
    slot_surface: str | None = None
    for slot in list_slots(slots_root):
        if slot.slug == slug:
            slot_surface = slot.surface
            issues = ",".join(f"#{n}" for n in slot.issues) or "-"
            prs = ",".join(f"#{n}" for n in slot.prs) or "-"
            print(
                renderer._c(
                    _BOLD,
                    f"== {slug} [{slot.surface}/{slot.tier}]"
                    f" issues {issues} prs {prs} ==",
                )
            )
            break
    plain = raw or lane_is_plain(slot_surface, log_path)

    def emit(lines: list[str]) -> None:
        for line in lines:
            rendered = line if plain else renderer.render(line)
            if rendered is not None:
                print(rendered, flush=True)

    follower = LogFollower(log_path)
    if start_mode == "tail":
        follower.skip_to_end()
    elif start_mode == "run-start":
        follower.pos = current_run_offset(log_path)
    while True:
        emit(follower.poll())
        if slug not in _cos_preflight.scan_slots(slots_root):
            # Lane over: drain what the poll above may have raced (including an
            # unterminated final line), then EXIT — a follower that lingers would
            # hold a process per done lane and, on slug reuse (dispatch logs are
            # per-slug, append-only), print the NEXT run's log into a pane titled
            # done:. remain-on-exit keeps the scrollback.
            emit(follower.poll())
            tail = follower.drain_tail()
            if tail is not None:
                emit([tail])
            print(renderer._c(_DIM, f"— slot {slug} released (lane ended)"), flush=True)
            return 0
        time.sleep(interval)


def follow_all(
    slots_root: Path,
    logs_root: Path,
    renderer: Renderer,
    *,
    raw: bool,
    start_mode: str,
    interval: float,
) -> int:
    followers: dict[str, LogFollower] = {}
    resume_pos: dict[str, int] = {}
    plain_lanes: dict[str, bool] = {}
    first_scan = True
    while True:
        _follow_all_tick(
            slots_root,
            logs_root,
            renderer,
            followers,
            resume_pos,
            plain_lanes,
            raw=raw,
            startup_mode=start_mode if first_scan else None,
        )
        first_scan = False
        time.sleep(interval)


def _follow_all_tick(
    slots_root: Path,
    logs_root: Path,
    renderer: Renderer,
    followers: dict[str, LogFollower],
    resume_pos: dict[str, int],
    plain_lanes: dict[str, bool],
    *,
    raw: bool,
    startup_mode: str | None,
) -> None:
    """One discovery/emit/prune pass of follow --all (split out for testability).

    `startup_mode` is the user's start mode on the first tick and None afterwards —
    a lane discovered mid-watch always starts at its current run's offset.
    """

    def emit(slug: str, lines: list[str]) -> None:
        prefix = renderer._c(_DIM, f"[{short_slug(slug)}]")
        for line in lines:
            rendered = line if plain_lanes.get(slug, raw) else renderer.render(line)
            if rendered is None:
                continue
            for out_line in rendered.splitlines():
                print(f"{prefix} {out_line}", flush=True)

    live_slots = list_slots(slots_root)
    live = {slot.slug for slot in live_slots}
    surfaces = {slot.slug: slot.surface for slot in live_slots}
    for slug in sorted(live):
        if slug not in followers:
            log_path = logs_root / f"{slug}.log"
            follower = LogFollower(log_path)
            mode = startup_mode if startup_mode is not None else "run-start"
            if slug in resume_pos:
                # Slug reuse seen by THIS process: resume exactly past the prior
                # run's bytes in the append-only log.
                follower.pos = resume_pos[slug]
            elif mode == "tail":
                follower.skip_to_end()
            elif mode == "run-start":
                follower.pos = current_run_offset(log_path)
            followers[slug] = follower
            plain_lanes[slug] = raw or lane_is_plain(surfaces.get(slug), log_path)
            print(renderer._c(_BOLD, f"== following {slug} =="), flush=True)
    for slug, follower in list(followers.items()):
        emit(slug, follower.poll())
        if slug not in live:
            # Lane over: flush the racing tail (including an unterminated final
            # line), note the release, and prune — a released lane polled forever
            # otherwise, and on slug reuse would swallow the next run's opening.
            emit(slug, follower.poll())
            tail = follower.drain_tail()
            if tail is not None:
                emit(slug, [tail])
            print(
                renderer._c(_DIM, f"[{short_slug(slug)}] — slot released (lane ended)"),
                flush=True,
            )
            resume_pos[slug] = follower.pos
            del followers[slug]
            plain_lanes.pop(slug, None)


# ---------------------------------------------------------------------------
# tmux orchestration


def plan_pane_actions(
    live_slugs: set[str], pane_titles: dict[str, str]
) -> tuple[list[str], list[str]]:
    """(slugs to spawn panes for, pane_ids to retitle done:) — the manager's pure core.

    `pane_titles` maps pane_id -> pane title; only `lane:<slug>` panes are ours.
    A done pane is retitled (not killed) so its scrollback survives for post-mortem.
    """
    tracked = {
        title.removeprefix(LANE_TITLE_PREFIX): pane_id
        for pane_id, title in pane_titles.items()
        if title.startswith(LANE_TITLE_PREFIX)
    }
    spawn = sorted(live_slugs - tracked.keys())
    retire = sorted(
        pane_id for slug, pane_id in tracked.items() if slug not in live_slugs
    )
    return spawn, retire


def tmux_session(state_root: Path | None) -> str:
    """Session name; a --state-root override gets its own session.

    Attaching reuses an existing session's manager, so its state root is part of
    the session identity — otherwise a leftover session for another root would
    silently keep showing that root's ledger and logs.
    """
    if state_root is None:
        return TMUX_SESSION
    digest = hashlib.sha256(str(state_root.resolve()).encode()).hexdigest()[:8]
    return f"{TMUX_SESSION}-{digest}"


def tmux_window(state_root: Path | None) -> str:
    # Target the window by NAME, never by index — a user's base-index setting moves
    # the first window's index, but `-n lanes` pins its name.
    return f"{tmux_session(state_root)}:lanes"


def _tmux(*args: str) -> str:
    proc = subprocess.run(["tmux", *args], capture_output=True, text=True, check=True)
    return proc.stdout


def _self_argv(args: argparse.Namespace, *tail: str) -> list[str]:
    """Re-invocation argv propagating the global flags, then `tail`.

    Global flags live on the top-level parser, so they must precede the subcommand
    in `tail` — argparse rejects them after it.
    """
    argv = [sys.executable, str(Path(__file__).resolve())]
    if args.state_root is not None:
        argv += ["--state-root", str(args.state_root)]
    argv += ["--interval", str(args.interval)]
    if args.verbose:
        argv.append("-v")
    if args.no_color:
        argv.append("--no-color")
    return argv + list(tail)


def manage(args: argparse.Namespace) -> int:
    """Manager loop for pane 0 of the tmux session: status display + pane lifecycle."""
    slots_root, logs_root = resolve_roots(args.state_root)
    window = tmux_window(args.state_root)
    last_table = None
    while True:
        slots = list_slots(slots_root)
        live = {slot.slug for slot in slots}
        panes = {}
        for line in _tmux(
            "list-panes", "-t", window, "-F", "#{pane_id}\t#{pane_title}"
        ).splitlines():
            pane_id, _, title = line.partition("\t")
            panes[pane_id] = title
        spawn, retire = plan_pane_actions(live, panes)
        for slug in spawn:
            # --from-run-start, not --from-start: the per-slug log is append-only,
            # so a reused slug's pane must not replay prior runs as if current.
            follow_cmd = _self_argv(args, "follow", slug, "--from-run-start")
            pane_id = _tmux(
                "split-window",
                "-d",
                "-t",
                window,
                "-P",
                "-F",
                "#{pane_id}",
                shlex.join(follow_cmd),
            ).strip()
            _tmux("select-pane", "-t", pane_id, "-T", f"{LANE_TITLE_PREFIX}{slug}")
            _tmux("select-layout", "-t", window, "tiled")
        for pane_id in retire:
            slug = panes[pane_id].removeprefix(LANE_TITLE_PREFIX)
            _tmux("select-pane", "-t", pane_id, "-T", f"{DONE_TITLE_PREFIX}{slug}")
        table = status_table(slots, logs_root, time.time())
        if table != last_table:
            # Redraw-on-change only, so the status pane isn't a scrolling ticker.
            print(f"\x1b[2J\x1b[H{table}", flush=True)
            last_table = table
        time.sleep(args.interval)


def cmd_tmux(args: argparse.Namespace) -> int:
    if shutil.which("tmux") is None:
        print("error: tmux not found on PATH", file=sys.stderr)
        return 2
    session = tmux_session(args.state_root)
    has_session = (
        subprocess.run(
            ["tmux", "has-session", "-t", session], capture_output=True, check=False
        ).returncode
        == 0
    )
    if not has_session:
        manage_cmd = _self_argv(args, "manage")
        _tmux(
            "new-session",
            "-d",
            "-s",
            session,
            "-n",
            "lanes",
            shlex.join(manage_cmd),
        )
        # Dead lane panes stay visible ([exited]) instead of vanishing mid-glance;
        # titled borders are how you tell lanes apart.
        _tmux("set-option", "-t", session, "remain-on-exit", "on")
        _tmux("set-option", "-t", session, "pane-border-status", "top")
    os.execvp("tmux", ["tmux", "attach", "-t", session])
    raise AssertionError("unreachable: execvp does not return")


# ---------------------------------------------------------------------------
# CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Live read-only viewer for dispatched pr-pipeline lanes."
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=None,
        help="state store root holding pipeline-slots/ and dispatch-logs/ "
        "(default: $XDG_STATE_HOME/registry-research-toolkit)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help=f"poll interval in seconds (default {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="always print command output, not only on failure",
    )
    parser.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="table of live pipeline slots")
    status.add_argument(
        "--watch", action="store_true", help="refresh the table every --interval"
    )

    follow = sub.add_parser("follow", help="render a lane's dispatch log, live")
    follow.add_argument("slug", nargs="?", help="lane slug (slot/log filename stem)")
    follow.add_argument(
        "--all", action="store_true", help="every live lane, prefix-multiplexed"
    )
    start = follow.add_mutually_exclusive_group()
    start.add_argument(
        "--from-start",
        action="store_true",
        help="replay the log from the beginning instead of tailing new events",
    )
    start.add_argument(
        "--from-run-start",
        action="store_true",
        help="replay from the CURRENT run's start in the append-only per-slug log "
        "(what the tmux panes use — a reused slug does not replay prior runs)",
    )
    follow.add_argument(
        "--raw", action="store_true", help="print log lines verbatim (no rendering)"
    )

    sub.add_parser("tmux", help="create/attach the auto-managed tmux session")
    sub.add_parser("manage", help=argparse.SUPPRESS)  # internal: tmux pane 0
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    slots_root, logs_root = resolve_roots(args.state_root)
    color = not args.no_color and sys.stdout.isatty() and not os.environ.get("NO_COLOR")
    renderer = Renderer(verbose=args.verbose, color=color)

    if args.command == "status":
        while True:
            table = status_table(list_slots(slots_root), logs_root, time.time())
            if args.watch:
                print(f"\x1b[2J\x1b[H{table}", flush=True)
                time.sleep(args.interval)
            else:
                print(table)
                return 0
    if args.command == "follow":
        if args.all == (args.slug is not None):
            print("error: follow takes exactly one of <slug> or --all", file=sys.stderr)
            return 2
        if args.from_start:
            start_mode = "start"
        elif args.from_run_start:
            start_mode = "run-start"
        else:
            start_mode = "tail"
        common = {
            "raw": args.raw,
            "start_mode": start_mode,
            "interval": args.interval,
        }
        if args.all:
            return follow_all(slots_root, logs_root, renderer, **common)
        return follow_one(args.slug, slots_root, logs_root, renderer, **common)
    if args.command == "manage":
        return manage(args)
    if args.command == "tmux":
        return cmd_tmux(args)
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
