"""Shared CLI scaffolding used by both `regmeta` and `regmeta-build` CLIs.

Envelope, hints, table/list rendering, global-flag handling, and the
`NoRepeatParser` argparse subclass live here. Importable across packages
without crossing the dep direction: `regmeta_build → regmeta`.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Any

from .db import get_manifest, utc_now

CONTRACT_VERSION = "3.0.0"

MAX_DISPLAY_ROWS = 100
_MAX_HINTS = 3


def success_envelope(
    *,
    command: str,
    args_payload: dict[str, Any],
    db_info: dict[str, str] | None,
    data: Any,
    duration_ms: int,
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "generated_at": utc_now(),
        "request": {"command": command, "args": args_payload},
    }
    if db_info:
        envelope["database"] = db_info
    envelope["data"] = data
    envelope["run"] = {"duration_ms": duration_ms}
    return envelope


def hint_add(hints: list[str] | None, msg: str) -> None:
    if hints is not None and len(hints) < _MAX_HINTS:
        hints.append(msg)


def emit_hints(hints: list[str]) -> None:
    sys.stderr.write("\n")
    for h in hints:
        sys.stderr.write(f"  hint: {h}\n")


def write_to(content: str, output_path: str | None, *, truncate: bool = False) -> None:
    if output_path:
        target = Path(output_path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w" if truncate else "a", encoding="utf-8") as f:
            f.write(content)
    else:
        sys.stdout.write(content)


def write_json(payload: dict[str, Any], output_path: str | None) -> None:
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output_path:
        tmp = Path(output_path).expanduser().resolve()
        tmp_file = tmp.with_suffix(tmp.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp_file.write_text(content, encoding="utf-8")
        tmp_file.replace(tmp)
    else:
        sys.stdout.write(content)


def terminal_width(output_path: str | None) -> int:
    if output_path:
        return 10_000
    return shutil.get_terminal_size().columns


def render_table(
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_width: int | None = None,
) -> tuple[str, int]:
    widths = {c: len(c) for c in columns}
    str_rows = []
    for row in rows:
        str_row = {c: str(row.get(c, "")) for c in columns}
        for c in columns:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)

    separators = 2 * (len(columns) - 1)
    table_width = sum(widths.values()) + separators

    # Shrink widest columns to fit terminal when max_width is set
    if max_width and table_width > max_width:
        budget = max_width - separators
        min_col = max(8, max(len(c) for c in columns))
        while sum(widths.values()) > budget:
            widest = max(columns, key=lambda c: widths[c])
            if widths[widest] <= min_col:
                break
            widths[widest] = max(
                min_col, budget - sum(w for c, w in widths.items() if c != widest)
            )
        table_width = sum(widths.values()) + separators
        # Truncate cell values that exceed their column width
        for sr in str_rows:
            for c in columns:
                if len(sr[c]) > widths[c]:
                    sr[c] = sr[c][: widths[c] - 1] + "…"

    header = "  ".join(c.ljust(widths[c]) for c in columns)
    sep = "  ".join("-" * widths[c] for c in columns)
    lines = [header, sep]
    for sr in str_rows:
        lines.append("  ".join(sr[c].ljust(widths[c]) for c in columns))
    return "\n".join(lines) + "\n", table_width


def render_list(rows: list[dict[str, Any]], columns: list[str]) -> str:
    key_width = max(len(c) for c in columns)
    lines: list[str] = []
    for i, row in enumerate(rows):
        if i > 0:
            lines.append("")
        for c in columns:
            lines.append(f"  {c.ljust(key_width)}  {row.get(c, '')}")
    return "\n".join(lines) + "\n"


def format_rows(
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_width: int | None = None,
) -> str:
    """Render rows as a table or list string.

    Auto-selects list format for ≤5 rows. Truncates wide columns to fit
    max_width when given. Importable by other packages (e.g. mock-data-wizard).
    """
    if not rows:
        return "(no results)\n"
    if len(rows) <= 5:
        return render_list(rows, columns)
    content, width = render_table(rows, columns)
    if max_width and width > max_width:
        content, _ = render_table(rows, columns, max_width=max_width)
    return content


def write_formatted(
    rows: list[dict[str, Any]],
    columns: list[str],
    output_path: str | None,
    *,
    fmt: str = "table",
    fmt_explicit: bool = False,
    hints: list[str] | None = None,
) -> None:
    if not rows:
        write_to("(no results)\n", output_path)
        return

    truncated = 0
    if len(rows) > MAX_DISPLAY_ROWS:
        truncated = len(rows) - MAX_DISPLAY_ROWS
        rows = rows[:MAX_DISPLAY_ROWS]

    if fmt == "list":
        content = render_list(rows, columns)
    elif not fmt_explicit and len(rows) <= 5:
        # Few results — list is more readable
        content = render_list(rows, columns)
    else:
        term_w = terminal_width(output_path)
        table_content, table_width = render_table(rows, columns)
        if table_width > term_w:
            table_content, _ = render_table(rows, columns, max_width=term_w)
            hint_add(hints, "Long values truncated (--format list for full text)")
            content = table_content
        else:
            content = table_content

    if truncated:
        hint_add(
            hints,
            f"Table view truncated {truncated} rows (--format json for full output)",
        )

    write_to(content, output_path)


def get_db_info(conn: sqlite3.Connection) -> dict[str, str]:
    manifest = get_manifest(conn)
    return {
        "schema_version": manifest.get("schema_version", "unknown"),
        "import_date": manifest.get("import_date", "unknown"),
    }


class NoRepeatParser(argparse.ArgumentParser):
    """ArgumentParser that rejects repeated optional flags."""

    def parse_known_args(self, args=None, namespace=None):
        if args is None:
            args = sys.argv[1:]
        seen: dict[str, str] = {}
        for token in args:
            if token.startswith("-") and "=" not in token:
                if token in seen:
                    self.error(f"{token} may only be specified once")
                seen[token] = token
        return super().parse_known_args(args, namespace)


GLOBAL_FLAGS = {
    "--db",
    "--format",
    "--output",
    "-v",
    "--verbose",
    "-q",
    "--quiet",
    "--version",
}
GLOBAL_FLAGS_WITH_VALUE = {"--db", "--format", "--output"}


def reorder_global_flags(argv: list[str]) -> list[str]:
    """Move global flags before the subcommand so argparse handles them.

    Handles both ``--flag value`` and ``--flag=value`` syntax.
    """
    front: list[str] = []
    rest: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        # Handle --flag=value for global flags
        eq_name = token.split("=", 1)[0] if "=" in token else None
        if token in GLOBAL_FLAGS:
            front.append(token)
            if token in GLOBAL_FLAGS_WITH_VALUE and i + 1 < len(argv):
                i += 1
                front.append(argv[i])
        elif eq_name in GLOBAL_FLAGS_WITH_VALUE:
            front.append(token)
        else:
            rest.append(token)
        i += 1
    return front + rest


def clean_leaf_help(parser: argparse.ArgumentParser) -> None:
    """Hide -h/--help from output, rename 'positional arguments', add epilog."""
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction):
            action.help = argparse.SUPPRESS
            break
    for group in parser._action_groups:
        if group.title == "positional arguments":
            group.title = "Arguments"
            break
    if not parser.epilog:
        # parser.prog already carries the right program prefix ("regmeta …"
        # or "regmeta-build …"), so we don't need to special-case per CLI.
        parser.epilog = f"Run `{parser.prog} --examples` for usage examples."
