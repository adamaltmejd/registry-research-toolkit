"""MONA upload probe.

Answers the open questions before we commit to the bundled-`.py` ship path:

  (1) Does MONA's upload UI accept a `.py` file at all?
      - Implicit: if this script runs and produces output, yes.

  (2) Does the upload preserve source bytes verbatim, or does it
      strip / re-encode non-ASCII?
      - Tested by reading our own source bytes and looking for both
        an ASCII-escaped sentinel (always parseable) and a raw UTF-8
        sentinel (only present if non-ASCII bytes survived).

  (3) Are the runtime imports we plan to use actually importable?
      - duckdb, pyodbc, numpy, plus stdlib bits we lean on.

  (4) Can we write output files next to the script?

PII safety: no DB queries, no network, no microdata. Only file-system and
self-introspection. Safe to run on any MONA project.

Output: `mdw_upload_probe_<HOST>_<TIMESTAMP>.txt` written next to this
script. Export that file back out of MONA to share results.
"""

from __future__ import annotations

import locale
import os
import platform
import socket
import sys
from datetime import datetime
from pathlib import Path

# MBS-batch stdout footgun: in batch mode stdout buffers to memory and can
# hang the script when full. Redirect immediately. Per MONA Python docs.
_HOST = socket.gethostname()
if _HOST.upper().startswith("MBS"):
    sys.stdout = open(os.devnull, "w")  # noqa: SIM115

_HERE = Path(__file__).resolve().parent
_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
_OUT = _HERE / f"mdw_upload_probe_{_HOST}_{_TS}.txt"

# ASCII-escaped sentinel: pure ASCII in source, expands to non-ASCII at
# runtime. Always parseable; lets us check whether the source-bytes round
# trip works at all.
_SENTINEL_ESCAPED = "Födelseår Kön Län"

# Raw UTF-8 sentinel: high bytes appear directly in the source file. If
# MONA re-encodes / strips non-ASCII on upload, this comparison fails (or
# the file fails to parse and the script never runs at all).
_SENTINEL_RAW = "Födelseår Kön Län"


def _line(fp, msg: str) -> None:
    fp.write(msg + "\n")
    fp.flush()


def main() -> None:
    with _OUT.open("w", encoding="utf-8") as fp:
        _line(fp, "=== MONA upload probe ===")
        _line(fp, f"timestamp: {datetime.now().isoformat()}")
        _line(fp, f"host: {_HOST}")
        _line(fp, f"platform: {platform.platform()}")
        _line(fp, f"python_version: {sys.version.splitlines()[0]}")
        _line(fp, f"python_executable: {sys.executable}")
        _line(fp, f"cwd: {os.getcwd()}")
        _line(fp, f"script_path: {Path(__file__).resolve()}")
        _line(fp, f"default_encoding: {sys.getdefaultencoding()}")
        _line(fp, f"filesystem_encoding: {sys.getfilesystemencoding()}")
        _line(fp, f"locale_preferred_encoding: {locale.getpreferredencoding(False)}")

        _line(fp, "")
        _line(fp, "-- (1) source byte integrity --")
        try:
            src_bytes = Path(__file__).read_bytes()
            _line(fp, f"source_size_bytes: {len(src_bytes)}")
            _line(
                fp, f"first_4_bytes_hex: {src_bytes[:4].hex()}  (UTF-8 BOM = ef bb bf)"
            )

            try:
                src_text = src_bytes.decode("utf-8")
                utf8_ok = True
                _line(fp, "source_decodes_as_utf8: yes")
            except UnicodeDecodeError as e:
                src_text = ""
                utf8_ok = False
                _line(fp, f"source_decodes_as_utf8: NO ({e})")

            if utf8_ok:
                _line(
                    fp, f"escaped_sentinel_in_source: {_SENTINEL_ESCAPED in src_text}"
                )
                _line(fp, f"raw_utf8_sentinel_in_source: {_SENTINEL_RAW in src_text}")
            else:
                # Source isn't UTF-8 anymore. Try Latin-1 (everything decodes)
                # to inspect what we got.
                src_latin1 = src_bytes.decode("latin-1")
                _line(
                    fp,
                    f"escaped_sentinel_in_latin1_view: {_SENTINEL_ESCAPED in src_latin1}",
                )
                _line(
                    fp,
                    f"raw_utf8_sentinel_in_latin1_view: {_SENTINEL_RAW in src_latin1}",
                )

            _line(fp, f"escaped_sentinel_runtime_value: {_SENTINEL_ESCAPED!r}")
            _line(fp, f"raw_sentinel_runtime_value: {_SENTINEL_RAW!r}")
            _line(
                fp,
                f"escaped_equals_raw_at_runtime: {_SENTINEL_ESCAPED == _SENTINEL_RAW}",
            )
        except Exception as e:
            _line(fp, f"source_read_failed: {type(e).__name__}: {e}")

        _line(fp, "")
        _line(fp, "-- (2) required imports --")
        for mod in ("duckdb", "pyodbc", "numpy", "json", "csv", "sqlite3"):
            try:
                m = __import__(mod)
                ver = getattr(m, "__version__", "(stdlib)")
                _line(fp, f"{mod}: ok, version {ver}")
            except Exception as e:
                _line(fp, f"{mod}: FAILED -- {type(e).__name__}: {e}")

        _line(fp, "")
        _line(fp, "-- (3) write next to script --")
        scratch = _HERE / f"mdw_upload_probe_scratch_{_TS}.txt"
        try:
            scratch.write_text("scratch ok\n", encoding="utf-8")
            ok = scratch.read_text(encoding="utf-8") == "scratch ok\n"
            scratch.unlink()
            _line(
                fp,
                f"write_next_to_script: {'ok' if ok else 'ROUNDTRIP MISMATCH'} ({scratch.name})",
            )
        except Exception as e:
            _line(fp, f"write_next_to_script: FAILED -- {type(e).__name__}: {e}")

        _line(fp, "")
        _line(fp, "-- (4) write with non-ASCII filename --")
        # If MONA's filesystem rejects non-ASCII filenames, our generated
        # mock CSVs (column headers like Födelseår are fine -- but file
        # names are usually ASCII anyway). This is a sanity check, not
        # load-bearing.
        try:
            non_ascii = _HERE / f"mdw_upload_probe_unicode_{_TS}_åäö.txt"
            non_ascii.write_text("ok", encoding="utf-8")
            non_ascii.unlink()
            _line(fp, "non_ascii_filename: ok")
        except Exception as e:
            _line(fp, f"non_ascii_filename: FAILED -- {type(e).__name__}: {e}")

        _line(fp, "")
        _line(fp, "=== done ===")
        _line(fp, "")
        _line(fp, "Interpretation:")
        _line(fp, "  * If this report file exists, MONA accepted .py upload AND")
        _line(fp, "    Python parsed it AND it executed -- (1) is answered yes.")
        _line(fp, "  * source_decodes_as_utf8=yes + raw_utf8_sentinel_in_source=True")
        _line(fp, "    => upload preserves bytes verbatim, the bundle can use UTF-8.")
        _line(
            fp,
            "  * raw_utf8_sentinel_in_source=False but escaped_sentinel_in_source=True",
        )
        _line(fp, "    => upload re-encoded or stripped non-ASCII source bytes; the")
        _line(fp, "    bundler must emit ASCII-only output (escape all non-ASCII).")
        _line(fp, "  * source_decodes_as_utf8=NO => upload converted to a non-UTF-8")
        _line(fp, "    codec; bundler must be ASCII-only AND we may want a coding")
        _line(fp, "    declaration matching MONA's chosen codec.")
        _line(fp, "")
        _line(fp, "If you DON'T see this file after running the script, the upload")
        _line(fp, "either rejected .py outright or mangled it badly enough that")
        _line(fp, "Python could not parse it (look for SyntaxError in the .Rout-")
        _line(fp, "equivalent stderr).")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        fail = _HERE / f"mdw_upload_probe_FAIL_{_TS}.txt"
        try:
            fail.write_text(
                f"PROBE FAILED before main report finished:\n"
                f"  {type(e).__name__}: {e}\n",
                encoding="utf-8",
            )
        except Exception:
            pass
        raise
