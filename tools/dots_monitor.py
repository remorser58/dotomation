#!/usr/bin/env python3
"""dots-monitor: read-only view of Dots live records plus independent tool activity.

Two sources are compared, never merged:

* the live ledger an agent writes about itself (``live/<DOT>/<agent>.jsonl``,
  live-record-v1), and
* the Web GPT Agent daemon log of the account that dot runs on, which records
  every MCP request the browser actually made.

A record says what the agent claims. The daemon log says whether that account
called a tool at that time. The evidence field of a record is resolved against
GitHub where it names a pull request, commit, issue, comment, workflow run or a
40-character SHA.

Standard library only; ``ssh`` and ``gh`` are called as subprocesses. The tool
never writes to a host it reads: the remote side runs only ``wc -c`` and
``tail -c`` over the configured files, every subprocess has a timeout, and
every printed value is redacted.
"""

from __future__ import annotations

import os
import sys

if __name__ == "__main__" and not sys.flags.isolated and sys.path:
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    if os.path.abspath(sys.path[0] or ".") == _SCRIPT_DIR:
        sys.path.pop(0)

import argparse
import json
import re
import secrets
import shlex
import stat
import subprocess
import time
import unicodedata
from datetime import datetime, timedelta, timezone

TOOL_VERSION = "1.0.0"

FIELDS = ("t", "agent", "role", "model", "project", "task", "state", "host", "note", "evidence")
STATES = ("start", "step", "blocked", "done")
TIME_ALIASES = ("timestamp", "time", "at", "datetime")
TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
TS_RE = re.compile(
    r"^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])"
    r"[Tt](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]+)?"
    r"(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$"
)
FRACTION_RE = re.compile(r"\.([0-9]+)")

CLASS_ORDER = ("BLOCKED", "UNPROVEN", "DONE", "STALLED", "WORKING", "QUIET",
               "ACTIVITY_UNKNOWN", "LEGACY", "NO_RECORD")
MAX_LIVE_FILES = 200

DEFAULT_CONFIG = {
    "ledger": {"host": "local", "root": "~/.dots-ops/live", "dots": ["DOT-A", "DOT-B"]},
    "dots": {},
    "activity": [],
    "github": {"enabled": True, "repos": [], "cache_seconds": 300, "max_lookups": 60},
    "thresholds": {"stall_minutes": 30, "active_window_minutes": 15, "long_window_minutes": 60,
                   "start_churn_window_minutes": 60, "start_churn_repeats": 3},
    "limits": {"tail_bytes": 262144, "roster_max_bytes": 1048576,
               "command_timeout_seconds": 25, "gh_timeout_seconds": 25},
    "daemon_health": {"enabled": False, "window_minutes": 30, "max_session_files": 20, "tail_bytes": 131072},
}

# ---------------------------------------------------------------------------
# redaction


REDACT_RULES = (
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}"), "[REDACTED]"),
    (re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}"), "[REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"), "[REDACTED]"),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), "[REDACTED]"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "[REDACTED]"),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b"), "[REDACTED]"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\b"), "[REDACTED]"),
    (re.compile(r"((?:https?|wss?)://)[^/\s:@]*:[^/\s@]+@"), r"\1[REDACTED]@"),
    (re.compile(r"(?i)\b(authorization\s*:\s*bearer)\s+[^\s\"',;]+"), r"\1 [REDACTED]"),
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}"), "Bearer [REDACTED]"),
    (
        re.compile(
            r"(?i)\b((?:[a-z0-9_]+_)?(?:bearer|token|secret|password|passwd|api[_-]?key)"
            r"|[a-z0-9_]+_key)\b\s*[:=]\s*[\"']?[^\s\"',;]+"
        ),
        r"\1=[REDACTED]",
    ),
    (
        re.compile(r"(?i)([?&](?:token|key|access_token|api_key|sig|signature)=)[^&\s]+"),
        r"\1[REDACTED]",
    ),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*"), "[REDACTED]"),
    (re.compile(r"(/mcp/(?:core|desktop|plugins)/)[A-Za-z0-9_-]{12,}"), r"\1[REDACTED]"),
)
# A long mixed-case alphanumeric run is treated as a token unless it is plain
# lowercase hex, which is how commit SHAs and content digests are written.
LONG_TOKEN_RE = re.compile(
    r"\b(?=[A-Za-z0-9]{32,}\b)(?=[A-Za-z0-9]*[A-Z])(?=[A-Za-z0-9]*[a-z])"
    r"(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{32,}\b"
)
_TERM_CTRL_RE = re.compile(
    r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]|[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\x9b]"
)


def redact(text):
    """Remove anything that looks like a credential from text about to be printed."""
    if not text:
        return text
    out = str(text)
    for pattern, replacement in REDACT_RULES:
        out = pattern.sub(replacement, out)
    out = LONG_TOKEN_RE.sub("[REDACTED]", out)
    return out


def one_line(text, limit=120):
    """Collapse a value to one printable, redacted, escape-free line."""
    if text is None:
        return ""
    cleaned = _TERM_CTRL_RE.sub("", str(text))
    out = redact(" ".join(cleaned.split()))
    if len(out) > limit:
        out = out[: max(0, limit - 1)] + "\u2026"
    return out


def redact_tree(value):
    """Return value with every string redacted, recursively.

    The JSON and watch paths apply this to the whole document, so a field added
    later cannot reach stdout unredacted by missing its call-site wrapper.
    """
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, list):
        return [redact_tree(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_tree(item) for item in value)
    if isinstance(value, dict):
        return {key: redact_tree(item) for key, item in value.items()}
    return value


# ---------------------------------------------------------------------------
# time


def parse_ts(value):
    """Parse an RFC 3339 timestamp to an aware UTC datetime, or None."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not TS_RE.match(text):
        return None
    if text[-1] in "Zz":
        text = text[:-1] + "+00:00"
    text = FRACTION_RE.sub(lambda m: "." + (m.group(1) + "000000")[:6], text, count=1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def age_text(seconds):
    if seconds is None:
        return "?"
    seconds = int(seconds)
    if seconds < 0:
        return "+%ds" % (-seconds)
    if seconds < 90:
        return "%ds" % seconds
    if seconds < 5400:
        return "%dm" % (seconds // 60)
    if seconds < 172800:
        return "%dh%02dm" % (seconds // 3600, (seconds % 3600) // 60)
    return "%dd" % (seconds // 86400)


# ---------------------------------------------------------------------------
# live records


def parse_record_line(raw):
    """Return (record, None) for a live-record-v1 line, else (None, reason).

    The reason is a short human phrase naming the first thing that makes the
    line not v1. Nothing is repaired: a line that fails here is legacy and is
    reported as unconfirmed, never rewritten.
    """
    line = raw.strip()
    if not line:
        return None, "blank line"
    try:
        obj = json.loads(line)
    except ValueError:
        return None, "not JSON"
    if not isinstance(obj, dict):
        return None, "not a JSON object"
    missing = [f for f in FIELDS if f not in obj]
    extra = [k for k in obj if k not in FIELDS]
    if missing:
        alias = [a for a in TIME_ALIASES if a in obj]
        if "t" in missing and alias:
            return None, "uses '%s' instead of 't'" % alias[0]
        return None, "missing %s" % ",".join(missing)
    if extra:
        return None, "extra %s" % ",".join(extra)
    for name in FIELDS:
        if not isinstance(obj[name], str):
            return None, "%s is not a string" % name
        if not obj[name].strip():
            return None, "empty %s" % name
        if "\n" in obj[name] or "\r" in obj[name]:
            return None, "%s spans lines" % name
    if obj["state"] not in STATES:
        return None, "state '%s'" % obj["state"][:24]
    if not TOKEN_RE.match(obj["agent"]):
        return None, "agent is not a token"
    t = parse_ts(obj["t"])
    if t is None:
        return None, "t is not RFC 3339"
    rec = dict(obj)
    rec["_t"] = t
    return rec, None


def new_nonce():
    """Return a per-run sentinel nonce that file content cannot predict."""
    return secrets.token_hex(8)


def parse_ledger_dump(text, nonce):
    """Parse the framed dump produced by ledger_script() for this run's nonce.

    Every sentinel carries the nonce, so a record or note that happens to
    contain ``==END`` or a ``==DOT`` line cannot close a file early or move the
    following lines into another dot's table.
    """
    dot_tag = "==DOT %s " % nonce
    nodir_tag = "==NODIR %s" % nonce
    unread_tag = "==UNREAD %s " % nonce
    too_many_tag = "==TOOMANY %s" % nonce
    file_re = re.compile(r"^==FILE %s (.+) ([0-9]+)$" % re.escape(nonce))
    end_tag = "==END %s" % nonce
    dots = {}
    current = None
    name = None
    size = None
    buf = []
    for line in text.splitlines():
        if line.startswith(dot_tag):
            current = line[len(dot_tag):].strip()
            dots.setdefault(current, {"files": {}, "nodir": False,
                                      "unreadable": [], "too_many": False})
            name = None
            continue
        if current is None:
            continue
        if line == nodir_tag:
            dots[current]["nodir"] = True
            continue
        if line.startswith(unread_tag):
            dots[current]["unreadable"].append(line[len(unread_tag):])
            name = None
            buf = []
            continue
        if name is None and line == too_many_tag:
            dots[current]["too_many"] = True
            continue
        match = file_re.match(line)
        if match and name is None:
            name = match.group(1)
            size = int(match.group(2))
            buf = []
            continue
        if line == end_tag and name is not None:
            dots[current]["files"][name] = {"size": size, "text": "\n".join(buf)}
            name = None
            buf = []
            continue
        if name is not None:
            buf.append(line)
    return dots


def probe_reader(tail_bytes, nonce):
    """One bounded read. A failed tail aborts its frame, even after partial output."""
    return """read_probe() {
  limit=${4:-%d}
  if [ -r "$1" ] && size=$(wc -c 2>&1 < "$1"); then
    while [ "${size# }" != "$size" ]; do size=${size# }; done
    case "$size" in ''|*[!0-9]*) ;; *)
      printf '%%s %%s\\n' "$2" "$size"
      # fd 3 streams file bytes; only the utility's diagnostic stays in the shell.
      if diagnostic=$(tail -c "$limit" "$1" 2>&1 >&3) && [ -z "$diagnostic" ]; then
        printf '\\n==END %s\\n'
      else
        printf '\\n%%s\\n' "$3"
      fi
      return
    esac
  fi
  printf '%%s\\n' "$3"
} 3>&1""" % (tail_bytes, nonce)


def ledger_script(root, dots, tail_bytes, nonce, roster_max_bytes=None):
    tbytes = max(1, int(tail_bytes))
    rmax = max(1, int(roster_max_bytes or tbytes))
    quoted_root = _remote_path(root)
    parts = ["set -u", "export LC_ALL=C", 'root=%s' % quoted_root, probe_reader(tbytes, nonce)]
    parts.append("for dot in %s; do" % " ".join(shlex.quote(d) for d in dots))
    parts.append("""  printf '==DOT %s %%s\\n' "$dot" """ % nonce)
    parts.append('  d="$root/$dot"')
    parts.append("""  if [ ! -d "$d" ]; then printf '==NODIR %s\\n'; continue; fi""" % nonce)
    parts.append('  n=0')
    parts.append('  for f in "$d"/*.jsonl; do')
    parts.append('    [ -f "$f" ] || continue')
    parts.append('    n=$((n+1))')
    parts.append("""    [ "$n" -le %d ] || { printf '==TOOMANY %s\\n'; break; }"""
                 % (MAX_LIVE_FILES, nonce))
    parts.append('    read_probe "$f" "==FILE %s ${f##*/}" "==UNREAD %s ${f##*/}"' % (nonce, nonce))
    parts.append('  done')
    parts.append('  if [ -f "$d/roster.json" ]; then')
    parts.append('    read_probe "$d/roster.json" "==FILE %s roster.json" "==UNREAD %s roster.json" %d'
                 % (nonce, nonce, rmax))
    parts.append('  fi')
    parts.append('done')
    return "\n".join(parts)


def _remote_path(path):
    """Quote a path for the remote shell, expanding a leading ~ to $HOME."""
    text = str(path)
    if text.startswith("~/"):
        return '"$HOME"/' + shlex.quote(text[2:])
    if text == "~":
        return '"$HOME"'
    return shlex.quote(text)


def split_tail(text, size, tail_bytes):
    """Return file text, dropping a first line that a byte tail cut in half."""
    if size is not None and size > tail_bytes and text:
        lines = text.split("\n", 1)
        return lines[1] if len(lines) > 1 else ""
    return text


def legacy_time(raw):
    """Best-effort timestamp from an off-format line, used only for the stall clock.

    A legacy line is never repaired or counted as a step. Its own ``t`` or time
    alias, when it is still RFC 3339, says how long ago that agent last spoke.
    """
    try:
        obj = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    for key in ("t",) + TIME_ALIASES:
        value = obj.get(key)
        if isinstance(value, str):
            parsed = parse_ts(value)
            if parsed is not None:
                return parsed
    return None


def collect_ledger(config, transport, errors):
    ledger = config["ledger"]
    dots = ledger["dots"]
    limits = config["limits"]
    tail_bytes = limits["tail_bytes"]
    roster_max_bytes = limits.get("roster_max_bytes", 1048576)
    nonce = new_nonce()
    script = ledger_script(ledger["root"], dots, tail_bytes, nonce, roster_max_bytes=roster_max_bytes)
    out, error = transport.run(ledger["host"], script)
    if error:
        errors.append({"source": "ledger", "host": ledger["host"], "detail": error})
        return {dot: {"files": {}, "nodir": True, "error": error} for dot in dots}
    dump = parse_ledger_dump(out, nonce)
    # A reachable host that answers without the dot's directory is a failure
    # too: a mistyped root, an unmounted share or a renamed dot must not render
    # as an empty table that looks like "nothing to report".
    for dot in dots:
        entry = dump.setdefault(dot, {"files": {}, "nodir": True})
        if entry["nodir"]:
            errors.append({"source": "ledger", "host": ledger["host"],
                           "detail": "%s: live directory missing under %s"
                                     % (dot, ledger["root"])})
        for name in entry.get("unreadable", ()):
            if name != "roster.json" and not (name.endswith(".jsonl") and TOKEN_RE.fullmatch(name[:-6])):
                name = "live file with invalid agent name"
            errors.append({"source": "ledger", "host": ledger["host"],
                           "detail": "%s: %s unreadable" % (dot, name)})
        if entry.get("too_many"):
            errors.append({"source": "ledger", "host": ledger["host"],
                           "detail": "%s: more than %d live files; read the first %d"
                                     % (dot, MAX_LIVE_FILES, MAX_LIVE_FILES)})
    return dump


def read_dot(dot, dump, tail_bytes, roster_max_bytes=None):
    """Build per-agent record lists for one dot from a parsed dump."""
    tbytes = max(1, int(tail_bytes))
    rmax = max(1, int(roster_max_bytes or tbytes))
    entry = dump.get(dot) or {"files": {}}
    agents = {}
    roster = None
    roster_error = None
    dropped_files = []
    for name, info in entry["files"].items():
        if name == "roster.json":
            size = info.get("size")
            if size is not None and size > rmax:
                roster_error = "roster.json exceeds %d bytes (%d)" % (rmax, size)
                continue
            text = split_tail(info["text"], size, rmax)
            try:
                value = json.loads(text)
            except ValueError:
                roster_error = "roster.json is not JSON"
            else:
                if isinstance(value, dict):
                    roster = value
                else:
                    roster_error = "roster.json is not an object"
            continue
        text = split_tail(info["text"], info["size"], tbytes)
        agent = name[:-6] if name.endswith(".jsonl") else name
        # The file name becomes the AGENT column; hold it to the same token rule
        # as a record's agent field so a name cannot carry a line break or escape.
        if not TOKEN_RE.match(agent):
            dropped_files.append(name)
            continue
        records = []
        legacy = []
        for raw in text.splitlines():
            if not raw.strip():
                continue
            rec, reason = parse_record_line(raw)
            if rec is None:
                legacy.append({"reason": reason, "line": raw.strip()[:400],
                               "_t": legacy_time(raw)})
            else:
                records.append(rec)
        records.sort(key=lambda r: r["_t"])
        agents[agent] = {"records": records, "legacy": legacy}
    return {
        "agents": agents,
        "roster": roster,
        "roster_error": roster_error,
        "dropped_files": dropped_files,
        "nodir": entry.get("nodir", False),
        "error": entry.get("error"),
    }


def roster_slots(roster):
    """Return (slots, dropped) from a roster using ``slots``, ``agents`` or ``actors``.

    A slot whose agent id is not a token is dropped, not printed: the id is
    written by the coordinator and would otherwise reach the table verbatim.
    ``dropped`` counts those slots so the caller can report them.
    """
    slots = {}
    dropped = 0
    if not isinstance(roster, dict):
        return slots, dropped
    for key in ("slots", "agents", "actors"):
        if key not in roster:
            continue
        entries = roster[key]
        if not isinstance(entries, list):
            dropped += 1
            continue
        for slot in entries:
            if not (isinstance(slot, dict) and isinstance(slot.get("agent"), str)
                    and TOKEN_RE.match(slot["agent"])):
                dropped += 1
                continue
            merged = dict(slot)
            merged.setdefault("_key", key)
            slots.setdefault(slot["agent"], merged)
    return slots, dropped


# ---------------------------------------------------------------------------
# activity


LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)\s+\S+\s+request\s+"
    r"(?P<method>[A-Z]+)\s+(?P<path>\S+)\s+(?:\u2192|->)\s+(?P<status>\d{3})\s+in\s+"
    r"(?P<ms>\d+)ms(?P<rest>.*)$"
)


def activity_script(paths, tail_bytes, nonce):
    tbytes = max(1, int(tail_bytes))
    parts = ["set -u", "export LC_ALL=C", "i=0", probe_reader(tbytes, nonce)]
    parts.append("for p in %s; do" % " ".join(_remote_path(p) for p in paths))
    parts.append("""  printf '==LOG %s %%s\\n' "$i" """ % nonce)
    parts.append("  i=$((i+1))")
    parts.append('  if [ -f "$p" ]; then')
    parts.append('    read_probe "$p" "==SIZE %s" "==UNREAD %s"' % (nonce, nonce))
    parts.append('  else')
    parts.append("""    printf '==MISSING %s\\n' """ % nonce)
    parts.append('  fi')
    parts.append('done')
    return "\n".join(parts)


def parse_activity_dump(text, nonce):
    """Parse per-index byte sizes and log text, marking unreadable logs.

    Sentinels carry this run's nonce, so log content cannot end a section.
    """
    log_tag = "==LOG %s " % nonce
    missing_tag = "==MISSING %s" % nonce
    unread_tag = "==UNREAD %s" % nonce
    size_re = re.compile(r"^==SIZE %s ([0-9]+)$" % re.escape(nonce))
    end_tag = "==END %s" % nonce
    out = {}
    path = None
    size = None
    buf = []
    for line in text.splitlines():
        if line.startswith(log_tag):
            path = line[len(log_tag):].strip()
            size = None
            buf = []
            out.setdefault(path, {"size": None, "text": None})
            continue
        if path is None:
            continue
        if line == unread_tag or (size is None and line == missing_tag):
            if line == unread_tag:
                out[path]["error"] = "unreadable"
            path = None
            continue
        match = size_re.match(line)
        if match and size is None:
            size = int(match.group(1))
            out[path]["size"] = size
            continue
        if line == end_tag:
            out[path]["text"] = "\n".join(buf)
            path = None
            continue
        buf.append(line)
    return out


def count_calls(text, size, tail_bytes, now, windows, exclude_probes=True):
    """Count mcp/core requests inside each window and note the oldest line seen."""
    result = {"calls": {name: 0 for name in windows}, "last_call_at": None,
              "covered_since": None, "partial": False, "path": None}
    if text is None:
        return result
    body = split_tail(text, size, tail_bytes)
    oldest = None
    last = None
    for line in body.splitlines():
        match = LOG_LINE_RE.match(line.strip())
        if not match:
            continue
        ts = parse_ts(match.group("ts"))
        if ts is None:
            continue
        if oldest is None or ts < oldest:
            oldest = ts
        if match.group("method") != "POST" or not match.group("path").startswith("mcp/core"):
            continue
        if exclude_probes and "tunnel probe" in match.group("rest"):
            continue
        if last is None or ts > last:
            last = ts
        for name, span in windows.items():
            if ts >= now - span:
                result["calls"][name] += 1
    result["last_call_at"] = last
    result["covered_since"] = oldest
    longest = max(windows.values()) if windows else timedelta(0)
    result["partial"] = oldest is not None and oldest > now - longest
    return result


def collect_activity(config, transport, now, errors):
    """Return host counts and request timestamps for dots with a readable log."""
    windows = {
        "short": timedelta(minutes=config["thresholds"]["active_window_minutes"]),
        "long": timedelta(minutes=config["thresholds"]["long_window_minutes"]),
    }
    by_host = {}
    for src in config["activity"]:
        by_host.setdefault(src["host"], []).append(src)
    per_dot = {}
    for host, sources in by_host.items():
        nonce = new_nonce()
        script = activity_script([s["path"] for s in sources], config["limits"]["tail_bytes"],
                                 nonce)
        out, error = transport.run(host, script)
        if error:
            errors.append({"source": "activity", "host": host, "detail": error})
            continue
        dump = parse_activity_dump(out, nonce)
        for index, src in enumerate(sources):
            info = dump.get(str(index))
            if info is None:
                errors.append({"source": "activity", "host": host,
                               "detail": "no output for %s" % os.path.basename(src["path"])})
                continue
            if info["text"] is None:
                errors.append({"source": "activity", "host": host,
                               "detail": "%s %s" % (src["path"], info.get("error", "missing"))})
                continue
            counted = count_calls(info["text"], info["size"], config["limits"]["tail_bytes"],
                                  now, windows)
            counted["path"] = src["path"]
            counted["timestamps"] = _timestamps(info["text"], info["size"],
                                               config["limits"]["tail_bytes"])
            slot = per_dot.setdefault(src["dot"], {"by_host": {}, "timestamps": []})
            label = src.get("label") or host
            slot["by_host"][label] = counted
            slot["timestamps"].extend(counted["timestamps"])
    return per_dot


def _timestamps(text, size, tail_bytes):
    stamps = []
    for line in split_tail(text, size, tail_bytes).splitlines():
        match = LOG_LINE_RE.match(line.strip())
        if not match:
            continue
        if match.group("method") != "POST" or not match.group("path").startswith("mcp/core"):
            continue
        if "tunnel probe" in match.group("rest"):
            continue
        ts = parse_ts(match.group("ts"))
        if ts is not None:
            stamps.append(ts)
    return stamps


RETAINED_LIMIT_RE = re.compile(
    r'`: CreateProcess \{ message: "too many retained terminal sessions \(limit (\d+)\);'
    r'[^"]*before starting another" \}\Z'
)


def daemon_health_script(sess_dirs, tail_bytes, max_files, window_minutes, nonce):
    """Build a read-only POSIX shell script scanning recent */events.jsonl under configured session dirs."""
    tail_bytes = max(1, int(tail_bytes))
    max_files = max(1, int(max_files))
    window_minutes = max(1, int(window_minutes))
    parts = [
        "set -u",
        "export LC_ALL=C",
        "i=0",
        probe_reader(tail_bytes, nonce),
        "for s in %s; do" % " ".join(_remote_path(p) for p in sess_dirs),
        """  printf '==DIR %s %%s\\n' "$i" """ % nonce,
        "  i=$((i+1))",
        """  if [ ! -d "$s" ]; then printf '==NOSESS %s\\n'; continue; fi""" % nonce,
        "  n=0",
        '  find "$s" -maxdepth 2 -type f -name events.jsonl -mmin -%d 2>/dev/null | while IFS= read -r f; do' % window_minutes,
        '    [ -f "$f" ] || continue',
        "    n=$((n+1))",
        """    [ "$n" -le %d ] || { printf '==TOOMANY %s\\n'; break; }""" % (max_files, nonce),
        '    read_probe "$f" "==FILE %s ${f#$s/}" "==UNREAD %s ${f#$s/}"' % (nonce, nonce),
        "  done",
        "done",
    ]
    return "\n".join(parts)


def parse_daemon_health_dump(text, nonce):
    """Parse multi-session read_probe output keyed by configured dir index."""
    dir_tag = "==DIR %s " % nonce
    nosess_tag = "==NOSESS %s" % nonce
    too_many_tag = "==TOOMANY %s" % nonce
    unread_tag = "==UNREAD %s " % nonce
    file_re = re.compile(r"^==FILE %s (.+) ([0-9]+)$" % re.escape(nonce))
    end_tag = "==END %s" % nonce
    out = {}
    cur = None
    name = None
    size = None
    buf = []
    for line in text.splitlines():
        if line.startswith(dir_tag):
            cur = line[len(dir_tag):].strip()
            out.setdefault(cur, {"files": {}, "nosess": False, "too_many": False, "unreadable": []})
            name = None
            buf = []
            continue
        if cur is None:
            continue
        if line == nosess_tag:
            out[cur]["nosess"] = True
            continue
        if line == too_many_tag and name is None:
            out[cur]["too_many"] = True
            continue
        if line.startswith(unread_tag):
            out[cur]["unreadable"].append(line[len(unread_tag):])
            name = None
            buf = []
            continue
        match = file_re.match(line)
        if match and name is None:
            name = match.group(1)
            size = int(match.group(2))
            buf = []
            continue
        if line == end_tag and name is not None:
            out[cur]["files"][name] = {"size": size, "text": "\n".join(buf)}
            name = None
            buf = []
            continue
        if name is not None:
            buf.append(line)
    return out


def _entry_timestamp(obj):
    if not isinstance(obj, dict):
        return None
    for key in ("t", "ts", "timestamp", "time", "at"):
        val = obj.get(key)
        if isinstance(val, str):
            parsed = parse_ts(val)
            if parsed is not None:
                return parsed
    return None


def _analyze_daemon_session_text(text, size, tail_bytes, now, window_minutes):
    """Scan bounded session JSONL text for retained terminal session rejections and chat_error entries."""
    cutoff = now - timedelta(minutes=window_minutes)
    retained_hits = 0
    retained_limit = None
    last_retained_at = None
    chat_error_hits = 0
    last_chat_error_at = None
    for raw in split_tail(text, size, tail_bytes).splitlines():
        raw_s = raw.strip()
        if not raw_s:
            continue
        try:
            obj = json.loads(raw_s)
        except ValueError:
            continue
        if not isinstance(obj, dict):
            continue
        ts = _entry_timestamp(obj)
        if ts is not None and ts < cutoff:
            continue
        ev_type = str(obj.get("type") or obj.get("event") or obj.get("kind") or "")
        if ev_type == "chat_error" or bool(obj.get("chat_error")):
            chat_error_hits += 1
            if ts is not None and (last_chat_error_at is None or ts > last_chat_error_at):
                last_chat_error_at = ts
        candidates = []
        call = obj.get("call")
        if isinstance(call, dict):
            outcome = call.get("outcome")
            if outcome in (None, "tool_rejected", "error", "failed"):
                res = call.get("result")
                if isinstance(res, dict) and isinstance(res.get("text"), str):
                    candidates.append(res["text"])
                elif isinstance(res, str):
                    candidates.append(res)
        for key in ("error",):
            val = obj.get(key)
            if isinstance(val, str):
                candidates.append(val)
            elif isinstance(val, dict) and isinstance(val.get("text"), str):
                candidates.append(val["text"])
        matched = False
        for txt in candidates:
            m = RETAINED_LIMIT_RE.search(txt.rstrip())
            if m:
                matched = True
                lim = int(m.group(1))
                retained_limit = lim if retained_limit is None else max(retained_limit, lim)
                break
        if matched:
            retained_hits += 1
            if ts is not None and (last_retained_at is None or ts > last_retained_at):
                last_retained_at = ts
    return {
        "retained_hits": retained_hits,
        "retained_limit": retained_limit,
        "last_retained_at": last_retained_at,
        "chat_error_hits": chat_error_hits,
        "last_chat_error_at": last_chat_error_at,
    }


def collect_daemon_health(config, transport, now, errors):
    """Collect optional read-only daemon session health per configured activity source or daemon_health.sessions_dir."""
    dh_cfg = config.get("daemon_health") or {}
    if not dh_cfg.get("enabled"):
        return {}
    tail_bytes = max(1, int(dh_cfg.get("tail_bytes", 131072)))
    max_files = max(1, int(dh_cfg.get("max_session_files", 20)))
    window_min = max(1, int(dh_cfg.get("window_minutes", 30)))
    default_sess = dh_cfg.get("sessions_dir")
    by_host = {}
    for src in config.get("activity") or []:
        sess_dir = src.get("sessions_dir") or (default_sess if isinstance(default_sess, str) and default_sess.strip() else None)
        if not sess_dir:
            base_dir = os.path.dirname(src["path"])
            if not base_dir:
                continue
            sess_dir = base_dir.rstrip("/") + "/sessions"
        by_host.setdefault(src["host"], []).append((src, sess_dir))
    if not by_host and isinstance(default_sess, str) and default_sess.strip():
        dots_cfg = (config.get("ledger") or {}).get("dots") or []
        first_dot = dots_cfg[0] if dots_cfg else "DOT"
        by_host.setdefault("local", []).append(({"dot": first_dot, "host": "local", "label": "local-sessions"}, default_sess.strip()))
    per_dot = {}
    for host, items in by_host.items():
        nonce = new_nonce()
        script = daemon_health_script([d for _s, d in items], tail_bytes, max_files, window_min, nonce)
        out, error = transport.run(host, script)
        if error:
            errors.append({"source": "daemon_health", "host": host, "detail": error})
            continue
        dump = parse_daemon_health_dump(out, nonce)
        for index, (src, sess_dir) in enumerate(items):
            info = dump.get(str(index))
            if info is None:
                continue
            for unread_name in info.get("unreadable") or []:
                errors.append({"source": "daemon_health", "host": host,
                               "detail": "%s unreadable: %s" % (src.get("dot"), one_line(unread_name, 80))})
            r_hits = 0
            r_lim = None
            r_last = None
            c_hits = 0
            c_last = None
            for fname, finfo in info["files"].items():
                stats = _analyze_daemon_session_text(finfo["text"], finfo["size"], tail_bytes, now, window_min)
                r_hits += stats["retained_hits"]
                if stats["retained_limit"] is not None:
                    r_lim = stats["retained_limit"] if r_lim is None else max(r_lim, stats["retained_limit"])
                if stats["last_retained_at"] is not None and (r_last is None or stats["last_retained_at"] > r_last):
                    r_last = stats["last_retained_at"]
                c_hits += stats["chat_error_hits"]
                if stats["last_chat_error_at"] is not None and (c_last is None or stats["last_chat_error_at"] > c_last):
                    c_last = stats["last_chat_error_at"]
            label = src.get("label") or host
            slot = per_dot.setdefault(src["dot"], {})
            slot[label] = {
                "sessions_dir": sess_dir,
                "sessions_scanned": len(info["files"]),
                "retained_limit_hits": r_hits,
                "retained_limit": r_lim,
                "last_retained_at": iso(r_last),
                "chat_error_hits": c_hits,
                "last_chat_error_at": iso(c_last),
                "truncated_scan": bool(info.get("too_many")),
            }
    return per_dot


def detect_start_churn(records, now, window_minutes=60, min_starts=3):
    """Return churn detail string when recent trailing records are consecutive 'start' lines."""
    if not records:
        return None
    cutoff = now - timedelta(minutes=window_minutes)
    count = 0
    tasks = set()
    for rec in reversed(records):
        if rec.get("state") != "start":
            break
        t = rec.get("_t")
        if t is None or t < cutoff or t > now + timedelta(minutes=5):
            break
        count += 1
        tname = str(rec.get("task") or "").strip()
        if tname:
            tasks.add(tname)
    if count >= min_starts and (len(tasks) >= 2 or count >= min_starts + 1):
        return "%d consecutive start lines (%d task(s)) in %dm without step/done" % (count, len(tasks), window_minutes)
    return None


# ---------------------------------------------------------------------------
# evidence


GH_URL_RE = re.compile(
    r"https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)(/[^\s;,)\]\"']*)?"
)
SHA_RE = re.compile(r"(?<![0-9A-Za-z])[0-9a-f]{40}(?![0-9A-Za-z])")


def evidence_refs(text):
    """Return GitHub references and bare commit SHAs named by an evidence string."""
    if not text:
        return [], []
    refs = []
    for match in GH_URL_RE.finditer(text):
        owner, repo, rest = match.group(1), match.group(2), (match.group(3) or "")
        if owner in (".", "..") or repo in (".", "..") or ".." in owner or ".." in repo:
            refs.append({"kind": "invalid", "label": "invalid repo path"})
            continue
        path, _, anchor = rest.partition("#")
        segs = [s for s in path.partition("?")[0].split("/") if s]
        base = "repos/%s/%s" % (owner, repo)
        repository = "%s/%s" % (owner, repo)
        conversation = bool(segs) and segs[0] in ("issues", "pull")
        if conversation and ("issuecomment-" in anchor or any("issuecomment-" in s for s in segs[1:])):
            comment = re.fullmatch(r"issuecomment-([0-9]+)", anchor)
            if len(segs) == 2 and comment and re.fullmatch(r"[0-9]+", segs[1]):
                cid = comment[1]
                refs.append({"kind": "comment", "endpoint": "%s/issues/comments/%s" % (base, cid),
                             "id": cid, "permalink": "https://github.com/%s/%s/%s#issuecomment-%s"
                             % (repository, segs[0], segs[1], cid),
                             "label": "%s comment %s" % (repository, cid), "repo": repository})
            else:
                refs.append({"kind": "invalid", "label": "%s issue comment" % repository})
        elif len(segs) >= 2 and segs[0] == "pull" and segs[1].isdigit():
            refs.append({"kind": "pull", "endpoint": "%s/pulls/%s" % (base, segs[1]),
                         "label": "%s/%s#%s" % (owner, repo, segs[1]), "repo": repository})
        elif len(segs) == 3 and segs[:2] == ["issues", "comments"] and segs[2].isdigit():
            refs.append({"kind": "comment", "endpoint": "%s/issues/comments/%s" % (base, segs[2]),
                         "id": segs[2], "label": "%s comment %s" % (repository, segs[2]), "repo": repository})
        elif len(segs) >= 2 and segs[0] == "issues" and segs[1].isdigit():
            refs.append({"kind": "issue", "endpoint": "%s/issues/%s" % (base, segs[1]),
                         "label": "%s/%s#%s" % (owner, repo, segs[1]), "repo": repository})
        elif len(segs) >= 2 and segs[0] in ("commit", "blob", "tree"):
            ref_tok = segs[1]
            if ref_tok in (".", "..") or ".." in ref_tok or not re.fullmatch(r"[A-Za-z0-9._-]+", ref_tok):
                refs.append({"kind": "invalid", "label": "%s/%s ref" % (owner, repo)})
            else:
                refs.append({"kind": "commit", "endpoint": "%s/commits/%s" % (base, ref_tok),
                             "label": "%s/%s@%s" % (owner, repo, ref_tok[:7]), "repo": "%s/%s" % (owner, repo)})
        elif len(segs) >= 3 and segs[0] == "actions" and segs[1] == "runs" and segs[2].isdigit():
            refs.append({"kind": "run", "endpoint": "%s/actions/runs/%s" % (base, segs[2]),
                         "label": "%s/%s run %s" % (owner, repo, segs[2]), "repo": "%s/%s" % (owner, repo)})
        else:
            refs.append({"kind": "repo", "endpoint": base,
                         "label": "%s/%s" % (owner, repo), "repo": "%s/%s" % (owner, repo)})
    matches = list(GH_URL_RE.finditer(text))
    if len(matches) == len(refs):
        for ref, match in zip(refs, matches):
            ref["end"] = match.end()
    url_shas = set()
    for match in GH_URL_RE.finditer(text):
        rest = match.group(3) or ""
        for sha in SHA_RE.findall(rest):
            url_shas.add(sha)
    bare = [sha for sha in SHA_RE.findall(text) if sha not in url_shas]
    return refs, bare


class EvidenceResolver:
    """Resolve GitHub references through ``gh api``, with a small cache."""

    def __init__(self, config, transport, now, errors):
        self.config = config["github"]
        self.transport = transport
        self.now = now
        self.errors = errors
        self.cache = getattr(transport, "gh_cache", None)
        if self.cache is None:
            self.cache = {}
            setattr(transport, "gh_cache", self.cache)
        self.lookups = 0
        self.limit = int(self.config.get("max_lookups", 60))

    def _lookup(self, endpoint, jq):
        if not self.config.get("enabled", True):
            return None, "unknown", "github resolution disabled"
        if self.lookups >= self.limit:
            return None, "unknown", "lookup budget exhausted"
        key = (endpoint, jq)
        cached = self.cache.get(key)
        if cached and (self.now - cached[0]).total_seconds() < float(self.config.get("cache_seconds", 300)):
            return cached[1], cached[2], cached[3]
        self.lookups += 1
        data, error = self.transport.gh(endpoint, jq)
        if error:
            absent = ("HTTP 404" in error or "HTTP 410" in error
                      or ("HTTP 422" in error and "No commit found" in error))
            status = "unresolved" if absent else "unknown"
            if status == "unknown":
                self.errors.append({"source": "github", "host": "gh", "detail": error})
            result = (data, status, one_line(error, 160))
        else:
            result = (data, "resolved", "")
        self.cache[key] = (self.now, result[0], result[1], result[2])
        return result

    def resolve(self, ref):
        kind = ref["kind"]
        if kind == "invalid":
            return "unresolved", "malformed issue comment URL"
        if kind == "pull":
            data, status, detail = self._lookup(
                ref["endpoint"], "{state: .state, draft: .draft, merged: .merged, "
                                 "head: .head.sha, merge: .merge_commit_sha}")
            if status == "resolved" and isinstance(data, dict):
                ref["head"] = data.get("head")
                ref["merge"] = data.get("merge")
                ref["state"] = data.get("state")
                ref["merged"] = data.get("merged")
            return status, detail
        if kind == "issue":
            data, status, detail = self._lookup(ref["endpoint"], "{state: .state}")
            if status == "resolved" and isinstance(data, dict):
                ref["state"] = data.get("state")
            return status, detail
        if kind == "comment":
            data, status, detail = self._lookup(ref["endpoint"], "{id: .id, html_url: .html_url}")
            if status == "resolved":
                if not isinstance(data, dict) or str(data.get("id")) != ref["id"]:
                    return "unresolved", "response does not identify this comment"
                if "permalink" in ref and str(data.get("html_url")).lower() != ref["permalink"].lower():
                    return "unresolved", "comment does not identify the cited parent"
            return status, detail
        if kind == "commit":
            data, status, detail = self._lookup(ref["endpoint"], "{sha: .sha}")
            return status, detail
        if kind == "run":
            data, status, detail = self._lookup(ref["endpoint"], "{status: .status, conclusion: .conclusion}")
            if status == "resolved" and isinstance(data, dict):
                ref["state"] = data.get("conclusion") or data.get("status")
            return status, detail
        data, status, detail = self._lookup(ref["endpoint"], "{full_name: .full_name}")
        return status, detail

    def resolve_sha(self, sha):
        repos = self.config.get("repos") or []
        if not repos:
            return "unknown", "no repositories configured"
        last = "not found in configured repositories"
        for repo in repos:
            data, status, detail = self._lookup("repos/%s/commits/%s" % (repo, sha), "{sha: .sha}")
            if status == "resolved":
                return "resolved", repo
            if status == "unknown":
                return "unknown", detail
            last = "%s not in %s" % (sha[:7], repo)
        return "unresolved", last


def evidence_status(text, resolver):
    """Return (status, detail, refs) for one evidence string.

    status is one of: resolved, stale, local, unresolved, unknown, missing.
    """
    if not text or not str(text).strip():
        return "missing", "no evidence", []
    refs, bare = evidence_refs(str(text))
    if not refs and not bare:
        return "local", "no resolvable reference", []
    statuses = []
    notes = []
    for ref in refs:
        status, detail = resolver.resolve(ref)
        statuses.append(status)
        if detail:
            notes.append("%s: %s" % (ref["label"], detail))
    # "pull/17 @<sha>" claims a pull request head; that SHA is proven by the pull
    # request itself, so it is compared instead of being looked up as a commit.
    claimed_heads = {}
    for ref in refs:
        if ref["kind"] != "pull" or "end" not in ref:
            continue
        window = str(text)[ref["end"]:ref["end"] + 48]
        found = SHA_RE.search(window)
        if found:
            claimed_heads[ref["label"]] = found.group(0)
    for sha in bare:
        if sha in claimed_heads.values():
            continue
        status, detail = resolver.resolve_sha(sha)
        statuses.append(status)
        if detail:
            notes.append("%s: %s" % (sha[:7], detail))
    # A cited head SHA that no longer matches the pull request is drift, not proof.
    stale = []
    for ref in refs:
        cited = claimed_heads.get(ref["label"])
        if not cited or not ref.get("head"):
            continue
        known = {value for value in (ref.get("head"), ref.get("merge")) if value}
        if cited not in known:
            stale.append("%s head is %s, record cites %s"
                         % (ref["label"], ref["head"][:7], cited[:7]))
    if "unresolved" in statuses:
        return "unresolved", "; ".join(notes) or "does not resolve", refs
    if "unknown" in statuses:
        return "unknown", "; ".join(notes) or "not checked", refs
    if stale:
        return "stale", "; ".join(stale), refs
    return "resolved", "%d reference(s) exist" % len(statuses), refs


# ---------------------------------------------------------------------------
# snapshot


def classify(row, stall_minutes):
    if not row["file_present"]:
        return "NO_RECORD"
    age = row["age_seconds"]
    stalled = age is None or age > stall_minutes * 60
    if row["state"] == "unconfirmed":
        # No v1 line means no confirmed clock (live-status: "an agent with no v1
        # line is checked as stalled"). Only a recent off-format line keeps it
        # LEGACY instead.
        return "STALLED" if stalled else "LEGACY"
    status = row["evidence_status"]
    if row["state"] == "blocked":
        return "BLOCKED"
    if row["state"] == "done":
        # A drifted pull-request head is not proof; "unknown" means GitHub could
        # not be asked, which is reported as an error rather than as a failure.
        if status in ("resolved", "unknown"):
            return "DONE"
        return "UNPROVEN"
    if status in ("missing", "unresolved"):
        return "UNPROVEN"
    if stalled:
        return "STALLED"
    if row["activity_since_record"] is None:
        return "ACTIVITY_UNKNOWN"
    return "WORKING" if row["activity_since_record"] > 0 else "QUIET"


def build_snapshot(config, transport, now, errors=None):
    if errors is None:
        errors = []
    thresholds = config.get("thresholds") or {}
    stall = thresholds["stall_minutes"]
    churn_window = int(thresholds.get("start_churn_window_minutes", 60))
    churn_repeats = int(thresholds.get("start_churn_repeats", 3))
    limits = config.get("limits") or {}
    tail_bytes = limits["tail_bytes"]
    roster_max_bytes = limits.get("roster_max_bytes", 1048576)
    ledger = collect_ledger(config, transport, errors)
    activity = collect_activity(config, transport, now, errors)
    daemon_health = collect_daemon_health(config, transport, now, errors)
    activity_configured = {src["dot"] for src in config["activity"]}
    resolver = EvidenceResolver(config, transport, now, errors)
    dots = {}
    for dot in config["ledger"]["dots"]:
        data = read_dot(dot, ledger, tail_bytes, roster_max_bytes=roster_max_bytes)
        slots, dropped_slots = roster_slots(data["roster"])
        if data["roster_error"]:
            errors.append({"source": "roster", "host": config["ledger"]["host"],
                           "detail": "%s: %s" % (dot, data["roster_error"])})
        if dropped_slots:
            errors.append({"source": "roster", "host": config["ledger"]["host"],
                           "detail": "%s: %d roster slot(s) dropped: expected lists with valid agent ids"
                                     % (dot, dropped_slots)})
        if data["dropped_files"]:
            errors.append({"source": "ledger", "host": config["ledger"]["host"],
                           "detail": "%s: %d live file(s) skipped: name is not an agent token"
                                     % (dot, len(data["dropped_files"]))})
        dot_activity = activity.get(dot, {"by_host": {}, "timestamps": []})
        stamps = dot_activity["timestamps"]
        rows = []
        agent_ids = sorted(set(data["agents"]) | set(slots))
        for agent in agent_ids:
            entry = data["agents"].get(agent)
            slot = slots.get(agent) or {}
            records = entry["records"] if entry else []
            legacy = entry["legacy"] if entry else []
            latest = records[-1] if records else None
            legacy_t = max((l["_t"] for l in legacy if l["_t"]), default=None)
            if latest:
                state = latest["state"]
                role = latest["role"]
                task = latest["task"]
                note = latest["note"]
                evidence = latest["evidence"]
                project = latest["project"]
                host = latest["host"]
                model = latest["model"]
                last_t = latest["_t"]
            else:
                state = "unconfirmed"
                role = slot.get("role") or "?"
                task = slot.get("task") or "?"
                note = legacy[-1]["reason"] if legacy else "no live record"
                evidence = ""
                project = slot.get("project") or ""
                host = slot.get("host") or ""
                model = slot.get("model") or ""
                last_t = None
            # The stall clock runs from the newest v1 line; an agent without one
            # falls back to its newest off-format line that still has a time.
            clock_t = last_t if last_t is not None else legacy_t
            age = (now - clock_t).total_seconds() if clock_t else None
            since = None
            if dot in activity:
                since = 0 if clock_t is None else sum(1 for ts in stamps if ts >= clock_t)
            status, detail, refs = evidence_status(evidence, resolver)
            churn_detail = detect_start_churn(records, now, window_minutes=churn_window, min_starts=churn_repeats)
            row = {
                "dot": dot,
                "agent": agent,
                "role": role,
                "task": task,
                "state": state,
                "project": project,
                "host": host,
                "model": model,
                "last_record_at": iso(last_t),
                "legacy_last_at": iso(legacy_t),
                "age_seconds": int(age) if age is not None else None,
                "age": age_text(age),
                "note": one_line(note, 200),
                "evidence": one_line(evidence, 300),
                "evidence_status": status,
                "evidence_detail": one_line(detail, 200),
                "evidence_refs": [{"label": r["label"], "kind": r["kind"],
                                   "state": r.get("state")} for r in refs],
                "activity_since_record": since,
                "file_present": entry is not None,
                "legacy_lines": len(legacy),
                "legacy_reasons": sorted({l["reason"] for l in legacy})[:4],
                "in_roster": bool(slot),
                "roster_state": slot.get("state"),
                "records": len(records),
                "start_churn": churn_detail is not None,
                "start_churn_detail": churn_detail or "",
            }
            row["classification"] = classify(row, stall)
            rows.append(row)
        rows.sort(key=lambda r: (CLASS_ORDER.index(r["classification"]), r["agent"]))
        dots[dot] = {
            "account": (config["dots"].get(dot) or {}).get("account"),
            "name": (config["dots"].get(dot) or {}).get("name"),
            "roster": {
                "coordinator": (data["roster"] or {}).get("coordinator"),
                "updated_at": (data["roster"] or {}).get("updated_at"),
                "slots": len(slots),
            },
            "activity_configured": dot in activity_configured,
            "activity": {
                label: {
                    "calls_15m": value["calls"].get("short", 0),
                    "calls_60m": value["calls"].get("long", 0),
                    "last_call_at": iso(value["last_call_at"]),
                    "covered_since": iso(value["covered_since"]),
                    "partial_window": value["partial"],
                    "path": value["path"],
                }
                for label, value in sorted(dot_activity["by_host"].items())
            },
            "daemon_health": daemon_health.get(dot, {}),
            "agents": rows,
            "unrostered": [r["agent"] for r in rows if not r["in_roster"]],
        }
    return {
        "tool": "dots-monitor",
        "version": TOOL_VERSION,
        "generated_at": iso(now),
        "config_dots": config["ledger"]["dots"],
        "thresholds": dict(config["thresholds"]),
        "dots": dots,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# output


def dwidth(text):
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


def pad(text, width):
    return text + " " * max(0, width - dwidth(text))


def render_table(snapshot):
    lines = []
    lines.append("dots-monitor %s  (live-record-v1 + Web GPT Agent daemon requests)"
                 % snapshot["generated_at"])
    if snapshot["errors"]:
        for err in snapshot["errors"]:
            lines.append("DOTS_ERROR %s %s" % (err["source"], one_line(err["detail"], 200)))
    for dot, data in snapshot["dots"].items():
        account = data.get("account")
        head = "%s" % dot
        if account:
            head += " (account %s)" % account
        roster = data["roster"]
        lines.append("")
        lines.append("%s  roster=%s@%s slots=%s" % (
            head, one_line(roster.get("coordinator"), 40) or "?",
            one_line(str(roster.get("updated_at") or "")[:19], 40) or "?", roster.get("slots") or 0))
        for label, act in data["activity"].items():
            lines.append("  activity %-6s 15m=%-4d 60m=%-5d last=%s covered_since=%s%s" % (
                one_line(label, 30), act["calls_15m"], act["calls_60m"], act["last_call_at"] or "-",
                act["covered_since"] or "-", " (window clipped by log rotation)" if act["partial_window"] else ""))
        if not data["activity"]:
            lines.append("  activity unknown" if data["activity_configured"]
                         else "  activity none configured")
        for label, dh in sorted((data.get("daemon_health") or {}).items()):
            if dh.get("retained_limit_hits") or dh.get("chat_error_hits") or dh.get("truncated_scan"):
                trunc_s = " (truncated >max_files)" if dh.get("truncated_scan") else ""
                lines.append("  ALERT daemon %-6s retained_limit_hits=%d limit=%s chat_errors=%d sessions=%d%s" % (
                    one_line(label, 30), dh.get("retained_limit_hits", 0), dh.get("retained_limit") or "?",
                    dh.get("chat_error_hits", 0), dh.get("sessions_scanned", 0), trunc_s))
        headers = ("AGENT", "CLASS", "STATE", "AGE", "EVID", "ROLE", "TASK")
        rows = []
        for row in data["agents"]:
            rows.append((
                one_line(row["agent"], 128), row["classification"], row["state"], row["age"],
                row["evidence_status"], one_line(row["role"], 10),
                one_line(row["task"], 30),
            ))
        widths = [max([dwidth(h)] + [dwidth(r[i]) for r in rows]) for i, h in enumerate(headers)]
        lines.append("  " + "  ".join(pad(h, widths[i]) for i, h in enumerate(headers)).rstrip())
        lines.append("  " + "  ".join("-" * widths[i] for i in range(len(headers))).rstrip())
        for row, agent_row in zip(rows, data["agents"]):
            lines.append("  " + "  ".join(pad(row[i], widths[i])
                                          for i in range(len(headers))).rstrip())
            lines.append("      note: %s  [project=%s host=%s records=%d]" % (
                one_line(agent_row["note"], 100),
                one_line(agent_row["project"], 20) or "-",
                one_line(agent_row["host"], 20) or "-",
                agent_row["records"]))
            if agent_row["evidence_detail"] and agent_row["evidence_status"] != "resolved":
                lines.append("      evidence: %s (%s)" % (
                    one_line(agent_row["evidence"], 80) or "-",
                    agent_row["evidence_detail"]))
            if agent_row.get("start_churn"):
                lines.append("      warn: START-CHURN (%s)" % one_line(agent_row.get("start_churn_detail"), 120))
        unrostered = data.get("unrostered") or []
        if unrostered:
            lines.append("  note: live file without roster slot: %s"
                         % one_line(", ".join(unrostered), 300))
        missing = [r["agent"] for r in data["agents"] if not r["file_present"]]
        if missing:
            lines.append("  note: roster slot without live file: %s"
                         % one_line(", ".join(missing), 300))
    stall = (snapshot.get("thresholds") or {}).get("stall_minutes", 30)
    lines.append("")
    lines.append("classes: WORKING record<=%dm and tool calls since it | QUIET record<=%dm, no calls since it | "
                 "ACTIVITY_UNKNOWN no activity source answered | "
                 "STALLED start/step older than %dm, or only off-format lines and none in %dm | BLOCKED | "
                 "DONE evidence resolves or GitHub was unreachable | UNPROVEN evidence missing/unresolved, "
                 "or done on local-only or stale evidence | LEGACY recent off-format lines only | "
                 "NO_RECORD roster slot without file"
                 % (stall, stall, stall, stall))
    return "\n".join(lines)


def render_json(snapshot):
    return json.dumps(redact_tree(snapshot), ensure_ascii=False, indent=2, sort_keys=True)


# ---------------------------------------------------------------------------
# watch


def diff_events(previous, snapshot):
    """Return (events, state) where events hold only real changes."""
    events = []
    state = {"agents": {}, "activity": {}, "errors": set(), "daemon_health": {}}
    prev_dh = previous.get("daemon_health") or {}
    for dot, data in snapshot["dots"].items():
        for row in data["agents"]:
            key = (dot, row["agent"])
            old = previous["agents"].get(key)
            churn_now = bool(row.get("start_churn"))
            state["agents"][key] = {"classification": row["classification"],
                                    "evidence_status": row["evidence_status"],
                                    "state": row["state"], "task": row["task"],
                                    "start_churn": churn_now}
            if old is None:
                events.append({"event": "DOTS_NEW_AGENT", "dot": dot, "agent": row["agent"],
                               "detail": {"role": row["role"], "task": row["task"],
                                          "class": row["classification"],
                                          "evidence": row["evidence_status"]}})
                if churn_now:
                    events.append({"event": "DOTS_START_CHURN", "dot": dot, "agent": row["agent"],
                                   "detail": {"task": row["task"], "detail": row.get("start_churn_detail")}})
                continue
            if old["classification"] != row["classification"]:
                events.append({
                    "event": "DOTS_" + row["classification"],
                    "dot": dot,
                    "agent": row["agent"],
                    "detail": {"from": old["classification"], "state": row["state"],
                               "age": row["age"], "task": row["task"],
                               "evidence": row["evidence_status"], "note": row["note"]},
                })
            if churn_now and not old.get("start_churn"):
                events.append({"event": "DOTS_START_CHURN", "dot": dot, "agent": row["agent"],
                               "detail": {"task": row["task"], "detail": row.get("start_churn_detail")}})
        dh_map = data.get("daemon_health") or {}
        if any(e.get("source") == "daemon_health" for e in snapshot.get("errors") or []):
            for dh_key, old_dh in prev_dh.items():
                if isinstance(dh_key, tuple) and dh_key[0] == dot and dh_key[1] not in dh_map:
                    state["daemon_health"][dh_key] = old_dh
        for label, dh in sorted(dh_map.items()):
            dh_key = (dot, label)
            has_ret = bool(dh.get("retained_limit_hits"))
            has_chat = bool(dh.get("chat_error_hits"))
            state["daemon_health"][dh_key] = {"retained": has_ret, "chat": has_chat}
            old_dh = prev_dh.get(dh_key) or {"retained": False, "chat": False}
            if has_ret and not old_dh.get("retained"):
                events.append({"event": "DOTS_DAEMON_LIMIT", "dot": dot, "agent": "-",
                               "detail": {"source": one_line(label, 30), "detail": "retained=%d limit=%s" % (
                                   dh.get("retained_limit_hits", 0), dh.get("retained_limit") or "?")}})
            if has_chat and not old_dh.get("chat"):
                events.append({"event": "DOTS_CHAT_ERROR", "dot": dot, "agent": "-",
                               "detail": {"source": one_line(label, 30), "detail": "chat_errors=%d" % dh.get("chat_error_hits", 0)}})
            if (old_dh.get("retained") and not has_ret) or (old_dh.get("chat") and not has_chat):
                if not has_ret and not has_chat:
                    events.append({"event": "DOTS_DAEMON_RECOVERED", "dot": dot, "agent": "-",
                                   "detail": {"source": one_line(label, 30)}})
        if not data["activity"]:
            # Compare the next successful read with the last observed activity.
            if dot in previous["activity"]:
                state["activity"][dot] = previous["activity"][dot]
            continue
        calls = sum(act["calls_15m"] for act in data["activity"].values())
        active = calls > 0
        old_active = previous["activity"].get(dot)
        state["activity"][dot] = active
        if old_active is None:
            continue
        if active and not old_active:
            events.append({"event": "DOTS_ACTIVE", "dot": dot, "agent": "-",
                           "detail": {"calls_15m": calls}})
        elif old_active and not active:
            events.append({"event": "DOTS_IDLE", "dot": dot, "agent": "-",
                           "detail": {"calls_15m": calls}})
    current_errors = {(e["source"], e["detail"]) for e in snapshot["errors"]}
    state["errors"] = current_errors
    for source, detail in sorted(current_errors - previous["errors"]):
        events.append({"event": "DOTS_ERROR", "dot": "-", "agent": "-",
                       "detail": {"source": source, "detail": one_line(detail, 200)}})
    for source, detail in sorted(previous["errors"] - current_errors):
        events.append({"event": "DOTS_RECOVERED", "dot": "-", "agent": "-",
                       "detail": {"source": source}})
    return events, state


def format_event(event):
    detail = event.get("detail") or {}
    parts = [event["event"], str(event["dot"]), str(event["agent"])]
    for key in ("from", "class", "state", "age", "evidence", "calls_15m", "role", "task",
                "source", "note", "detail"):
        if key in detail and detail[key] not in (None, ""):
            value = detail[key]
            if isinstance(value, str) and (" " in value or '"' in value):
                value = '"%s"' % value.replace('"', "'")
            parts.append("%s=%s" % (key, value))
    return one_line(" ".join(parts), 400)


def emit_event(event, as_json):
    if as_json:
        print(json.dumps(redact_tree(event), ensure_ascii=False), flush=True)
    else:
        print(format_event(event), flush=True)


def watch(config, transport, interval, as_json, out=sys.stdout):
    previous = {"agents": {}, "activity": {}, "errors": set(), "daemon_health": {}}
    first = True
    while True:
        started = time.time()
        errors = []
        try:
            snapshot = build_snapshot(config, transport, utcnow(), errors)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # keep the watcher alive on a source failure
            snapshot = {"generated_at": iso(utcnow()), "dots": {},
                        "errors": [{"source": "snapshot", "detail": "%s: %s" % (type(exc).__name__, exc)}]}
        events, state = diff_events(previous, snapshot)
        previous = state
        if first:
            first = False
            if not as_json:
                print("DOTS_MONITOR started at %s interval=%ss" % (snapshot["generated_at"], interval),
                      flush=True)
            for dot, data in snapshot["dots"].items():
                detail = {"agents": len(data["agents"])}
                if data["activity"]:
                    detail["calls_15m"] = sum(a["calls_15m"] for a in data["activity"].values())
                emit_event({"event": "DOTS_DOT", "dot": dot, "agent": "-", "detail": detail}, as_json)
        for event in events:
            emit_event(event, as_json)
        elapsed = time.time() - started
        time.sleep(max(1.0, interval - elapsed))


# ---------------------------------------------------------------------------
# transport


class Transport:
    def __init__(self, timeout=25, gh_timeout=25):
        self.timeout = timeout
        self.gh_timeout = gh_timeout

    def run(self, host, script):
        """Run one read-only shell script on a host; return (stdout, error)."""
        if host in (None, "", "local", "localhost"):
            cmd = ["/bin/sh", "-c", script]
        else:
            # "--" ends ssh's options, so a configured host can never be parsed as one.
            cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                   "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2",
                   "--", host, script]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, errors="replace",
                                  timeout=self.timeout)
        except FileNotFoundError:
            return None, "ssh not found"
        except subprocess.TimeoutExpired:
            return None, "timed out after %ss" % self.timeout
        # Shell/SSH diagnostics can contain private paths or arbitrary file data.
        # Neither a successful final printf nor captured stdout proves a clean probe.
        if proc.returncode != 0 or (proc.stderr or "").strip():
            return None, "probe exit %s%s" % (proc.returncode, " (stderr)" if proc.stderr else "")
        return proc.stdout, None

    def gh(self, endpoint, jq):
        cmd = ["gh", "api", endpoint, "--jq", jq]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, errors="replace",
                                  timeout=self.gh_timeout)
        except FileNotFoundError:
            return None, "gh not found"
        except subprocess.TimeoutExpired:
            return None, "gh timed out after %ss" % self.gh_timeout
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            return None, one_line(detail, 200) or ("exit %s" % proc.returncode)
        text = (proc.stdout or "").strip()
        if not text:
            return None, "empty response"
        try:
            return json.loads(text), None
        except ValueError:
            return text, None


# ---------------------------------------------------------------------------
# config and cli


def default_config_path():
    here = os.path.dirname(os.path.abspath(__file__))
    for name in ("dots-monitor.owner.json", "dots-monitor.example.json"):
        candidate = os.path.join(here, name)
        if os.path.exists(candidate):
            return candidate
    return os.path.join(here, "dots-monitor.example.json")


def load_config(path):
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, os.O_RDONLY | nofollow | getattr(os, "O_NONBLOCK", 0))
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise SystemExit("config not regular file: %s" % path)
            with os.fdopen(fd, "rb") as handle:
                blob = handle.read(1024 * 1024 + 1)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        if len(blob) > 1024 * 1024:
            raise SystemExit("config too large: %s" % path)
        raw = json.loads(blob.decode("utf-8"))
    except OSError:
        raise SystemExit("config not found: %s" % path) from None
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in raw.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key].update(value)
        else:
            config[key] = value
    raw_stall = (config.get("thresholds") or {}).get("stall_minutes", 30)
    try:
        stall_val = 30 if isinstance(raw_stall, bool) else int(raw_stall)
    except (TypeError, ValueError):
        stall_val = 30
    config["thresholds"]["stall_minutes"] = max(1, stall_val)
    for dot in config["ledger"]["dots"]:
        config["dots"].setdefault(dot, {})
    for index, source in enumerate(config["activity"]):
        if not isinstance(source, dict) or "dot" not in source or "path" not in source:
            raise SystemExit("activity entry %d needs 'dot' and 'path'" % index)
        source.setdefault("host", "local")
    return config


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="dots-monitor",
        description="Read-only view of Dots live records and the tool activity that backs them.")
    parser.add_argument("--config", default=None, help="config JSON (default: next to this file)")
    parser.add_argument("--version", action="version", version="dots-monitor %s" % TOOL_VERSION)
    sub = parser.add_subparsers(dest="command", required=True)

    snap = sub.add_parser("snapshot", help="print one snapshot and exit")
    snap.add_argument("--json", action="store_true", help="machine-readable output")
    snap.add_argument("--no-github", action="store_true", help="do not resolve evidence references")
    snap.add_argument("--daemon-health", action="store_true", help="scan recent daemon sessions for retained-session limits and chat errors")
    snap.add_argument("--stall-minutes", type=int, default=None)

    watch_cmd = sub.add_parser("watch", help="print one line per state change")
    watch_cmd.add_argument("--interval", type=int, default=60, help="seconds between scans")
    watch_cmd.add_argument("--json", action="store_true", help="one JSON object per line")
    watch_cmd.add_argument("--no-github", action="store_true")
    watch_cmd.add_argument("--daemon-health", action="store_true", help="scan recent daemon sessions for retained-session limits and chat errors")
    watch_cmd.add_argument("--stall-minutes", type=int, default=None)

    args = parser.parse_args(argv)
    path = args.config or default_config_path()
    config = load_config(path)
    if args.stall_minutes is not None:
        config["thresholds"]["stall_minutes"] = max(1, int(args.stall_minutes))
    if args.no_github:
        config["github"]["enabled"] = False
    if getattr(args, "daemon_health", False):
        config.setdefault("daemon_health", dict(DEFAULT_CONFIG["daemon_health"]))["enabled"] = True
    transport = Transport(config["limits"]["command_timeout_seconds"],
                          config["limits"]["gh_timeout_seconds"])
    if args.command == "snapshot":
        errors = []
        snapshot = build_snapshot(config, transport, utcnow(), errors)
        print(render_json(snapshot) if args.json else render_table(snapshot))
        return 0 if not snapshot["errors"] else 2
    try:
        watch(config, transport, args.interval, args.json)
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
