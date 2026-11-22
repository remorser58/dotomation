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
never writes to a host it reads: every remote command is ``tail``/``wc``/``ls``
style, every subprocess has a timeout, and log reads are bounded byte tails.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
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

CLASS_ORDER = ("BLOCKED", "UNPROVEN", "DONE", "STALLED", "WORKING", "QUIET", "LEGACY", "NO_RECORD")

DEFAULT_CONFIG = {
    "ledger": {"host": "osaka", "root": "~/CodeProjects/.dots-ops/live", "dots": ["REM17", "REV18"]},
    "dots": {},
    "activity": [],
    "github": {"enabled": True, "repos": [], "cache_seconds": 300, "max_lookups": 60},
    "thresholds": {"stall_minutes": 30, "active_window_minutes": 15, "long_window_minutes": 60},
    "limits": {"tail_bytes": 262144, "command_timeout_seconds": 25, "gh_timeout_seconds": 25},
}

# ---------------------------------------------------------------------------
# redaction


REDACT_RULES = (
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}"), "[REDACTED]"),
    (re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}"), "[REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"), "[REDACTED]"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "[REDACTED]"),
    (
        re.compile(
            r"(?i)\b(bearer|token|secret|password|passwd|api[_-]?key|control_token|"
            r"client_secret|access_token)\b\s*[:=]\s*[\"']?[^\s\"',;]+"
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
    """Collapse a value to one printable, redacted line."""
    if text is None:
        return ""
    out = redact(" ".join(str(text).split()))
    if len(out) > limit:
        out = out[: max(0, limit - 1)] + "\u2026"
    return out


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


def parse_ledger_dump(text):
    """Parse the framed dump produced by ledger_script()."""
    dots = {}
    current = None
    name = None
    size = None
    buf = []
    for line in text.splitlines():
        if line.startswith("==DOT "):
            current = line[6:].strip()
            dots.setdefault(current, {"files": {}, "nodir": False})
            name = None
            continue
        if current is None:
            continue
        if line == "==NODIR":
            dots[current]["nodir"] = True
            continue
        match = re.match(r"^==FILE (.+) ([0-9]+)$", line)
        if match and name is None:
            name = match.group(1)
            size = int(match.group(2))
            buf = []
            continue
        if line == "==END" and name is not None:
            dots[current]["files"][name] = {"size": size, "text": "\n".join(buf)}
            name = None
            buf = []
            continue
        if name is not None:
            buf.append(line)
    return dots


def ledger_script(root, dots, tail_bytes):
    quoted_root = _remote_path(root)
    parts = ["set -u", "export LC_ALL=C", 'root=%s' % quoted_root]
    parts.append("for dot in %s; do" % " ".join(shlex.quote(d) for d in dots))
    parts.append("""  printf '==DOT %s\\n' "$dot" """)
    parts.append('  d="$root/$dot"')
    parts.append('  if [ ! -d "$d" ]; then printf \'==NODIR\\n\'; continue; fi')
    parts.append('  for f in "$d"/*.jsonl; do')
    parts.append('    [ -f "$f" ] || continue')
    parts.append("""    printf '==FILE %s %s\\n' "${f##*/}" "$(wc -c < "$f" | tr -d ' ')" """)
    parts.append('    tail -c %d "$f"' % tail_bytes)
    parts.append("""    printf '\\n==END\\n' """)
    parts.append('  done')
    parts.append('  if [ -f "$d/roster.json" ]; then')
    parts.append("""    printf '==FILE roster.json %s\\n' "$(wc -c < "$d/roster.json" | tr -d ' ')" """)
    parts.append('    tail -c %d "$d/roster.json"' % tail_bytes)
    parts.append("""    printf '\\n==END\\n' """)
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


def collect_ledger(config, transport, errors):
    ledger = config["ledger"]
    dots = ledger["dots"]
    tail_bytes = config["limits"]["tail_bytes"]
    script = ledger_script(ledger["root"], dots, tail_bytes)
    out, error = transport.run(ledger["host"], script)
    if error:
        errors.append({"source": "ledger", "host": ledger["host"], "detail": error})
        return {dot: {"files": {}, "nodir": True, "error": error} for dot in dots}
    dump = parse_ledger_dump(out)
    for dot in dots:
        dump.setdefault(dot, {"files": {}, "nodir": True})
    return dump


def read_dot(dot, dump, tail_bytes):
    """Build per-agent record lists for one dot from a parsed dump."""
    entry = dump.get(dot) or {"files": {}}
    agents = {}
    roster = None
    roster_error = None
    for name, info in entry["files"].items():
        text = split_tail(info["text"], info["size"], tail_bytes)
        if name == "roster.json":
            try:
                roster = json.loads(text)
            except ValueError:
                roster_error = "roster.json is not JSON"
            continue
        agent = name[:-6] if name.endswith(".jsonl") else name
        records = []
        legacy = []
        for raw in text.splitlines():
            if not raw.strip():
                continue
            rec, reason = parse_record_line(raw)
            if rec is None:
                legacy.append({"reason": reason, "line": raw.strip()[:400]})
            else:
                records.append(rec)
        records.sort(key=lambda r: r["_t"])
        agents[agent] = {"records": records, "legacy": legacy}
    return {
        "agents": agents,
        "roster": roster if isinstance(roster, dict) else None,
        "roster_error": roster_error,
        "nodir": entry.get("nodir", False),
        "error": entry.get("error"),
    }


def roster_slots(roster):
    """Accept both observed roster shapes: ``slots`` or ``agents``."""
    slots = {}
    if not isinstance(roster, dict):
        return slots
    for key in ("slots", "agents", "actors"):
        for slot in roster.get(key) or []:
            if isinstance(slot, dict) and isinstance(slot.get("agent"), str):
                merged = dict(slot)
                merged.setdefault("_key", key)
                slots.setdefault(slot["agent"], merged)
    return slots


# ---------------------------------------------------------------------------
# activity


LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)\s+\S+\s+request\s+"
    r"(?P<method>[A-Z]+)\s+(?P<path>\S+)\s+(?:\u2192|->)\s+(?P<status>\d{3})\s+in\s+"
    r"(?P<ms>\d+)ms(?P<rest>.*)$"
)


def activity_script(paths, tail_bytes):
    parts = ["set -u", "export LC_ALL=C", "i=0"]
    parts.append("for p in %s; do" % " ".join(_remote_path(p) for p in paths))
    parts.append("""  printf '==LOG %s\\n' "$i" """)
    parts.append("  i=$((i+1))")
    parts.append('  if [ -f "$p" ]; then')
    parts.append("""    printf '==SIZE %s\\n' "$(wc -c < "$p" | tr -d ' ')" """)
    parts.append('    tail -c %d "$p"' % tail_bytes)
    parts.append("""    printf '\\n==END\\n' """)
    parts.append('  else')
    parts.append("""    printf '==MISSING\\n' """)
    parts.append('  fi')
    parts.append('done')
    return "\n".join(parts)


def parse_activity_dump(text):
    """Parse the framed dump into {index: {"size": int|None, "text": str|None}}."""
    out = {}
    path = None
    size = None
    buf = []
    for line in text.splitlines():
        if line.startswith("==LOG "):
            path = line[6:].strip()
            size = None
            buf = []
            out.setdefault(path, {"size": None, "text": None})
            continue
        if path is None:
            continue
        if line == "==MISSING":
            out[path]["text"] = None
            path = None
            continue
        match = re.match(r"^==SIZE ([0-9]+)$", line)
        if match and size is None:
            size = int(match.group(1))
            out[path]["size"] = size
            continue
        if line == "==END":
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
    """Return {dot: {"by_host": {label: counts}, "since": {label: [timestamps]}}}."""
    windows = {
        "short": timedelta(minutes=config["thresholds"]["active_window_minutes"]),
        "long": timedelta(minutes=config["thresholds"]["long_window_minutes"]),
    }
    by_host = {}
    for src in config["activity"]:
        by_host.setdefault(src["host"], []).append(src)
    per_dot = {}
    for host, sources in by_host.items():
        script = activity_script([s["path"] for s in sources], config["limits"]["tail_bytes"])
        out, error = transport.run(host, script)
        if error:
            errors.append({"source": "activity", "host": host, "detail": error})
            continue
        dump = parse_activity_dump(out)
        for index, src in enumerate(sources):
            info = dump.get(str(index))
            if info is None:
                errors.append({"source": "activity", "host": host,
                               "detail": "no output for %s" % os.path.basename(src["path"])})
                continue
            if info["text"] is None:
                errors.append({"source": "activity", "host": host,
                               "detail": "%s missing" % src["path"]})
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


# ---------------------------------------------------------------------------
# evidence


GH_URL_RE = re.compile(
    r"https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)(/[^\s;,)\]\"']*)?"
)
SHA_RE = re.compile(r"(?<![0-9A-Za-z])[0-9a-f]{40}(?![0-9A-Za-z])")


def evidence_refs(text):
    """Return (endpoint refs, bare commit SHAs) named by an evidence string."""
    if not text:
        return [], []
    refs = []
    for match in GH_URL_RE.finditer(text):
        owner, repo, rest = match.group(1), match.group(2), (match.group(3) or "")
        segs = [s for s in rest.split("/") if s]
        base = "repos/%s/%s" % (owner, repo)
        if len(segs) >= 2 and segs[0] == "pull" and segs[1].isdigit():
            refs.append({"kind": "pull", "endpoint": "%s/pulls/%s" % (base, segs[1]),
                         "label": "%s/%s#%s" % (owner, repo, segs[1]), "repo": "%s/%s" % (owner, repo)})
        elif len(segs) >= 3 and segs[0] == "issues" and segs[1] == "comments" and segs[2].isdigit():
            refs.append({"kind": "comment", "endpoint": "%s/issues/comments/%s" % (base, segs[2]),
                         "label": "%s/%s comment %s" % (owner, repo, segs[2]),
                         "repo": "%s/%s" % (owner, repo)})
        elif len(segs) >= 2 and segs[0] == "issues" and "#" in segs[1] \
                and "issuecomment-" in segs[1]:
            number, anchor = segs[1].split("#", 1)
            comment = anchor.split("issuecomment-", 1)[1]
            refs.append({"kind": "comment",
                         "endpoint": "%s/issues/comments/%s" % (base, comment),
                         "label": "%s/%s comment %s" % (owner, repo, comment),
                         "repo": "%s/%s" % (owner, repo)})
        elif len(segs) >= 2 and segs[0] == "issues" and segs[1].isdigit():
            refs.append({"kind": "issue", "endpoint": "%s/issues/%s" % (base, segs[1]),
                         "label": "%s/%s#%s" % (owner, repo, segs[1]), "repo": "%s/%s" % (owner, repo)})
        elif len(segs) >= 2 and segs[0] == "commit":
            refs.append({"kind": "commit", "endpoint": "%s/commits/%s" % (base, segs[1]),
                         "label": "%s/%s@%s" % (owner, repo, segs[1][:7]), "repo": "%s/%s" % (owner, repo)})
        elif len(segs) >= 2 and segs[0] in ("blob", "tree"):
            refs.append({"kind": "commit", "endpoint": "%s/commits/%s" % (base, segs[1]),
                         "label": "%s/%s@%s" % (owner, repo, segs[1][:7]), "repo": "%s/%s" % (owner, repo)})
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
            data, status, detail = self._lookup(ref["endpoint"], "{id: .id}")
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
    if row["state"] == "unconfirmed":
        return "LEGACY"
    status = row["evidence_status"]
    if row["state"] == "blocked":
        return "BLOCKED"
    if row["state"] == "done":
        if status in ("resolved", "stale", "unknown"):
            return "DONE"
        return "UNPROVEN"
    if status in ("missing", "unresolved"):
        return "UNPROVEN"
    age = row["age_seconds"]
    if age is None or age > stall_minutes * 60:
        return "STALLED"
    return "WORKING" if row["activity_since_record"] > 0 else "QUIET"


def build_snapshot(config, transport, now, errors=None):
    if errors is None:
        errors = []
    stall = config["thresholds"]["stall_minutes"]
    ledger = collect_ledger(config, transport, errors)
    activity = collect_activity(config, transport, now, errors)
    resolver = EvidenceResolver(config, transport, now, errors)
    dots = {}
    for dot in config["ledger"]["dots"]:
        data = read_dot(dot, ledger, config["limits"]["tail_bytes"])
        slots = roster_slots(data["roster"])
        if data["roster_error"]:
            errors.append({"source": "roster", "host": config["ledger"]["host"],
                           "detail": "%s: %s" % (dot, data["roster_error"])})
        dot_activity = activity.get(dot) or {"by_host": {}, "timestamps": []}
        stamps = dot_activity.get("timestamps") or []
        rows = []
        agent_ids = sorted(set(data["agents"]) | set(slots))
        for agent in agent_ids:
            entry = data["agents"].get(agent)
            slot = slots.get(agent) or {}
            records = entry["records"] if entry else []
            legacy = entry["legacy"] if entry else []
            latest = records[-1] if records else None
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
            age = (now - last_t).total_seconds() if last_t else None
            since = 0
            if last_t is not None:
                since = sum(1 for ts in stamps if ts >= last_t)
            status, detail, refs = evidence_status(evidence, resolver)
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
            "agents": rows,
            "unrostered": [r["agent"] for r in rows if not r["in_roster"]],
        }
    return {
        "tool": "dots-monitor",
        "version": TOOL_VERSION,
        "generated_at": iso(now),
        "config_dots": config["ledger"]["dots"],
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
            head, roster.get("coordinator") or "?",
            (roster.get("updated_at") or "?")[:19], roster.get("slots") or 0))
        for label, act in data["activity"].items():
            lines.append("  activity %-6s 15m=%-4d 60m=%-5d last=%s covered_since=%s%s" % (
                label, act["calls_15m"], act["calls_60m"], act["last_call_at"] or "-",
                act["covered_since"] or "-", " (window clipped by log rotation)" if act["partial_window"] else ""))
        if not data["activity"]:
            lines.append("  activity none configured")
        headers = ("AGENT", "CLASS", "STATE", "AGE", "EVID", "ROLE", "TASK")
        rows = []
        for row in data["agents"]:
            rows.append((
                row["agent"], row["classification"], row["state"], row["age"],
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
        unrostered = data.get("unrostered") or []
        if unrostered:
            lines.append("  note: live file without roster slot: %s" % ", ".join(unrostered))
        missing = [r["agent"] for r in data["agents"] if not r["file_present"]]
        if missing:
            lines.append("  note: roster slot without live file: %s" % ", ".join(missing))
    lines.append("")
    lines.append("classes: WORKING record<=%dm and tool calls since it | QUIET record<=%dm, no calls since it | "
                 "STALLED start/step older than %dm | BLOCKED | DONE | UNPROVEN evidence missing/unresolved "
                 "or done on local-only evidence | LEGACY off-format line | NO_RECORD roster slot without file"
                 % (30, 30, 30))
    return "\n".join(lines)


def render_json(snapshot):
    return json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True)


# ---------------------------------------------------------------------------
# watch


def diff_events(previous, snapshot):
    """Return (events, state) where events hold only real changes."""
    events = []
    state = {"agents": {}, "activity": {}, "errors": set()}
    for dot, data in snapshot["dots"].items():
        for row in data["agents"]:
            key = (dot, row["agent"])
            old = previous["agents"].get(key)
            state["agents"][key] = {"classification": row["classification"],
                                    "evidence_status": row["evidence_status"],
                                    "state": row["state"], "task": row["task"]}
            if old is None:
                events.append({"event": "DOTS_NEW_AGENT", "dot": dot, "agent": row["agent"],
                               "detail": {"role": row["role"], "task": row["task"],
                                          "class": row["classification"],
                                          "evidence": row["evidence_status"]}})
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
        print(json.dumps(event, ensure_ascii=False), flush=True)
    else:
        print(format_event(event), flush=True)


def watch(config, transport, interval, as_json, out=sys.stdout):
    previous = {"agents": {}, "activity": {}, "errors": set()}
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
                calls = sum(a["calls_15m"] for a in data["activity"].values())
                emit_event({"event": "DOTS_DOT", "dot": dot, "agent": "-",
                            "detail": {"agents": len(data["agents"]), "calls_15m": calls}},
                           as_json)
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
            cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                   "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2",
                   host, script]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, errors="replace",
                                  timeout=self.timeout)
        except FileNotFoundError:
            return None, "ssh not found"
        except subprocess.TimeoutExpired:
            return None, "timed out after %ss" % self.timeout
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip().splitlines()
            return None, "exit %s: %s" % (proc.returncode, one_line(detail[-1], 160) if detail else "")
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
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in raw.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key].update(value)
        else:
            config[key] = value
    for dot in config["ledger"]["dots"]:
        config["dots"].setdefault(dot, {})
    for source in config["activity"]:
        if "dot" not in source or "path" not in source:
            raise SystemExit("activity entry needs 'dot' and 'path': %r" % (source,))
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
    snap.add_argument("--stall-minutes", type=int, default=None)

    watch_cmd = sub.add_parser("watch", help="print one line per state change")
    watch_cmd.add_argument("--interval", type=int, default=60, help="seconds between scans")
    watch_cmd.add_argument("--json", action="store_true", help="one JSON object per line")
    watch_cmd.add_argument("--no-github", action="store_true")
    watch_cmd.add_argument("--stall-minutes", type=int, default=None)

    args = parser.parse_args(argv)
    path = args.config or default_config_path()
    config = load_config(path)
    if args.stall_minutes:
        config["thresholds"]["stall_minutes"] = args.stall_minutes
    if args.no_github:
        config["github"]["enabled"] = False
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
