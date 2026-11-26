#!/usr/bin/env python3
"""subdots.py: Reverse web-gpt-agent steering bridge and anti-death/anti-spin harness.

Mode 2 (`sub-dots`) of Dotomation:
Instead of Web -> Local Tools (`web-gpt-agent`), `subdots.py` runs the inverted route:
Local AI Session -> Local Reverse Bridge Hub -> Dots Internal Sub-Agent (CLI or Reverse MCP),
without routing messages through the Dots root chat.

Architecture (Python 3.9+ standard library only):
  1. Owner-private Hub daemon (`hub start|status|stop`):
     - SQLite WAL ledger (`<state_dir>/subdots.sqlite`) under `0700` owner directory.
     - Unix control socket (`c.sock`, mode `0600`) for local CLI & `mcp-stdio`.
     - Loopback HTTP + Reverse Streamable-HTTP MCP server (`/<token>/agent/...`, `/<token>/mcp`)
       with Host/Origin/token checks and RFC 9728 metadata.
     - Two-phase steering delivery (`queued -> offered -> acked -> answered`) piggybacked onto
       EVERY sub-agent call, plus priority interrupt propagation.
     - Epoch/session_token fencing against zombie workers.
     - Active lease + connection watchdog (`stalled` -> `dead`, wake backoff, notify escalation).
     - Deterministic spin detector (`SPIN_COMMAND_LOOP`, `SPIN_NO_PROGRESS`, `SPIN_POLL_THRASH`)
       that injects high-priority `HARNESS_INTERVENTION` steering and triggers notify hooks.
     - Optional `.dots-ops/` mirror (`mail-v1`, `live-record-v1`, `memory-v1`) using non-blocking
       regular-file checks so `dots.py board` and `dots_monitor.py` stay compatible.
  2. Local Session Client & Stdio MCP Server:
     - `agents`, `status`, `send`, `steer`, `ask`, `inbox`, `read`, `events`,
       `pause`, `resume`, `cancel`, `release`, `wake`, `mcp-stdio`.
  3. Dots Sub-Agent Harness Client:
     - `agent register|wait|step|reply|exec|ack|heartbeat`
     - `agent exec` supervises the child process group (`start_new_session=True`), watches for
       mid-command interrupts (`steer --interrupt` / `pause` / `cancel`), terminates cleanly,
       and feeds `stderr_hash` + exit code back to the Hub spin detector.
"""
from __future__ import annotations

import sys

# Drop script directory from sys.path[0] when invoked directly so a sibling file
# (e.g. json.py or sqlite3.py) in a writable folder cannot shadow the stdlib.
if __name__ == "__main__" and sys.path:
    import os as _os
    _script_dir = _os.path.dirname(_os.path.abspath(__file__))
    if sys.path[0] == _script_dir:
        del sys.path[0]
    del _os, _script_dir

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import hmac
import http.client
import http.server
import ipaddress
import json
import math
import os
import pathlib
import re
import secrets
import select
import shlex
import signal
import socket
import socketserver
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

VERSION = "1.1.1"
PROTOCOL_VERSION = 1
MCP_PROTOCOL_VERSION = "2024-11-05"

TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
MSG_ID_RE = re.compile(r"^m-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{4}$")
PLACEHOLDER_RE = re.compile(r"\{(project|agent|id|dot|age|task|state|why|hub_url)\}")

MAX_CONTROL_LINE_BYTES = 1024 * 1024
MAX_HTTP_BODY_BYTES = 2 * 1024 * 1024
MAX_TEXT_LEN = 500
MAX_ITEM_LEN = 300
MAX_DECISIONS = 20
MAX_BLOCKERS = 10
MAX_CHUNK_S = 55.0
WAKE_TIMEOUT_S = 25

TO_AGENT_KINDS = (
    "task",
    "steer",
    "interrupt",
    "question",
    "answer",
    "review",
    "pause",
    "resume",
    "cancel",
    "release",
    "note",
    "intervention",
)
TO_LOCAL_KINDS = (
    "ack",
    "answer",
    "question",
    "result",
    "block",
    "step",
    "note",
    "exec_interrupted",
)
NEEDS_AGENT_ANSWER = ("task", "steer", "interrupt", "question", "review", "intervention")
REPLY_WAITS = ("result", "question", "block")
INTERRUPT_KINDS = ("interrupt", "pause", "cancel", "intervention")
LIVE_STATES = ("start", "step", "blocked", "done")
SIGNALS = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)

DEFAULT_SETTINGS: Dict[str, Any] = {
    "lease_ttl_s": 45.0,
    "dead_grace_s": 60.0,
    "watchdog_interval_s": 5.0,
    "reoffer_s": 45.0,
    "max_offers": 5,
    "wake_backoff_s": 30.0,
    "max_wakes": 3,
    "notify_after_s": 120.0,
    "spin_window_s": 600.0,
    "spin_cmd_repeats": 3,
    "spin_step_repeats": 4,
    "spin_poll_burst": 12,
}

_SECRET_KEY_RE = re.compile(r"(?i)(?:token|api[_-]?key|secret|password|passwd|credential|session)")
_ASSIGN_SECRET_RE = re.compile(
    r"""(?i)\b([A-Za-z0-9_-]{1,96})([ \t]*[=:][ \t]*)(?:"([^"\r\n]+)"|'([^'\r\n]+)'|([^\s,;\"']+))"""
)
_CLI_SECRET_FLAG_RE = re.compile(
    r"""(?i)(--(?:session[-_]?token|api[-_]?key|auth[-_]?token|token|secret|password|passwd))([ \t]+)(?:"([^"\r\n]+)"|'([^'\r\n]+)'|([^\s,;\"']+))"""
)
_JSON_SECRET_RE = re.compile(
    r"""(?i)"([A-Za-z0-9_-]{1,96})"([ \t]*:[ \t]*)"([^"\r\n]+)" """.strip()
)
_ANSI_CTRL_RE = re.compile(
    r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]|[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\x9b]"
)

_PEM_BEGIN_RE = re.compile(r"-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----[^\r\n]*")
_PEM_END_RE = re.compile(r"-----END [A-Z0-9 ]{0,40}PRIVATE KEY-----[^\r\n]*")
_PEM_BODY_LINE_RE = re.compile(r"^[ \t]*(?:[A-Za-z0-9+/=]{8,}|[A-Za-z0-9-]+:[ \t][^\r\n]*)[ \t]*$")

REDACT_RULES = (
    (re.compile(r"(https?://[^\s?#/]+/(?:[^\s?#]+/)?)[0-9a-f]{48}(?=/|$|\b)"), r"\1[REDACTED]"),
    (re.compile(r"(/\.well-known/oauth-protected-resource/(?:a/)?)[0-9a-f]{48}(?=/|$|\b)"), r"\1[REDACTED]"),
    (re.compile(r"((?:^|[\s'\"(=:])/(?:a/)?)[0-9a-f]{48}(?=/|$|\b)"), r"\1[REDACTED]"),
    (re.compile(r"\b(gh[pousr]_[A-Za-z0-9_]{12,}|github_pat_[A-Za-z0-9_]{20,})\b"), "[REDACTED]"),
    (re.compile(r"\b(sk-(?:proj-|ant-)?[A-Za-z0-9_-]{12,})\b"), "[REDACTED]"),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "[REDACTED]"),
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), "[REDACTED]"),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"), "[REDACTED]"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\b"), "[REDACTED]"),
    (re.compile(r"(https?://)[^/\s:@]+:[^/\s@]+@"), r"\1[REDACTED]@"),
    (re.compile(r"(?i)\b(authorization:\s*(?:bearer|basic|token|apikey|api-key|aws4-hmac-sha256)\s+)[^\s,;]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)\b((?:bearer|basic|apikey)\s+)[A-Za-z0-9._+/=-]{16,}"), r"\1[REDACTED]"),
    (re.compile(r"(?i)([?&](?:token|key|secret|password|passwd|auth|access_token|api_key)=)[^&\s]+"), r"\1[REDACTED]"),
)


def sanitize_terminal(value: Any) -> str:
    """Strip ANSI/OSC escape sequences and control characters before terminal output."""
    return _ANSI_CTRL_RE.sub("", str(value))


def _redact_pem_keys(value: str) -> str:
    """Linear-time O(N) redaction for complete, oversized, and truncated PEM private key blocks."""
    if "PRIVATE KEY-----" not in value:
        return value
    lines = value.splitlines(keepends=True)
    out: List[str] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.rstrip("\r\n")
        if _PEM_BEGIN_RE.search(stripped):
            end_idx = None
            char_budget = 65536
            j = i + 1
            while j < n and char_budget > 0:
                stripped_j = lines[j].rstrip("\r\n")
                char_budget -= len(lines[j])
                if _PEM_END_RE.search(stripped_j):
                    end_idx = j
                    break
                if _PEM_BEGIN_RE.search(stripped_j):
                    break
                j += 1
            if end_idx is not None:
                eol = "\n" if lines[end_idx].endswith("\n") else ""
                out.append("[REDACTED_PRIVATE_KEY]" + eol)
                i = end_idx + 1
                continue
            # Truncated/unclosed PEM block: redact header + immediately following armor/base64 lines.
            eol = "\n" if line.endswith("\n") else ""
            j = i + 1
            while j < n and _PEM_BODY_LINE_RE.match(lines[j].rstrip("\r\n")):
                eol = "\n" if lines[j].endswith("\n") else eol
                j += 1
            out.append("[REDACTED_PRIVATE_KEY]" + eol)
            i = j
            continue
        if _PEM_END_RE.search(stripped):
            # Front-truncated PEM tail: scrub immediately preceding contiguous base64 lines too.
            while out and _PEM_BODY_LINE_RE.match(out[-1].rstrip("\r\n")):
                out.pop()
            eol = "\n" if line.endswith("\n") else ""
            out.append("[REDACTED_PRIVATE_KEY]" + eol)
            i += 1
            continue
        out.append(line)
        i += 1
    return "".join(out)


def _redact_assign_match(m: re.Match) -> str:
    key, sep, dq, sq, _uq = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
    if not _SECRET_KEY_RE.search(key):
        return m.group(0)
    if dq is not None:
        return f'{key}{sep}"[REDACTED]"'
    if sq is not None:
        return f"{key}{sep}'[REDACTED]'"
    return f"{key}{sep}[REDACTED]"


def _redact_cli_flag_match(m: re.Match) -> str:
    flag, sep, dq, sq, _uq = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
    if dq is not None:
        return f'{flag}{sep}"[REDACTED]"'
    if sq is not None:
        return f"{flag}{sep}'[REDACTED]'"
    return f"{flag}{sep}[REDACTED]"


def _redact_json_match(m: re.Match) -> str:
    key, sep = m.group(1), m.group(2)
    if not _SECRET_KEY_RE.search(key):
        return m.group(0)
    return f'"{key}"{sep}"[REDACTED]"'


def redact(value: str) -> str:
    out = _redact_pem_keys(value)
    for pat, repl in REDACT_RULES:
        out = pat.sub(repl, out)
    out = _ASSIGN_SECRET_RE.sub(_redact_assign_match, out)
    out = _CLI_SECRET_FLAG_RE.sub(_redact_cli_flag_match, out)
    out = _JSON_SECRET_RE.sub(_redact_json_match, out)
    return out


def utc_now_ts() -> float:
    return time.time()


def iso_utc(ts: Optional[float] = None) -> str:
    dt = datetime.datetime.fromtimestamp(utc_now_ts() if ts is None else ts, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def new_msg_id() -> str:
    return f"m-{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(2)}"


def die(msg: str, code: int = 2) -> None:
    print(f"subdots: {msg}", file=sys.stderr)
    raise SystemExit(code)


def validate_token(name: str, value: Any) -> str:
    if not isinstance(value, str) or not TOKEN_RE.fullmatch(value):
        raise ValueError(f"{name} must match {TOKEN_RE.pattern}, got {value!r}")
    return value


def validate_one_line(name: str, value: Any, limit: int = MAX_TEXT_LEN) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{name} must be a single line without NUL bytes")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")
    if len(value) > limit:
        raise ValueError(f"{name} exceeds {limit} chars")
    return value


def validate_body(name: str, value: Any, limit: int = MAX_HTTP_BODY_BYTES) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must not be empty")
    if "\x00" in value:
        raise ValueError(f"{name} must not contain NUL bytes")
    if len(value) > limit:
        raise ValueError(f"{name} exceeds {limit} chars")
    return value


def validate_finite_float(
    name: str,
    value: Any,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    out = float(value)
    if not math.isfinite(out):
        raise ValueError(f"{name} must be a finite number")
    if minimum is not None and out < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and out > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return out


def validate_str_list(
    name: str,
    value: Any,
    max_items: int = 32,
    item_limit: int = MAX_ITEM_LEN,
) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list of strings")
    if len(value) > max_items:
        raise ValueError(f"{name} exceeds {max_items} items")
    out: List[str] = []
    for item in value:
        out.append(validate_one_line(f"{name}[]", item, item_limit))
    return out


def resolve_state_dir(raw: Optional[str] = None) -> str:
    """Resolve state_dir across symlinked parents (e.g. /var -> /private/var) while rejecting leaf symlinks."""
    if raw is None:
        raw = os.environ.get("SUBDOTS_STATE_DIR") or "~/.subdots"
    expanded = os.path.abspath(os.path.expanduser(raw))
    if os.path.islink(expanded):
        raise OSError(f"{expanded} must not be a symlink")
    parent = os.path.dirname(expanded)
    name = os.path.basename(expanded)
    return os.path.join(os.path.realpath(parent), name) if name else os.path.realpath(expanded)


def default_state_dir() -> str:
    raw = os.environ.get("SUBDOTS_STATE_DIR") or "~/.subdots"
    return os.path.abspath(os.path.expanduser(raw))


def ensure_private_dir(path: str) -> str:
    expanded = os.path.abspath(os.path.expanduser(path))
    if os.path.islink(expanded):
        raise OSError(f"{expanded} must not be a symlink")
    canon = resolve_state_dir(expanded)
    os.makedirs(canon, mode=0o700, exist_ok=True)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    o_dir = getattr(os, "O_DIRECTORY", 0)
    fd = os.open(canon, os.O_RDONLY | o_dir | nofollow | os.O_NONBLOCK)
    try:
        st = os.fstat(fd)
        if not stat.S_ISDIR(st.st_mode):
            raise OSError(f"{canon} is not a directory")
        if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
            raise OSError(f"{canon} is not owned by current user")
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o700)
        else:
            os.chmod(canon, 0o700)
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)
    return canon


def write_private_atomic(
    path: str,
    data: str,
    mode: int = 0o600,
    enforce_parent_dir: bool = True,
) -> None:
    abs_path = os.path.abspath(path)
    parent = os.path.dirname(abs_path)
    if enforce_parent_dir:
        ensure_private_dir(parent)
    else:
        os.makedirs(parent, exist_ok=True)
    if os.path.islink(abs_path):
        raise OSError(f"{abs_path} must not be a symlink")
    tmp = f"{abs_path}.tmp.{os.getpid()}.{secrets.token_hex(6)}"
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow | os.O_NONBLOCK
    fd = os.open(tmp, flags, mode)
    opened = False
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            opened = True
            if hasattr(os, "fchmod"):
                os.fchmod(fh.fileno(), mode)
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        if not hasattr(os, "fchmod"):
            os.chmod(tmp, mode)
        if os.path.islink(abs_path):
            raise OSError(f"{abs_path} must not be a symlink")
        os.replace(tmp, abs_path)
    except BaseException:
        if not opened:
            with contextlib.suppress(OSError):
                os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def read_regular_text(path: str, max_bytes: int = MAX_HTTP_BODY_BYTES) -> Optional[str]:
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        raise OSError(f"{path} is not a regular file")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, os.O_RDONLY | nofollow | os.O_NONBLOCK)
    opened = False
    try:
        fst = os.fstat(fd)
        if not stat.S_ISREG(fst.st_mode):
            raise OSError(f"{path} is not a regular file")
        with os.fdopen(fd, "rb") as fh:
            opened = True
            data = fh.read(max_bytes + 1)
    except BaseException:
        if not opened:
            with contextlib.suppress(OSError):
                os.close(fd)
        raise
    if len(data) > max_bytes:
        raise OSError(f"{path} exceeds {max_bytes} bytes")
    return data.decode("utf-8")


@contextlib.contextmanager
def open_ops_dir_nofollow(base_dir: str, rel_parts: Sequence[str]):
    """Create and traverse a relative ops directory chain rejecting symlinks at every segment."""
    try:
        bst = os.lstat(base_dir)
        if stat.S_ISLNK(bst.st_mode) or not stat.S_ISDIR(bst.st_mode):
            raise OSError(f"{base_dir} must be a non-symlink directory")
    except FileNotFoundError:
        os.makedirs(base_dir, mode=0o755, exist_ok=True)
        bst = os.lstat(base_dir)
        if stat.S_ISLNK(bst.st_mode) or not stat.S_ISDIR(bst.st_mode):
            raise OSError(f"{base_dir} must be a non-symlink directory")
        with contextlib.suppress(OSError):
            os.chmod(base_dir, 0o755)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    o_dir = getattr(os, "O_DIRECTORY", 0)
    cur_fd = os.open(base_dir, os.O_RDONLY | o_dir | nofollow | os.O_NONBLOCK)
    try:
        for part in rel_parts:
            if not part or part in (".", "..") or "/" in part or "\\" in part or "\x00" in part:
                raise OSError(f"unsafe ops path component: {part!r}")
            created = False
            try:
                os.mkdir(part, 0o755, dir_fd=cur_fd)
                created = True
            except FileExistsError:
                pass
            next_fd = os.open(part, os.O_RDONLY | o_dir | nofollow | os.O_NONBLOCK, dir_fd=cur_fd)
            os.close(cur_fd)
            cur_fd = next_fd
            if created and hasattr(os, "fchmod"):
                with contextlib.suppress(OSError):
                    os.fchmod(cur_fd, 0o755)
        yield cur_fd
    finally:
        with contextlib.suppress(OSError):
            os.close(cur_fd)


def _flock_nb_with_retry(fh: Any, timeout_s: float = 0.05) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise OSError("ops file lock busy")
            time.sleep(0.005)


def append_regular_jsonl(path: str, obj: Dict[str, Any], base_dir: Optional[str] = None) -> None:
    abs_path = os.path.abspath(path)
    filename = os.path.basename(abs_path)
    parent = os.path.dirname(abs_path)
    if base_dir is not None:
        abs_base = os.path.abspath(base_dir)
        rel_dir = os.path.relpath(parent, abs_base)
        if rel_dir.startswith(".."):
            raise OSError(f"{abs_path} escapes base_dir {abs_base}")
        rel_parts = [] if rel_dir == "." else rel_dir.split(os.sep)
        dir_ctx = open_ops_dir_nofollow(abs_base, rel_parts)
    else:
        dir_ctx = open_ops_dir_nofollow(parent, [])

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    with dir_ctx as dir_fd:
        try:
            st = os.lstat(filename, dir_fd=dir_fd)
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
                raise OSError(f"{abs_path} is not a regular file")
            if st.st_nlink > 1:
                raise OSError(f"{abs_path} must not have hardlinks (nlink={st.st_nlink})")
        except FileNotFoundError:
            pass
        fd = os.open(
            filename,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT | nofollow | os.O_NONBLOCK,
            0o644,
            dir_fd=dir_fd,
        )
        opened = False
        try:
            fst = os.fstat(fd)
            if not stat.S_ISREG(fst.st_mode):
                raise OSError(f"{abs_path} is not a regular file")
            if fst.st_nlink != 1:
                raise OSError(f"{abs_path} must not have hardlinks (nlink={fst.st_nlink})")
            if hasattr(os, "geteuid") and fst.st_uid != os.geteuid():
                raise OSError(f"{abs_path} owner mismatch")
            if hasattr(os, "fchmod"):
                with contextlib.suppress(OSError):
                    os.fchmod(fd, 0o644)
            with os.fdopen(fd, "ab") as fh:
                opened = True
                _flock_nb_with_retry(fh)
                fh.write(line)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            if not opened:
                with contextlib.suppress(OSError):
                    os.close(fd)
            raise


def write_ops_atomic(path: str, data: str, base_dir: Optional[str] = None) -> None:
    abs_path = os.path.abspath(path)
    filename = os.path.basename(abs_path)
    parent = os.path.dirname(abs_path)
    if base_dir is not None:
        abs_base = os.path.abspath(base_dir)
        rel_dir = os.path.relpath(parent, abs_base)
        if rel_dir.startswith(".."):
            raise OSError(f"{abs_path} escapes base_dir {abs_base}")
        rel_parts = [] if rel_dir == "." else rel_dir.split(os.sep)
        dir_ctx = open_ops_dir_nofollow(abs_base, rel_parts)
    else:
        dir_ctx = open_ops_dir_nofollow(parent, [])

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    tmp_name = f".{filename}.tmp.{os.getpid()}.{secrets.token_hex(6)}"
    with dir_ctx as dir_fd:
        try:
            st = os.lstat(filename, dir_fd=dir_fd)
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
                raise OSError(f"{abs_path} is not a regular file")
        except FileNotFoundError:
            pass
        fd = os.open(
            tmp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow | os.O_NONBLOCK,
            0o644,
            dir_fd=dir_fd,
        )
        opened = False
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                opened = True
                if hasattr(os, "fchmod"):
                    with contextlib.suppress(OSError):
                        os.fchmod(fh.fileno(), 0o644)
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            try:
                st = os.lstat(filename, dir_fd=dir_fd)
                if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
                    raise OSError(f"{abs_path} is not a regular file")
            except FileNotFoundError:
                pass
            os.replace(tmp_name, filename, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except BaseException:
            if not opened:
                with contextlib.suppress(OSError):
                    os.close(fd)
            with contextlib.suppress(OSError):
                os.unlink(tmp_name, dir_fd=dir_fd)
            raise


def socket_path_for(state_dir: str) -> Tuple[str, Optional[str]]:
    """Return (socket_path, temp_dir_to_cleanup) respecting macOS 104-byte sun_path."""
    real = os.path.realpath(state_dir)
    candidate = os.path.join(real, "c.sock")
    if len(candidate.encode("utf-8")) <= 96:
        return candidate, None
    digest = hashlib.sha256(real.encode("utf-8")).hexdigest()[:8]
    sock_dir = tempfile.mkdtemp(prefix=f"sd-{digest}-")
    sock_path = os.path.join(sock_dir, "c.sock")
    if len(sock_path.encode("utf-8")) > 96:
        with contextlib.suppress(OSError):
            os.rmdir(sock_dir)
        sock_dir = tempfile.mkdtemp(prefix=f"sd-{digest}-", dir="/tmp")
        sock_path = os.path.join(sock_dir, "c.sock")
    os.chmod(sock_dir, 0o700)
    return sock_path, sock_dir


def runtime_path_for(state_dir: str) -> str:
    return os.path.join(state_dir, "runtime.json")


def read_runtime(state_dir: str) -> Dict[str, Any]:
    canon_dir = resolve_state_dir(state_dir)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    o_dir = getattr(os, "O_DIRECTORY", 0)
    dir_fd = os.open(canon_dir, os.O_RDONLY | o_dir | nofollow | os.O_NONBLOCK)
    try:
        dst = os.fstat(dir_fd)
        if not stat.S_ISDIR(dst.st_mode):
            raise OSError(f"{canon_dir} is not a directory")
        if hasattr(os, "geteuid") and dst.st_uid != os.geteuid():
            raise OSError(f"{canon_dir} owner mismatch")
        if (dst.st_mode & 0o077) != 0:
            raise OSError(f"{canon_dir} permissions too open")
        fd = os.open("runtime.json", os.O_RDONLY | nofollow | os.O_NONBLOCK, dir_fd=dir_fd)
    finally:
        with contextlib.suppress(OSError):
            os.close(dir_fd)
    path = runtime_path_for(canon_dir)
    opened = False
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise OSError(f"{path} is not a regular file")
        if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
            raise OSError(f"{path} owner mismatch")
        if (st.st_mode & 0o077) != 0:
            raise OSError(f"{path} permissions too open")
        with os.fdopen(fd, "rb") as fh:
            opened = True
            raw_bytes = fh.read(65537)
    except BaseException:
        if not opened:
            with contextlib.suppress(OSError):
                os.close(fd)
        raise
    if len(raw_bytes) > 65536:
        raise OSError(f"{path} exceeds 65536 bytes")
    try:
        doc = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"invalid runtime.json: {exc}") from exc
    if not isinstance(doc, dict) or doc.get("v") != 1:
        raise ValueError("invalid runtime.json")
    pid = doc.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError("invalid pid in runtime.json")
    sock = doc.get("socket_path")
    if isinstance(sock, str) and sock:
        try:
            sst = os.lstat(sock)
        except FileNotFoundError:
            sst = None
        if sst is not None:
            if stat.S_ISLNK(sst.st_mode) or not stat.S_ISSOCK(sst.st_mode):
                raise OSError(f"control socket {sock} is invalid")
            if hasattr(os, "geteuid") and sst.st_uid != os.geteuid():
                raise OSError(f"control socket {sock} owner mismatch")
            if (sst.st_mode & 0o077) != 0:
                raise OSError(f"control socket {sock} permissions too open")
    return doc


def pid_alive(pid: int) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def normalize_text_sig(value: str) -> str:
    return " ".join(value.strip().lower().split())


def hash_payload(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_cmd_argv(spec: Optional[str], flag: str) -> Optional[List[str]]:
    if spec is None:
        return None
    try:
        argv = json.loads(spec)
    except ValueError as exc:
        raise ValueError(f"{flag} must be a JSON array of strings: {exc}") from exc
    if not isinstance(argv, list) or not argv or len(argv) > 32:
        raise ValueError(f"{flag} must be a non-empty list of <=32 strings")
    for item in argv:
        if not isinstance(item, str) or not item.strip() or "\x00" in item or "\n" in item or "\r" in item:
            raise ValueError(f"{flag} items must be non-empty single-line strings without NUL bytes")
    return argv


def _sanitize_argv_value(key: str, raw: Any) -> str:
    text = sanitize_terminal(
        str(raw).replace("\x00", "").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    )
    text = re.sub(r" +", " ", text).strip()
    if key in ("task", "why"):
        text = redact(text)
        text = re.sub(r"[^A-Za-z0-9._:/@+,=\s-]+", "_", text)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > 200:
            text = text[:197] + "..."
        if text in ("", "-"):
            return "-"
        while text.startswith("-"):
            text = text.lstrip("-").strip()
        return text or "-"
    return text


def render_argv(argv: Sequence[str], values: Dict[str, Any]) -> Optional[List[str]]:
    for key in set(PLACEHOLDER_RE.findall(" ".join(argv))):
        val = values.get(key)
        if val is None or val == "":
            return None
    return [PLACEHOLDER_RE.sub(lambda m: _sanitize_argv_value(m.group(1), values[m.group(1)]), part) for part in argv]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class HubError(Exception):
    def __init__(self, code: str, message: str, status: int = 400, extra: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.extra = extra or {}


# ---------------------------------------------------------------------------
# Optional .dots-ops Mirror
# ---------------------------------------------------------------------------


_RESERVED_OPS_NAMES = ("live", "memory", "listen", "watch")


class OpsMirror:
    """Best-effort, non-blocking mirror to .dots-ops/ for dots.py/dots_monitor.py compatibility."""

    def __init__(self, ops_base: Optional[str], scrubber: Optional[Callable[[str], str]] = None) -> None:
        self.ops_base = os.path.abspath(os.path.expanduser(ops_base)) if ops_base else None
        self._scrub: Callable[[str], str] = scrubber or redact
        self._cfg_stat: Optional[Tuple[int, int]] = None
        self._cfg_projects: Dict[str, str] = {}
        self._cfg_default_main: Optional[str] = None

    @property
    def enabled(self) -> bool:
        return bool(self.ops_base)

    def _resolve_main_name(self, project: str) -> str:
        fallback = f"{project}-main" if project else "main"
        if not self.ops_base:
            return fallback
        try:
            with open_ops_dir_nofollow(self.ops_base, []) as dir_fd:
                nofollow = getattr(os, "O_NOFOLLOW", 0)
                fd = os.open("dots.json", os.O_RDONLY | nofollow | os.O_NONBLOCK, dir_fd=dir_fd)
                opened = False
                try:
                    fst = os.fstat(fd)
                    if not stat.S_ISREG(fst.st_mode) or fst.st_size > 65536:
                        raise OSError("dots.json invalid size or type")
                    sig = (int(getattr(fst, "st_mtime_ns", int(fst.st_mtime * 1e9))), int(fst.st_size))
                    if self._cfg_stat != sig:
                        with os.fdopen(fd, "rb") as fh:
                            opened = True
                            raw_bytes = fh.read(65537)
                        if len(raw_bytes) > 65536:
                            raise OSError("dots.json exceeds 65536 bytes")
                        cfg = json.loads(raw_bytes.decode("utf-8"))
                        proj_map: Dict[str, str] = {}
                        def_main: Optional[str] = None
                        if isinstance(cfg, dict):
                            for entry in cfg.get("projects") or []:
                                if isinstance(entry, dict):
                                    pid = entry.get("id")
                                    pmain = entry.get("main")
                                    if (
                                        isinstance(pid, str)
                                        and isinstance(pmain, str)
                                        and TOKEN_RE.fullmatch(pid)
                                        and TOKEN_RE.fullmatch(pmain)
                                    ):
                                        proj_map[pid] = pmain
                            cand = cfg.get("main")
                            if isinstance(cand, str) and TOKEN_RE.fullmatch(cand):
                                def_main = cand
                        self._cfg_projects = proj_map
                        self._cfg_default_main = def_main
                        self._cfg_stat = sig
                finally:
                    if not opened:
                        with contextlib.suppress(OSError):
                            os.close(fd)
        except (OSError, UnicodeDecodeError, ValueError, RecursionError):
            pass
        if project in self._cfg_projects:
            return self._cfg_projects[project]
        if self._cfg_default_main:
            return self._cfg_default_main
        return fallback

    def mirror_mail(self, direction: str, msg: Dict[str, Any], dot_main: Optional[str] = None, dot: Optional[str] = None) -> Optional[str]:
        if not self.ops_base:
            return None
        try:
            project = validate_token("project", msg.get("project") or "default")
            if project in _RESERVED_OPS_NAMES:
                return f"mirror_mail skipped: reserved project name {project!r}"
            agent = validate_token("agent_id", msg["agent_id"])
            if dot:
                validate_token("dot", dot)
        except ValueError as exc:
            return f"mirror_mail skipped: {exc}"
        box = os.path.join(self.ops_base, project, "mail", agent)
        filename = "in.jsonl" if direction == "to_agent" else "out.jsonl"
        resolved_main = dot_main or self._resolve_main_name(project)
        sender = resolved_main if direction == "to_agent" else agent
        recipient = agent if direction == "to_agent" else resolved_main
        kind = msg["kind"]
        if direction == "to_agent" and kind not in ("task", "question", "answer", "review", "note"):
            kind = "note"
        if direction == "to_local" and kind not in ("ack", "answer", "question", "result", "block", "note"):
            kind = "note"
        line = {
            "v": 1,
            "id": msg["id"],
            "t": msg["created_at_iso"],
            "from": sender,
            "to": recipient,
            "task": self._scrub(str(msg["task"])),
            "kind": kind,
            "re": msg.get("re_id"),
            "text": self._scrub(str(msg["text"])),
            "refs": [self._scrub(str(r)) for r in (msg.get("refs") or [])],
        }
        try:
            append_regular_jsonl(os.path.join(box, filename), line, base_dir=self.ops_base)
            return None
        except OSError as exc:
            return f"mirror_mail {filename}: {self._scrub(str(exc))}"

    def mirror_step(
        self,
        agent: Dict[str, Any],
        step_data: Dict[str, Any],
        decisions: List[Dict[str, str]],
        blockers: List[str],
    ) -> Optional[str]:
        if not self.ops_base:
            return None
        try:
            dot = validate_token("dot", agent.get("dot") or "DOT")
            agent_id = validate_token("agent_id", agent["agent_id"])
            project = validate_token("project", agent.get("project") or "default")
        except ValueError as exc:
            return f"mirror_step skipped: {exc}"
        live_path = os.path.join(self.ops_base, "live", dot, f"{agent_id}.jsonl")
        mem_path = os.path.join(self.ops_base, "memory", dot, f"{agent_id}.json")
        ts_iso = step_data["t"]
        live_line = {
            "t": ts_iso,
            "agent": agent_id,
            "role": self._scrub(str(agent.get("role") or "구현")),
            "model": self._scrub(str(agent.get("model") or "requested gpt-6.2-astra xhigh; runtime does not display effective")),
            "project": project,
            "task": self._scrub(str(step_data["task"])),
            "state": step_data["state"],
            "host": self._scrub(str(step_data["host"])),
            "note": self._scrub(str(step_data["note"])),
            "evidence": self._scrub(str(step_data["evidence"])),
        }
        redacted_decisions = [
            {"t": str(d.get("t") or ts_iso), "text": self._scrub(str(d.get("text", "")))}
            for d in decisions[-MAX_DECISIONS:]
        ]
        redacted_blockers = [self._scrub(str(b)) for b in blockers[:MAX_BLOCKERS]]
        mem_doc = {
            "v": 1,
            "t": ts_iso,
            "dot": dot,
            "agent": agent_id,
            "project": project,
            "task": self._scrub(str(step_data["task"])),
            "state": step_data["state"],
            "host": self._scrub(str(step_data["host"])),
            "goal": self._scrub(str(step_data["goal"])),
            "doing": self._scrub(str(step_data["doing"])),
            "next": self._scrub(str(step_data["next_step"])),
            "decisions": redacted_decisions,
            "blockers": redacted_blockers,
        }
        try:
            append_regular_jsonl(live_path, live_line, base_dir=self.ops_base)
            write_ops_atomic(mem_path, json.dumps(mem_doc, ensure_ascii=False, indent=1) + "\n", base_dir=self.ops_base)
            return None
        except OSError as exc:
            return f"mirror_step: {self._scrub(str(exc))}"


# ---------------------------------------------------------------------------
# SQLite WAL Ledger
# ---------------------------------------------------------------------------


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agents (
    agent_id TEXT PRIMARY KEY,
    dot TEXT NOT NULL DEFAULT '',
    project TEXT NOT NULL DEFAULT 'default',
    role TEXT NOT NULL DEFAULT '구현',
    host TEXT NOT NULL DEFAULT 'local',
    model TEXT NOT NULL DEFAULT 'requested gpt-6.2-astra xhigh; runtime does not display effective',
    epoch INTEGER NOT NULL DEFAULT 1,
    session_token TEXT NOT NULL,
    agent_token TEXT,
    state TEXT NOT NULL DEFAULT 'registered',
    task TEXT NOT NULL DEFAULT '',
    goal TEXT NOT NULL DEFAULT '',
    doing TEXT NOT NULL DEFAULT '',
    next_step TEXT NOT NULL DEFAULT '',
    decisions_json TEXT NOT NULL DEFAULT '[]',
    blockers_json TEXT NOT NULL DEFAULT '[]',
    lease_ttl_s REAL NOT NULL DEFAULT 45.0,
    last_seen_at REAL NOT NULL,
    lease_expires_at REAL NOT NULL,
    active_poll_count INTEGER NOT NULL DEFAULT 0,
    poll_timestamps_json TEXT NOT NULL DEFAULT '[]',
    recent_steps_json TEXT NOT NULL DEFAULT '[]',
    recent_execs_json TEXT NOT NULL DEFAULT '[]',
    spin_strikes INTEGER NOT NULL DEFAULT 0,
    spin_reason TEXT NOT NULL DEFAULT '',
    wake_count INTEGER NOT NULL DEFAULT 0,
    last_wake_at REAL,
    stalled_since REAL,
    notified_dead_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    payload_hash TEXT NOT NULL,
    direction TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    project TEXT NOT NULL,
    task TEXT NOT NULL,
    kind TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    interrupt INTEGER NOT NULL DEFAULT 0,
    re_id TEXT,
    text TEXT NOT NULL,
    refs_json TEXT NOT NULL DEFAULT '[]',
    delivery_state TEXT NOT NULL,
    offer_count INTEGER NOT NULL DEFAULT 0,
    first_offered_at REAL,
    last_offered_at REAL,
    acked_at REAL,
    answered_at REAL,
    created_at REAL NOT NULL,
    created_at_iso TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_agent_dir_state
    ON messages(agent_id, direction, delivery_state, priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_messages_dir_state_agent
    ON messages(direction, delivery_state, agent_id);
CREATE INDEX IF NOT EXISTS idx_messages_agent_dir_re
    ON messages(agent_id, direction, re_id);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    ts_iso TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_agent_seq
    ON events(agent_id, seq);

CREATE TABLE IF NOT EXISTS exec_runs (
    exec_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    cmd TEXT NOT NULL,
    cmd_norm TEXT NOT NULL,
    cwd TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_code INTEGER,
    stderr_hash TEXT NOT NULL DEFAULT '',
    output_tail TEXT NOT NULL DEFAULT '',
    started_at REAL NOT NULL,
    ended_at REAL,
    interrupted_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_exec_runs_agent_started
    ON exec_runs(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_exec_runs_status
    ON exec_runs(status);
"""

_MAX_EVENTS_RETENTION = 10000


def _verify_sqlite_file_safe(path: str) -> None:
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        raise OSError(f"{path} must be a regular file")
    if st.st_nlink > 1:
        raise OSError(f"{path} must not have hardlinks")
    if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
        raise OSError(f"{path} owner mismatch")


class Ledger:
    def __init__(self, db_path: str, scrubber: Optional[Callable[[str], str]] = None) -> None:
        self.db_path = db_path
        self._scrubber: Callable[[str], str] = scrubber or redact
        self._event_inserts = 0
        self._lock = threading.RLock()
        for suffix in ("", "-wal", "-shm"):
            _verify_sqlite_file_safe(db_path + suffix)
        self._conn = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.execute("PRAGMA busy_timeout=5000;")
            self._conn.executescript(SCHEMA_SQL)
            cols = {r[1] for r in self._conn.execute("PRAGMA table_info(agents)").fetchall()}
            if "agent_token" not in cols:
                self._conn.execute("ALTER TABLE agents ADD COLUMN agent_token TEXT")
            self._conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_token ON agents(agent_token) "
                "WHERE agent_token IS NOT NULL"
            )
            self._conn.commit()
        self.ensure_private_files()

    def ensure_private_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            p = self.db_path + suffix
            try:
                st = os.lstat(p)
            except FileNotFoundError:
                continue
            if not stat.S_ISLNK(st.st_mode) and stat.S_ISREG(st.st_mode) and (st.st_mode & 0o077) != 0:
                with contextlib.suppress(OSError):
                    os.chmod(p, 0o600)

    def _scrub_value(self, obj: Any) -> Any:
        if isinstance(obj, str):
            return self._scrubber(obj)
        if isinstance(obj, list):
            return [self._scrub_value(x) for x in obj]
        if isinstance(obj, dict):
            return {str(k): self._scrub_value(v) for k, v in obj.items()}
        return obj

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextlib.contextmanager
    def tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    @staticmethod
    def _row_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        return dict(row)

    def emit_event_locked(
        self,
        conn: sqlite3.Connection,
        agent_id: str,
        event_type: str,
        payload: Dict[str, Any],
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = utc_now_ts() if now_ts is None else now_ts
        ts_iso = iso_utc(ts)
        safe_payload = self._scrub_value(payload)
        cur = conn.execute(
            "INSERT INTO events(ts, ts_iso, agent_id, event_type, payload_json) VALUES (?, ?, ?, ?, ?)",
            (ts, ts_iso, agent_id, event_type, json.dumps(safe_payload, ensure_ascii=False)),
        )
        self._event_inserts += 1
        if self._event_inserts % 250 == 0:
            cutoff = conn.execute(
                "SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?",
                (_MAX_EVENTS_RETENTION,),
            ).fetchone()
            if cutoff is not None:
                conn.execute("DELETE FROM events WHERE seq <= ?", (cutoff[0],))
        return {
            "seq": cur.lastrowid,
            "ts": ts,
            "ts_iso": ts_iso,
            "agent_id": agent_id,
            "event_type": event_type,
            "payload": safe_payload,
        }


# ---------------------------------------------------------------------------
# WakeHub & Core Hub Engine
# ---------------------------------------------------------------------------


_VOLATILE_OUTPUT_PATTERNS = (
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?\b"), "<TS>"),
    (re.compile(r"\b\d{1,2}:\d{2}:\d{2}(?:\.\d+)?\b"), "<TIME>"),
    (re.compile(r"\b\d+(?:\.\d+)?(?:ms|us|ns|s|m)\b"), "<DUR>"),
    (re.compile(r"\b0x[0-9a-fA-F]{4,}\b"), "<HEX>"),
    (re.compile(r"(?i)\bpid[=:\s]+\d+\b"), "pid=<PID>"),
    (re.compile(r"/(?:tmp|var/folders)/[^\s'\"]+"), "<TMP>"),
)


def normalize_spin_output(value: str) -> str:
    out = value
    for pat, repl in _VOLATILE_OUTPUT_PATTERNS:
        out = pat.sub(repl, out)
    return normalize_text_sig(out)


class WakeHub:
    """Generation-tracked condition signals preventing lost wakeups for polls and event followers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._agent_conds: Dict[str, threading.Condition] = {}
        self._agent_gen: Dict[str, int] = {}
        self.events_cond = threading.Condition(self._lock)
        self.events_gen = 0
        self.stop_event = threading.Event()

    def cond_for(self, agent_id: str) -> threading.Condition:
        with self._lock:
            cond = self._agent_conds.get(agent_id)
            if cond is None:
                cond = threading.Condition(self._lock)
                self._agent_conds[agent_id] = cond
            return cond

    def agent_gen(self, agent_id: str) -> int:
        with self._lock:
            return self._agent_gen.get(agent_id, 0)

    def current_events_gen(self) -> int:
        with self._lock:
            return self.events_gen

    def wait_agent(self, agent_id: str, observed_gen: int, timeout_s: float) -> None:
        if timeout_s <= 0.0:
            return
        with self._lock:
            if self.stop_event.is_set() or self._agent_gen.get(agent_id, 0) != observed_gen:
                return
            cond = self._agent_conds.get(agent_id)
            if cond is None:
                cond = threading.Condition(self._lock)
                self._agent_conds[agent_id] = cond
            cond.wait(timeout=timeout_s)

    def wait_events(self, observed_gen: int, timeout_s: float) -> None:
        if timeout_s <= 0.0:
            return
        with self._lock:
            if self.stop_event.is_set() or self.events_gen != observed_gen:
                return
            self.events_cond.wait(timeout=timeout_s)

    def wake_agent(self, agent_id: str) -> None:
        with self._lock:
            self._agent_gen[agent_id] = self._agent_gen.get(agent_id, 0) + 1
            cond = self._agent_conds.get(agent_id)
            if cond is not None:
                cond.notify_all()

    def wake_events(self) -> None:
        with self._lock:
            self.events_gen += 1
            self.events_cond.notify_all()

    def stop_all(self) -> None:
        self.stop_event.set()
        with self._lock:
            self.events_gen += 1
            for aid in list(self._agent_conds.keys()):
                self._agent_gen[aid] = self._agent_gen.get(aid, 0) + 1
            for cond in self._agent_conds.values():
                cond.notify_all()
            self.events_cond.notify_all()


class HubCore:
    def __init__(
        self,
        state_dir: str,
        settings: Optional[Dict[str, Any]] = None,
        ops_base: Optional[str] = None,
        wake_cmd: Optional[List[str]] = None,
        notify_cmd: Optional[List[str]] = None,
        public_host: Optional[str] = None,
        require_provision: bool = False,
    ) -> None:
        self.state_dir = ensure_private_dir(state_dir)
        self.settings = dict(DEFAULT_SETTINGS)
        if settings:
            self.settings.update(settings)
        self.require_provision = bool(require_provision)
        self._secret_lock = threading.Lock()
        self._secret_tokens: Set[str] = set()
        self.ledger = Ledger(os.path.join(self.state_dir, "subdots.sqlite"), scrubber=self._scrub_hook_text)
        self.wake_hub = WakeHub()
        self.mirror = OpsMirror(ops_base, scrubber=self._scrub_hook_text)
        self.wake_cmd = wake_cmd
        self.notify_cmd = notify_cmd
        self.public_host = public_host
        self.http_url = ""
        self.mcp_url = ""
        self._hub_token = ""
        self.started_at = iso_utc()
        self._action_pool: List[threading.Thread] = []
        self._action_lock = threading.Lock()
        self._action_sem = threading.Semaphore(8)

        # Reconcile stale active_poll_count / running execs after an unclean hub restart
        with self.ledger.tx() as conn:
            conn.execute("UPDATE agents SET active_poll_count = 0 WHERE active_poll_count != 0")
            conn.execute(
                "UPDATE exec_runs SET status = 'failed', ended_at = ? WHERE status = 'running'",
                (utc_now_ts(),),
            )
            for row in conn.execute("SELECT session_token, agent_token FROM agents").fetchall():
                self._remember_secret(row["session_token"])
                self._remember_secret(row["agent_token"])
        self.ledger.ensure_private_files()

    def _remember_secret(self, tok: Optional[str]) -> None:
        if tok and len(tok) >= 16:
            with self._secret_lock:
                self._secret_tokens.add(tok)

    def close(self) -> None:
        self.wake_hub.stop_all()
        with self._action_lock:
            threads = list(self._action_pool)
        for th in threads:
            th.join(timeout=2.0)
        self.ledger.close()

    def _scrub_hook_text(self, value: str) -> str:
        out = redact(value)
        with self._secret_lock:
            secrets_snap = list(self._secret_tokens)
        if self._hub_token and self._hub_token not in secrets_snap:
            secrets_snap.append(self._hub_token)
        for tok in secrets_snap:
            if tok in out:
                out = out.replace(tok, "[REDACTED]")
        return out

    def _spawn_cmd_hook(self, argv: List[str], values: Dict[str, Any], event_type: str, agent_id: str) -> None:
        if self.wake_hub.stop_event.is_set():
            return
        rendered = render_argv(argv, values)
        if not rendered:
            with contextlib.suppress(sqlite3.Error, ValueError, RuntimeError):
                with self.ledger.tx() as conn:
                    self.ledger.emit_event_locked(
                        conn,
                        agent_id,
                        f"{event_type}_skipped",
                        {"reason": "missing_placeholder", "argv": [self._scrub_hook_text(a) for a in argv]},
                    )
                self.wake_hub.wake_events()
            return
        if not self._action_sem.acquire(blocking=False):
            with contextlib.suppress(sqlite3.Error, ValueError, RuntimeError):
                with self.ledger.tx() as conn:
                    self.ledger.emit_event_locked(
                        conn,
                        agent_id,
                        f"{event_type}_skipped",
                        {"reason": "hook_concurrency_limit", "argv": [self._scrub_hook_text(a) for a in rendered]},
                    )
                self.wake_hub.wake_events()
            return

        def _runner() -> None:
            rc: Any = None
            err = ""
            try:
                proc: Optional[subprocess.Popen] = None
                try:
                    proc = subprocess.Popen(
                        rendered,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )
                    rc = proc.wait(timeout=WAKE_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    rc = "timeout"
                    if proc is not None and proc.poll() is None:
                        with contextlib.suppress(ProcessLookupError, OSError):
                            os.killpg(proc.pid, signal.SIGKILL)
                        with contextlib.suppress(subprocess.SubprocessError):
                            proc.wait(timeout=2.0)
                except OSError as exc:
                    rc = "error"
                    err = str(exc)
                if not self.wake_hub.stop_event.is_set():
                    with contextlib.suppress(sqlite3.Error, ValueError, RuntimeError):
                        safe_argv = [self._scrub_hook_text(p) for p in rendered]
                        with self.ledger.tx() as conn:
                            self.ledger.emit_event_locked(
                                conn,
                                agent_id,
                                event_type,
                                {"argv": safe_argv, "exit": rc, "error": self._scrub_hook_text(err)}
                                if err
                                else {"argv": safe_argv, "exit": rc},
                            )
                        self.wake_hub.wake_events()
            finally:
                self._action_sem.release()

        th = threading.Thread(target=_runner, daemon=True)
        with self._action_lock:
            self._action_pool = [t for t in self._action_pool if t.is_alive()]
            self._action_pool.append(th)
        th.start()

    def _priority_for_kind(self, kind: str, interrupt: bool) -> int:
        if interrupt or kind in INTERRUPT_KINDS:
            return 10
        if kind in ("steer", "release"):
            return 5
        return 0

    def _format_agent_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)
        out.pop("session_token", None)
        out.pop("agent_token", None)
        for jkey, outkey in (
            ("decisions_json", "decisions"),
            ("blockers_json", "blockers"),
        ):
            try:
                out[outkey] = json.loads(out.pop(jkey, "[]"))
            except ValueError:
                out[outkey] = []
        out.pop("poll_timestamps_json", None)
        out.pop("recent_steps_json", None)
        out.pop("recent_execs_json", None)
        now_ts = utc_now_ts()
        out["lease_remaining_s"] = round(max(0.0, float(out["lease_expires_at"]) - now_ts), 2)
        out["last_seen_age_s"] = round(max(0.0, now_ts - float(out["last_seen_at"])), 2)
        return out

    def _format_msg_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)
        try:
            out["refs"] = json.loads(out.pop("refs_json", "[]"))
        except ValueError:
            out["refs"] = []
        out["interrupt"] = bool(out.get("interrupt"))
        return out

    def _agent_exists(self, conn: sqlite3.Connection, agent_id: str) -> Dict[str, Any]:
        row = conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
        if row is None:
            raise HubError("AGENT_NOT_FOUND", f"unknown agent {agent_id!r}", status=404)
        return dict(row)

    def _verify_epoch_locked(
        self,
        conn: sqlite3.Connection,
        agent_id: str,
        epoch: Optional[int],
        session_token: Optional[str],
        require_fencing: bool = True,
    ) -> Dict[str, Any]:
        agent = self._agent_exists(conn, agent_id)
        if require_fencing:
            if epoch is None or isinstance(epoch, bool) or not isinstance(session_token, str) or not session_token:
                raise HubError("MISSING_FENCE", "epoch and session_token are required", status=401)
            try:
                epoch_int = int(epoch)
            except (TypeError, ValueError) as exc:
                raise ValueError("epoch must be an integer") from exc
            if epoch_int != int(agent["epoch"]) or not hmac.compare_digest(
                session_token.encode("utf-8"), str(agent["session_token"]).encode("utf-8")
            ):
                raise HubError(
                    "STALE_EPOCH",
                    f"epoch/token mismatch for {agent_id}: expected epoch={agent['epoch']}, got {epoch}",
                    status=409,
                    extra={"current_epoch": agent["epoch"]},
                )
        return agent

    def _recover_agent_liveness_locked(
        self,
        conn: sqlite3.Connection,
        agent: Dict[str, Any],
        via: str,
        now_ts: float,
        override_state: Optional[str] = None,
        recent_steps_json: Optional[str] = None,
        recent_execs_json: Optional[str] = None,
    ) -> bool:
        agent_id = agent["agent_id"]
        ttl = float(agent["lease_ttl_s"])
        recovered = agent["state"] in ("stalled", "dead", "disconnected")
        new_state = "working" if recovered else (override_state if override_state is not None else agent["state"])
        sets = [
            "state = ?",
            "last_seen_at = ?",
            "lease_expires_at = ?",
            "wake_count = 0",
            "stalled_since = NULL",
            "notified_dead_at = NULL",
            "updated_at = ?",
        ]
        vals: List[Any] = [new_state, now_ts, now_ts + ttl, now_ts]
        if recent_steps_json is not None:
            sets.append("recent_steps_json = ?")
            vals.append(recent_steps_json)
        if recent_execs_json is not None:
            sets.append("recent_execs_json = ?")
            vals.append(recent_execs_json)
        vals.append(agent_id)
        conn.execute(f"UPDATE agents SET {', '.join(sets)} WHERE agent_id = ?", vals)
        if recovered:
            self.ledger.emit_event_locked(conn, agent_id, "agent_recovered", {"via": via}, now_ts)
        return recovered

    def _insert_message_locked(
        self,
        conn: sqlite3.Connection,
        *,
        request_id: Optional[str],
        payload_hash: str,
        direction: str,
        agent_id: str,
        project: str,
        task: str,
        kind: str,
        priority: int,
        interrupt: int,
        re_id: Optional[str],
        text_body: str,
        refs_json: str,
        delivery_state: str,
        created_at: float,
        created_at_iso: str,
    ) -> str:
        for attempt in range(8):
            mid = new_msg_id()
            try:
                conn.execute(
                    "INSERT INTO messages(id, request_id, payload_hash, direction, agent_id, project, task, kind, "
                    "priority, interrupt, re_id, text, refs_json, delivery_state, created_at, created_at_iso) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        mid,
                        request_id,
                        payload_hash,
                        direction,
                        agent_id,
                        project,
                        task,
                        kind,
                        priority,
                        interrupt,
                        re_id,
                        text_body,
                        refs_json,
                        delivery_state,
                        created_at,
                        created_at_iso,
                    ),
                )
                return mid
            except sqlite3.IntegrityError:
                if attempt < 7 and conn.execute("SELECT 1 FROM messages WHERE id = ?", (mid,)).fetchone() is not None:
                    continue
                raise
        raise HubError("INTERNAL_ERROR", "failed to allocate unique message ID", status=500)

    def _apply_acks_locked(self, conn: sqlite3.Connection, agent_id: str, ack_ids: Sequence[str], now_ts: float) -> List[str]:
        acked: List[str] = []
        for mid in ack_ids:
            if not isinstance(mid, str) or not mid:
                continue
            row = conn.execute(
                "SELECT id, kind, delivery_state, created_at, rowid FROM messages "
                "WHERE id = ? AND agent_id = ? AND direction = 'to_agent'",
                (mid, agent_id),
            ).fetchone()
            if row is None:
                continue
            if row["delivery_state"] in ("queued", "offered", "unacked_failed"):
                conn.execute(
                    "UPDATE messages SET delivery_state = 'acked', acked_at = COALESCE(acked_at, ?) WHERE id = ?",
                    (now_ts, mid),
                )
                acked.append(mid)
                self.ledger.emit_event_locked(conn, agent_id, "message_acked", {"id": mid}, now_ts)
                if row["kind"] == "cancel":
                    # Acknowledging a cancel clears earlier unanswered work, leaving newer messages intact.
                    conn.execute(
                        "UPDATE messages SET delivery_state = 'cancelled' "
                        "WHERE agent_id = ? AND direction = 'to_agent' AND id != ? "
                        "AND delivery_state IN ('queued', 'offered', 'acked') "
                        "AND (created_at < ? OR (created_at = ? AND rowid <= ?))",
                        (agent_id, mid, row["created_at"], row["created_at"], row["rowid"]),
                    )
        return acked

    def _collect_piggyback_locked(
        self,
        conn: sqlite3.Connection,
        agent_id: str,
        now_ts: float,
        ack_ids: Sequence[str] = (),
        interrupt_only: bool = False,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Two-phase steering collector: acks supplied IDs, re-offers timed-out unacked rows,
        marks failed after max_offers, and returns newly offered messages + failed notifications."""
        if ack_ids:
            self._apply_acks_locked(conn, agent_id, ack_ids, now_ts)

        reoffer_s = float(self.settings["reoffer_s"])
        max_offers = int(self.settings["max_offers"])
        clause = "AND (interrupt = 1 OR kind IN ('interrupt', 'pause', 'cancel', 'intervention'))" if interrupt_only else ""
        rows = conn.execute(
            f"SELECT * FROM messages WHERE agent_id = ? AND direction = 'to_agent' "
            f"AND delivery_state IN ('queued', 'offered') {clause} "
            f"ORDER BY priority DESC, created_at ASC, rowid ASC",
            (agent_id,),
        ).fetchall()

        deliver: List[Dict[str, Any]] = []
        failed_rows: List[Dict[str, Any]] = []
        for raw in rows:
            m = dict(raw)
            if m["delivery_state"] == "queued":
                deliver.append(m)
            elif m["delivery_state"] == "offered":
                last_off = float(m["last_offered_at"] or 0.0)
                if now_ts - last_off >= reoffer_s:
                    if int(m["offer_count"]) >= max_offers:
                        conn.execute(
                            "UPDATE messages SET delivery_state = 'unacked_failed' WHERE id = ?",
                            (m["id"],),
                        )
                        self.ledger.emit_event_locked(
                            conn,
                            agent_id,
                            "delivery_unacked",
                            {"id": m["id"], "offer_count": m["offer_count"]},
                            now_ts,
                        )
                        failed_rows.append(self._format_msg_row(m))
                    else:
                        deliver.append(m)

        out: List[Dict[str, Any]] = []
        for m in deliver:
            new_count = int(m["offer_count"]) + 1
            conn.execute(
                "UPDATE messages SET delivery_state = 'offered', offer_count = ?, "
                "first_offered_at = COALESCE(first_offered_at, ?), last_offered_at = ? WHERE id = ?",
                (new_count, now_ts, now_ts, m["id"]),
            )
            m["delivery_state"] = "offered"
            m["offer_count"] = new_count
            m["last_offered_at"] = now_ts
            m["is_retry"] = new_count > 1
            self.ledger.emit_event_locked(
                conn,
                agent_id,
                "message_offered",
                {"id": m["id"], "kind": m["kind"], "offer_count": new_count, "interrupt": bool(m["interrupt"])},
                now_ts,
            )
            out.append(self._format_msg_row(m))
        return out, failed_rows

    def _trigger_intervention_locked(
        self,
        conn: sqlite3.Connection,
        agent: Dict[str, Any],
        reason: str,
        detail: str,
        now_ts: float,
    ) -> Tuple[Optional[Dict[str, Any]], int]:
        agent_id = agent["agent_id"]
        strikes = int(agent["spin_strikes"]) + 1
        conn.execute(
            "UPDATE agents SET state = 'spinning', spin_strikes = ?, spin_reason = ?, updated_at = ? "
            "WHERE agent_id = ?",
            (strikes, reason, now_ts, agent_id),
        )
        self.ledger.emit_event_locked(
            conn,
            agent_id,
            "spin_detected",
            {"reason": reason, "detail": detail, "strikes": strikes},
            now_ts,
        )
        existing = conn.execute(
            "SELECT id FROM messages WHERE agent_id = ? AND direction = 'to_agent' "
            "AND kind = 'intervention' AND delivery_state IN ('queued', 'offered') AND task = ?",
            (agent_id, f"SPIN:{reason}"),
        ).fetchone()
        if existing is None:
            body = (
                f"[HARNESS_INTERVENTION:{reason}] {detail}. "
                f"Stop repeating this loop immediately. Report your blocker via `subdots.py agent reply {agent_id} block` "
                f"(or `subdots_reply`) and wait for local-session steering."
            )
            p_hash = hash_payload({"intervention": reason, "strikes": strikes, "agent": agent_id})
            mid = self._insert_message_locked(
                conn,
                request_id=f"spin-{agent_id}-{strikes}-{uuid.uuid4().hex[:8]}",
                payload_hash=p_hash,
                direction="to_agent",
                agent_id=agent_id,
                project=agent.get("project") or "default",
                task=f"SPIN:{reason}",
                kind="intervention",
                priority=10,
                interrupt=1,
                re_id=None,
                text_body=body,
                refs_json="[]",
                delivery_state="queued",
                created_at=now_ts,
                created_at_iso=iso_utc(now_ts),
            )
            msg_dict: Dict[str, Any] = {"id": mid, "reason": reason, "detail": detail, "strikes": strikes, "is_new": True}
        else:
            msg_dict = {"id": existing["id"], "reason": reason, "detail": detail, "strikes": strikes, "is_new": False}
        return msg_dict, strikes

    def lookup_agent_by_token(self, agent_token: str) -> Optional[str]:
        if not agent_token or not re.fullmatch(r"[0-9a-f]{48}", agent_token):
            return None
        with self.ledger._lock:
            row = self.ledger._conn.execute(
                "SELECT agent_id, agent_token FROM agents WHERE agent_token = ?",
                (agent_token,),
            ).fetchone()
            if row is not None and row["agent_token"] and hmac.compare_digest(
                agent_token.encode("utf-8"), str(row["agent_token"]).encode("utf-8")
            ):
                return str(row["agent_id"])
        return None

    def _enforce_agent_scope(self, bound_agent_id: Optional[str], params: Dict[str, Any]) -> None:
        if bound_agent_id is None:
            return
        req_aid = params.get("agent_id")
        if not isinstance(req_aid, str) or not hmac.compare_digest(req_aid.encode("utf-8"), bound_agent_id.encode("utf-8")):
            raise HubError("AGENT_SCOPE_MISMATCH", "token is scoped to a different agent_id", status=403)

    def rollback_offered_messages(self, agent_id: str, messages: Sequence[Dict[str, Any]]) -> None:
        """Roll back messages whose HTTP 200 write failed so a dead poll does not strand steering."""
        if not messages:
            return
        with self.ledger.tx() as conn:
            for m in messages:
                mid = m.get("id")
                off_cnt = int(m.get("offer_count") or 1)
                if not isinstance(mid, str) or not mid:
                    continue
                if off_cnt <= 1:
                    conn.execute(
                        "UPDATE messages SET delivery_state = 'queued', offer_count = 0, "
                        "first_offered_at = NULL, last_offered_at = NULL "
                        "WHERE id = ? AND agent_id = ? AND direction = 'to_agent' "
                        "AND delivery_state = 'offered' AND offer_count = ?",
                        (mid, agent_id, off_cnt),
                    )
                else:
                    conn.execute(
                        "UPDATE messages SET offer_count = ?, last_offered_at = 0.0 "
                        "WHERE id = ? AND agent_id = ? AND direction = 'to_agent' "
                        "AND delivery_state = 'offered' AND offer_count = ?",
                        (off_cnt - 1, mid, agent_id, off_cnt),
                    )
                self.ledger.emit_event_locked(
                    conn,
                    agent_id,
                    "message_offer_rolled_back",
                    {"id": mid, "prev_offer_count": off_cnt},
                )

    def local_provision_agent(self, params: Dict[str, Any]) -> Dict[str, Any]:
        agent_id = validate_token("agent_id", params.get("agent_id"))
        dot = validate_token("dot", params.get("dot") or "DOT")
        project = validate_token("project", params.get("project") or "default")
        role = validate_one_line("role", params.get("role") or "구현", 128)
        host = validate_one_line("host", params.get("host") or socket.gethostname(), 128)
        model = validate_one_line(
            "model",
            params.get("model") or "requested gpt-6.2-astra xhigh; runtime does not display effective",
            256,
        )
        raw_ttl = params.get("lease_ttl_s")
        lease_ttl_s = validate_finite_float(
            "lease_ttl_s",
            self.settings["lease_ttl_s"] if raw_ttl is None else raw_ttl,
            minimum=0.1,
            maximum=3600.0,
        )
        now_ts = utc_now_ts()
        session_token = secrets.token_hex(24)
        agent_token = secrets.token_hex(24)
        self._remember_secret(session_token)
        self._remember_secret(agent_token)

        with self.ledger.tx() as conn:
            prev = conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            if prev is None:
                if conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0] >= 256:
                    raise HubError("TOO_MANY_AGENTS", "maximum agent limit (256) reached", status=429)
                epoch = 1
                conn.execute(
                    "INSERT INTO agents(agent_id, dot, project, role, host, model, epoch, session_token, "
                    "agent_token, state, lease_ttl_s, last_seen_at, lease_expires_at, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'registered', ?, ?, ?, ?, ?)",
                    (
                        agent_id,
                        dot,
                        project,
                        role,
                        host,
                        model,
                        epoch,
                        session_token,
                        agent_token,
                        lease_ttl_s,
                        now_ts,
                        now_ts + lease_ttl_s,
                        now_ts,
                        now_ts,
                    ),
                )
            else:
                epoch = int(prev["epoch"]) + 1
                conn.execute(
                    "UPDATE agents SET dot = ?, project = ?, role = ?, host = ?, model = ?, epoch = ?, "
                    "session_token = ?, agent_token = ?, "
                    "state = CASE WHEN state IN ('paused', 'spinning') THEN state ELSE 'registered' END, "
                    "lease_ttl_s = ?, last_seen_at = ?, lease_expires_at = ?, active_poll_count = 0, "
                    "wake_count = 0, stalled_since = NULL, notified_dead_at = NULL, "
                    "poll_timestamps_json = '[]', updated_at = ? WHERE agent_id = ?",
                    (
                        dot,
                        project,
                        role,
                        host,
                        model,
                        epoch,
                        session_token,
                        agent_token,
                        lease_ttl_s,
                        now_ts,
                        now_ts + lease_ttl_s,
                        now_ts,
                        agent_id,
                    ),
                )
                conn.execute(
                    "UPDATE exec_runs SET status = 'failed', ended_at = ? WHERE agent_id = ? AND status = 'running'",
                    (now_ts, agent_id),
                )
                conn.execute(
                    "UPDATE messages SET delivery_state = 'queued' "
                    "WHERE agent_id = ? AND direction = 'to_agent' AND delivery_state = 'offered'",
                    (agent_id,),
                )
            self.ledger.emit_event_locked(
                conn,
                agent_id,
                "agent_provisioned",
                {"dot": dot, "project": project, "host": host, "epoch": epoch},
                now_ts,
            )
            agent_row = dict(conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone())

        base_origin = self.http_url
        if self._hub_token and self.http_url.endswith("/" + self._hub_token):
            base_origin = self.http_url[: -(len(self._hub_token) + 1)]
        agent_http_url = f"{base_origin}/a/{agent_token}" if base_origin else ""
        agent_mcp_url = f"{agent_http_url}/mcp" if agent_http_url else ""
        self.wake_hub.wake_agent(agent_id)
        self.wake_hub.wake_events()
        return {
            "agent_id": agent_id,
            "epoch": epoch,
            "session_token": session_token,
            "agent_token": agent_token,
            "agent_http_url": agent_http_url,
            "agent_mcp_url": agent_mcp_url,
            "lease_ttl_s": lease_ttl_s,
            "agent": self._format_agent_row(agent_row),
        }

    # ---------------- Agent Operations ----------------

    def agent_register(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        if self.require_provision and bound_agent_id is None:
            raise HubError(
                "REGISTRATION_DISABLED",
                "hub requires owner provisioning via `subdots.py provision`",
                status=403,
            )
        agent_id = validate_token("agent_id", params.get("agent_id"))
        dot = validate_token("dot", params.get("dot") or "DOT")
        project = validate_token("project", params.get("project") or "default")
        role = validate_one_line("role", params.get("role") or "구현", 128)
        host = validate_one_line("host", params.get("host") or socket.gethostname(), 128)
        model = validate_one_line(
            "model",
            params.get("model") or "requested gpt-6.2-astra xhigh; runtime does not display effective",
            256,
        )
        raw_ttl = params.get("lease_ttl_s")
        lease_ttl_s = validate_finite_float(
            "lease_ttl_s",
            self.settings["lease_ttl_s"] if raw_ttl is None else raw_ttl,
            minimum=0.1,
            maximum=3600.0,
        )
        now_ts = utc_now_ts()
        session_token = secrets.token_hex(24)
        self._remember_secret(session_token)

        with self.ledger.tx() as conn:
            prev = conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            if prev is None:
                if conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0] >= 256:
                    raise HubError("TOO_MANY_AGENTS", "maximum agent limit (256) reached", status=429)
                epoch = 1
                conn.execute(
                    "INSERT INTO agents(agent_id, dot, project, role, host, model, epoch, session_token, state, "
                    "lease_ttl_s, last_seen_at, lease_expires_at, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'registered', ?, ?, ?, ?, ?)",
                    (
                        agent_id,
                        dot,
                        project,
                        role,
                        host,
                        model,
                        epoch,
                        session_token,
                        lease_ttl_s,
                        now_ts,
                        now_ts + lease_ttl_s,
                        now_ts,
                        now_ts,
                    ),
                )
            else:
                epoch = int(prev["epoch"]) + 1
                conn.execute(
                    "UPDATE agents SET dot = ?, project = ?, role = ?, host = ?, model = ?, epoch = ?, "
                    "session_token = ?, state = CASE WHEN state IN ('paused', 'spinning') THEN state ELSE 'registered' END, "
                    "lease_ttl_s = ?, last_seen_at = ?, lease_expires_at = ?, active_poll_count = 0, "
                    "wake_count = 0, stalled_since = NULL, notified_dead_at = NULL, "
                    "poll_timestamps_json = '[]', updated_at = ? WHERE agent_id = ?",
                    (
                        dot,
                        project,
                        role,
                        host,
                        model,
                        epoch,
                        session_token,
                        lease_ttl_s,
                        now_ts,
                        now_ts + lease_ttl_s,
                        now_ts,
                        agent_id,
                    ),
                )
                # Close stale running execs from previous epochs and re-queue offered messages.
                conn.execute(
                    "UPDATE exec_runs SET status = 'failed', ended_at = ? "
                    "WHERE agent_id = ? AND status = 'running'",
                    (now_ts, agent_id),
                )
                conn.execute(
                    "UPDATE messages SET delivery_state = 'queued' "
                    "WHERE agent_id = ? AND direction = 'to_agent' AND delivery_state = 'offered'",
                    (agent_id,),
                )
            self.ledger.emit_event_locked(
                conn,
                agent_id,
                "agent_registered",
                {"dot": dot, "project": project, "host": host, "epoch": epoch},
                now_ts,
            )
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)
            agent_row = dict(conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone())

        self.wake_hub.wake_agent(agent_id)
        self.wake_hub.wake_events()
        self._notify_unacked_failures(agent_id, failed, dot=agent_row.get("dot") or "")
        return {
            "agent_id": agent_id,
            "epoch": epoch,
            "session_token": session_token,
            "lease_ttl_s": lease_ttl_s,
            "agent": self._format_agent_row(agent_row),
            "messages": messages,
        }

    def _notify_unacked_failures(self, agent_id: str, failed: List[Dict[str, Any]], dot: str = "") -> None:
        if not failed or not self.notify_cmd:
            return
        resolved_dot = dot
        if not resolved_dot:
            with self.ledger._lock:
                row = self.ledger._conn.execute("SELECT dot FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
                if row is not None:
                    resolved_dot = row["dot"] or ""
        for m in failed:
            self._spawn_cmd_hook(
                self.notify_cmd,
                {
                    "agent": agent_id,
                    "project": m.get("project") or "default",
                    "id": m["id"],
                    "dot": resolved_dot or "-",
                    "task": m.get("task") or "-",
                    "state": "unacked_failed",
                    "why": "UNACKED_DELIVERY_FAILED",
                    "age": "0",
                    "hub_url": self.http_url,
                },
                "notify_triggered",
                agent_id,
            )

    def agent_heartbeat(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        ack_ids = validate_str_list("ack_ids", params.get("ack_ids"), max_items=64, item_limit=64)
        now_ts = utc_now_ts()

        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            recovered = self._recover_agent_liveness_locked(conn, agent, "heartbeat", now_ts)
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts, ack_ids=ack_ids)
            agent_row = dict(conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone())

        if recovered:
            self.wake_hub.wake_events()
        self._notify_unacked_failures(agent_id, failed, dot=agent_row.get("dot") or "")
        return {"ok": True, "agent": self._format_agent_row(agent_row), "messages": messages}

    def agent_poll(
        self,
        params: Dict[str, Any],
        is_disconnected: Optional[Callable[[], bool]] = None,
        bound_agent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        ack_ids = validate_str_list("ack_ids", params.get("ack_ids"), max_items=64, item_limit=64)
        hold_s = validate_finite_float(
            "hold_s",
            params.get("hold_s") if params.get("hold_s") is not None else 20.0,
            minimum=0.0,
            maximum=MAX_CHUNK_S,
        )
        interrupt_only = bool(params.get("interrupt_only", False))
        expect_re = params.get("expect_re")
        if expect_re is not None:
            expect_re = validate_token("expect_re", expect_re)
        exec_id = params.get("exec_id")
        if exec_id is not None:
            exec_id = validate_token("exec_id", exec_id)

        start_ts = utc_now_ts()
        spin_trigger: Optional[Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]] = None
        observed_gen = self.wake_hub.agent_gen(agent_id)
        exec_ended = False
        disconnected = False
        agent_dot = ""
        messages: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []
        recovered = False

        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            agent_dot = agent.get("dot") or ""
            if exec_id is not None:
                ex = conn.execute(
                    "SELECT status FROM exec_runs WHERE exec_id = ? AND agent_id = ?",
                    (exec_id, agent_id),
                ).fetchone()
                if ex is None or ex["status"] != "running":
                    return {
                        "agent_id": agent_id,
                        "epoch": int(epoch),
                        "messages": [],
                        "matched_expect": False,
                        "exec_finished": True,
                        "stopping": self.wake_hub.stop_event.is_set(),
                    }
            if int(agent["active_poll_count"]) >= 4:
                raise HubError("TOO_MANY_POLLS", f"too many concurrent polls for {agent_id}", status=429)
            ttl = float(agent["lease_ttl_s"])
            new_state = agent["state"]
            recovered = new_state in ("stalled", "dead", "disconnected")
            if new_state not in ("paused", "spinning", "completed"):
                new_state = "waiting" if not interrupt_only else "working"
            lease_ext = max(ttl, hold_s + 15.0)
            conn.execute(
                "UPDATE agents SET state = ?, last_seen_at = ?, lease_expires_at = ?, "
                "active_poll_count = active_poll_count + 1, wake_count = 0, stalled_since = NULL, "
                "notified_dead_at = NULL, updated_at = ? WHERE agent_id = ?",
                (new_state, start_ts, start_ts + lease_ext, start_ts, agent_id),
            )
            if recovered:
                self.ledger.emit_event_locked(conn, agent_id, "agent_recovered", {"via": "poll"}, start_ts)
            messages, failed = self._collect_piggyback_locked(
                conn, agent_id, start_ts, ack_ids=ack_ids, interrupt_only=interrupt_only
            )

        self._notify_unacked_failures(agent_id, failed, dot=agent_dot)
        if recovered:
            self.wake_hub.wake_events()

        deadline = time.monotonic() + hold_s
        try:
            while not messages and not exec_ended and hold_s > 0.0 and not self.wake_hub.stop_event.is_set():
                if is_disconnected is not None and is_disconnected():
                    disconnected = True
                    break
                rem = deadline - time.monotonic()
                if rem <= 0:
                    break
                self.wake_hub.wait_agent(agent_id, observed_gen, min(rem, 1.0))
                if self.wake_hub.stop_event.is_set():
                    break
                if is_disconnected is not None and is_disconnected():
                    disconnected = True
                    break
                observed_gen = self.wake_hub.agent_gen(agent_id)
                now_ts = utc_now_ts()
                with self.ledger.tx() as conn:
                    agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
                    if exec_id is not None:
                        ex = conn.execute(
                            "SELECT status FROM exec_runs WHERE exec_id = ? AND agent_id = ?",
                            (exec_id, agent_id),
                        ).fetchone()
                        if ex is None or ex["status"] != "running":
                            exec_ended = True
                            continue
                    ttl = float(agent["lease_ttl_s"])
                    conn.execute(
                        "UPDATE agents SET last_seen_at = ?, lease_expires_at = ?, updated_at = ? WHERE agent_id = ?",
                        (now_ts, now_ts + max(ttl, 15.0), now_ts, agent_id),
                    )
                    messages, failed = self._collect_piggyback_locked(
                        conn, agent_id, now_ts, interrupt_only=interrupt_only
                    )
                self._notify_unacked_failures(agent_id, failed, dot=agent_dot)
        finally:
            end_ts = utc_now_ts()
            elapsed = end_ts - start_ts
            with self.ledger.tx() as conn:
                row = conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
                if row is not None and int(row["epoch"]) == int(epoch):
                    if disconnected:
                        # Peer closed before response: drop poll ownership without extending lease.
                        conn.execute(
                            "UPDATE agents SET active_poll_count = MAX(0, active_poll_count - 1), updated_at = ? "
                            "WHERE agent_id = ?",
                            (end_ts, agent_id),
                        )
                    else:
                        agent = dict(row)
                        ttl = float(agent["lease_ttl_s"])
                        try:
                            poll_ts = [float(x) for x in json.loads(agent["poll_timestamps_json"]) if end_ts - float(x) <= 60.0]
                        except (ValueError, RecursionError):
                            poll_ts = []
                        if messages:
                            poll_ts = []
                        elif hold_s < 1.0 and elapsed < 0.5 and not interrupt_only:
                            poll_ts.append(end_ts)
                            if len(poll_ts) >= int(self.settings["spin_poll_burst"]):
                                iv_msg, strikes = self._trigger_intervention_locked(
                                    conn,
                                    agent,
                                    "SPIN_POLL_THRASH",
                                    f"{len(poll_ts)} rapid empty zero-hold polls within 60s",
                                    end_ts,
                                )
                                poll_ts = []
                                spin_trigger = (agent, iv_msg, strikes)
                                # Collect immediately so caller sees intervention right away.
                                extra_msgs, _ = self._collect_piggyback_locked(conn, agent_id, end_ts)
                                messages.extend(extra_msgs)
                        conn.execute(
                            "UPDATE agents SET active_poll_count = MAX(0, active_poll_count - 1), "
                            "last_seen_at = ?, lease_expires_at = ?, poll_timestamps_json = ?, updated_at = ? "
                            "WHERE agent_id = ?",
                            (end_ts, end_ts + ttl, json.dumps(poll_ts), end_ts, agent_id),
                        )

        if spin_trigger is not None:
            ag, iv_msg, strikes = spin_trigger
            self.wake_hub.wake_events()
            if self.notify_cmd and iv_msg is not None and iv_msg.get("is_new", True):
                self._spawn_cmd_hook(
                    self.notify_cmd,
                    {
                        "agent": agent_id,
                        "project": ag.get("project") or "default",
                        "id": iv_msg["id"],
                        "dot": ag.get("dot") or "-",
                        "task": ag.get("task") or "-",
                        "state": "spinning",
                        "why": f"SPIN_POLL_THRASH(strike={strikes})",
                        "age": "0",
                        "hub_url": self.http_url,
                    },
                    "notify_triggered",
                    agent_id,
                )

        if messages:
            self.wake_hub.wake_events()
        matched_expect = bool(
            expect_re
            and any(
                m.get("re_id") == expect_re
                or bool(m.get("interrupt"))
                or m.get("kind") in ("release", "steer", "interrupt", "pause", "cancel", "intervention")
                for m in messages
            )
        )
        return {
            "agent_id": agent_id,
            "epoch": int(epoch),
            "messages": messages,
            "matched_expect": matched_expect,
            "exec_finished": exec_ended,
            "stopping": self.wake_hub.stop_event.is_set(),
        }

    def agent_step(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        state = params.get("state")
        if state not in LIVE_STATES:
            raise ValueError(f"state must be one of {LIVE_STATES}")
        task = validate_one_line("task", params.get("task"))
        note = validate_one_line("note", params.get("note"))
        evidence = validate_one_line("evidence", params.get("evidence"))
        host = validate_one_line("host", params.get("host") or socket.gethostname(), 128)
        ack_ids = validate_str_list("ack_ids", params.get("ack_ids"), max_items=64, item_limit=64)
        decisions_in = validate_str_list("decisions", params.get("decisions"), max_items=MAX_DECISIONS, item_limit=MAX_ITEM_LEN)
        blockers_raw = params.get("blockers")
        blockers_in = validate_str_list("blockers", blockers_raw, max_items=MAX_BLOCKERS, item_limit=MAX_ITEM_LEN) if blockers_raw is not None else None
        unblock = bool(params.get("unblock", False))
        now_ts = utc_now_ts()
        ts_iso = iso_utc(now_ts)

        spin_trigger: Optional[Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]] = None
        mirror_payload: Optional[Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, str]], List[str]]] = None

        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            if ack_ids:
                self._apply_acks_locked(conn, agent_id, ack_ids, now_ts)

            if state == "done":
                if agent["state"] == "paused":
                    raise HubError("AGENT_PAUSED", f"done refused: {agent_id} is paused", status=409)
                owed = conn.execute(
                    "SELECT id, kind, task FROM messages WHERE agent_id = ? AND direction = 'to_agent' "
                    "AND kind IN ('task', 'steer', 'interrupt', 'question', 'review', 'intervention') "
                    "AND delivery_state IN ('queued', 'offered', 'acked', 'unacked_failed') ORDER BY created_at ASC",
                    (agent_id,),
                ).fetchall()
                if owed:
                    raise HubError(
                        "UNANSWERED_WORK",
                        f"done refused: {agent_id} still owes an answer to {len(owed)} steering message(s)",
                        status=409,
                        extra={"owed": [dict(r) for r in owed]},
                    )

            same_task = agent["task"] == task
            goal = params.get("goal") or (agent["goal"] if same_task else None)
            doing = params.get("doing") or (agent["doing"] if same_task else None)
            next_step = params.get("next_step") or (agent["next_step"] if same_task else None)
            goal = validate_one_line("goal", goal)
            doing = validate_one_line("doing", doing)
            next_step = validate_one_line("next", next_step)

            prior_decisions = json.loads(agent["decisions_json"]) if same_task else []
            for d in decisions_in:
                prior_decisions.append({"t": ts_iso, "text": d})
            decisions = prior_decisions[-MAX_DECISIONS:]

            prior_blockers = [] if (unblock or not same_task) else json.loads(agent["blockers_json"])
            if blockers_in:
                for bt in blockers_in:
                    if bt not in prior_blockers:
                        prior_blockers.append(bt)
            if len(prior_blockers) > MAX_BLOCKERS:
                raise ValueError(f"at most {MAX_BLOCKERS} blockers allowed")
            if state == "blocked" and not prior_blockers:
                raise ValueError("state='blocked' requires at least one blocker")

            # Map live state -> hub agent state
            hub_state = {
                "start": "working",
                "step": "working",
                "blocked": "blocked",
                "done": "completed",
            }[state]
            if agent["state"] in ("paused", "spinning") and state != "done":
                hub_state = agent["state"]

            # Anti-spin: no-progress detection on consecutive identical start/step reports
            recent_steps: List[Dict[str, Any]] = []
            if state in ("start", "step"):
                try:
                    recent_steps = [
                        s for s in json.loads(agent["recent_steps_json"])
                        if now_ts - float(s.get("t", 0)) <= float(self.settings["spin_window_s"])
                    ]
                except ValueError:
                    recent_steps = []
                sig = hash_payload(
                    [
                        state,
                        normalize_text_sig(task),
                        normalize_text_sig(doing),
                        normalize_text_sig(note),
                        normalize_text_sig(evidence),
                    ]
                )
                recent_steps.append({"t": now_ts, "sig": sig})
                recent_steps = recent_steps[-20:]
                repeats = int(self.settings["spin_step_repeats"])
                if len(recent_steps) >= repeats and all(s["sig"] == sig for s in recent_steps[-repeats:]):
                    iv_msg, strikes = self._trigger_intervention_locked(
                        conn,
                        agent,
                        "SPIN_NO_PROGRESS",
                        f"{repeats} consecutive identical steps on task {task!r} ({doing})",
                        now_ts,
                    )
                    recent_steps = []
                    hub_state = "spinning"
                    spin_trigger = (agent, iv_msg, strikes)

            ttl = float(agent["lease_ttl_s"])
            conn.execute(
                "UPDATE agents SET host = ?, state = ?, task = ?, goal = ?, doing = ?, next_step = ?, "
                "decisions_json = ?, blockers_json = ?, recent_steps_json = ?, last_seen_at = ?, "
                "lease_expires_at = ?, wake_count = 0, stalled_since = NULL, notified_dead_at = NULL, "
                "updated_at = ? WHERE agent_id = ?",
                (
                    host,
                    hub_state,
                    task,
                    goal,
                    doing,
                    next_step,
                    json.dumps(decisions, ensure_ascii=False),
                    json.dumps(prior_blockers, ensure_ascii=False),
                    json.dumps(recent_steps, ensure_ascii=False),
                    now_ts,
                    now_ts + ttl,
                    now_ts,
                    agent_id,
                ),
            )
            self.ledger.emit_event_locked(
                conn,
                agent_id,
                "agent_step",
                {"state": state, "task": task, "host": host, "doing": doing, "evidence": evidence},
                now_ts,
            )
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)
            agent_row = dict(conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone())
            mirror_payload = (
                agent_row,
                {
                    "t": ts_iso,
                    "task": task,
                    "state": state,
                    "host": host,
                    "note": note,
                    "evidence": evidence,
                    "goal": goal,
                    "doing": doing,
                    "next_step": next_step,
                },
                decisions,
                prior_blockers,
            )

        if self.mirror.enabled and mirror_payload is not None:
            try:
                m_err = self.mirror.mirror_step(*mirror_payload)
            except Exception as exc:
                m_err = f"mirror_step: {self._scrub_hook_text(str(exc))}"
            if m_err:
                with self.ledger.tx() as conn:
                    self.ledger.emit_event_locked(conn, agent_id, "mirror_error", {"detail": m_err})

        if spin_trigger is not None:
            ag, iv_msg, strikes = spin_trigger
            if self.notify_cmd and iv_msg is not None and iv_msg.get("is_new", True):
                self._spawn_cmd_hook(
                    self.notify_cmd,
                    {
                        "agent": agent_id,
                        "project": ag.get("project") or "default",
                        "id": iv_msg["id"],
                        "dot": ag.get("dot") or "-",
                        "task": task or "-",
                        "state": "spinning",
                        "why": f"SPIN_NO_PROGRESS(strike={strikes})",
                        "age": "0",
                        "hub_url": self.http_url,
                    },
                    "notify_triggered",
                    agent_id,
                )

        self._notify_unacked_failures(agent_id, failed, dot=agent_row.get("dot") or "")
        self.wake_hub.wake_events()
        return {"ok": True, "t": ts_iso, "agent": self._format_agent_row(agent_row), "messages": messages}

    def agent_reply(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        kind = params.get("kind")
        if kind not in TO_LOCAL_KINDS:
            raise ValueError(f"reply kind must be one of {TO_LOCAL_KINDS}")
        task = validate_one_line("task", params.get("task"))
        text_body = validate_body("text", params.get("text"))
        re_id = params.get("re_id")
        if re_id is not None:
            re_id = validate_token("re_id", re_id)
        refs = validate_str_list("refs", params.get("refs"), max_items=32, item_limit=MAX_TEXT_LEN)
        ack_ids = validate_str_list("ack_ids", params.get("ack_ids"), max_items=64, item_limit=64)
        if re_id and re_id not in ack_ids:
            ack_ids.append(re_id)
        now_ts = utc_now_ts()
        ts_iso = iso_utc(now_ts)

        msg_row: Dict[str, Any]
        dot_name: Optional[str] = None
        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            dot_name = agent.get("dot")
            project = validate_token("project", params.get("project") or agent.get("project") or "default")
            if re_id:
                parent = conn.execute(
                    "SELECT id FROM messages WHERE id = ? AND agent_id = ? AND direction = 'to_agent'",
                    (re_id, agent_id),
                ).fetchone()
                if parent is None:
                    raise HubError("INVALID_RE", f"re_id {re_id!r} not found in {agent_id} inbox", status=404)
            elif kind in REPLY_WAITS:
                if not refs:
                    raise ValueError(f"{kind} without --re requires at least one --ref")
            elif kind != "exec_interrupted":
                raise ValueError(f"{kind} requires --re")

            if ack_ids:
                self._apply_acks_locked(conn, agent_id, ack_ids, now_ts)
            if re_id and kind in ("answer", "result", "question", "block", "exec_interrupted"):
                conn.execute(
                    "UPDATE messages SET delivery_state = 'answered', acked_at = COALESCE(acked_at, ?), "
                    "answered_at = ? WHERE id = ? AND agent_id = ? AND direction = 'to_agent'",
                    (now_ts, now_ts, re_id, agent_id),
                )
                self.ledger.emit_event_locked(conn, agent_id, "message_answered", {"id": re_id}, now_ts)

            p_hash = hash_payload([agent_id, project, task, kind, re_id, refs, text_body])
            mid = self._insert_message_locked(
                conn,
                request_id=None,
                payload_hash=p_hash,
                direction="to_local",
                agent_id=agent_id,
                project=project,
                task=task,
                kind=kind,
                priority=0,
                interrupt=0,
                re_id=re_id,
                text_body=text_body,
                refs_json=json.dumps(refs),
                delivery_state="delivered",
                created_at=now_ts,
                created_at_iso=ts_iso,
            )
            self._recover_agent_liveness_locked(conn, agent, "reply", now_ts, recent_steps_json="[]")
            self.ledger.emit_event_locked(
                conn,
                agent_id,
                "agent_reply",
                {"id": mid, "kind": kind, "re_id": re_id, "task": task},
                now_ts,
            )
            msg_row = dict(conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone())
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)

        if self.mirror.enabled:
            try:
                m_err = self.mirror.mirror_mail("to_local", msg_row, dot=dot_name)
            except Exception as exc:
                m_err = f"mirror_mail: {self._scrub_hook_text(str(exc))}"
            if m_err:
                with self.ledger.tx() as conn:
                    self.ledger.emit_event_locked(conn, agent_id, "mirror_error", {"detail": m_err})

        self._notify_unacked_failures(agent_id, failed, dot=dot_name or "")
        self.wake_hub.wake_events()
        return {"ok": True, "reply": self._format_msg_row(msg_row), "messages": messages}

    def agent_ack(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        ack_ids = validate_str_list("ack_ids", params.get("ack_ids"), max_items=64, item_limit=64)
        now_ts = utc_now_ts()

        dot_name = ""
        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            dot_name = agent.get("dot") or ""
            acked = self._apply_acks_locked(conn, agent_id, ack_ids, now_ts)
            recovered = self._recover_agent_liveness_locked(conn, agent, "ack", now_ts)
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)

        self._notify_unacked_failures(agent_id, failed, dot=dot_name)
        if acked or recovered:
            self.wake_hub.wake_events()
        return {"ok": True, "acked": acked, "messages": messages}

    def exec_start(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        cmd = self._scrub_hook_text(validate_body("cmd", params.get("cmd")))
        cwd = self._scrub_hook_text(validate_one_line("cwd", str(params.get("cwd") or "."), limit=1024))
        ack_ids = validate_str_list("ack_ids", params.get("ack_ids"), max_items=64, item_limit=64)
        now_ts = utc_now_ts()
        exec_id = f"ex-{uuid.uuid4().hex[:12]}"
        cmd_norm = normalize_text_sig(cmd)
        blocked_stop: Optional[Dict[str, Any]] = None
        blocked_spinning = False
        dot_name = ""
        messages: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            dot_name = agent.get("dot") or ""
            if agent["state"] == "paused":
                raise HubError("AGENT_PAUSED", f"exec refused: {agent_id} is paused", status=409)
            if ack_ids:
                self._apply_acks_locked(conn, agent_id, ack_ids, now_ts)
                agent = dict(conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone())
            pending_stop = conn.execute(
                "SELECT id, kind FROM messages WHERE agent_id = ? AND direction = 'to_agent' "
                "AND kind IN ('pause', 'cancel', 'intervention') AND delivery_state IN ('queued', 'offered') "
                "ORDER BY created_at ASC LIMIT 1",
                (agent_id,),
            ).fetchone()
            if pending_stop is not None:
                blocked_stop = dict(pending_stop)
                messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)
            elif agent["state"] == "spinning":
                blocked_spinning = True
                messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)
            else:
                conn.execute(
                    "INSERT INTO exec_runs(exec_id, agent_id, epoch, cmd, cmd_norm, cwd, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'running', ?)",
                    (exec_id, agent_id, int(epoch), cmd, cmd_norm, cwd, now_ts),
                )
                self._recover_agent_liveness_locked(conn, agent, "exec_start", now_ts, override_state="working")
                self.ledger.emit_event_locked(
                    conn, agent_id, "exec_started", {"exec_id": exec_id, "cmd": cmd_norm[:160]}, now_ts
                )
                messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)

        self._notify_unacked_failures(agent_id, failed, dot=dot_name)
        self.wake_hub.wake_events()
        if blocked_stop is not None:
            raise HubError(
                "INTERRUPT_PENDING",
                f"exec refused: pending {blocked_stop['kind']} message {blocked_stop['id']}",
                status=409,
                extra={"messages": messages},
            )
        if blocked_spinning:
            raise HubError(
                "AGENT_SPINNING",
                f"exec refused: {agent_id} is spinning; acknowledge intervention and wait for steering",
                status=409,
                extra={"messages": messages},
            )
        return {"ok": True, "exec_id": exec_id, "messages": messages}

    def exec_heartbeat(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        exec_id = validate_token("exec_id", params.get("exec_id"))
        now_ts = utc_now_ts()

        dot_name = ""
        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            dot_name = agent.get("dot") or ""
            recovered = self._recover_agent_liveness_locked(conn, agent, "exec_heartbeat", now_ts)
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)

        if recovered:
            self.wake_hub.wake_events()
        self._notify_unacked_failures(agent_id, failed, dot=dot_name)
        interrupt_msgs = [m for m in messages if m.get("interrupt") or m.get("kind") in INTERRUPT_KINDS]
        return {"ok": True, "exec_id": exec_id, "messages": messages, "interrupt_requested": bool(interrupt_msgs)}

    def exec_finish(self, params: Dict[str, Any], bound_agent_id: Optional[str] = None) -> Dict[str, Any]:
        self._enforce_agent_scope(bound_agent_id, params)
        agent_id = validate_token("agent_id", params.get("agent_id"))
        epoch = params.get("epoch")
        session_token = params.get("session_token")
        exec_id = validate_token("exec_id", params.get("exec_id"))
        status = params.get("status")
        if status not in ("completed", "failed", "interrupted"):
            raise ValueError("status must be completed, failed, or interrupted")
        exit_code = params.get("exit_code")
        if exit_code is not None:
            if isinstance(exit_code, bool):
                raise ValueError("exit_code must be an integer")
            exit_code = int(exit_code)
        raw_stderr_hash = str(params.get("stderr_hash") or "")
        stderr_hash = validate_one_line("stderr_hash", raw_stderr_hash, 128) if raw_stderr_hash else ""
        output_tail = self._scrub_hook_text(str(params.get("output_tail") or "")[-2000:])
        if output_tail.strip():
            raw_sig = hashlib.sha256(normalize_text_sig(output_tail).encode("utf-8")).hexdigest()[:16]
            if not stderr_hash or stderr_hash == raw_sig:
                stderr_hash = hashlib.sha256(normalize_spin_output(output_tail).encode("utf-8")).hexdigest()[:16]
        interrupted_by = params.get("interrupted_by")
        if interrupted_by is not None:
            interrupted_by = validate_token("interrupted_by", interrupted_by)
        now_ts = utc_now_ts()

        spin_trigger: Optional[Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]] = None
        dot_name = ""
        with self.ledger.tx() as conn:
            agent = self._verify_epoch_locked(conn, agent_id, epoch, session_token)
            dot_name = agent.get("dot") or ""
            ex = conn.execute(
                "SELECT * FROM exec_runs WHERE exec_id = ? AND agent_id = ?",
                (exec_id, agent_id),
            ).fetchone()
            if ex is None:
                raise HubError("EXEC_NOT_FOUND", f"unknown exec_id {exec_id!r}", status=404)
            conn.execute(
                "UPDATE exec_runs SET status = ?, exit_code = ?, stderr_hash = ?, output_tail = ?, "
                "ended_at = ?, interrupted_by = ? WHERE exec_id = ?",
                (status, exit_code, stderr_hash, output_tail, now_ts, interrupted_by, exec_id),
            )
            if status == "interrupted" and interrupted_by:
                self._apply_acks_locked(conn, agent_id, [interrupted_by], now_ts)

            try:
                recent_execs = [
                    r for r in json.loads(agent["recent_execs_json"])
                    if now_ts - float(r.get("t", 0)) <= float(self.settings["spin_window_s"])
                ]
            except (ValueError, RecursionError):
                recent_execs = []

            cmd_norm = ex["cmd_norm"]
            if status == "completed" and exit_code == 0:
                recent_execs = [r for r in recent_execs if r.get("cmd_norm") != cmd_norm]
            elif status != "interrupted" and exit_code is not None and exit_code != 0:
                recent_execs.append(
                    {"t": now_ts, "cmd_norm": cmd_norm, "exit_code": exit_code, "stderr_hash": stderr_hash}
                )
                recent_execs = recent_execs[-20:]
                matches = [
                    r for r in recent_execs
                    if r["cmd_norm"] == cmd_norm and r["exit_code"] == exit_code and r["stderr_hash"] == stderr_hash
                ]
                repeats = int(self.settings["spin_cmd_repeats"])
                if len(matches) >= repeats:
                    iv_msg, strikes = self._trigger_intervention_locked(
                        conn,
                        agent,
                        "SPIN_COMMAND_LOOP",
                        f"Command {self._scrub_hook_text(cmd_norm)!r} failed {len(matches)}x with exit={exit_code}",
                        now_ts,
                    )
                    recent_execs = [r for r in recent_execs if r.get("cmd_norm") != cmd_norm]
                    spin_trigger = (agent, iv_msg, strikes)

            fresh_ag = dict(conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone())
            self._recover_agent_liveness_locked(
                conn,
                fresh_ag,
                "exec_finish",
                now_ts,
                recent_execs_json=json.dumps(recent_execs),
            )
            self.ledger.emit_event_locked(
                conn,
                agent_id,
                "exec_finished",
                {"exec_id": exec_id, "status": status, "exit_code": exit_code},
                now_ts,
            )
            messages, failed = self._collect_piggyback_locked(conn, agent_id, now_ts)

        if spin_trigger is not None:
            ag, iv_msg, strikes = spin_trigger
            if self.notify_cmd and iv_msg is not None and iv_msg.get("is_new", True):
                self._spawn_cmd_hook(
                    self.notify_cmd,
                    {
                        "agent": agent_id,
                        "project": ag.get("project") or "default",
                        "id": iv_msg["id"],
                        "dot": ag.get("dot") or "-",
                        "task": ag.get("task") or "-",
                        "state": "spinning",
                        "why": f"SPIN_COMMAND_LOOP(strike={strikes})",
                        "age": "0",
                        "hub_url": self.http_url,
                    },
                    "notify_triggered",
                    agent_id,
                )

        self._notify_unacked_failures(agent_id, failed, dot=dot_name)
        self.wake_hub.wake_agent(agent_id)
        self.wake_hub.wake_events()
        return {"ok": True, "exec_id": exec_id, "messages": messages}

    # ---------------- Local Session Operations ----------------

    def local_send(self, params: Dict[str, Any]) -> Dict[str, Any]:
        agent_id = validate_token("agent_id", params.get("agent_id"))
        kind = params.get("kind") or "task"
        if kind not in TO_AGENT_KINDS:
            raise ValueError(f"kind must be one of {TO_AGENT_KINDS}")
        task = validate_one_line("task", params.get("task") or kind)
        text_body = validate_body("text", params.get("text"))
        re_id = params.get("re_id")
        if re_id is not None:
            re_id = validate_token("re_id", re_id)
        refs = validate_str_list("refs", params.get("refs"), max_items=32, item_limit=MAX_TEXT_LEN)
        interrupt = bool(params.get("interrupt", False)) or kind in INTERRUPT_KINDS
        priority = self._priority_for_kind(kind, interrupt)
        request_id = params.get("request_id")
        if request_id is not None:
            request_id = validate_token("request_id", request_id)
        now_ts = utc_now_ts()
        ts_iso = iso_utc(now_ts)

        msg_row: Dict[str, Any] = {}
        deduped = False
        wake_needed = False
        wake_agent_row: Optional[Dict[str, Any]] = None

        with self.ledger.tx() as conn:
            agent_row = conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            if agent_row is not None:
                wake_agent_row = dict(agent_row)
            project = validate_token(
                "project", params.get("project") or (agent_row["project"] if agent_row else "default")
            )
            if re_id is not None:
                ref_msg = conn.execute(
                    "SELECT id, direction, delivery_state FROM messages WHERE id = ? AND agent_id = ?",
                    (re_id, agent_id),
                ).fetchone()
                if ref_msg is None:
                    raise HubError("INVALID_RE", f"re_id {re_id!r} not found for agent {agent_id!r}", status=404)
                if ref_msg["direction"] == "to_local" and ref_msg["delivery_state"] == "delivered":
                    conn.execute("UPDATE messages SET delivery_state = 'read' WHERE id = ?", (re_id,))

            payload_sig = hash_payload([agent_id, project, task, kind, re_id, refs, text_body, interrupt])

            if request_id is not None:
                prev = conn.execute("SELECT * FROM messages WHERE request_id = ?", (request_id,)).fetchone()
                if prev is not None:
                    if prev["payload_hash"] != payload_sig:
                        raise HubError(
                            "REQUEST_ID_CONFLICT",
                            f"request_id {request_id!r} already used with a different payload",
                            status=409,
                        )
                    msg_row = dict(prev)
                    deduped = True

            if not deduped:
                mid = self._insert_message_locked(
                    conn,
                    request_id=request_id,
                    payload_hash=payload_sig,
                    direction="to_agent",
                    agent_id=agent_id,
                    project=project,
                    task=task,
                    kind=kind,
                    priority=priority,
                    interrupt=1 if interrupt else 0,
                    re_id=re_id,
                    text_body=text_body,
                    refs_json=json.dumps(refs),
                    delivery_state="queued",
                    created_at=now_ts,
                    created_at_iso=ts_iso,
                )
                if wake_agent_row is not None:
                    ag = wake_agent_row
                    rearm_ttl = float(ag["lease_ttl_s"])
                    if kind == "pause":
                        conn.execute(
                            "UPDATE agents SET state = 'paused', updated_at = ? WHERE agent_id = ?",
                            (now_ts, agent_id),
                        )
                    elif kind == "resume" and ag["state"] in ("paused", "spinning"):
                        conn.execute(
                            "UPDATE agents SET state = 'working', lease_expires_at = MAX(lease_expires_at, ?), "
                            "stalled_since = NULL, notified_dead_at = NULL, wake_count = 0, updated_at = ? "
                            "WHERE agent_id = ?",
                            (now_ts + rearm_ttl, now_ts, agent_id),
                        )
                    elif kind == "release":
                        conn.execute(
                            "UPDATE agents SET state = 'completed', updated_at = ? WHERE agent_id = ?",
                            (now_ts, agent_id),
                        )
                    elif ag["state"] == "spinning" and kind in ("steer", "task", "answer", "review", "interrupt", "cancel"):
                        # Human steering clears spinning lockout so agent can continue under new direction.
                        conn.execute(
                            "UPDATE agents SET state = 'working', lease_expires_at = MAX(lease_expires_at, ?), "
                            "stalled_since = NULL, notified_dead_at = NULL, wake_count = 0, updated_at = ? "
                            "WHERE agent_id = ?",
                            (now_ts + rearm_ttl, now_ts, agent_id),
                        )
                    elif ag["state"] == "completed" and kind in (
                        "task",
                        "steer",
                        "question",
                        "review",
                        "interrupt",
                        "intervention",
                    ):
                        # Re-arm completed agent for Task 2+ so watchdog monitors it if offline.
                        conn.execute(
                            "UPDATE agents SET state = 'working', lease_expires_at = MAX(lease_expires_at, ?), "
                            "stalled_since = NULL, notified_dead_at = NULL, wake_count = 0, updated_at = ? "
                            "WHERE agent_id = ?",
                            (now_ts + rearm_ttl, now_ts, agent_id),
                        )
                    elif ag["state"] in ("stalled", "dead", "disconnected") and kind != "pause":
                        conn.execute(
                            "UPDATE agents SET lease_expires_at = MAX(lease_expires_at, ?), "
                            "stalled_since = NULL, notified_dead_at = NULL, wake_count = 0, updated_at = ? "
                            "WHERE agent_id = ?",
                            (now_ts + rearm_ttl, now_ts, agent_id),
                        )
                    if (
                        self.wake_cmd is not None
                        and kind not in ("pause", "release")
                        and int(ag["active_poll_count"]) == 0
                        and ag["state"] in ("paused", "completed", "stalled", "dead", "disconnected")
                    ):
                        wake_count = int(ag["wake_count"])
                        last_wake = float(ag["last_wake_at"] or 0.0)
                        due_wait = float(self.settings["wake_backoff_s"]) * (2 ** wake_count)
                        if wake_count < int(self.settings["max_wakes"]) and (
                            ag["last_wake_at"] is None or now_ts - last_wake >= due_wait
                        ):
                            conn.execute(
                                "UPDATE agents SET wake_count = ?, last_wake_at = ?, updated_at = ? WHERE agent_id = ?",
                                (wake_count + 1, now_ts, now_ts, agent_id),
                            )
                            wake_needed = True

                self.ledger.emit_event_locked(
                    conn,
                    agent_id,
                    "message_queued",
                    {"id": mid, "kind": kind, "task": task, "interrupt": interrupt},
                    now_ts,
                )
                msg_row = dict(conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone())

        if not deduped:
            if self.mirror.enabled:
                try:
                    m_err = self.mirror.mirror_mail(
                        "to_agent",
                        msg_row,
                        dot=wake_agent_row.get("dot") if wake_agent_row else None,
                    )
                except Exception as exc:
                    m_err = f"mirror_mail: {self._scrub_hook_text(str(exc))}"
                if m_err:
                    with self.ledger.tx() as conn:
                        self.ledger.emit_event_locked(conn, agent_id, "mirror_error", {"detail": m_err})
            self.wake_hub.wake_agent(agent_id)
            self.wake_hub.wake_events()
            if wake_needed and self.wake_cmd and wake_agent_row is not None:
                self._spawn_cmd_hook(
                    self.wake_cmd,
                    {
                        "agent": agent_id,
                        "project": msg_row["project"],
                        "id": msg_row["id"],
                        "dot": wake_agent_row.get("dot") or "-",
                        "task": task or "-",
                        "state": wake_agent_row["state"],
                        "why": "NEW_STEERING_TO_INACTIVE_AGENT",
                        "age": str(round(max(0.0, now_ts - float(wake_agent_row["last_seen_at"])))),
                        "hub_url": self.http_url,
                    },
                    "wake_triggered",
                    agent_id,
                )

        return {
            "ok": True,
            "deduped": deduped,
            "message": self._format_msg_row(msg_row),
            "agent_listening": bool(wake_agent_row and int(wake_agent_row["active_poll_count"]) > 0),
        }

    def local_await_reply(self, params: Dict[str, Any]) -> Dict[str, Any]:
        agent_id = validate_token("agent_id", params.get("agent_id"))
        re_id = params.get("re_id")
        if re_id is not None:
            re_id = validate_token("re_id", re_id)
        raw_tout = params.get("timeout_s")
        timeout_s = validate_finite_float(
            "timeout_s",
            60.0 if raw_tout is None else raw_tout,
            minimum=0.0,
            maximum=3600.0,
        )
        since_ts = validate_finite_float("since_ts", params.get("since_ts") or 0.0, minimum=0.0)

        def _find_reply() -> Optional[Dict[str, Any]]:
            with self.ledger._lock:
                if re_id:
                    row = self.ledger._conn.execute(
                        "SELECT * FROM messages WHERE agent_id = ? AND direction = 'to_local' "
                        "AND re_id = ? ORDER BY created_at DESC LIMIT 1",
                        (agent_id, re_id),
                    ).fetchone()
                else:
                    row = self.ledger._conn.execute(
                        "SELECT * FROM messages WHERE agent_id = ? AND direction = 'to_local' "
                        "AND created_at >= ? AND delivery_state = 'delivered' ORDER BY created_at ASC LIMIT 1",
                        (agent_id, since_ts),
                    ).fetchone()
                return self._format_msg_row(dict(row)) if row is not None else None

        observed_gen = self.wake_hub.current_events_gen()
        found = _find_reply()
        deadline = time.monotonic() + timeout_s
        while found is None and not self.wake_hub.stop_event.is_set():
            rem = deadline - time.monotonic()
            if rem <= 0:
                break
            self.wake_hub.wait_events(observed_gen, min(rem, 5.0))
            observed_gen = self.wake_hub.current_events_gen()
            found = _find_reply()

        return {"found": found is not None, "reply": found}

    def local_list_agents(self) -> Dict[str, Any]:
        with self.ledger._lock:
            rows = self.ledger._conn.execute("SELECT * FROM agents ORDER BY updated_at DESC").fetchall()
            pending_counts = {
                r["agent_id"]: r["cnt"]
                for r in self.ledger._conn.execute(
                    "SELECT agent_id, COUNT(*) AS cnt FROM messages WHERE direction = 'to_agent' "
                    "AND delivery_state IN ('queued', 'offered') GROUP BY agent_id"
                ).fetchall()
            }
            unread_out_counts = {
                r["agent_id"]: r["cnt"]
                for r in self.ledger._conn.execute(
                    "SELECT agent_id, COUNT(*) AS cnt FROM messages WHERE direction = 'to_local' "
                    "AND delivery_state = 'delivered' GROUP BY agent_id"
                ).fetchall()
            }
        agents = []
        for raw in rows:
            item = self._format_agent_row(dict(raw))
            item["pending_to_agent"] = pending_counts.get(item["agent_id"], 0)
            item["unread_from_agent"] = unread_out_counts.get(item["agent_id"], 0)
            agents.append(item)
        return {"t": iso_utc(), "agents": agents}

    def local_agent_status(self, agent_id: str) -> Dict[str, Any]:
        agent_id = validate_token("agent_id", agent_id)
        with self.ledger._lock:
            row = self.ledger._conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            if row is None:
                raise HubError("AGENT_NOT_FOUND", f"unknown agent {agent_id!r}", status=404)
            msgs = [
                self._format_msg_row(dict(m))
                for m in self.ledger._conn.execute(
                    "SELECT * FROM messages WHERE agent_id = ? ORDER BY created_at DESC LIMIT 20",
                    (agent_id,),
                ).fetchall()
            ]
            execs = [
                dict(e)
                for e in self.ledger._conn.execute(
                    "SELECT * FROM exec_runs WHERE agent_id = ? ORDER BY started_at DESC LIMIT 10",
                    (agent_id,),
                ).fetchall()
            ]
        return {"agent": self._format_agent_row(dict(row)), "recent_messages": msgs, "recent_execs": execs}

    def local_inbox(self, agent_id: Optional[str] = None, include_read: bool = False) -> Dict[str, Any]:
        with self.ledger._lock:
            clauses = ["direction = 'to_local'"]
            args: List[Any] = []
            if agent_id:
                clauses.append("agent_id = ?")
                args.append(validate_token("agent_id", agent_id))
            if not include_read:
                clauses.append("delivery_state = 'delivered'")
            rows = self.ledger._conn.execute(
                f"SELECT * FROM messages WHERE {' AND '.join(clauses)} ORDER BY created_at ASC",
                args,
            ).fetchall()
        return {"messages": [self._format_msg_row(dict(r)) for r in rows]}

    def local_mark_read(self, agent_id: str, msg_id: str) -> Dict[str, Any]:
        agent_id = validate_token("agent_id", agent_id)
        msg_id = validate_token("id", msg_id)
        with self.ledger.tx() as conn:
            cur = conn.execute(
                "UPDATE messages SET delivery_state = 'read' WHERE id = ? AND agent_id = ? AND direction = 'to_local'",
                (msg_id, agent_id),
            )
            if cur.rowcount == 0:
                raise HubError("MESSAGE_NOT_FOUND", f"message {msg_id!r} not found for {agent_id}", status=404)
        return {"ok": True, "id": msg_id}

    def local_events(self, agent_id: Optional[str] = None, after_seq: int = 0, limit: int = 100) -> Dict[str, Any]:
        if isinstance(after_seq, bool) or isinstance(limit, bool):
            raise ValueError("after_seq and limit must be integers")
        seq_val = max(0, int(after_seq))
        lim_val = max(1, min(int(limit), 500))
        with self.ledger._lock:
            clauses = ["seq > ?"]
            args: List[Any] = [seq_val]
            if agent_id:
                clauses.append("agent_id = ?")
                args.append(validate_token("agent_id", agent_id))
            args.append(lim_val)
            rows = self.ledger._conn.execute(
                f"SELECT * FROM events WHERE {' AND '.join(clauses)} ORDER BY seq ASC LIMIT ?",
                args,
            ).fetchall()
        events = []
        for raw in rows:
            d = dict(raw)
            try:
                d["payload"] = json.loads(d.pop("payload_json", "{}"))
            except ValueError:
                d["payload"] = {}
            events.append(d)
        return {"events": events}

    def local_wake(self, agent_id: str) -> Dict[str, Any]:
        agent_id = validate_token("agent_id", agent_id)
        with self.ledger._lock:
            row = self.ledger._conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            if row is None:
                raise HubError("AGENT_NOT_FOUND", f"unknown agent {agent_id!r}", status=404)
            ag = dict(row)
        self.wake_hub.wake_agent(agent_id)
        if not self.wake_cmd:
            return {"ok": False, "ran": False, "reason": "no_wake_cmd_configured"}
        self._spawn_cmd_hook(
            self.wake_cmd,
            {
                "agent": agent_id,
                "project": ag.get("project") or "default",
                "id": "manual-wake",
                "dot": ag.get("dot") or "-",
                "task": ag.get("task") or "-",
                "state": ag["state"],
                "why": "MANUAL_WAKE",
                "age": str(round(max(0.0, utc_now_ts() - float(ag["last_seen_at"])))),
                "hub_url": self.http_url,
            },
            "wake_triggered",
            agent_id,
        )
        return {"ok": True, "ran": True}

    # ---------------- Background Anti-Death Watchdog ----------------

    def watchdog_tick(self) -> None:
        now_ts = utc_now_ts()
        dead_grace_s = float(self.settings["dead_grace_s"])
        wake_backoff_s = float(self.settings["wake_backoff_s"])
        max_wakes = int(self.settings["max_wakes"])
        notify_after_s = float(self.settings["notify_after_s"])

        wakes_to_run: List[Tuple[Dict[str, Any], str]] = []
        notifies_to_run: List[Tuple[Dict[str, Any], str]] = []
        agents_total = 0
        final_states: List[str] = []
        events_emitted = False

        with self.ledger.tx() as conn:
            rows = conn.execute("SELECT * FROM agents").fetchall()
            agents_total = len(rows)
            pending_map = {
                r["agent_id"]: int(r["cnt"])
                for r in conn.execute(
                    "SELECT agent_id, COUNT(*) AS cnt FROM messages WHERE direction = 'to_agent' "
                    "AND kind IN ('task', 'steer', 'interrupt', 'question', 'review', 'intervention') "
                    "AND delivery_state IN ('queued', 'offered', 'acked') GROUP BY agent_id"
                ).fetchall()
            }
            resume_cancel_map = {
                r["agent_id"]: int(r["cnt"])
                for r in conn.execute(
                    "SELECT agent_id, COUNT(*) AS cnt FROM messages WHERE direction = 'to_agent' "
                    "AND kind IN ('resume', 'cancel') AND delivery_state IN ('queued', 'offered') "
                    "GROUP BY agent_id"
                ).fetchall()
            }
            for raw in rows:
                ag = dict(raw)
                agent_id = ag["agent_id"]
                state = ag["state"]
                prev_state = state
                pending = pending_map.get(agent_id, 0)
                resume_cancel = resume_cancel_map.get(agent_id, 0)

                if state == "paused" and resume_cancel == 0:
                    final_states.append(state)
                    continue
                if state == "completed" and pending == 0:
                    final_states.append(state)
                    continue
                if int(ag["active_poll_count"]) > 0:
                    final_states.append(state)
                    continue

                lease_exp = float(ag["lease_expires_at"])
                overdue = now_ts > lease_exp
                if not overdue:
                    final_states.append(state)
                    continue

                # Lease expired while no poll is open (or new Task queued for completed agent).
                if overdue and state in ("registered", "working", "waiting", "blocked", "paused", "spinning", "completed"):
                    stalled_since = ag["stalled_since"] or now_ts
                    conn.execute(
                        "UPDATE agents SET state = 'stalled', stalled_since = ?, updated_at = ? WHERE agent_id = ?",
                        (stalled_since, now_ts, agent_id),
                    )
                    self.ledger.emit_event_locked(
                        conn,
                        agent_id,
                        "lease_expired",
                        {"prev_state": state, "overdue_s": round(now_ts - lease_exp, 2)},
                        now_ts,
                    )
                    events_emitted = True
                    state = "stalled"
                    ag["state"] = "stalled"
                    ag["stalled_since"] = stalled_since

                if overdue and now_ts > lease_exp + dead_grace_s and state in ("stalled", "disconnected"):
                    conn.execute(
                        "UPDATE agents SET state = 'dead', updated_at = ? WHERE agent_id = ?",
                        (now_ts, agent_id),
                    )
                    self.ledger.emit_event_locked(
                        conn,
                        agent_id,
                        "agent_dead",
                        {"dead_after_s": round(now_ts - lease_exp, 2)},
                        now_ts,
                    )
                    events_emitted = True
                    state = "dead"
                    ag["state"] = "dead"

                final_states.append(state)
                has_unfinished = (
                    pending > 0
                    or bool(ag.get("task"))
                    or prev_state in ("working", "spinning")
                    or (
                        prev_state in ("stalled", "dead")
                        and (int(ag["wake_count"]) > 0 or ag["last_wake_at"] is not None)
                    )
                )
                if not has_unfinished:
                    continue

                wake_count = int(ag["wake_count"])
                last_wake = float(ag["last_wake_at"] or 0.0)
                due_wake_wait = wake_backoff_s * (2 ** wake_count)
                if self.wake_cmd and wake_count < max_wakes and (ag["last_wake_at"] is None or now_ts - last_wake >= due_wake_wait):
                    conn.execute(
                        "UPDATE agents SET wake_count = ?, last_wake_at = ?, updated_at = ? WHERE agent_id = ?",
                        (wake_count + 1, now_ts, now_ts, agent_id),
                    )
                    wakes_to_run.append((ag, f"WATCHDOG_{state.upper()}"))

                stalled_at = float(ag["stalled_since"] or lease_exp)
                if (
                    self.notify_cmd
                    and ag["notified_dead_at"] is None
                    and now_ts - stalled_at >= notify_after_s
                ):
                    conn.execute(
                        "UPDATE agents SET notified_dead_at = ?, updated_at = ? WHERE agent_id = ?",
                        (now_ts, now_ts, agent_id),
                    )
                    notifies_to_run.append((ag, f"AGENT_{state.upper()}"))

        agents_alive = sum(1 for st in final_states if st not in ("stalled", "dead", "disconnected"))

        for ag, why in wakes_to_run:
            self._spawn_cmd_hook(
                self.wake_cmd or [],
                {
                    "agent": ag["agent_id"],
                    "project": ag.get("project") or "default",
                    "id": why,
                    "dot": ag.get("dot") or "-",
                    "task": ag.get("task") or "-",
                    "state": ag["state"],
                    "why": why,
                    "age": str(round(max(0.0, now_ts - float(ag["last_seen_at"])))),
                    "hub_url": self.http_url,
                },
                "wake_triggered",
                ag["agent_id"],
            )
        for ag, why in notifies_to_run:
            self._spawn_cmd_hook(
                self.notify_cmd or [],
                {
                    "agent": ag["agent_id"],
                    "project": ag.get("project") or "default",
                    "id": why,
                    "dot": ag.get("dot") or "-",
                    "task": ag.get("task") or "-",
                    "state": ag["state"],
                    "why": why,
                    "age": str(round(max(0.0, now_ts - float(ag["last_seen_at"])))),
                    "hub_url": self.http_url,
                },
                "notify_triggered",
                ag["agent_id"],
            )
        if wakes_to_run or notifies_to_run or events_emitted:
            self.wake_hub.wake_events()

        hb_path = os.path.join(self.state_dir, "heartbeat.json")
        with contextlib.suppress(OSError):
            write_private_atomic(
                hb_path,
                json.dumps(
                    {
                        "t": iso_utc(now_ts),
                        "pid": os.getpid(),
                        "agents_total": agents_total,
                        "agents_alive": agents_alive,
                    }
                )
                + "\n",
            )


# ---------------------------------------------------------------------------
# Steering Text Formatting (Piggybacked into Agent & MCP Responses)
# ---------------------------------------------------------------------------


def format_piggyback_blocks(messages: Sequence[Dict[str, Any]]) -> str:
    if not messages:
        return ""
    interventions = [m for m in messages if m.get("kind") == "intervention"]
    steering = [m for m in messages if m.get("kind") != "intervention"]
    parts: List[str] = []
    if interventions:
        parts.append("--- [HARNESS_INTERVENTION] ---")
        for m in interventions:
            parts.append(
                f"• [{sanitize_terminal(m['id'])}] task={sanitize_terminal(m['task'])} "
                f"(ACK REQUIRED via --ack {sanitize_terminal(m['id'])})\n  {sanitize_terminal(m['text'])}"
            )
    if steering:
        parts.append(f"--- [STEERING_INPUT] ({len(steering)} message(s); ack IDs on next call) ---")
        for m in steering:
            retry_tag = f" (re-offer #{m['offer_count']})" if m.get("offer_count", 1) > 1 else ""
            intr_tag = " [INTERRUPT]" if m.get("interrupt") else ""
            re_tag = f" re={sanitize_terminal(m['re_id'])}" if m.get("re_id") else ""
            refs = f" refs={sanitize_terminal(','.join(m['refs']))}" if m.get("refs") else ""
            parts.append(
                f"• [{sanitize_terminal(m['id'])}] kind={sanitize_terminal(m['kind'])}{intr_tag}{retry_tag} "
                f"project={sanitize_terminal(m['project'])} task={sanitize_terminal(m['task'])}{re_tag}{refs}\n"
                f"  {sanitize_terminal(m['text'])}"
            )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# MCP Dispatchers (Reverse Streamable-HTTP for Dots Sub-Agents & Stdio for Local Session)
# ---------------------------------------------------------------------------


def reverse_mcp_tools() -> List[Dict[str, Any]]:
    return [
        {
            "name": "subdots_register",
            "description": "Register or re-attach this Dots sub-agent to the local Reverse Bridge Hub and obtain a fencing epoch/session_token plus any pending steering messages.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "dot": {"type": "string"},
                    "project": {"type": "string"},
                    "role": {"type": "string"},
                    "host": {"type": "string"},
                    "model": {"type": "string"},
                    "lease_ttl_s": {"type": "number"},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
        {
            "name": "subdots_wait",
            "description": "Hold a bounded wait (max 25s per MCP call) for direct steering from the local session; repeats cleanly without tripping spin detection.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "epoch", "session_token"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "epoch": {"type": "integer"},
                    "session_token": {"type": "string"},
                    "hold_s": {"type": "number"},
                    "ack_ids": {"type": "array", "items": {"type": "string"}},
                    "expect_re": {"type": "string"},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
        {
            "name": "subdots_step",
            "description": "Report live progress (start|step|blocked|done) and receive any new local steering messages piggybacked first.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "epoch", "session_token", "state", "task", "note", "evidence"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "epoch": {"type": "integer"},
                    "session_token": {"type": "string"},
                    "state": {"type": "string", "enum": list(LIVE_STATES)},
                    "task": {"type": "string"},
                    "note": {"type": "string"},
                    "evidence": {"type": "string"},
                    "host": {"type": "string"},
                    "goal": {"type": "string"},
                    "doing": {"type": "string"},
                    "next_step": {"type": "string"},
                    "decisions": {"type": "array", "items": {"type": "string"}},
                    "blockers": {"type": "array", "items": {"type": "string"}},
                    "unblock": {"type": "boolean"},
                    "ack_ids": {"type": "array", "items": {"type": "string"}},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
        {
            "name": "subdots_reply",
            "description": "Send a reply (ack|answer|question|result|block|note) directly to the local session, marking any --re steering message answered.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "epoch", "session_token", "kind", "task", "text"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "epoch": {"type": "integer"},
                    "session_token": {"type": "string"},
                    "kind": {"type": "string", "enum": list(TO_LOCAL_KINDS)},
                    "task": {"type": "string"},
                    "text": {"type": "string"},
                    "re_id": {"type": "string"},
                    "refs": {"type": "array", "items": {"type": "string"}},
                    "ack_ids": {"type": "array", "items": {"type": "string"}},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
        {
            "name": "subdots_ack",
            "description": "Acknowledge receipt of offered steering message IDs so they are not re-offered.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "epoch", "session_token", "ack_ids"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "epoch": {"type": "integer"},
                    "session_token": {"type": "string"},
                    "ack_ids": {"type": "array", "items": {"type": "string"}},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_heartbeat",
            "description": "Renew lease TTL and pull any queued local steering.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "epoch", "session_token"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "epoch": {"type": "integer"},
                    "session_token": {"type": "string"},
                    "ack_ids": {"type": "array", "items": {"type": "string"}},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
    ]


def handle_reverse_mcp_rpc(
    core: HubCore,
    req: Any,
    bound_agent_id: Optional[str] = None,
    is_disconnected: Optional[Callable[[], bool]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(req, dict) or req.get("jsonrpc") != "2.0":
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid JSON-RPC Request"}}
    req_id = req.get("id")
    method = req.get("method")
    raw_params = req.get("params")
    if raw_params is not None and not isinstance(raw_params, dict):
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "Invalid params: object expected"}}
    params = raw_params or {}

    if req_id is None:
        return None
    if method == "initialize":
        proto = params.get("protocolVersion")
        chosen = proto if isinstance(proto, str) and proto in ("2024-11-05", "2025-03-26") else MCP_PROTOCOL_VERSION
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": chosen,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "dotomation-subdots-reverse-mcp", "version": VERSION},
            },
        }
    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": reverse_mcp_tools()}}
    if method == "tools/call":
        name = params.get("name")
        raw_args = params.get("arguments")
        if raw_args is not None and not isinstance(raw_args, dict):
            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "Invalid arguments: object expected"}}
        args = raw_args or {}
        try:
            if name == "subdots_register":
                res = core.agent_register(args, bound_agent_id=bound_agent_id)
            elif name == "subdots_wait":
                bounded = dict(args)
                raw_hold = bounded.get("hold_s")
                bounded["hold_s"] = validate_finite_float(
                    "hold_s",
                    20.0 if raw_hold is None else raw_hold,
                    minimum=0.0,
                    maximum=25.0,
                )
                res = core.agent_poll(bounded, is_disconnected=is_disconnected, bound_agent_id=bound_agent_id)
            elif name == "subdots_step":
                res = core.agent_step(args, bound_agent_id=bound_agent_id)
            elif name == "subdots_reply":
                res = core.agent_reply(args, bound_agent_id=bound_agent_id)
            elif name == "subdots_ack":
                res = core.agent_ack(args, bound_agent_id=bound_agent_id)
            elif name == "subdots_heartbeat":
                res = core.agent_heartbeat(args, bound_agent_id=bound_agent_id)
            else:
                return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown tool: {name}"}}

            content: List[Dict[str, Any]] = []
            piggyback = format_piggyback_blocks(res.get("messages") or [])
            if piggyback:
                content.append({"type": "text", "text": piggyback})
            content.append({"type": "text", "text": json.dumps(res, ensure_ascii=False)})
            return {"jsonrpc": "2.0", "id": req_id, "result": {"content": content, "isError": False}}
        except (HubError, ValueError, TypeError) as exc:
            if isinstance(exc, HubError):
                extra_txt = f" ({json.dumps(exc.extra, ensure_ascii=False)})" if exc.extra else ""
                err_msg = f"[{exc.code}] {exc.message}{extra_txt}"
            else:
                err_msg = str(exc)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": f"ERROR: {err_msg}"}], "isError": True},
            }
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}


# ---------------------------------------------------------------------------
# HTTP Server (Loopback + Token Path/Bearer + Reverse Streamable-HTTP MCP)
# ---------------------------------------------------------------------------


def _extract_host_only(raw_host: str) -> str:
    h = raw_host.strip().lower()
    if not h:
        return ""
    if h.startswith("["):
        end = h.find("]")
        if end == -1:
            return ""
        rest = h[end + 1 :]
        if rest:
            if not rest.startswith(":"):
                return ""
            port_s = rest[1:]
            if not port_s.isascii() or not port_s.isdigit():
                return ""
            port_num = int(port_s)
            if port_num < 1 or port_num > 65535:
                return ""
        return h[: end + 1]
    if h.count(":") == 1:
        name, port_s = h.split(":", 1)
        if not port_s.isascii() or not port_s.isdigit():
            return ""
        port_num = int(port_s)
        if port_num < 1 or port_num > 65535:
            return ""
        return name
    if ":" in h:
        return ""
    return h


def _validate_public_host(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    cleaned = raw.strip().lower()
    if not cleaned:
        return None
    if any(c in cleaned for c in ("\x00", "\r", "\n", "\t", " ", "/", "?", "#", "@")):
        raise ValueError("public-host must be a valid host[:port] without scheme, path, or userinfo")
    bare = _extract_host_only(cleaned)
    if not bare or not cleaned.isascii():
        raise ValueError(f"invalid public-host: {raw!r}")
    return cleaned


_CHUNK_SIZE_RE = re.compile(rb"^[0-9A-Fa-f]{1,8}$")
_CL_RE = re.compile(r"^[0-9]+$")
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]{1,128}$")
MAX_HTTP_HEAD_BYTES = 16384
MAX_HTTP_HEADERS = 48
HTTP_HEAD_TIMEOUT_S = 10.0


class SubdotsHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: Tuple[str, int], core: HubCore, token: str, stop_cb: Any) -> None:
        self.core = core
        self.token = token
        self.stop_cb = stop_cb
        self._conn_sem = threading.BoundedSemaphore(64)
        super().__init__(server_address, SubdotsHTTPHandler)

    def handle_error(self, request: Any, client_address: Any) -> None:
        return

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._conn_sem.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._conn_sem.release()
            self.shutdown_request(request)

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._conn_sem.release()


class SubdotsHTTPHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "subdots"
    sys_version = ""
    timeout = 15.0

    def setup(self) -> None:
        super().setup()
        self._body_buf = bytearray()

    def log_message(self, format: str, *args: Any) -> None:
        return  # Keep stdout/stderr clean; events live in SQLite.

    def log_request(self, code: Any = "-", size: Any = "-") -> None:
        return  # Avoid reading requestline during early error responses.

    def handle_expect_100(self) -> bool:
        self._send_json(417, {"error": "Expect header not supported", "code": "EXPECTATION_FAILED"}, close=True)
        return False

    def send_error(self, code: int, message: Optional[str] = None, explain: Optional[str] = None) -> None:
        self._send_json(code, {"error": message or "http error", "code": f"HTTP_{code}"}, close=True)

    def _peer_disconnected(self) -> bool:
        sock = getattr(self, "connection", None)
        if sock is None:
            return True
        try:
            r, _, _ = select.select([sock], [], [], 0.0)
            if not r:
                return False
            flags = socket.MSG_PEEK | getattr(socket, "MSG_DONTWAIT", 0)
            prev_t = sock.gettimeout()
            try:
                sock.settimeout(0.0)
                peeked = sock.recv(1, flags)
            finally:
                with contextlib.suppress(OSError):
                    sock.settimeout(prev_t)
            return peeked == b""
        except BlockingIOError:
            return False
        except OSError:
            return True

    def _send_json(self, status: int, obj: Any, close: bool = True) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _send_empty(self, status: int) -> None:
        self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.flush()

    def _read_http_head(self) -> bool:
        """Read HTTP/1.1 request line + headers under a strict total deadline and byte/header caps."""
        self.raw_requestline = b""
        self.requestline = ""
        self.command = ""
        self.path = "/"
        self.request_version = "HTTP/1.1"
        self.headers = http.client.HTTPMessage()
        deadline = time.monotonic() + HTTP_HEAD_TIMEOUT_S
        buf = self._body_buf
        try:
            while True:
                idx = buf.find(b"\r\n\r\n")
                if idx != -1:
                    head_bytes = bytes(buf[:idx])
                    del buf[: idx + 4]
                    break
                if len(buf) > MAX_HTTP_HEAD_BYTES:
                    self._send_json(431, {"error": "request headers too large", "code": "HEADER_TOO_LARGE"}, close=True)
                    return False
                rem = deadline - time.monotonic()
                if rem <= 0:
                    self._send_json(408, {"error": "request header timeout", "code": "REQUEST_TIMEOUT"}, close=True)
                    return False
                self.connection.settimeout(min(5.0, rem))
                chunk = self.connection.recv(min(4096, MAX_HTTP_HEAD_BYTES + 4 - len(buf)))
                if not chunk:
                    if not buf:
                        self.close_connection = True
                        return False
                    self._send_json(400, {"error": "incomplete HTTP headers", "code": "INVALID_INPUT"}, close=True)
                    return False
                buf.extend(chunk)
        except (TimeoutError, socket.timeout):
            self._send_json(408, {"error": "request header timeout", "code": "REQUEST_TIMEOUT"}, close=True)
            return False
        except OSError:
            self.close_connection = True
            return False
        finally:
            with contextlib.suppress(OSError):
                self.connection.settimeout(15.0)

        if len(head_bytes) > MAX_HTTP_HEAD_BYTES:
            self._send_json(431, {"error": "request headers too large", "code": "HEADER_TOO_LARGE"}, close=True)
            return False
        lines = head_bytes.split(b"\r\n")
        if not lines or any(b"\r" in ln or b"\n" in ln or b"\x00" in ln for ln in lines):
            self._send_json(400, {"error": "malformed HTTP header line", "code": "INVALID_INPUT"}, close=True)
            return False
        req_line = lines[0].decode("latin-1")
        self.raw_requestline = lines[0]
        self.requestline = req_line
        req_parts = req_line.split(" ")
        if len(req_parts) != 3 or any(not p for p in req_parts):
            self._send_json(400, {"error": "malformed request line", "code": "INVALID_INPUT"}, close=True)
            return False
        method, target, version = req_parts
        if version not in ("HTTP/1.1", "HTTP/1.0"):
            self._send_json(505, {"error": "unsupported HTTP version", "code": "HTTP_VERSION_NOT_SUPPORTED"}, close=True)
            return False
        if len(lines) - 1 > MAX_HTTP_HEADERS:
            self._send_json(431, {"error": "too many HTTP headers", "code": "HEADER_TOO_LARGE"}, close=True)
            return False
        headers = http.client.HTTPMessage()
        for raw_h in lines[1:]:
            if not raw_h:
                continue
            if raw_h[:1] in (b" ", b"\t"):
                self._send_json(400, {"error": "obs-fold headers disallowed", "code": "INVALID_INPUT"}, close=True)
                return False
            if b":" not in raw_h:
                self._send_json(400, {"error": "malformed header", "code": "INVALID_INPUT"}, close=True)
                return False
            name_b, val_b = raw_h.split(b":", 1)
            name = name_b.decode("latin-1")
            if not _HEADER_NAME_RE.fullmatch(name):
                self._send_json(400, {"error": "invalid header name", "code": "INVALID_INPUT"}, close=True)
                return False
            headers[name] = val_b.decode("latin-1").strip(" \t")
        self.command = method
        self.path = target
        self.request_version = version
        self.headers = headers
        self.close_connection = True
        if headers.get_all("Expect"):
            self._send_json(417, {"error": "Expect header not supported", "code": "EXPECTATION_FAILED"}, close=True)
            return False
        return True

    def handle_one_request(self) -> None:
        self.close_connection = True
        try:
            if not self._read_http_head():
                return
            if self.command == "GET":
                self.do_GET()
            elif self.command == "POST":
                self.do_POST()
            elif self.command == "DELETE":
                self.do_DELETE()
            else:
                ctx = self._check_security_and_route()
                if ctx is not None:
                    self._send_json(405, {"error": "method not allowed", "code": "METHOD_NOT_ALLOWED"}, close=True)
        except (BrokenPipeError, ConnectionResetError, TimeoutError, socket.timeout, OSError):
            self.close_connection = True

    def _public_host_bare(self) -> Optional[str]:
        pub = self.server.core.public_host
        if not pub:
            return None
        extracted = _extract_host_only(pub)
        return extracted or pub.strip().lower()

    def _allowed_host(self) -> bool:
        host_vals = self.headers.get_all("Host") or []
        if len(host_vals) != 1:
            return False
        host = host_vals[0].strip().lower()
        hostname = _extract_host_only(host)
        if not hostname:
            return False
        allowed = {"127.0.0.1", "localhost", "[::1]"}
        pub = self._public_host_bare()
        if pub:
            allowed.add(pub)
        return hostname in allowed

    def _allowed_origin(self) -> bool:
        origin_vals = self.headers.get_all("Origin") or []
        if len(origin_vals) > 1:
            return False
        origin = origin_vals[0].strip() if origin_vals else ""
        if not origin:
            return True
        try:
            parsed = urllib.parse.urlsplit(origin)
            parsed.port
        except ValueError:
            return False
        if parsed.scheme not in ("http", "https"):
            return False
        if parsed.username is not None or parsed.password is not None:
            return False
        if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            return False
        host = (parsed.hostname or "").lower()
        allowed = {"127.0.0.1", "localhost", "::1"}
        pub = self._public_host_bare()
        if pub:
            allowed.add(pub.strip("[]"))
        return host in allowed

    def _effective_mcp_resource_url(self, scope_prefix: str = "") -> str:
        pub = self._public_host_bare()
        raw_host = (self.headers.get("Host") or "").strip()
        if pub and _extract_host_only(raw_host) == pub and self.server.core.public_host:
            suffix = f"/{scope_prefix}" if scope_prefix else f"/{self.server.token}"
            return f"https://{self.server.core.public_host}{suffix}/mcp"
        if scope_prefix:
            base_origin = self.server.core.http_url
            if self.server.token and base_origin.endswith("/" + self.server.token):
                base_origin = base_origin[: -(len(self.server.token) + 1)]
            return f"{base_origin}/{scope_prefix}/mcp"
        return self.server.core.mcp_url

    def _check_security_and_route(self) -> Optional[Tuple[str, Optional[str], str]]:
        if not self._allowed_host():
            self._send_json(403, {"error": "forbidden host", "code": "FORBIDDEN_HOST"}, close=True)
            return None
        if not self._allowed_origin():
            self._send_json(403, {"error": "forbidden origin", "code": "FORBIDDEN_ORIGIN"}, close=True)
            return None
        if not self.path.startswith("/") or self.path.startswith("//"):
            self._send_json(400, {"error": "request-target must start with single '/'", "code": "INVALID_INPUT"}, close=True)
            return None
        auth_vals = self.headers.get_all("Authorization") or []
        if len(auth_vals) > 1:
            self._send_json(400, {"error": "duplicate Authorization header", "code": "INVALID_INPUT"}, close=True)
            return None

        path = urllib.parse.urlsplit(self.path).path
        prm_global = f"/.well-known/oauth-protected-resource/{self.server.token}/mcp"
        if hmac.compare_digest(path.encode("utf-8"), prm_global.encode("utf-8")):
            return "__PRM__", None, ""
        if path.startswith("/.well-known/oauth-protected-resource/a/") and path.endswith("/mcp"):
            candidate_tok = path[len("/.well-known/oauth-protected-resource/a/") : -len("/mcp")]
            scoped_aid = self.server.core.lookup_agent_by_token(candidate_tok)
            if scoped_aid is not None:
                return "__PRM__", scoped_aid, f"a/{candidate_tok}"

        parts = [p for p in path.split("/") if p]
        bearer = ""
        auth = auth_vals[0] if auth_vals else ""
        if auth.lower().startswith("bearer "):
            bearer = auth[7:].strip()

        tok_b = self.server.token.encode("utf-8")
        if parts and hmac.compare_digest(parts[0].encode("utf-8"), tok_b):
            return "/" + "/".join(parts[1:]), None, ""
        if len(parts) >= 2 and parts[0] == "a":
            scoped_aid = self.server.core.lookup_agent_by_token(parts[1])
            if scoped_aid is not None:
                return "/" + "/".join(parts[2:]), scoped_aid, f"a/{parts[1]}"
        if bearer and hmac.compare_digest(bearer.encode("utf-8"), tok_b):
            return path, None, ""
        if bearer:
            b_tok = bearer[2:] if bearer.startswith("a:") else bearer
            scoped_aid = self.server.core.lookup_agent_by_token(b_tok)
            if scoped_aid is not None:
                return path, scoped_aid, f"a/{b_tok}"
        self._send_json(401, {"error": "unauthorized", "code": "UNAUTHORIZED"}, close=True)
        return None

    def _read_exact_deadline(self, n: int, deadline: float, timeout_msg: str, trunc_msg: str) -> Optional[bytes]:
        buf = bytearray()
        if self._body_buf:
            take = min(n, len(self._body_buf))
            buf.extend(self._body_buf[:take])
            del self._body_buf[:take]
        while len(buf) < n:
            rem = deadline - time.monotonic()
            if rem <= 0:
                self._send_json(408, {"error": timeout_msg, "code": "REQUEST_TIMEOUT"}, close=True)
                return None
            self.connection.settimeout(min(5.0, rem))
            try:
                chunk = self.connection.recv(min(65536, n - len(buf)))
            except (TimeoutError, socket.timeout):
                continue
            if not chunk:
                self._send_json(400, {"error": trunc_msg, "code": "INVALID_INPUT"}, close=True)
                return None
            buf.extend(chunk)
        return bytes(buf)

    def _readline_deadline(self, max_len: int, deadline: float, timeout_msg: str, invalid_msg: str) -> Optional[bytes]:
        while True:
            idx = self._body_buf.find(b"\r\n")
            if idx != -1:
                if idx + 2 > max_len:
                    self._send_json(400, {"error": invalid_msg, "code": "INVALID_INPUT"}, close=True)
                    return None
                line = bytes(self._body_buf[: idx + 2])
                del self._body_buf[: idx + 2]
                return line
            if len(self._body_buf) >= max_len:
                self._send_json(400, {"error": invalid_msg, "code": "INVALID_INPUT"}, close=True)
                return None
            rem = deadline - time.monotonic()
            if rem <= 0:
                self._send_json(408, {"error": timeout_msg, "code": "REQUEST_TIMEOUT"}, close=True)
                return None
            self.connection.settimeout(min(5.0, rem))
            try:
                chunk = self.connection.recv(min(4096, max_len - len(self._body_buf)))
            except (TimeoutError, socket.timeout):
                continue
            if not chunk:
                self._send_json(400, {"error": invalid_msg, "code": "INVALID_INPUT"}, close=True)
                return None
            self._body_buf.extend(chunk)

    def _read_chunked_body(self) -> Optional[bytes]:
        buf = bytearray()
        deadline = time.monotonic() + 30.0
        chunk_count = 0
        try:
            while True:
                chunk_count += 1
                if chunk_count > 1024:
                    self._send_json(400, {"error": "too many chunks", "code": "INVALID_INPUT"}, close=True)
                    return None
                size_line = self._readline_deadline(120, deadline, "chunked body timeout", "invalid chunk size line")
                if size_line is None:
                    return None
                raw_size_line = size_line[:-2]
                if b"\r" in raw_size_line or b"\n" in raw_size_line or b"\x00" in raw_size_line:
                    self._send_json(400, {"error": "malformed chunk size line", "code": "INVALID_INPUT"}, close=True)
                    return None
                size_part = raw_size_line.split(b";", 1)[0].strip(b" \t")
                if not _CHUNK_SIZE_RE.fullmatch(size_part):
                    self._send_json(400, {"error": "invalid chunk length", "code": "INVALID_INPUT"}, close=True)
                    return None
                chunk_len = int(size_part, 16)
                if len(buf) + chunk_len > MAX_HTTP_BODY_BYTES:
                    self._send_json(413, {"error": "payload too large", "code": "PAYLOAD_TOO_LARGE"}, close=True)
                    return None
                if chunk_len == 0:
                    for _ in range(64):
                        trailer = self._readline_deadline(4096, deadline, "chunked trailer timeout", "invalid chunk trailer")
                        if trailer is None:
                            return None
                        if trailer == b"\r\n":
                            return bytes(buf)
                        if b"\r" in trailer[:-2] or b"\n" in trailer[:-2] or b"\x00" in trailer[:-2]:
                            self._send_json(400, {"error": "invalid chunk trailer", "code": "INVALID_INPUT"}, close=True)
                            return None
                    self._send_json(400, {"error": "too many chunk trailers", "code": "INVALID_INPUT"}, close=True)
                    return None
                chunk = self._read_exact_deadline(chunk_len, deadline, "chunked body timeout", "truncated chunk")
                if chunk is None:
                    return None
                crlf = self._read_exact_deadline(2, deadline, "chunked body timeout", "missing chunk CRLF")
                if crlf is None:
                    return None
                if crlf != b"\r\n":
                    self._send_json(400, {"error": "missing chunk CRLF", "code": "INVALID_INPUT"}, close=True)
                    return None
                buf.extend(chunk)
        finally:
            with contextlib.suppress(OSError):
                self.connection.settimeout(15.0)

    def _read_json_body(self) -> Tuple[bool, Any]:
        te_vals = self.headers.get_all("Transfer-Encoding") or []
        cl_vals = self.headers.get_all("Content-Length") or []
        ct_vals = self.headers.get_all("Content-Type") or []
        if len(cl_vals) > 1 or len(ct_vals) > 1:
            self._send_json(400, {"error": "duplicate Content-Length or Content-Type", "code": "INVALID_INPUT"}, close=True)
            return False, None
        if te_vals and cl_vals:
            self._send_json(400, {"error": "conflicting Content-Length and Transfer-Encoding", "code": "INVALID_INPUT"}, close=True)
            return False, None

        raw = b""
        if te_vals:
            encodings = [p.strip().lower() for h in te_vals for p in h.split(",") if p.strip()]
            if encodings != ["chunked"]:
                self._send_json(501, {"error": "unsupported Transfer-Encoding", "code": "UNSUPPORTED_TE"}, close=True)
                return False, None
            chunked = self._read_chunked_body()
            if chunked is None:
                return False, None
            raw = chunked
        elif cl_vals:
            raw_len = cl_vals[0].strip()
            if not _CL_RE.fullmatch(raw_len):
                self._send_json(400, {"error": "invalid content-length", "code": "INVALID_INPUT"}, close=True)
                return False, None
            clen = int(raw_len)
            if clen > MAX_HTTP_BODY_BYTES:
                self._send_json(413, {"error": "payload too large", "code": "PAYLOAD_TOO_LARGE"}, close=True)
                return False, None
            if clen:
                deadline = time.monotonic() + 30.0
                try:
                    data = self._read_exact_deadline(clen, deadline, "body timeout", "truncated body")
                finally:
                    with contextlib.suppress(OSError):
                        self.connection.settimeout(15.0)
                if data is None:
                    return False, None
                raw = data

        if raw:
            ctype = (ct_vals[0] if ct_vals else "").split(";", 1)[0].strip().lower()
            if ctype and ctype != "application/json" and not ctype.endswith("+json"):
                self._send_json(415, {"error": "Content-Type must be application/json", "code": "UNSUPPORTED_MEDIA_TYPE"}, close=True)
                return False, None
            if not ctype and (self.headers.get("Origin") or "").strip():
                self._send_json(415, {"error": "Content-Type application/json required", "code": "UNSUPPORTED_MEDIA_TYPE"}, close=True)
                return False, None

        if not raw:
            return True, {}
        try:
            return True, json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError):
            self._send_json(400, {"error": "invalid json", "code": "INVALID_JSON"}, close=True)
            return False, None

    def _reject_body_headers(self) -> bool:
        if (self.headers.get_all("Content-Length") or []) or (self.headers.get_all("Transfer-Encoding") or []):
            self._send_json(400, {"error": "body framing not allowed on this method", "code": "INVALID_INPUT"}, close=True)
            return True
        return False

    def do_GET(self) -> None:
        ctx = self._check_security_and_route()
        if ctx is None or self._reject_body_headers():
            return
        route, _bound_aid, scope_prefix = ctx
        if route == "__PRM__":
            self._send_json(
                200,
                {
                    "resource": self._effective_mcp_resource_url(scope_prefix),
                    "resource_name": "Dotomation sub-dots Reverse Bridge",
                    "authorization_servers": [],
                },
            )
            return
        if route == "/health":
            self._send_json(200, {"ok": True, "version": VERSION})
            return
        if route == "/mcp":
            self._send_json(405, {"error": "use POST for Streamable-HTTP MCP", "code": "METHOD_NOT_ALLOWED"})
            return
        self._send_json(404, {"error": "not found", "code": "NOT_FOUND"})

    def do_DELETE(self) -> None:
        ctx = self._check_security_and_route()
        if ctx is None or self._reject_body_headers():
            return
        route, _bound_aid, _scope_prefix = ctx
        if route == "/mcp":
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"error": "not found", "code": "NOT_FOUND"})

    def do_POST(self) -> None:
        ctx = self._check_security_and_route()
        if ctx is None:
            return
        route, bound_aid, _scope_prefix = ctx
        ok, body = self._read_json_body()
        if not ok:
            return

        if route == "/mcp":
            try:
                if isinstance(body, list):
                    if not body:
                        self._send_json(
                            400,
                            {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Empty batch"}},
                        )
                        return
                    if len(body) > 20:
                        self._send_json(400, {"error": "JSON-RPC batch exceeds 20 items", "code": "INVALID_INPUT"})
                        return
                    for item in body:
                        if (
                            isinstance(item, dict)
                            and item.get("method") == "tools/call"
                            and isinstance(item.get("params"), dict)
                            and item["params"].get("name") == "subdots_wait"
                        ):
                            self._send_json(
                                400,
                                {
                                    "jsonrpc": "2.0",
                                    "id": item.get("id"),
                                    "error": {"code": -32600, "message": "subdots_wait cannot be used in a batch"},
                                },
                            )
                            return
                    replies = [
                        r
                        for r in (
                            handle_reverse_mcp_rpc(
                                self.server.core,
                                item,
                                bound_agent_id=bound_aid,
                                is_disconnected=self._peer_disconnected,
                            )
                            for item in body
                        )
                        if r is not None
                    ]
                    if not replies:
                        self._send_empty(202)
                    else:
                        self._send_json(200, replies)
                    return
                reply = handle_reverse_mcp_rpc(
                    self.server.core,
                    body,
                    bound_agent_id=bound_aid,
                    is_disconnected=self._peer_disconnected,
                )
                if reply is None:
                    self._send_empty(202)
                else:
                    self._send_json(200, reply)
                return
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.close_connection = True
                return
            except Exception:
                with contextlib.suppress(OSError):
                    self._send_json(
                        500,
                        {"jsonrpc": "2.0", "id": body.get("id") if isinstance(body, dict) else None, "error": {"code": -32603, "message": "Internal error"}},
                    )
                return

        if not isinstance(body, dict):
            self._send_json(400, {"error": "JSON object expected", "code": "INVALID_INPUT"})
            return

        if route == "/agent/poll":
            try:
                result = self.server.core.agent_poll(
                    body,
                    is_disconnected=self._peer_disconnected,
                    bound_agent_id=bound_aid,
                )
            except HubError as exc:
                out = {"error": exc.message, "code": exc.code}
                out.update(exc.extra)
                self._send_json(exc.status, out)
                return
            except (ValueError, TypeError) as exc:
                self._send_json(400, {"error": str(exc), "code": "INVALID_INPUT"})
                return
            except Exception:
                self._send_json(500, {"error": "internal server error", "code": "INTERNAL_ERROR"})
                return
            try:
                self._send_json(200, result)
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.close_connection = True
                self.server.core.rollback_offered_messages(result["agent_id"], result.get("messages") or [])
            return

        routes = {
            "/agent/register": self.server.core.agent_register,
            "/agent/heartbeat": self.server.core.agent_heartbeat,
            "/agent/step": self.server.core.agent_step,
            "/agent/reply": self.server.core.agent_reply,
            "/agent/ack": self.server.core.agent_ack,
            "/agent/exec/start": self.server.core.exec_start,
            "/agent/exec/heartbeat": self.server.core.exec_heartbeat,
            "/agent/exec/finish": self.server.core.exec_finish,
        }
        handler = routes.get(route)
        if handler is None:
            self._send_json(404, {"error": f"unknown route {route}", "code": "NOT_FOUND"})
            return
        try:
            result = handler(body, bound_agent_id=bound_aid)
        except HubError as exc:
            out = {"error": exc.message, "code": exc.code}
            out.update(exc.extra)
            self._send_json(exc.status, out)
            return
        except (ValueError, TypeError) as exc:
            self._send_json(400, {"error": str(exc), "code": "INVALID_INPUT"})
            return
        except Exception:
            self._send_json(500, {"error": "internal server error", "code": "INTERNAL_ERROR"})
            return
        try:
            self._send_json(200, result)
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True
            if isinstance(result, dict) and result.get("messages") and isinstance(body.get("agent_id"), str):
                with contextlib.suppress(Exception):
                    self.server.core.rollback_offered_messages(body["agent_id"], result.get("messages") or [])


# ---------------------------------------------------------------------------
# Unix Control Socket Server & Client
# ---------------------------------------------------------------------------


class ControlSocketServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def __init__(self, socket_path: str, core: HubCore, stop_cb: Any) -> None:
        self.core = core
        self.stop_cb = stop_cb
        self._conn_sem = threading.BoundedSemaphore(64)
        try:
            st = os.lstat(socket_path)
        except FileNotFoundError:
            st = None
        if st is not None:
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISSOCK(st.st_mode):
                raise OSError(f"{socket_path} exists and is not a Unix domain socket")
            if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
                raise OSError(f"{socket_path} owner mismatch")
            os.unlink(socket_path)
        super().__init__(socket_path, ControlSocketHandler)
        os.chmod(socket_path, 0o600)

    def handle_error(self, request: Any, client_address: Any) -> None:
        return

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._conn_sem.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._conn_sem.release()
            self.shutdown_request(request)

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._conn_sem.release()


class ControlSocketHandler(socketserver.StreamRequestHandler):
    timeout = 15.0

    def _write_line(self, obj: Dict[str, Any]) -> bool:
        data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
        try:
            self.wfile.write(data)
            self.wfile.flush()
            return True
        except OSError:
            return False

    def handle(self) -> None:
        hello_done = False
        while not self.server.core.wake_hub.stop_event.is_set():
            try:
                raw = self.rfile.readline(MAX_CONTROL_LINE_BYTES + 1)
            except (TimeoutError, socket.timeout, OSError):
                return
            if not raw:
                return
            if len(raw) > MAX_CONTROL_LINE_BYTES:
                self._write_line({"id": None, "error": {"code": "LINE_TOO_LARGE", "message": "request line too large"}})
                return
            try:
                req = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError, RecursionError):
                self._write_line({"id": None, "error": {"code": "INVALID_JSON", "message": "invalid JSON"}})
                return
            if not isinstance(req, dict):
                self._write_line({"id": None, "error": {"code": "INVALID_INPUT", "message": "object expected"}})
                return
            req_id = req.get("id")
            method = req.get("method")
            params = req.get("params") if isinstance(req.get("params"), dict) else {}

            if not hello_done:
                if method != "hello" or params.get("protocol_version") != PROTOCOL_VERSION:
                    self._write_line(
                        {
                            "id": req_id,
                            "error": {"code": "PROTOCOL_MISMATCH", "message": "expected hello with protocol_version=1"},
                        }
                    )
                    return
                hello_done = True
                if not self._write_line({"id": req_id, "result": {"protocol_version": PROTOCOL_VERSION, "version": VERSION}}):
                    return
                continue

            if method == "events.follow":
                with contextlib.suppress(OSError):
                    self.connection.settimeout(None)
                try:
                    self._stream_events(req_id, params)
                except HubError as exc:
                    self._write_line({"id": req_id, "error": {"code": exc.code, "message": exc.message, **exc.extra}})
                except (ValueError, TypeError) as exc:
                    self._write_line({"id": req_id, "error": {"code": "INVALID_INPUT", "message": str(exc)}})
                finally:
                    with contextlib.suppress(OSError):
                        self.connection.settimeout(15.0)
                return

            if method == "message.await":
                raw_tout = params.get("timeout_s")
                extra_tout = 60.0 if not isinstance(raw_tout, (int, float)) or isinstance(raw_tout, bool) else max(0.0, min(float(raw_tout), 3600.0))
                with contextlib.suppress(OSError):
                    self.connection.settimeout(extra_tout + 15.0)
            try:
                result = self._dispatch(method, params)
                if not self._write_line({"id": req_id, "result": result}):
                    return
                if method == "hub.stop":
                    return
            except HubError as exc:
                if not self._write_line({"id": req_id, "error": {"code": exc.code, "message": exc.message, **exc.extra}}):
                    return
            except (ValueError, TypeError) as exc:
                if not self._write_line({"id": req_id, "error": {"code": "INVALID_INPUT", "message": str(exc)}}):
                    return
            except Exception:
                if not self._write_line({"id": req_id, "error": {"code": "INTERNAL_ERROR", "message": "internal server error"}}):
                    return
            finally:
                with contextlib.suppress(OSError):
                    self.connection.settimeout(15.0)

    def _stream_events(self, req_id: Any, params: Dict[str, Any]) -> None:
        raw_after = params.get("after_seq")
        if raw_after is None:
            after_seq = 0
        elif isinstance(raw_after, bool) or not isinstance(raw_after, int):
            raise ValueError("after_seq must be an integer")
        else:
            after_seq = max(0, raw_after)
        agent_id = params.get("agent_id")
        if agent_id is not None:
            agent_id = validate_token("agent_id", agent_id)
        raw_tout = params.get("timeout_s")
        timeout_s = validate_finite_float(
            "timeout_s",
            30.0 if raw_tout is None else raw_tout,
            minimum=0.0,
            maximum=3600.0,
        )
        deadline = time.monotonic() + timeout_s
        if not self._write_line({"id": req_id, "result": {"streaming": True}}):
            return
        while not self.server.core.wake_hub.stop_event.is_set():
            observed_gen = self.server.core.wake_hub.current_events_gen()
            batch = self.server.core.local_events(agent_id=agent_id, after_seq=after_seq, limit=100)["events"]
            for ev in batch:
                after_seq = max(after_seq, int(ev["seq"]))
                if not self._write_line({"event": ev}):
                    return
            if len(batch) >= 100:
                continue
            rem = deadline - time.monotonic()
            if rem <= 0:
                self._write_line({"done": True, "last_seq": after_seq})
                return
            self.server.core.wake_hub.wait_events(observed_gen, min(rem, 1.0))

    def _dispatch(self, method: Any, params: Dict[str, Any]) -> Any:
        core: HubCore = self.server.core
        if method == "hub.status":
            return {
                "ok": True,
                "version": VERSION,
                "pid": os.getpid(),
                "started_at": core.started_at,
                "state_dir": core.state_dir,
                "http_url": core.http_url,
                "mcp_url": core.mcp_url,
                "ops_base": core.mirror.ops_base,
                "require_provision": core.require_provision,
                "settings": core.settings,
                "agents_count": len(core.local_list_agents()["agents"]),
            }
        if method == "hub.stop":
            threading.Thread(target=self.server.stop_cb, daemon=True).start()
            return {"ok": True, "stopping": True}
        if method == "agent.list":
            return core.local_list_agents()
        if method == "agent.provision":
            return core.local_provision_agent(params)
        if method == "agent.status":
            return core.local_agent_status(params.get("agent_id", ""))
        if method == "message.send":
            return core.local_send(params)
        if method == "message.await":
            return core.local_await_reply(params)
        if method == "message.inbox":
            return core.local_inbox(params.get("agent_id"), bool(params.get("include_read", False)))
        if method == "message.mark_read":
            return core.local_mark_read(params.get("agent_id", ""), params.get("id", ""))
        if method == "events.list":
            raw_after = params.get("after_seq")
            if raw_after is None:
                after_seq = 0
            elif isinstance(raw_after, bool) or not isinstance(raw_after, int):
                raise ValueError("after_seq must be an integer")
            else:
                after_seq = raw_after
            raw_lim = params.get("limit")
            if raw_lim is None:
                limit = 100
            elif isinstance(raw_lim, bool) or not isinstance(raw_lim, int):
                raise ValueError("limit must be an integer")
            else:
                limit = raw_lim
            return core.local_events(
                agent_id=params.get("agent_id"),
                after_seq=after_seq,
                limit=limit,
            )
        if method == "control.wake":
            return core.local_wake(params.get("agent_id", ""))
        if method in ("control.pause", "control.resume", "control.cancel", "control.release"):
            kind = method.split(".", 1)[1]
            p = dict(params)
            p["kind"] = kind
            p.setdefault("task", kind)
            p.setdefault("text", f"Local session issued {kind}")
            return core.local_send(p)
        raise HubError("UNKNOWN_METHOD", f"unknown method {method!r}", status=404)


def control_rpc(
    state_dir: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 120.0,
    cancel_tracker: Optional[Tuple[threading.Lock, List[socket.socket]]] = None,
) -> Any:
    rt = read_runtime(state_dir)
    sock_path = rt["socket_path"]
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        if cancel_tracker is not None:
            lock, active = cancel_tracker
            with lock:
                active.append(sock)
        try:
            sock.settimeout(timeout)
            sock.connect(sock_path)
            fh_w = sock.makefile("w", encoding="utf-8")
            fh_r = sock.makefile("r", encoding="utf-8")
            try:
                fh_w.write(json.dumps({"id": "h", "method": "hello", "params": {"protocol_version": PROTOCOL_VERSION}}) + "\n")
                fh_w.flush()
                hello_line = fh_r.readline(MAX_CONTROL_LINE_BYTES + 1)
                if not hello_line:
                    raise HubError("SOCKET_CLOSED", "hub closed control connection during handshake")
                if len(hello_line) > MAX_CONTROL_LINE_BYTES:
                    raise HubError("RESPONSE_TOO_LARGE", "control handshake exceeded size limit")
                try:
                    hello = json.loads(hello_line)
                except (ValueError, RecursionError) as exc:
                    raise HubError("INVALID_JSON", f"invalid control handshake JSON: {exc}") from exc
                if "error" in hello:
                    raise HubError(hello["error"].get("code", "ERROR"), hello["error"].get("message", "hello failed"))
                fh_w.write(json.dumps({"id": "r", "method": method, "params": params or {}}) + "\n")
                fh_w.flush()
                line = fh_r.readline(MAX_CONTROL_LINE_BYTES + 1)
                if not line:
                    raise HubError("SOCKET_CLOSED", "hub closed control connection")
                if len(line) > MAX_CONTROL_LINE_BYTES:
                    raise HubError("RESPONSE_TOO_LARGE", "control response exceeded size limit")
                try:
                    resp = json.loads(line)
                except (ValueError, RecursionError) as exc:
                    raise HubError("INVALID_JSON", f"invalid control response JSON: {exc}") from exc
                if "error" in resp:
                    err = resp["error"]
                    raise HubError(err.get("code", "ERROR"), err.get("message", "error"), extra=err)
                return resp.get("result")
            finally:
                with contextlib.suppress(OSError):
                    fh_w.close()
                with contextlib.suppress(OSError):
                    fh_r.close()
        finally:
            if cancel_tracker is not None:
                lock, active = cancel_tracker
                with lock:
                    if sock in active:
                        active.remove(sock)


def control_follow_events(
    state_dir: str, agent_id: Optional[str], after_seq: int, timeout_s: float, on_event: Any
) -> int:
    rt = read_runtime(state_dir)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout_s + 5.0)
        sock.connect(rt["socket_path"])
        fh_w = sock.makefile("w", encoding="utf-8")
        fh_r = sock.makefile("r", encoding="utf-8")
        try:
            fh_w.write(json.dumps({"id": "h", "method": "hello", "params": {"protocol_version": PROTOCOL_VERSION}}) + "\n")
            fh_w.flush()
            hello_line = fh_r.readline(MAX_CONTROL_LINE_BYTES + 1)
            if not hello_line:
                raise HubError("SOCKET_CLOSED", "hub closed control connection during handshake")
            try:
                hello = json.loads(hello_line)
            except (ValueError, RecursionError) as exc:
                raise HubError("INVALID_JSON", f"invalid control handshake JSON: {exc}") from exc
            if "error" in hello:
                raise HubError(hello["error"].get("code", "ERROR"), hello["error"].get("message", "hello failed"))
            fh_w.write(
                json.dumps(
                    {
                        "id": "f",
                        "method": "events.follow",
                        "params": {"agent_id": agent_id, "after_seq": after_seq, "timeout_s": timeout_s},
                    }
                )
                + "\n"
            )
            fh_w.flush()
            ack_line = fh_r.readline(MAX_CONTROL_LINE_BYTES + 1)
            if not ack_line:
                raise HubError("SOCKET_CLOSED", "hub closed control connection during events.follow")
            try:
                ack = json.loads(ack_line)
            except (ValueError, RecursionError) as exc:
                raise HubError("INVALID_JSON", f"invalid events.follow ack JSON: {exc}") from exc
            if "error" in ack:
                raise HubError(ack["error"].get("code", "ERROR"), ack["error"].get("message", "follow failed"))
            last_seq = after_seq
            while True:
                line = fh_r.readline(MAX_CONTROL_LINE_BYTES + 1)
                if not line:
                    break
                if len(line) > MAX_CONTROL_LINE_BYTES:
                    raise HubError("RESPONSE_TOO_LARGE", "event line exceeded size limit")
                try:
                    msg = json.loads(line)
                except (ValueError, RecursionError):
                    continue
                if "event" in msg:
                    ev = msg["event"]
                    last_seq = max(last_seq, int(ev["seq"]))
                    on_event(ev)
                elif msg.get("done"):
                    last_seq = max(last_seq, int(msg.get("last_seq", last_seq)))
                    break
            return last_seq
        finally:
            with contextlib.suppress(OSError):
                fh_w.close()
            with contextlib.suppress(OSError):
                fh_r.close()


# ---------------------------------------------------------------------------
# Hub Lifecycle (Foreground Runner & Detached Starter)
# ---------------------------------------------------------------------------


def _acquire_hub_lock(state_dir: str) -> int:
    """Acquire non-blocking exclusive flock on <state_dir>/hub.lock."""
    lock_path = os.path.join(state_dir, "hub.lock")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | nofollow | os.O_NONBLOCK, 0o600)
    try:
        fst = os.fstat(fd)
        if not stat.S_ISREG(fst.st_mode):
            raise OSError(f"{lock_path} is not a regular file")
        if fst.st_nlink != 1:
            raise OSError(f"{lock_path} must not have hardlinks")
        if hasattr(os, "geteuid") and fst.st_uid != os.geteuid():
            raise OSError(f"{lock_path} owner mismatch")
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise HubError(
                "HUB_ALREADY_RUNNING",
                f"another hub already holds lock for {state_dir}",
                status=409,
            ) from exc
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"{os.getpid()}\n".encode("ascii"))
        return fd
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(fd)
        raise


def _cleanup_stale_hub_files(state_dir: str, rt: Dict[str, Any]) -> None:
    """Remove stale runtime.json and c.sock if hub.lock is uncontested."""
    lock_path = os.path.join(state_dir, "hub.lock")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    lock_fd: Optional[int] = None
    try:
        if os.path.lexists(lock_path):
            lock_fd = os.open(lock_path, os.O_RDWR | nofollow | os.O_NONBLOCK)
            fst = os.fstat(lock_fd)
            if not stat.S_ISREG(fst.st_mode) or fst.st_nlink != 1:
                return
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return
    except OSError:
        pass
    try:
        rt_path = runtime_path_for(state_dir)
        with contextlib.suppress(OSError):
            if os.path.lexists(rt_path) and not os.path.islink(rt_path):
                os.unlink(rt_path)
        sock = rt.get("socket_path")
        if isinstance(sock, str) and sock:
            with contextlib.suppress(OSError):
                sst = os.lstat(sock)
                if not stat.S_ISLNK(sst.st_mode) and stat.S_ISSOCK(sst.st_mode):
                    if not hasattr(os, "geteuid") or sst.st_uid == os.geteuid():
                        os.unlink(sock)
            parent_dir = os.path.dirname(sock)
            if os.path.basename(parent_dir).startswith("sd-") and os.path.abspath(parent_dir) != os.path.abspath(state_dir):
                with contextlib.suppress(OSError):
                    os.rmdir(parent_dir)
    finally:
        if lock_fd is not None:
            with contextlib.suppress(OSError):
                os.close(lock_fd)


def run_hub_foreground(a: argparse.Namespace) -> int:
    state_dir = ensure_private_dir(a.state_dir)

    if a.port < 0 or a.port > 65535:
        raise ValueError("port must be in [0, 65535]")
    public_host = _validate_public_host(a.public_host)
    settings = {
        "lease_ttl_s": validate_finite_float("lease-ttl", float(a.lease_ttl), minimum=0.1, maximum=3600.0),
        "dead_grace_s": validate_finite_float("dead-grace", float(a.dead_grace), minimum=0.0, maximum=86400.0),
        "watchdog_interval_s": validate_finite_float("watchdog-interval", float(a.watchdog_interval), minimum=0.05, maximum=3600.0),
        "reoffer_s": validate_finite_float("reoffer-s", float(a.reoffer_s), minimum=0.1, maximum=86400.0),
        "max_offers": max(1, int(a.max_offers)),
        "wake_backoff_s": validate_finite_float("wake-backoff", float(a.wake_backoff), minimum=0.0, maximum=86400.0),
        "max_wakes": max(0, int(a.max_wakes)),
        "notify_after_s": validate_finite_float("notify-after", float(a.notify_after), minimum=0.0, maximum=86400.0),
        "spin_window_s": validate_finite_float("spin-window", float(a.spin_window), minimum=1.0, maximum=86400.0),
        "spin_cmd_repeats": max(1, int(a.spin_cmd_repeats)),
        "spin_step_repeats": max(1, int(a.spin_step_repeats)),
        "spin_poll_burst": max(1, int(a.spin_poll_burst)),
    }
    ops_base = a.ops_base or (os.environ.get("DOTS_OPS_BASE") if a.mirror_ops else None)
    wake_cmd = parse_cmd_argv(a.wake_cmd, "--wake-cmd")
    notify_cmd = parse_cmd_argv(a.notify_cmd, "--notify-cmd")
    token = secrets.token_hex(24)

    lock_fd = _acquire_hub_lock(state_dir)
    old_umask = os.umask(0o077)
    core: Optional[HubCore] = None
    sock_path: Optional[str] = None
    sock_tmp_dir: Optional[str] = None
    rt_path = runtime_path_for(state_dir)
    shutdown_event = threading.Event()
    http_srv: Optional[SubdotsHTTPServer] = None
    ctrl_srv: Optional[ControlSocketServer] = None
    wd_th: Optional[threading.Thread] = None
    previous: Dict[Any, Any] = {}

    try:
        core = HubCore(
            state_dir=state_dir,
            settings=settings,
            ops_base=ops_base,
            wake_cmd=wake_cmd,
            notify_cmd=notify_cmd,
            public_host=public_host,
            require_provision=bool(getattr(a, "require_provision", False)),
        )
        sock_path, sock_tmp_dir = socket_path_for(state_dir)

        def trigger_stop() -> None:
            assert core is not None
            core.wake_hub.stop_all()
            shutdown_event.set()

        core._hub_token = token
        core._remember_secret(token)
        http_srv = SubdotsHTTPServer(("127.0.0.1", int(a.port)), core, token, trigger_stop)
        bound_port = http_srv.server_address[1]
        base_url = f"http://127.0.0.1:{bound_port}/{token}"
        core.http_url = base_url
        core.mcp_url = f"{base_url}/mcp"

        ctrl_srv = ControlSocketServer(sock_path, core, trigger_stop)

        runtime_doc = {
            "v": 1,
            "version": VERSION,
            "pid": os.getpid(),
            "started_at": core.started_at,
            "state_dir": state_dir,
            "socket_path": sock_path,
            "http_host": "127.0.0.1",
            "http_port": bound_port,
            "token": token,
            "http_url": core.http_url,
            "mcp_url": core.mcp_url,
            "public_host": public_host,
            "ops_base": core.mirror.ops_base,
            "require_provision": core.require_provision,
        }
        write_private_atomic(rt_path, json.dumps(runtime_doc, indent=1) + "\n", mode=0o600)

        def _on_signal(signum: int, frame: Any) -> None:
            trigger_stop()

        previous = {s: signal.getsignal(s) for s in SIGNALS}
        for s in SIGNALS:
            signal.signal(s, _on_signal)

        http_th = threading.Thread(target=http_srv.serve_forever, kwargs={"poll_interval": 0.25}, daemon=True)
        ctrl_th = threading.Thread(target=ctrl_srv.serve_forever, kwargs={"poll_interval": 0.25}, daemon=True)

        def _watchdog_runner() -> None:
            assert core is not None
            interval = max(0.05, float(core.settings["watchdog_interval_s"]))
            while not shutdown_event.wait(interval):
                try:
                    core.watchdog_tick()
                except Exception as exc:
                    with contextlib.suppress(Exception):
                        with core.ledger.tx() as conn:
                            core.ledger.emit_event_locked(
                                conn, "-", "watchdog_error", {"detail": core._scrub_hook_text(str(exc))}
                            )

        wd_th = threading.Thread(target=_watchdog_runner, daemon=True)
        http_th.start()
        ctrl_th.start()
        wd_th.start()

        while not shutdown_event.is_set():
            shutdown_event.wait(0.5)
    finally:
        if core is not None:
            core.wake_hub.stop_all()
        if ctrl_srv is not None:
            with contextlib.suppress(Exception):
                ctrl_srv.shutdown()
                ctrl_srv.server_close()
        if http_srv is not None:
            with contextlib.suppress(Exception):
                http_srv.shutdown()
                http_srv.server_close()
        if wd_th is not None:
            wd_th.join(timeout=2.0)
        if core is not None:
            core.close()
        with contextlib.suppress(OSError, ValueError, RecursionError):
            if os.path.exists(rt_path):
                cur_rt = json.loads(read_regular_text(rt_path, 65536) or "{}")
                if isinstance(cur_rt, dict) and cur_rt.get("pid") == os.getpid():
                    os.unlink(rt_path)
        if ctrl_srv is not None and sock_path:
            with contextlib.suppress(OSError):
                os.unlink(sock_path)
        if sock_tmp_dir:
            with contextlib.suppress(OSError):
                os.rmdir(sock_tmp_dir)
        for s, h in previous.items():
            signal.signal(s, h)
        os.umask(old_umask)
        with contextlib.suppress(OSError):
            os.close(lock_fd)
    return 0


def cmd_hub_start(a: argparse.Namespace) -> int:
    state_dir = ensure_private_dir(a.state_dir)

    with contextlib.suppress(Exception):
        existing = control_rpc(state_dir, "hub.status", timeout=2.0)
        if not getattr(a, "show_token", False):
            existing["http_url"] = re.sub(r"/[0-9a-f]{48}$", "/<token>", existing["http_url"])
            existing["mcp_url"] = re.sub(r"/[0-9a-f]{48}/mcp$", "/<token>/mcp", existing["mcp_url"])
        if a.json:
            print(json.dumps({"already_running": True, **existing}, ensure_ascii=False))
        else:
            print(f"HUB already running pid={existing['pid']} http={existing['http_url']} state_dir={state_dir}")
        return 0

    if a.foreground:
        return run_hub_foreground(a)

    cmd = [sys.executable, os.path.abspath(__file__), "hub", "start", "--foreground", "--state-dir", state_dir]
    cmd += ["--port", str(a.port)]
    if a.ops_base:
        cmd += ["--ops-base", a.ops_base]
    if a.mirror_ops:
        cmd.append("--mirror-ops")
    if a.public_host:
        cmd += ["--public-host", a.public_host]
    if getattr(a, "require_provision", False):
        cmd.append("--require-provision")
    if a.wake_cmd is not None:
        cmd += ["--wake-cmd", a.wake_cmd]
    if a.notify_cmd is not None:
        cmd += ["--notify-cmd", a.notify_cmd]
    for flag, val in (
        ("--lease-ttl", a.lease_ttl),
        ("--dead-grace", a.dead_grace),
        ("--watchdog-interval", a.watchdog_interval),
        ("--reoffer-s", a.reoffer_s),
        ("--max-offers", a.max_offers),
        ("--wake-backoff", a.wake_backoff),
        ("--max-wakes", a.max_wakes),
        ("--notify-after", a.notify_after),
        ("--spin-window", a.spin_window),
        ("--spin-cmd-repeats", a.spin_cmd_repeats),
        ("--spin-step-repeats", a.spin_step_repeats),
        ("--spin-poll-burst", a.spin_poll_burst),
    ):
        cmd += [flag, str(val)]

    log_path = os.path.join(state_dir, "hub.log")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | nofollow | os.O_NONBLOCK, 0o600)
    opened = False
    try:
        fst = os.fstat(log_fd)
        if not stat.S_ISREG(fst.st_mode):
            raise OSError(f"{log_path} is not a regular file")
        if fst.st_nlink != 1:
            raise OSError(f"{log_path} must not have hardlinks")
        if hasattr(os, "geteuid") and fst.st_uid != os.geteuid():
            raise OSError(f"{log_path} owner mismatch")
        if hasattr(os, "fchmod"):
            os.fchmod(log_fd, 0o600)
        else:
            os.chmod(log_path, 0o600)
        log_fh = os.fdopen(log_fd, "ab")
        opened = True
    except BaseException:
        if not opened:
            with contextlib.suppress(OSError):
                os.close(log_fd)
        raise
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=log_fh,
            stderr=log_fh,
            start_new_session=True,
        )
    finally:
        log_fh.close()

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            die(f"hub exited prematurely with code {proc.returncode}; check {log_path}", code=3)
        try:
            st = control_rpc(state_dir, "hub.status", timeout=1.5)
            if not getattr(a, "show_token", False):
                st["http_url"] = re.sub(r"/[0-9a-f]{48}$", "/<token>", st["http_url"])
                st["mcp_url"] = re.sub(r"/[0-9a-f]{48}/mcp$", "/<token>/mcp", st["mcp_url"])
            if a.json:
                print(json.dumps(st, ensure_ascii=False))
            else:
                print(f"HUB started pid={st['pid']} state_dir={state_dir}")
                print(f"  http_url={st['http_url']}")
                print(f"  mcp_url={st['mcp_url']}")
            return 0
        except Exception:
            time.sleep(0.05)
    with contextlib.suppress(ProcessLookupError, OSError):
        proc.terminate()
    with contextlib.suppress(subprocess.SubprocessError):
        proc.wait(timeout=1.0)
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError, OSError):
            proc.kill()
        with contextlib.suppress(subprocess.SubprocessError):
            proc.wait(timeout=1.0)
    die("timed out waiting for hub startup", code=3)
    return 3


def cmd_hub_status(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    try:
        st = control_rpc(state_dir, "hub.status", timeout=5.0)
    except Exception as exc:
        if a.json:
            print(json.dumps({"running": False, "error": str(exc)}, ensure_ascii=False))
        else:
            print(f"HUB stopped ({exc})")
        return 3
    if not a.show_token:
        st["http_url"] = re.sub(r"/[0-9a-f]{48}$", "/<token>", st["http_url"])
        st["mcp_url"] = re.sub(r"/[0-9a-f]{48}/mcp$", "/<token>/mcp", st["mcp_url"])
    if a.json:
        print(json.dumps({"running": True, **st}, ensure_ascii=False))
    else:
        print(f"HUB running pid={st['pid']} agents={st['agents_count']} http={st['http_url']}")
    return 0


def _pid_matches_subdots_hub(pid: int, state_dir: str) -> bool:
    if pid <= 0 or pid == os.getpid() or not pid_alive(pid):
        return False
    try:
        proc = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "uid=,command="],
            capture_output=True,
            text=True,
            timeout=2.0,
        )
        raw_out = proc.stdout.strip()
        if proc.returncode != 0 or not raw_out:
            return False
        uid_s, sep, cmd_line = raw_out.partition(" ")
        if not sep:
            return False
        if hasattr(os, "geteuid") and int(uid_s) != os.geteuid():
            return False
        if not re.search(r"(?:^|[/\s])subdots\.py(?:\s|$)", cmd_line):
            return False
        if not re.search(r"\bhub\s+start\b", cmd_line) or not re.search(r"(?:^|\s)--foreground(?:\s|$)", cmd_line):
            return False
        candidates = {os.path.abspath(os.path.expanduser(state_dir))}
        with contextlib.suppress(OSError):
            candidates.add(resolve_state_dir(state_dir))
        if "--state-dir" in cmd_line:
            for cand in candidates:
                pattern = r"(?:^|\s)--state-dir(?:=|\s+)" + re.escape(cand) + r"(?=\s+--|\s*$)"
                if re.search(pattern, cmd_line):
                    return True
            return False
        # Direct foreground invocation with implicit default state_dir
        try:
            return resolve_state_dir(state_dir) == resolve_state_dir(default_state_dir())
        except OSError:
            return False
    except (OSError, ValueError, subprocess.SubprocessError):
        return False


def cmd_hub_stop(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    try:
        rt = read_runtime(state_dir)
    except Exception as exc:
        print(f"HUB not running ({exc})")
        return 0
    pid = int(rt.get("pid") or 0)
    started_at = rt.get("started_at")
    if not pid_alive(pid):
        _cleanup_stale_hub_files(state_dir, rt)
        if a.json:
            print(json.dumps({"ok": True, "pid": pid, "stale_cleaned": True}))
        else:
            print(f"HUB stale runtime cleaned pid={pid}")
        return 0
    with contextlib.suppress(Exception):
        control_rpc(state_dir, "hub.stop", timeout=3.0)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and pid_alive(pid):
        time.sleep(0.05)
    if pid_alive(pid):
        cur_rt: Optional[Dict[str, Any]] = None
        with contextlib.suppress(Exception):
            cur_rt = read_runtime(state_dir)
        if (
            cur_rt is not None
            and cur_rt.get("pid") == pid
            and cur_rt.get("started_at") == started_at
            and _pid_matches_subdots_hub(pid, state_dir)
        ):
            with contextlib.suppress(OSError):
                os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pid_alive(pid):
                time.sleep(0.05)
    if pid_alive(pid):
        if a.json:
            print(json.dumps({"ok": False, "pid": pid, "error": "hub still running"}))
        else:
            print(f"subdots: hub still running pid={pid}", file=sys.stderr)
        return 1
    _cleanup_stale_hub_files(state_dir, rt)
    if a.json:
        print(json.dumps({"ok": True, "pid": pid}))
    else:
        print(f"HUB stopped pid={pid}")
    return 0


# ---------------------------------------------------------------------------
# Local Session Stdio MCP Server
# ---------------------------------------------------------------------------


def local_mcp_tools() -> List[Dict[str, Any]]:
    return [
        {
            "name": "subdots_list_agents",
            "description": "List connected Dots sub-agents, their epoch, live state, lease health, spin strikes, and pending steering counts.",
            "inputSchema": {"type": "object", "properties": {}},
            "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_agent_status",
            "description": "Show detailed live status, recent messages, and recent command runs for a specific Dots sub-agent.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id"],
                "properties": {"agent_id": {"type": "string"}},
            },
            "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_inbox",
            "description": "Read direct messages (replies, questions, results, blockers) sent from Dots sub-agents to the local session.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string"},
                    "include_read": {"type": "boolean"},
                },
            },
            "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_mark_read",
            "description": "Mark a sub-agent reply message as read in the local session inbox.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "id"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "id": {"type": "string"},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_send",
            "description": "Send a direct steering message, task, question, answer, review, or interrupt to a Dots sub-agent without going through the Dots root.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "kind", "task", "text"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "kind": {"type": "string", "enum": list(TO_AGENT_KINDS)},
                    "task": {"type": "string"},
                    "text": {"type": "string"},
                    "project": {"type": "string"},
                    "re_id": {"type": "string"},
                    "refs": {"type": "array", "items": {"type": "string"}},
                    "interrupt": {"type": "boolean"},
                    "request_id": {"type": "string"},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
        {
            "name": "subdots_steer",
            "description": "Steer a working or waiting Dots sub-agent mid-task (optionally interrupting an in-flight exec).",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "text"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "text": {"type": "string"},
                    "task": {"type": "string"},
                    "interrupt": {"type": "boolean"},
                    "request_id": {"type": "string"},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
        {
            "name": "subdots_await_reply",
            "description": "Wait for a sub-agent reply matching a sent message ID (or any unread reply from that agent).",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "re_id": {"type": "string"},
                    "timeout_s": {"type": "number"},
                },
            },
            "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_events",
            "description": "Read recent harness events (registrations, deliveries, acks, spin interventions, lease deaths, wakes).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string"},
                    "after_seq": {"type": "integer"},
                    "limit": {"type": "integer"},
                },
            },
            "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        },
        {
            "name": "subdots_control",
            "description": "Control a Dots sub-agent: pause, resume, cancel, release, or wake.",
            "inputSchema": {
                "type": "object",
                "required": ["agent_id", "action"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "action": {"type": "string", "enum": ["pause", "resume", "cancel", "release", "wake"]},
                    "text": {"type": "string"},
                },
            },
            "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        },
    ]


def cmd_mcp_stdio(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    out_lock = threading.Lock()
    sem = threading.BoundedSemaphore(16)
    in_flight: List[threading.Thread] = []
    flight_lock = threading.Lock()
    sock_lock = threading.Lock()
    active_socks: List[socket.socket] = []
    cancel_tracker: Tuple[threading.Lock, List[socket.socket]] = (sock_lock, active_socks)
    closing = threading.Event()

    def _emit(obj: Dict[str, Any]) -> None:
        if closing.is_set():
            return
        raw = json.dumps(obj, ensure_ascii=False) + "\n"
        with out_lock:
            if closing.is_set():
                return
            try:
                sys.stdout.write(raw)
                sys.stdout.flush()
            except (BrokenPipeError, OSError):
                closing.set()

    def _handle_req(req: Dict[str, Any]) -> None:
        try:
            req_id = req.get("id")
            method = req.get("method")
            raw_params = req.get("params")
            if raw_params is not None and not isinstance(raw_params, dict):
                _emit({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "Invalid params"}})
                return
            params = raw_params or {}
            if req_id is None:
                return
            if method == "initialize":
                proto = params.get("protocolVersion")
                chosen = proto if isinstance(proto, str) and proto in ("2024-11-05", "2025-03-26") else MCP_PROTOCOL_VERSION
                resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": chosen,
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {"name": "dotomation-subdots-local-mcp", "version": VERSION},
                    },
                }
            elif method == "ping":
                resp = {"jsonrpc": "2.0", "id": req_id, "result": {}}
            elif method == "tools/list":
                resp = {"jsonrpc": "2.0", "id": req_id, "result": {"tools": local_mcp_tools()}}
            elif method == "tools/call":
                name = params.get("name")
                raw_args = params.get("arguments")
                if raw_args is not None and not isinstance(raw_args, dict):
                    _emit({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "Invalid arguments"}})
                    return
                args = raw_args or {}
                try:
                    if name == "subdots_list_agents":
                        out = control_rpc(state_dir, "agent.list", cancel_tracker=cancel_tracker)
                    elif name == "subdots_agent_status":
                        out = control_rpc(state_dir, "agent.status", {"agent_id": args.get("agent_id", "")}, cancel_tracker=cancel_tracker)
                    elif name == "subdots_inbox":
                        out = control_rpc(
                            state_dir,
                            "message.inbox",
                            {"agent_id": args.get("agent_id"), "include_read": bool(args.get("include_read", False))},
                            cancel_tracker=cancel_tracker,
                        )
                    elif name == "subdots_mark_read":
                        out = control_rpc(
                            state_dir,
                            "message.mark_read",
                            {"agent_id": args.get("agent_id", ""), "id": args.get("id", "")},
                            cancel_tracker=cancel_tracker,
                        )
                    elif name == "subdots_send":
                        out = control_rpc(state_dir, "message.send", args, cancel_tracker=cancel_tracker)
                    elif name == "subdots_steer":
                        p = dict(args)
                        p["kind"] = "interrupt" if p.get("interrupt") else "steer"
                        p.setdefault("task", "steer")
                        out = control_rpc(state_dir, "message.send", p, cancel_tracker=cancel_tracker)
                    elif name == "subdots_await_reply":
                        raw_tout = args.get("timeout_s")
                        tout = validate_finite_float(
                            "timeout_s",
                            60.0 if raw_tout is None else raw_tout,
                            minimum=0.0,
                            maximum=3600.0,
                        )
                        out = control_rpc(state_dir, "message.await", args, timeout=tout + 10.0, cancel_tracker=cancel_tracker)
                    elif name == "subdots_events":
                        out = control_rpc(state_dir, "events.list", args, cancel_tracker=cancel_tracker)
                    elif name == "subdots_control":
                        action = args.get("action")
                        if action not in ("pause", "resume", "cancel", "release", "wake"):
                            raise ValueError(f"invalid control action {action!r}")
                        out = control_rpc(
                            state_dir,
                            f"control.{action}",
                            {"agent_id": args.get("agent_id"), "text": args.get("text") or action},
                            cancel_tracker=cancel_tracker,
                        )
                    else:
                        out = None
                    if out is None:
                        resp = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown tool: {name}"}}
                    else:
                        resp = {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "result": {"content": [{"type": "text", "text": json.dumps(out, ensure_ascii=False)}], "isError": False},
                        }
                except Exception as exc:
                    resp = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "result": {"content": [{"type": "text", "text": f"ERROR: {exc}"}], "isError": True},
                    }
            else:
                resp = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
            _emit(resp)
        finally:
            sem.release()

    stdin_buf = getattr(sys.stdin, "buffer", None)
    try:
        while not closing.is_set():
            if stdin_buf is not None:
                raw_b = stdin_buf.readline(MAX_CONTROL_LINE_BYTES + 1)
                if not raw_b:
                    break
                if len(raw_b) > MAX_CONTROL_LINE_BYTES:
                    _emit({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Request line too large"}})
                    continue
                try:
                    line = raw_b.decode("utf-8").strip()
                except UnicodeDecodeError:
                    _emit({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
                    continue
            else:
                line = sys.stdin.readline(MAX_CONTROL_LINE_BYTES + 1)
                if not line:
                    break
                if len(line) > MAX_CONTROL_LINE_BYTES:
                    _emit({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Request line too large"}})
                    continue
                line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except (ValueError, RecursionError):
                _emit({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
                continue
            if not isinstance(req, dict):
                _emit({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}})
                continue
            sem.acquire()
            if closing.is_set():
                sem.release()
                break
            th = threading.Thread(target=_handle_req, args=(req,), daemon=True)
            with flight_lock:
                in_flight = [t for t in in_flight if t.is_alive()]
                in_flight.append(th)
            th.start()
    finally:
        closing.set()
        with sock_lock:
            socks_to_abort = list(active_socks)
        for sock in socks_to_abort:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                sock.close()
        with flight_lock:
            pending = list(in_flight)
        for th in pending:
            th.join(timeout=1.0)
    return 0


# ---------------------------------------------------------------------------
# Sub-Agent HTTP Client & Harness Helpers
# ---------------------------------------------------------------------------


def resolve_hub_url(state_dir: str, explicit_url: Optional[str]) -> str:
    url = explicit_url or os.environ.get("SUBDOTS_HUB_URL")
    if not url:
        rt = read_runtime(state_dir)
        url = rt["http_url"]
    return url.rstrip("/")


def session_cache_path(state_dir: str, agent_id: str, explicit_path: Optional[str]) -> str:
    agent_id = validate_token("agent_id", agent_id)
    if explicit_path:
        return os.path.abspath(os.path.expanduser(explicit_path))
    sess_dir = os.path.join(state_dir, "sessions")
    ensure_private_dir(sess_dir)
    return os.path.join(sess_dir, f"{agent_id}.json")


def load_agent_session(
    state_dir: str, agent_id: str, explicit_path: Optional[str], epoch: Optional[int], session_token: Optional[str]
) -> Tuple[int, str]:
    agent_id = validate_token("agent_id", agent_id)
    if epoch is not None and session_token is not None:
        if isinstance(epoch, bool):
            raise ValueError("epoch must be an integer")
        return int(epoch), session_token
    path = session_cache_path(state_dir, agent_id, explicit_path)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        st = os.lstat(path)
    except FileNotFoundError as exc:
        raise HubError("NOT_REGISTERED", f"no cached session for {agent_id}; run `agent register {agent_id}` first") from exc
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        raise OSError(f"{path} is not a regular file")
    if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
        raise OSError(f"{path} owner mismatch")
    if (st.st_mode & 0o077) != 0:
        raise OSError(f"{path} permissions too open")
    fd = os.open(path, os.O_RDONLY | nofollow | os.O_NONBLOCK)
    opened = False
    try:
        fst = os.fstat(fd)
        if not stat.S_ISREG(fst.st_mode):
            raise OSError(f"{path} is not a regular file")
        if hasattr(os, "geteuid") and fst.st_uid != os.geteuid():
            raise OSError(f"{path} owner mismatch")
        if (fst.st_mode & 0o077) != 0:
            raise OSError(f"{path} permissions too open")
        with os.fdopen(fd, "rb") as fh:
            opened = True
            raw_bytes = fh.read(16385)
    finally:
        if not opened:
            with contextlib.suppress(OSError):
                os.close(fd)
    if len(raw_bytes) > 16384:
        raise OSError(f"{path} exceeds 16384 bytes")
    doc = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(doc, dict):
        raise ValueError("invalid session cache format")
    return int(doc["epoch"]), str(doc["session_token"])


def save_agent_session(state_dir: str, agent_id: str, explicit_path: Optional[str], epoch: int, session_token: str) -> None:
    path = session_cache_path(state_dir, agent_id, explicit_path)
    write_private_atomic(
        path,
        json.dumps({"agent_id": agent_id, "epoch": epoch, "session_token": session_token}) + "\n",
        enforce_parent_dir=(explicit_path is None),
    )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        raise urllib.error.HTTPError(req.full_url, code, "redirects disallowed", headers, fp)


def _is_loopback_host(hostname: Optional[str]) -> bool:
    if not hostname:
        return False
    h = hostname.strip().strip("[]").lower()
    if h in ("localhost", "localhost."):
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


def _parse_and_validate_hub_url(hub_url: str, route: str) -> urllib.parse.SplitResult:
    if not isinstance(hub_url, str) or any(c in hub_url for c in ("\x00", "\r", "\n", "\t")):
        raise ValueError("hub_url contains invalid control characters")
    url = hub_url.rstrip("/") + route
    safe_url = redact(hub_url)
    try:
        parsed = urllib.parse.urlsplit(url)
        parsed.port
    except ValueError as exc:
        raise ValueError(f"invalid hub_url {safe_url!r}") from exc
    if not parsed.netloc or not parsed.hostname:
        raise ValueError(f"invalid hub_url {safe_url!r}")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("hub_url must not contain userinfo")
    if parsed.query or parsed.fragment:
        raise ValueError("hub_url must not contain query or fragment")
    if parsed.scheme == "https":
        return parsed
    if parsed.scheme == "http" and _is_loopback_host(parsed.hostname):
        return parsed
    raise ValueError(f"non-loopback hub_url must use https: {safe_url!r}")


def _build_url_opener(parsed: urllib.parse.SplitResult) -> urllib.request.OpenerDirector:
    handlers: List[Any] = [_NoRedirectHandler()]
    if _is_loopback_host(parsed.hostname):
        handlers.append(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener(*handlers)


def http_post(hub_url: str, route: str, payload: Dict[str, Any], timeout: float = 35.0) -> Dict[str, Any]:
    parsed = _parse_and_validate_hub_url(hub_url, route)
    url = urllib.parse.urlunsplit(parsed)
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    opener = _build_url_opener(parsed)
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw_bytes = resp.read(MAX_HTTP_BODY_BYTES + 1)
            if len(raw_bytes) > MAX_HTTP_BODY_BYTES:
                raise HubError("RESPONSE_TOO_LARGE", "hub response exceeded size limit", status=502)
            return json.loads(raw_bytes.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read(MAX_HTTP_BODY_BYTES).decode("utf-8", errors="replace")
        try:
            doc = json.loads(raw)
        except ValueError:
            doc = {"error": raw, "code": f"HTTP_{exc.code}"}
        raise HubError(doc.get("code", f"HTTP_{exc.code}"), doc.get("error", raw), status=exc.code, extra=doc) from exc
    except (
        urllib.error.URLError,
        http.client.HTTPException,
        TimeoutError,
        socket.timeout,
        OSError,
    ) as exc:
        reason = getattr(exc, "reason", exc)
        raise HubError("HUB_UNAVAILABLE", f"cannot reach hub at {route}: {reason}", status=503) from exc


def print_agent_piggyback(messages: Sequence[Dict[str, Any]]) -> None:
    banner = format_piggyback_blocks(messages)
    if banner:
        print(banner, flush=True)


class _WaitSignalAbort(BaseException):
    """Raised cleanly from signal handler to interrupt blocking socket recv under PEP 475."""


def run_agent_wait_loop(
    hub_url: str,
    agent_id: str,
    epoch: int,
    session_token: str,
    timeout_s: float,
    chunk_s: float,
    ack_ids: Sequence[str],
    expect_re: Optional[str] = None,
    as_json: bool = False,
    initial_messages: Sequence[Dict[str, Any]] = (),
    reply_id: Optional[str] = None,
    state_dir: Optional[str] = None,
    explicit_hub_url: Optional[str] = None,
    reply_obj: Optional[Dict[str, Any]] = None,
) -> int:
    stop_flag = False

    def _sig(signum: int, frame: Any) -> None:
        nonlocal stop_flag
        stop_flag = True
        raise _WaitSignalAbort()

    previous = {s: signal.getsignal(s) for s in SIGNALS}
    for s in SIGNALS:
        signal.signal(s, _sig)

    collected: List[Dict[str, Any]] = list(initial_messages)
    printed_ids: Set[str] = {str(m["id"]) for m in initial_messages if isinstance(m, dict) and m.get("id")}
    pending_acks = list(ack_ids)
    actionable = ("release", "steer", "interrupt", "pause", "cancel", "intervention")
    if expect_re is not None:
        for m in initial_messages:
            mid = m.get("id") if isinstance(m, dict) else None
            if (
                isinstance(mid, str)
                and mid not in pending_acks
                and not m.get("interrupt")
                and m.get("kind") not in actionable
            ):
                pending_acks.append(mid)

    deadline = time.monotonic() + timeout_s
    first_poll = True
    try:
        while not stop_flag:
            rem = deadline - time.monotonic()
            if rem <= 0 and not first_poll:
                if as_json:
                    out: Dict[str, Any] = {"timeout": True, "messages": collected}
                    if reply_id is not None:
                        out["reply_id"] = reply_id
                    if reply_obj is not None:
                        out["reply"] = reply_obj
                    print(json.dumps(out, ensure_ascii=False), flush=True)
                else:
                    print(f"WAIT timeout agent={agent_id} after={round(timeout_s, 1)}s", flush=True)
                return 3
            first_poll = False
            this_chunk = 0.0 if rem <= 0 else min(rem, chunk_s, MAX_CHUNK_S)
            try:
                res = http_post(
                    hub_url,
                    "/agent/poll",
                    {
                        "agent_id": agent_id,
                        "epoch": epoch,
                        "session_token": session_token,
                        "hold_s": this_chunk,
                        "ack_ids": pending_acks,
                        "expect_re": expect_re,
                    },
                    timeout=this_chunk + 15.0,
                )
                pending_acks = []
            except HubError as exc:
                if exc.code == "STALE_EPOCH":
                    print(f"subdots: {exc.message}", file=sys.stderr)
                    return 5
                if exc.code == "HUB_UNAVAILABLE":
                    if state_dir and explicit_hub_url is None and not os.environ.get("SUBDOTS_HUB_URL"):
                        with contextlib.suppress(Exception):
                            hub_url = resolve_hub_url(state_dir, None)
                    if stop_flag or time.monotonic() >= deadline:
                        break
                    time.sleep(min(0.5, max(0.05, deadline - time.monotonic())))
                    continue
                raise

            msgs = res.get("messages") or []
            if msgs:
                fresh_msgs: List[Dict[str, Any]] = []
                for m in msgs:
                    mid = m.get("id")
                    if isinstance(mid, str) and mid in printed_ids:
                        continue
                    if isinstance(mid, str):
                        printed_ids.add(mid)
                    fresh_msgs.append(m)
                    collected.append(m)
                if expect_re is None:
                    if as_json:
                        out = {"messages": collected}
                        if reply_id is not None:
                            out["reply_id"] = reply_id
                        if reply_obj is not None:
                            out["reply"] = reply_obj
                        print(json.dumps(out, ensure_ascii=False), flush=True)
                    else:
                        print_agent_piggyback(fresh_msgs or msgs)
                    return 0
                for m in fresh_msgs:
                    mid = m.get("id")
                    if (
                        isinstance(mid, str)
                        and mid not in pending_acks
                        and not m.get("interrupt")
                        and m.get("kind") not in actionable
                        and m.get("re_id") != expect_re
                    ):
                        pending_acks.append(mid)
                if not as_json and fresh_msgs:
                    print_agent_piggyback(fresh_msgs)
                if res.get("matched_expect"):
                    if as_json:
                        out = {"matched_expect": True, "messages": collected}
                        if reply_id is not None:
                            out["reply_id"] = reply_id
                        if reply_obj is not None:
                            out["reply"] = reply_obj
                        print(json.dumps(out, ensure_ascii=False), flush=True)
                    return 0
            if res.get("stopping"):
                return 4
        if stop_flag:
            return 4
        if as_json:
            out = {"timeout": True, "messages": collected}
            if reply_id is not None:
                out["reply_id"] = reply_id
            if reply_obj is not None:
                out["reply"] = reply_obj
            print(json.dumps(out, ensure_ascii=False), flush=True)
        else:
            print(f"WAIT timeout agent={agent_id} after={round(timeout_s, 1)}s", flush=True)
        return 3
    except _WaitSignalAbort:
        return 4
    finally:
        for s, h in previous.items():
            signal.signal(s, h)


# ---------------------------------------------------------------------------
# Sub-Agent CLI Handlers
# ---------------------------------------------------------------------------


def cmd_agent_register(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    res = http_post(
        hub_url,
        "/agent/register",
        {
            "agent_id": a.agent,
            "dot": a.dot,
            "project": a.project,
            "role": a.role,
            "host": a.host or socket.gethostname(),
            "model": a.model,
            "lease_ttl_s": a.lease_ttl,
        },
    )
    save_agent_session(state_dir, a.agent, a.session_file, res["epoch"], res["session_token"])
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print_agent_piggyback(res.get("messages") or [])
        print(f"REGISTERED {a.agent} epoch={res['epoch']} lease_ttl={res['lease_ttl_s']}s")
    return 0


def cmd_agent_wait(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    timeout_s = validate_finite_float("timeout", float(a.timeout), minimum=0.0, maximum=86400.0)
    chunk_s = validate_finite_float("chunk", float(a.chunk), minimum=0.0, maximum=MAX_CHUNK_S)
    if timeout_s > 0.0 and chunk_s < 0.1:
        raise ValueError("chunk must be >= 0.1 when timeout > 0")
    epoch, tok = load_agent_session(state_dir, a.agent, a.session_file, a.epoch, a.session_token)
    return run_agent_wait_loop(
        hub_url,
        a.agent,
        epoch,
        tok,
        timeout_s=timeout_s,
        chunk_s=chunk_s,
        ack_ids=a.ack,
        as_json=a.json,
        state_dir=state_dir,
        explicit_hub_url=a.hub_url,
    )


def cmd_agent_step(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    epoch, tok = load_agent_session(state_dir, a.agent, a.session_file, a.epoch, a.session_token)
    res = http_post(
        hub_url,
        "/agent/step",
        {
            "agent_id": a.agent,
            "epoch": epoch,
            "session_token": tok,
            "state": a.state,
            "task": a.task,
            "note": a.note,
            "evidence": a.evidence,
            "host": a.host or socket.gethostname(),
            "goal": a.goal,
            "doing": a.doing,
            "next_step": a.next,
            "decisions": a.decision,
            "blockers": a.blocker,
            "unblock": a.unblock,
            "ack_ids": a.ack,
        },
    )
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print_agent_piggyback(res.get("messages") or [])
        ag = res["agent"]
        print(f"STEP {ag['agent_id']} state={ag['state']} live_state={a.state} t={res['t']}")
    return 0


def cmd_agent_reply(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    timeout_s = validate_finite_float("timeout", float(a.timeout), minimum=0.0, maximum=86400.0)
    chunk_s = validate_finite_float("chunk", float(a.chunk), minimum=0.0, maximum=MAX_CHUNK_S)
    if a.kind in REPLY_WAITS and not a.no_wait and timeout_s > 0.0 and chunk_s < 0.1:
        raise ValueError("chunk must be >= 0.1 when timeout > 0")
    epoch, tok = load_agent_session(state_dir, a.agent, a.session_file, a.epoch, a.session_token)
    text_body = a.text if a.text is not None else sys.stdin.read()
    res = http_post(
        hub_url,
        "/agent/reply",
        {
            "agent_id": a.agent,
            "epoch": epoch,
            "session_token": tok,
            "kind": a.kind,
            "task": a.task,
            "text": text_body,
            "re_id": a.re,
            "refs": a.ref,
            "ack_ids": a.ack,
        },
    )
    reply_id = res["reply"]["id"]
    waits = a.kind in REPLY_WAITS and not a.no_wait
    piggy_msgs = res.get("messages") or []
    actionable = ("release", "steer", "interrupt", "pause", "cancel", "intervention")
    already_answered = any(
        m.get("re_id") == reply_id or bool(m.get("interrupt")) or m.get("kind") in actionable
        for m in piggy_msgs
    )
    if not waits or already_answered:
        if a.json:
            out = dict(res)
            out["reply_id"] = reply_id
            print(json.dumps(out, ensure_ascii=False))
        else:
            print_agent_piggyback(piggy_msgs)
            print(reply_id)
        return 0
    if not a.json:
        print(reply_id, flush=True)
        print_agent_piggyback(piggy_msgs)
        print(f"REPLY waiting for answer to {reply_id}", flush=True)
    return run_agent_wait_loop(
        hub_url,
        a.agent,
        epoch,
        tok,
        timeout_s=timeout_s,
        chunk_s=chunk_s,
        ack_ids=[],
        expect_re=reply_id,
        as_json=a.json,
        initial_messages=piggy_msgs,
        reply_id=reply_id,
        state_dir=state_dir,
        explicit_hub_url=a.hub_url,
        reply_obj=res["reply"],
    )


def cmd_agent_ack(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    epoch, tok = load_agent_session(state_dir, a.agent, a.session_file, a.epoch, a.session_token)
    res = http_post(
        hub_url,
        "/agent/ack",
        {"agent_id": a.agent, "epoch": epoch, "session_token": tok, "ack_ids": a.ids},
    )
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print_agent_piggyback(res.get("messages") or [])
        print(f"ACKED {','.join(res['acked']) or 'none'}")
    return 0


def cmd_agent_heartbeat(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    epoch, tok = load_agent_session(state_dir, a.agent, a.session_file, a.epoch, a.session_token)
    res = http_post(
        hub_url,
        "/agent/heartbeat",
        {"agent_id": a.agent, "epoch": epoch, "session_token": tok, "ack_ids": a.ack},
    )
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print_agent_piggyback(res.get("messages") or [])
        print(f"HEARTBEAT {a.agent} lease_remaining={res['agent']['lease_remaining_s']}s")
    return 0


class _ExecSignalAbort(BaseException):
    """Raised cleanly from signal handler to interrupt blocking proc.wait() under PEP 475."""


def _terminate_process_group(
    pid: int,
    proc: Any,
    grace_s: float,
    proc_lock: Optional[threading.Lock] = None,
    reaped_flag: Optional[List[bool]] = None,
) -> None:
    def _is_alive() -> bool:
        if reaped_flag and reaped_flag[0]:
            return False
        return proc.poll() is None

    if proc_lock is not None:
        with proc_lock:
            if not _is_alive():
                return
            with contextlib.suppress(ProcessLookupError, OSError):
                os.killpg(pid, signal.SIGTERM)
    else:
        if not _is_alive():
            return
        with contextlib.suppress(ProcessLookupError, OSError):
            os.killpg(pid, signal.SIGTERM)

    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline:
        if proc_lock is not None:
            with proc_lock:
                if not _is_alive():
                    return
        elif not _is_alive():
            return
        time.sleep(0.02)

    if proc_lock is not None:
        with proc_lock:
            if not _is_alive():
                return
            with contextlib.suppress(ProcessLookupError, OSError):
                os.killpg(pid, signal.SIGKILL)
    else:
        if not _is_alive():
            return
        with contextlib.suppress(ProcessLookupError, OSError):
            os.killpg(pid, signal.SIGKILL)


def _abortable_http_post(
    hub_url: str,
    route: str,
    payload: Dict[str, Any],
    timeout: float,
    conn_holder: Dict[str, Any],
    conn_lock: threading.Lock,
) -> Dict[str, Any]:
    parsed = _parse_and_validate_hub_url(hub_url, route)
    host = (parsed.hostname or "").strip("[]")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(host, port, timeout=timeout)
    try:
        conn.connect()
    except (http.client.HTTPException, TimeoutError, socket.timeout, OSError) as exc:
        with contextlib.suppress(Exception):
            conn.close()
        raise HubError("HUB_UNAVAILABLE", f"cannot reach hub at {route}: {exc}", status=503) from exc
    with conn_lock:
        if conn_holder.get("closed"):
            conn.close()
            raise HubError("HUB_UNAVAILABLE", "watcher stopped", status=503)
        conn_holder["conn"] = conn
    try:
        conn.request("POST", path, body=body, headers={"Content-Type": "application/json", "Connection": "close"})
        resp = conn.getresponse()
        raw_bytes = resp.read(MAX_HTTP_BODY_BYTES + 1)
        if len(raw_bytes) > MAX_HTTP_BODY_BYTES:
            raise HubError("RESPONSE_TOO_LARGE", "hub response exceeded size limit", status=502)
        raw = raw_bytes.decode("utf-8", errors="replace")
        if resp.status < 200 or resp.status >= 300:
            try:
                doc = json.loads(raw)
            except (ValueError, RecursionError):
                doc = {"error": raw, "code": f"HTTP_{resp.status}"}
            raise HubError(doc.get("code", f"HTTP_{resp.status}"), doc.get("error", raw), status=resp.status, extra=doc)
        return json.loads(raw)
    except HubError:
        raise
    except (ValueError, RecursionError, http.client.HTTPException, TimeoutError, socket.timeout, OSError) as exc:
        raise HubError("HUB_UNAVAILABLE", f"cannot reach hub at {route}: {exc}", status=503) from exc
    finally:
        with conn_lock:
            if conn_holder.get("conn") is conn:
                conn_holder["conn"] = None
        with contextlib.suppress(Exception):
            conn.close()


def cmd_agent_exec(a: argparse.Namespace) -> int:
    cmd_args = list(a.command)
    if cmd_args and cmd_args[0] == "--":
        cmd_args = cmd_args[1:]
    if not cmd_args:
        die("agent exec requires a command after --")
    heartbeat_s = validate_finite_float("heartbeat-s", float(a.heartbeat_s), minimum=0.1, maximum=60.0)
    grace_s = validate_finite_float("grace-s", float(a.grace_s), minimum=0.1, maximum=30.0)
    cmd_str = " ".join(cmd_args)
    state_dir = resolve_state_dir(a.state_dir)
    hub_url = resolve_hub_url(state_dir, a.hub_url)
    epoch, tok = load_agent_session(state_dir, a.agent, a.session_file, a.epoch, a.session_token)

    try:
        start_res = http_post(
            hub_url,
            "/agent/exec/start",
            {
                "agent_id": a.agent,
                "epoch": epoch,
                "session_token": tok,
                "cmd": cmd_str,
                "cwd": os.path.abspath(a.cwd or os.getcwd()),
                "ack_ids": a.ack,
            },
        )
    except HubError as exc:
        if exc.code == "INTERRUPT_PENDING":
            msgs = exc.extra.get("messages") or []
            print_agent_piggyback(msgs)
            print(f"\n[INTERRUPTED_BY_STEERING] before exec: {exc.message}", flush=True)
            return 130
        if exc.code == "AGENT_SPINNING":
            msgs = exc.extra.get("messages") or []
            print_agent_piggyback(msgs)
            print(f"subdots: [{exc.code}] {exc.message}", file=sys.stderr)
            return 2
        raise
    exec_id = start_res["exec_id"]
    start_msgs = start_res.get("messages") or []
    print_agent_piggyback(start_msgs)

    pre_interrupt = next(
        (m for m in start_msgs if bool(m.get("interrupt")) or m.get("kind") in INTERRUPT_KINDS),
        None,
    )
    if pre_interrupt is not None:
        with contextlib.suppress(HubError):
            http_post(
                hub_url,
                "/agent/exec/finish",
                {
                    "agent_id": a.agent,
                    "epoch": epoch,
                    "session_token": tok,
                    "exec_id": exec_id,
                    "status": "interrupted",
                    "exit_code": None,
                    "stderr_hash": "",
                    "output_tail": "",
                    "interrupted_by": pre_interrupt["id"],
                },
            )
        print(f"\n[INTERRUPTED_BY_STEERING] exec_id={exec_id} by={pre_interrupt['id']}", flush=True)
        return 130

    try:
        proc = subprocess.Popen(
            cmd_args,
            cwd=a.cwd or None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        with contextlib.suppress(HubError):
            http_post(
                hub_url,
                "/agent/exec/finish",
                {
                    "agent_id": a.agent,
                    "epoch": epoch,
                    "session_token": tok,
                    "exec_id": exec_id,
                    "status": "failed",
                    "exit_code": 127,
                    "stderr_hash": "",
                    "output_tail": str(exc)[-2000:],
                    "interrupted_by": None,
                },
            )
        raise

    ring = bytearray()
    ring_cap = 16384
    out_lock = threading.Lock()
    stdout_broken = False

    def _drain_output() -> None:
        nonlocal stdout_broken
        assert proc.stdout is not None
        fd = proc.stdout.fileno()
        while True:
            try:
                chunk = os.read(fd, 4096)
            except InterruptedError:
                continue
            except OSError:
                break
            if not chunk:
                break
            with out_lock:
                ring.extend(chunk)
                if len(ring) > ring_cap:
                    del ring[:-ring_cap]
            if not stdout_broken:
                try:
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.buffer.flush()
                except (BrokenPipeError, OSError):
                    stdout_broken = True

    drain_th = threading.Thread(target=_drain_output, daemon=True)
    drain_th.start()

    done_event = threading.Event()
    sig_aborted = False
    state_lock = threading.Lock()
    proc_lock = threading.Lock()
    reaped_flag = [False]
    conn_lock = threading.Lock()
    conn_holder: Dict[str, Any] = {"conn": None, "closed": False}
    interrupt_info: Dict[str, Any] = {
        "messages": [],
        "late_messages": [],
        "triggered": False,
        "stale_exc": None,
    }

    def _abort_watch_conn() -> None:
        with conn_lock:
            conn_holder["closed"] = True
            conn = conn_holder.get("conn")
        if conn is not None:
            sock = getattr(conn, "sock", None)
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(Exception):
                conn.close()

    def _watch_interrupt() -> None:
        while not done_event.is_set():
            hold = min(heartbeat_s, 20.0)
            try:
                res = _abortable_http_post(
                    hub_url,
                    "/agent/poll",
                    {
                        "agent_id": a.agent,
                        "epoch": epoch,
                        "session_token": tok,
                        "exec_id": exec_id,
                        "hold_s": hold,
                        "interrupt_only": True,
                    },
                    timeout=hold + 10.0,
                    conn_holder=conn_holder,
                    conn_lock=conn_lock,
                )
            except HubError as exc:
                if done_event.is_set():
                    return
                if exc.code == "STALE_EPOCH":
                    with state_lock:
                        interrupt_info["stale_exc"] = exc
                    _terminate_process_group(proc.pid, proc, grace_s, proc_lock=proc_lock, reaped_flag=reaped_flag)
                    return
                if done_event.wait(0.2):
                    return
                continue
            msgs = res.get("messages") or []
            if msgs:
                with state_lock:
                    if done_event.is_set() or proc.poll() is not None:
                        interrupt_info["late_messages"].extend(msgs)
                        done_event.set()
                        return
                    interrupt_info["messages"] = msgs
                    interrupt_info["triggered"] = True
                _terminate_process_group(proc.pid, proc, grace_s, proc_lock=proc_lock, reaped_flag=reaped_flag)
                return
            if done_event.is_set() or res.get("exec_finished"):
                return

    def _on_exec_sig(signum: int, frame: Any) -> None:
        raise _ExecSignalAbort()

    previous_sigs = {s: signal.getsignal(s) for s in SIGNALS}
    for s in SIGNALS:
        signal.signal(s, _on_exec_sig)

    watch_th = threading.Thread(target=_watch_interrupt, daemon=True)
    watch_th.start()

    try:
        try:
            rc = proc.wait()
            with proc_lock:
                reaped_flag[0] = True
        except _ExecSignalAbort:
            sig_aborted = True
            done_event.set()
            _abort_watch_conn()
            _terminate_process_group(proc.pid, proc, grace_s, proc_lock=proc_lock, reaped_flag=reaped_flag)
            rc = proc.wait()
            with proc_lock:
                reaped_flag[0] = True
        except BaseException:
            done_event.set()
            _abort_watch_conn()
            _terminate_process_group(proc.pid, proc, grace_s, proc_lock=proc_lock, reaped_flag=reaped_flag)
            with contextlib.suppress(Exception):
                proc.wait(timeout=1.0)
            with proc_lock:
                reaped_flag[0] = True
            raise
    finally:
        for s, h in previous_sigs.items():
            signal.signal(s, h)

    drain_th.join(timeout=2.0)

    with state_lock:
        stale_exc = interrupt_info.get("stale_exc")
        interrupted = bool(interrupt_info["triggered"])
        intr_msgs = list(interrupt_info["messages"])
    if stale_exc is not None:
        done_event.set()
        _abort_watch_conn()
        watch_th.join(timeout=1.0)
        raise stale_exc

    with out_lock:
        raw_out = bytes(ring)
    text_out = raw_out.decode("utf-8", errors="replace")
    tail = text_out[-2000:]
    norm_tail = normalize_spin_output(tail)
    stderr_hash = hashlib.sha256(norm_tail.encode("utf-8")).hexdigest()[:16] if rc != 0 else ""

    status = "interrupted" if interrupted else ("completed" if rc == 0 else "failed")
    interrupted_by = intr_msgs[0]["id"] if interrupted and intr_msgs else None

    fin_res: Dict[str, Any] = {}
    if interrupted or sig_aborted:
        done_event.set()
        _abort_watch_conn()
        watch_th.join(timeout=1.0)
        try:
            fin_res = http_post(
                hub_url,
                "/agent/exec/finish",
                {
                    "agent_id": a.agent,
                    "epoch": epoch,
                    "session_token": tok,
                    "exec_id": exec_id,
                    "status": status,
                    "exit_code": rc,
                    "stderr_hash": stderr_hash,
                    "output_tail": tail,
                    "interrupted_by": interrupted_by,
                },
            )
        except HubError as exc:
            if exc.code == "STALE_EPOCH":
                raise
            print(f"subdots: warning: exec/finish failed ({exc.code}): {exc.message}", file=sys.stderr)
    else:
        # Normal child exit: mark exec finished first so the Hub wakes the watch poll cleanly without losing interrupts.
        finish_ok = False
        try:
            fin_res = http_post(
                hub_url,
                "/agent/exec/finish",
                {
                    "agent_id": a.agent,
                    "epoch": epoch,
                    "session_token": tok,
                    "exec_id": exec_id,
                    "status": status,
                    "exit_code": rc,
                    "stderr_hash": stderr_hash,
                    "output_tail": tail,
                    "interrupted_by": interrupted_by,
                },
            )
            finish_ok = True
        except HubError as exc:
            if exc.code == "STALE_EPOCH":
                raise
            print(f"subdots: warning: exec/finish failed ({exc.code}): {exc.message}", file=sys.stderr)
        finally:
            done_event.set()
            if finish_ok:
                watch_th.join(timeout=1.0)
            if not finish_ok or watch_th.is_alive():
                _abort_watch_conn()
                watch_th.join(timeout=0.5)

    with state_lock:
        late_msgs = list(interrupt_info["late_messages"])
    if interrupted:
        print(f"\n[INTERRUPTED_BY_STEERING] exec_id={exec_id} by={interrupted_by}", flush=True)
        print_agent_piggyback(intr_msgs)
        print_agent_piggyback(late_msgs)
        print_agent_piggyback(fin_res.get("messages") or [])
        return 130
    print_agent_piggyback(late_msgs)
    print_agent_piggyback(fin_res.get("messages") or [])
    if sig_aborted:
        return 130
    return rc


# ---------------------------------------------------------------------------
# Local Session CLI Handlers
# ---------------------------------------------------------------------------


def cmd_local_provision(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    res = control_rpc(
        state_dir,
        "agent.provision",
        {
            "agent_id": a.agent,
            "dot": a.dot,
            "project": a.project,
            "role": a.role,
            "host": a.host or socket.gethostname(),
            "model": a.model,
            "lease_ttl_s": a.lease_ttl,
        },
    )
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        shown = dict(res)
        if not getattr(a, "show_token", False):
            shown["session_token"] = "<redacted; pass --show-token or --json>"
            shown["agent_token"] = "<redacted; pass --show-token or --json>"
            shown["agent_http_url"] = redact(shown.get("agent_http_url") or "")
            shown["agent_mcp_url"] = redact(shown.get("agent_mcp_url") or "")
        print(
            f"PROVISIONED {a.agent} epoch={shown['epoch']} http={shown['agent_http_url']} "
            f"mcp={shown['agent_mcp_url']}"
        )
        if getattr(a, "show_token", False):
            print(f"  session_token={shown['session_token']} agent_token={shown['agent_token']}")
    return 0


def cmd_local_agents(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    res = control_rpc(state_dir, "agent.list")
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
        return 0
    if not res["agents"]:
        print("AGENTS none")
        return 0
    for ag in res["agents"]:
        print(
            f"{sanitize_terminal(ag['dot'] or '-')} {sanitize_terminal(ag['agent_id'])} "
            f"state={sanitize_terminal(ag['state'])} epoch={ag['epoch']} "
            f"host={sanitize_terminal(ag['host'])} lease={ag['lease_remaining_s']}s polls={ag['active_poll_count']} "
            f"spins={ag['spin_strikes']} pending={ag['pending_to_agent']} unread={ag['unread_from_agent']} "
            f"task={sanitize_terminal(ag['task'] or '-')}"
        )
        if ag["doing"] or ag["next_step"]:
            print(f"  doing: {sanitize_terminal(ag['doing'] or '-')} | next: {sanitize_terminal(ag['next_step'] or '-')}")
    return 0


def cmd_local_status(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    if not a.agent:
        return cmd_local_agents(a)
    res = control_rpc(state_dir, "agent.status", {"agent_id": a.agent})
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        ag = res["agent"]
        print(
            f"{sanitize_terminal(ag['agent_id'])} dot={sanitize_terminal(ag['dot'])} "
            f"state={sanitize_terminal(ag['state'])} epoch={ag['epoch']} "
            f"lease={ag['lease_remaining_s']}s polls={ag['active_poll_count']} spins={ag['spin_strikes']}"
        )
        print(
            f"  task={sanitize_terminal(ag['task'] or '-')} "
            f"doing={sanitize_terminal(ag['doing'] or '-')} next={sanitize_terminal(ag['next_step'] or '-')}"
        )
    return 0


def cmd_local_send(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    tout: Optional[float] = None
    if getattr(a, "wait_reply", False):
        tout = validate_finite_float("timeout", float(a.timeout), minimum=0.0, maximum=3600.0)
    text_body = a.text if a.text is not None else sys.stdin.read()
    res = control_rpc(
        state_dir,
        "message.send",
        {
            "agent_id": a.agent,
            "project": a.project,
            "kind": a.kind,
            "task": a.task,
            "text": text_body,
            "re_id": a.re,
            "refs": a.ref,
            "interrupt": a.interrupt,
            "request_id": a.request_id,
        },
    )
    msg = res["message"]
    if tout is not None:
        awaited = control_rpc(
            state_dir,
            "message.await",
            {"agent_id": a.agent, "re_id": msg["id"], "timeout_s": tout},
            timeout=tout + 10.0,
        )
        out = {"sent": res, **awaited}
        if a.json:
            print(json.dumps(out, ensure_ascii=False))
        else:
            print(msg["id"])
            if awaited["found"]:
                r = awaited["reply"]
                print(
                    f"REPLY {sanitize_terminal(r['id'])} kind={sanitize_terminal(r['kind'])} "
                    f"re={sanitize_terminal(r['re_id'])}\n{sanitize_terminal(r['text'])}"
                )
            else:
                print(f"AWAIT timeout after {tout}s")
        return 0 if awaited["found"] else 3

    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print(f"{msg['id']} listening={res['agent_listening']} deduped={res['deduped']}")
    return 0


def cmd_local_steer(a: argparse.Namespace) -> int:
    a.kind = "interrupt" if a.interrupt else "steer"
    a.task = a.task or a.kind
    a.project = None
    a.re = None
    a.ref = []
    a.wait_reply = False
    return cmd_local_send(a)


def cmd_local_ask(a: argparse.Namespace) -> int:
    a.kind = "question"
    a.task = a.task or "question"
    a.re = None
    a.ref = []
    a.interrupt = False
    a.wait_reply = True
    return cmd_local_send(a)


def cmd_local_inbox(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    res = control_rpc(state_dir, "message.inbox", {"agent_id": a.agent, "include_read": a.all})
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
        return 0
    if not res["messages"]:
        print("INBOX none")
        return 0
    for m in res["messages"]:
        re_s = f" re={sanitize_terminal(m['re_id'])}" if m.get("re_id") else ""
        print(
            f"INBOX {sanitize_terminal(m['agent_id'])} {sanitize_terminal(m['id'])} "
            f"kind={sanitize_terminal(m['kind'])} task={sanitize_terminal(m['task'])}{re_s}"
        )
        print(f"  {sanitize_terminal(m['text'])}")
    return 0


def cmd_local_read(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    res = control_rpc(state_dir, "message.mark_read", {"agent_id": a.agent, "id": a.id})
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print(f"READ {a.agent} {a.id}")
    return 0


def cmd_local_events(a: argparse.Namespace) -> int:
    state_dir = resolve_state_dir(a.state_dir)
    after_seq = max(0, int(a.after))
    if a.follow:
        timeout_s = validate_finite_float("timeout", float(a.timeout), minimum=0.0, maximum=3600.0)
        def _emit(ev: Dict[str, Any]) -> None:
            if a.json:
                print(json.dumps(ev, ensure_ascii=False), flush=True)
            else:
                print(
                    f"{ev['seq']} {ev['ts_iso']} {sanitize_terminal(ev['agent_id'])} "
                    f"{sanitize_terminal(ev['event_type'])} {sanitize_terminal(json.dumps(ev['payload'], ensure_ascii=False))}",
                    flush=True,
                )

        control_follow_events(state_dir, a.agent, after_seq, timeout_s, _emit)
        return 0

    limit = max(1, min(int(a.limit), 500))
    res = control_rpc(state_dir, "events.list", {"agent_id": a.agent, "after_seq": after_seq, "limit": limit})
    if a.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        for ev in res["events"]:
            print(
                f"{ev['seq']} {ev['ts_iso']} {sanitize_terminal(ev['agent_id'])} "
                f"{sanitize_terminal(ev['event_type'])} {sanitize_terminal(json.dumps(ev['payload'], ensure_ascii=False))}"
            )
    return 0


def cmd_local_control(action: str) -> Any:
    def _run(a: argparse.Namespace) -> int:
        state_dir = resolve_state_dir(a.state_dir)
        params = {"agent_id": a.agent}
        if hasattr(a, "text") and a.text:
            params["text"] = a.text
        res = control_rpc(state_dir, f"control.{action}", params)
        if a.json:
            print(json.dumps(res, ensure_ascii=False))
        else:
            print(f"{action.upper()} {a.agent} ok={res.get('ok')}")
        return 0 if res.get("ok", True) else 1

    return _run


# ---------------------------------------------------------------------------
# Argument Parser & Entrypoint
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="subdots.py", description="Reverse web-gpt-agent steering bridge and sub-agent harness.")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_state_flags(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--state-dir", default=default_state_dir(), help="owner-private hub state dir (default ~/.subdots)")
        sp.add_argument("--json", action="store_true", help="machine-readable JSON output")

    def add_agent_conn_flags(sp: argparse.ArgumentParser) -> None:
        add_state_flags(sp)
        sp.add_argument("--hub-url", help="hub HTTP base URL including token (or SUBDOTS_HUB_URL)")
        sp.add_argument("--session-file", help="override cached (epoch, session_token) file path")
        sp.add_argument("--epoch", type=int, help="explicit fencing epoch")
        sp.add_argument("--session-token", help="explicit fencing session token")

    # hub
    hub = sub.add_parser("hub", help="manage the owner-private Reverse Bridge Hub")
    hub_sub = hub.add_subparsers(dest="hub_cmd", required=True)

    hs = hub_sub.add_parser("start", help="start the hub daemon (or --foreground)")
    add_state_flags(hs)
    hs.add_argument("--port", type=int, default=0, help="HTTP port on 127.0.0.1 (0=ephemeral)")
    hs.add_argument("--ops-base", help="optional .dots-ops mirror base folder")
    hs.add_argument("--mirror-ops", action="store_true", help="mirror to $DOTS_OPS_BASE")
    hs.add_argument("--public-host", help="additional allowed Host header (e.g. tunnel domain)")
    hs.add_argument("--require-provision", action="store_true", help="require owner `subdots.py provision` instead of open registration")
    hs.add_argument("--wake-cmd", help="JSON argv list run when waking a stalled/dead agent")
    hs.add_argument("--notify-cmd", help="JSON argv list run on dead/spin/unacked escalations")
    hs.add_argument("--foreground", action="store_true", help="run in foreground instead of detaching")
    hs.add_argument("--lease-ttl", type=float, default=DEFAULT_SETTINGS["lease_ttl_s"])
    hs.add_argument("--dead-grace", type=float, default=DEFAULT_SETTINGS["dead_grace_s"])
    hs.add_argument("--watchdog-interval", type=float, default=DEFAULT_SETTINGS["watchdog_interval_s"])
    hs.add_argument("--reoffer-s", type=float, default=DEFAULT_SETTINGS["reoffer_s"])
    hs.add_argument("--max-offers", type=int, default=DEFAULT_SETTINGS["max_offers"])
    hs.add_argument("--wake-backoff", type=float, default=DEFAULT_SETTINGS["wake_backoff_s"])
    hs.add_argument("--max-wakes", type=int, default=DEFAULT_SETTINGS["max_wakes"])
    hs.add_argument("--notify-after", type=float, default=DEFAULT_SETTINGS["notify_after_s"])
    hs.add_argument("--spin-window", type=float, default=DEFAULT_SETTINGS["spin_window_s"])
    hs.add_argument("--spin-cmd-repeats", type=int, default=DEFAULT_SETTINGS["spin_cmd_repeats"])
    hs.add_argument("--spin-step-repeats", type=int, default=DEFAULT_SETTINGS["spin_step_repeats"])
    hs.add_argument("--spin-poll-burst", type=int, default=DEFAULT_SETTINGS["spin_poll_burst"])
    hs.add_argument("--show-token", action="store_true", help="print unredacted hub/mcp URLs")

    hst = hub_sub.add_parser("status", help="check hub status")
    add_state_flags(hst)
    hst.add_argument("--show-token", action="store_true", help="print unredacted hub/mcp URLs")

    hsp = hub_sub.add_parser("stop", help="stop the hub daemon")
    add_state_flags(hsp)

    # mcp-stdio
    ms = sub.add_parser("mcp-stdio", help="run stdio MCP server for local AI sessions")
    ms.add_argument("--state-dir", default=default_state_dir())

    # local session commands
    pr = sub.add_parser("provision", help="provision an agent with a scoped per-agent HTTP/MCP URL and fencing credentials")
    add_state_flags(pr)
    pr.add_argument("agent")
    pr.add_argument("--dot", default="DOT")
    pr.add_argument("--project", default="default")
    pr.add_argument("--role", default="구현")
    pr.add_argument("--host")
    pr.add_argument("--model", default="requested gpt-6.2-astra xhigh; runtime does not display effective")
    pr.add_argument("--lease-ttl", type=float, default=None)
    pr.add_argument("--show-token", action="store_true", help="print unredacted credentials and scoped URLs")

    ag_ls = sub.add_parser("agents", help="list connected sub-agents")
    add_state_flags(ag_ls)

    st = sub.add_parser("status", help="show agent status")
    add_state_flags(st)
    st.add_argument("agent", nargs="?")

    sd = sub.add_parser("send", help="send steering/task/answer/review to a sub-agent")
    add_state_flags(sd)
    sd.add_argument("agent")
    sd.add_argument("kind", choices=TO_AGENT_KINDS)
    sd.add_argument("task")
    sd.add_argument("--project")
    sd.add_argument("--text")
    sd.add_argument("--re")
    sd.add_argument("--ref", action="append", default=[])
    sd.add_argument("--interrupt", action="store_true")
    sd.add_argument("--request-id")
    sd.add_argument("--wait-reply", action="store_true")
    sd.add_argument("--timeout", type=float, default=60.0)

    sr = sub.add_parser("steer", help="send mid-task steering (optionally --interrupt)")
    add_state_flags(sr)
    sr.add_argument("agent")
    sr.add_argument("--text")
    sr.add_argument("--task")
    sr.add_argument("--interrupt", action="store_true")
    sr.add_argument("--request-id")

    ak = sub.add_parser("ask", help="send question and block until sub-agent replies")
    add_state_flags(ak)
    ak.add_argument("agent")
    ak.add_argument("--text")
    ak.add_argument("--task")
    ak.add_argument("--project")
    ak.add_argument("--request-id")
    ak.add_argument("--timeout", type=float, default=60.0)

    ib = sub.add_parser("inbox", help="read sub-agent replies")
    add_state_flags(ib)
    ib.add_argument("agent", nargs="?")
    ib.add_argument("--all", action="store_true")

    rd = sub.add_parser("read", help="mark a sub-agent reply read")
    add_state_flags(rd)
    rd.add_argument("agent")
    rd.add_argument("id")

    ev = sub.add_parser("events", help="inspect or follow hub events")
    add_state_flags(ev)
    ev.add_argument("agent", nargs="?")
    ev.add_argument("--after", type=int, default=0)
    ev.add_argument("--limit", type=int, default=100)
    ev.add_argument("--follow", action="store_true")
    ev.add_argument("--timeout", type=float, default=30.0)

    for act in ("pause", "resume", "cancel", "release", "wake"):
        sp = sub.add_parser(act, help=f"{act} a sub-agent")
        add_state_flags(sp)
        sp.add_argument("agent")
        if act != "wake":
            sp.add_argument("--text")

    # agent commands
    ag = sub.add_parser("agent", help="sub-agent harness client commands")
    ag_sub = ag.add_subparsers(dest="agent_cmd", required=True)

    ar = ag_sub.add_parser("register", help="register sub-agent and obtain fencing epoch")
    add_agent_conn_flags(ar)
    ar.add_argument("agent")
    ar.add_argument("--dot", default="DOT")
    ar.add_argument("--project", default="default")
    ar.add_argument("--role", default="구현")
    ar.add_argument("--host")
    ar.add_argument("--model", default="requested gpt-6.2-astra xhigh; runtime does not display effective")
    ar.add_argument("--lease-ttl", type=float, default=None)

    aw = ag_sub.add_parser("wait", help="wait for steering via chunked long-poll")
    add_agent_conn_flags(aw)
    aw.add_argument("agent")
    aw.add_argument("--timeout", type=float, default=3300.0)
    aw.add_argument("--chunk", type=float, default=20.0)
    aw.add_argument("--ack", action="append", default=[])

    asp = ag_sub.add_parser("step", help="report live progress and pull piggybacked steering")
    add_agent_conn_flags(asp)
    asp.add_argument("agent")
    asp.add_argument("state", choices=LIVE_STATES)
    asp.add_argument("--task", required=True)
    asp.add_argument("--note", required=True)
    asp.add_argument("--evidence", required=True)
    asp.add_argument("--host")
    asp.add_argument("--goal")
    asp.add_argument("--doing")
    asp.add_argument("--next")
    asp.add_argument("--decision", action="append", default=[])
    asp.add_argument("--blocker", action="append", default=[])
    asp.add_argument("--unblock", action="store_true")
    asp.add_argument("--ack", action="append", default=[])

    arp = ag_sub.add_parser("reply", help="reply to local session; result/question/block wait for answer")
    add_agent_conn_flags(arp)
    arp.add_argument("agent")
    arp.add_argument("kind", choices=TO_LOCAL_KINDS)
    arp.add_argument("task")
    arp.add_argument("--text")
    arp.add_argument("--re")
    arp.add_argument("--ref", action="append", default=[])
    arp.add_argument("--ack", action="append", default=[])
    arp.add_argument("--no-wait", action="store_true")
    arp.add_argument("--timeout", type=float, default=3300.0)
    arp.add_argument("--chunk", type=float, default=20.0)

    aak = ag_sub.add_parser("ack", help="acknowledge offered steering messages")
    add_agent_conn_flags(aak)
    aak.add_argument("agent")
    aak.add_argument("ids", nargs="+")

    ahb = ag_sub.add_parser("heartbeat", help="renew agent lease")
    add_agent_conn_flags(ahb)
    ahb.add_argument("agent")
    ahb.add_argument("--ack", action="append", default=[])

    aex = ag_sub.add_parser("exec", help="supervised command execution with live interrupt & spin tracking")
    add_agent_conn_flags(aex)
    aex.add_argument("agent")
    aex.add_argument("--cwd")
    aex.add_argument("--heartbeat-s", type=float, default=10.0)
    aex.add_argument("--grace-s", type=float, default=2.0)
    aex.add_argument("--ack", action="append", default=[])
    aex.add_argument("command", nargs="*")

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if len(raw_argv) >= 2 and raw_argv[0] == "agent" and raw_argv[1] == "exec" and "--" in raw_argv:
        dash_idx = raw_argv.index("--")
        a = parser.parse_args(raw_argv[:dash_idx])
        a.command = raw_argv[dash_idx + 1 :]
    else:
        a = parser.parse_args(raw_argv)
    try:
        if a.cmd == "hub":
            return {"start": cmd_hub_start, "status": cmd_hub_status, "stop": cmd_hub_stop}[a.hub_cmd](a)
        if a.cmd == "mcp-stdio":
            return cmd_mcp_stdio(a)
        if a.cmd == "agent":
            return {
                "register": cmd_agent_register,
                "wait": cmd_agent_wait,
                "step": cmd_agent_step,
                "reply": cmd_agent_reply,
                "ack": cmd_agent_ack,
                "heartbeat": cmd_agent_heartbeat,
                "exec": cmd_agent_exec,
            }[a.agent_cmd](a)
        dispatch = {
            "provision": cmd_local_provision,
            "agents": cmd_local_agents,
            "status": cmd_local_status,
            "send": cmd_local_send,
            "steer": cmd_local_steer,
            "ask": cmd_local_ask,
            "inbox": cmd_local_inbox,
            "read": cmd_local_read,
            "events": cmd_local_events,
            "pause": cmd_local_control("pause"),
            "resume": cmd_local_control("resume"),
            "cancel": cmd_local_control("cancel"),
            "release": cmd_local_control("release"),
            "wake": cmd_local_control("wake"),
        }
        return dispatch[a.cmd](a)
    except KeyboardInterrupt:
        return 130
    except (HubError, ValueError, OSError) as exc:
        if isinstance(exc, HubError):
            print(f"subdots: [{exc.code}] {exc.message}", file=sys.stderr)
            return 5 if exc.code == "STALE_EPOCH" else 2
        print(f"subdots: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
