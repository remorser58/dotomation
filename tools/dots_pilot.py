#!/usr/bin/env python3
"""Zero-AI-token, Python 3.9+ stdlib-only single-repo bootstrap & pilot for dotomation.

Replaces external LLM-backed coordinator daemons with deterministic local orchestration:
- Initializes owner-private pilot state (~/.dotomation-pilot, 0700/0600 + flock).
- Seeds Mode 1 multi-track loop contracts (`dots.py loop init`) from built-in presets
  (`generic`, `research-3track`, `ux-ui-impl-3track`) or a JSON template.
- Runs single-instance `dots.py watch` cycles authenticated via per-run cryptographic nonces,
  monitors `dots_monitor` + daemon health (retained terminal sessions, start churn),
  optionally heals an exhausted daemon unit behind an explicit opt-in cooldown guard,
  and dispatches wake/notify actions over pluggable transports (`stdout|command|aside`, `webhook`).
- Prints launchd/systemd templates (`render-service`) on demand without installing services.
"""

import os
import sys

_TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
if __name__ == "__main__" and not sys.flags.isolated and sys.path:
    if os.path.abspath(sys.path[0] or ".") == _TOOL_DIR:
        sys.path.pop(0)

import argparse
import base64
import contextlib
import datetime
import fcntl
import hashlib
import hmac
import ipaddress
import json
import re
import secrets
import shlex
import signal
import socket
import stat
import subprocess
import time
import types
import urllib.error
import urllib.parse
import urllib.request
from xml.sax.saxutils import escape as xml_escape

VERSION = "1.3.1"

TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
UNIT_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]{0,63}\Z")
HEX64_RE = re.compile(r"^[0-9a-f]{64}\Z")
ASIDE_URL_RE = re.compile(r"^https://chatgpt\.com/dots/[A-Za-z0-9._/?=&%-]+\Z")
PLACEHOLDER_RE = re.compile(r"\{(dot|agent|project|id|why|lanes|track|task|age|text|reason)\}")

_TERM_CTRL_RE = re.compile(
    r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]|[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\x9b]"
)
_PEM_BEGIN_RE = re.compile(r"-----BEGIN [A-Z0-9 ]{0,40}(?:PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----")
_PEM_END_RE = re.compile(r"-----END [A-Z0-9 ]{0,40}(?:PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----")
_PEM_HEADER_LINE_RE = re.compile(r"^[ \t]*[A-Za-z0-9-]+:[ \t][^\r\n]*[ \t]*$")
_PEM_B64_LINE_RE = re.compile(r"^[ \t]*[A-Za-z0-9+/=]{8,}[ \t]*$")
_PEM_BODY_LINE_RE = re.compile(r"^[ \t]*(?:[A-Za-z0-9+/=]{8,}|[A-Za-z0-9-]+:[ \t][^\r\n]*)[ \t]*$")
_PGP_CRC24_LINE_RE = re.compile(r"^[ \t]*=[A-Za-z0-9+/]{4}[ \t]*$")
_PEM_PADDED_TAIL_RE = re.compile(r"^[ \t]*[A-Za-z0-9+/]{1,7}={1,2}[ \t]*$")
_PEM_SHORT_B64_LINE_RE = re.compile(r"^[ \t]*[A-Za-z0-9+/]{1,7}[ \t]*$")

_SECRET_RE = re.compile(
    r"(?:gh[pousr]_[A-Za-z0-9_]{10,}|github_pat_[A-Za-z0-9_]{10,}|sk-[A-Za-z0-9_-]{10,}"
    r"|xox[abps]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{20,}"
    r"|-----BEGIN [A-Z0-9 ]{0,40}(?:PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----"
    r"|-----END [A-Z0-9 ]{0,40}(?:PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----"
    r"|(?:(?<=http://)|(?<=https://))(?:[^/\s:@]{1,128}:[^/\s@]{1,128}@)"
    r"|(?<=/)(?:a/)?[0-9a-f]{48}(?=/|$|[\s?#\"'\),;])"
    r"|(?i:\bBearer\s+[A-Za-z0-9._~+/-]{16,}|Authorization:\s*Basic\s+[A-Za-z0-9._+/=-]{8,})"
    r"|\b(?i:Basic)\s+(?=[A-Za-z0-9+/=]{8,}\b)(?:(?=[A-Za-z0-9+/=]*[0-9+/=])|(?=[A-Za-z0-9+/=]{12,}\b)(?=[A-Za-z0-9+/=]*[A-Z])(?=[A-Za-z0-9+/=]*[a-z]))[A-Za-z0-9+/=]{8,}"
    r"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
    r"|https://hooks\.slack\.com/services/[A-Za-z0-9/_-]{10,}"
    r"|https://(?:discord\.com|discordapp\.com)/api/webhooks/[A-Za-z0-9/_-]{10,}"
    r"|(?i:\"(?:[a-z0-9_]+_)?(?:api[_-]?key|access[_-]?key|secret[_-]?key|private[_-]?key|signing[_-]?key|encryption[_-]?key|password|passwd|secret|token|access[_-]?token|auth[_-]?token|refresh[_-]?token|session[_-]?token|agent[_-]?token|webhook[_-]?url|credential)\"\s*:\s*\"(?:\\[^\r\n]|[^\"\\\r\n]){1,4096}\")"
    r"|(?i:\b(?:[a-z0-9_]+_)?(?:api[_-]?key|access[_-]?key|secret[_-]?key|private[_-]?key|signing[_-]?key|encryption[_-]?key|password|passwd|secret|token|access[_-]?token|auth[_-]?token|refresh[_-]?token|session[_-]?token|agent[_-]?token|webhook[_-]?url|credential)\s*[:=]\s*(?:\"(?:\\[^\r\n]|[^\"\\\r\n]){1,4096}\"|'(?:\\[^\r\n]|[^'\\\r\n]){1,4096}'|[^\s\"',;]{6,4096}))"
    r"|(?i:[?&](?:token|key|api_key|access_token|secret|password|auth|sig|signature)=[^&\s]+))"
)

DEFAULT_PILOT_DIR = os.path.expanduser("~/.dotomation-pilot")
DEFAULT_OPS_BASE = os.environ.get("DOTS_OPS_BASE") or os.path.expanduser("~/.dots-ops")
MAX_JSON_BYTES = 1024 * 1024
MAX_CMD_TIMEOUT = 25
MIN_HEAL_COOLDOWN_MIN = 10
DEFAULT_WAIT_SECONDS = 180
RESERVED_OPS_DIRS = ("live", "memory", "listen", "watch", "loops")

PRESETS = {
    "generic": [
        {
            "track": "work",
            "role": "worker",
            "objective": "Execute assigned in-scope tasks sequentially, verify locally, commit on rev/* branches, and continue cleanly.",
            "feeds_to": [],
            "continuous": True,
            "gates": [],
            "allowed_paths": ["*"],
        }
    ],
    "research-3track": [
        {
            "track": "research",
            "role": "researcher",
            "objective": "Analyze lectures, sources, and items sequentially; verify findings and extract atomic insight units without stalling.",
            "feeds_to": ["ontology"],
            "continuous": True,
            "gates": [],
            "allowed_paths": ["research/", "notes/", "data/"],
        },
        {
            "track": "ontology",
            "role": "ontologist",
            "objective": "Formalize verified insight units into explicit ontology schemas, relationships, and structural rules.",
            "feeds_to": ["authoring"],
            "continuous": True,
            "gates": [],
            "allowed_paths": ["ontology/", "specs/", "schemas/"],
        },
        {
            "track": "authoring",
            "role": "implementer",
            "objective": "Implement generation and verification algorithms from ontology rules and verify locally with tests on rev/* branches.",
            "feeds_to": [],
            "continuous": True,
            "gates": ["UNIT_TESTS"],
            "allowed_paths": ["src/", "tests/", "scripts/"],
        },
    ],
    "ux-ui-impl-3track": [
        {
            "track": "ux-research",
            "role": "ux-researcher",
            "objective": "Define target personas, ergonomic workflows, and user friction hypotheses grounded in evidence.",
            "feeds_to": ["ui-research"],
            "continuous": True,
            "gates": [],
            "allowed_paths": ["research/", "ux/", "personas/"],
        },
        {
            "track": "ui-research",
            "role": "ui-designer",
            "objective": "Translate verified UX journeys into layout hierarchy, visual clarity, and concrete UI specifications.",
            "feeds_to": ["skill-impl"],
            "continuous": True,
            "gates": [],
            "allowed_paths": ["ui/", "specs/", "design/"],
        },
        {
            "track": "skill-impl",
            "role": "implementer",
            "objective": "Codify verified UX/UI specs into skills and UI artifacts and verify with lints and checks on rev/* branches.",
            "feeds_to": [],
            "continuous": True,
            "gates": ["LINT_AND_TESTS"],
            "allowed_paths": ["skills/", "src/", "ui/", "tests/"],
        },
    ],
}

_MOD_CACHE = {}


def _now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def _now_iso():
    return _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(ts):
    if not isinstance(ts, str) or not ts:
        return None
    try:
        dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(datetime.timezone.utc)


def _age_min(ts, current=None):
    dt = _parse_iso(ts)
    if dt is None:
        return None
    ref = current if current is not None else _now_utc()
    return (ref - dt).total_seconds() / 60.0


def _clean_term(value):
    return _TERM_CTRL_RE.sub("", str(value))


def _clean_oneline(value):
    return re.sub(r"[\r\n\t]+", " ", _clean_term(value)).strip()


def _redact_pem_keys(value):
    if "PRIVATE KEY-----" not in value and "PGP PRIVATE KEY BLOCK-----" not in value:
        return value
    lines = value.splitlines(keepends=True)
    out = []
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
                out.append("[REDACTED]" + eol)
                i = end_idx + 1
                continue
            eol = "\n" if line.endswith("\n") else ""
            j = i + 1
            saw_header = False
            while j < n:
                sj = lines[j].rstrip("\r\n")
                if _PEM_HEADER_LINE_RE.match(sj):
                    saw_header = True
                    eol = "\n" if lines[j].endswith("\n") else eol
                    j += 1
                    continue
                if sj.strip() == "" and saw_header and j + 1 < n and _PEM_B64_LINE_RE.match(lines[j + 1].rstrip("\r\n")):
                    saw_header = False
                    eol = "\n" if lines[j].endswith("\n") else eol
                    j += 1
                    continue
                if _PEM_B64_LINE_RE.match(sj):
                    saw_header = False
                    eol = "\n" if lines[j].endswith("\n") else eol
                    j += 1
                    continue
                break
            out.append("[REDACTED]" + eol)
            i = j
            continue
        if _PEM_END_RE.search(stripped):
            if out and _PGP_CRC24_LINE_RE.match(out[-1].rstrip("\r\n")):
                out.pop()
            if out and _PEM_PADDED_TAIL_RE.match(out[-1].rstrip("\r\n")):
                out.pop()
            elif (
                len(out) >= 2
                and _PEM_SHORT_B64_LINE_RE.match(out[-1].rstrip("\r\n"))
                and (_PEM_BODY_LINE_RE.match(out[-2].rstrip("\r\n")) or out[-2].rstrip("\r\n") == "[REDACTED]")
            ):
                out.pop()
            while out and _PEM_BODY_LINE_RE.match(out[-1].rstrip("\r\n")):
                out.pop()
            eol = "\n" if line.endswith("\n") else ""
            out.append("[REDACTED]" + eol)
            i += 1
            continue
        out.append(line)
        i += 1
    return "".join(out)


def _redact_text(value):
    return _SECRET_RE.sub("[REDACTED]", _redact_pem_keys(_clean_term(value)))


def _redact_tree(value):
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, list):
        return [_redact_tree(v) for v in value]
    if isinstance(value, dict):
        return {(_redact_text(k) if isinstance(k, str) else k): _redact_tree(v) for k, v in value.items()}
    return value


def _safe_int(value, default=0, minimum=None):
    if isinstance(value, bool) or value is None:
        return default
    if isinstance(value, int):
        out = value
    elif isinstance(value, str) and re.fullmatch(r"\s*[+-]?\d+\s*", value):
        out = int(value)
    else:
        return default
    if minimum is not None and out < minimum:
        return default
    return out


def _die(msg, code=2):
    print("dots_pilot: " + _clean_oneline(_redact_text(msg)), file=sys.stderr, flush=True)
    raise SystemExit(code)


def _validate_token(label, value):
    if not isinstance(value, str) or not TOKEN_RE.fullmatch(value):
        _die(f"{label} must match {TOKEN_RE.pattern} (got {value!r})")
    return value


def _check_no_symlink_under(base, path):
    base_abs = os.path.abspath(base)
    target_abs = os.path.abspath(path)
    try:
        rel = os.path.relpath(target_abs, base_abs)
    except ValueError:
        return
    if rel in (".", "..") or rel.startswith(".." + os.sep):
        return
    cur = base_abs
    for part in rel.split(os.sep):
        if not part or part == ".":
            continue
        cur = os.path.join(cur, part)
        if os.path.islink(cur):
            raise OSError(f"symlink not allowed in private path: {cur}")


def _resolve_safe_pilot_path(path):
    if not isinstance(path, str) or any(ch in path for ch in ("\n", "\r", "\x00")):
        _die("pilot directory must not contain newlines or NUL bytes")
    expanded = os.path.abspath(os.path.expanduser(path))
    parent = os.path.dirname(expanded)
    leaf = os.path.basename(expanded)
    if os.path.islink(expanded):
        _die(f"pilot directory must not be a symlink: {expanded}")
    if parent:
        cur = os.path.sep if os.path.isabs(parent) else "."
        for part in parent.split(os.path.sep):
            if not part or part == ".":
                continue
            cur = os.path.join(cur, part)
            if os.path.islink(cur):
                try:
                    lst = os.lstat(cur)
                except OSError as exc:
                    _die(f"cannot inspect pilot parent {cur}: {exc}")
                # Allow OS system links like macOS /tmp -> private/tmp owned by root
                if lst.st_uid != 0:
                    _die(f"untrusted symlink in pilot parent path: {cur}")
        parent_real = os.path.realpath(parent)
        resolved = os.path.join(parent_real, leaf)
    else:
        resolved = expanded
    if any(ch in resolved for ch in ("\n", "\r", "\x00")):
        _die("pilot directory must not contain newlines or NUL bytes")
    if os.path.islink(resolved):
        _die(f"pilot directory must not be a symlink: {resolved}")
    return resolved


def _ensure_private_dir(path):
    apath = _resolve_safe_pilot_path(path)
    os.makedirs(apath, mode=0o700, exist_ok=True)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    o_dir = getattr(os, "O_DIRECTORY", 0)
    try:
        dfd = os.open(apath, os.O_RDONLY | o_dir | nofollow | os.O_NONBLOCK)
    except OSError as exc:
        _die(f"unsafe pilot directory {apath}: {exc}")
    try:
        st = os.fstat(dfd)
        if not stat.S_ISDIR(st.st_mode):
            _die(f"pilot path is not a directory: {apath}")
        if st.st_uid != os.geteuid():
            _die(f"pilot directory uid mismatch: {apath}")
        os.fchmod(dfd, 0o700)
    finally:
        with contextlib.suppress(OSError):
            os.close(dfd)
    return apath


def _read_private_bytes(path, base, max_bytes=MAX_JSON_BYTES, require_strict_mode=True):
    _check_no_symlink_under(base, path)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, os.O_RDONLY | nofollow | os.O_NONBLOCK)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise OSError("not a regular file")
        if st.st_nlink > 1:
            raise OSError("hardlinked file rejected")
        if st.st_uid != os.geteuid():
            raise OSError("file uid mismatch")
        if require_strict_mode and (st.st_mode & 0o077) != 0:
            raise OSError("file permissions too permissive (expected 0600)")
        chunks = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise OSError(f"file exceeds {max_bytes} bytes")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)


def _load_private_json(path, base, default=None, require_strict_mode=True):
    if not os.path.lexists(path):
        return default
    raw = _read_private_bytes(path, base, require_strict_mode=require_strict_mode)
    return json.loads(raw.decode("utf-8"))


def _load_private_json_safe(path, base, default=None, require_strict_mode=True):
    try:
        return _load_private_json(path, base, default=default, require_strict_mode=require_strict_mode)
    except (OSError, ValueError, UnicodeDecodeError, RecursionError):
        return default


def _write_private_atomic(path, text_data, base):
    _check_no_symlink_under(base, os.path.dirname(path))
    _check_no_symlink_under(base, path)
    parent = os.path.dirname(path)
    os.makedirs(parent, mode=0o700, exist_ok=True)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    tmp = os.path.join(parent, f".{os.path.basename(path)}.tmp.{os.getpid()}.{secrets.token_hex(4)}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow, 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
            raise OSError("unsafe temporary file")
        data = text_data.encode("utf-8")
        view = memoryview(data)
        sent = 0
        while sent < len(data):
            written = os.write(fd, view[sent:])
            if written <= 0:
                raise OSError("short write")
            sent += written
        os.fsync(fd)
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    os.close(fd)
    if os.path.islink(path):
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise OSError(f"target is a symlink: {path}")
    os.replace(tmp, path)
    os.chmod(path, 0o600)


def _append_private_jsonl(path, obj, base):
    _check_no_symlink_under(base, os.path.dirname(path))
    _check_no_symlink_under(base, path)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | nofollow, 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink > 1 or st.st_uid != os.geteuid():
            raise OSError("unsafe jsonl log file")
        if (st.st_mode & 0o077) != 0:
            os.fchmod(fd, 0o600)
        os.write(fd, line)
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)


@contextlib.contextmanager
def _pilot_lock(pilot_dir):
    lock_path = os.path.join(pilot_dir, "pilot.lock")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    fd = None
    acquired = False
    err_msg = None
    try:
        _check_no_symlink_under(pilot_dir, lock_path)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | nofollow | os.O_NONBLOCK, 0o600)
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink > 1 or st.st_uid != os.geteuid():
            err_msg = "unsafe lock file"
            os.close(fd)
            fd = None
        else:
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                acquired = False
    except OSError as exc:
        err_msg = str(exc)
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)
            fd = None
    if err_msg:
        _die(f"cannot lock pilot directory: {err_msg}")
    try:
        yield acquired
    finally:
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)


def _fstat_regular_fd(fd, apath, max_bytes=4 * 1024 * 1024):
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode):
        raise OSError(f"expected regular file: {apath}")
    if st.st_nlink > 1:
        raise OSError(f"refusing hardlinked file: {apath}")
    chunks = []
    total = 0
    while True:
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise OSError(f"file exceeds {max_bytes} bytes: {apath}")
        chunks.append(chunk)
    return b"".join(chunks)


def _safe_read_regular_bytes(path, max_bytes=4 * 1024 * 1024):
    apath = os.path.abspath(os.path.expanduser(path))
    if os.path.islink(apath):
        raise OSError(f"refusing symlink: {apath}")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(apath, os.O_RDONLY | nofollow | os.O_NONBLOCK)
    try:
        return _fstat_regular_fd(fd, apath, max_bytes=max_bytes), apath
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)


def _sha256_file(path):
    raw, _ = _safe_read_regular_bytes(path)
    return hashlib.sha256(raw).hexdigest()


def _load_pinned_module(mod_name, file_path, expected_sha256=None):
    if not isinstance(expected_sha256, str) or not HEX64_RE.fullmatch(expected_sha256):
        raise RuntimeError(f"{mod_name} expected_sha256 missing or invalid")
    raw, apath = _safe_read_regular_bytes(file_path)
    actual = hashlib.sha256(raw).hexdigest()
    if actual != expected_sha256:
        raise RuntimeError(f"{mod_name} sha256 mismatch expected={expected_sha256} actual={actual}")
    cached = _MOD_CACHE.get(apath)
    if cached is not None and cached[0] == actual:
        return cached[1]
    mod_full_name = f"_dotomation_pilot_{mod_name}"
    mod = types.ModuleType(mod_full_name)
    mod.__file__ = apath
    sys.modules[mod_full_name] = mod
    exec(compile(raw.decode("utf-8"), apath, "exec"), mod.__dict__)
    _MOD_CACHE[apath] = (actual, mod)
    return mod


def _parse_argv_json(spec, label):
    if spec is None:
        return None
    if isinstance(spec, list):
        arr = spec
    else:
        try:
            arr = json.loads(spec)
        except ValueError as exc:
            _die(f"{label} must be JSON argv list: {exc}")
    if not isinstance(arr, list) or not arr or not all(isinstance(x, str) and x for x in arr):
        _die(f"{label} must be a non-empty JSON list of non-empty strings")
    return arr


def _safe_placeholder(name, val):
    if name in ("dot", "agent", "project", "id", "track"):
        sv = _clean_oneline(val)
        return sv if TOKEN_RE.fullmatch(sv) else "none"
    if name == "lanes":
        s = re.sub(r"[^A-Za-z0-9._:,-]+", ",", _clean_oneline(val))
        s = re.sub(r",+", ",", s).strip(",")
        while s.startswith("-"):
            s = s.lstrip("-,").strip()
        return (s[:200] if s else "-")
    if name in ("why", "task", "age"):
        s = re.sub(r"[^A-Za-z0-9._:/@+,=\s-]+", "_", _clean_oneline(val))
        s = re.sub(r"\s+", " ", s).strip()
        while s.startswith("-"):
            s = s.lstrip("-").strip()
        return (s[:200] if s else "-")
    # For free-form {text}/{reason}: preserve multiline safely for argv execution (no shell), strip term controls/NUL and secrets
    s = _clean_term(str(val).replace("\x00", "")).strip()
    s = _redact_text(s)
    while s.startswith("-"):
        s = s.lstrip("-").strip()
    if len(s) > 4000:
        s = s[:3997] + "..."
    return s or "-"


def _format_cmd_argv(argv, values):
    return [PLACEHOLDER_RE.sub(lambda m: _safe_placeholder(m.group(1), values.get(m.group(1), "")), part) for part in argv]


def _validate_aside_target(url):
    if not isinstance(url, str) or not ASIDE_URL_RE.fullmatch(url):
        raise ValueError("aside target_url must match https://chatgpt.com/dots/<id>")
    return url


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)


def _open_https_no_redirect(req, timeout=10):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirectHandler())
    return opener.open(req, timeout=timeout)


def _validate_webhook_url(url):
    if not isinstance(url, str) or not url.strip():
        return False, "empty webhook URL"
    raw = url.strip()
    if any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in raw):
        return False, "webhook URL contains whitespace or control chars"
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme != "https" or not parsed.netloc or not parsed.hostname:
        return False, "webhook URL must be valid https://"
    if parsed.username is not None or parsed.password is not None:
        return False, "webhook URL must not contain userinfo"
    try:
        _ = parsed.port
    except ValueError:
        return False, "webhook URL has invalid port"
    return True, raw


def _verify_loopback_host(host, port):
    h_low = (host or "").lower()
    if h_low not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("cdp_url must target loopback (127.0.0.1, localhost, or ::1)")
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ValueError(f"cannot resolve loopback host {host}: {exc}") from exc
    if not infos:
        raise ValueError(f"no addresses resolved for {host}")
    for _fam, _type, _proto, _canon, sockaddr in infos:
        ip_str = sockaddr[0].split("%", 1)[0]
        if not ipaddress.ip_address(ip_str).is_loopback:
            raise ValueError(f"non-loopback resolution for {host}: {ip_str}")


def _validate_cdp_url(cdp_url):
    if not isinstance(cdp_url, str) or not cdp_url.strip():
        raise ValueError("cdp_url must be a non-empty loopback URL")
    parsed = urllib.parse.urlparse(cdp_url.strip())
    if parsed.scheme not in ("http", "https"):
        raise ValueError("cdp_url scheme must be http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("cdp_url must not contain credentials")
    host = (parsed.hostname or "").lower()
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    _verify_loopback_host(host, port)
    return parsed.scheme, host, port


def _ws_send_frame(sock, text_payload):
    data = text_payload.encode("utf-8")
    mask = secrets.token_bytes(4)
    header = bytearray([0x81])  # FIN + text opcode
    n = len(data)
    if n < 126:
        header.append(0x80 | n)
    elif n < 65536:
        header.append(0x80 | 126)
        header.extend(n.to_bytes(2, "big"))
    else:
        header.append(0x80 | 127)
        header.extend(n.to_bytes(8, "big"))
    header.extend(mask)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    sock.sendall(bytes(header) + masked)


def _ws_recv_exact(sock, buf, n):
    while len(buf) < n:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("unexpected WebSocket EOF")
        buf.extend(chunk)
    out = bytes(buf[:n])
    del buf[:n]
    return out


def _ws_recv_frame(sock, buf):
    first2 = _ws_recv_exact(sock, buf, 2)
    b1, b2 = first2[0], first2[1]
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
    length = b2 & 0x7F
    if length == 126:
        length = int.from_bytes(_ws_recv_exact(sock, buf, 2), "big")
    elif length == 127:
        length = int.from_bytes(_ws_recv_exact(sock, buf, 8), "big")
    if length > MAX_JSON_BYTES:
        raise ValueError("WebSocket frame too large")
    mask = _ws_recv_exact(sock, buf, 4) if masked else b""
    payload = _ws_recv_exact(sock, buf, length)
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def _cdp_send_aside(cdp_url, target_url, message, timeout=15):
    _validate_aside_target(target_url)
    scheme, host, port = _validate_cdp_url(cdp_url)
    url_host = f"[{host}]" if ":" in host else host
    list_url = f"{scheme}://{url_host}:{port}/json/list"
    req = urllib.request.Request(list_url, method="GET")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirectHandler())
    with opener.open(req, timeout=timeout) as resp:
        tabs = json.loads(resp.read(MAX_JSON_BYTES).decode("utf-8"))
    if not isinstance(tabs, list):
        raise RuntimeError("unexpected CDP /json/list response")
    ws_url = None
    norm_tgt = target_url.rstrip("/")
    for tab in tabs:
        if not isinstance(tab, dict):
            continue
        tab_u = str(tab.get("url") or "")
        url_matches = (
            tab_u == target_url
            or tab_u.startswith(target_url + "?")
            or tab_u.startswith(target_url + "#")
            or tab_u.startswith(norm_tgt + "/")
        )
        if tab.get("type") == "page" and url_matches:
            cand = tab.get("webSocketDebuggerUrl")
            if isinstance(cand, str) and cand:
                ws_url = cand
                break
    if not ws_url:
        raise RuntimeError(f"no open browser tab matching {target_url}")
    wsp = urllib.parse.urlparse(ws_url)
    ws_host = (wsp.hostname or "").lower()
    if wsp.scheme != "ws" or wsp.username is not None or wsp.password is not None:
        raise RuntimeError("refused invalid CDP webSocketDebuggerUrl")
    ws_port = wsp.port or port
    _verify_loopback_host(ws_host, ws_port)
    ws_path = (wsp.path or "/") + (f"?{wsp.query}" if wsp.query else "")
    if any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in ws_path):
        raise RuntimeError("refused invalid characters in CDP webSocketDebuggerUrl path")

    js_expr = (
        "(async function(text){"
        "const el = document.querySelector('#prompt-textarea, textarea[data-id], form textarea, [contenteditable=\"true\"]');"
        "if(!el) return {ok:false, reason:'no_prompt_input'};"
        "el.focus();"
        "if('value' in el && el.tagName === 'TEXTAREA'){"
        "const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value').set;"
        "setter.call(el, text);"
        "el.dispatchEvent(new Event('input', {bubbles:true}));"
        "} else {"
        "el.textContent = text;"
        "el.dispatchEvent(new InputEvent('input', {bubbles:true, data:text}));"
        "}"
        "await new Promise(r => setTimeout(r, 80));"
        "const btn = document.querySelector('button[data-testid=\"send-button\"], button[aria-label*=\"Send\"], form button[type=\"submit\"]');"
        "if(btn && !btn.disabled){ btn.click(); return {ok:true, method:'button'}; }"
        "el.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter', code:'Enter', keyCode:13, which:13, bubbles:true}));"
        "return {ok:true, method:'enter'};"
        "})(" + json.dumps(message, ensure_ascii=False) + ")"
    )
    rpc_req = json.dumps({
        "id": 1,
        "method": "Runtime.evaluate",
        "params": {"expression": js_expr, "awaitPromise": True, "returnByValue": True},
    })

    key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    expected_accept = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    ).decode("ascii")
    ws_hdr_host = f"[{ws_host}]" if ":" in ws_host else ws_host
    handshake = (
        f"GET {ws_path} HTTP/1.1\r\n"
        f"Host: {ws_hdr_host}:{ws_port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    ).encode("ascii")

    with socket.create_connection((ws_host, ws_port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(handshake)
        buf = bytearray()
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("CDP WebSocket handshake closed")
            buf.extend(chunk)
            if len(buf) > 16384:
                raise ConnectionError("CDP handshake too large")
        head_end = buf.index(b"\r\n\r\n")
        head_raw = bytes(buf[:head_end]).decode("latin-1")
        del buf[:head_end + 4]
        status_line = head_raw.split("\r\n", 1)[0]
        if " 101 " not in status_line:
            raise ConnectionError(f"CDP WebSocket rejected: {status_line}")
        accept_hdrs = [
            ln.split(":", 1)[1].strip()
            for ln in head_raw.split("\r\n")[1:]
            if ":" in ln and ln.split(":", 1)[0].strip().lower() == "sec-websocket-accept"
        ]
        if expected_accept not in accept_hdrs:
            raise ConnectionError("CDP Sec-WebSocket-Accept mismatch")

        _ws_send_frame(sock, rpc_req)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            opcode, payload = _ws_recv_frame(sock, buf)
            if opcode == 0x8:
                raise ConnectionError("CDP closed connection before reply")
            if opcode == 0x9:
                if len(payload) > 125:
                    raise ConnectionError("CDP ping frame too large")
                # Ping -> Pong
                pong_mask = secrets.token_bytes(4)
                hdr = bytearray([0x8A, 0x80 | len(payload)]) + pong_mask
                sock.sendall(bytes(hdr) + bytes(b ^ pong_mask[i % 4] for i, b in enumerate(payload)))
                continue
            if opcode != 0x1:
                continue
            resp_obj = json.loads(payload.decode("utf-8"))
            if isinstance(resp_obj, dict) and resp_obj.get("id") == 1:
                val = ((resp_obj.get("result") or {}).get("result") or {}).get("value")
                if isinstance(val, dict) and val.get("ok"):
                    return val
                raise RuntimeError(f"CDP eval failed: {resp_obj}")
    raise TimeoutError("CDP Runtime.evaluate timed out")


def _send_chat_message(cfg, message, values=None):
    chat = cfg.get("chat") if isinstance(cfg.get("chat"), dict) else {}
    transport = chat.get("transport") or "stdout"
    safe_msg = _redact_text(_clean_term(message)).strip()
    if not safe_msg:
        return True, "empty"
    if transport == "stdout":
        print(f"PILOT_CHAT {safe_msg}", flush=True)
        return True, "stdout"
    if transport == "command":
        argv = _parse_argv_json(chat.get("command"), "chat.command")
        if not argv:
            return False, "chat.command not configured"
        vals = dict(values or {})
        vals.setdefault("dot", cfg.get("dot") or "none")
        vals.setdefault("project", cfg.get("project") or "none")
        vals["text"] = safe_msg
        final_argv = _format_cmd_argv(argv, vals)
        try:
            proc = subprocess.run(final_argv, stdin=subprocess.DEVNULL, timeout=MAX_CMD_TIMEOUT)
            return proc.returncode == 0, f"exit={proc.returncode}"
        except subprocess.TimeoutExpired:
            return False, "timeout"
        except OSError as exc:
            return False, f"os_error:{exc.errno}"
    if transport == "aside":
        target_url = chat.get("target_url")
        cdp_url = chat.get("cdp_url") or "http://127.0.0.1:9222"
        try:
            res = _cdp_send_aside(cdp_url, target_url, safe_msg, timeout=15)
            return True, f"aside:{res.get('method', 'ok')}"
        except Exception as exc:
            return False, _clean_oneline(_redact_text(str(exc)))
    return False, f"unknown transport {transport}"


def _send_notification(cfg, title, body_text):
    notify = cfg.get("notify") if isinstance(cfg.get("notify"), dict) else {}
    mode = notify.get("mode") or "stdout"
    msg = _redact_text(_clean_oneline(f"[{title}] {body_text}"))
    if mode == "stdout":
        print(f"PILOT_NOTIFY {msg}", flush=True)
        return True, "stdout"
    if mode == "command":
        argv = _parse_argv_json(notify.get("command"), "notify.command")
        if not argv:
            return False, "notify.command not configured"
        final_argv = _format_cmd_argv(argv, {"dot": cfg.get("dot") or "none", "project": cfg.get("project") or "none", "text": msg, "reason": title})
        try:
            proc = subprocess.run(final_argv, stdin=subprocess.DEVNULL, timeout=MAX_CMD_TIMEOUT)
            return proc.returncode == 0, f"exit={proc.returncode}"
        except subprocess.TimeoutExpired:
            return False, "timeout"
        except OSError as exc:
            return False, f"os_error:{exc.errno}"
    if mode == "webhook":
        env_name = notify.get("webhook_url_env") or "DOTS_PILOT_WEBHOOK_URL"
        if not isinstance(env_name, str) or not ENV_NAME_RE.fullmatch(env_name):
            return False, "invalid webhook_url_env"
        raw_url = os.environ.get(env_name, "").strip()
        if not raw_url:
            return False, f"env {env_name} not set"
        ok_u, val_or_err = _validate_webhook_url(raw_url)
        if not ok_u:
            return False, val_or_err
        payload = json.dumps({"text": msg, "content": msg}, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            val_or_err,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": f"dotomation-pilot/{VERSION}"},
            method="POST",
        )
        try:
            with _open_https_no_redirect(req, timeout=10) as resp:
                if 200 <= resp.status < 300:
                    return True, f"http:{resp.status}"
                return False, f"http:{resp.status}"
        except urllib.error.HTTPError as exc:
            return False, f"http:{exc.code}"
        except Exception as exc:
            return False, f"webhook_error:{exc.__class__.__name__}"
    return False, f"unknown notify mode {mode}"


def _build_monitor_cfg(cfg):
    ops_base = os.path.expanduser(cfg.get("ops_base") or DEFAULT_OPS_BASE)
    dh = cfg.get("daemon_health") if isinstance(cfg.get("daemon_health"), dict) else {}
    default_limits = {
        "tail_bytes": 262144,
        "roster_max_bytes": 1048576,
        "command_timeout_seconds": 25,
        "gh_timeout_seconds": 25,
    }
    sess_dir = os.path.expanduser(str(dh["sessions_dir"]).rstrip("/")) if dh.get("sessions_dir") else None
    mpath = cfg.get("monitor_config")
    if mpath:
        raw_mon, _ = _safe_read_regular_bytes(mpath, max_bytes=MAX_JSON_BYTES)
        loaded = json.loads(raw_mon.decode("utf-8"))
        if isinstance(loaded, dict):
            limits = dict(default_limits)
            if isinstance(loaded.get("limits"), dict):
                limits.update(loaded["limits"])
            loaded["limits"] = limits
            loaded.setdefault("thresholds", {"stall_minutes": 30, "active_window_minutes": 15, "long_window_minutes": 60})
            loaded.setdefault("github", {"enabled": False, "repos": []})
            loaded.setdefault("activity", [])
            loaded.setdefault("dots", {cfg["dot"]: {"account": 1, "name": cfg["dot"]}})
            loaded.setdefault("daemon_health", {})
            if isinstance(loaded["daemon_health"], dict):
                loaded["daemon_health"].setdefault("enabled", bool(dh.get("enabled", True)))
                loaded["daemon_health"].setdefault("tail_bytes", 131072)
                loaded["daemon_health"].setdefault("max_session_files", 20)
                loaded["daemon_health"].setdefault("window_minutes", 30)
                if sess_dir and not loaded["daemon_health"].get("sessions_dir"):
                    loaded["daemon_health"]["sessions_dir"] = sess_dir
            return loaded
    act_entries = []
    for i, p in enumerate(dh.get("activity_paths") or []):
        if isinstance(p, str) and p.strip():
            entry = {
                "dot": cfg["dot"],
                "host": "local",
                "label": f"local-{i + 1}",
                "path": os.path.expanduser(p.strip()),
            }
            if sess_dir:
                entry["sessions_dir"] = sess_dir
            act_entries.append(entry)
    dh_cfg = {
        "enabled": bool(dh.get("enabled", True)),
        "window_minutes": 30,
        "max_session_files": 20,
        "tail_bytes": 131072,
    }
    if sess_dir:
        dh_cfg["sessions_dir"] = sess_dir
    return {
        "ledger": {
            "host": "local",
            "root": os.path.join(ops_base, "live"),
            "dots": [cfg["dot"]],
        },
        "dots": {cfg["dot"]: {"account": 1, "name": cfg["dot"]}},
        "activity": act_entries,
        "github": {"enabled": False, "repos": []},
        "thresholds": {"stall_minutes": 30, "active_window_minutes": 15, "long_window_minutes": 60},
        "limits": default_limits,
        "daemon_health": dh_cfg,
    }


def _decode_monitor_state(doc):
    st = {"agents": {}, "activity": {}, "errors": set(), "daemon_health": {}}
    if not isinstance(doc, dict):
        return st
    for raw_k, val in (doc.get("agents") or {}).items():
        if isinstance(raw_k, str) and "\t" in raw_k and isinstance(val, dict):
            st["agents"][tuple(raw_k.split("\t", 1))] = val
    for k, val in (doc.get("activity") or {}).items():
        if isinstance(k, str) and isinstance(val, bool):
            st["activity"][k] = val
    for item in doc.get("errors") or []:
        if isinstance(item, list) and len(item) == 2 and all(isinstance(x, str) for x in item):
            st["errors"].add((item[0], item[1]))
    for raw_k, val in (doc.get("daemon_health") or {}).items():
        if isinstance(raw_k, str) and "\t" in raw_k and isinstance(val, dict):
            st["daemon_health"][tuple(raw_k.split("\t", 1))] = val
    return st


def _encode_monitor_state(st):
    return {
        "agents": {f"{k[0]}\t{k[1]}": v for k, v in (st.get("agents") or {}).items() if isinstance(k, tuple) and len(k) == 2},
        "activity": {str(k): bool(v) for k, v in (st.get("activity") or {}).items()},
        "errors": sorted([list(x) for x in (st.get("errors") or set()) if isinstance(x, tuple) and len(x) == 2]),
        "daemon_health": {f"{k[0]}\t{k[1]}": v for k, v in (st.get("daemon_health") or {}).items() if isinstance(k, tuple) and len(k) == 2},
    }


def _cooldown_active(ledger_bucket, key, cooldown_min, current=None):
    ts = ledger_bucket.get(key)
    age = _age_min(ts, current=current)
    if age is None or age < 0:
        return False
    return age < cooldown_min


def _verify_dots_pin(cfg):
    raw_dots = cfg.get("dots_py")
    if raw_dots is not None and not isinstance(raw_dots, str):
        return False, None, None, "dots_py must be a string"
    dots_py = os.path.abspath(os.path.expanduser(raw_dots or os.path.join(_TOOL_DIR, "dots.py")))
    expected = cfg.get("dots_sha256")
    try:
        actual = _sha256_file(dots_py)
    except OSError as exc:
        return False, dots_py, None, str(exc)
    if not isinstance(expected, str) or not HEX64_RE.fullmatch(expected):
        return False, dots_py, actual, "dots_sha256 missing or invalid; re-run init --force"
    if actual != expected:
        return False, dots_py, actual, f"sha256 mismatch expected={expected} actual={actual}"
    return True, dots_py, actual, None


def _verify_monitor_pin(cfg):
    mon_py = cfg.get("dots_monitor_py")
    if not mon_py:
        return False, None, None, "dots_monitor_py not configured"
    if not isinstance(mon_py, str):
        return False, None, None, "dots_monitor_py must be a string"
    mon_abs = os.path.abspath(os.path.expanduser(mon_py))
    expected = cfg.get("monitor_sha256")
    try:
        actual = _sha256_file(mon_abs)
    except OSError as exc:
        return False, mon_abs, None, str(exc)
    if not isinstance(expected, str) or not HEX64_RE.fullmatch(expected):
        return False, mon_abs, actual, "monitor_sha256 missing or invalid; re-run init --force"
    if actual != expected:
        return False, mon_abs, actual, f"sha256 mismatch expected={expected} actual={actual}"
    return True, mon_abs, actual, None


def _run_dots(dots_py, ops_base, args, check=False, env_extra=None, expected_sha256=None):
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["DOTS_AUTO_UPDATE"] = "0"
    env["DOTS_OPS_BASE"] = ops_base
    if env_extra:
        env.update(env_extra)
    if expected_sha256 is not None:
        if not isinstance(expected_sha256, str) or not HEX64_RE.fullmatch(expected_sha256):
            msg = "dots.py expected_sha256 missing or invalid"
            if check:
                raise RuntimeError(msg)
            return subprocess.CompletedProcess([dots_py] + list(args), 2, "", f"dots_pilot: {msg}\n")
        apath = os.path.abspath(os.path.expanduser(dots_py))
        if os.path.islink(apath):
            msg = f"refusing symlink: {apath}"
            if check:
                raise RuntimeError(msg)
            return subprocess.CompletedProcess([apath] + list(args), 2, "", f"dots_pilot: {msg}\n")
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        fd = None
        try:
            fd = os.open(apath, os.O_RDONLY | nofollow | os.O_NONBLOCK)
            raw = _fstat_regular_fd(fd, apath)
            actual = hashlib.sha256(raw).hexdigest()
            if actual != expected_sha256:
                raise RuntimeError(f"dots.py sha256 mismatch expected={expected_sha256} actual={actual}")
            os.lseek(fd, 0, os.SEEK_SET)
        except (OSError, RuntimeError) as exc:
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.close(fd)
            if check:
                raise RuntimeError(str(exc)) from exc
            return subprocess.CompletedProcess([apath] + list(args), 2, "", f"dots_pilot: {exc}\n")
        runner = (
            "import hashlib, os, sys\n"
            "fd = int(sys.argv[1])\n"
            "apath = sys.argv[2]\n"
            "exp = sys.argv[3]\n"
            "chunks = []\n"
            "while True:\n"
            "    c = os.read(fd, 65536)\n"
            "    if not c:\n"
            "        break\n"
            "    chunks.append(c)\n"
            "os.close(fd)\n"
            "raw = b''.join(chunks)\n"
            "if hashlib.sha256(raw).hexdigest() != exp:\n"
            "    raise RuntimeError('dots.py sha256 mismatch on fd')\n"
            "sys.argv = [apath] + sys.argv[4:]\n"
            "ns = {'__name__': '__main__', '__file__': apath}\n"
            "exec(compile(raw.decode('utf-8'), apath, 'exec'), ns)\n"
        )
        cmd = [sys.executable, "-I", "-B", "-c", runner, str(fd), apath, expected_sha256] + list(args)
        try:
            proc = subprocess.run(
                cmd, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120, pass_fds=(fd,)
            )
        finally:
            with contextlib.suppress(OSError):
                os.close(fd)
    else:
        cmd = [sys.executable, dots_py] + list(args)
        proc = subprocess.run(cmd, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120)
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}")
    return proc


def _resolve_tracks_from_cfg(cfg):
    raw_tracks = cfg.get("tracks")
    if isinstance(raw_tracks, list) and raw_tracks:
        return raw_tracks
    preset_name = cfg.get("preset") or "generic"
    return list(PRESETS.get(preset_name, PRESETS["generic"]))


def _apply_tracks_to_ops(cfg, dots_py, force=False, dots_sha256=None):
    ops_base = os.path.abspath(os.path.expanduser(cfg.get("ops_base") or DEFAULT_OPS_BASE))
    dot = _validate_token("dot", cfg["dot"])
    project = _validate_token("project", cfg["project"])
    if project in RESERVED_OPS_DIRS:
        _die(f"project {project!r} is reserved for dotomation ops dirs")
    sha = dots_sha256 if dots_sha256 is not None else cfg.get("dots_sha256")
    tracks = _resolve_tracks_from_cfg(cfg)
    results = []
    # Ensure ops base exists (Mode 1 dots-only idempotent init when dots.json absent)
    if not os.path.exists(os.path.join(ops_base, "dots.json")):
        _run_dots(dots_py, ops_base, [
            "init", "--mode", "dots-only", "--dot", dot, "--project", f"{project}={project}-main", "--yes"
        ], check=True, expected_sha256=sha)
    for t in tracks:
        if not isinstance(t, dict) or not t.get("track"):
            continue
        tname = _validate_token("track", t.get("track"))
        role = str(t.get("role") or "").strip()
        obj = str(t.get("objective") or f"Execute {tname} track continuously.")
        cmd = [
            "loop", "init", dot, tname,
            "--project", project,
            f"--objective={obj}",
        ]
        if role:
            cmd.append(f"--role={role}")
        if t.get("astra_role"):
            cmd.extend(["--astra-role", str(t["astra_role"])])
        if cfg.get("require_astra_ack") or t.get("require_astra_ack"):
            cmd.append("--require-astra-ack")
        if t.get("continuous", True):
            cmd.append("--continuous")
        for ft in t.get("feeds_to") or []:
            cmd.extend(["--feeds-to", _validate_token("feeds_to", ft)])
        for ap in t.get("allowed_paths") or []:
            cmd.append(f"--allowed-path={ap}")
        for g in t.get("gates") or []:
            cmd.extend(["--gate", _validate_token("gate", g)])
        if force:
            cmd.append("--force")
        proc = _run_dots(dots_py, ops_base, cmd, check=True, expected_sha256=sha)
        results.append({"track": tname, "stdout": proc.stdout.strip()})
    return results


def _format_wake_nudge(dot, project, events):
    reasons = sorted({ev.get("why") or ev.get("kind") or "wake" for ev in events})
    tracks = sorted({ev.get("track") for ev in events if ev.get("track") and ev.get("track") != "none"})
    lanes = sorted({ev.get("lanes") for ev in events if ev.get("lanes") and ev.get("lanes") not in ("-", "none")})
    agents = sorted({ev.get("agent") for ev in events if ev.get("agent") and ev.get("agent") != "none"})
    parts = [
        f"[PILOT_WAKE dot={dot} project={project}]",
        f"reasons={','.join(reasons)}",
    ]
    if tracks:
        parts.append(f"tracks={','.join(tracks)}")
    if lanes:
        parts.append(f"lanes={','.join(lanes)}")
    if agents:
        parts.append(f"agents={','.join(agents)}")
    parts.append(
        "Action: execute `dots.py loop claim` / skip single blocked items (`loop block`) / keep reversible local & rev/* progress moving directly without asking owner unless a true hard-stop occurs."
    )
    return " ".join(parts)


def _perform_daemon_heal(cfg, ledger, pilot_dir):
    dh = cfg.get("daemon_health") if isinstance(cfg.get("daemon_health"), dict) else {}
    if not dh.get("heal"):
        return False, "heal_disabled"
    heal_cooldown = max(MIN_HEAL_COOLDOWN_MIN, _safe_int(dh.get("heal_cooldown_min"), 15, 1))
    last_heal = ledger.get("last_heal_at")
    age = _age_min(last_heal)
    if age is not None and 0 <= age < heal_cooldown:
        return False, f"cooldown_active({int(age)}m<{heal_cooldown}m)"

    custom_cmd = dh.get("heal_cmd")
    if custom_cmd:
        argv = _parse_argv_json(custom_cmd, "daemon_health.heal_cmd")
    else:
        label = dh.get("unit_label")
        if not isinstance(label, str) or not UNIT_LABEL_RE.fullmatch(label):
            return False, "invalid_or_missing_unit_label"
        argv = ["launchctl", "kickstart", "-k", f"gui/{os.geteuid()}/{label}"]

    ledger["last_heal_at"] = _now_iso()
    _write_private_atomic(os.path.join(pilot_dir, "ledger.json"), json.dumps(ledger, indent=1) + "\n", pilot_dir)
    try:
        proc = subprocess.run(argv, stdin=subprocess.DEVNULL, timeout=MAX_CMD_TIMEOUT)
        ok = proc.returncode == 0
        return ok, f"exit={proc.returncode}"
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except OSError as exc:
        return False, f"os_error:{exc.errno}"


def cmd_init(args):
    pilot_dir = _ensure_private_dir(args.pilot_dir)
    base_cfg = {}
    if args.config:
        try:
            raw_cfg, _ = _safe_read_regular_bytes(args.config, max_bytes=MAX_JSON_BYTES)
            loaded = json.loads(raw_cfg.decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            _die(f"cannot load --config: {exc}")
        if not isinstance(loaded, dict):
            _die("--config must contain a JSON object")
        base_cfg = loaded
    dot = _validate_token("dot", args.dot or base_cfg.get("dot") or "DOT-A")
    project = _validate_token("project", args.project or base_cfg.get("project") or "default")
    if project in RESERVED_OPS_DIRS:
        _die(f"project {project!r} is reserved for dotomation ops dirs")
    ops_base = os.path.abspath(os.path.expanduser(args.ops_base or base_cfg.get("ops_base") or DEFAULT_OPS_BASE))
    has_custom_tracks = args.preset is None and args.config and isinstance(base_cfg.get("tracks"), list) and bool(base_cfg["tracks"])
    if args.preset is not None:
        if args.preset not in PRESETS:
            _die(f"unknown preset {args.preset!r}; choose from {list(PRESETS)}")
        preset = args.preset
    elif args.config:
        raw_preset = base_cfg.get("preset")
        if has_custom_tracks:
            preset = str(raw_preset or "custom")
            if preset != "custom" and preset not in PRESETS:
                _die(f"unknown config preset {preset!r}; choose from {list(PRESETS)} or 'custom'")
        else:
            preset = str(raw_preset or "research-3track")
            if preset not in PRESETS:
                _die(f"unknown config preset {preset!r}; choose from {list(PRESETS)}")
    else:
        preset = "research-3track"

    dots_py = os.path.abspath(os.path.expanduser(args.dots_py or os.path.join(_TOOL_DIR, "dots.py")))
    mon_py = os.path.abspath(os.path.expanduser(args.dots_monitor_py or os.path.join(_TOOL_DIR, "dots_monitor.py")))
    try:
        dots_sha = _sha256_file(dots_py)
    except OSError as exc:
        _die(f"dots.py unusable at {dots_py}: {exc}")
    mon_sha = None
    if os.path.lexists(mon_py):
        try:
            mon_sha = _sha256_file(mon_py)
        except OSError as exc:
            _die(f"dots_monitor.py unusable at {mon_py}: {exc}")
    elif args.dots_monitor_py:
        _die(f"dots_monitor.py not found at {mon_py}")

    chat_cfg = dict(base_cfg.get("chat") or {})
    if args.chat_transport:
        chat_cfg["transport"] = args.chat_transport
    chat_cfg.setdefault("transport", "stdout")
    if chat_cfg["transport"] not in ("stdout", "command", "aside"):
        _die("chat.transport must be stdout, command, or aside")
    if args.target_url:
        chat_cfg["target_url"] = args.target_url
    if chat_cfg["transport"] == "aside":
        try:
            _validate_aside_target(chat_cfg.get("target_url"))
            _validate_cdp_url(args.cdp_url or chat_cfg.get("cdp_url") or "http://127.0.0.1:9222")
        except ValueError as exc:
            _die(str(exc))
    if args.cdp_url:
        chat_cfg["cdp_url"] = args.cdp_url
    if args.chat_cmd is not None:
        chat_cfg["command"] = _parse_argv_json(args.chat_cmd, "--chat-cmd")
    chat_cfg.setdefault("board_heartbeat_min", 30)

    notify_cfg = dict(base_cfg.get("notify") or {})
    if args.notify_mode:
        notify_cfg["mode"] = args.notify_mode
    notify_cfg.setdefault("mode", "stdout")
    if notify_cfg["mode"] not in ("stdout", "command", "webhook"):
        _die("notify.mode must be stdout, command, or webhook")
    if args.webhook_url_env:
        if not ENV_NAME_RE.fullmatch(args.webhook_url_env):
            _die("--webhook-url-env must be a valid uppercase env var name")
        notify_cfg["webhook_url_env"] = args.webhook_url_env
    notify_cfg.setdefault("webhook_url_env", "DOTS_PILOT_WEBHOOK_URL")
    if args.notify_cmd is not None:
        notify_cfg["command"] = _parse_argv_json(args.notify_cmd, "--notify-cmd")

    dh_cfg = dict(base_cfg.get("daemon_health") or {})
    dh_cfg.setdefault("enabled", True)
    if args.daemon_heal:
        dh_cfg["heal"] = True
    else:
        dh_cfg.setdefault("heal", False)
    if args.unit_label:
        if not UNIT_LABEL_RE.fullmatch(args.unit_label):
            _die("--unit-label must match token format")
        dh_cfg["unit_label"] = args.unit_label
    dh_cfg.setdefault("heal_cooldown_min", 15)
    dh_cfg.setdefault("activity_paths", [])

    tracks = (
        list(base_cfg["tracks"])
        if (args.preset is None and args.config and isinstance(base_cfg.get("tracks"), list) and base_cfg["tracks"])
        else list(PRESETS.get(preset, PRESETS["research-3track"]))
    )

    cfg = {
        "v": 1,
        "ops_base": ops_base,
        "dots_py": dots_py,
        "dots_sha256": dots_sha,
        "dots_monitor_py": mon_py if os.path.isfile(mon_py) else None,
        "monitor_sha256": mon_sha,
        "monitor_config": os.path.abspath(os.path.expanduser(args.monitor_config)) if args.monitor_config else base_cfg.get("monitor_config"),
        "dot": dot,
        "project": project,
        "preset": preset,
        "require_astra_ack": bool(getattr(args, "require_astra_ack", False) or base_cfg.get("require_astra_ack", False)),
        "keep_full": bool(base_cfg.get("keep_full", True)),
        "interval_sec": _safe_int(args.interval, _safe_int(base_cfg.get("interval_sec"), 180, 15), 15),
        "cooldown_min": _safe_int(args.cooldown_min, _safe_int(base_cfg.get("cooldown_min"), 10, 1), 1),
        "tracks": tracks,
        "chat": chat_cfg,
        "daemon_health": dh_cfg,
        "notify": notify_cfg,
    }

    cfg_path = os.path.join(pilot_dir, "config.json")
    apply_sha = dots_sha
    if os.path.lexists(cfg_path) and not args.force:
        print(f"PILOT_INIT_KEPT config={cfg_path} (pass --force to overwrite)", flush=True)
        cfg = _load_private_json(cfg_path, pilot_dir, default=cfg)
        if not isinstance(cfg, dict):
            _die("existing config.json is not a JSON object")
        ok_kept, dots_py, apply_sha, err_kept = _verify_dots_pin(cfg)
        if not ok_kept:
            _die(err_kept)
    else:
        _write_private_atomic(cfg_path, json.dumps(cfg, ensure_ascii=False, indent=1) + "\n", pilot_dir)
        print(f"PILOT_INIT config={cfg_path} preset={preset} dot={dot} project={project} dots_sha256={dots_sha[:12]}", flush=True)

    applied = _apply_tracks_to_ops(cfg, dots_py, force=args.force, dots_sha256=apply_sha)
    for item in applied:
        print(f"PILOT_TRACK_READY {dot}/{item['track']}", flush=True)
    return 0


def cmd_apply_tracks(args):
    pilot_dir = _ensure_private_dir(args.pilot_dir)
    cfg = _load_private_json(os.path.join(pilot_dir, "config.json"), pilot_dir)
    if not isinstance(cfg, dict):
        _die("pilot config.json missing; run dots_pilot.py init first")
    ok, dots_py, actual_sha, err = _verify_dots_pin(cfg)
    if not ok:
        _die(err)
    with _pilot_lock(pilot_dir) as locked:
        if not locked:
            print("PILOT_SKIP reason=pilot_locked", flush=True)
            return 0
        applied = _apply_tracks_to_ops(cfg, dots_py, force=args.force, dots_sha256=actual_sha)
    for item in applied:
        print(f"PILOT_TRACK_APPLIED {cfg['dot']}/{item['track']}", flush=True)
    return 0


def cmd_sentinel(args):
    pilot_dir = _ensure_private_dir(args.pilot_dir)
    nonce_path = os.path.join(pilot_dir, "active_nonce.json")
    try:
        doc = _load_private_json(nonce_path, pilot_dir, require_strict_mode=True)
    except OSError as exc:
        _die(f"sentinel nonce unreadable: {exc}")
    if not isinstance(doc, dict) or not isinstance(doc.get("nonce"), str) or not doc["nonce"]:
        _die("no active sentinel nonce")
    supplied = args.nonce if args.nonce is not None else os.environ.get("DOTS_PILOT_SENTINEL_NONCE", "")
    if not supplied or not hmac.compare_digest(doc["nonce"].encode("utf-8"), str(supplied).encode("utf-8", errors="surrogatepass")):
        _die("sentinel nonce mismatch")
    issued_at = _parse_iso(doc.get("t"))
    age_s = (_now_utc() - issued_at).total_seconds() if issued_at is not None else None
    if age_s is None or age_s < -5 or age_s > 300:
        _die("sentinel nonce expired")
    owner_pid = doc.get("pid")
    if not isinstance(owner_pid, int) or isinstance(owner_pid, bool) or owner_pid <= 0:
        _die("sentinel nonce invalid pid")
    try:
        os.kill(owner_pid, 0)
    except ProcessLookupError:
        _die("sentinel owner process not alive")
    except PermissionError:
        pass
    ev = {
        "t": _now_iso(),
        "kind": args.kind,
        "dot": _validate_token("dot", args.dot),
        "agent": _validate_token("agent", args.agent),
        "project": _validate_token("project", args.project),
        "id": _validate_token("id", args.id),
        "why": _clean_oneline(args.why or args.kind),
        "lanes": _clean_oneline(args.lanes or "-"),
        "track": _validate_token("track", args.track or "none"),
        "task": _clean_oneline(_redact_text(args.task or "")),
        "age": _clean_oneline(args.age or "-"),
    }
    _append_private_jsonl(os.path.join(pilot_dir, "sentinel_queue.jsonl"), ev, pilot_dir)
    print(f"PILOT_SENTINEL_ACK kind={ev['kind']} dot={ev['dot']} why={ev['why']}", flush=True)
    return 0


def _drain_sentinel_queue(pilot_dir):
    qpath = os.path.join(pilot_dir, "sentinel_queue.jsonl")
    if not os.path.lexists(qpath):
        return []
    raw = _read_private_bytes(qpath, pilot_dir, require_strict_mode=True).decode("utf-8", errors="replace")
    _write_private_atomic(qpath, "", pilot_dir)
    out = []
    for ln in raw.splitlines():
        if not ln.strip():
            continue
        try:
            obj = json.loads(ln)
            if isinstance(obj, dict):
                out.append(obj)
        except ValueError:
            continue
    return out


def _run_once_locked(cfg, pilot_dir):
    ok, dots_py, actual_sha, err = _verify_dots_pin(cfg)
    if not ok:
        print(f"PILOT_ERROR {err}", flush=True)
        return 2
    ops_base = os.path.abspath(os.path.expanduser(cfg.get("ops_base") or DEFAULT_OPS_BASE))
    dot = _validate_token("dot", cfg["dot"])
    project = _validate_token("project", cfg["project"])
    cooldown_min = _safe_int(cfg.get("cooldown_min"), 10, 1)

    ledger_path = os.path.join(pilot_dir, "ledger.json")
    ledger = _load_private_json(ledger_path, pilot_dir, default={})
    if not isinstance(ledger, dict):
        ledger = {}
    ledger.setdefault("wake_cooldowns", {})
    ledger.setdefault("notify_cooldowns", {})

    nonce = secrets.token_hex(16)
    nonce_path = os.path.join(pilot_dir, "active_nonce.json")
    qpath = os.path.join(pilot_dir, "sentinel_queue.jsonl")
    _write_private_atomic(qpath, "", pilot_dir)
    _write_private_atomic(nonce_path, json.dumps({"nonce": nonce, "t": _now_iso(), "pid": os.getpid()}) + "\n", pilot_dir)

    script_self = os.path.abspath(__file__)
    base_sentinel = [sys.executable, "-I", "-B", script_self, "--pilot-dir", pilot_dir, "sentinel"]
    wake_cmd = base_sentinel + ["--kind", "wake", "--dot", dot, "--agent", "{agent}", "--project", "{project}", "--id", "{id}", "--age", "{age}"]
    fill_cmd = base_sentinel + ["--kind", "fill", "--dot", "{dot}", "--agent", "{agent}", "--project", project, "--id", "{id}", "--why", "{why}", "--lanes", "{lanes}", "--track", "{track}", "--task", "{task}", "--age", "{age}"]
    idle_cmd = base_sentinel + ["--kind", "idle", "--dot", "{dot}", "--agent", "{agent}", "--project", project, "--id", "{id}", "--why", "{why}", "--lanes", "{lanes}", "--track", "{track}", "--task", "{task}", "--age", "{age}"]
    notify_cmd = base_sentinel + ["--kind", "notify", "--dot", dot, "--agent", "{agent}", "--project", "{project}", "--id", "{id}", "--age", "{age}"]

    watch_state = os.path.join(pilot_dir, "watch-state.json")
    watch_out = ""
    try:
        watch_args = [
            "watch", "--dot", dot, "--loop", "0", "--expect-sha256", actual_sha,
            "--state", watch_state,
            "--wake-cmd", json.dumps(wake_cmd),
            "--idle-cmd", json.dumps(idle_cmd),
            "--notify-cmd", json.dumps(notify_cmd),
        ]
        if cfg.get("keep_full", True):
            watch_args.extend(["--keep-full", dot, "--fill-cmd", json.dumps(fill_cmd)])
        proc = _run_dots(
            dots_py, ops_base, watch_args, check=False,
            env_extra={"DOTS_PILOT_SENTINEL_NONCE": nonce}, expected_sha256=actual_sha,
        )
        watch_out = proc.stdout + proc.stderr
        if proc.returncode != 0:
            print(f"PILOT_WATCH_WARN exit={proc.returncode} detail={_clean_oneline(_redact_text(watch_out))}", flush=True)
        else:
            for wline in watch_out.splitlines():
                if "WATCH_ERROR" in wline or "WATCH_TOOL_CHANGED" in wline:
                    print(f"PILOT_WATCH_WARN {_clean_oneline(_redact_text(wline))}", flush=True)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(nonce_path)

    events = _drain_sentinel_queue(pilot_dir)

    # Check monitor / daemon health cleanly (read-only, pinned & TOCTOU-safe)
    daemon_events = []
    mon_snap = None
    if cfg.get("dots_monitor_py"):
        ok_mon, mon_abs, _, mon_err = _verify_monitor_pin(cfg)
        if not ok_mon:
            print(f"PILOT_MONITOR_WARN {_clean_oneline(_redact_text(mon_err))}", flush=True)
        else:
            try:
                mon_mod = _load_pinned_module("dots_monitor", mon_abs, expected_sha256=cfg.get("monitor_sha256"))
                mon_cfg = _build_monitor_cfg(cfg)
                prev_mon_path = os.path.join(pilot_dir, "monitor-state.json")
                prev_mon = _decode_monitor_state(_load_private_json(prev_mon_path, pilot_dir, default=None))
                now_dt = _now_utc()
                errors = []
                transport = mon_mod.Transport(25, 25)
                mon_snap = mon_mod.build_snapshot(mon_cfg, transport, now_dt, errors)
                daemon_events, next_mon_state = mon_mod.diff_events(prev_mon, mon_snap)
                _write_private_atomic(prev_mon_path, json.dumps(_encode_monitor_state(next_mon_state), ensure_ascii=False, indent=1) + "\n", pilot_dir)
            except Exception as exc:
                print(f"PILOT_MONITOR_WARN {_clean_oneline(_redact_text(str(exc)))}", flush=True)

    # Read loop status JSON to spot TRACK_BLOCKED / GOAL_SATISFIED cleanly
    lproc = _run_dots(dots_py, ops_base, ["loop", "status", dot, "--json"], check=False, expected_sha256=actual_sha)
    loop_tracks = []
    if lproc.returncode != 0:
        err_txt = _clean_oneline(_redact_text(lproc.stderr or lproc.stdout or f"exit={lproc.returncode}"))
        print(f"PILOT_LOOP_STATUS_WARN exit={lproc.returncode} detail={err_txt}", flush=True)
    else:
        try:
            parsed = json.loads(lproc.stdout)
            if not isinstance(parsed, dict):
                print(
                    f"PILOT_LOOP_STATUS_WARN exit=0 detail=invalid loop status JSON shape: {type(parsed).__name__}",
                    flush=True,
                )
            else:
                raw_tracks = parsed.get("tracks")
                loop_tracks = raw_tracks if isinstance(raw_tracks, list) else []
        except (ValueError, RecursionError) as exc:
            print(
                f"PILOT_LOOP_STATUS_WARN exit=0 detail=invalid loop status JSON: {_clean_oneline(_redact_text(str(exc)))}",
                flush=True,
            )

    current = _now_utc()
    wake_events = [ev for ev in events if ev.get("kind") in ("wake", "fill", "idle")]
    notify_events = [ev for ev in events if ev.get("kind") == "notify"]

    # Dispatch grouped chat wake respecting per-signature cooldown
    eligible_wakes = []
    for ev in wake_events:
        ckey = f"{ev.get('dot')}/{ev.get('track')}/{ev.get('agent')}/{ev.get('why')}"
        if not _cooldown_active(ledger["wake_cooldowns"], ckey, cooldown_min, current=current):
            eligible_wakes.append((ckey, ev))
        else:
            print(f"PILOT_WAKE_COOLDOWN {ckey}", flush=True)

    if eligible_wakes:
        msg = _format_wake_nudge(dot, project, [e for _, e in eligible_wakes])
        ok_chat, detail_chat = _send_chat_message(cfg, msg, values=eligible_wakes[0][1])
        if ok_chat:
            ts = _now_iso()
            for ckey, _ in eligible_wakes:
                ledger["wake_cooldowns"][ckey] = ts
        else:
            print(f"PILOT_CHAT_ERROR {detail_chat}", flush=True)
            notify_events.append({"dot": dot, "agent": "-", "why": f"chat_error:{detail_chat}"})

    # Process daemon_health limits & opt-in self-heal
    dh_map = ((mon_snap or {}).get("dots") or {}).get(dot, {}).get("daemon_health") or {}
    retained_hits = sum(_safe_int((v or {}).get("retained_limit_hits"), 0, 0) for v in dh_map.values() if isinstance(v, dict))
    if retained_hits > 0:
        healed, heal_detail = _perform_daemon_heal(cfg, ledger, pilot_dir)
        print(f"PILOT_DAEMON_LIMIT dot={dot} healed={str(healed).lower()} detail={heal_detail}", flush=True)
        nkey = f"daemon_limit:{dot}"
        if not _cooldown_active(ledger["notify_cooldowns"], nkey, cooldown_min, current=current):
            ok_n, _ = _send_notification(cfg, f"DAEMON_LIMIT {dot}", f"retained terminal session limit hit; healed={healed} ({heal_detail})")
            if ok_n:
                ledger["notify_cooldowns"][nkey] = _now_iso()

    # Notify on circuit open / blocked tracks or WATCH_NOTIFY
    for ev in notify_events:
        nkey = f"notify:{ev.get('dot')}:{ev.get('id') or ev.get('why')}"
        if not _cooldown_active(ledger["notify_cooldowns"], nkey, cooldown_min, current=current):
            ok_n, _ = _send_notification(cfg, f"WATCH_NOTIFY {ev.get('dot')}", f"agent={ev.get('agent')} id={ev.get('id')} why={ev.get('why')}")
            if ok_n:
                ledger["notify_cooldowns"][nkey] = _now_iso()

    for lt in loop_tracks:
        if isinstance(lt, dict) and lt.get("consecutive_blocks", 0) >= 3 and not lt.get("paused") and not lt.get("goal_complete"):
            nkey = f"track_blocked:{dot}/{lt.get('track')}"
            if not _cooldown_active(ledger["notify_cooldowns"], nkey, cooldown_min, current=current):
                ok_n, _ = _send_notification(cfg, f"TRACK_BLOCKED {dot}/{lt.get('track')}", f"consecutive_blocks={lt.get('consecutive_blocks')}")
                if ok_n:
                    ledger["notify_cooldowns"][nkey] = _now_iso()

    # Optional concise board heartbeat chat post (disabled when 0)
    hb_min = _safe_int((cfg.get("chat") or {}).get("board_heartbeat_min"), 30, 0)
    if hb_min > 0 and not eligible_wakes:
        last_hb = ledger.get("last_chat_heartbeat_at")
        hb_age = _age_min(last_hb, current=current)
        if hb_age is None or hb_age < 0 or hb_age >= hb_min:
            summary_parts = [f"{t.get('track')}:done={t.get('done')},q={t.get('queued')},blk={t.get('blocked')}" for t in loop_tracks if isinstance(t, dict)]
            if summary_parts:
                hb_msg = f"[PILOT_HEARTBEAT dot={dot} project={project}] " + " | ".join(summary_parts)
                ok_hb, _ = _send_chat_message(cfg, hb_msg)
                if ok_hb:
                    ledger["last_chat_heartbeat_at"] = _now_iso()

    _write_private_atomic(ledger_path, json.dumps(ledger, ensure_ascii=False, indent=1) + "\n", pilot_dir)
    _write_private_atomic(
        os.path.join(pilot_dir, "heartbeat.json"),
        json.dumps({"v": 1, "t": _now_iso(), "pid": os.getpid(), "dot": dot, "wakes": len(eligible_wakes), "notifies": len(notify_events), "daemon_events": len(daemon_events)}, indent=1) + "\n",
        pilot_dir,
    )
    print(f"PILOT_TICK dot={dot} wakes={len(eligible_wakes)} notifies={len(notify_events)} tracks={len(loop_tracks)}", flush=True)
    if cfg.get("_verbose") and watch_out.strip():
        print(_redact_text(watch_out.strip()), flush=True)
    return 0


def cmd_once(args):
    pilot_dir = _ensure_private_dir(args.pilot_dir)
    cfg = _load_private_json(os.path.join(pilot_dir, "config.json"), pilot_dir)
    if not isinstance(cfg, dict):
        _die("pilot config.json missing; run dots_pilot.py init first")
    if args.verbose:
        cfg = dict(cfg)
        cfg["_verbose"] = True
    with _pilot_lock(pilot_dir) as locked:
        if not locked:
            print("PILOT_SKIP reason=pilot_locked", flush=True)
            return 0
        return _run_once_locked(cfg, pilot_dir)


def _stop_quiet(signum, frame):
    raise SystemExit(0)


def cmd_run(args):
    pilot_dir = _ensure_private_dir(args.pilot_dir)
    cfg = _load_private_json(os.path.join(pilot_dir, "config.json"), pilot_dir)
    if not isinstance(cfg, dict):
        _die("pilot config.json missing; run dots_pilot.py init first")
    interval = _safe_int(args.interval, _safe_int(cfg.get("interval_sec"), 180, 15), 15)
    max_ticks = _safe_int(args.max_ticks, 0, 0)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        with contextlib.suppress(OSError, ValueError):
            signal.signal(sig, _stop_quiet)
    tick = 0
    while True:
        with _pilot_lock(pilot_dir) as locked:
            if not locked:
                print("PILOT_SKIP reason=pilot_locked", flush=True)
            else:
                _run_once_locked(cfg, pilot_dir)
        tick += 1
        if max_ticks > 0 and tick >= max_ticks:
            return 0
        time.sleep(interval)


def cmd_say(args):
    pilot_dir = _ensure_private_dir(args.pilot_dir)
    cfg = _load_private_json(os.path.join(pilot_dir, "config.json"), pilot_dir)
    if not isinstance(cfg, dict):
        _die("pilot config.json missing; run dots_pilot.py init first")
    text = args.text if args.text is not None else sys.stdin.read()
    if not text or not text.strip():
        _die("--text or stdin message required")
    with _pilot_lock(pilot_dir) as locked:
        if not locked:
            _die("pilot is busy in another process")
        ok, detail = _send_chat_message(cfg, text)
    if not ok:
        _die(f"say failed: {detail}")
    print(f"PILOT_SAY ok detail={detail}", flush=True)
    return 0


def cmd_status(args):
    pilot_dir = _resolve_safe_pilot_path(args.pilot_dir)
    cfg = _load_private_json_safe(os.path.join(pilot_dir, "config.json"), pilot_dir, default={})
    if not isinstance(cfg, dict):
        cfg = {}
    hb = _load_private_json_safe(os.path.join(pilot_dir, "heartbeat.json"), pilot_dir, default=None)
    if not isinstance(hb, dict):
        hb = None
    ledger = _load_private_json_safe(os.path.join(pilot_dir, "ledger.json"), pilot_dir, default={})
    if not isinstance(ledger, dict):
        ledger = {}
    ok_pin, dots_py, actual_sha, pin_err = _verify_dots_pin(cfg) if cfg else (False, None, None, "uninitialized")
    ok_mon_pin, _, _, mon_pin_err = _verify_monitor_pin(cfg) if cfg else (False, None, None, "uninitialized")
    tracks = []
    loop_status_err = None
    dot_val = cfg.get("dot")
    if ok_pin and isinstance(dot_val, str) and TOKEN_RE.fullmatch(dot_val):
        raw_ops = cfg.get("ops_base")
        ops_base = os.path.abspath(
            os.path.expanduser(raw_ops if isinstance(raw_ops, str) and raw_ops.strip() else DEFAULT_OPS_BASE)
        )
        lproc = _run_dots(dots_py, ops_base, ["loop", "status", dot_val, "--json"], check=False, expected_sha256=actual_sha)
        if lproc.returncode != 0:
            loop_status_err = _clean_oneline(_redact_text(lproc.stderr or lproc.stdout or f"exit={lproc.returncode}"))
        else:
            try:
                parsed = json.loads(lproc.stdout)
                if not isinstance(parsed, dict):
                    loop_status_err = f"invalid loop status JSON shape: {type(parsed).__name__}"
                else:
                    raw_tracks = parsed.get("tracks")
                    tracks = raw_tracks if isinstance(raw_tracks, list) else []
            except (ValueError, RecursionError) as exc:
                loop_status_err = _clean_oneline(f"invalid loop status JSON: {exc}")
    out = _redact_tree({
        "v": 1,
        "pilot_dir": pilot_dir,
        "initialized": bool(cfg),
        "dot": cfg.get("dot"),
        "project": cfg.get("project"),
        "preset": cfg.get("preset"),
        "dots_pin_ok": ok_pin,
        "dots_pin_error": pin_err,
        "dots_sha256": actual_sha,
        "monitor_pin_ok": ok_mon_pin,
        "monitor_pin_error": mon_pin_err,
        "loop_status_error": loop_status_err,
        "heartbeat": hb,
        "tracks": tracks,
        "last_heal_at": ledger.get("last_heal_at"),
    })
    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=1))
        return 0
    dot_txt = _clean_oneline(out["dot"] if out.get("dot") not in (None, "") else "-")
    proj_txt = _clean_oneline(out["project"] if out.get("project") not in (None, "") else "-")
    print(f"PILOT_STATUS dot={dot_txt} project={proj_txt} pin={'ok' if ok_pin else _clean_oneline(out['dots_pin_error'] or '')} tracks={len(out['tracks'])}")
    if loop_status_err:
        print(f"  loop_status_error={_clean_oneline(_redact_text(loop_status_err))}")
    out_hb = out.get("heartbeat")
    if isinstance(out_hb, dict):
        print(
            f"  heartbeat t={_clean_oneline(out_hb.get('t'))}"
            f" wakes={_clean_oneline(out_hb.get('wakes'))}"
            f" notifies={_clean_oneline(out_hb.get('notifies'))}"
        )
    for t in out["tracks"]:
        if isinstance(t, dict):
            print(f"  track={_clean_oneline(t.get('track'))} done={t.get('done')} queued={t.get('queued')} blocked={t.get('blocked')} paused={t.get('paused')}")
    return 0


def cmd_doctor(args):
    checks = []
    ok_all = True

    def add_check(name, ok, detail):
        nonlocal ok_all
        if not ok:
            ok_all = False
        checks.append({"check": name, "ok": bool(ok), "detail": _clean_oneline(_redact_text(detail))})

    add_check("python_version", sys.version_info >= (3, 9), sys.version.split()[0])
    try:
        pilot_dir = _resolve_safe_pilot_path(args.pilot_dir)
        pilot_dir_safe = True
        pilot_dir_err = None
    except SystemExit:
        pilot_dir = os.path.abspath(os.path.expanduser(args.pilot_dir))
        pilot_dir_safe = False
        pilot_dir_err = f"unsafe path or symlink: {pilot_dir}"
    if not pilot_dir_safe or not os.path.isdir(pilot_dir) or os.path.islink(pilot_dir):
        add_check("pilot_dir", False, pilot_dir_err or f"missing or symlink: {pilot_dir}")
        cfg = None
    else:
        st = os.lstat(pilot_dir)
        add_check("pilot_dir_perms", st.st_uid == os.geteuid() and (st.st_mode & 0o077) == 0, oct(stat.S_IMODE(st.st_mode)))
        cfg_path = os.path.join(pilot_dir, "config.json")
        try:
            cfg = _load_private_json(cfg_path, pilot_dir, require_strict_mode=True)
            add_check("config_json", isinstance(cfg, dict), cfg_path)
        except Exception as exc:
            cfg = None
            add_check("config_json", False, str(exc))

    if isinstance(cfg, dict):
        ok_pin, dots_py, _, pin_err = _verify_dots_pin(cfg)
        add_check("dots_py_pin", ok_pin, dots_py if ok_pin else pin_err)
        ok_mon_pin, mon_abs, _, mon_pin_err = _verify_monitor_pin(cfg)
        add_check("dots_monitor_py_pin", ok_mon_pin, mon_abs if ok_mon_pin else mon_pin_err)
        ops_base = os.path.abspath(os.path.expanduser(cfg.get("ops_base") or DEFAULT_OPS_BASE))
        add_check("ops_base", os.path.isdir(ops_base), ops_base)
        chat = cfg.get("chat") or {}
        tr = chat.get("transport") or "stdout"
        if tr == "aside":
            try:
                _validate_aside_target(chat.get("target_url"))
                _validate_cdp_url(chat.get("cdp_url") or "http://127.0.0.1:9222")
                add_check("chat_transport", True, f"aside:{chat.get('target_url')}")
            except ValueError as exc:
                add_check("chat_transport", False, str(exc))
        elif tr == "command":
            cmd_v = chat.get("command")
            add_check("chat_transport", isinstance(cmd_v, list) and bool(cmd_v), f"command:{cmd_v[0] if isinstance(cmd_v, list) and cmd_v else 'missing'}")
        else:
            add_check("chat_transport", tr == "stdout", tr)
        notify = cfg.get("notify") or {}
        nm = notify.get("mode") or "stdout"
        if nm == "webhook":
            env_name = notify.get("webhook_url_env") or "DOTS_PILOT_WEBHOOK_URL"
            raw_u = os.environ.get(env_name, "").strip()
            valid_u, u_detail = _validate_webhook_url(raw_u) if raw_u else (False, "not set")
            add_check("notify_webhook", valid_u, f"env={env_name} {'ok' if valid_u else u_detail}")
        else:
            add_check("notify_mode", nm in ("stdout", "command"), nm)

    if args.json:
        print(json.dumps({"v": 1, "ok": ok_all, "checks": checks}, ensure_ascii=False, indent=1))
    else:
        for c in checks:
            mark = "PASS" if c["ok"] else "FAIL"
            print(f"DOCTOR {mark} {c['check']}: {c['detail']}", flush=True)
    return 0 if ok_all else 2


def _systemd_quote(value):
    s = str(value)
    if any(ch in s for ch in ("\n", "\r", "\x00")):
        _die("systemd value must not contain newlines or NUL bytes")
    escaped = s.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%").replace("$", "$$")
    return f'"{escaped}"'


def cmd_brief(args):
    pilot_dir = _resolve_safe_pilot_path(args.pilot_dir)
    cfg = _load_private_json_safe(os.path.join(pilot_dir, "config.json"), pilot_dir, default=None)
    if not isinstance(cfg, dict):
        _die("pilot config.json missing; run dots_pilot.py init first")
    ok_pin, dots_py, actual_sha, pin_err = _verify_dots_pin(cfg)
    if not ok_pin:
        _die(pin_err)
    dot = _validate_token("dot", cfg.get("dot") or "DOT-A")
    project = _validate_token("project", cfg.get("project") or "default")
    raw_ops = cfg.get("ops_base")
    ops_base = os.path.abspath(
        os.path.expanduser(raw_ops if isinstance(raw_ops, str) and raw_ops.strip() else DEFAULT_OPS_BASE)
    )
    tracks = _resolve_tracks_from_cfg(cfg)
    if args.track:
        want = _validate_token("track", args.track)
        tracks = [t for t in tracks if isinstance(t, dict) and t.get("track") == want]
        if not tracks:
            _die(f"track {want!r} not found in pilot config")

    items = []
    for t in tracks:
        if not isinstance(t, dict) or not t.get("track"):
            continue
        tname = _validate_token("track", t.get("track"))
        role_in = str(t.get("astra_role") or t.get("role") or tname)
        p_proc = _run_dots(
            dots_py,
            ops_base,
            ["prompt", "--role", role_in, "--mode", "dots-only", "--json"],
            check=True,
            expected_sha256=actual_sha,
        )
        bundle = json.loads(p_proc.stdout)
        req_ack = bool(cfg.get("require_astra_ack") or t.get("require_astra_ack"))
        claim_cmd = f"DOTS_OPS_BASE={shlex.quote(ops_base)} python3 {shlex.quote(dots_py)} loop claim {dot} {tname} --agent <agentId>"
        ack_cmd = (
            f"DOTS_OPS_BASE={shlex.quote(ops_base)} python3 {shlex.quote(dots_py)} loop ack {dot} {tname} --agent <agentId> --profile-sha {bundle['sha256']}"
            if req_ack
            else None
        )
        brief_lines = [
            f"Track `{tname}` ({dot}/{project}) — Astra {bundle['role']} ({bundle['effort']})",
            f"Objective: {_clean_oneline(_redact_text(t.get('objective') or '-'))}",
            f"Claim loop: {claim_cmd}",
        ]
        if ack_cmd:
            brief_lines.append(f"Ack required: {ack_cmd}")
        brief_lines.append(bundle["full"])
        items.append({
            "dot": dot,
            "project": project,
            "track": tname,
            "role": bundle["role"],
            "effort": bundle["effort"],
            "sha256": bundle["sha256"],
            "require_astra_ack": req_ack,
            "claim_cmd": claim_cmd,
            "ack_cmd": ack_cmd,
            "profile": bundle,
            "brief_text": "\n".join(brief_lines),
        })

    if args.json:
        print(json.dumps({"v": 1, "dot": dot, "project": project, "ops_base": ops_base, "tracks": items}, ensure_ascii=False, indent=1), flush=True)
        return 0
    for idx, it in enumerate(items):
        if idx > 0:
            print("=" * 60, flush=True)
        print(it["brief_text"], flush=True)
    return 0


def cmd_render_service(args):
    pilot_dir = _resolve_safe_pilot_path(args.pilot_dir)
    cfg = _load_private_json_safe(os.path.join(pilot_dir, "config.json"), pilot_dir, default={})
    if not isinstance(cfg, dict):
        cfg = {}
    interval = _safe_int(args.interval, _safe_int(cfg.get("interval_sec"), 180, 15), 15)
    dot_raw = cfg.get("dot")
    dot_clean = _clean_oneline(dot_raw) if isinstance(dot_raw, str) else ""
    dot = dot_clean or "DOT-A"
    py = os.path.abspath(sys.executable)
    script = os.path.abspath(__file__)
    log_out = os.path.join(pilot_dir, "pilot.stdout.log")
    log_err = os.path.join(pilot_dir, "pilot.stderr.log")
    label = f"com.dotomation.pilot.{re.sub(r'[^A-Za-z0-9._-]+', '-', dot.lower())}"

    if args.target == "launchd":
        plist = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
            '<plist version="1.0">\n'
            '<dict>\n'
            f'  <key>Label</key><string>{xml_escape(label)}</string>\n'
            '  <key>ProgramArguments</key>\n'
            '  <array>\n'
            f'    <string>{xml_escape(py)}</string>\n'
            '    <string>-B</string>\n'
            f'    <string>{xml_escape(script)}</string>\n'
            '    <string>--pilot-dir</string>\n'
            f'    <string>{xml_escape(pilot_dir)}</string>\n'
            '    <string>run</string>\n'
            '    <string>--interval</string>\n'
            f'    <string>{interval}</string>\n'
            '  </array>\n'
            '  <key>RunAtLoad</key><true/>\n'
            '  <key>KeepAlive</key><true/>\n'
            f'  <key>StandardOutPath</key><string>{xml_escape(log_out)}</string>\n'
            f'  <key>StandardErrorPath</key><string>{xml_escape(log_err)}</string>\n'
            '</dict>\n'
            '</plist>\n'
        )
        print(plist, end="", flush=True)
        return 0

    unit = (
        "[Unit]\n"
        f"Description=Dotomation Zero-Token Pilot ({dot})\n"
        "After=network.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"ExecStart={_systemd_quote(py)} -B {_systemd_quote(script)} --pilot-dir {_systemd_quote(pilot_dir)} run --interval {interval}\n"
        "Restart=on-failure\n"
        "RestartSec=10\n\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )
    print(unit, end="", flush=True)
    return 0


def build_parser():
    p = argparse.ArgumentParser(
        prog="dots_pilot.py",
        description="Zero-AI-token, Python 3.9+ stdlib-only single-repo bootstrap & pilot for dotomation",
    )
    p.add_argument("--pilot-dir", default=DEFAULT_PILOT_DIR, help="owner-private state directory (default ~/.dotomation-pilot)")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("init", help="initialize private pilot dir, config, ops_base, and loop track contracts")
    pi.add_argument("--dot", default=None)
    pi.add_argument("--project", default=None)
    pi.add_argument("--ops-base", dest="ops_base", default=None)
    pi.add_argument("--preset", choices=sorted(PRESETS), default=None)
    pi.add_argument("--config", help="optional JSON template matching dots-pilot.example.json")
    pi.add_argument("--dots-py", dest="dots_py")
    pi.add_argument("--dots-monitor-py", dest="dots_monitor_py")
    pi.add_argument("--monitor-config", dest="monitor_config")
    pi.add_argument("--chat-transport", dest="chat_transport", choices=("stdout", "command", "aside"))
    pi.add_argument("--target-url", dest="target_url")
    pi.add_argument("--cdp-url", dest="cdp_url")
    pi.add_argument("--chat-cmd", dest="chat_cmd")
    pi.add_argument("--notify-mode", dest="notify_mode", choices=("stdout", "command", "webhook"))
    pi.add_argument("--webhook-url-env", dest="webhook_url_env")
    pi.add_argument("--notify-cmd", dest="notify_cmd")
    pi.add_argument("--daemon-heal", dest="daemon_heal", action="store_true")
    pi.add_argument("--unit-label", dest="unit_label")
    pi.add_argument("--interval", type=int)
    pi.add_argument("--cooldown-min", dest="cooldown_min", type=int)
    pi.add_argument("--require-astra-ack", dest="require_astra_ack", action="store_true")
    pi.add_argument("--force", action="store_true")

    pa = sub.add_parser("apply-tracks", help="apply configured preset/tracks into ops_base via dots.py loop init")
    pa.add_argument("--force", action="store_true")

    po = sub.add_parser("once", help="run one nonce-authenticated watch + monitor + heal/wake tick")
    po.add_argument("--verbose", action="store_true")

    pr = sub.add_parser("run", help="foreground loop running once every --interval seconds")
    pr.add_argument("--interval", type=int)
    pr.add_argument("--max-ticks", dest="max_ticks", type=int, default=0)

    ps = sub.add_parser("sentinel", help="internal nonce-authenticated hook called by dots.py watch")
    ps.add_argument("--nonce", required=False, default=None)
    ps.add_argument("--kind", required=True, choices=("wake", "fill", "idle", "notify"))
    ps.add_argument("--dot", default="none")
    ps.add_argument("--agent", default="none")
    ps.add_argument("--project", default="none")
    ps.add_argument("--id", default="none")
    ps.add_argument("--why", default="")
    ps.add_argument("--lanes", default="")
    ps.add_argument("--track", default="none")
    ps.add_argument("--task", default="")
    ps.add_argument("--age", default="-")

    psay = sub.add_parser("say", help="send one manual message via configured chat transport")
    psay.add_argument("--text")

    pst = sub.add_parser("status", help="show pilot pin, heartbeat, and loop track states")
    pst.add_argument("--json", action="store_true")

    pbr = sub.add_parser("brief", help="print role-bound Astra v2 Mode 1 subagent briefs for configured tracks")
    pbr.add_argument("--track")
    pbr.add_argument("--json", action="store_true")

    pdoc = sub.add_parser("doctor", help="validate environment, private permissions, pins, and transports")
    pdoc.add_argument("--json", action="store_true")

    prs = sub.add_parser("render-service", help="print launchd or systemd user template (does not install)")
    prs.add_argument("--target", required=True, choices=("launchd", "systemd"))
    prs.add_argument("--interval", type=int)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    dispatch = {
        "init": cmd_init,
        "apply-tracks": cmd_apply_tracks,
        "once": cmd_once,
        "run": cmd_run,
        "sentinel": cmd_sentinel,
        "say": cmd_say,
        "status": cmd_status,
        "brief": cmd_brief,
        "doctor": cmd_doctor,
        "render-service": cmd_render_service,
    }
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
