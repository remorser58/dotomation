#!/usr/bin/env python3
"""dots.py: one call per step for a Dots agent, one view for the people who read it.

Shared folder on `local` (rules: dots-superkit workflows/live-status.md,
workflows/session-memory.md, workflows/direct-mail.md):
  live/<DOT>/<agent>.jsonl     live-record-v1 lines, append-only, written by that agent only
  memory/<DOT>/<agent>.json    memory-v1 session memory, rewritten whole by that agent only
  <project>/mail/<agent>/      direct mail (mail-v1); in.jsonl is the agent's inbox
  listen/<agent>.<pid>.json   one `wait` that is blocking right now, removed when that process exits
  dots.json                    optional: dots, projects, hosts and the wake command

Agent, once per step (start, each material step and at least every 15 min, blocked, done):
  dots.py tick <DOT> <agent> <start|step|blocked|done> --task T --host H --note N --evidence E
      [--role R] [--model M] [--project P]   default: the agent's newest v1 live line
      [--goal G] [--doing D] [--next X]      kept from memory; a new task needs all three
      [--decision D]... [--blocker B]... [--unblock]
      [--took MAIL-ID]                       mark own inbox read up to that message
  Appends the live line, rewrites the memory file, then prints unread mail and mail already
  read but not yet answered. Unread mail is printed first, before the live line, so it steers
  the agent mid-task. Every value is checked first; nothing is written if one fails.

Agent with no assigned work (the steering receiver; it does not end its turn):
  dots.py wait <DOT> <agent> [--timeout SECONDS]   default 3300 (55 min)
      Prints unread mail at once and exits 0; otherwise blocks, polls its mailboxes about once
      a second, prints a message as soon as it lands and exits 0. Either way it marks what it
      printed read, so the same message is not delivered twice. Exits 3 on timeout, 4 when a
      signal ends it. Holds listen/<agent>.<pid>.json while blocking.

Agent handing work back (the reply that holds the answer open):
  dots.py reply <project> <agent> <ack|answer|question|result|block|note> <task> [--re ID]
      [--ref R]... [--dot DOT] [--timeout SECONDS] [--no-wait]   text on stdin
      Prints the id. For a result, question or block it then stays in the same loop `wait` runs,
      so the main session's answer is printed as this command's output; exit 0 mail, 3 timeout,
      4 signal. `--no-wait` returns at once (the exception: when the answer cannot arrive in
      this turn). ack, answer and note return at once. `--re` may be left out only for those
      three kinds, when the message answers no mail line (a first result for work the dot root
      gave); at least one --ref is then required. Nothing is written when an input is refused.

Main session outside Dots:
  dots.py send <project> <agent> <kind> <task> [--re ID] [--ref R]...  text on stdin
      Prints the id, then LISTENING or NOT_LISTENING last_tick=<age>; when not listening it runs
      the configured wake command once (or prints the manual doorbell).
  dots.py inbox [project] [agent]     agent replies not yet marked read
  dots.py read <project> <agent> <id> mark replies up to that id read

Readers (read-only):
  dots.py board [DOT...] [--json]   each agent: newest line, age, doing, next, blockers, mail,
      flags; MAIL_WAITING marks mail that owes an answer while no wait is open on that agent
  dots.py memo <DOT> <agent>        that agent's memory and its last 3 live lines

Owner side (the dot root is not trusted):
  dots.py watch [--loop MINUTES] [--expect-sha256 HEX] [--wake-cmd ARGV] [--notify-cmd ARGV]
      [--after-min N] [--notify-after-min N] [--state PATH]   --loop 0 (the default) runs once
      For every inbox line that still owes an answer and whose agent holds no live wait marker:
      after `after_min` minutes run the wake argv once; if the line is still unanswered
      `notify_after_min` minutes later, run the notify argv once. A message counts as owed until
      an out.jsonl line answers it, read or not: a read marker adds WATCH_STUCK, it never silences
      the notify. Every run starts with one WATCH_CONFIG line naming the source of each value
      (`argv`, `dots.json`, `default`), and the flags win, because dots.json sits in the folder the
      dot root writes. The ledger defaults to <base>/watch/state.json; --state keeps it elsewhere.
      The ledger may only add a line: a record that suppresses a step prints WATCH_SKIP. Other
      lines are WATCH_WAKE, WATCH_NOTIFY, WATCH_STUCK, WATCH_RESOLVED, WATCH_TOOL_CHANGED and
      WATCH_ERROR. --expect-sha256 compares this file's own bytes each run: the shared folder is
      writable by the dot root, the command line is not.
      A marker counts as a waiter only with a `until` between now and a day out, so a marker naming
      a live local pid with a plausible `until` is the one case the tool cannot tell from a real
      wait; workflows/direct-mail.md says how to check it.

Setup (once per machine; idempotent, adds only):
  dots.py init [--base DIR] [--dot ID]... [--project ID=MAIN]... [--host LABEL=PURPOSE]...
      [--wake-cmd ARGV-JSON] [--notify-cmd ARGV-JSON] [--yes]
      Interactive when stdin is a terminal, scriptable with the flags otherwise. Writes
      <base>/dots.json, live/<DOT>/, memory/<DOT>/ and <project>/mail/, then prints the brief
      to paste to each dot root. Never deletes or rewrites an existing record or entry.

Base folder: $DOTS_OPS_BASE, else ~/.dots-ops; `init` resolves it the same way, so an instance
that already keeps its folder elsewhere points that variable at it. `init --base DIR` writes
another one for this machine and prints the export line to set, because later commands still
read the environment first. Every command works without dots.json: the dot and the project are
read from the folders that exist.
"""
import sys

# Run as a script, sys.path[0] is the folder this file sits in — and the tool may be copied into a
# folder the dot root can write. A sibling `json.py` or `socket.py` there would shadow the standard
# library before any check could run, so drop that entry before the imports below. Imported as a
# module, sys.path[0] belongs to the importer and is left alone.
if __name__ == "__main__" and sys.path:
    del sys.path[0]

import argparse, contextlib, datetime, fcntl, glob, hashlib, json, os, re, secrets, signal, socket, stat, subprocess, time

def default_base():
    env = os.environ.get("DOTS_OPS_BASE")
    if env:
        return os.path.expanduser(env)
    return os.path.expanduser("~/.dots-ops")

BASE = default_base()
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ID = re.compile(r"^m-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{4}$")
STATES = ("start", "step", "blocked", "done")
FIELDS = ("t", "agent", "role", "model", "project", "task", "state", "host", "note", "evidence")
MAIL_FIELDS = ("v", "id", "t", "from", "to", "task", "kind", "re", "text", "refs")
TO_AGENT = ("task", "question", "answer", "review", "note")
TO_MAIN = ("ack", "answer", "question", "result", "block", "note")
NEEDS_ANSWER = ("task", "question", "review")   # an inbox kind that owes an out.jsonl line with re=id
REPLY_WAITS = ("result", "question", "block")   # an agent's reply that keeps the command open for the answer
STALL_MIN = 30
WATCH_DEFAULTS = {"after_min": 10, "notify_after_min": 20}   # dots.json "watch": {...} overrides
ROSTER_DONE = ("done", "idle", "free", "closed", "finished")  # a claim the live line can disprove
PLACEHOLDER = re.compile(r"\{(project|agent|id|dot|age)\}")
MAX_TEXT = 500      # goal, doing, next
MAX_ITEM = 300      # one decision or blocker
MAX_DECISIONS = 20  # newest kept; the full history is the live log
MAX_BLOCKERS = 10
POLL_S = 1.0        # wait's mailbox poll; a message lands within about a second
WAKE_TIMEOUT = 25   # one wake command, once, never retried; under the connector's 30 s exec yield
LISTEN_MAX_S = 86400  # a listen marker that claims a longer wait than this is not evidence of a waiter
LISTEN_SLACK_S = 3600  # the reader's clock may lag the waiter's, so that cap is judged with slack
SIGNALS = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
SECRET = re.compile(r"(sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}"
                    r"|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY"
                    r"|Bearer\s+[A-Za-z0-9._~+/-]{20,})")

def die(msg):
    print(f"dots: {msg}", file=sys.stderr)
    sys.exit(2)

def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_after(seconds):
    t = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_t(s):
    try:
        t = datetime.datetime.fromisoformat(str(s).replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else None

def age_min(s):
    t = parse_t(s)
    return None if t is None else (datetime.datetime.now(datetime.timezone.utc) - t).total_seconds() / 60

def text(name, value, limit=None):
    if not isinstance(value, str) or not value.strip() or "\n" in value or "\r" in value:
        die(f"--{name} must be one non-empty line")
    if limit and len(value) > limit:
        die(f"--{name} is {len(value)} characters; keep it under {limit}")
    if SECRET.search(value):
        die(f"--{name} looks like it contains a secret; leave it out")
    return value.strip()

def block(name, value):
    """Mail text: several lines are allowed, an empty or whitespace-only body is not."""
    if not isinstance(value, str) or not value.strip():
        die(f"{name} on stdin is empty")
    if SECRET.search(value):
        die(f"{name} looks like it contains a secret; leave it out")
    return value.strip("\r\n")

def token(name, value):
    if not TOKEN.match(value or ""):
        die(f"bad {name} {value!r}")
    return value

READ_WARNED = set()
BYTE_WARNED = {}

def regular(path):
    """False for a path that is not a regular file. This is the cheap first check; it cannot be the only
    one, because the path can be replaced between this stat and the open, so every read goes through
    open_raw, which opens non-blocking and asks the descriptor what it really holds."""
    try:
        return stat.S_ISREG(os.stat(path).st_mode)
    except OSError:
        return True

def unreadable(path, exc):
    """One stderr line per path that exists but cannot be read — a folder or a FIFO where a file
    belongs, a file with no read permission. A missing file is the normal case and stays silent. The
    caller reads the path as no lines either way, and the set keeps a once-a-second poll from repeating
    the line; a read that succeeds clears the entry, so a path that breaks again is reported again."""
    if path in READ_WARNED:
        return
    READ_WARNED.add(path)
    name = exc if isinstance(exc, str) else exc.__class__.__name__
    print(f"dots: cannot read {path} ({name}); treated as no lines", file=sys.stderr)

def skipped_lines(path, count):
    """One stderr line per path and count: a line whose bytes are not UTF-8 is skipped on its own, so
    the count says how much of the file was not read and every other line still is. A poll that finds
    the same count again stays quiet, and a clean read clears the entry, so damage that comes back is
    reported again."""
    if not count:
        BYTE_WARNED.pop(path, None)
        return
    if BYTE_WARNED.get(path) == count:
        return
    BYTE_WARNED[path] = count
    print(f"dots: skipped {count} line{'' if count == 1 else 's'} in {path}, not UTF-8; "
          f"every other line was read", file=sys.stderr)

def open_raw(path):
    """An open descriptor for reading, or None. O_NONBLOCK is what makes this safe: a FIFO open returns
    at once instead of waiting for a writer, and the fstat on the descriptor decides what was really
    opened — the path may be replaced between regular() and this call, and only the descriptor knows
    what it holds. Anything that is not a regular file is reported and closed, never read."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    except FileNotFoundError:
        READ_WARNED.discard(path)
        return None
    except OSError as exc:
        unreadable(path, exc)
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            unreadable(path, "not a regular file")
            os.close(fd)
            return None
    except OSError as exc:
        os.close(fd)
        unreadable(path, exc)
        return None
    return fd

def open_read(path):
    """open_raw as a text file, or None."""
    fd = open_raw(path)
    return None if fd is None else os.fdopen(fd, encoding="utf-8")

def jsonl(path):
    """Lines of one JSONL file, skipping any line that is not a JSON object. A path the root made
    unreadable — a folder or a FIFO where the file should be, a file with no read permission — reads as
    no lines, never as a crash and never as a block, and says so once on stderr: every reader here runs
    on shapes the root can write, so a silent empty read would be a claim this tool cannot make.

    Decoding is per line on purpose. A mailbox is bytes the root can append to, and one byte that is
    not UTF-8 must cost that one line: the bytes are split on b"\\n" first and each line decoded on
    its own, so damage cannot swallow the good lines after it, and the skipped count is printed once
    per change (skipped_lines). Decoding the stream instead would drop everything from the bad byte to
    the next buffer boundary, which is a silent loss of mail a later `watch` never rings."""
    if not regular(path):
        unreadable(path, "not a regular file")
        return []
    out = []
    fd = open_raw(path)
    if fd is None:
        return out
    skipped = 0
    try:
        with os.fdopen(fd, "rb") as raw:
            for bline in raw:
                try:
                    ln = bline.decode("utf-8")
                except UnicodeDecodeError:
                    skipped += 1
                    continue
                try:
                    obj = json.loads(ln)
                except ValueError:
                    continue
                if isinstance(obj, dict):
                    out.append(obj)
    except OSError as exc:
        unreadable(path, exc)
        return out
    READ_WARNED.discard(path)
    skipped_lines(path, skipped)
    return out

def v1_lines(path):
    """Live lines that are live-record-v1 shaped; legacy lines are left out, never repaired."""
    return [x for x in jsonl(path) if set(x) == set(FIELDS) and x.get("state") in STATES and parse_t(x.get("t"))]

def legacy_t(path):
    """Newest time on a non-v1 line (t, timestamp, time, at, datetime), for the legacy stall clock only."""
    best = None
    for x in jsonl(path):
        for k in ("t", "timestamp", "time", "at", "datetime"):
            t = parse_t(x.get(k)) if isinstance(x.get(k), str) else None
            if t:
                best = t if best is None or t > best else best
                break
    return best

def load_json(path):
    if not regular(path):
        unreadable(path, "not a regular file")
        return None
    fh = open_read(path)
    if fh is None:
        return None
    try:
        with fh:
            obj = json.load(fh)
    except (OSError, UnicodeDecodeError) as exc:
        unreadable(path, exc)
        return None
    except ValueError:   # not JSON: no config, which is the same as none written
        READ_WARNED.discard(path)
        return None
    READ_WARNED.discard(path)
    return obj if isinstance(obj, dict) else None

def marker(path):
    if not regular(path):
        unreadable(path, "not a regular file")
        return None
    fh = open_read(path)
    if fh is None:
        return None
    try:
        with fh:
            text = fh.read().strip() or None
    except (OSError, UnicodeDecodeError) as exc:
        unreadable(path, exc)
        return None
    READ_WARNED.discard(path)
    return text

def after(msgs, last):
    """Lines after the marker id. A duplicate id already on disk resolves to its last occurrence,
    so a marker can never stop short of a later line that repeats it."""
    ids = [m.get("id") for m in msgs]
    return msgs if last not in ids else msgs[len(ids) - ids[::-1].index(last):]

def project_of(box):
    return os.path.basename(os.path.dirname(os.path.dirname(box)))

def mailboxes(agent):
    token("agent", agent)
    return sorted(d for d in glob.glob(os.path.join(BASE, "*", "mail", glob.escape(agent))) if os.path.isdir(d))

def answered_ids(outbox):
    """Ids that a line in `outbox` answers. Only a string `re` can match an inbox id, so a line the
    root wrote with a list or an object there answers nothing and cannot crash a read."""
    return {m.get("re") for m in outbox if isinstance(m.get("re"), str)}

def owes_answer(m, answered):
    """A line owes an answer when its kind asks for one and no answer names its id. An id that is not
    a string cannot be answered, so it stays owed instead of raising as an unhashable set member."""
    mid = m.get("id")
    return m.get("kind") in NEEDS_ANSWER and not (isinstance(mid, str) and mid in answered)

def mail_state(box):
    inbox = jsonl(os.path.join(box, "in.jsonl"))
    outbox = jsonl(os.path.join(box, "out.jsonl"))
    mark = marker(os.path.join(box, ".agent-read"))
    new = after(inbox, mark)
    taken = inbox[:len(inbox) - len(new)]
    answered = answered_ids(outbox)
    return {"new": new, "unanswered": [m for m in taken if owes_answer(m, answered)],
            "unread_out": after(outbox, marker(os.path.join(box, ".main-read")))}

def unread_upto(box, took):
    """Unread messages in one box, minus the ones this tick is marking read with --took."""
    msgs = mail_state(box)["new"]
    ids = [m.get("id") for m in msgs]
    return msgs[len(ids) - ids[::-1].index(took):] if took in ids else msgs

def unread_all(agent):
    """(box, project, message) for every unread line in every mailbox of that agent id."""
    return [(box, project_of(box), m) for box in mailboxes(agent) for m in mail_state(box)["new"]]

def waiting_on(box):
    """Inbox lines that still owe an answer: kind task, question or review with no out.jsonl line
    whose `re` is that id. Read state is deliberately ignored — a wait whose session was abandoned
    marks mail read without anyone handling it, so the answer is still owed."""
    answered = answered_ids(jsonl(os.path.join(box, "out.jsonl")))
    return [m for m in jsonl(os.path.join(box, "in.jsonl")) if owes_answer(m, answered)]

def write_atomic(path, data):
    """Replace `path` with `data` through a temp file beside it. The temp name is opened non-blocking
    on purpose: a FIFO planted at that name would otherwise hold the open until a reader appears, and
    a failed open is reported as the OSError the callers already handle."""
    tmp = f"{path}.tmp.{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NONBLOCK, 0o666)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

def append_fd(path):
    """A descriptor to append one line through, or exit 2. O_NONBLOCK on top of O_APPEND is what makes
    this safe: a FIFO renamed onto the path after the stat above returns at once instead of holding the
    open until a reader appears, and the fstat on the descriptor says what was really opened. A path
    that is not a real file is refused before a byte is written, so a line can never be lost into one."""
    if not regular(path):
        die(f"cannot append to {path}: it is not a regular file")
    try:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NONBLOCK, 0o666)
    except OSError as exc:
        die(f"cannot append to {path}: {exc.strerror or exc}")
    try:
        real = stat.S_ISREG(os.fstat(fd).st_mode)
    except OSError:
        real = False
    if not real:
        os.close(fd)
        die(f"cannot append to {path}: it is not a regular file")
    return fd

def append_line(path, obj):
    fd = append_fd(path)
    data = (json.dumps(obj, ensure_ascii=False) + "\n").encode()
    with os.fdopen(fd, "ab") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())

def append_unique(path, msg):
    """Append one mail line, drawing another id if that mailbox already used this one.
    The check runs under the same lock as the write, so two senders cannot pick the same id."""
    fd = append_fd(path)
    with os.fdopen(fd, "ab") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        seen = {m.get("id") for m in jsonl(path) if isinstance(m.get("id"), str)}
        while msg["id"] in seen:
            msg["id"] = new_id()
        fh.write((json.dumps(msg, ensure_ascii=False) + "\n").encode())
        fh.flush()
        os.fsync(fh.fileno())

def show_mail(project, m, tag):
    re_ = f" re={m['re']}" if m.get("re") else ""
    refs = [x for x in m["refs"] if isinstance(x, str)] if isinstance(m.get("refs"), list) else []
    refs = f" refs={','.join(refs)}" if refs else ""
    print(f"{tag} {project} {m.get('id')} {m.get('t')} {m.get('from')} kind={m.get('kind')} task={m.get('task')}{re_}{refs}")
    print(f"    {m.get('text', '')}")

@contextlib.contextmanager
def dot_lock(live_dir):
    """Serialize ticks per dot by locking the dot's live folder itself, so no lock file is ever created."""
    fd = os.open(live_dir, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)

def tick(a):
    dot, agent = token("DOT", a.dot), token("agent", a.agent)
    live_dir = os.path.join(BASE, "live", dot)
    if not os.path.isdir(live_dir):
        die(f"no live/{dot} folder; check the DOT name")
    with dot_lock(live_dir):
        tick_locked(a, dot, agent, live_dir)

def tick_locked(a, dot, agent, live_dir):
    live_path = os.path.join(live_dir, f"{agent}.jsonl")
    mem_dir = os.path.join(BASE, "memory", dot)
    mem_path = os.path.join(mem_dir, f"{agent}.json")
    prev_line = (v1_lines(live_path) or [None])[-1] or {}
    prev = load_json(mem_path) or {}

    line = {"t": now(), "agent": agent}
    for f in ("role", "model", "project"):
        v = getattr(a, f) or prev_line.get(f)
        if not v:
            die(f"--{f} is required: {agent} has no earlier v1 line to take it from")
        line[f] = text(f, v)
    line["task"] = text("task", a.task)
    line["state"] = a.state
    line["host"] = text("host", a.host)
    line["note"] = text("note", a.note)
    line["evidence"] = text("evidence", a.evidence)

    same_task = prev.get("task") == line["task"] and prev.get("agent") == agent
    if not same_task and not (a.goal and a.doing and a.next):
        die("a new task (or a first memory) needs --goal, --doing and --next")
    mem = {"v": 1, "t": line["t"], "dot": dot, "agent": agent, "project": line["project"], "task": line["task"],
           "state": line["state"], "host": line["host"]}
    for f, limit in (("goal", MAX_TEXT), ("doing", MAX_TEXT), ("next", MAX_TEXT)):
        v = getattr(a, f) or (prev.get(f) if same_task else None)
        mem[f] = text(f, v, limit)
    decisions = [d for d in (prev.get("decisions") if same_task else None) or [] if isinstance(d, dict)]
    decisions += [{"t": line["t"], "text": text("decision", d, MAX_ITEM)} for d in a.decision]
    mem["decisions"] = decisions[-MAX_DECISIONS:]
    blockers = [] if a.unblock or not same_task else [b for b in prev.get("blockers") or [] if isinstance(b, str)]
    for b in a.blocker:
        b = text("blocker", b, MAX_ITEM)
        if b not in blockers:
            blockers.append(b)
    if len(blockers) > MAX_BLOCKERS:
        die(f"{len(blockers)} blockers; keep at most {MAX_BLOCKERS} (use --unblock and restate the live ones)")
    if line["state"] == "blocked" and not blockers:
        die("state blocked needs --blocker naming the missing fact or capability")
    mem["blockers"] = blockers

    boxes = mailboxes(agent)
    if line["state"] == "done":
        owed = [(project_of(box), m) for box in boxes for m in waiting_on(box)]
        if owed:
            out = [f"dots: done refused: {agent} still owes an answer to {len(owed)} message(s)"]
            for project, m in owed:
                out.append(f"  {m.get('id')} {project} kind={m.get('kind')} task={m.get('task')}")
            project, m = owed[0]
            out.append(f"  answer it: python3 {os.path.abspath(__file__)} reply {project} {agent} result "
                       f"{json.dumps(m.get('task'), ensure_ascii=False)} --re {m.get('id')} "
                       f"--ref <what it is about>")
            out.append("  that reply stays open and prints the main session's answer, so it is the way out; "
                       "nothing was written")
            print("\n".join(out), file=sys.stderr)
            sys.exit(2)
    took_box = None
    if a.took:
        took_box = next((b for b in boxes if a.took in [m.get("id") for m in jsonl(os.path.join(b, "in.jsonl"))]), None)
        if not took_box:
            die(f"--took {a.took} is not in any {agent} inbox")
    # Read before the write: this is the mail that arrived since the agent's last tick.
    steer = [(project_of(box), m) for box in boxes for m in unread_upto(box, a.took)]

    os.makedirs(mem_dir, exist_ok=True)
    append_line(live_path, line)
    write_atomic(mem_path, json.dumps(mem, ensure_ascii=False, indent=1) + "\n")
    if took_box:
        write_atomic(os.path.join(took_box, ".agent-read"), a.took + "\n")
    if steer:
        print(f"MAIL_FIRST {len(steer)} unread for {agent} — handle these before your next step")
        for project, m in steer:
            show_mail(project, m, "MAIL_NEW")
    print(f"LIVE {dot}/{agent} {line['state']} t={line['t']}")
    print(f"MEMORY {dot}/{agent} decisions={len(mem['decisions'])} blockers={len(mem['blockers'])}")
    if not boxes:
        print("MAIL none (no mailbox for this agent)")
        return
    shown = 0
    for box in boxes:
        project = project_of(box)
        for m in mail_state(box)["unanswered"]:
            show_mail(project, m, "MAIL_UNANSWERED")
            shown += 1
    if not shown and not steer:
        print("MAIL none (inbox read and answered)")

def roster_states(roster):
    """Agent id -> roster state from slots, agents or actors. Ids that are not tokens are dropped and counted."""
    out, dropped = {}, 0
    for key in ("slots", "agents", "actors"):
        for s in (roster or {}).get(key) or []:
            if not isinstance(s, dict):
                continue
            name = s.get("agent") or s.get("agent_id") or s.get("id")
            if not (isinstance(name, str) and TOKEN.match(name)):
                dropped += 1
                continue
            if name not in out:
                out[name] = s.get("state") or s.get("slot_state") or s.get("status") or "-"
    return out, dropped

def board_rows(dots):
    rows, errors = [], []
    for dot in dots:
        live_dir = os.path.join(BASE, "live", dot)
        if not os.path.isdir(live_dir):
            errors.append({"dot": dot, "error": "no live folder"})
            continue
        mem_dir = os.path.join(BASE, "memory", dot)
        roster, dropped = roster_states(load_json(os.path.join(live_dir, "roster.json")))
        if dropped:
            errors.append({"dot": dot, "error": f"roster.json: {dropped} slot(s) without a valid agent id skipped"})
        names = list(roster)
        names += sorted(f[:-6] for f in os.listdir(live_dir) if f.endswith(".jsonl") and f[:-6] not in names and TOKEN.match(f[:-6]))
        if os.path.isdir(mem_dir):
            names += sorted(f[:-5] for f in os.listdir(mem_dir) if f.endswith(".json") and f[:-5] not in names and TOKEN.match(f[:-5]))
        for agent in names:
            live_path = os.path.join(live_dir, f"{agent}.jsonl")
            last = (v1_lines(live_path) or [None])[-1]
            mem = load_json(os.path.join(mem_dir, f"{agent}.json"))
            age = age_min(last["t"]) if last else None
            flags = []
            if not last:
                flags.append("NO-LIVE")
                lt = legacy_t(live_path) if os.path.exists(live_path) else None
                # live-status: an agent with no v1 line is checked as stalled; only a recent legacy line holds that off.
                if os.path.exists(live_path) and (lt is None or age_min(lt.isoformat()) > STALL_MIN):
                    flags.append("STALLED")
            elif last["state"] == "blocked":
                flags.append("BLOCKED")
            elif last["state"] in ("start", "step") and age is not None and age > STALL_MIN:
                flags.append("STALLED")
            if last and (not mem or mem.get("task") != last["task"]
                         or (parse_t(mem.get("t")) or last_t_min()) < parse_t(last["t"])):
                flags.append("MEMO-BEHIND" if mem else "NO-MEMO")
            mail = {"new": 0, "unanswered": 0, "waiting": 0, "oldest_waiting_min": None, "unread_out": 0}
            boxes = mailboxes(agent)
            waiting = []
            for box in boxes:
                st = mail_state(box)
                for k in ("new", "unanswered", "unread_out"):
                    mail[k] += len(st[k])
                waiting += waiting_on(box)
            if waiting:
                ages = [x for x in (age_min(m.get("t")) for m in waiting) if x is not None]
                mail["waiting"] = len(waiting)
                mail["oldest_waiting_min"] = round(max(ages)) if ages else None
            if mail["unanswered"]:
                flags.append("MAIL-UNANSWERED")
            # an agent that holds a wait open will read it; one that ended its turn never will
            if waiting and listen_state(agent) is None:
                flags.append("MAIL_WAITING")
            claim = claim_stale(last, waiting, roster.get(agent))
            if claim:
                flags.append("CLAIM-STALE")
            rows.append({"dot": dot, "agent": agent, "roster": roster.get(agent),
                         "state": last["state"] if last else None, "t": last["t"] if last else None,
                         "age_min": None if age is None else round(age), "task": last["task"] if last else None,
                         "host": last["host"] if last else None,
                         "doing": (mem or {}).get("doing"), "next": (mem or {}).get("next"),
                         "blockers": (mem or {}).get("blockers") or [], "mail": mail if boxes else None,
                         "claim_stale": claim, "flags": flags})
    return rows, errors

def claim_stale(last, waiting, roster_state):
    """A claim the evidence disproves, from the files only. The roster is the root's word and is
    never evidence: this returns a reason string, or None.

    (a) The agent's own newest live line says done or blocked, yet a main-session message newer than
        that line is still waiting for an answer.
    (b) The roster claims the slot is done or idle while the agent's own newest live line is start or
        step, which is the agent working.
    """
    if last:
        t_last = parse_t(last.get("t"))
        # the message's second may equal the line's: a message at or after the line is still newer
        newer = [m for m in waiting
                 if t_last is None or (parse_t(m.get("t")) or t_last) >= t_last]
        if newer and last.get("state") in ("done", "blocked"):
            return (f"live {last['state']} at {last.get('t')} but {len(newer)} message(s) newer than that line "
                    f"still need an answer")
    if last and last.get("state") in ("start", "step") and isinstance(roster_state, str) \
            and roster_state.lower() in ROSTER_DONE:
        return f"roster claims {roster_state} but the agent's own newest live line is {last['state']}"
    return None

def last_t_min():
    return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

def clip(s, n=110):
    s = str(s or "-")
    return s if len(s) <= n else s[:n - 1] + "…"

def board(a):
    live_root = os.path.join(BASE, "live")
    dots = a.dots or sorted(d for d in os.listdir(live_root) if os.path.isdir(os.path.join(live_root, d)))
    for d in dots:
        token("DOT", d)
    rows, errors = board_rows(dots)
    if a.json:
        print(json.dumps({"t": now(), "agents": rows, "errors": errors}, ensure_ascii=False, indent=1))
        return
    print(f"board {now()}  stall>{STALL_MIN}min")
    for e in errors:
        print(f"{e['dot']}: {e['error']}")
    for r in rows:
        age = "-" if r["age_min"] is None else f"{r['age_min']}m"
        flags = f"  [{' '.join(r['flags'])}]" if r["flags"] else ""
        print(f"{r['dot']} {r['agent']}  {r['state'] or '-'} {age} host={r['host'] or '-'} "
              f"roster={r['roster'] or '-'} (the root's claim)  {clip(r['task'], 60)}{flags}")
        if r.get("claim_stale"):
            print(f"    claim: {clip(r['claim_stale'], 140)}")
        if r["doing"] or r["next"]:
            print(f"    doing: {clip(r['doing'])}")
            print(f"    next:  {clip(r['next'])}")
        for b in r["blockers"][:3]:
            print(f"    block: {clip(b)}")
        if r["mail"]:
            m = r["mail"]
            waiting = (f"; {m['waiting']} waiting (oldest {age_label(m['oldest_waiting_min'])})"
                       if m.get("waiting") else "")
            # the old line stays an exact prefix: an external reader that greps it keeps working
            print(f"    mail:  to agent {m['new']} unread, {m['unanswered']} unanswered; "
                  f"from agent {m['unread_out']} unread{waiting}")

def memo(a):
    dot, agent = token("DOT", a.dot), token("agent", a.agent)
    mem = load_json(os.path.join(BASE, "memory", dot, f"{agent}.json"))
    print(json.dumps(mem, ensure_ascii=False, indent=1) if mem else f"(no memory for {dot}/{agent})")
    for ln in v1_lines(os.path.join(BASE, "live", dot, f"{agent}.jsonl"))[-3:]:
        print(f"live {ln['t']} {ln['state']} host={ln['host']} task={ln['task']}")
        print(f"    {ln['note']}  [{ln['evidence']}]")

# ---------------------------------------------------------------- config, init

def config():
    cfg = load_json(os.path.join(BASE, "dots.json")) or {}
    return cfg

def configured(cfg, key):
    """The list under key, tolerating a missing or hand-edited entry."""
    items = cfg.get(key)
    return [x for x in items if isinstance(x, dict)] if isinstance(items, list) else []

def main_name(project, cfg=None):
    for p in configured(cfg or config(), "projects"):
        if p.get("id") == project and isinstance(p.get("main"), str) and TOKEN.match(p["main"]):
            return p["main"]
    return f"{project}-main"

def configured_dots(cfg=None):
    return [d["id"] for d in configured(cfg or config(), "dots")
            if isinstance(d.get("id"), str) and TOKEN.match(d["id"])]

def last_tick(agent):
    """(datetime, dot) of the agent's newest v1 live line in any dot, or None."""
    best = None
    for path in glob.glob(os.path.join(BASE, "live", "*", glob.escape(agent) + ".jsonl")):
        lines = v1_lines(path)
        if not lines:
            continue
        t = parse_t(lines[-1]["t"])
        if t and (best is None or t > best[0]):
            best = (t, os.path.basename(os.path.dirname(path)))
    return best

def dot_for(agent, cfg=None):
    """The dot the agent's newest live line belongs to; else the only configured dot; else None."""
    seen = last_tick(agent)
    if seen:
        return seen[1]
    dots = configured_dots(cfg)
    return dots[0] if len(dots) == 1 else None

def age_label(minutes):
    if minutes is None:
        return "none"
    if minutes >= 120:
        return f"{int(minutes // 60)}h"
    return f"{max(0, int(minutes))}m"

def marker_paths(agent):
    """Every `wait` marker for that agent: one file per waiting process, so two waiters never collide.
    Only `<agent>.<digits>.json` matches, so a dotted id such as `agent-1.x` never shadows `agent-1`."""
    exact = re.compile(re.escape(agent) + r"\.[0-9]+\.json\Z")
    return sorted(p for p in glob.glob(os.path.join(BASE, "listen", glob.escape(agent) + ".*.json"))
                  if exact.match(os.path.basename(p)))

def marker_live(m):
    """A marker is not listening when its `until` is missing, past or implausibly far away, when its
    pid is invalid, or when its own process here is gone. A marker written on another host is judged
    by `until` alone, because its pid says nothing here: with no usable `until` it proves nothing.

    The cap matters as much as the rule: the folder is writable by the dot root, so a marker naming a
    live local pid and a date far in the future would otherwise look like a waiter forever. A marker
    that names a live local pid and a plausible `until` is indistinguishable from a real waiter — the
    process has to be looked at, and that is stated in workflows/direct-mail.md. The cap carries an
    hour of slack because the waiter's clock may run ahead of the reader's: a waiter holding the
    longest allowed timeout must not be judged dead by a reader whose clock is a few minutes behind."""
    until = parse_t(m.get("until"))
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if until is None or until <= now_utc \
            or until > now_utc + datetime.timedelta(seconds=LISTEN_MAX_S + LISTEN_SLACK_S):
        return False
    pid = m.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return False
    if m.get("host") and m["host"] != socket.gethostname():
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True

def listen_state(agent):
    """One live `wait` marker for that agent, or None."""
    for path in marker_paths(agent):
        m = load_json(path)
        if m and marker_live(m):
            return m
    return None

def prune_markers(agent):
    """Remove markers of waiters that are gone; never one that is still listening. A marker path the
    root made undeletable — a folder named like a marker, a file in a folder with no write permission —
    is left alone: the marker already reads as not listening, and the command must not die on it."""
    for path in marker_paths(agent):
        m = load_json(path)
        if m is None or not marker_live(m):
            try:
                os.unlink(path)
            except OSError:
                pass

# ---------------------------------------------------------------- mail

def new_id():
    return f"m-{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(2)}"

def stdin_body(name="message text"):
    return block(name, sys.stdin.read())

def refs_of(values):
    refs = []
    for r in values:
        refs.append(text("ref", r))
    return refs

def mail_line(project, agent, kind, task, re_id, refs, body, sender):
    return {"v": 1, "id": new_id(), "t": now(), "from": sender, "to": agent, "task": task,
            "kind": kind, "re": re_id, "text": body, "refs": refs}

def mail_box(project, agent, create=False):
    box = os.path.join(BASE, project, "mail", agent)
    if create:
        os.makedirs(box, exist_ok=True)
    return box

def project_dirs():
    return sorted(os.path.basename(os.path.dirname(d)) for d in glob.glob(os.path.join(BASE, "*", "mail")) if os.path.isdir(d))

def send(a):
    project, agent = token("project", a.project), token("agent", a.agent)
    if a.kind not in TO_AGENT:
        die(f"kind must be one of {list(TO_AGENT)}")
    task = text("task", a.task)
    refs = refs_of(a.ref)
    body = stdin_body()
    re_id = token("re", a.re) if a.re else None
    new_project = not os.path.isdir(os.path.join(BASE, project))
    box = mail_box(project, agent, create=True)
    known = {m.get("id") for m in jsonl(os.path.join(box, "in.jsonl")) if isinstance(m.get("id"), str)}
    known |= {m.get("id") for m in jsonl(os.path.join(box, "out.jsonl")) if isinstance(m.get("id"), str)}
    if re_id and re_id not in known:
        die(f"--re {re_id} is not in {project}/mail/{agent}/in.jsonl or out.jsonl")
    msg = mail_line(project, agent, a.kind, task, re_id, refs, body, main_name(project))
    append_unique(os.path.join(box, "in.jsonl"), msg)
    print(msg["id"], flush=True)
    if new_project:
        print(f"PROJECT {project} (new mail folder)", flush=True)
    cfg = config()
    prune_markers(agent)
    live = listen_state(agent)
    if live:
        print(f"LISTENING dot={live.get('dot', '-')} pid={live.get('pid')} since={live.get('since', '-')}"
              f" host={live.get('host', '-')}", flush=True)
        return
    seen = last_tick(agent)
    print(f"NOT_LISTENING last_tick={age_label(None if seen is None else age_min(seen[0].isoformat()))}", flush=True)
    dot = dot_for(agent, cfg)
    argv, missing = wake_argv(cfg, project, agent, msg["id"], dot)
    if argv:
        code = run_wake(argv)
        print(f"WAKE exit={code}", flush=True)
    elif missing:
        print(f"WAKE not_run missing={missing}", flush=True)
        doorbell(project, agent, msg["id"], dot)
    else:
        doorbell(project, agent, msg["id"], dot)

def argv_list(argv):
    return argv if isinstance(argv, list) and argv and all(isinstance(x, str) and x for x in argv) else None

def argv_for(argv, values):
    """Substitute {project} {agent} {id} {dot} {age}; a placeholder with no value is reported, never blanked."""
    argv = argv_list(argv)
    if not argv:
        return None, None
    for name in sorted(set(PLACEHOLDER.findall(" ".join(argv)))):
        if name == "dot" and not values.get("dot"):
            return None, "dot"
        if name == "age" and values.get("age") is None:
            return None, "age"
    return [PLACEHOLDER.sub(lambda m: str(values[m.group(1)]), x) for x in argv], None

def wake_argv(cfg, project, agent, msg_id, dot):
    return argv_for(cfg.get("wake"), {"project": project, "agent": agent, "id": msg_id, "dot": dot or ""})

def run_wake(argv):
    print(f"WAKE ran {json.dumps(argv, ensure_ascii=False)}", flush=True)
    try:
        return subprocess.run(argv, stdin=subprocess.DEVNULL, timeout=WAKE_TIMEOUT).returncode
    except subprocess.TimeoutExpired:
        return "timeout"
    except OSError as exc:
        return f"error:{exc.errno}"

def doorbell(project, agent, msg_id, dot):
    print(f"DOORBELL [{main_name(project)} → {dot or '<DOT>'}] {agent}: mail {msg_id} unread; "
          f"wake it to read its inbox (dots.py wait {dot or '<DOT>'} {agent})", flush=True)

def inbox(a):
    projects = [token("project", a.project)] if a.project else project_dirs()
    if not projects:
        print("MAIL none (no project mail folders)")
        return
    shown = 0
    for project in projects:
        mail_root = os.path.join(BASE, project, "mail")
        if not os.path.isdir(mail_root):
            if a.project:
                die(f"no {project}/mail folder")
            continue
        agents = [token("agent", a.agent)] if a.agent else sorted(os.listdir(mail_root))
        for agent in agents:
            box = os.path.join(mail_root, agent)
            if not os.path.isdir(box):
                if a.agent:
                    die(f"no {project}/mail/{agent} folder")
                continue
            for m in mail_state(box)["unread_out"]:
                show_mail(project, m, "MAIL_OUT")
                shown += 1
    if not shown:
        print("MAIL none (no unread replies)")

def read(a):
    project, agent, mid = token("project", a.project), token("agent", a.agent), token("id", a.id)
    box = mail_box(project, agent)
    if mid not in [m.get("id") for m in jsonl(os.path.join(box, "out.jsonl"))]:
        die(f"{mid} is not in {project}/mail/{agent}/out.jsonl")
    write_atomic(os.path.join(box, ".main-read"), mid + "\n")
    print(f"READ {project}/{agent} {mid}")

def reply(a):
    project, agent = token("project", a.project), token("agent", a.agent)
    if a.kind not in TO_MAIN:
        die(f"kind must be one of {list(TO_MAIN)}")
    task = text("task", a.task)
    refs = refs_of(a.ref)
    re_id = token("re", a.re) if a.re else None
    waits = a.kind in REPLY_WAITS and not a.no_wait
    timeout = timeout_of(a) if waits else None
    body = stdin_body()
    box = mail_box(project, agent)
    if re_id and (not os.path.isdir(box)
                  or re_id not in [m.get("id") for m in jsonl(os.path.join(box, "in.jsonl"))]):
        die(f"--re {re_id} is not in {project}/mail/{agent}/in.jsonl")
    elif not re_id and a.kind in REPLY_WAITS:
        # A first result often answers a task the agent got from its dot root, not a mail line,
        # so `re` is null there; a ref then says what the result is about.
        if not refs:
            die(f"a {a.kind} with no --re needs at least one --ref naming what it is about "
                f"(rev/*, a commit SHA, gate:ID, supersedes:ID)")
    elif not re_id:
        die("reply needs --re with the id of the message it answers")
    dot = None
    if waits:
        dot = token("DOT", a.dot) if a.dot else dot_for(agent)
        if not dot:
            die(f"no dot for {agent}: pass --dot (no live/ folder names it)")
    new_box = not os.path.isdir(box)
    os.makedirs(box, exist_ok=True)   # a first result may be the first line in this mailbox
    msg = mail_line(project, agent, a.kind, task, re_id, refs, body, agent)
    append_unique(os.path.join(box, "out.jsonl"), msg)
    print(msg["id"], flush=True)
    if new_box:
        print(f"MAILBOX {project}/{agent} (new folder)", flush=True)
    if waits:
        print(f"REPLY waiting for the answer to {msg['id']} (dot={dot}); do not end the turn — this command "
              f"prints it. On exit 3 tick step and wait again: the result is already written.", flush=True)
        return wait_loop(dot, agent, timeout, expect=msg["id"])
    return 0

# ---------------------------------------------------------------- wait

def timeout_of(a):
    if a.timeout <= 0:
        die("--timeout must be a positive number of seconds")
    if a.timeout > LISTEN_MAX_S:
        die(f"--timeout takes at most {LISTEN_MAX_S} seconds (a day); a longer wait is restarted, not held")
    return a.timeout

def wait(a):
    dot, agent = token("DOT", a.dot), token("agent", a.agent)
    return wait_loop(dot, agent, timeout_of(a))

def wait_loop(dot, agent, timeout, expect=None):
    """Print unread mail at once, or block until it lands; the loop `reply` continues into after a
    result, question or block. Marks what it printed read, so the next call blocks instead of
    repeating it. 0 mail, 3 timeout, 4 signal. Holds listen/<agent>.<pid>.json while blocking.

    With `expect`, the id of the reply that is waiting, only a message whose `re` is that id ends the
    wait: any other mail is printed and marked delivered on the way, because a note or a question is
    not the answer, and mail already unread when the reply started must not end it either.

    `shown` holds what this process has already printed. A line whose id cannot be marked delivered —
    a hand-written one with a bad id, or a mailbox whose `.agent-read` the root made a folder — would
    otherwise print again on every poll, once a second; `warned` keeps the marker failure to one line."""
    shown = set()
    warned = set()

    def fresh(pending):
        out = []
        for box, project, m in pending:
            key = (box, json.dumps(m, sort_keys=True, ensure_ascii=False))
            if key in shown:
                continue
            shown.add(key)
            out.append((box, project, m))
        return out

    def answer_arrived(pending):
        return bool(pending) if expect is None else any(m.get("re") == expect for _box, _proj, m in pending)

    pending = fresh(unread_all(agent))
    if pending:
        print_pending(pending)
        mark_delivered(pending, warned)
        if answer_arrived(pending):
            return 0
    if not os.path.isdir(os.path.join(BASE, "live", dot)):
        print(f"dots: no live/{dot} folder; waiting anyway", file=sys.stderr)
    listen_dir = os.path.join(BASE, "listen")
    os.makedirs(listen_dir, exist_ok=True)
    path = os.path.join(listen_dir, f"{agent}.{os.getpid()}.json")
    until = iso_after(timeout)
    previous = {s: signal.getsignal(s) for s in SIGNALS}
    for s in SIGNALS:
        signal.signal(s, stop_now)
    try:
        write_atomic(path, json.dumps({"pid": os.getpid(), "dot": dot, "since": now(), "until": until,
                                       "host": socket.gethostname()}, ensure_ascii=False) + "\n")
        prune_markers(agent)
        print(f"WAIT listening dot={dot} agent={agent} pid={os.getpid()} until={until} host={socket.gethostname()}",
              flush=True)
        deadline = time.monotonic() + timeout
        while True:
            time.sleep(POLL_S)
            pending = fresh(unread_all(agent))
            if pending:
                print_pending(pending)
                mark_delivered(pending, warned)
                if answer_arrived(pending):
                    return 0
            if time.monotonic() >= deadline:
                print(f"WAIT timeout dot={dot} agent={agent} after={timeout}s unread=0", flush=True)
                return 3
    finally:
        drop_marker(path)
        for s, handler in previous.items():
            signal.signal(s, handler)

def stop_now(signum, frame):
    raise SystemExit(4)

def drop_marker(path):
    """Remove this process's own marker. Another waiter has its own file and keeps it."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

def print_pending(pending):
    for _box, project, m in pending:
        show_mail(project, m, "MAIL_NEW")
    print(f"WAIT mail={len(pending)} unread (handle these before your next step)", flush=True)

def mark_delivered(pending, warned):
    """Move each box's read marker to the newest message `wait` printed, so the next wait blocks
    instead of printing it again. `tick --took` moves the same marker for a working agent.

    A marker path the root made unwritable — a folder named `.agent-read`, a mailbox with no write
    permission — is reported once per box and never raised: the line was printed and the set of what
    was shown keeps this command from printing it again, but the mail stays unread on disk."""
    for box, _project, m in pending:
        mid = m.get("id")
        if isinstance(mid, str) and TOKEN.match(mid):
            try:
                write_atomic(os.path.join(box, ".agent-read"), mid + "\n")
            except OSError as exc:
                if box not in warned:
                    warned.add(box)
                    print(f"dots: cannot mark {box} read ({exc.__class__.__name__}); it stays unread",
                          file=sys.stderr)

# ---------------------------------------------------------------- watch

def watch_cfg(cfg):
    """The two delays, and where each came from: dots.json's own `watch` object, else the default."""
    w = cfg.get("watch") if isinstance(cfg.get("watch"), dict) else {}
    out, src = {}, {}
    for k, default in WATCH_DEFAULTS.items():
        v = w.get(k)
        if isinstance(v, int) and not isinstance(v, bool) and v >= 0:
            out[k], src[k] = v, "dots.json"
        else:
            out[k], src[k] = default, "default"
    return out, src

def watch_settings(a, cfg):
    """Effective watchdog settings; the command line wins over dots.json, which sits in the folder the
    dot root writes. Returns (wake argv, notify argv, delays, state path, sources) where every source
    is `argv`, `dots.json`, `default` or `none`."""
    wake = notify = None
    src = {}
    for key in ("wake", "notify"):
        spec = getattr(a, f"{key}_cmd", None)
        if spec is not None:
            argv = cli_argv(spec, f"--{key}-cmd")
            src[key] = "argv"
        else:
            argv = cfg.get(key) if argv_list(cfg.get(key)) else None
            src[key] = "dots.json" if argv else "none"
        if key == "wake":
            wake = argv
        else:
            notify = argv
    delays, dsrc = watch_cfg(cfg)
    for key in WATCH_DEFAULTS:
        v = getattr(a, key, None)
        if v is not None:
            if v < 0:
                die(f"--{key.replace('_', '-')} takes minutes, 0 or more")
            delays[key], src[key] = v, "argv"
        else:
            src[key] = dsrc[key]
    path = os.path.abspath(os.path.expanduser(a.state)) if a.state else state_path()
    src["state"] = "argv" if a.state else "default"
    return wake, notify, delays, path, src

def state_path():
    return os.path.join(BASE, "watch", "state.json")

def load_state(path):
    """The watchdog's own ledger: one record per message id, so a restart never rings twice. It is
    the only thing that can silence a ring, so every suppression it causes is printed. A record that
    is not an object — a list, a string, null — is dropped: nothing in it can be read, and dropping
    it can only make a later run ring again."""
    st = load_json(path) or {}
    msgs = st.get("messages")
    clean = {k: v for k, v in (msgs.items() if isinstance(msgs, dict) else ())
             if isinstance(k, str) and isinstance(v, dict)}
    return {"v": 1, "messages": clean}

def save_state(path, st, warn=None):
    """Write the ledger. A path the root made unwritable — a folder where `state.json` should be, a
    folder with no write permission — is reported once and never raised: a record that cannot be
    written can only make a later run ring again, which is the direction this ledger may fail in."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        write_atomic(path, json.dumps(st, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    except OSError as exc:
        if warn is not None and path not in warn:
            warn.add(path)
            print(f"dots: cannot write the ledger {path} ({exc.__class__.__name__}); "
                  f"every run rings again until it is writable", file=sys.stderr)

def owed_mail():
    """(project, agent, box, message) for every inbox line that still owes an answer, read from the
    mail folders only: no roster, no live line and no dots.json entry decides what is listed. A line
    is owed until an out.jsonl line answers it, whether or not it was marked read."""
    out = []
    for box in sorted(glob.glob(os.path.join(BASE, "*", "mail", "*"))):
        agent = os.path.basename(box)
        if not (os.path.isdir(box) and TOKEN.match(agent)):
            continue
        out += [(project_of(box), agent, box, m) for m in waiting_on(box)]
    return out

def unread_in(box, mid):
    return mid in [m.get("id") for m in mail_state(box)["new"]]

def ring(argv, values):
    """Run one configured command once: (code, None) or (None, why it did not run)."""
    sub, missing = argv_for(argv, values)
    if sub is None:
        return None, f"missing={missing}" if missing else "not_configured"
    try:
        return subprocess.run(sub, stdin=subprocess.DEVNULL, timeout=WAKE_TIMEOUT).returncode, None
    except subprocess.TimeoutExpired:
        return "timeout", None
    except OSError as exc:
        return f"error:{exc.errno}", None

def watch_once(expected, wake, notify, delays, st_path, src):
    print("WATCH_CONFIG " + " ".join(
        [f"wake={src['wake']}:{json.dumps(wake, ensure_ascii=False)}" if wake else "wake=none",
         f"notify={src['notify']}:{json.dumps(notify, ensure_ascii=False)}" if notify else "notify=none",
         f"after_min={src['after_min']}:{delays['after_min']}",
         f"notify_after_min={src['notify_after_min']}:{delays['notify_after_min']}",
         f"state={src['state']}:{st_path}"]), flush=True)
    if expected:
        path = os.path.abspath(__file__)
        actual = None
        fd = open_raw(path)   # O_NONBLOCK: a copy swapped for a FIFO returns here, not in open
        if fd is not None:
            try:
                with os.fdopen(fd, "rb") as fh:
                    actual = hashlib.sha256(fh.read()).hexdigest()
            except OSError:
                actual = None
        if actual != expected:
            print(f"WATCH_TOOL_CHANGED expected={expected} actual={actual or 'unreadable'} "
                  f"path={path}", flush=True)
    cfg = config()
    st = load_state(st_path)
    seen = set()
    dirty = False
    no_wake = no_notify = False
    ledger_warned = set()
    for project, agent, box, m in owed_mail():
        mid = m.get("id")
        if not isinstance(mid, str) or not TOKEN.match(mid):
            # no id, no ledger record and no wake: say so rather than skip the line in silence,
            # because board still counts this message as owed mail
            print(f"WATCH_ERROR {project} {agent} {mid!r} unusable id", flush=True)
            continue
        seen.add(mid)
        rec = st["messages"].setdefault(mid, {})
        rec.setdefault("project", project)
        rec.setdefault("agent", agent)
        if not rec.get("seen_t"):
            # the ledger remembers the first sight of an owed line, so one deleted before its first
            # ring still ends as WATCH_RESOLVED instead of vanishing
            rec["seen_t"] = now()
            dirty = True
        # an unparseable or future `t` must not hide a message: fall back to when it was first seen,
        # and only a time in the past is an age — a future one cannot make a message old enough
        ages = [a for a in (age_min(m.get("t")), age_min(rec.get("seen_t")))
                if a is not None and a >= 0]
        if not ages:
            print(f"WATCH_ERROR {project} {agent} {mid} no usable time t={m.get('t')!r} "
                  f"seen_t={rec.get('seen_t')!r}", flush=True)
            continue
        age = max(ages)
        held = listen_state(agent)
        dot = dot_for(agent, cfg)
        values = {"project": project, "agent": agent, "id": mid, "dot": dot or "", "age": round(age)}
        if held and age >= delays["after_min"]:
            # a marker holds the wake back. A real waiter prints and marks unread mail within POLL_S,
            # so owed mail still unread past after_min contradicts it: say who holds it, every run.
            print(f"WATCH_HELD {project} {agent} {mid} age={round(age)}m pid={held.get('pid')} "
                  f"host={held.get('host') or '-'} until={held.get('until') or '-'} "
                  f"read={'no' if unread_in(box, mid) else 'yes'}", flush=True)
        if held is None and age >= delays["after_min"]:
            if rec.get("wake_t"):
                # the ledger, not the evidence, is why no wake runs: say so, then go on to notify
                print(f"WATCH_SKIP {project} {agent} {mid} reason=wake_already wake_t={rec['wake_t']} "
                      f"wake_age_min={rec.get('wake_age_min', '-')}", flush=True)
            else:
                code, detail = ring(wake, values)
                if detail == "not_configured":
                    if not no_wake:
                        print("WATCH_ERROR no wake command; an ended agent cannot be restarted", flush=True)
                        no_wake = True
                else:
                    if code is not None:
                        rec.update({"wake_t": now(), "wake_age_min": round(age), "dot": dot or None})
                        save_state(st_path, st, ledger_warned)
                    print(f"WATCH_WAKE {project} {agent} {mid} age={round(age)}m dot={dot or '-'} "
                          f"exit={detail or code}", flush=True)
        if not rec.get("wake_t"):
            continue
        since = age_min(rec["wake_t"])
        if since is None or since < delays["notify_after_min"]:
            continue
        # A message is owed until it is answered, read or not. A read marker lives in the same folder
        # as the mail, so it can add a visible event here, never silence this one.
        if not unread_in(box, mid) and not rec.get("stuck_t"):
            rec["stuck_t"] = now()
            save_state(st_path, st, ledger_warned)
            print(f"WATCH_STUCK {project} {agent} {mid} age={round(age)}m read=yes unanswered=yes "
                  f"wake_t={rec['wake_t']}", flush=True)
        if rec.get("notify_t"):
            print(f"WATCH_SKIP {project} {agent} {mid} reason=notify_already notify_t={rec['notify_t']}",
                  flush=True)
            continue
        code, detail = ring(notify, values)
        if detail == "not_configured":
            if not no_notify:
                print("WATCH_ERROR no notify command; the owner is never told", flush=True)
                no_notify = True
            continue
        if code is not None:
            rec["notify_t"] = now()
            save_state(st_path, st, ledger_warned)
        print(f"WATCH_NOTIFY {project} {agent} {mid} age={round(age)}m after_wake={round(since)}m "
              f"read={'no' if unread_in(box, mid) else 'yes'} exit={detail or code}", flush=True)
    if dirty:
        save_state(st_path, st, ledger_warned)   # first sight is a ledger write: a vanished line must resolve, not vanish
    for mid, rec in sorted(st["messages"].items()):
        if mid in seen:
            continue
        del st["messages"][mid]
        save_state(st_path, st, ledger_warned)
        print(f"WATCH_RESOLVED {rec.get('project') or '-'} {rec.get('agent') or '-'} {mid}", flush=True)

def stop_quiet(signum, frame):
    raise SystemExit(0)

def watch(a):
    if a.loop < 0:
        die("--loop takes minutes, 0 to run once")
    expected = (a.expect_sha256 or "").strip().lower()
    if expected and not re.fullmatch(r"[0-9a-f]{64}", expected):
        die("--expect-sha256 takes the 64 hex characters of sha256sum tools/dots.py")
    wake, notify, delays, st_path, src = watch_settings(a, config())
    for s in SIGNALS:
        signal.signal(s, stop_quiet)
    while True:
        watch_once(expected, wake, notify, delays, st_path, src)
        if a.loop == 0:
            return 0
        time.sleep(a.loop * 60)

# ---------------------------------------------------------------- init

DEFAULT_HOSTS = (
    ("vm", "Dots VM: research, read-only analysis, derived work that does not change the canonical copy"),
    ("local", "This computer: canonical writes, the shared folder, long runs, logins, devices, native build"),
)

def ask(prompt, default=""):
    try:
        value = input(prompt).strip()
    except EOFError:
        print()
        return default
    return value or default

def pairs(values, flag):
    out = []
    for v in values:
        left, sep, right = v.partition("=")
        if not sep:
            die(f"--{flag} takes LABEL=VALUE, got {v!r}")
        out.append((left.strip(), right.strip()))
    return out

def cli_argv(spec, flag):
    """One argv list from a --*-cmd JSON argument. `init` and `watch` share the same rule."""
    try:
        argv = json.loads(spec)
    except ValueError:
        die(f"{flag} takes a JSON list of strings; run dots.py init -h for an example")
    if not (isinstance(argv, list) and 0 < len(argv) <= 32
            and all(isinstance(x, str) and x.strip() for x in argv)):
        die(f"{flag} must be a JSON list of 1-32 non-empty strings")
    if any("\n" in x or "\r" in x for x in argv):
        die(f"{flag} entries must be one line each")
    return argv

def set_cmd(cfg, key, spec, report):
    """Store one configured argv list (wake or notify) from --KEY-cmd JSON, validating it first."""
    if spec:
        argv = cli_argv(spec, f"--{key}-cmd")
        if cfg.get(key) and cfg[key] != argv:
            report.append(f"INIT {key} (replaced)")
        elif not cfg.get(key):
            report.append(f"INIT {key} (set)")
        else:
            report.append(f"INIT {key} (kept)")
        cfg[key] = argv
    elif cfg.get(key):
        report.append(f"INIT {key} (kept)")

def init(a):
    tty = sys.stdin.isatty() and not a.yes
    base = os.path.expanduser(a.base) if a.base else default_base()
    if tty:
        base = os.path.expanduser(ask(f"base folder [{base}]: ", base))
    if not a.yes and not tty:
        die("no terminal: pass --yes with --dot/--project/--host flags")
    if os.path.exists(base) and not os.path.isdir(base):
        die(f"{base} is not a folder")

    cfg = load_json(os.path.join(base, "dots.json")) or {"v": 1, "dots": [], "projects": [], "hosts": []}
    cfg.setdefault("v", 1)
    for key in ("dots", "projects", "hosts"):
        if not isinstance(cfg.get(key), list):
            cfg[key] = []

    dot_ids = [d for d in a.dot]
    project_specs = list(a.project)
    host_specs = list(a.host)
    wake_spec = a.wake_cmd or ""
    notify_spec = a.notify_cmd or ""
    if tty:
        have = ", ".join(configured_dots(cfg)) or "none"
        dot_ids += [x.strip() for x in ask(f"dot ids, comma-separated (have {have}): ").split(",") if x.strip()]
        while True:
            spec = ask("project id=main session id (blank to finish): ")
            if not spec:
                break
            project_specs.append(spec)
        while True:
            spec = ask("extra host label=purpose beyond vm and local (blank to finish): ")
            if not spec:
                break
            host_specs.append(spec)
        wake_spec = ask("wake command as a JSON argv list with {dot} {agent} {id} {project} (blank for none): ",
                        wake_spec)
        notify_spec = ask("notify command as a JSON argv list, same placeholders plus {age} (blank for none): ",
                          notify_spec)

    report, dots = [], cfg["dots"]
    known = {d.get("id") for d in dots if isinstance(d, dict)}
    for d in dot_ids:
        d = token("dot", d)
        if d in known:
            report.append(f"INIT dot {d} (kept)")
            continue
        dots.append({"id": d})
        known.add(d)
        report.append(f"INIT dot {d} (added)")

    projects, by_id = cfg["projects"], {p.get("id"): p for p in cfg["projects"] if isinstance(p, dict)}
    for pid, main in pairs(project_specs, "project"):
        pid, main = token("project", pid), token("main", main)
        if pid in by_id:
            if by_id[pid].get("main") != main:
                report.append(f"INIT project {pid} main={by_id[pid].get('main')} (kept; given main={main} not applied)")
            else:
                report.append(f"INIT project {pid} main={main} (kept)")
            continue
        projects.append({"id": pid, "main": main})
        by_id[pid] = projects[-1]
        report.append(f"INIT project {pid} main={main} (added)")

    hosts, labels = cfg["hosts"], {h.get("label") for h in cfg["hosts"] if isinstance(h, dict)}
    for label, purpose in DEFAULT_HOSTS:
        if label not in labels:
            hosts.append({"label": label, "purpose": purpose})
            labels.add(label)
            report.append(f"INIT host {label} (added default)")
    for label, purpose in pairs(host_specs, "host"):
        label = token("host", label)
        purpose = text("host purpose", purpose)
        if label in labels:
            report.append(f"INIT host {label} (kept)")
            continue
        hosts.append({"label": label, "purpose": purpose})
        labels.add(label)
        report.append(f"INIT host {label} (added)")

    set_cmd(cfg, "wake", wake_spec, report)
    set_cmd(cfg, "notify", notify_spec, report)

    if not dots:
        die("no dots: pass --dot ID at least once")

    os.makedirs(base, exist_ok=True)   # only once the input is accepted
    for d in dots:
        if isinstance(d, dict) and isinstance(d.get("id"), str):
            os.makedirs(os.path.join(base, "live", d["id"]), exist_ok=True)
            os.makedirs(os.path.join(base, "memory", d["id"]), exist_ok=True)
    for p in projects:
        if isinstance(p, dict) and isinstance(p.get("id"), str):
            os.makedirs(os.path.join(base, p["id"], "mail"), exist_ok=True)

    path = os.path.join(base, "dots.json")
    data = json.dumps(cfg, ensure_ascii=False, indent=1) + "\n"
    current = None
    fh = open_read(path)   # O_NONBLOCK: a FIFO or a folder at dots.json returns here, not in open
    if fh is not None:
        try:
            with fh:
                current = fh.read()
        except (OSError, UnicodeDecodeError):
            current = None
    if current != data:
        write_atomic(path, data)
    print(f"INIT base={base}")
    if os.path.abspath(base) != os.path.abspath(default_base()):
        print(f"INIT export DOTS_OPS_BASE={base} (every other command resolves this folder)")
    for line in report:
        print(line)
    print(f"INIT config {path} ({'unchanged' if current == data else 'written'})")
    brief(base, dots, projects, cfg, os.path.abspath(__file__))

def tool_memory(dot, agent):
    """True when this agent already has the memory file `tick` would have written: v1, its own name,
    and the three text fields a first task requires. A file that merely exists is not enough — the
    folder is writable by the dot root, and a stub there must not swallow the rules banner."""
    mem = load_json(os.path.join(BASE, "memory", dot, f"{agent}.json"))
    return bool(mem and mem.get("v") == 1 and mem.get("agent") == agent
                and mem.get("task") and mem.get("goal") and mem.get("doing") and mem.get("next"))

def rules_lines(script):
    """The agent-side rules, printed once by init's brief and again by the first command an agent
    runs with no memory file. The dot root may never paste the brief, so the tool repeats them."""
    return [
        "AGENT_RULES dots-superkit — this is your first command, so the rules are printed once:",
        f"  every step: python3 {script} tick <DOT> <agentId> <start|step|blocked|done> --task T --host H "
        f"--note N --evidence E",
        f"  hand work back: python3 {script} reply <project> <agentId> <result|question|block> <task> --re ID "
        f"--ref R   (text on stdin); it stays open and prints the main session's answer, so do not end the turn",
        f"  no assigned work: python3 {script} wait <DOT> <agentId> --timeout 3300   (it marks what it prints read)",
        "  exit 3 or 4, from wait or reply: tick step, then run wait again; a reply is already written, "
        "so running reply again would write the result twice.",
        "  tick done is refused while a task, question or review in your inbox has no answer in out.jsonl.",
    ]

def announce_rules(a):
    """Before the first output of an agent that has no memory yet, print the rules it must follow.
    Read from the memory file, not from dots.json or the roster: the root may never pass a brief."""
    agent = getattr(a, "agent", None)
    if a.cmd not in ("tick", "reply", "wait") or not isinstance(agent, str) or not TOKEN.match(agent):
        return
    dot = getattr(a, "dot", None) or dot_for(agent)
    if not (isinstance(dot, str) and TOKEN.match(dot)):
        return
    if tool_memory(dot, agent):
        return
    for line in rules_lines(os.path.abspath(__file__)):
        print(line)
    print(flush=True)

def brief(base, dots, projects, cfg, script):
    names = [d["id"] for d in dots if isinstance(d, dict) and isinstance(d.get("id"), str)]
    main_by = {p["id"]: p.get("main") for p in projects if isinstance(p, dict) and isinstance(p.get("id"), str)}
    print()
    print("Paste this into each dot root's chat (fill the pin with the kit commit SHA):")
    print()
    print(f"Dots Superkit shared folder on local: {base}")
    print(f"Helper: python3 {script}")
    print(f"Dots: {', '.join(names)}")
    print("Projects: " + (", ".join(f"{k} (main {v})" for k, v in sorted(main_by.items())) or "none"))
    print("Rules for every agent (the tool also prints them on its first command):")
    for line in rules_lines(script):
        print(line)
    print("Main session: python3 " + script + " send <project> <agentId> <kind> <task>  (text on stdin)")
    print("Watchdog (the owner, not the dot root): python3 " + script + " watch --loop 5 --expect-sha256 "
          "<sha256 of dots.py> --wake-cmd '<argv JSON>' --notify-cmd '<argv JSON>' --after-min 10 "
          "--notify-after-min 20 --state ~/.dots-ops-watch.json")
    print("  Pass the commands on the watch command line, not only in dots.json: that file sits in the "
          "folder the dot root writes, the owner's own command line does not. Every run prints one "
          "WATCH_CONFIG line saying where each value came from.")
    print(f"Kit pin: dots-superkit/handoff-v1 @ <kit commit SHA>")
    if cfg.get("wake"):
        print("Wake command: configured (send and watch run it when no agent is listening)")
    else:
        print("Wake command: none (send prints the manual doorbell; watch prints WATCH_ERROR)")
    if cfg.get("notify"):
        print("Notify command: configured (watch runs it when a woken agent has still not answered)")
    else:
        print("Notify command: none (watch prints WATCH_ERROR when a notify is due and no command is set)")

class Piped:
    """A reader that stops early (`dots.py send ... | head -1`) must not raise and must not skip
    the rest of the command: a write to a closed pipe is dropped, and fd 1 moves to /dev/null so
    the flush at exit cannot fail either."""

    def __init__(self, stream):
        self._stream = stream

    def write(self, data):
        try:
            return self._stream.write(data)
        except BrokenPipeError:
            self._silence()
            return len(data)

    def flush(self):
        try:
            self._stream.flush()
        except BrokenPipeError:
            self._silence()

    def _silence(self):
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), self._stream.fileno())
        except (OSError, ValueError):
            pass

    def __getattr__(self, name):
        return getattr(self._stream, name)

def main(argv):
    sys.stdout = Piped(sys.stdout)
    p = argparse.ArgumentParser(prog="dots.py", description=__doc__.split("\n")[0],
                                epilog="Run `dots.py <command> -h` for one command; the folder is "
                                       "$DOTS_OPS_BASE or ~/.dots-ops.")
    sub = p.add_subparsers(dest="cmd", required=True, metavar="command")
    t = sub.add_parser("tick", help="one step: live line, memory, then unread mail first")
    t.add_argument("dot")
    t.add_argument("agent")
    t.add_argument("state", choices=STATES)
    for f in ("task", "host", "note", "evidence"):
        t.add_argument(f"--{f}", required=True)
    for f in ("role", "model", "project", "goal", "doing", "next", "took"):
        t.add_argument(f"--{f}")
    t.add_argument("--decision", action="append", default=[], help="a choice made on this task; repeatable")
    t.add_argument("--blocker", action="append", default=[], help="an open blocker; repeatable")
    t.add_argument("--unblock", action="store_true", help="drop the carried blockers")
    b = sub.add_parser("board", help="every agent: newest line, age, doing, next, mail, flags")
    b.add_argument("dots", nargs="*")
    b.add_argument("--json", action="store_true")
    m = sub.add_parser("memo", help="one agent's memory and its last three live lines")
    m.add_argument("dot")
    m.add_argument("agent")
    s = sub.add_parser("send", help="main session: write one message; text on stdin")
    s.add_argument("project")
    s.add_argument("agent")
    s.add_argument("kind", help=f"one of {', '.join(TO_AGENT)}")
    s.add_argument("task")
    s.add_argument("--re", help="id this answers, from in.jsonl or out.jsonl")
    s.add_argument("--ref", action="append", default=[], help="exact reference; repeatable")
    i = sub.add_parser("inbox", help="main session: agent replies not yet marked read")
    i.add_argument("project", nargs="?")
    i.add_argument("agent", nargs="?")
    r = sub.add_parser("read", help="main session: mark replies up to this id read")
    r.add_argument("project")
    r.add_argument("agent")
    r.add_argument("id")
    y = sub.add_parser("reply", help="agent: write one message back, then hold the answer open")
    y.add_argument("project")
    y.add_argument("agent")
    y.add_argument("kind", help=f"one of {', '.join(TO_MAIN)}")
    y.add_argument("task")
    y.add_argument("--re", help="id of the message this answers; omitted only for a result, question or "
                                "block that answers no mail line, which then needs --ref")
    y.add_argument("--ref", action="append", default=[], help="exact reference; repeatable")
    y.add_argument("--dot", help="dot holding the agent; default: the dot of its newest live line")
    y.add_argument("--timeout", type=int, default=3300,
                   help="seconds to wait for the answer (default 3300; exit 3 after this)")
    y.add_argument("--no-wait", action="store_true",
                   help="return at once instead of waiting for the answer")
    w = sub.add_parser("wait", help="agent: block until mail arrives; exits 0 with mail, 3 on timeout, 4 on a signal")
    w.add_argument("dot")
    w.add_argument("agent")
    w.add_argument("--timeout", type=int, default=3300,
                   help="seconds to block; exit 3 after this (default 3300)")
    n = sub.add_parser("init", help="once per machine: config, folder tree, brief for each dot root")
    n.add_argument("--base", help="shared folder; default $DOTS_OPS_BASE or ~/.dots-ops")
    n.add_argument("--dot", action="append", default=[], help="dot id; repeatable")
    n.add_argument("--project", action="append", default=[], help="ID=MAIN session id; repeatable")
    n.add_argument("--host", action="append", default=[], help="LABEL=PURPOSE beyond vm and local; repeatable")
    n.add_argument("--wake-cmd", help='argv list as JSON, e.g. \'["ssh","box","wake","{agent}"]\'')
    n.add_argument("--notify-cmd", help='argv list as JSON for the watchdog\'s notify step, same placeholders '
                                        'plus {age}, e.g. \'["ssh","box","notify","{id}"]\'')
    n.add_argument("--yes", action="store_true", help="no questions; flags only")
    c = sub.add_parser("watch", help="owner side: wake, then notify, mail that nobody answered")
    c.add_argument("--loop", type=float, default=0.0, help="minutes between runs; 0 runs once (default 0)")
    c.add_argument("--expect-sha256", dest="expect_sha256",
                   help="sha256 of this file; a different file prints WATCH_TOOL_CHANGED")
    c.add_argument("--wake-cmd", dest="wake_cmd",
                   help='argv list as JSON; wins over dots.json (which the dot root can write), '
                        'e.g. \'["ssh","box","wake","{agent}"]\'')
    c.add_argument("--notify-cmd", dest="notify_cmd",
                   help='argv list as JSON for the notify step, same placeholders plus {age}; wins over dots.json')
    c.add_argument("--after-min", dest="after_min", type=int,
                   help="minutes a message waits unanswered before the wake (wins over dots.json; default 10)")
    c.add_argument("--notify-after-min", dest="notify_after_min", type=int,
                   help="minutes after the wake before the notify (wins over dots.json; default 20)")
    c.add_argument("--state", help="ledger path; keep it outside the shared folder (default <base>/watch/state.json)")
    a = p.parse_args(argv)
    announce_rules(a)
    code = {"tick": tick, "board": board, "memo": memo, "send": send, "inbox": inbox, "read": read,
            "reply": reply, "wait": wait, "init": init, "watch": watch}[a.cmd](a)
    sys.exit(code or 0)

if __name__ == "__main__":
    main(sys.argv[1:])
