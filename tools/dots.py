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

Main session outside Dots:
  dots.py send <project> <agent> <kind> <task> [--re ID] [--ref R]...  text on stdin
      Prints the id, then LISTENING or NOT_LISTENING last_tick=<age>; when not listening it runs
      the configured wake command once (or prints the manual doorbell).
  dots.py inbox [project] [agent]     agent replies not yet marked read
  dots.py read <project> <agent> <id> mark replies up to that id read

Readers (read-only):
  dots.py board [DOT...] [--json]   each agent: newest line, age, doing, next, blockers, mail, flags
  dots.py memo <DOT> <agent>        that agent's memory and its last 3 live lines

Setup (once per machine; idempotent, adds only):
  dots.py init [--base DIR] [--dot ID]... [--project ID=MAIN]... [--host LABEL=PURPOSE]...
      [--wake-cmd ARGV-JSON] [--yes]
      Interactive when stdin is a terminal, scriptable with the flags otherwise. Writes
      <base>/dots.json, live/<DOT>/, memory/<DOT>/ and <project>/mail/, then prints the brief
      to paste to each dot root. Never deletes or rewrites an existing record or entry.

Base folder: $DOTS_OPS_BASE, else ~/.dots-ops; `init` resolves it the same way, so an instance
that already keeps its folder elsewhere points that variable at it. `init --base DIR` writes
another one for this machine and prints the export line to set, because later commands still
read the environment first. Every command works without dots.json: the dot and the project are
read from the folders that exist.
"""
import argparse, contextlib, datetime, fcntl, glob, json, os, re, secrets, signal, socket, subprocess, sys, time

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
STALL_MIN = 30
MAX_TEXT = 500      # goal, doing, next
MAX_ITEM = 300      # one decision or blocker
MAX_DECISIONS = 20  # newest kept; the full history is the live log
MAX_BLOCKERS = 10
POLL_S = 1.0        # wait's mailbox poll; a message lands within about a second
WAKE_TIMEOUT = 25   # one wake command, once, never retried; under the connector's 30 s exec yield
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

def jsonl(path):
    out = []
    try:
        with open(path, encoding="utf-8") as fh:
            for ln in fh:
                try:
                    obj = json.loads(ln)
                except ValueError:
                    continue
                if isinstance(obj, dict):
                    out.append(obj)
    except FileNotFoundError:
        pass
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
    try:
        with open(path, encoding="utf-8") as fh:
            obj = json.load(fh)
        return obj if isinstance(obj, dict) else None
    except (FileNotFoundError, ValueError):
        return None

def marker(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read().strip() or None
    except FileNotFoundError:
        return None

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

def mail_state(box):
    inbox = jsonl(os.path.join(box, "in.jsonl"))
    outbox = jsonl(os.path.join(box, "out.jsonl"))
    mark = marker(os.path.join(box, ".agent-read"))
    new = after(inbox, mark)
    taken = inbox[:len(inbox) - len(new)]
    answered = {m.get("re") for m in outbox}
    return {"new": new, "unanswered": [m for m in taken if m.get("id") not in answered],
            "unread_out": after(outbox, marker(os.path.join(box, ".main-read")))}

def unread_upto(box, took):
    """Unread messages in one box, minus the ones this tick is marking read with --took."""
    msgs = mail_state(box)["new"]
    ids = [m.get("id") for m in msgs]
    return msgs[len(ids) - ids[::-1].index(took):] if took in ids else msgs

def unread_all(agent):
    """(box, project, message) for every unread line in every mailbox of that agent id."""
    return [(box, project_of(box), m) for box in mailboxes(agent) for m in mail_state(box)["new"]]

def write_atomic(path, data):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

def append_line(path, obj):
    data = (json.dumps(obj, ensure_ascii=False) + "\n").encode()
    with open(path, "ab") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())

def append_unique(path, msg):
    """Append one mail line, drawing another id if that mailbox already used this one.
    The check runs under the same lock as the write, so two senders cannot pick the same id."""
    with open(path, "ab") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        seen = {m.get("id") for m in jsonl(path)}
        while msg["id"] in seen:
            msg["id"] = new_id()
        fh.write((json.dumps(msg, ensure_ascii=False) + "\n").encode())
        fh.flush()
        os.fsync(fh.fileno())

def show_mail(project, m, tag):
    re_ = f" re={m['re']}" if m.get("re") else ""
    refs = f" refs={','.join(m['refs'])}" if m.get("refs") else ""
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
            mail = {"new": 0, "unanswered": 0, "unread_out": 0}
            boxes = mailboxes(agent)
            for box in boxes:
                st = mail_state(box)
                for k in mail:
                    mail[k] += len(st[k])
            if mail["unanswered"]:
                flags.append("MAIL-UNANSWERED")
            rows.append({"dot": dot, "agent": agent, "roster": roster.get(agent),
                         "state": last["state"] if last else None, "t": last["t"] if last else None,
                         "age_min": None if age is None else round(age), "task": last["task"] if last else None,
                         "host": last["host"] if last else None,
                         "doing": (mem or {}).get("doing"), "next": (mem or {}).get("next"),
                         "blockers": (mem or {}).get("blockers") or [], "mail": mail if boxes else None,
                         "flags": flags})
    return rows, errors

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
        print(f"{r['dot']} {r['agent']}  {r['state'] or '-'} {age} host={r['host'] or '-'} roster={r['roster'] or '-'}"
              f"  {clip(r['task'], 60)}{flags}")
        if r["doing"] or r["next"]:
            print(f"    doing: {clip(r['doing'])}")
            print(f"    next:  {clip(r['next'])}")
        for b in r["blockers"][:3]:
            print(f"    block: {clip(b)}")
        if r["mail"]:
            m = r["mail"]
            print(f"    mail:  to agent {m['new']} unread, {m['unanswered']} unanswered; from agent {m['unread_out']} unread")

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
    """Every `wait` marker for that agent: one file per waiting process, so two waiters never collide."""
    return sorted(glob.glob(os.path.join(BASE, "listen", glob.escape(agent) + ".*.json")))

def marker_live(m):
    """A marker is not listening when its pid is invalid, its process is gone or its `until` has passed.
    A marker written on another host is judged by `until` alone: a pid there means nothing here."""
    pid = m.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return False
    until = parse_t(m.get("until"))
    if until is not None and until < datetime.datetime.now(datetime.timezone.utc):
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
    """Remove markers of waiters that are gone; never one that is still listening."""
    for path in marker_paths(agent):
        m = load_json(path)
        if m is None or not marker_live(m):
            try:
                os.unlink(path)
            except FileNotFoundError:
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
    known = {m.get("id") for m in jsonl(os.path.join(box, "in.jsonl"))}
    known |= {m.get("id") for m in jsonl(os.path.join(box, "out.jsonl"))}
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

def wake_argv(cfg, project, agent, msg_id, dot):
    argv = cfg.get("wake")
    if not (isinstance(argv, list) and argv and all(isinstance(x, str) and x for x in argv)):
        return None, None
    values = {"project": project, "agent": agent, "id": msg_id, "dot": dot or ""}
    used = set(re.findall(r"\{(project|agent|id|dot)\}", " ".join(argv)))
    for name in sorted(used):
        if name == "dot" and not dot:
            return None, "dot"
    def sub(m):
        return values[m.group(1)]
    return [re.sub(r"\{(project|agent|id|dot)\}", sub, x) for x in argv], None

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
    if not a.re:
        die("reply needs --re with the id of the message it answers")
    re_id = token("re", a.re)
    body = stdin_body()
    box = mail_box(project, agent)
    if re_id not in [m.get("id") for m in jsonl(os.path.join(box, "in.jsonl"))]:
        die(f"--re {re_id} is not in {project}/mail/{agent}/in.jsonl")
    msg = mail_line(project, agent, a.kind, task, re_id, refs, body, agent)
    append_unique(os.path.join(box, "out.jsonl"), msg)
    print(msg["id"])

# ---------------------------------------------------------------- wait

def wait(a):
    dot, agent = token("DOT", a.dot), token("agent", a.agent)
    timeout = a.timeout
    if timeout <= 0:
        die("--timeout must be a positive number of seconds")
    pending = unread_all(agent)
    if pending:
        print_pending(pending)
        mark_delivered(pending)
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
            pending = unread_all(agent)
            if pending:
                print_pending(pending)
                mark_delivered(pending)
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

def mark_delivered(pending):
    """Move each box's read marker to the newest message `wait` printed, so the next wait blocks
    instead of printing it again. `tick --took` moves the same marker for a working agent."""
    for box, _project, m in pending:
        mid = m.get("id")
        if isinstance(mid, str) and TOKEN.match(mid):
            write_atomic(os.path.join(box, ".agent-read"), mid + "\n")

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

    if wake_spec:
        try:
            argv = json.loads(wake_spec)
        except ValueError:
            die("--wake-cmd must be a JSON list of strings, for example '[\"ssh\",\"box\",\"wake\",\"{agent}\"]'")
        if not (isinstance(argv, list) and 0 < len(argv) <= 32 and all(isinstance(x, str) and x.strip() for x in argv)):
            die("--wake-cmd must be a JSON list of 1-32 non-empty strings")
        if any("\n" in x or "\r" in x for x in argv):
            die("--wake-cmd entries must be one line each")
        if cfg.get("wake") and cfg["wake"] != argv:
            report.append("INIT wake (replaced)")
        elif not cfg.get("wake"):
            report.append("INIT wake (set)")
        else:
            report.append("INIT wake (kept)")
        cfg["wake"] = argv
    elif cfg.get("wake"):
        report.append("INIT wake (kept)")

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
    try:
        current = open(path, encoding="utf-8").read()
    except FileNotFoundError:
        pass
    if current != data:
        write_atomic(path, data)
    print(f"INIT base={base}")
    if os.path.abspath(base) != os.path.abspath(default_base()):
        print(f"INIT export DOTS_OPS_BASE={base} (every other command resolves this folder)")
    for line in report:
        print(line)
    print(f"INIT config {path} ({'unchanged' if current == data else 'written'})")
    brief(base, dots, projects, cfg, os.path.abspath(__file__))

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
    print("Record every step: python3 " + script + " tick <DOT> <agentId> <state> --task T --host H --note N --evidence E")
    print("No assigned work: do not end the turn. python3 " + script + " wait <DOT> <agentId> --timeout 3300; "
          "it marks what it prints read, so the next wait blocks. On exit 3 tick step, then wait again; "
          "exit 4 means it was interrupted, so wait again.")
    print("Main session: python3 " + script + " send <project> <agentId> <kind> <task>  (text on stdin)")
    print(f"Kit pin: dots-superkit/handoff-v1 @ <kit commit SHA>")
    if cfg.get("wake"):
        print("Wake command: configured (send runs it when no agent is listening)")
    else:
        print("Wake command: none (send prints the manual doorbell)")

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
    y = sub.add_parser("reply", help="agent: write one message back; text on stdin")
    y.add_argument("project")
    y.add_argument("agent")
    y.add_argument("kind", help=f"one of {', '.join(TO_MAIN)}")
    y.add_argument("task")
    y.add_argument("--re", required=True, help="id of the message this answers")
    y.add_argument("--ref", action="append", default=[], help="exact reference; repeatable")
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
    n.add_argument("--yes", action="store_true", help="no questions; flags only")
    a = p.parse_args(argv)
    code = {"tick": tick, "board": board, "memo": memo, "send": send, "inbox": inbox,
            "read": read, "reply": reply, "wait": wait, "init": init}[a.cmd](a)
    sys.exit(code or 0)

if __name__ == "__main__":
    main(sys.argv[1:])
