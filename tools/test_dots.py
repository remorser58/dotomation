#!/usr/bin/env python3
"""Tests for dots.py: steering (wait), mail, init, and the tick/board regressions.

Run with: cd tools && python3 -m unittest test_dots -q
No network and no ssh: every case uses a temporary base folder and a bounded timeout.
"""

import glob
import hashlib
import io
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.dont_write_bytecode = True  # leave no __pycache__ next to the tool
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import dots as d  # noqa: E402

SCRIPT = os.path.join(HERE, "dots.py")
ID_RE = re.compile(r"^m-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{4}$")
MAIL_KEYS = {"v", "id", "t", "from", "to", "task", "kind", "re", "text", "refs"}
LIVE_KEYS = {"t", "agent", "role", "model", "project", "task", "state", "host", "note", "evidence"}
MEMORY_KEYS = {"v", "t", "dot", "agent", "project", "task", "state", "host", "goal", "doing", "next",
               "decisions", "blockers"}
TICK = ("--task", "issue 1", "--host", "local", "--note", "note", "--evidence", "rev/x-20261123")
FIRST = ("--role", "구현", "--model", "requested gpt-6.2-astra xhigh; runtime does not display effective",
         "--project", "proj", "--goal", "goal", "--doing", "doing", "--next", "next")


def run(args, base, stdin=None, timeout=20):
    env = dict(os.environ, DOTS_OPS_BASE=base, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run([sys.executable, SCRIPT] + list(args), input=stdin, env=env,
                          capture_output=True, text=True, timeout=timeout)


def spawn(args, base):
    env = dict(os.environ, DOTS_OPS_BASE=base, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.Popen([sys.executable, SCRIPT] + list(args), env=env, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for(target, seconds=5.0):
    """True once the path exists, or once the callable returns true."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if target() if callable(target) else os.path.exists(target):
            return True
        time.sleep(0.02)
    return False


def marker_files(base, agent="agent-1"):
    """Every `wait` marker for that agent: one file per waiting process."""
    folder = os.path.join(base, "listen")
    return sorted(glob.glob(os.path.join(folder, f"{agent}.*.json"))) if os.path.isdir(folder) else []


def lines(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return [json.loads(ln) for ln in fh if ln.strip()]
    except FileNotFoundError:
        return []


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class Base(unittest.TestCase):
    """A temporary shared folder patched into the module, so in-process calls use it too."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self._saved = d.BASE
        d.BASE = self.base
        self.procs = []
        self.make_dot("DOT-A")
        self.make_project("proj", "proj-main")

    def tearDown(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.wait(timeout=5)
            for stream in (p.stdout, p.stderr):
                if stream:
                    stream.close()
        d.BASE = self._saved
        self._tmp.cleanup()

    def make_dot(self, dot):
        os.makedirs(os.path.join(self.base, "live", dot), exist_ok=True)
        os.makedirs(os.path.join(self.base, "memory", dot), exist_ok=True)

    def make_project(self, project, main):
        os.makedirs(os.path.join(self.base, project, "mail"), exist_ok=True)
        cfg = d.load_json(os.path.join(self.base, "dots.json")) or {
            "v": 1, "dots": [], "projects": [], "hosts": []}
        if project not in [p.get("id") for p in cfg["projects"]]:
            cfg["projects"].append({"id": project, "main": main})
        d.write_atomic(os.path.join(self.base, "dots.json"), json.dumps(cfg, indent=1) + "\n")

    def send(self, agent="agent-1", kind="task", task="issue 1", text="do it", project="proj", refs=()):
        args = ["send", project, agent, kind, task] + [x for r in refs for x in ("--ref", r)]
        res = run(args, self.base, stdin=text)
        self.assertEqual(res.returncode, 0, res.stderr)
        return res.stdout.splitlines()[0]

    def tick(self, *extra, agent="agent-1", dot="DOT-A", state="start"):
        """A tick with the first-tick fields; every value is valid unless a case overrides it."""
        args = ["tick", dot, agent, state] + list(TICK) + list(FIRST) + list(extra)
        return run(args, self.base)

    def start_wait(self, agent="agent-1", dot="DOT-A", timeout=30):
        p = spawn(["wait", dot, agent, "--timeout", str(timeout)], self.base)
        self.procs.append(p)
        self.assertTrue(wait_for(lambda: marker_files(self.base, agent), 5.0),
                        "wait never wrote its listening marker")
        return p

    def take(self, mid, agent="agent-1"):
        """Mark everything up to mid read, the way an agent's next tick does."""
        res = self.tick("--took", mid, agent=agent, state="step")
        self.assertEqual(res.returncode, 0, res.stderr)
        return res


class WaitTests(Base):
    def test_send_wakes_a_blocking_wait_under_three_seconds(self):
        p = self.start_wait()
        started = time.monotonic()
        mid = self.send(text="steer now")
        out, err = p.communicate(timeout=10)
        elapsed = time.monotonic() - started
        self.assertEqual(p.returncode, 0, err)
        self.assertLess(elapsed, 3.0, out)
        self.assertIn(mid, out)
        self.assertIn("MAIL_NEW", out)
        self.assertIn("WAIT mail=1", out)
        self.assertEqual(marker_files(self.base), [])

    def test_wait_returns_at_once_on_unread_mail(self):
        mid = self.send(text="already here")
        started = time.monotonic()
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "30"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertIn(mid, res.stdout)
        self.assertNotIn("WAIT listening", res.stdout)
        self.assertEqual(marker_files(self.base), [])

    def test_delivered_mail_is_marked_read_so_the_next_wait_blocks(self):
        mid = self.send(text="steer once")
        self.assertEqual(run(["wait", "DOT-A", "agent-1", "--timeout", "30"], self.base).returncode, 0)
        self.assertEqual(d.marker(os.path.join(self.base, "proj", "mail", "agent-1", ".agent-read")), mid)
        # the documented loop is wait -> act -> wait; the same line must not come back
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "2"], self.base)
        self.assertEqual(res.returncode, 3, res.stdout)
        self.assertNotIn(mid, res.stdout)
        self.assertIn("WAIT timeout", res.stdout)

    def test_wait_times_out_with_exit_three_and_no_marker(self):
        started = time.monotonic()
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "1"], self.base)
        self.assertEqual(res.returncode, 3, res.stdout + res.stderr)
        self.assertLess(time.monotonic() - started, 10)
        tail = res.stdout.strip().splitlines()[-1]
        self.assertTrue(tail.startswith("WAIT timeout"), tail)
        self.assertIn("unread=0", tail)
        self.assertEqual(marker_files(self.base), [])

    def test_sigterm_removes_the_marker_and_exits_four(self):
        p = self.start_wait()
        p.send_signal(signal.SIGTERM)
        p.communicate(timeout=10)
        self.assertEqual(p.returncode, 4)  # not 0: no mail was delivered
        self.assertEqual(marker_files(self.base), [])

    def test_marker_records_pid_dot_until_and_host(self):
        p = self.start_wait()
        marker = d.load_json(marker_files(self.base)[0])
        self.assertEqual(marker["pid"], p.pid)
        self.assertEqual(marker["dot"], "DOT-A")
        self.assertIsNotNone(d.parse_t(marker["until"]))
        self.assertTrue(marker["host"])
        p.send_signal(signal.SIGTERM)
        p.communicate(timeout=10)

    def test_two_waiters_keep_their_own_marker(self):
        first = self.start_wait()
        second = self.start_wait()
        self.assertTrue(wait_for(lambda: len(marker_files(self.base)) == 2, 5.0), marker_files(self.base))
        second.send_signal(signal.SIGTERM)
        second.communicate(timeout=10)
        # the survivor still owns a marker, so send does not fall through to the wake command
        self.assertEqual(len(marker_files(self.base)), 1)
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="two")
        self.assertIn("LISTENING", res.stdout)
        first.communicate(timeout=10)
        self.assertEqual(marker_files(self.base), [])

    def test_send_reports_listening_then_not_listening_after_a_stale_marker(self):
        p = self.start_wait()
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="one")
        self.assertIn("LISTENING", res.stdout)
        self.assertIn("pid=", res.stdout)
        p.communicate(timeout=10)
        self.take(res.stdout.splitlines()[0])
        # a marker of this host whose pid is gone counts as not listening
        d.write_atomic(os.path.join(self.base, "listen", "agent-1.999999.json"),
                       json.dumps({"pid": 999999, "dot": "DOT-A", "since": d.now(),
                                   "until": d.iso_after(600), "host": socket.gethostname()}) + "\n")
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="two")
        self.assertIn("NOT_LISTENING", res.stdout)
        self.assertIn("last_tick=", res.stdout)
        self.assertIn("DOORBELL", res.stdout)
        self.assertEqual(marker_files(self.base), [])  # send pruned the dead marker

    def test_a_marker_from_another_host_is_judged_by_until_not_by_its_pid(self):
        os.makedirs(os.path.join(self.base, "listen"), exist_ok=True)
        path = os.path.join(self.base, "listen", "agent-1.999999.json")
        # a foreign pid says nothing here: while `until` is ahead, that agent is listening
        d.write_atomic(path, json.dumps({"pid": 999999, "dot": "DOT-A", "since": d.now(),
                                         "until": d.iso_after(600), "host": "the-other-vm"}) + "\n")
        self.assertIsNotNone(d.listen_state("agent-1"))
        # ... even when the number happens to be a live pid on this machine
        d.write_atomic(path, json.dumps({"pid": os.getpid(), "dot": "DOT-A", "since": d.now(),
                                         "until": d.iso_after(600), "host": "the-other-vm"}) + "\n")
        self.assertIsNotNone(d.listen_state("agent-1"))
        # a foreign marker whose `until` has passed is not listening
        d.write_atomic(path, json.dumps({"pid": os.getpid(), "dot": "DOT-A", "since": "2020-01-01T00:00:00Z",
                                         "until": "2020-01-01T00:00:01Z", "host": "the-other-vm"}) + "\n")
        self.assertIsNone(d.listen_state("agent-1"))

    def test_a_past_until_counts_as_not_listening(self):
        os.makedirs(os.path.join(self.base, "listen"), exist_ok=True)
        d.write_atomic(os.path.join(self.base, "listen", "agent-1.1234.json"),
                       json.dumps({"pid": os.getpid(), "dot": "DOT-A", "since": "2020-01-01T00:00:00Z",
                                   "until": "2020-01-01T00:00:01Z", "host": socket.gethostname()}) + "\n")
        self.assertIsNone(d.listen_state("agent-1"))

    def test_a_dotted_agent_id_does_not_count_as_a_listener_for_its_prefix(self):
        os.makedirs(os.path.join(self.base, "listen"), exist_ok=True)
        d.write_atomic(os.path.join(self.base, "listen", "agent-1.x.{}.json".format(os.getpid())),
                       json.dumps({"pid": os.getpid(), "dot": "DOT-A", "since": d.now(),
                                   "until": d.iso_after(600), "host": socket.gethostname()}) + "\n")
        self.assertIsNone(d.listen_state("agent-1"))
        self.assertIsNotNone(d.listen_state("agent-1.x"))
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="one")
        self.assertIn("NOT_LISTENING", res.stdout)
        # agent-1's send never prunes agent-1.x's live marker
        self.assertEqual(len(marker_files(self.base)), 1)

    def test_bad_ids_are_refused(self):
        self.assertEqual(run(["wait", "DOT-A", "bad id", "--timeout", "1"], self.base).returncode, 2)
        self.assertEqual(run(["wait", "bad dot", "agent-1", "--timeout", "1"], self.base).returncode, 2)
        self.assertEqual(run(["wait", "DOT-A", "agent-1", "--timeout", "0"], self.base).returncode, 2)

    def test_a_folder_where_the_read_marker_belongs_does_not_kill_wait(self):
        # .agent-read sits in the mailbox the root writes: a folder there used to end the wait with a
        # traceback, which for an agent holding the command open is the same as ending its turn
        mid = self.send(text="handle this")
        box = os.path.join(self.base, "proj", "mail", "agent-1")
        os.makedirs(os.path.join(box, ".agent-read"))
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "3"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"MAIL_NEW proj {mid}", res.stdout)
        self.assertIn("it stays unread", res.stderr)      # said once, on stderr, not raised
        self.assertNotIn("Traceback", res.stderr)

class WakeTests(Base):
    def test_wake_argv_runs_once_with_placeholders(self):
        target = os.path.join(self.base, "wake.txt")
        argv = [sys.executable, "-c",
                "import sys, pathlib; pathlib.Path(sys.argv[1]).write_text(' '.join(sys.argv[2:]))",
                target, "{project}", "{agent}", "{id}", "{dot}"]
        cfg = d.load_json(os.path.join(self.base, "dots.json"))
        cfg["dots"] = [{"id": "DOT-A"}]
        cfg["wake"] = argv
        d.write_atomic(os.path.join(self.base, "dots.json"), json.dumps(cfg, indent=1) + "\n")
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="wake the agent")
        mid = res.stdout.splitlines()[0]
        self.assertIn("NOT_LISTENING", res.stdout)
        self.assertIn("WAKE ran", res.stdout)
        self.assertIn("WAKE exit=0", res.stdout)
        self.assertEqual(read(target).split(), ["proj", "agent-1", mid, "DOT-A"])

    def test_wake_is_not_run_while_the_agent_is_listening(self):
        target = os.path.join(self.base, "wake.txt")
        cfg = d.load_json(os.path.join(self.base, "dots.json"))
        cfg["dots"] = [{"id": "DOT-A"}]
        cfg["wake"] = [sys.executable, "-c", "open(%r, 'a').write('x')" % target]
        d.write_atomic(os.path.join(self.base, "dots.json"), json.dumps(cfg, indent=1) + "\n")
        p = self.start_wait()
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="go")
        self.assertIn("LISTENING", res.stdout)
        self.assertNotIn("WAKE", res.stdout)
        p.communicate(timeout=10)
        self.assertFalse(os.path.exists(target))

    def test_wake_without_a_resolvable_dot_is_not_run(self):
        cfg = d.load_json(os.path.join(self.base, "dots.json"))
        cfg["dots"] = [{"id": "DOT-A"}, {"id": "DOT-B"}]
        cfg["wake"] = [sys.executable, "-c", "raise SystemExit(9)", "{dot}"]
        d.write_atomic(os.path.join(self.base, "dots.json"), json.dumps(cfg, indent=1) + "\n")
        self.make_dot("DOT-B")
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="go")
        self.assertIn("WAKE not_run missing=dot", res.stdout)
        self.assertIn("DOORBELL", res.stdout)

    def test_a_closed_reader_does_not_skip_the_wake_command(self):
        target = os.path.join(self.base, "wake.txt")
        cfg = d.load_json(os.path.join(self.base, "dots.json"))
        cfg["dots"] = [{"id": "DOT-A"}]
        cfg["wake"] = [sys.executable, "-c",
                       "import sys, time, pathlib; time.sleep(0.3); pathlib.Path(sys.argv[1]).write_text('ran')",
                       target]
        d.write_atomic(os.path.join(self.base, "dots.json"), json.dumps(cfg, indent=1) + "\n")
        p = subprocess.Popen([sys.executable, SCRIPT, "send", "proj", "agent-1", "task", "issue 1"],
                             env=dict(os.environ, DOTS_OPS_BASE=self.base, PYTHONDONTWRITEBYTECODE="1"),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.procs.append(p)
        p.stdin.write("go")
        p.stdin.close()
        self.assertRegex(p.stdout.readline().strip(), ID_RE)  # the id only; then the reader goes away
        p.stdout.close()
        err = p.stderr.read()
        p.wait(timeout=20)
        self.assertEqual(p.returncode, 0, err)
        self.assertNotIn("Traceback", err)
        self.assertTrue(os.path.exists(target))


class TickTests(Base):
    def test_unread_mail_is_printed_before_the_live_line(self):
        mid = self.send(text="steer mid-task")
        res = self.tick()
        self.assertEqual(res.returncode, 0, res.stderr)
        out = res.stdout
        self.assertIn(f"MAIL_FIRST 1 unread", out)
        self.assertIn(mid, out)
        self.assertLess(out.index("MAIL_NEW"), out.index("LIVE "))
        self.assertIn("handle these before your next step", out)

    def test_took_moves_the_marker_and_is_not_repeated_as_new(self):
        first = self.send(text="one")
        second = self.send(text="two")
        res = self.tick("--took", first)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(second, res.stdout)
        self.assertNotIn(f"MAIL_NEW proj {first}", res.stdout)
        self.assertEqual(d.marker(os.path.join(self.base, "proj", "mail", "agent-1", ".agent-read")), first)

    def test_refused_tick_writes_nothing(self):
        live = os.path.join(self.base, "live", "DOT-A", "agent-1.jsonl")
        res = run(["tick", "DOT-A", "agent-1", "start"] + list(TICK) +
                  ["--role", "구현", "--model", "m", "--project", "proj"], self.base)  # no --goal/--doing/--next
        self.assertEqual(res.returncode, 2)
        self.assertIn("--goal", res.stderr)
        self.assertFalse(os.path.exists(live))
        self.assertFalse(os.path.exists(os.path.join(self.base, "memory", "DOT-A", "agent-1.json")))
        res = run(["tick", "DOT-A", "agent-1", "blocked"] + list(TICK) + list(FIRST), self.base)
        self.assertEqual(res.returncode, 2)
        self.assertIn("blocker", res.stderr)
        self.assertFalse(os.path.exists(live))

    def test_concurrent_ticks_keep_every_decision(self):
        self.assertEqual(self.tick().returncode, 0)
        procs = [spawn(["tick", "DOT-A", "agent-1", "step"] + list(TICK) + ["--decision", f"d{i}"], self.base)
                 for i in range(5)]
        for p in procs:
            p.communicate(timeout=30)
            self.assertEqual(p.returncode, 0)
        mem = d.load_json(os.path.join(self.base, "memory", "DOT-A", "agent-1.json"))
        # the lock serialises the ticks; which one wins the race does not matter, losing one does
        self.assertEqual(sorted(x["text"] for x in mem["decisions"]), ["d0", "d1", "d2", "d3", "d4"])
        self.assertEqual(len(lines(os.path.join(self.base, "live", "DOT-A", "agent-1.jsonl"))), 6)

    def test_board_reports_roster_slot_state_and_skips_bad_ids(self):
        self.assertEqual(self.tick().returncode, 0)
        roster = {"slots": [{"agent": "agent-1", "slot_state": "working"}, {"agent": "bad id!", "state": "x"}]}
        d.write_atomic(os.path.join(self.base, "live", "DOT-A", "roster.json"),
                       json.dumps(roster) + "\n")
        res = run(["board", "--json"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        row = next(r for r in data["agents"] if r["agent"] == "agent-1")
        self.assertEqual(row["roster"], "working")
        self.assertTrue(any("without a valid agent id" in e["error"] for e in data["errors"]))
        text = run(["board"], self.base).stdout
        self.assertIn("roster=working", text)


class MailTests(Base):
    def test_reply_inbox_read_round_trip(self):
        mid = self.send(text="question?")
        self.assertEqual(self.tick().returncode, 0)
        res = run(["reply", "proj", "agent-1", "ack", "issue 1", "--re", mid], self.base, stdin="taken")
        self.assertEqual(res.returncode, 0, res.stderr)
        reply_id = res.stdout.strip()
        self.assertRegex(reply_id, ID_RE)
        self.assertIn(reply_id, run(["inbox"], self.base).stdout)
        self.assertIn(reply_id, run(["inbox", "proj", "agent-1"], self.base).stdout)
        self.assertEqual(run(["read", "proj", "agent-1", reply_id], self.base).returncode, 0)
        self.assertIn("MAIL none", run(["inbox"], self.base).stdout)
        self.assertEqual(d.marker(os.path.join(self.base, "proj", "mail", "agent-1", ".main-read")), reply_id)

    def test_reply_needs_a_message_it_answers(self):
        self.assertEqual(run(["reply", "proj", "agent-1", "ack", "issue 1", "--re",
                              "m-20260101T000000Z-0000"], self.base, stdin="x").returncode, 2)
        self.assertEqual(run(["reply", "proj", "agent-1", "ack", "issue 1"], self.base, stdin="x").returncode, 2)
        self.assertEqual(run(["read", "proj", "agent-1", "m-20260101T000000Z-0000"], self.base).returncode, 2)

    def test_written_lines_are_mail_v1_and_marker_compatible(self):
        mid = self.send(text="first line\nsecond line", refs=("rev/x-20261123", "gate:G1"))
        self.assertEqual(self.tick("--took", mid).returncode, 0)
        run(["reply", "proj", "agent-1", "ack", "issue 1", "--re", mid], self.base, stdin="ok")
        inbox = lines(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"))
        outbox = lines(os.path.join(self.base, "proj", "mail", "agent-1", "out.jsonl"))
        for msg in inbox + outbox:
            self.assertEqual(set(msg), MAIL_KEYS)
            self.assertEqual(msg["v"], 1)
            self.assertRegex(msg["id"], ID_RE)
            self.assertIsNotNone(d.parse_t(msg["t"]))
        self.assertEqual(inbox[0]["from"], "proj-main")
        self.assertEqual(inbox[0]["to"], "agent-1")
        self.assertIsNone(inbox[0]["re"])
        self.assertEqual(inbox[0]["refs"], ["rev/x-20261123", "gate:G1"])
        self.assertEqual(inbox[0]["text"], "first line\nsecond line")
        self.assertEqual(outbox[0]["re"], mid)
        self.assertEqual(outbox[0]["from"], "agent-1")
        # the same unread rule mail.py uses: the lines after the marker, in file order
        marker = d.marker(os.path.join(self.base, "proj", "mail", "agent-1", ".agent-read"))
        self.assertEqual(marker, mid)
        remaining = d.after(inbox, marker)
        self.assertEqual(remaining, [])

    def test_kind_and_text_are_checked(self):
        self.assertEqual(run(["send", "proj", "agent-1", "result", "issue 1"], self.base, stdin="x").returncode, 2)
        self.assertEqual(run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="  \n").returncode, 2)
        self.assertEqual(run(["reply", "proj", "agent-1", "task", "issue 1", "--re",
                              "m-20260101T000000Z-0000"], self.base, stdin="x").returncode, 2)
        leak = run(["send", "proj", "agent-1", "task", "issue 1"], self.base,
                   stdin="token sk-" + "a" * 24)
        self.assertEqual(leak.returncode, 2)
    def test_send_answers_an_agent_result_id(self):
        mid = self.send(text="do the fix")
        self.assertEqual(self.tick("--took", mid).returncode, 0)
        res = run(["reply", "proj", "agent-1", "result", "issue 1", "--re", mid, "--no-wait"], self.base,
                  stdin="done, rev/x-20261123")
        result_id = res.stdout.splitlines()[0]
        self.assertRegex(result_id, ID_RE)
        # the review and answer legs of direct mail answer the agent's own result, in out.jsonl
        review = run(["send", "proj", "agent-1", "review", "issue 1", "--re", result_id,
                      "--ref", "gate:G1 FAIL"], self.base, stdin="G1 FAIL: see the log")
        self.assertEqual(review.returncode, 0, review.stderr)
        answer = run(["send", "proj", "agent-1", "answer", "issue 1", "--re", result_id], self.base,
                     stdin="all gates PASS")
        self.assertEqual(answer.returncode, 0, answer.stderr)
        inbox = lines(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"))
        self.assertEqual([m["re"] for m in inbox[1:]], [result_id, result_id])
        # an id in neither file is still refused, and nothing is written
        bad = run(["send", "proj", "agent-1", "review", "issue 1", "--re", "m-20260101T000000Z-0000"],
                  self.base, stdin="x")
        self.assertEqual(bad.returncode, 2)
        self.assertEqual(len(lines(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"))), len(inbox))

    def test_ids_stay_unique_inside_one_mailbox(self):
        path = os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fixed = "m-20261123T034500Z-1a2b"
        real = d.new_id
        d.new_id = lambda: fixed
        try:
            d.append_unique(path, d.mail_line("proj", "agent-1", "task", "t", None, [], "one", "proj-main"))
        finally:
            d.new_id = real
        again = d.mail_line("proj", "agent-1", "task", "t", None, [], "two", "proj-main")
        again["id"] = fixed
        d.append_unique(path, again)
        ids = [m["id"] for m in lines(path)]
        self.assertEqual(ids[0], fixed)
        self.assertNotEqual(ids[1], fixed)  # the second line drew another id instead of colliding
        # a duplicate already on disk cannot hold the read marker: after() resolves to the last one
        self.assertEqual(d.after([{"id": "a"}, {"id": "a"}, {"id": "b"}], "a"), [{"id": "b"}])


def board_rows(base):
    res = run(["board", "--json"], base)
    assert res.returncode == 0, res.stderr
    return {r["agent"]: r for r in json.loads(res.stdout)["agents"]}


class RulesTests(Base):
    """The dot root may never paste the brief, so the tool repeats the rules by itself."""

    def test_the_first_agent_command_prints_the_rules_before_its_output(self):
        res = self.tick()
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("AGENT_RULES", res.stdout)
        self.assertLess(res.stdout.index("AGENT_RULES"), res.stdout.index("LIVE "))
        again = self.tick(state="step")
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertNotIn("AGENT_RULES", again.stdout)   # once, not on every command

    def test_a_first_wait_prints_the_rules_too(self):
        mid = self.send()
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "5"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("AGENT_RULES", res.stdout)
        self.assertLess(res.stdout.index("AGENT_RULES"), res.stdout.index(mid))

    def test_the_main_session_commands_never_print_the_rules(self):
        self.send()
        for args in (["board"], ["inbox"], ["memo", "DOT-A", "agent-1"]):
            self.assertNotIn("AGENT_RULES", run(args, self.base).stdout, args)
    def test_a_stub_memory_does_not_swallow_the_rules(self):
        # the memory folder is writable by the dot root, so a file that merely exists must not count:
        # only the memory `tick` would have written silences the banner
        path = os.path.join(self.base, "memory", "DOT-A", "agent-1.json")
        d.write_atomic(path, "{}\n")
        res = self.tick()
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("AGENT_RULES", res.stdout)
        self.assertEqual(d.load_json(path)["agent"], "agent-1")   # tick wrote the real one over it
        self.assertNotIn("AGENT_RULES", self.tick(state="step").stdout)


class ReplyWaitTests(Base):
    """reply result|question|block stays open, so the answer is that command's output."""

    def start_reply(self, *args, agent="agent-1", body="done, rev/x-20261123"):
        p = spawn(["reply", "proj", agent, *args], self.base)
        p.stdin.write(body)
        p.stdin.close()
        self.procs.append(p)
        return p

    def test_a_result_waits_and_prints_the_answer(self):
        mid = self.send(text="fix it")
        self.assertEqual(self.tick("--took", mid).returncode, 0)
        p = self.start_reply("result", "issue 1", "--re", mid, "--timeout", "30")
        self.assertTrue(wait_for(lambda: marker_files(self.base), 5.0), "reply never opened a wait marker")
        out_path = os.path.join(self.base, "proj", "mail", "agent-1", "out.jsonl")
        result = lines(out_path)[0]   # written before the wait, so the id is here
        rid = result["id"]
        answer = run(["send", "proj", "agent-1", "answer", "issue 1", "--re", rid], self.base,
                     stdin="all gates PASS")
        self.assertEqual(answer.returncode, 0, answer.stderr)
        aid = answer.stdout.splitlines()[0]
        out, err = p.communicate(timeout=30)
        self.assertEqual(p.returncode, 0, err)
        self.assertEqual(out.splitlines()[0], rid)   # the id is still the first line
        self.assertIn("REPLY waiting for the answer", out)
        self.assertIn("do not end the turn", out)
        self.assertIn(aid, out)   # the answer is this command's output
        self.assertIn("all gates PASS", out)
        self.assertEqual(marker_files(self.base), [])
        self.assertEqual((result["kind"], result["re"]), ("result", mid))

    def test_a_result_that_gets_no_answer_exits_three_and_leaves_no_marker(self):
        p = self.start_reply("result", "issue 1", "--ref", "rev/x", "--dot", "DOT-A", "--timeout", "1")
        out, err = p.communicate(timeout=20)
        self.assertEqual(p.returncode, 3, out + err)   # not 0: no answer arrived
        self.assertIn("WAIT timeout", out)
        self.assertEqual(marker_files(self.base), [])

    def test_no_wait_returns_at_once(self):
        started = time.monotonic()
        res = run(["reply", "proj", "agent-1", "result", "issue 1", "--ref", "rev/x", "--no-wait"],
                  self.base, stdin="done")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertNotIn("WAIT listening", res.stdout)
        self.assertEqual(marker_files(self.base), [])
        self.assertEqual(len(lines(os.path.join(self.base, "proj", "mail", "agent-1", "out.jsonl"))), 1)

    def test_ack_answer_and_note_never_block(self):
        mid = self.send()
        for kind in ("ack", "answer", "note"):
            started = time.monotonic()
            res = run(["reply", "proj", "agent-1", kind, "issue 1", "--re", mid], self.base, stdin="ok")
            self.assertEqual(res.returncode, 0, res.stderr)
            self.assertLess(time.monotonic() - started, 5.0, kind)
            self.assertNotIn("WAIT listening", res.stdout, kind)
        self.assertEqual(marker_files(self.base), [])

    def test_only_the_answer_to_this_reply_ends_the_wait(self):
        # a note or a question that arrives while the reply waits is printed, but it is not the answer
        mid = self.send(text="fix it")
        self.assertEqual(self.tick("--took", mid).returncode, 0)
        self.send(kind="note", task="issue 1", text="a note left unread")
        p = self.start_reply("result", "issue 1", "--re", mid, "--timeout", "30")
        self.assertTrue(wait_for(lambda: marker_files(self.base), 5.0), "reply never opened a wait marker")
        rid = lines(os.path.join(self.base, "proj", "mail", "agent-1", "out.jsonl"))[0]["id"]
        time.sleep(2.0)
        self.assertIsNone(p.poll(), "mail that is not the answer ended the wait")
        answer = run(["send", "proj", "agent-1", "answer", "issue 1", "--re", rid], self.base,
                     stdin="all gates PASS")
        aid = answer.stdout.splitlines()[0]
        out, err = p.communicate(timeout=30)
        self.assertEqual(p.returncode, 0, out + err)
        self.assertIn(aid, out)
        self.assertIn("all gates PASS", out)

    def test_a_line_that_cannot_be_marked_delivered_prints_once(self):
        # a hand-written line with a bad id cannot move .agent-read, so a reply that keeps waiting
        # printed it again on every poll, once a second, for as long as the command stayed open
        path = os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        d.write_atomic(path, json.dumps({"v": 1, "id": "bad id!", "t": d.now(), "from": "proj-main",
                                         "to": "agent-1", "task": "issue 1", "kind": "task", "re": None,
                                         "text": "hand written", "refs": []}, ensure_ascii=False) + "\n")
        p = spawn(["reply", "proj", "agent-1", "result", "issue 1", "--ref", "rev/x", "--dot", "DOT-A",
                   "--timeout", "4"], self.base)
        p.stdin.write("done")
        p.stdin.close()
        self.procs.append(p)
        out, err = p.communicate(timeout=30)
        self.assertEqual(p.returncode, 3, out + err)     # the bad line is not the answer, so it waits
        self.assertEqual(out.count("MAIL_NEW"), 1, out)  # once, not once per poll
        self.assertEqual(out.count("bad id!"), 1, out)

    def test_a_first_result_without_re_is_schema_valid(self):
        # a result for work the dot root gave answers no mail line: re is null and a ref says what it is
        res = run(["reply", "proj", "agent-2", "result", "issue 1", "--ref", "rev/x-20261123", "--no-wait"],
                  self.base, stdin="done")
        self.assertEqual(res.returncode, 0, res.stderr)
        msg = lines(os.path.join(self.base, "proj", "mail", "agent-2", "out.jsonl"))[0]
        self.assertEqual(set(msg), MAIL_KEYS)
        self.assertIsNone(msg["re"])
        self.assertEqual(msg["refs"], ["rev/x-20261123"])
        self.assertEqual(msg["kind"], "result")

    def test_a_refused_reply_writes_nothing(self):
        out = os.path.join(self.base, "proj", "mail", "agent-1", "out.jsonl")
        self.assertEqual(run(["reply", "proj", "agent-1", "result", "issue 1"], self.base,
                             stdin="x").returncode, 2)        # no --re and no --ref
        self.assertEqual(run(["reply", "proj", "agent-1", "ack", "issue 1"], self.base,
                             stdin="x").returncode, 2)        # ack needs --re
        self.assertEqual(run(["reply", "proj", "agent-1", "result", "issue 1", "--re",
                              "m-20260101T000000Z-0000"], self.base, stdin="x").returncode, 2)
        self.assertFalse(os.path.exists(out))

    def test_done_is_refused_while_an_answer_is_owed(self):
        mid = self.send(kind="review", text="G1 FAIL: see the log")
        self.assertEqual(self.tick("--took", mid).returncode, 0)   # read, still unanswered
        live = os.path.join(self.base, "live", "DOT-A", "agent-1.jsonl")
        before = read(live)
        res = run(["tick", "DOT-A", "agent-1", "done"] + list(TICK) + list(FIRST), self.base)
        self.assertEqual(res.returncode, 2)
        self.assertIn("done refused", res.stderr)
        self.assertIn(mid, res.stderr)
        self.assertIn("reply proj agent-1 result", res.stderr)
        self.assertEqual(read(live), before)   # nothing was written
        # answering the review is the way out, and then done is allowed
        self.assertEqual(run(["reply", "proj", "agent-1", "ack", "issue 1", "--re", mid], self.base,
                             stdin="fixed").returncode, 0)
        done = run(["tick", "DOT-A", "agent-1", "done"] + list(TICK) + list(FIRST), self.base)
        self.assertEqual(done.returncode, 0, done.stderr)


class BoardEvidenceTests(Base):
    """Every flag comes from a line an agent or the main session wrote, never from the roster."""

    def test_mail_read_but_unanswered_on_a_silent_agent_is_flagged(self):
        mid = self.send(kind="review", text="G1 FAIL: see the log")
        self.assertEqual(self.tick("--took", mid).returncode, 0)   # an abandoned wait leaves this state
        row = board_rows(self.base)["agent-1"]
        self.assertIn("MAIL_WAITING", row["flags"])
        self.assertEqual(row["mail"]["waiting"], 1)
        self.assertEqual(row["mail"]["unanswered"], 1)
        text = run(["board"], self.base).stdout
        self.assertIn("MAIL_WAITING", text)
        self.assertIn("1 waiting", text)

    def test_an_answered_message_is_not_flagged(self):
        self.assertEqual(self.tick().returncode, 0)   # the agent's own live line puts it on the board
        mid = self.send(kind="review", text="G1 FAIL")
        self.assertEqual(run(["reply", "proj", "agent-1", "ack", "issue 1", "--re", mid], self.base,
                             stdin="fixed").returncode, 0)
        row = board_rows(self.base)["agent-1"]
        self.assertNotIn("MAIL_WAITING", row["flags"])
        self.assertEqual(row["mail"]["waiting"], 0)

    def test_a_listening_agent_is_not_flagged(self):
        mid = self.send(kind="review", text="G1 FAIL")
        self.assertEqual(self.tick("--took", mid).returncode, 0)
        p = self.start_wait()
        try:
            self.assertNotIn("MAIL_WAITING", board_rows(self.base)["agent-1"]["flags"])
        finally:
            p.send_signal(signal.SIGTERM)
            p.communicate(timeout=10)

    def test_a_done_line_with_newer_mail_is_a_stale_claim(self):
        self.assertEqual(self.tick(state="done").returncode, 0)   # nothing owed yet
        self.send(kind="task", text="keep going")
        row = board_rows(self.base)["agent-1"]
        self.assertIn("CLAIM-STALE", row["flags"])
        self.assertIn("done", row["claim_stale"])
        self.assertIn("claim:", run(["board"], self.base).stdout)

    def test_a_roster_claim_of_done_is_shown_as_the_roots_claim(self):
        self.assertEqual(self.tick().returncode, 0)   # the agent's own line says start
        d.write_atomic(os.path.join(self.base, "live", "DOT-A", "roster.json"),
                       json.dumps({"slots": [{"agent": "agent-1", "state": "done"}]}) + "\n")
        row = board_rows(self.base)["agent-1"]
        self.assertEqual(row["roster"], "done")   # the root's word, reported as such
        self.assertIn("CLAIM-STALE", row["flags"])
        self.assertIn("roster claims done", row["claim_stale"])
        self.assertIn("(the root's claim)", run(["board"], self.base).stdout)


class WatchTests(Base):
    """The watchdog runs outside the dot and reads only mail, live lines and listen markers."""

    def config_with(self, **kw):
        path = os.path.join(self.base, "dots.json")
        cfg = d.load_json(path)
        cfg.update(kw)
        d.write_atomic(path, json.dumps(cfg, indent=1) + "\n")

    def recorder(self, name, placeholders=("{id}",)):
        """An argv that appends one line of its substituted arguments to a file in the base."""
        target = os.path.join(self.base, name)
        code = ("import pathlib, sys;"
                "pathlib.Path(sys.argv[1]).open('a').write(' '.join(sys.argv[2:]) + chr(10))")
        return target, [sys.executable, "-c", code, target, *placeholders]

    def waiting_message(self, read=True, agent="agent-1", kind="review"):
        mid = self.send(kind=kind, text="G1 FAIL: see the log", agent=agent)
        if read:
            self.assertEqual(self.tick("--took", mid, agent=agent).returncode, 0)
        return mid

    def test_a_wake_runs_once_for_a_message_nobody_answered(self):
        mid = self.waiting_message(read=False)
        target, argv = self.recorder("wake.txt")
        self.config_with(wake=argv, watch={"after_min": 0, "notify_after_min": 100})
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)
        self.assertIn("exit=0", res.stdout)
        self.assertEqual(read(target).split(), [mid])
        again = run(["watch"], self.base)
        self.assertNotIn("WATCH_WAKE", again.stdout)
        self.assertEqual(read(target).split(), [mid])   # its ledger kept a restart from ringing twice
        self.assertEqual(marker_files(self.base), [])   # the watchdog never claims to be a waiter

    def test_a_message_younger_than_the_threshold_is_not_woken(self):
        self.waiting_message(read=False)
        target, argv = self.recorder("wake.txt")
        self.config_with(wake=argv)   # the default after_min is 10 minutes
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("WATCH_WAKE", res.stdout)
        self.assertFalse(os.path.exists(target))

    def test_a_notify_runs_once_when_the_wake_was_not_read(self):
        mid = self.waiting_message(read=False)
        _, wake = self.recorder("wake.txt")
        target, notify = self.recorder("notify.txt", ("{id}", "{age}"))
        self.config_with(wake=wake, notify=notify, watch={"after_min": 0, "notify_after_min": 0})
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("WATCH_WAKE", res.stdout)
        self.assertIn(f"WATCH_NOTIFY proj agent-1 {mid}", res.stdout)
        self.assertEqual(read(target).split()[0], mid)   # {age} was substituted too
        self.assertEqual(len(read(target).split()), 2)
        again = run(["watch"], self.base)
        self.assertNotIn("WATCH_NOTIFY", again.stdout)
        self.assertEqual(len(read(target).strip().splitlines()), 1)

    def test_an_answered_message_is_reported_resolved_once(self):
        mid = self.waiting_message(read=False)
        _, argv = self.recorder("wake.txt")
        self.config_with(wake=argv, watch={"after_min": 0, "notify_after_min": 100})
        self.assertIn("WATCH_WAKE", run(["watch"], self.base).stdout)
        self.assertEqual(run(["reply", "proj", "agent-1", "ack", "issue 1", "--re", mid], self.base,
                             stdin="fixed").returncode, 0)
        res = run(["watch"], self.base)
        self.assertIn(f"WATCH_RESOLVED proj agent-1 {mid}", res.stdout)
        # the config line is printed every run, so "nothing happened" is the empty tail after it
        self.assertEqual(run(["watch"], self.base).stdout.strip().splitlines()[1:], [])

    def test_a_listening_agent_is_not_woken(self):
        self.waiting_message(read=True)   # read, so the waiter has nothing to print
        target, argv = self.recorder("wake.txt")
        self.config_with(wake=argv, watch={"after_min": 0, "notify_after_min": 100})
        p = self.start_wait()
        try:
            res = run(["watch"], self.base)
            self.assertNotIn("WATCH_WAKE", res.stdout)
            self.assertFalse(os.path.exists(target))
        finally:
            p.send_signal(signal.SIGTERM)
            p.communicate(timeout=10)

    def test_the_expected_hash_catches_a_swapped_tool(self):
        res = run(["watch", "--expect-sha256", "0" * 64], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("WATCH_TOOL_CHANGED", res.stdout)
        real = hashlib.sha256(read(SCRIPT).encode()).hexdigest()
        res = run(["watch", "--expect-sha256", real], self.base)
        self.assertNotIn("WATCH_TOOL_CHANGED", res.stdout)

    def test_a_missing_wake_command_is_reported_once(self):
        self.waiting_message(read=False)
        self.config_with(watch={"after_min": 0})
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("WATCH_ERROR no wake command", res.stdout)
        self.assertEqual(res.stdout.count("WATCH_ERROR"), 1)

    def test_a_bad_loop_or_hash_is_refused(self):
        self.assertEqual(run(["watch", "--loop", "-1"], self.base).returncode, 2)
        self.assertEqual(run(["watch", "--expect-sha256", "abc"], self.base).returncode, 2)

    def test_the_command_line_beats_a_hostile_dots_json(self):
        # dots.json sits in the folder the dot root writes: a changed wake, a huge delay and a notify
        # that never fires are all one edit away, so the owner's own flags decide, and say so
        mid = self.waiting_message(read=False)
        target, argv = self.recorder("wake.txt")
        ntarget, notify = self.recorder("notify.txt", ("{id}", "{age}"))
        self.config_with(wake=["/usr/bin/true"], notify=["/usr/bin/true"],
                         watch={"after_min": 100000, "notify_after_min": 100000})
        res = run(["watch", "--wake-cmd", json.dumps(argv), "--notify-cmd", json.dumps(notify),
                   "--after-min", "0", "--notify-after-min", "0"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)
        self.assertIn(f"WATCH_NOTIFY proj agent-1 {mid}", res.stdout)
        self.assertEqual(read(target).split(), [mid])   # the owner's wake, not dots.json's
        self.assertEqual(read(ntarget).split()[0], mid)
        config = [ln for ln in res.stdout.splitlines() if ln.startswith("WATCH_CONFIG")][0]
        for part in ("wake=argv:", "notify=argv:", "after_min=argv:0", "notify_after_min=argv:0"):
            self.assertIn(part, config)

    def test_the_ledger_cannot_hide_a_ring(self):
        # the ledger lives in the same folder, so a forged record may only add a visible line
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        ntarget, notify = self.recorder("notify.txt", ("{id}", "{age}"))
        self.config_with(wake=wake, notify=notify, watch={"after_min": 0, "notify_after_min": 0})
        ledger = os.path.join(self.base, "watch", "state.json")
        os.makedirs(os.path.dirname(ledger), exist_ok=True)
        old = d.iso_after(-3600)
        d.write_atomic(ledger, json.dumps({"v": 1, "messages": {mid: {
            "project": "proj", "agent": "agent-1", "wake_t": old, "notify_t": old}}}) + "\n")
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_SKIP proj agent-1 {mid} reason=wake_already", res.stdout)
        self.assertIn(f"WATCH_SKIP proj agent-1 {mid} reason=notify_already", res.stdout)
        self.assertNotIn("WATCH_WAKE", res.stdout)
        self.assertFalse(os.path.exists(target))
        # a record with only a wake behind it still lets the notify run: the ledger may not end the
        # escalation halfway, so the owner is told even when the wake step is suppressed
        d.write_atomic(ledger, json.dumps({"v": 1, "messages": {mid: {
            "project": "proj", "agent": "agent-1", "wake_t": old}}}) + "\n")
        res = run(["watch"], self.base)
        self.assertIn("reason=wake_already", res.stdout)
        self.assertIn(f"WATCH_NOTIFY proj agent-1 {mid}", res.stdout)
        self.assertEqual(read(ntarget).split()[0], mid)
        # --state keeps the ledger outside the folder the dot root writes
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        path = os.path.join(outside.name, "ledger.json")
        res = run(["watch", "--state", path, "--after-min", "0", "--notify-after-min", "0"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"state=argv:{path}", res.stdout)
        self.assertEqual(set(d.load_json(path)["messages"]), {mid})
        self.assertEqual(set(d.load_json(ledger)["messages"]), {mid})   # the default ledger is untouched

    def forget(self):
        """Drop the watchdog's ledger, so the next run is a first run again."""
        path = os.path.join(self.base, "watch", "state.json")
        if os.path.exists(path):
            os.unlink(path)

    def test_a_marker_without_until_or_far_in_the_future_is_not_a_waiter(self):
        mid = self.waiting_message(read=True)   # read, so no real waiter has anything to print
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        marker = os.path.join(self.base, "listen", "agent-1.999.json")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        d.write_atomic(marker, json.dumps({"pid": 1, "host": "elsewhere"}) + "\n")   # no until
        res = run(["watch"], self.base)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)
        self.assertIn("MAIL_WAITING", board_rows(self.base)["agent-1"]["flags"])
        os.unlink(target)
        self.forget()
        d.write_atomic(marker, json.dumps({"pid": 1, "host": "elsewhere",
                                           "until": "2099-01-01T00:00:00Z"}) + "\n")
        res = run(["watch"], self.base)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)   # a decade is not a wait
        os.unlink(target)
        self.forget()
        d.write_atomic(marker, json.dumps({"pid": 1, "host": "elsewhere",
                                           "until": d.iso_after(300)}) + "\n")
        res = run(["watch"], self.base)
        self.assertNotIn("WATCH_WAKE", res.stdout)   # a foreign waiter with a real until is believed
        self.assertFalse(os.path.exists(target))

    def test_a_forged_read_marker_is_reported_and_the_notify_still_runs(self):
        # .agent-read lives in the same folder as the mail, so it may add an event, never silence one
        mid = self.waiting_message(read=False)
        _, wake = self.recorder("wake.txt")
        ntarget, notify = self.recorder("notify.txt", ("{id}", "{age}"))
        self.config_with(wake=wake, notify=notify, watch={"after_min": 0, "notify_after_min": 100})
        self.assertIn("WATCH_WAKE", run(["watch"], self.base).stdout)   # the notify is not due yet
        self.assertFalse(os.path.exists(ntarget))
        d.write_atomic(os.path.join(self.base, "proj", "mail", "agent-1", ".agent-read"), mid + "\n")
        res = run(["watch", "--notify-after-min", "0"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_STUCK proj agent-1 {mid}", res.stdout)
        self.assertIn("read=yes unanswered=yes", res.stdout)
        self.assertIn(f"WATCH_NOTIFY proj agent-1 {mid}", res.stdout)
        self.assertIn("read=yes", res.stdout)
        self.assertEqual(read(ntarget).split()[0], mid)   # the owner was told anyway
        again = run(["watch", "--notify-after-min", "0"], self.base)
        self.assertNotIn("WATCH_STUCK", again.stdout)   # once, not on every run

    def test_a_live_marker_never_silences_the_watchdog(self):
        # the listen folder sits where the dot root writes, so a marker naming a live local pid and a
        # plausible until can hold the wake back; the mail it contradicts is still reported, every run
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        marker = os.path.join(self.base, "listen", "agent-1.4242.json")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        d.write_atomic(marker, json.dumps({"pid": 1, "host": socket.gethostname(),
                                           "until": d.iso_after(23 * 3600)}) + "\n")
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        state = os.path.join(outside.name, "ledger.json")
        real = hashlib.sha256(read(SCRIPT).encode()).hexdigest()
        args = ["watch", "--wake-cmd", json.dumps(wake), "--notify-cmd", json.dumps(["/usr/bin/true"]),
                "--after-min", "0", "--notify-after-min", "100", "--state", state,
                "--expect-sha256", real]
        res = run(args, self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_HELD proj agent-1 {mid} age=0m pid=1", res.stdout)
        self.assertIn("read=no", res.stdout)
        self.assertNotIn("WATCH_TOOL_CHANGED", res.stdout)
        self.assertNotIn("WATCH_WAKE", res.stdout)   # a real waiter would have consumed the mail
        self.assertFalse(os.path.exists(target))
        again = run(args, self.base)
        self.assertIn(f"WATCH_HELD proj agent-1 {mid}", again.stdout)   # every run, not once

    def test_a_message_with_no_usable_time_still_rings(self):
        # the root writes `t` too: an unparseable or future value must not hide a message from the wake
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        inbox = os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl")
        line = lines(inbox)[0]
        for stamp in ("garbage", "2099-01-01T00:00:00Z"):
            self.forget()
            if os.path.exists(target):
                os.unlink(target)
            d.write_atomic(inbox, json.dumps(dict(line, t=stamp), ensure_ascii=False) + "\n")
            res = run(["watch"], self.base)
            self.assertEqual(res.returncode, 0, res.stderr)
            self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout, stamp)
            self.assertEqual(read(target).split(), [mid], stamp)

    def test_a_message_deleted_before_its_first_ring_still_resolves(self):
        # the ledger remembers the first sight, so a line removed before any wake does not just vanish
        mid = self.waiting_message(read=False)
        _, wake = self.recorder("wake.txt")
        self.config_with(wake=wake)   # the default after_min of 10 minutes: no wake on this run
        self.assertNotIn("WATCH_WAKE", run(["watch"], self.base).stdout)
        os.unlink(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"))
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_RESOLVED proj agent-1 {mid}", res.stdout)

    def test_a_marker_at_the_timeout_limit_with_a_clock_ahead_still_counts(self):
        # a waiter holding the longest allowed timeout writes until = now + 86400 on its own clock, so
        # a reader whose clock lags must not judge that waiter dead and ring over it
        mid = self.waiting_message(read=True)   # read, so no real waiter has anything left to print
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        marker = os.path.join(self.base, "listen", "agent-1.4242.json")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        d.write_atomic(marker, json.dumps({"pid": 1, "host": socket.gethostname(),
                                           "until": d.iso_after(86400 + 180)}) + "\n")
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("WATCH_WAKE", res.stdout)
        self.assertFalse(os.path.exists(target))
        self.assertTrue(os.path.exists(marker))   # the watchdog never prunes a marker it believes
        # a day and a half out is still not a waiter: the slack is an hour, not an open-ended cap
        d.write_atomic(marker, json.dumps({"pid": 1, "host": socket.gethostname(),
                                           "until": d.iso_after(86400 + 7200)}) + "\n")
        res = run(["watch"], self.base)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)

    def test_a_non_string_re_does_not_kill_the_watchdog(self):
        # out.jsonl is the root's to write as well: a `re` that is not a string answers nothing, and
        # feeding it to a set of answers used to end the run with a traceback
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        d.append_line(os.path.join(self.base, "proj", "mail", "agent-1", "out.jsonl"),
                      {"v": 1, "id": "m-20260101T000000Z-0001", "t": d.now(), "from": "agent-1",
                       "to": "proj-main", "task": "issue 1", "kind": "note", "re": {"n": 1},
                       "text": "not an answer", "refs": []})
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)   # the task is still owed
        self.assertEqual(read(target).split(), [mid])
        box = os.path.join(self.base, "proj", "mail", "agent-1")
        self.assertEqual([m.get("id") for m in d.waiting_on(box)], [mid])   # a non-answer answers nothing

    def test_a_non_string_id_does_not_kill_the_watchdog(self):
        # the same for in.jsonl: an id that is a list cannot be answered, and cannot be hashed either
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        d.append_line(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"),
                      {"v": 1, "id": ["x"], "t": d.now(), "from": "proj-main", "to": "agent-1",
                       "task": "issue 1", "kind": "task", "re": None, "text": "hand written", "refs": []})
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)   # the real task still rings
        self.assertEqual(run(["board"], self.base).returncode, 0)

    def test_a_folder_where_a_marker_belongs_does_not_kill_the_watchdog(self):
        # listen/ is the root's folder too: a directory named like a marker is not a waiter, and it
        # must not be read or pruned into an exception
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        os.makedirs(os.path.join(self.base, "listen", "agent-1.5.json"))
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)
        # send prunes markers on the same path and must survive the same folder
        self.assertEqual(run(["send", "proj", "agent-1", "note", "issue 1"], self.base,
                             stdin="still here").returncode, 0)

    def test_a_folder_where_a_mailbox_file_belongs_does_not_kill_the_watchdog(self):
        self.waiting_message(read=False)
        box = os.path.join(self.base, "proj", "mail", "agent-1")
        os.unlink(os.path.join(box, "in.jsonl"))
        os.makedirs(os.path.join(box, "in.jsonl"))
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertEqual(run(["board"], self.base).returncode, 0)

    def test_a_folder_where_the_ledger_belongs_still_rings(self):
        # a record that cannot be written may only make a later run ring again, never end the run
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        os.makedirs(os.path.join(self.base, "watch", "state.json"))
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)
        self.assertIn("cannot write the ledger", res.stderr)
        self.assertIn("every run rings again until it is writable", res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertEqual(read(target).split(), [mid])

    def test_a_time_in_the_future_on_both_sides_is_reported_not_hidden(self):
        # a future t is not an age, and a future seen_t is not one either: silence would hide the
        # message, so both are dropped and the run says why
        mid = self.waiting_message(read=False)
        self.config_with(watch={"after_min": 0})
        inbox = os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl")
        line = lines(inbox)[0]
        d.write_atomic(inbox, json.dumps(dict(line, t="2099-01-01T00:00:00Z"), ensure_ascii=False) + "\n")
        ledger = os.path.join(self.base, "watch", "state.json")
        os.makedirs(os.path.dirname(ledger), exist_ok=True)
        d.write_atomic(ledger, json.dumps({"v": 1, "messages": {mid: {
            "project": "proj", "agent": "agent-1", "seen_t": "2099-01-01T00:00:00Z"}}}) + "\n")
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"WATCH_ERROR proj agent-1 {mid} no usable time", res.stdout)

    def test_a_ledger_record_that_is_not_an_object_still_rings(self):
        # the ledger is the root's to write too: a record that is a list, a string or null has nothing
        # in it to read, and setdefault on one used to end the run with a traceback
        mid = self.waiting_message(read=False)
        target, wake = self.recorder("wake.txt")
        self.config_with(wake=wake, watch={"after_min": 0, "notify_after_min": 100})
        ledger = os.path.join(self.base, "watch", "state.json")
        os.makedirs(os.path.dirname(ledger), exist_ok=True)
        d.write_atomic(ledger, json.dumps({"v": 1, "messages": {
            mid: ["not", "a", "record"],
            "m-20260101T000000Z-bbbb": "x",
            "m-20260101T000000Z-cccc": None}}) + "\n")
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertIn(f"WATCH_WAKE proj agent-1 {mid}", res.stdout)   # dropping a record re-rings
        self.assertEqual(read(target).split(), [mid])
        self.assertEqual(json.loads(read(ledger))["messages"][mid]["project"], "proj")

    def test_an_unusable_id_in_owed_mail_is_reported_not_skipped(self):
        # board counts this line as owed mail, so watch may not pass over it in silence
        self.waiting_message(read=False)
        d.append_line(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"),
                      {"v": 1, "id": ["x"], "t": d.now(), "from": "proj-main", "to": "agent-1",
                       "task": "issue 1", "kind": "task", "re": None, "text": "hand written", "refs": []})
        res = run(["watch"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("WATCH_ERROR proj agent-1 ['x'] unusable id", res.stdout)

    def test_a_mailbox_that_cannot_be_read_says_so(self):
        # a chmod 000 inbox is not the same thing as a missing one: empty mail with no trace would be
        # a claim the tool cannot make
        self.waiting_message(read=False)
        inbox = os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl")
        os.chmod(inbox, 0)
        try:
            res = run(["watch"], self.base)
        finally:
            os.chmod(inbox, 0o644)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"dots: cannot read {inbox} (PermissionError); treated as no lines", res.stderr)
        self.assertNotIn("Traceback", res.stderr)

class InitTests(Base):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = os.path.join(self._tmp.name, "shared")
        self._saved = d.BASE
        d.BASE = self.base

    def tearDown(self):
        d.BASE = self._saved
        self._tmp.cleanup()

    def test_non_interactive_init_creates_the_tree_and_the_config(self):
        res = run(["init", "--yes", "--base", self.base, "--dot", "DOT-A",
                   "--project", "proj=proj-main", "--host", "extra=A second computer"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        cfg = d.load_json(os.path.join(self.base, "dots.json"))
        self.assertEqual(cfg["v"], 1)
        self.assertEqual([x["id"] for x in cfg["dots"]], ["DOT-A"])
        self.assertEqual([(x["id"], x["main"]) for x in cfg["projects"]], [("proj", "proj-main")])
        self.assertEqual([h["label"] for h in cfg["hosts"]], ["vm", "local", "extra"])
        self.assertTrue(all(h["purpose"] for h in cfg["hosts"]))
        for path in ("live/DOT-A", "memory/DOT-A", "proj/mail"):
            self.assertTrue(os.path.isdir(os.path.join(self.base, path)), path)
        self.assertIn("Paste this into each dot root", res.stdout)
        self.assertIn("<kit commit SHA>", res.stdout)
        self.assertIn("wait", res.stdout)

    def test_a_second_init_adds_a_dot_and_leaves_existing_data_alone(self):
        first = run(["init", "--yes", "--base", self.base, "--dot", "DOT-A", "--project", "proj=proj-main"],
                    self.base)
        self.assertEqual(first.returncode, 0, first.stderr)
        mail_file = os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl")
        os.makedirs(os.path.dirname(mail_file))
        d.append_line(mail_file, {"v": 1, "id": "m-20260101T000000Z-0000", "t": "2026-01-01T00:00:00Z",
                                  "from": "proj-main", "to": "agent-1", "task": "t", "kind": "task",
                                  "re": None, "text": "keep me", "refs": []})
        before = read(mail_file)
        cfg_before = read(os.path.join(self.base, "dots.json"))
        res = run(["init", "--yes", "--base", self.base, "--dot", "DOT-B"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("INIT dot DOT-B (added)", res.stdout)
        self.assertIn("Dots: DOT-A, DOT-B", res.stdout)
        self.assertEqual(read(mail_file), before)
        cfg = d.load_json(os.path.join(self.base, "dots.json"))
        self.assertEqual([x["id"] for x in cfg["dots"]], ["DOT-A", "DOT-B"])
        self.assertEqual([(x["id"], x["main"]) for x in cfg["projects"]], [("proj", "proj-main")])
        self.assertTrue(os.path.isdir(os.path.join(self.base, "live", "DOT-B")))
        self.assertNotEqual(read(os.path.join(self.base, "dots.json")), cfg_before)

    def test_an_unchanged_init_does_not_rewrite_the_config(self):
        args = ["init", "--yes", "--base", self.base, "--dot", "DOT-A", "--project", "proj=proj-main"]
        self.assertEqual(run(args, self.base).returncode, 0)
        path = os.path.join(self.base, "dots.json")
        before = os.stat(path).st_mtime_ns
        res = run(args, self.base)
        self.assertIn("(unchanged)", res.stdout)
        self.assertEqual(os.stat(path).st_mtime_ns, before)

    def test_wake_command_is_scriptable_and_kept(self):
        argv = [sys.executable, "-c", "pass", "{agent}"]
        res = run(["init", "--yes", "--base", self.base, "--dot", "DOT-A", "--wake-cmd", json.dumps(argv)],
                  self.base)
        self.assertIn("INIT wake (set)", res.stdout)
        self.assertEqual(d.load_json(os.path.join(self.base, "dots.json"))["wake"], argv)
        res = run(["init", "--yes", "--base", self.base, "--dot", "DOT-A"], self.base)
        self.assertIn("INIT wake (kept)", res.stdout)

    def test_bad_init_input_is_refused(self):
        self.assertEqual(run(["init", "--yes", "--base", self.base], self.base).returncode, 2)
        self.assertEqual(run(["init", "--yes", "--base", self.base, "--dot", "DOT-A",
                              "--wake-cmd", "not json"], self.base).returncode, 2)
        self.assertEqual(run(["init", "--yes", "--base", self.base, "--dot", "DOT-A",
                              "--host", "no-separator"], self.base).returncode, 2)

    def test_a_refused_init_creates_nothing(self):
        nested = os.path.join(self.base, "h4", "nested")
        res = run(["init", "--yes", "--base", nested], self.base)
        self.assertEqual(res.returncode, 2)
        self.assertIn("no dots", res.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.base, "h4")))

    def test_init_prints_the_export_line_for_a_base_that_is_not_the_default(self):
        home = tempfile.mkdtemp(dir=self._tmp.name)
        env = {k: v for k, v in os.environ.items() if k != "DOTS_OPS_BASE"}
        env.update(HOME=home, PYTHONDONTWRITEBYTECODE="1")
        elsewhere = os.path.join(home, "shared")
        res = subprocess.run([sys.executable, SCRIPT, "init", "--yes", "--base", elsewhere, "--dot", "DOT-A"],
                             env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(f"INIT export DOTS_OPS_BASE={elsewhere}", res.stdout)
        # the default base prints no export line: nothing has to be set
        res = subprocess.run([sys.executable, SCRIPT, "init", "--yes", "--dot", "DOT-B"],
                             env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("INIT export", res.stdout)
    def test_init_and_board_resolve_the_same_base(self):
        home = tempfile.mkdtemp(dir=self._tmp.name)
        env = {k: v for k, v in os.environ.items() if k != "DOTS_OPS_BASE"}
        env.update(HOME=home, PYTHONDONTWRITEBYTECODE="1")
        res = subprocess.run([sys.executable, SCRIPT, "init", "--yes", "--dot", "DOT-A"], env=env,
                             capture_output=True, text=True, timeout=20)
        self.assertEqual(res.returncode, 0, res.stderr)
        base = os.path.join(home, ".dots-ops")
        self.assertIn(f"INIT base={base}", res.stdout)
        live = os.path.join(base, "live", "DOT-A")
        self.assertTrue(os.path.isdir(live))
        # a line written under that base is the one board reads, so both resolved the same folder
        d.append_line(os.path.join(live, "agent-1.jsonl"),
                      {"t": d.now(), "agent": "agent-1", "role": "구현", "model": "m", "project": "proj",
                       "task": "issue 1", "state": "step", "host": "local", "note": "n", "evidence": "e"})
        board = subprocess.run([sys.executable, SCRIPT, "board"], env=env, capture_output=True, text=True,
                               timeout=20)
        self.assertEqual(board.returncode, 0, board.stderr)
        self.assertIn("DOT-A agent-1", board.stdout)
        # the maintainer's own directory name is not a candidate anywhere in the tool
        self.assertNotIn("CodeProjects", read(SCRIPT))



class WithoutConfigTests(Base):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self._saved = d.BASE
        d.BASE = self.base
        self.make_dot("DOT-A")
        os.makedirs(os.path.join(self.base, "proj", "mail", "agent-1"))

    def tearDown(self):
        d.BASE = self._saved
        self._tmp.cleanup()

    def test_every_command_works_without_dots_json(self):
        self.assertFalse(os.path.exists(os.path.join(self.base, "dots.json")))
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="hello")
        self.assertEqual(res.returncode, 0, res.stderr)
        mid = res.stdout.splitlines()[0]
        msg = lines(os.path.join(self.base, "proj", "mail", "agent-1", "in.jsonl"))[0]
        self.assertEqual(msg["from"], "proj-main")  # no config: the documented default
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "5"], self.base)
        self.assertEqual(res.returncode, 0)
        self.assertIn(mid, res.stdout)
        res = run(["tick", "DOT-A", "agent-1", "start"] + list(TICK) +
                  ["--role", "구현", "--model", "m", "--project", "proj", "--goal", "g", "--doing", "d",
                   "--next", "n"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("LIVE DOT-A/agent-1", res.stdout)
        self.assertEqual(run(["board"], self.base).returncode, 0)
        self.assertEqual(run(["memo", "DOT-A", "agent-1"], self.base).returncode, 0)


class ScriptHijackTests(unittest.TestCase):
    """The tool may be copied into a folder the dot root writes, so a sibling module must not win."""

    def test_a_sibling_module_cannot_hijack_the_stdlib(self):
        with tempfile.TemporaryDirectory() as tmp:
            tool = os.path.join(tmp, "dots.py")
            with open(tool, "w", encoding="utf-8") as fh:
                fh.write(read(SCRIPT))
            with open(os.path.join(tmp, "json.py"), "w", encoding="utf-8") as fh:
                fh.write("print('HIJACKED')\nraise SystemExit(9)\n")
            base = os.path.join(tmp, "shared")
            os.makedirs(os.path.join(base, "live", "DOT-A"))
            env = dict(os.environ, DOTS_OPS_BASE=base, PYTHONDONTWRITEBYTECODE="1")
            res = subprocess.run([sys.executable, tool, "board"], env=env, capture_output=True,
                                 text=True, timeout=20)
            self.assertNotEqual(res.returncode, 9, res.stdout + res.stderr)
            self.assertNotIn("HIJACKED", res.stdout + res.stderr)
if __name__ == "__main__":
    unittest.main()
