#!/usr/bin/env python3
"""Tests for dots.py: steering (wait), mail, init, and the tick/board regressions.

Run with: cd tools && python3 -m unittest test_dots -q
No network and no ssh: every case uses a temporary base folder and a bounded timeout.
"""

import io
import json
import os
import re
import signal
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
    return subprocess.Popen([sys.executable, SCRIPT] + list(args), env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for(path, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.02)
    return False


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
        if os.path.basename(os.path.dirname(os.path.dirname(self.base))):
            pass
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
        self.assertTrue(wait_for(os.path.join(self.base, "listen", f"{agent}.json"), 5.0),
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
        self.assertFalse(os.path.exists(os.path.join(self.base, "listen", "agent-1.json")))

    def test_wait_returns_at_once_on_unread_mail(self):
        mid = self.send(text="already here")
        started = time.monotonic()
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "30"], self.base)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertIn(mid, res.stdout)
        self.assertNotIn("WAIT listening", res.stdout)
        self.assertFalse(os.path.exists(os.path.join(self.base, "listen", "agent-1.json")))

    def test_wait_times_out_with_exit_three_and_no_marker(self):
        started = time.monotonic()
        res = run(["wait", "DOT-A", "agent-1", "--timeout", "1"], self.base)
        self.assertEqual(res.returncode, 3, res.stdout + res.stderr)
        self.assertLess(time.monotonic() - started, 10)
        tail = res.stdout.strip().splitlines()[-1]
        self.assertTrue(tail.startswith("WAIT timeout"), tail)
        self.assertIn("unread=0", tail)
        self.assertFalse(os.path.exists(os.path.join(self.base, "listen", "agent-1.json")))

    def test_sigterm_removes_the_marker(self):
        p = self.start_wait()
        p.send_signal(signal.SIGTERM)
        p.communicate(timeout=10)
        self.assertFalse(os.path.exists(os.path.join(self.base, "listen", "agent-1.json")))

    def test_marker_records_pid_dot_until_and_host(self):
        p = self.start_wait()
        marker = d.load_json(os.path.join(self.base, "listen", "agent-1.json"))
        self.assertEqual(marker["pid"], p.pid)
        self.assertEqual(marker["dot"], "DOT-A")
        self.assertIsNotNone(d.parse_t(marker["until"]))
        self.assertTrue(marker["host"])
        p.send_signal(signal.SIGTERM)
        p.communicate(timeout=10)

    def test_send_reports_listening_then_not_listening_after_a_stale_marker(self):
        p = self.start_wait()
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="one")
        self.assertIn("LISTENING", res.stdout)
        self.assertIn("pid=", res.stdout)
        p.communicate(timeout=10)
        self.take(res.stdout.splitlines()[0])
        # a marker whose pid is gone counts as not listening
        d.write_atomic(os.path.join(self.base, "listen", "agent-1.json"),
                       json.dumps({"pid": 999999, "dot": "DOT-A", "since": d.now(),
                                   "until": d.iso_after(600), "host": "test"}) + "\n")
        res = run(["send", "proj", "agent-1", "task", "issue 1"], self.base, stdin="two")
        self.assertIn("NOT_LISTENING", res.stdout)
        self.assertIn("last_tick=", res.stdout)
        self.assertIn("DOORBELL", res.stdout)

    def test_a_past_until_counts_as_not_listening(self):
        os.makedirs(os.path.join(self.base, "listen"), exist_ok=True)
        d.write_atomic(os.path.join(self.base, "listen", "agent-1.json"),
                       json.dumps({"pid": os.getpid(), "dot": "DOT-A", "since": "2020-01-01T00:00:00Z",
                                   "until": "2020-01-01T00:00:01Z", "host": "test"}) + "\n")
        self.assertIsNone(d.listen_state("agent-1"))

    def test_bad_ids_are_refused(self):
        self.assertEqual(run(["wait", "DOT-A", "bad id", "--timeout", "1"], self.base).returncode, 2)
        self.assertEqual(run(["wait", "bad dot", "agent-1", "--timeout", "1"], self.base).returncode, 2)
        self.assertEqual(run(["wait", "DOT-A", "agent-1", "--timeout", "0"], self.base).returncode, 2)


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


if __name__ == "__main__":
    unittest.main()
