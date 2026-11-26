#!/usr/bin/env python3
"""Tests for tools/subdots.py: Mode 2 reverse web-gpt-agent bridge, steering,
two-phase delivery, epoch fencing, supervised exec interrupt, anti-death watchdog,
anti-spin interventions, Reverse/Stdio MCP, and .dots-ops/ compatibility."""
from __future__ import annotations

import contextlib
import json
import os
import pathlib
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

import dots as d
import dots_monitor as dm
import subdots as sd


def cli_run(args, state_dir, stdin_text=None, timeout=20.0, env_extra=None):
    env = {k: v for k, v in os.environ.items() if k != "DOTS_OPS_BASE"}
    env["SUBDOTS_STATE_DIR"] = state_dir
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["DOTS_AUTO_UPDATE"] = "0"
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, os.path.join(TOOLS_DIR, "subdots.py")] + list(args),
        input=stdin_text,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def http_raw(url, method="GET", body=None, headers=None, timeout=10.0):
    data = json.dumps(body).encode("utf-8") if isinstance(body, (dict, list)) else body
    hdrs = dict(headers or {})
    if data is not None and "Content-Type" not in hdrs:
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def wait_until(pred, timeout=5.0, step=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = pred()
        if val:
            return val
        time.sleep(step)
    return pred()


class HubTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir = os.path.join(self._tmp.name, "deep", "private", "subdots-state")
        self.ops_base = os.path.join(self._tmp.name, "dots-ops")
        self._hub_procs = []

    def tearDown(self):
        with contextlib.suppress(Exception):
            cli_run(["hub", "stop", "--state-dir", self.state_dir], self.state_dir, timeout=5.0)
        for p in self._hub_procs:
            if p.poll() is None:
                with contextlib.suppress(Exception):
                    p.terminate()
                    p.wait(timeout=2.0)
        self._tmp.cleanup()

    def start_hub(self, extra_args=()):
        res = cli_run(
            ["hub", "start", "--state-dir", self.state_dir, "--json"] + list(extra_args),
            self.state_dir,
            timeout=15.0,
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        return json.loads(res.stdout)

    def register_agent(self, agent="worker-1", dot="DOT-A", project="proj", lease_ttl=None):
        args = ["agent", "register", agent, "--dot", dot, "--project", project, "--state-dir", self.state_dir, "--json"]
        if lease_ttl is not None:
            args += ["--lease-ttl", str(lease_ttl)]
        res = cli_run(args, self.state_dir)
        self.assertEqual(res.returncode, 0, res.stderr)
        return json.loads(res.stdout)


class TestHubLifecycleAndSecurity(HubTestBase):
    def test_hub_start_permissions_socket_fallback_and_persistence_across_restart(self):
        # Create a very long path so macOS sun_path (>96 bytes) exercises /tmp/sd-<hash>/c.sock fallback.
        long_dir = os.path.join(self._tmp.name, "x" * 95, "subdots")
        res = cli_run(["hub", "start", "--state-dir", long_dir, "--json"], long_dir)
        self.assertEqual(res.returncode, 0, res.stderr)
        try:
            self.assertEqual(stat.S_IMODE(os.stat(long_dir).st_mode), 0o700)
            rt = sd.read_runtime(long_dir)
            self.assertEqual(stat.S_IMODE(os.stat(sd.runtime_path_for(long_dir)).st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(os.stat(rt["socket_path"]).st_mode), 0o600)
            self.assertLessEqual(len(rt["socket_path"].encode("utf-8")), 96)

            reg = cli_run(["agent", "register", "w1", "--dot", "DOT-A", "--state-dir", long_dir, "--json"], long_dir)
            self.assertEqual(reg.returncode, 0, reg.stderr)
            sent = cli_run(
                ["send", "w1", "task", "T1", "--text", "persist me", "--state-dir", long_dir, "--json"], long_dir
            )
            self.assertEqual(sent.returncode, 0, sent.stderr)
            mid = json.loads(sent.stdout)["message"]["id"]

            self.assertEqual(cli_run(["hub", "stop", "--state-dir", long_dir], long_dir).returncode, 0)
            # Socket directory should be cleaned up and status should report stopped (exit 3).
            st_down = cli_run(["hub", "status", "--state-dir", long_dir, "--json"], long_dir)
            self.assertEqual(st_down.returncode, 3)

            # Restart hub on same state dir and confirm SQLite WAL persisted agent & queued message.
            res2 = cli_run(["hub", "start", "--state-dir", long_dir, "--json"], long_dir)
            self.assertEqual(res2.returncode, 0, res2.stderr)
            status_res = cli_run(["status", "w1", "--state-dir", long_dir, "--json"], long_dir)
            self.assertEqual(status_res.returncode, 0, status_res.stderr)
            rec = json.loads(status_res.stdout)
            self.assertTrue(any(m["id"] == mid for m in rec["recent_messages"]))
        finally:
            cli_run(["hub", "stop", "--state-dir", long_dir], long_dir)

    def test_http_security_boundaries_host_origin_token_and_prm(self):
        st = self.start_hub()
        rt = sd.read_runtime(self.state_dir)
        base = f"http://127.0.0.1:{rt['http_port']}"

        # 1) Missing/bad token -> 401 JSON
        code, body = http_raw(f"{base}/wrongtoken/health")
        self.assertEqual(code, 401)
        self.assertEqual(json.loads(body)["code"], "UNAUTHORIZED")

        # 2) Valid token works
        code, body = http_raw(f"{rt['http_url']}/health")
        self.assertEqual(code, 200)
        self.assertTrue(json.loads(body)["ok"])

        # 3) RFC 9728 protected-resource metadata
        code, body = http_raw(f"{base}/.well-known/oauth-protected-resource/{rt['token']}/mcp")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["authorization_servers"], [])

        # 4) Bad Origin -> 403 JSON
        code, body = http_raw(f"{rt['http_url']}/health", headers={"Origin": "https://evil.example"})
        self.assertEqual(code, 403)
        self.assertEqual(json.loads(body)["code"], "FORBIDDEN_ORIGIN")

        # 5) Bad Host -> 403 JSON
        code, body = http_raw(f"{rt['http_url']}/health", headers={"Host": "evil.example"})
        self.assertEqual(code, 403)
        self.assertEqual(json.loads(body)["code"], "FORBIDDEN_HOST")

        # 6) Oversized payload -> 413 JSON
        import http.client
        conn = http.client.HTTPConnection("127.0.0.1", rt["http_port"], timeout=5.0)
        try:
            conn.putrequest("POST", f"/{rt['token']}/agent/heartbeat")
            conn.putheader("Content-Type", "application/json")
            conn.putheader("Content-Length", str(sd.MAX_HTTP_BODY_BYTES + 10))
            conn.endheaders()
            resp = conn.getresponse()
            body = resp.read().decode("utf-8")
            self.assertEqual(resp.status, 413)
            self.assertEqual(json.loads(body)["code"], "PAYLOAD_TOO_LARGE")
        finally:
            conn.close()


class TestReverseSteeringAndDelivery(HubTestBase):
    def test_idempotency_and_request_id_conflict(self):
        self.start_hub()
        self.register_agent("w1")
        r1 = cli_run(
            ["send", "w1", "task", "T1", "--text", "hello", "--request-id", "req-1", "--state-dir", self.state_dir, "--json"],
            self.state_dir,
        )
        r2 = cli_run(
            ["send", "w1", "task", "T1", "--text", "hello", "--request-id", "req-1", "--state-dir", self.state_dir, "--json"],
            self.state_dir,
        )
        self.assertEqual(r1.returncode, 0)
        self.assertEqual(r2.returncode, 0)
        d1, d2 = json.loads(r1.stdout), json.loads(r2.stdout)
        self.assertFalse(d1["deduped"])
        self.assertTrue(d2["deduped"])
        self.assertEqual(d1["message"]["id"], d2["message"]["id"])

        conflict = cli_run(
            ["send", "w1", "task", "T1", "--text", "changed", "--request-id", "req-1", "--state-dir", self.state_dir, "--json"],
            self.state_dir,
        )
        self.assertEqual(conflict.returncode, 2)
        self.assertIn("REQUEST_ID_CONFLICT", conflict.stderr)

    def test_live_bidirectional_wait_piggyback_and_done_refusal(self):
        self.start_hub()
        self.register_agent("w1")

        # Start agent wait in background; local send should wake it immediately (<2s).
        with subprocess.Popen(
            [
                sys.executable,
                os.path.join(TOOLS_DIR, "subdots.py"),
                "agent",
                "wait",
                "w1",
                "--timeout",
                "10",
                "--chunk",
                "5",
                "--state-dir",
                self.state_dir,
                "--json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as wait_proc:
            try:
                self.assertTrue(
                    wait_until(
                        lambda: json.loads(cli_run(["status", "w1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["active_poll_count"] == 1
                    )
                )
                t0 = time.monotonic()
                sent = cli_run(
                    ["send", "w1", "task", "T-LIVE", "--text", "implement X", "--state-dir", self.state_dir, "--json"],
                    self.state_dir,
                )
                self.assertEqual(sent.returncode, 0, sent.stderr)
                sent_doc = json.loads(sent.stdout)
                self.assertTrue(sent_doc["agent_listening"])
                mid = sent_doc["message"]["id"]

                out, err = wait_proc.communicate(timeout=5.0)
                elapsed = time.monotonic() - t0
                self.assertEqual(wait_proc.returncode, 0, err)
                self.assertLess(elapsed, 2.5)
                wdoc = json.loads(out)
                self.assertEqual(wdoc["messages"][0]["id"], mid)
            finally:
                if wait_proc.poll() is None:
                    wait_proc.terminate()
                    wait_proc.wait(timeout=2.0)

        # Send mid-task steer while agent is working; step must print it FIRST and refuse done until answered.
        steer = cli_run(
            ["steer", "w1", "--task", "T-LIVE", "--text", "check edge case Y", "--state-dir", self.state_dir, "--json"],
            self.state_dir,
        )
        steer_id = json.loads(steer.stdout)["message"]["id"]

        step_res = cli_run(
            [
                "agent", "step", "w1", "start",
                "--task", "T-LIVE", "--note", "started", "--evidence", "local:init",
                "--goal", "implement X", "--doing", "coding", "--next", "test",
                "--ack", mid, "--state-dir", self.state_dir,
            ],
            self.state_dir,
        )
        self.assertEqual(step_res.returncode, 0, step_res.stderr)
        self.assertIn("[STEERING_INPUT]", step_res.stdout)
        self.assertIn("check edge case Y", step_res.stdout)
        self.assertLess(step_res.stdout.index("[STEERING_INPUT]"), step_res.stdout.index("STEP w1"))

        # done refused while task and steer remain unanswered
        done_refused = cli_run(
            [
                "agent", "step", "w1", "done",
                "--task", "T-LIVE", "--note", "done", "--evidence", "local:done",
                "--ack", steer_id, "--state-dir", self.state_dir,
            ],
            self.state_dir,
        )
        self.assertEqual(done_refused.returncode, 2)
        self.assertIn("UNANSWERED_WORK", done_refused.stderr)

        # Agent answers steer, then replies result (which waits for local answer/release).
        ack_steer = cli_run(
            [
                "agent", "reply", "w1", "answer", "T-LIVE",
                "--re", steer_id, "--text", "edge case handled", "--state-dir", self.state_dir,
            ],
            self.state_dir,
        )
        self.assertEqual(ack_steer.returncode, 0, ack_steer.stderr)

        with subprocess.Popen(
            [
                sys.executable,
                os.path.join(TOOLS_DIR, "subdots.py"),
                "agent",
                "reply",
                "w1",
                "result",
                "T-LIVE",
                "--re",
                mid,
                "--ref",
                "rev/fix-1",
                "--text",
                "all tests pass",
                "--timeout",
                "10",
                "--chunk",
                "5",
                "--state-dir",
                self.state_dir,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as reply_proc:
            try:
                # Wait until agent reply shows up in local inbox
                inbox_doc = wait_until(
                    lambda: [
                        m for m in json.loads(cli_run(["inbox", "w1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["messages"]
                        if m["kind"] == "result"
                    ]
                )
                self.assertTrue(inbox_doc)
                res_id = inbox_doc[0]["id"]

                # Local session sends accepting answer -> unblocks reply_proc!
                ans = cli_run(
                    ["send", "w1", "answer", "T-LIVE", "--re", res_id, "--text", "LGTM accepted", "--state-dir", self.state_dir],
                    self.state_dir,
                )
                self.assertEqual(ans.returncode, 0, ans.stderr)
                rout, rerr = reply_proc.communicate(timeout=5.0)
                self.assertEqual(reply_proc.returncode, 0, rerr)
                self.assertIn("LGTM accepted", rout)
            finally:
                if reply_proc.poll() is None:
                    reply_proc.terminate()
                    reply_proc.wait(timeout=2.0)

        # Now step done succeeds cleanly!
        done_ok = cli_run(
            [
                "agent", "step", "w1", "done",
                "--task", "T-LIVE", "--note", "accepted", "--evidence", "rev/fix-1",
                "--state-dir", self.state_dir,
            ],
            self.state_dir,
        )
        self.assertEqual(done_ok.returncode, 0, done_ok.stderr)

    def test_two_phase_reoffer_and_unacked_escalation(self):
        notify_log = os.path.join(self._tmp.name, "notify.log")
        notify_cmd = json.dumps([
            sys.executable,
            "-c",
            f"import pathlib, sys; pathlib.Path({notify_log!r}).write_text(sys.argv[1])",
            "{why}",
        ])
        self.start_hub(["--reoffer-s", "0.1", "--max-offers", "2", "--notify-cmd", notify_cmd])
        reg = self.register_agent("w1")
        rt = sd.read_runtime(self.state_dir)

        sent = cli_run(["send", "w1", "task", "T1", "--text", "must ack", "--state-dir", self.state_dir, "--json"], self.state_dir)
        mid = json.loads(sent.stdout)["message"]["id"]

        # Poll 1: offered (#1)
        code, body = http_raw(
            f"{rt['http_url']}/agent/poll",
            method="POST",
            body={"agent_id": "w1", "epoch": reg["epoch"], "session_token": reg["session_token"], "hold_s": 0.0},
        )
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["messages"][0]["offer_count"], 1)

        # Wait past reoffer_s without acking -> Poll 2 re-offers (#2)
        time.sleep(0.15)
        code, body = http_raw(
            f"{rt['http_url']}/agent/poll",
            method="POST",
            body={"agent_id": "w1", "epoch": reg["epoch"], "session_token": reg["session_token"], "hold_s": 0.0},
        )
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["messages"][0]["offer_count"], 2)

        # Wait past reoffer_s again -> exceeds max_offers=2 -> unacked_failed + notify hook
        time.sleep(0.15)
        code, body = http_raw(
            f"{rt['http_url']}/agent/poll",
            method="POST",
            body={"agent_id": "w1", "epoch": reg["epoch"], "session_token": reg["session_token"], "hold_s": 0.0},
        )
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["messages"], [])
        self.assertEqual(wait_until(lambda: os.path.exists(notify_log) and pathlib.Path(notify_log).read_text()), "UNACKED_DELIVERY_FAILED")


class TestSupervisedExecAndEpochAndWatchdog(HubTestBase):
    def test_supervised_exec_mid_command_interrupt_kills_process_group(self):
        self.start_hub()
        self.register_agent("w1")
        child_pid_file = os.path.join(self._tmp.name, "grandchild.pid")
        script = (
            f"import os, pathlib, subprocess, sys, time; "
            f"p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
            f"pathlib.Path({child_pid_file!r}).write_text(str(p.pid)); "
            f"print('EXEC_READY', flush=True); "
            f"time.sleep(30)"
        )
        with subprocess.Popen(
            [
                sys.executable,
                os.path.join(TOOLS_DIR, "subdots.py"),
                "agent",
                "exec",
                "w1",
                "--heartbeat-s",
                "2",
                "--grace-s",
                "0.5",
                "--state-dir",
                self.state_dir,
                "--",
                sys.executable,
                "-c",
                script,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as exec_proc:
            try:
                gpid_str = wait_until(lambda: os.path.exists(child_pid_file) and pathlib.Path(child_pid_file).read_text().strip(), timeout=5.0)
                self.assertTrue(gpid_str, "grandchild pid file was not written")
                gpid = int(gpid_str)
                self.assertTrue(sd.pid_alive(gpid))

                # Send high-priority interrupt steering mid-command!
                steer = cli_run(
                    ["steer", "w1", "--interrupt", "--text", "abort and switch target", "--state-dir", self.state_dir, "--json"],
                    self.state_dir,
                )
                self.assertEqual(steer.returncode, 0, steer.stderr)

                out, err = exec_proc.communicate(timeout=6.0)
                self.assertEqual(exec_proc.returncode, 130, err)
                self.assertIn("[INTERRUPTED_BY_STEERING]", out)
                self.assertIn("abort and switch target", out)
                # Grandchild in the same process group must also be dead.
                self.assertTrue(wait_until(lambda: not sd.pid_alive(gpid), timeout=3.0))
            finally:
                if exec_proc.poll() is None:
                    exec_proc.terminate()
                    exec_proc.wait(timeout=2.0)

    def test_epoch_fencing_blocks_stale_worker(self):
        self.start_hub()
        reg1 = self.register_agent("w1")
        reg2 = self.register_agent("w1")
        self.assertEqual(reg2["epoch"], reg1["epoch"] + 1)

        stale = cli_run(
            [
                "agent", "heartbeat", "w1",
                "--epoch", str(reg1["epoch"]),
                "--session-token", reg1["session_token"],
                "--state-dir", self.state_dir,
            ],
            self.state_dir,
        )
        self.assertEqual(stale.returncode, 5)
        self.assertIn("STALE_EPOCH", stale.stderr)

    def test_anti_death_watchdog_transitions_stalled_to_dead_and_runs_wake_and_notify(self):
        wake_log = os.path.join(self._tmp.name, "wake.log")
        notify_log = os.path.join(self._tmp.name, "dead_notify.log")
        wake_cmd = json.dumps([
            sys.executable, "-c", f"import pathlib, sys; pathlib.Path({wake_log!r}).write_text(sys.argv[1])", "{why}"
        ])
        notify_cmd = json.dumps([
            sys.executable, "-c", f"import pathlib, sys; pathlib.Path({notify_log!r}).write_text(sys.argv[1])", "{why}"
        ])
        self.start_hub([
            "--lease-ttl", "0.25",
            "--dead-grace", "0.25",
            "--watchdog-interval", "0.08",
            "--wake-backoff", "0.1",
            "--notify-after", "0.2",
            "--wake-cmd", wake_cmd,
            "--notify-cmd", notify_cmd,
        ])
        self.register_agent("w1", lease_ttl=0.25)
        cli_run(["send", "w1", "task", "T1", "--text", "do work", "--state-dir", self.state_dir], self.state_dir)

        dead_state = wait_until(
            lambda: json.loads(cli_run(["status", "w1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"] == "dead",
            timeout=5.0,
        )
        self.assertTrue(dead_state)
        self.assertTrue(wait_until(lambda: os.path.exists(wake_log), timeout=3.0))
        self.assertTrue(wait_until(lambda: os.path.exists(notify_log), timeout=3.0))

        # Reconnecting recovers the agent cleanly
        reg2 = self.register_agent("w1", lease_ttl=10.0)
        self.assertEqual(reg2["agent"]["state"], "registered")


class TestAntiSpinAndMcpAndMirror(HubTestBase):
    def test_anti_spin_command_loop_and_no_progress_and_poll_thrash(self):
        spin_log = os.path.join(self._tmp.name, "spin_notify.log")
        notify_cmd = json.dumps([
            sys.executable,
            "-c",
            f"import pathlib, sys; p = pathlib.Path({spin_log!r}); p.write_text((p.read_text() if p.exists() else '') + sys.argv[1] + '\\n')",
            "{why}",
        ])
        self.start_hub([
            "--spin-cmd-repeats", "3",
            "--spin-step-repeats", "3",
            "--spin-poll-burst", "4",
            "--notify-cmd", notify_cmd,
        ])
        reg = self.register_agent("w1")
        rt = sd.read_runtime(self.state_dir)

        # 1) SPIN_COMMAND_LOOP: 3 identical failing execs
        for _ in range(3):
            ex = cli_run(
                ["agent", "exec", "w1", "--state-dir", self.state_dir, "--", sys.executable, "-c", "import sys; print('boom'); sys.exit(2)"],
                self.state_dir,
            )
            self.assertEqual(ex.returncode, 2)
        self.assertIn("[HARNESS_INTERVENTION]", ex.stdout)
        self.assertIn("SPIN_COMMAND_LOOP", ex.stdout)

        # 2) SPIN_NO_PROGRESS: 3 identical step reports
        for _ in range(3):
            st = cli_run(
                [
                    "agent", "step", "w1", "step",
                    "--task", "T-LOOP", "--note", "trying same thing", "--evidence", "none",
                    "--goal", "fix", "--doing", "looping", "--next", "retry",
                    "--state-dir", self.state_dir,
                ],
                self.state_dir,
            )
            self.assertEqual(st.returncode, 0, st.stderr)
        self.assertIn("SPIN_NO_PROGRESS", st.stdout)

        # 3) SPIN_POLL_THRASH: 4 rapid zero-hold empty polls (after acking prior interventions)
        evs = json.loads(cli_run(["events", "w1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["events"]
        self.assertTrue(any(e["event_type"] == "spin_detected" and e["payload"]["reason"] == "SPIN_COMMAND_LOOP" for e in evs))
        self.assertTrue(any(e["event_type"] == "spin_detected" and e["payload"]["reason"] == "SPIN_NO_PROGRESS" for e in evs))

        reg = self.register_agent("w2")
        for _ in range(4):
            code, body = http_raw(
                f"{rt['http_url']}/agent/poll",
                method="POST",
                body={"agent_id": "w2", "epoch": reg["epoch"], "session_token": reg["session_token"], "hold_s": 0.0},
            )
            self.assertEqual(code, 200)
        self.assertIn("SPIN_POLL_THRASH", json.dumps(json.loads(body)))
        self.assertTrue(wait_until(lambda: os.path.exists(spin_log) and "SPIN_" in pathlib.Path(spin_log).read_text(), timeout=3.0))

    def test_reverse_streamable_http_mcp_and_local_stdio_mcp(self):
        self.start_hub()
        rt = sd.read_runtime(self.state_dir)

        # 1) Local stdio MCP: initialize + tools/list
        stdio_reqs = "\n".join([
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}}),
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
        ]) + "\n"
        res_stdio = cli_run(["mcp-stdio", "--state-dir", self.state_dir], self.state_dir, stdin_text=stdio_reqs)
        self.assertEqual(res_stdio.returncode, 0, res_stdio.stderr)
        lines = [json.loads(x) for x in res_stdio.stdout.splitlines() if x.strip()]
        self.assertEqual(len(lines), 2)
        tool_names = [t["name"] for t in lines[1]["result"]["tools"]]
        self.assertIn("subdots_steer", tool_names)

        # 2) Reverse HTTP MCP: register sub-agent
        code, body = http_raw(
            rt["mcp_url"],
            method="POST",
            body={"jsonrpc": "2.0", "id": 10, "method": "tools/call", "params": {"name": "subdots_register", "arguments": {"agent_id": "mcp-w1", "dot": "DOT-A", "project": "proj"}}},
        )
        self.assertEqual(code, 200)
        reg_payload = json.loads(json.loads(body)["result"]["content"][-1]["text"])
        epoch, tok = reg_payload["epoch"], reg_payload["session_token"]

        # 3) Local stdio MCP sends steering -> Reverse MCP subdots_wait receives piggybacked [STEERING_INPUT]
        send_req = json.dumps({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "subdots_steer", "arguments": {"agent_id": "mcp-w1", "text": "hello via stdio mcp"}},
        }) + "\n"
        res_send = cli_run(["mcp-stdio", "--state-dir", self.state_dir], self.state_dir, stdin_text=send_req)
        self.assertEqual(res_send.returncode, 0, res_send.stderr)

        code, body = http_raw(
            rt["mcp_url"],
            method="POST",
            body={
                "jsonrpc": "2.0", "id": 11, "method": "tools/call",
                "params": {"name": "subdots_wait", "arguments": {"agent_id": "mcp-w1", "epoch": epoch, "session_token": tok, "hold_s": 1.0}},
            },
        )
        self.assertEqual(code, 200)
        texts = "\n".join(c["text"] for c in json.loads(body)["result"]["content"])
        self.assertIn("[STEERING_INPUT]", texts)
        self.assertIn("hello via stdio mcp", texts)

    def test_ops_mirror_compatibility_with_dots_board_and_monitor_and_fifo_safety(self):
        os.makedirs(self.ops_base, exist_ok=True)
        self.start_hub(["--ops-base", self.ops_base])
        self.register_agent("w1", dot="DOT-A", project="proj")
        sent = cli_run(["send", "w1", "task", "T-MIR", "--project", "proj", "--text", "mirrored", "--state-dir", self.state_dir, "--json"], self.state_dir)
        mid = json.loads(sent.stdout)["message"]["id"]

        self.assertEqual(
            cli_run([
                "agent", "step", "w1", "step",
                "--task", "T-MIR", "--note", "working", "--evidence", "local:file",
                "--goal", "mirror", "--doing", "writing", "--next", "reply",
                "--ack", mid, "--state-dir", self.state_dir,
            ], self.state_dir).returncode,
            0,
        )
        # dots.py board reads mirrored live/memory/mail cleanly
        env = dict(os.environ, DOTS_OPS_BASE=self.ops_base, DOTS_AUTO_UPDATE="0", PYTHONDONTWRITEBYTECODE="1")
        board = subprocess.run([sys.executable, os.path.join(TOOLS_DIR, "dots.py"), "board"], env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(board.returncode, 0, board.stderr)
        self.assertIn("DOT-A w1", board.stdout)

        # dots_monitor.py snapshot --no-github reads mirrored live ledger cleanly
        cfg_path = os.path.join(self._tmp.name, "mon.json")
        pathlib.Path(cfg_path).write_text(json.dumps({
            "ledger": {"host": "local", "root": os.path.join(self.ops_base, "live"), "dots": ["DOT-A"]},
            "activity": [],
            "github": {"enabled": False, "repos": []},
        }))
        mon = subprocess.run([sys.executable, os.path.join(TOOLS_DIR, "dots_monitor.py"), "--config", cfg_path, "snapshot", "--no-github", "--json"], capture_output=True, text=True, timeout=15)
        self.assertEqual(mon.returncode, 0, mon.stderr)
        self.assertTrue(any(r["agent"] == "w1" for r in json.loads(mon.stdout)["dots"]["DOT-A"]["agents"]))

        # Planting a FIFO in .dots-ops/ must never wedge or crash the Hub
        fifo_path = os.path.join(self.ops_base, "proj", "mail", "fifo-agent", "in.jsonl")
        os.makedirs(os.path.dirname(fifo_path), exist_ok=True)
        os.mkfifo(fifo_path)
        fifo_send = cli_run(["send", "fifo-agent", "task", "T-F", "--project", "proj", "--text", "safe", "--state-dir", self.state_dir], self.state_dir, timeout=5.0)
        self.assertEqual(fifo_send.returncode, 0, fifo_send.stderr)


if __name__ == "__main__":
    unittest.main()
