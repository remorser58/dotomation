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
                "--task", "T-MIR", "--note", "working", "--evidence", "local:file token=ghp_MirSecret1234567",
                "--goal", "mirror", "--doing", "writing", "--next", "reply",
                "--decision", "use WAL",
                "--ack", mid, "--state-dir", self.state_dir,
            ], self.state_dir).returncode,
            0,
        )
        # dots.py board reads mirrored live/memory/mail cleanly without NO-LIVE / STALLED
        env = dict(os.environ, DOTS_OPS_BASE=self.ops_base, DOTS_AUTO_UPDATE="0", PYTHONDONTWRITEBYTECODE="1")
        board = subprocess.run([sys.executable, os.path.join(TOOLS_DIR, "dots.py"), "board"], env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(board.returncode, 0, board.stderr)
        self.assertIn("DOT-A w1", board.stdout)
        self.assertIn("T-MIR", board.stdout)
        self.assertNotIn("NO-LIVE", board.stdout)
        self.assertNotIn("STALLED", board.stdout)

        mem_doc = json.loads(pathlib.Path(os.path.join(self.ops_base, "memory", "DOT-A", "w1.json")).read_text(encoding="utf-8"))
        self.assertEqual(mem_doc["decisions"][0]["text"], "use WAL")
        live_txt = pathlib.Path(os.path.join(self.ops_base, "live", "DOT-A", "w1.jsonl")).read_text(encoding="utf-8")
        live_obj = json.loads(live_txt.strip().splitlines()[-1])
        self.assertIsInstance(live_obj["evidence"], str)
        self.assertNotIn("ghp_MirSecret1234567", live_obj["evidence"])
        self.assertIn("[REDACTED]", live_obj["evidence"])

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


class TestAuditRegressionsAndHardening(HubTestBase):
    def test_fast_exec_and_post_exit_interrupt_not_stolen(self):
        self.start_hub()
        self.register_agent("w1")
        t0 = time.monotonic()
        ex = cli_run(
            ["agent", "exec", "w1", "--state-dir", self.state_dir, "--", sys.executable, "-c", "print('fast')"],
            self.state_dir,
        )
        elapsed = time.monotonic() - t0
        self.assertEqual(ex.returncode, 0, ex.stderr)
        self.assertLess(elapsed, 1.0)

        # Immediately send an interrupt after exec exited: must NOT be stolen into 'offered' by an orphan watcher.
        sr = cli_run(["steer", "w1", "--interrupt", "--text", "stop now", "--state-dir", self.state_dir, "--json"], self.state_dir)
        self.assertEqual(sr.returncode, 0, sr.stderr)
        w = cli_run(["agent", "wait", "w1", "--timeout", "1.0", "--chunk", "0.5", "--state-dir", self.state_dir, "--json"], self.state_dir)
        self.assertEqual(w.returncode, 0, w.stderr)
        msgs = json.loads(w.stdout)["messages"]
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["kind"], "interrupt")

    def test_prequeued_interrupt_and_pause_block_exec_and_reply_wakes_on_steer(self):
        self.start_hub()
        reg = self.register_agent("w1")
        rt = sd.read_runtime(self.state_dir)
        marker = os.path.join(self._tmp.name, "should_not_run.txt")

        # Pre-queued interrupt prevents child spawn in agent exec
        cli_run(["steer", "w1", "--interrupt", "--text", "abort before start", "--state-dir", self.state_dir], self.state_dir)
        ex = cli_run(
            [
                "agent", "exec", "w1", "--state-dir", self.state_dir, "--",
                sys.executable, "-c", f"import pathlib; pathlib.Path({marker!r}).write_text('ran')",
            ],
            self.state_dir,
        )
        self.assertEqual(ex.returncode, 130)
        self.assertFalse(os.path.exists(marker))

        # Reply waiting on question wakes immediately when human sends steer (even without --re)
        reply_proc = subprocess.Popen(
            [
                sys.executable, os.path.join(TOOLS_DIR, "subdots.py"),
                "agent", "reply", "w1", "question", "T-ASK", "--text", "need guidance", "--ref", "file:x",
                "--chunk", "10.0", "--timeout", "10.0", "--state-dir", self.state_dir, "--json",
            ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(lambda: reply_proc.kill() if reply_proc.poll() is None else None)
        self.assertTrue(
            wait_until(lambda: json.loads(cli_run(["status", "w1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["active_poll_count"] > 0)
        )
        t0 = time.monotonic()
        cli_run(["steer", "w1", "--text", "pivot immediately", "--state-dir", self.state_dir], self.state_dir)
        out, err = reply_proc.communicate(timeout=5.0)
        self.assertEqual(reply_proc.returncode, 0, err)
        self.assertLess(time.monotonic() - t0, 2.0)
        self.assertIn("pivot immediately", out)

        # Pause blocks exec_start and watchdog dead transition (when lease expires)
        cli_run(["pause", "w1", "--state-dir", self.state_dir], self.state_dir)
        code, body = http_raw(
            f"{rt['http_url']}/agent/exec/start",
            method="POST",
            body={"agent_id": "w1", "epoch": reg["epoch"], "session_token": reg["session_token"], "cmd": "echo hi"},
        )
        self.assertEqual(code, 409)
        self.assertEqual(json.loads(body)["code"], "AGENT_PAUSED")

    def test_paused_watchdog_exempt_completed_rearms_for_task2(self):
        self.start_hub([
            "--lease-ttl", "0.2",
            "--dead-grace", "0.2",
            "--watchdog-interval", "0.08",
        ])
        reg1 = self.register_agent("p1", lease_ttl=0.2)
        cli_run(["pause", "p1", "--state-dir", self.state_dir], self.state_dir)

        reg2 = self.register_agent("c1", lease_ttl=0.2)
        t1 = json.loads(cli_run(["send", "c1", "task", "T1", "--text", "first", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        cli_run(["agent", "reply", "c1", "result", "T1", "--re", t1, "--text", "done1", "--no-wait", "--state-dir", self.state_dir], self.state_dir)
        cli_run([
            "agent", "step", "c1", "done", "--task", "T1", "--note", "finished", "--evidence", "ok",
            "--goal", "g", "--doing", "d", "--next", "none", "--state-dir", self.state_dir,
        ], self.state_dir)
        # Queue Task 2 while c1 is completed; watchdog must catch it and mark dead once lease lapses
        cli_run(["send", "c1", "task", "T2", "--text", "second", "--state-dir", self.state_dir], self.state_dir)
        self.assertTrue(
            wait_until(lambda: json.loads(cli_run(["status", "c1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"] == "dead", timeout=5.0)
        )
        # Paused p1 must STILL be paused (never escalated to dead)
        p1_state = json.loads(cli_run(["status", "p1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"]
        self.assertEqual(p1_state, "paused")

    def test_spin_hardening_ack_note_and_cancel_scope(self):
        self.start_hub(["--spin-cmd-repeats", "3", "--spin-poll-burst", "3"])
        reg = self.register_agent("w1")
        rt = sd.read_runtime(self.state_dir)

        # Volatile timestamps & durations must still trip SPIN_COMMAND_LOOP
        spin_script = "import datetime, time, sys; print(datetime.datetime.utcnow().isoformat() + f'Z fail in {time.monotonic():.3f}s'); sys.exit(3)"
        for _ in range(3):
            ex = cli_run(
                ["agent", "exec", "w1", "--state-dir", self.state_dir, "--", sys.executable, "-c", spin_script],
                self.state_dir,
            )
            self.assertEqual(ex.returncode, 3)
            time.sleep(0.02)
        self.assertIn("SPIN_COMMAND_LOOP", ex.stdout)
        # spinning state must persist across agent step until human steers
        st = cli_run([
            "agent", "step", "w1", "step", "--task", "T", "--note", "still trying", "--evidence", "log",
            "--goal", "g", "--doing", "d", "--next", "n", "--state-dir", self.state_dir, "--json",
        ], self.state_dir)
        self.assertEqual(json.loads(st.stdout)["agent"]["state"], "spinning")
        cli_run(["steer", "w1", "--text", "try option B", "--state-dir", self.state_dir], self.state_dir)
        self.assertEqual(json.loads(cli_run(["status", "w1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"], "working")

        # Poll thrash history preserved across interrupt_only poll
        reg2 = self.register_agent("w2")
        for _ in range(2):
            http_raw(f"{rt['http_url']}/agent/poll", method="POST", body={"agent_id": "w2", "epoch": reg2["epoch"], "session_token": reg2["session_token"], "hold_s": 0.0})
        http_raw(f"{rt['http_url']}/agent/poll", method="POST", body={"agent_id": "w2", "epoch": reg2["epoch"], "session_token": reg2["session_token"], "hold_s": 0.0, "interrupt_only": True})
        code, body = http_raw(f"{rt['http_url']}/agent/poll", method="POST", body={"agent_id": "w2", "epoch": reg2["epoch"], "session_token": reg2["session_token"], "hold_s": 0.0})
        self.assertEqual(code, 200)
        self.assertIn("SPIN_POLL_THRASH", body)

        # ack/note with --re must NOT allow tick done; cancel only cancels older messages
        reg3 = self.register_agent("w3")
        t1 = json.loads(cli_run(["send", "w3", "task", "T1", "--text", "m1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        cli_run(["agent", "reply", "w3", "note", "T1", "--re", t1, "--text", "working on it", "--state-dir", self.state_dir], self.state_dir)
        done_fail = cli_run([
            "agent", "step", "w3", "done", "--task", "T1", "--note", "d", "--evidence", "e",
            "--goal", "g", "--doing", "d", "--next", "n", "--state-dir", self.state_dir,
        ], self.state_dir)
        self.assertEqual(done_fail.returncode, 2)

        old_m = json.loads(cli_run(["send", "w3", "task", "T-OLD", "--text", "old", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        cancel_m = json.loads(cli_run(["cancel", "w3", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        new_m = json.loads(cli_run(["send", "w3", "task", "T-NEW", "--text", "new", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        cli_run(["agent", "ack", "w3", cancel_m, "--state-dir", self.state_dir], self.state_dir)
        rec = json.loads(cli_run(["status", "w3", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)
        by_id = {m["id"]: m["delivery_state"] for m in rec["recent_messages"]}
        self.assertEqual(by_id[old_m], "cancelled")
        self.assertIn(by_id[new_m], ("queued", "offered"))

    def test_http_chunked_public_host_and_security_and_mcp_stdio_concurrency(self):
        import http.client

        self.start_hub(["--public-host", "bridge.example.test:8443", "--ops-base", self.ops_base])
        rt = sd.read_runtime(self.state_dir)

        # hub start redacts token unless --show-token
        hs = cli_run(["hub", "status", "--state-dir", self.state_dir, "--json"], self.state_dir)
        self.assertIn("/<token>", json.loads(hs.stdout)["http_url"])
        hs_raw = cli_run(["hub", "status", "--state-dir", self.state_dir, "--show-token", "--json"], self.state_dir)
        self.assertIn(rt["token"], json.loads(hs_raw.stdout)["http_url"])

        # --public-host allowed in Host, Origin, and reflected in PRM resource
        base = f"http://127.0.0.1:{rt['http_port']}"
        code, body = http_raw(
            f"{base}/.well-known/oauth-protected-resource/{rt['token']}/mcp",
            headers={"Host": "bridge.example.test:8443", "Origin": "https://bridge.example.test"},
        )
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["resource"], f"https://bridge.example.test:8443/{rt['token']}/mcp")

        # HTTP Transfer-Encoding: chunked works cleanly; non-JSON Content-Type returns 415
        conn = http.client.HTTPConnection("127.0.0.1", rt["http_port"], timeout=5.0)
        try:
            payload = json.dumps({"agent_id": "ch1", "dot": "DOT-A", "project": "proj"}).encode("utf-8")
            chunked_wire = f"{len(payload):x}\r\n".encode("ascii") + payload + b"\r\n0\r\n\r\n"
            conn.putrequest("POST", f"/{rt['token']}/agent/register")
            conn.putheader("Content-Type", "application/json")
            conn.putheader("Transfer-Encoding", "chunked")
            conn.endheaders(chunked_wire)
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            reg = json.loads(resp.read().decode("utf-8"))
        finally:
            conn.close()

        code415, _ = http_raw(
            f"{rt['http_url']}/agent/heartbeat",
            method="POST",
            body=b"a=1",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        self.assertEqual(code415, 415)

        # Path traversal in agent_reply project rejected
        code_trav, _ = http_raw(
            f"{rt['http_url']}/agent/reply",
            method="POST",
            body={
                "agent_id": "ch1", "epoch": reg["epoch"], "session_token": reg["session_token"],
                "kind": "note", "task": "t", "text": "x", "project": "../../escape",
            },
        )
        self.assertEqual(code_trav, 400)

        # Symlink in .dots-ops/ mail folder cannot write outside ops_base, and secrets are redacted
        escape_dir = os.path.join(self._tmp.name, "outside")
        os.makedirs(escape_dir, exist_ok=True)
        os.makedirs(os.path.join(self.ops_base, "proj", "mail"), exist_ok=True)
        os.symlink(escape_dir, os.path.join(self.ops_base, "proj", "mail", "sym-agent"))
        cli_run(["send", "sym-agent", "task", "T-SYM", "--project", "proj", "--text", "Authorization: Bearer supersecrettoken123", "--state-dir", self.state_dir], self.state_dir)
        self.assertFalse(os.path.exists(os.path.join(escape_dir, "in.jsonl")))

        # Valid mirrored mail redacts secrets, render_argv strips leading '-' & secrets, and locked flock does not freeze Hub
        cli_run(["send", "ch1", "task", "T-SEC", "--project", "proj", "--text", "token=ghp_SecretValue1234567890", "--state-dir", self.state_dir], self.state_dir)
        mail_in = os.path.join(self.ops_base, "proj", "mail", "ch1", "in.jsonl")
        mail_txt = pathlib.Path(mail_in).read_text(encoding="utf-8")
        self.assertNotIn("ghp_SecretValue1234567890", mail_txt)
        self.assertIn("[REDACTED]", mail_txt)

        rendered = sd.render_argv(["echo", "{task}"], {"task": "--flag token=ghp_SecretValue1234567890"})
        self.assertIsNotNone(rendered)
        self.assertFalse(rendered[1].startswith("-"))
        self.assertNotIn("ghp_SecretValue1234567890", rendered[1])

        import fcntl
        lock_fh = open(mail_in, "ab")
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            t0 = time.monotonic()
            locked_send = cli_run(["send", "ch1", "note", "T-LOCK", "--project", "proj", "--text", "nonblocking", "--state-dir", self.state_dir], self.state_dir, timeout=3.0)
            self.assertEqual(locked_send.returncode, 0, locked_send.stderr)
            self.assertLess(time.monotonic() - t0, 1.5)
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
            lock_fh.close()

        # NaN numeric value returns 400 INVALID_INPUT
        code_nan, body_nan = http_raw(
            f"{rt['http_url']}/agent/poll",
            method="POST",
            body=b'{"agent_id":"ch1","epoch":1,"session_token":"x","hold_s":NaN}',
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(code_nan, 400)
        self.assertEqual(json.loads(body_nan)["code"], "INVALID_INPUT")

        # Reverse MCP tools/list hints & batch caps (no >20 items, no subdots_wait in batch)
        code_tl, body_tl = http_raw(
            rt["mcp_url"],
            method="POST",
            body={"jsonrpc": "2.0", "id": 100, "method": "tools/list", "params": {}},
        )
        self.assertEqual(code_tl, 200)
        rev_tools = {t["name"]: t["annotations"] for t in json.loads(body_tl)["result"]["tools"]}
        self.assertFalse(rev_tools["subdots_wait"]["readOnlyHint"])
        self.assertFalse(rev_tools["subdots_wait"]["idempotentHint"])
        self.assertFalse(rev_tools["subdots_heartbeat"]["readOnlyHint"])
        self.assertFalse(rev_tools["subdots_heartbeat"]["idempotentHint"])

        code_b21, _ = http_raw(
            rt["mcp_url"],
            method="POST",
            body=[{"jsonrpc": "2.0", "id": i, "method": "ping"} for i in range(21)],
        )
        self.assertEqual(code_b21, 400)
        code_bw, _ = http_raw(
            rt["mcp_url"],
            method="POST",
            body=[{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "subdots_wait", "arguments": {}}}],
        )
        self.assertEqual(code_bw, 400)

        # ask CLI + events --follow E2E
        ask_proc = subprocess.Popen(
            [sys.executable, os.path.join(TOOLS_DIR, "subdots.py"), "ask", "ch1", "--task", "T-ASK-E2E", "--text", "ok?", "--timeout", "5.0", "--state-dir", self.state_dir, "--json"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(lambda: ask_proc.kill() if ask_proc.poll() is None else None)
        ask_mid = wait_until(
            lambda: next(
                (m["id"] for m in json.loads(cli_run(["status", "ch1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["recent_messages"]
                 if m["task"] == "T-ASK-E2E" and m["direction"] == "to_agent"),
                None,
            )
        )
        self.assertIsNotNone(ask_mid)
        rep = cli_run(
            [
                "agent", "reply", "ch1", "answer", "T-ASK-E2E", "--re", ask_mid, "--text", "yes ok",
                "--epoch", str(reg["epoch"]), "--session-token", reg["session_token"],
                "--state-dir", self.state_dir,
            ],
            self.state_dir,
        )
        self.assertEqual(rep.returncode, 0, rep.stderr)
        ask_out, ask_err = ask_proc.communicate(timeout=5.0)
        self.assertEqual(ask_proc.returncode, 0, ask_err)
        self.assertEqual(json.loads(ask_out)["reply"]["text"], "yes ok")

        ev_follow = cli_run(["events", "ch1", "--follow", "--timeout", "0.2", "--state-dir", self.state_dir, "--json"], self.state_dir)
        self.assertEqual(ev_follow.returncode, 0, ev_follow.stderr)
        self.assertIn("agent_reply", ev_follow.stdout)

        # Concurrent mcp-stdio: subdots_list_agents responds while subdots_await_reply is blocking
        mcp_proc = subprocess.Popen(
            [sys.executable, os.path.join(TOOLS_DIR, "subdots.py"), "mcp-stdio", "--state-dir", self.state_dir],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(lambda: mcp_proc.kill() if mcp_proc.poll() is None else None)
        assert mcp_proc.stdin is not None and mcp_proc.stdout is not None
        mcp_proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}) + "\n")
        mcp_proc.stdin.flush()
        tl = json.loads(mcp_proc.stdout.readline())
        t_names = {t["name"] for t in tl["result"]["tools"]}
        self.assertTrue({"subdots_agent_status", "subdots_inbox", "subdots_mark_read"}.issubset(t_names))

        mcp_proc.stdin.write(json.dumps({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "subdots_await_reply", "arguments": {"agent_id": "ch1", "re_id": "m-20990101T000000Z-ffff", "timeout_s": 5.0}},
        }) + "\n")
        mcp_proc.stdin.write(json.dumps({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "subdots_list_agents", "arguments": {}},
        }) + "\n")
        mcp_proc.stdin.flush()
        fast_resp = json.loads(mcp_proc.stdout.readline())
        self.assertEqual(fast_resp["id"], 3)
        mcp_proc.stdin.close()
        mcp_proc.stdout.close()
        mcp_proc.stderr.close()
        mcp_proc.terminate()
        mcp_proc.wait(timeout=3.0)

        # --session-file inside /tmp works and wake exits 1 when unconfigured
        sess_tmp = os.path.join(tempfile.gettempdir(), f"subdots-sess-{os.getpid()}.json")
        try:
            r_tmp = cli_run(["agent", "register", "tmp-w", "--session-file", sess_tmp, "--state-dir", self.state_dir], self.state_dir)
            self.assertEqual(r_tmp.returncode, 0, r_tmp.stderr)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(sess_tmp)
        self.assertEqual(cli_run(["wake", "ch1", "--state-dir", self.state_dir], self.state_dir).returncode, 1)

    def test_audit_part2_security_redos_smuggling_watchdog_and_exec_hardening(self):
        import http.client

        # 1) O(N) redaction (no quadratic ReDoS), quoted secret redaction, and Hub token path redaction
        t0 = time.monotonic()
        r_tok = sd.redact("token" * 40000)
        r_pem = sd.redact("-----BEGIN PRIVATE KEY-----" * 2000)
        self.assertLess(time.monotonic() - t0, 0.5)
        self.assertGreater(len(r_tok), 0)
        self.assertIn("[REDACTED_PRIVATE_KEY]", r_pem)

        red_q = sd.redact('{"api_key": "topsecret123"} PASSWORD="mypass" http://127.0.0.1:9999/' + ("a" * 48) + "/mcp")
        self.assertNotIn("topsecret123", red_q)
        self.assertNotIn("mypass", red_q)
        self.assertNotIn("a" * 48, red_q)

        # render_argv strips shell metacharacters from {task}
        rav = sd.render_argv(["cmd", "{task}"], {"task": "--bad $(id); rm -rf"})
        self.assertIsNotNone(rav)
        self.assertFalse(rav[1].startswith("-"))
        self.assertNotIn("$(", rav[1])
        self.assertNotIn(";", rav[1])

        # Non-loopback cleartext http rejected
        with self.assertRaises(ValueError):
            sd.http_post("http://remote.example.com/tok", "/agent/heartbeat", {})

        # 2) Completed agent after answer/release stays completed; paused stays paused after prior acked resume
        self.start_hub(["--lease-ttl", "0.2", "--dead-grace", "0.2", "--watchdog-interval", "0.08"])
        rt = sd.read_runtime(self.state_dir)

        # Agent registered without --lease-ttl inherits Hub's custom 0.2s TTL
        reg_inh = self.register_agent("inh-w")
        self.assertAlmostEqual(reg_inh["lease_ttl_s"], 0.2, places=2)

        reg_done = self.register_agent("done-w", lease_ttl=0.2)
        t_id = json.loads(cli_run(["send", "done-w", "task", "T1", "--text", "do 1", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        cli_run(["agent", "reply", "done-w", "result", "T1", "--re", t_id, "--text", "r1", "--no-wait", "--state-dir", self.state_dir], self.state_dir)
        cli_run(["agent", "step", "done-w", "done", "--task", "T1", "--note", "ok", "--evidence", "ok", "--goal", "g", "--doing", "d", "--next", "n", "--state-dir", self.state_dir], self.state_dir)
        # Operator sends terminal answer & release (neither gets answered by agent_reply --re)
        cli_run(["send", "done-w", "answer", "T1", "--text", "LGTM", "--state-dir", self.state_dir], self.state_dir)
        cli_run(["release", "done-w", "--state-dir", self.state_dir], self.state_dir)

        # Paused agent after an earlier acked resume stays paused even past lease expiry
        self.register_agent("pr-w", lease_ttl=0.2)
        res_m = json.loads(cli_run(["resume", "pr-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        cli_run(["agent", "ack", "pr-w", res_m, "--state-dir", self.state_dir], self.state_dir)
        cli_run(["pause", "pr-w", "--state-dir", self.state_dir], self.state_dir)

        time.sleep(0.6)
        st_done = json.loads(cli_run(["status", "done-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"]
        st_pr = json.loads(cli_run(["status", "pr-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"]
        self.assertEqual(st_done, "completed")
        self.assertEqual(st_pr, "paused")

        # 3) Recovering dead agent via exec_start resets state to working
        self.register_agent("rec-w", lease_ttl=0.15)
        cli_run(["send", "rec-w", "task", "T-REC", "--text", "go", "--state-dir", self.state_dir], self.state_dir)
        self.assertTrue(wait_until(lambda: json.loads(cli_run(["status", "rec-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"] == "dead", timeout=3.0))
        ex_rec = cli_run(["agent", "exec", "rec-w", "--state-dir", self.state_dir, "--", sys.executable, "-c", "print('ok')"], self.state_dir)
        self.assertEqual(ex_rec.returncode, 0, ex_rec.stderr)
        self.assertEqual(json.loads(cli_run(["status", "rec-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["state"], "working")

        # 4) exec_start --ack <cancel_id> unblocks exec cleanly
        c_mid = json.loads(cli_run(["cancel", "rec-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["message"]["id"]
        ex_unblock = cli_run(["agent", "exec", "rec-w", "--ack", c_mid, "--state-dir", self.state_dir, "--", sys.executable, "-c", "print('unblocked')"], self.state_dir)
        self.assertEqual(ex_unblock.returncode, 0, ex_unblock.stderr)

        # Pre-queued interrupt=True with kind="task" aborts exec before child runs
        marker2 = os.path.join(self._tmp.name, "no_task_intr.txt")
        cli_run(["send", "rec-w", "task", "T-INTR", "--interrupt", "--text", "abort!", "--state-dir", self.state_dir], self.state_dir)
        ex_abort = cli_run(["agent", "exec", "rec-w", "--state-dir", self.state_dir, "--", sys.executable, "-c", f"import pathlib; pathlib.Path({marker2!r}).write_text('bad')"], self.state_dir)
        self.assertEqual(ex_abort.returncode, 130)
        self.assertFalse(os.path.exists(marker2))

        # 5) HTTP smuggling, null body, empty MCP batch, non-ASCII auth token
        def raw_http_wire(wire_bytes):
            import socket
            with socket.create_connection(("127.0.0.1", rt["http_port"]), timeout=5.0) as s:
                s.sendall(wire_bytes)
                s.settimeout(2.0)
                return s.recv(4096)

        # Duplicate Content-Length -> 400
        resp_dup = raw_http_wire(f"POST /{rt['token']}/agent/heartbeat HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n{{}}".encode())
        self.assertIn(b"400 ", resp_dup)
        # Empty TE + TE chunked + CL -> 400
        resp_smug = raw_http_wire(f"POST /{rt['token']}/agent/heartbeat HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: \r\nTransfer-Encoding: chunked\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n{{}}".encode())
        self.assertIn(b"400 ", resp_smug)
        # GET with Content-Length -> 400
        resp_getcl = raw_http_wire(f"GET /{rt['token']}/health HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n".encode())
        self.assertIn(b"400 ", resp_getcl)
        # JSON null body -> 400
        resp_null = raw_http_wire(f"POST /{rt['token']}/agent/heartbeat HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 4\r\nContent-Type: application/json\r\n\r\nnull".encode())
        self.assertIn(b"400 ", resp_null)
        # Empty MCP batch [] -> 400
        resp_empty_batch = raw_http_wire(f"POST /{rt['token']}/mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n[]".encode())
        self.assertIn(b"400 ", resp_empty_batch)
        # Non-ASCII Bearer token -> 401 cleanly (no crash)
        resp_nonascii = raw_http_wire(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer \xff\xfe\r\n\r\n")
        self.assertIn(b"401 ", resp_nonascii)

        # 6) agent wait terminates immediately (< 1.0s) on SIGTERM with exit 4
        self.register_agent("sig-w")
        wproc = subprocess.Popen(
            [sys.executable, os.path.join(TOOLS_DIR, "subdots.py"), "agent", "wait", "sig-w", "--timeout", "30", "--chunk", "20", "--state-dir", self.state_dir],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            self.assertTrue(wait_until(lambda: json.loads(cli_run(["status", "sig-w", "--state-dir", self.state_dir, "--json"], self.state_dir).stdout)["agent"]["active_poll_count"] > 0))
            t0_sig = time.monotonic()
            wproc.terminate()
            _, werr = wproc.communicate(timeout=2.0)
            self.assertEqual(wproc.returncode, 4, werr)
            self.assertLess(time.monotonic() - t0_sig, 1.0)
        finally:
            if wproc.poll() is None:
                wproc.kill()
                wproc.communicate()


if __name__ == "__main__":
    unittest.main()
