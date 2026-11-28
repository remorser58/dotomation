#!/usr/bin/env python3
"""Standard-library tests for tools/dots_pilot.py."""

import base64
import fcntl
import hashlib
import json
import os
import pathlib
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from unittest import mock

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS_DIR)
import dots_pilot as dp


def run_pilot(args, pilot_dir, env_extra=None, stdin_text=None):
    cmd = [sys.executable, os.path.join(TOOLS_DIR, "dots_pilot.py"), "--pilot-dir", pilot_dir] + list(args)
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if env_extra:
        env.update(env_extra)
    return subprocess.run(cmd, env=env, input=stdin_text, capture_output=True, text=True, timeout=60)


def _read_json(path):
    return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))


def _write_json(path, obj):
    pathlib.Path(path).write_text(json.dumps(obj, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


class DotsPilotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dots_pilot_test_")
        self.pilot_dir = os.path.join(self.tmp, "pilot")
        self.ops_base = os.path.join(self.tmp, "ops")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_init_presets_permissions_sha_pin_and_doctor(self):
        res = run_pilot([
            "init", "--dot", "DOT-A", "--project", "kice",
            "--ops-base", self.ops_base, "--preset", "research-3track",
        ], self.pilot_dir)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("PILOT_INIT", res.stdout)
        self.assertEqual(stat.S_IMODE(os.stat(self.pilot_dir).st_mode), 0o700)

        cfg_path = os.path.join(self.pilot_dir, "config.json")
        self.assertEqual(stat.S_IMODE(os.stat(cfg_path).st_mode), 0o600)
        cfg = _read_json(cfg_path)
        self.assertEqual(cfg["preset"], "research-3track")
        self.assertEqual([t["track"] for t in cfg["tracks"]], ["research", "ontology", "authoring"])
        expected_sha = hashlib.sha256(pathlib.Path(os.path.join(TOOLS_DIR, "dots.py")).read_bytes()).hexdigest()
        self.assertEqual(cfg["dots_sha256"], expected_sha)

        # Contracts initialized inside ops_base without external AI tokens
        for tr in ("research", "ontology", "authoring"):
            self.assertTrue(os.path.isfile(os.path.join(self.ops_base, "loops", "DOT-A", f"{tr}.contract.json")))

        # Zero personal AI tokens anywhere in config
        cfg_txt = pathlib.Path(cfg_path).read_text(encoding="utf-8")
        self.assertNotIn("sk-", cfg_txt)

        doc = run_pilot(["doctor"], self.pilot_dir)
        self.assertEqual(doc.returncode, 0, doc.stdout + doc.stderr)
        self.assertIn("DOCTOR PASS dots_py_pin", doc.stdout)

        # ux-ui-impl-3track preset initialization works cleanly too
        pilot2 = os.path.join(self.tmp, "pilot2")
        ops2 = os.path.join(self.tmp, "ops2")
        res2 = run_pilot(["init", "--dot", "DOT-B", "--project", "rem", "--ops-base", ops2, "--preset", "ux-ui-impl-3track"], pilot2)
        self.assertEqual(res2.returncode, 0, res2.stderr)
        for tr in ("ux-research", "ui-research", "skill-impl"):
            self.assertTrue(os.path.isfile(os.path.join(ops2, "loops", "DOT-B", f"{tr}.contract.json")))

        # Doctor fails if config.json perms become 0o644
        os.chmod(cfg_path, 0o644)
        bad_doc = run_pilot(["doctor"], self.pilot_dir)
        self.assertEqual(bad_doc.returncode, 2)
        self.assertIn("FAIL config_json", bad_doc.stdout)
        os.chmod(cfg_path, 0o600)

        # Aside validation refuses non-chatgpt dots URLs and non-loopback CDP
        with self.assertRaises(ValueError):
            dp._validate_aside_target("https://evil.example.com/dots/123")
        with self.assertRaises(ValueError):
            dp._validate_cdp_url("http://192.168.1.50:9222")

    def test_nonce_sentinel_rejects_forgery_and_once_dispatches_with_cooldown(self):
        chat_log = os.path.join(self.tmp, "chat.log")
        notify_log = os.path.join(self.tmp, "notify.log")
        chat_cmd = [sys.executable, "-c", "import pathlib, sys; p=pathlib.Path(sys.argv[1]); p.write_text((p.read_text() if p.exists() else '') + sys.argv[2] + '\\n')", chat_log, "{text}"]
        notify_cmd = [sys.executable, "-c", "import pathlib, sys; p=pathlib.Path(sys.argv[1]); p.write_text((p.read_text() if p.exists() else '') + sys.argv[2] + '\\n')", notify_log, "{text}"]

        res = run_pilot([
            "init", "--dot", "DOT-A", "--project", "kice",
            "--ops-base", self.ops_base, "--preset", "generic",
            "--chat-transport", "command", "--chat-cmd", json.dumps(chat_cmd),
            "--notify-mode", "command", "--notify-cmd", json.dumps(notify_cmd),
            "--cooldown-min", "15",
        ], self.pilot_dir)
        self.assertEqual(res.returncode, 0, res.stderr)

        # Direct sentinel without active nonce fails loudly
        forged = run_pilot(["sentinel", "--nonce", "deadbeef", "--kind", "fill", "--dot", "DOT-A"], self.pilot_dir)
        self.assertEqual(forged.returncode, 2)

        # Make the track idle past 15m AND roster underfilled -> both LOOP_IDLE and UNDERFILLED hit fill sentinel cleanly (no missing=project)
        spath = os.path.join(self.ops_base, "loops", "DOT-A", "work.state.json")
        st = _read_json(spath)
        st["last_activity_at"] = "2020-01-01T00:00:00Z"
        _write_json(spath, st)
        _write_json(os.path.join(self.ops_base, "live", "DOT-A", "roster.json"), {
            "coordinator": "lead", "slots_total": 2, "slots_unassigned": 1,
            "actors": [{"agent": "lead", "role": "coordinator", "occupies_slot": True, "slot_state": "working"}],
        })

        # Ensure fill_after_min=0 triggers UNDERFILLED on immediate scan via pre-seeded capacity record
        ws_path = os.path.join(self.pilot_dir, "watch-state.json")
        _write_json(ws_path, {
            "v": 1, "messages": {}, "agents": {}, "roster": {}, "vm": {}, "churn": {},
            "capacity": {"DOT-A": {"since": "2020-01-01T00:00:00Z"}}, "loops": {},
        })

        o1 = run_pilot(["once", "--verbose"], self.pilot_dir)
        self.assertEqual(o1.returncode, 0, o1.stdout + o1.stderr)
        self.assertNotIn("missing=project", o1.stdout)
        self.assertIn("PILOT_TICK dot=DOT-A wakes=2", o1.stdout)
        self.assertFalse(os.path.exists(os.path.join(self.pilot_dir, "active_nonce.json")))
        chat_lines = pathlib.Path(chat_log).read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(chat_lines), 1)
        self.assertIn("PILOT_WAKE dot=DOT-A project=kice", chat_lines[0])
        self.assertIn("LOOP_IDLE", chat_lines[0])
        self.assertIn("UNDERFILLED", chat_lines[0])

        # Second immediate tick respects wake cooldown even if watch-state resets, and posts heartbeat once (not repeated on third tick)
        ws = _read_json(ws_path)
        ws["loops"] = {}
        ws["capacity"] = {"DOT-A": {"since": "2020-01-01T00:00:00Z"}}
        _write_json(ws_path, ws)
        o2 = run_pilot(["once"], self.pilot_dir)
        self.assertEqual(o2.returncode, 0, o2.stderr)
        self.assertIn("PILOT_WAKE_COOLDOWN", o2.stdout)
        lines2 = pathlib.Path(chat_log).read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines2), 2)
        self.assertIn("[PILOT_HEARTBEAT dot=DOT-A project=kice]", lines2[1])
        o3 = run_pilot(["once"], self.pilot_dir)
        self.assertEqual(o3.returncode, 0, o3.stderr)
        self.assertEqual(len(pathlib.Path(chat_log).read_text(encoding="utf-8").splitlines()), 2)

    def test_daemon_heal_opt_in_cooldown_flock_and_webhook_redaction(self):
        sessions_root = os.path.join(self.tmp, "daemon", "sessions", "sess-1")
        os.makedirs(sessions_root, exist_ok=True)
        ev_file = os.path.join(sessions_root, "events.jsonl")
        real_rejection = (
            'Rejected exec call: `cmd`: CreateProcess { message: '
            '"too many retained terminal sessions (limit 64); close one before starting another" }'
        )
        pathlib.Path(ev_file).write_text(json.dumps({
            "ts": dp._now_iso(),
            "type": "tool_result",
            "call": {"outcome": "tool_rejected", "result": {"text": real_rejection}},
        }) + "\n", encoding="utf-8")

        heal_log = os.path.join(self.tmp, "heal.log")
        heal_cmd = [sys.executable, "-c", "import pathlib, sys; p=pathlib.Path(sys.argv[1]); p.write_text((p.read_text() if p.exists() else '') + 'healed\\n')", heal_log]

        res_init = run_pilot([
            "init", "--dot", "DOT-A", "--project", "p",
            "--ops-base", self.ops_base, "--preset", "generic",
        ], self.pilot_dir)
        self.assertEqual(res_init.returncode, 0, res_init.stderr)

        cfg_path = os.path.join(self.pilot_dir, "config.json")
        cfg = _read_json(cfg_path)
        cfg["chat"]["board_heartbeat_min"] = 0
        cfg["daemon_health"] = {
            "enabled": True,
            "heal": False,
            "heal_cmd": heal_cmd,
            "heal_cooldown_min": 15,
            "sessions_dir": os.path.dirname(sessions_root),
            "activity_paths": [],
        }
        _write_json(cfg_path, cfg)

        # Disabled heal never invokes heal_cmd even on retained_limit_hit; empty activity_paths must not fabricate app.log error
        o1 = run_pilot(["once"], self.pilot_dir)
        self.assertEqual(o1.returncode, 0, o1.stdout + o1.stderr)
        self.assertNotIn("PILOT_MONITOR_WARN", o1.stdout)
        self.assertIn("PILOT_DAEMON_LIMIT dot=DOT-A healed=false detail=heal_disabled", o1.stdout)
        self.assertFalse(os.path.exists(heal_log))
        mon_state_path = os.path.join(self.pilot_dir, "monitor-state.json")
        self.assertTrue(os.path.isfile(mon_state_path))
        self.assertEqual(_read_json(mon_state_path)["errors"], [])

        # Enabling heal runs heal_cmd once, then cooldown blocks immediate repeat
        cfg["daemon_health"]["heal"] = True
        _write_json(cfg_path, cfg)
        o2 = run_pilot(["once"], self.pilot_dir)
        self.assertEqual(o2.returncode, 0, o2.stderr)
        self.assertIn("PILOT_DAEMON_LIMIT dot=DOT-A healed=true detail=exit=0", o2.stdout)
        self.assertEqual(pathlib.Path(heal_log).read_text(encoding="utf-8").splitlines(), ["healed"])

        o3 = run_pilot(["once"], self.pilot_dir)
        self.assertIn("PILOT_DAEMON_LIMIT dot=DOT-A healed=false detail=cooldown_active", o3.stdout)
        self.assertEqual(len(pathlib.Path(heal_log).read_text(encoding="utf-8").splitlines()), 1)

        # Overlapping flock holder skips cleanly
        lock_fd = os.open(os.path.join(self.pilot_dir, "pilot.lock"), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            o_locked = run_pilot(["once"], self.pilot_dir)
            self.assertEqual(o_locked.returncode, 0)
            self.assertIn("PILOT_SKIP reason=pilot_locked", o_locked.stdout)
        finally:
            os.close(lock_fd)

        # Webhook notify rejects plaintext HTTP, accepts mock HTTPS, and redacts secrets
        cfg["notify"] = {"mode": "webhook", "webhook_url_env": "TEST_PILOT_HOOK"}
        with mock.patch.dict(os.environ, {"TEST_PILOT_HOOK": "http://insecure.example.com/hook"}):
            ok_w, det_w = dp._send_notification(cfg, "TEST", "body")
            self.assertFalse(ok_w)
            self.assertIn("https://", det_w)

        captured_payload = {}

        class _FakeResp:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): return False

        def fake_urlopen(req, timeout=10):
            captured_payload["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResp()

        with mock.patch.dict(os.environ, {"TEST_PILOT_HOOK": "https://hooks.example.com/services/mock"}):
            with mock.patch.object(dp, "_open_https_no_redirect", side_effect=fake_urlopen):
                ok_s, det_s = dp._send_notification(
                    cfg,
                    "ALERT",
                    "leak sk-secret1234567890 sk-proj-abc1234567890 sk-ant-xyz1234567890 OPENAI_API_KEY=tokval1234 here",
                )
        self.assertTrue(ok_s, det_s)
        body_txt = captured_payload["body"]["text"]
        for leaked in ("sk-secret1234567890", "sk-proj-abc1234567890", "sk-ant-xyz1234567890", "tokval1234"):
            self.assertNotIn(leaked, body_txt)
        self.assertIn("[REDACTED]", body_txt)

        # _validate_webhook_url rejects empty userinfo, bad port, and whitespace; _open_https_no_redirect refuses HTTP 302 redirects
        for bad_u in ("https://user:pass@evil.example/hook", "https://:@evil.example/hook", "https://evil.example:badport/hook", "https://evil.example/\nhook"):
            ok_u, _ = dp._validate_webhook_url(bad_u)
            self.assertFalse(ok_u, bad_u)

        redir_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        redir_sock.bind(("127.0.0.1", 0))
        redir_sock.listen(2)
        redir_port = redir_sock.getsockname()[1]

        def serve_redir():
            with contextlib.suppress(OSError):
                conn, _ = redir_sock.accept()
                with conn:
                    conn.recv(4096)
                    conn.sendall(
                        b"HTTP/1.1 302 Found\r\nLocation: https://evil.example/stolen\r\nContent-Length: 0\r\n\r\n"
                    )

        import contextlib
        rth = threading.Thread(target=serve_redir, daemon=True)
        rth.start()
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{redir_port}/hook", data=b"{}", method="POST")
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                dp._open_https_no_redirect(req, timeout=3)
            self.assertEqual(ctx.exception.code, 302)
            self.assertIn("redirect refused", str(ctx.exception))
        finally:
            redir_sock.close()
            rth.join(timeout=2.0)

    def test_render_service_and_loopback_cdp_aside(self):
        run_pilot(["init", "--dot", "REM17", "--project", "p", "--ops-base", self.ops_base], self.pilot_dir)
        plist_res = run_pilot(["render-service", "--target", "launchd", "--interval", "120"], self.pilot_dir)
        self.assertEqual(plist_res.returncode, 0, plist_res.stderr)
        root = ET.fromstring(plist_res.stdout)
        self.assertEqual(root.tag, "plist")
        self.assertIn("com.dotomation.pilot.rem17", plist_res.stdout)

        sysd_res = run_pilot(["render-service", "--target", "systemd", "--interval", "120"], self.pilot_dir)
        self.assertEqual(sysd_res.returncode, 0, sysd_res.stderr)
        self.assertIn("[Unit]", sysd_res.stdout)
        self.assertIn("[Service]", sysd_res.stdout)
        self.assertIn("run --interval 120", sysd_res.stdout)

        # Mock loopback CDP server (/json/list + minimal RFC 6455 WebSocket upgrade + Runtime.evaluate)
        received_rpc = {}
        srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv_sock.bind(("127.0.0.1", 0))
        srv_sock.listen(4)
        port = srv_sock.getsockname()[1]
        stop_flag = threading.Event()

        def serve():
            srv_sock.settimeout(0.2)
            while not stop_flag.is_set():
                try:
                    conn, _ = srv_sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                with conn:
                    conn.settimeout(2.0)
                    data = b""
                    while b"\r\n\r\n" not in data:
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        data += chunk
                    head = data.decode("latin-1")
                    if head.startswith("GET /json/list "):
                        body = json.dumps([{
                            "type": "page",
                            "url": "https://chatgpt.com/dots/test-room",
                            "webSocketDebuggerUrl": f"ws://127.0.0.1:{port}/devtools/page/1",
                        }]).encode("utf-8")
                        conn.sendall(
                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                            + str(len(body)).encode("ascii") + b"\r\n\r\n" + body
                        )
                    elif "Upgrade: websocket" in head:
                        key_line = [ln for ln in head.splitlines() if ln.lower().startswith("sec-websocket-key:")][0]
                        ws_key = key_line.split(":", 1)[1].strip()
                        accept = base64.b64encode(
                            hashlib.sha1((ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
                        ).decode("ascii")
                        conn.sendall((
                            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                            f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                        ).encode("ascii"))
                        buf = bytearray(data.split(b"\r\n\r\n", 1)[1])
                        op, payload = dp._ws_recv_frame(conn, buf)
                        received_rpc["op"] = op
                        received_rpc["msg"] = json.loads(payload.decode("utf-8"))
                        reply = json.dumps({"id": 1, "result": {"result": {"value": {"ok": True, "method": "button"}}}}).encode("utf-8")
                        hdr = bytearray([0x81])
                        if len(reply) < 126:
                            hdr.append(len(reply))
                        else:
                            hdr.append(126)
                            hdr.extend(len(reply).to_bytes(2, "big"))
                        conn.sendall(bytes(hdr) + reply)

        th = threading.Thread(target=serve, daemon=True)
        th.start()
        try:
            out = dp._cdp_send_aside(f"http://127.0.0.1:{port}", "https://chatgpt.com/dots/test-room", "Wake REM17 tracks", timeout=5)
            self.assertEqual(out["ok"], True)
            self.assertEqual(received_rpc["msg"]["method"], "Runtime.evaluate")
            self.assertIn("Wake REM17 tracks", received_rpc["msg"]["params"]["expression"])
        finally:
            stop_flag.set()
            srv_sock.close()
            th.join(timeout=2.0)

    def test_config_inherit_loop_status_monitor_pin_symlink_and_nonce_security(self):
        # 1) init --config inherits dot/project/preset/keep_full and pins monitor_sha256
        example_cfg = os.path.join(TOOLS_DIR, "dots-pilot.example.json")
        mon_copy = os.path.join(self.tmp, "mon_copy.py")
        shutil.copy2(os.path.join(TOOLS_DIR, "dots_monitor.py"), mon_copy)
        expected_mon_sha = hashlib.sha256(pathlib.Path(mon_copy).read_bytes()).hexdigest()

        tmp_pilot_ctx = None
        pilot_path = self.pilot_dir
        if os.path.isdir("/tmp"):
            tmp_pilot_ctx = tempfile.TemporaryDirectory(dir="/tmp", prefix="dots_pilot_sys_")
            pilot_path = os.path.join(tmp_pilot_ctx.name, "pilot")
        try:
            res = run_pilot([
                "init", "--config", example_cfg,
                "--ops-base", self.ops_base,
                "--dots-monitor-py", mon_copy,
            ], pilot_path)
            self.assertEqual(res.returncode, 0, res.stdout + res.stderr)

            resolved_pilot = os.path.join(os.path.realpath(tmp_pilot_ctx.name), "pilot") if tmp_pilot_ctx else pilot_path
            cfg_path = os.path.join(resolved_pilot, "config.json")
            cfg = _read_json(cfg_path)
            self.assertEqual(cfg["dot"], "DOT-A")
            self.assertEqual(cfg["project"], "example-proj")
            self.assertEqual(cfg["preset"], "research-3track")
            self.assertTrue(cfg["keep_full"])
            self.assertEqual(cfg["monitor_sha256"], expected_mon_sha)

            # macOS /tmp parent symlink resolves cleanly under doctor while leaf symlink fails
            doc = run_pilot(["doctor"], pilot_path)
            self.assertEqual(doc.returncode, 0, doc.stdout + doc.stderr)
            self.assertIn("DOCTOR PASS dots_monitor_py_pin", doc.stdout)

            leaf_link = os.path.join(self.tmp, "pilot_link")
            os.symlink(resolved_pilot, leaf_link)
            bad_leaf_doc = run_pilot(["doctor"], leaf_link)
            self.assertEqual(bad_leaf_doc.returncode, 2)
            self.assertIn("FAIL pilot_dir", bad_leaf_doc.stdout)

            # 2) once --verbose executes positional `loop status DOT-A --json`, emits PILOT_HEARTBEAT, omits nonce from argv
            o1 = run_pilot(["once", "--verbose"], pilot_path)
            self.assertEqual(o1.returncode, 0, o1.stdout + o1.stderr)
            self.assertIn("PILOT_TICK dot=DOT-A", o1.stdout)
            self.assertIn("tracks=3", o1.stdout)
            self.assertNotIn("PILOT_LOOP_STATUS_WARN", o1.stdout)
            self.assertIn("[PILOT_HEARTBEAT dot=DOT-A project=example-proj]", o1.stdout)
            self.assertNotIn("--nonce", o1.stdout)

            # status --json reports all 3 initialized tracks and monitor_pin_ok=True
            st_proc = run_pilot(["status", "--json"], pilot_path)
            self.assertEqual(st_proc.returncode, 0, st_proc.stderr)
            st_doc = json.loads(st_proc.stdout)
            self.assertEqual(len(st_doc["tracks"]), 3)
            self.assertTrue(st_doc["monitor_pin_ok"])

            # 3) consecutive_blocks >= 3 triggers TRACK_BLOCKED notification
            notify_log = os.path.join(self.tmp, "blocked_notify.log")
            notify_cmd = [sys.executable, "-c", "import pathlib, sys; p=pathlib.Path(sys.argv[1]); p.write_text((p.read_text() if p.exists() else '') + sys.argv[2] + '\\n')", notify_log, "{text}"]
            cfg["notify"] = {"mode": "command", "command": notify_cmd}
            _write_json(cfg_path, cfg)
            rstate_path = os.path.join(self.ops_base, "loops", "DOT-A", "research.state.json")
            rstate = _read_json(rstate_path)
            rstate["consecutive_blocks"] = 3
            _write_json(rstate_path, rstate)
            o_blk = run_pilot(["once"], pilot_path)
            self.assertEqual(o_blk.returncode, 0, o_blk.stdout + o_blk.stderr)
            self.assertIn("TRACK_BLOCKED DOT-A/research", pathlib.Path(notify_log).read_text(encoding="utf-8"))

            # 4) Tampered or symlinked dots_monitor.py triggers pin failure and is NOT executed
            pathlib.Path(mon_copy).write_text(pathlib.Path(mon_copy).read_text(encoding="utf-8") + "\nraise RuntimeError('TAMPERED')\n", encoding="utf-8")
            bad_mon_doc = run_pilot(["doctor"], pilot_path)
            self.assertEqual(bad_mon_doc.returncode, 2)
            self.assertIn("FAIL dots_monitor_py_pin", bad_mon_doc.stdout)
            o_tamper = run_pilot(["once"], pilot_path)
            self.assertEqual(o_tamper.returncode, 0, o_tamper.stdout + o_tamper.stderr)
            self.assertIn("PILOT_MONITOR_WARN", o_tamper.stdout)
            self.assertIn("sha256 mismatch", o_tamper.stdout)
            self.assertNotIn("TAMPERED", o_tamper.stdout + o_tamper.stderr)

            mon_link = os.path.join(self.tmp, "mon_link.py")
            os.symlink(mon_copy, mon_link)
            cfg["dots_monitor_py"] = mon_link
            _write_json(cfg_path, cfg)
            o_link = run_pilot(["once"], pilot_path)
            self.assertIn("PILOT_MONITOR_WARN", o_link.stdout)
            self.assertIn("refusing symlink", o_link.stdout)

            # 5) Expired nonce & dead owner PID are rejected even with matching nonce string
            nonce_path = os.path.join(resolved_pilot, "active_nonce.json")
            _write_json(nonce_path, {"nonce": "abc12345", "t": "2020-01-01T00:00:00Z", "pid": os.getpid()})
            os.chmod(nonce_path, 0o600)
            exp_res = run_pilot(["sentinel", "--kind", "wake", "--dot", "DOT-A"], pilot_path, env_extra={"DOTS_PILOT_SENTINEL_NONCE": "abc12345"})
            self.assertEqual(exp_res.returncode, 2)
            self.assertIn("expired", exp_res.stderr)

            _write_json(nonce_path, {"nonce": "abc12345", "t": dp._now_iso(), "pid": 99999999})
            os.chmod(nonce_path, 0o600)
            dead_res = run_pilot(["sentinel", "--kind", "wake", "--dot", "DOT-A"], pilot_path, env_extra={"DOTS_PILOT_SENTINEL_NONCE": "abc12345"})
            self.assertEqual(dead_res.returncode, 2)
            self.assertIn("not alive", dead_res.stderr)

            # 6) Pin-downgrade protection rejects missing/null monitor_sha256 and dots_sha256
            cfg_null_mon = dict(cfg)
            cfg_null_mon["dots_monitor_py"] = mon_copy
            cfg_null_mon["monitor_sha256"] = None
            _write_json(cfg_path, cfg_null_mon)
            doc_null_mon = run_pilot(["doctor"], pilot_path)
            self.assertEqual(doc_null_mon.returncode, 2)
            self.assertIn("FAIL dots_monitor_py_pin", doc_null_mon.stdout)
            once_null_mon = run_pilot(["once"], pilot_path)
            self.assertEqual(once_null_mon.returncode, 0, once_null_mon.stdout + once_null_mon.stderr)
            self.assertIn("PILOT_MONITOR_WARN", once_null_mon.stdout)
            self.assertNotIn("TAMPERED", once_null_mon.stdout + once_null_mon.stderr)

            cfg_null_dots = dict(cfg_null_mon)
            cfg_null_dots["dots_sha256"] = None
            _write_json(cfg_path, cfg_null_dots)
            doc_null_dots = run_pilot(["doctor"], pilot_path)
            self.assertEqual(doc_null_dots.returncode, 2)
            self.assertIn("FAIL dots_py_pin", doc_null_dots.stdout)
            once_null_dots = run_pilot(["once"], pilot_path)
            self.assertEqual(once_null_dots.returncode, 2)
            self.assertIn("PILOT_ERROR dots_sha256 missing or invalid", once_null_dots.stdout)

            # init without --force must reject corrupted existing config.json pin
            kept_bad = run_pilot(["init", "--ops-base", self.ops_base], pilot_path)
            self.assertEqual(kept_bad.returncode, 2)
            self.assertIn("dots_sha256 missing or invalid", kept_bad.stderr)

            # 7) Explicit missing --dots-monitor-py, unknown config preset, reserved project, and non-ASCII nonce rejection
            p3 = os.path.join(self.tmp, "pilot3")
            ops3 = os.path.join(self.tmp, "ops3")
            bad_mon_arg = run_pilot(["init", "--dot", "DOT-A", "--project", "p", "--ops-base", ops3, "--dots-monitor-py", os.path.join(self.tmp, "missing_mon.py")], p3)
            self.assertEqual(bad_mon_arg.returncode, 2)

            bad_preset_cfg = os.path.join(self.tmp, "bad_preset.json")
            _write_json(bad_preset_cfg, {"v": 1, "dot": "DOT-A", "project": "p", "preset": "nonexistent-preset"})
            bad_preset_res = run_pilot(["init", "--config", bad_preset_cfg, "--ops-base", ops3], p3)
            self.assertEqual(bad_preset_res.returncode, 2)

            res_loops = run_pilot(["init", "--dot", "DOT-A", "--project", "loops", "--ops-base", ops3], p3)
            self.assertEqual(res_loops.returncode, 2)
            self.assertIn("reserved", res_loops.stderr)

            # Valid pilot3 for PILOT_WATCH_WARN, loop_status_error, and non-ASCII sentinel nonce
            ok3 = run_pilot(["init", "--dot", "DOT-A", "--project", "p", "--ops-base", ops3, "--preset", "generic"], p3)
            self.assertEqual(ok3.returncode, 0, ok3.stderr)
            n3_path = os.path.join(p3, "active_nonce.json")
            _write_json(n3_path, {"nonce": "validhex", "t": dp._now_iso(), "pid": os.getpid()})
            os.chmod(n3_path, 0o600)
            non_ascii_s = run_pilot(["sentinel", "--kind", "wake", "--dot", "DOT-A"], p3, env_extra={"DOTS_PILOT_SENTINEL_NONCE": "한글secret"})
            self.assertEqual(non_ascii_s.returncode, 2)

            # Corrupt state timestamp triggers PILOT_WATCH_WARN even without --verbose;
            # SHA256-pinned failing dots.py triggers nonzero watch + loop status surfaced in once and status (--json and text) with secret redaction
            wstate_p3 = os.path.join(ops3, "loops", "DOT-A", "work.state.json")
            wst3 = _read_json(wstate_p3)
            wst3["last_activity_at"] = "not-a-ts"
            _write_json(wstate_p3, wst3)
            once_watch_warn = run_pilot(["once"], p3)
            self.assertEqual(once_watch_warn.returncode, 0, once_watch_warn.stdout + once_watch_warn.stderr)
            self.assertIn("PILOT_WATCH_WARN", once_watch_warn.stdout)

            fail_dots = os.path.join(self.tmp, "dots_fail.py")
            fail_src = "import sys\nprint('boom sk-secret1234567890', file=sys.stderr)\nsys.exit(2)\n"
            pathlib.Path(fail_dots).write_text(fail_src, encoding="utf-8")
            fail_sha = hashlib.sha256(fail_src.encode("utf-8")).hexdigest()
            p3_cfg_path = os.path.join(p3, "config.json")
            p3_cfg = _read_json(p3_cfg_path)
            p3_cfg["dots_py"] = fail_dots
            p3_cfg["dots_sha256"] = fail_sha
            _write_json(p3_cfg_path, p3_cfg)

            once_ls_warn = run_pilot(["once"], p3)
            self.assertEqual(once_ls_warn.returncode, 0, once_ls_warn.stdout + once_ls_warn.stderr)
            self.assertIn("PILOT_WATCH_WARN exit=2", once_ls_warn.stdout)
            self.assertIn("PILOT_LOOP_STATUS_WARN exit=2", once_ls_warn.stdout)
            self.assertNotIn("sk-secret1234567890", once_ls_warn.stdout)
            self.assertIn("[REDACTED]", once_ls_warn.stdout)

            st_err_json = run_pilot(["status", "--json"], p3)
            self.assertEqual(st_err_json.returncode, 0, st_err_json.stderr)
            st_err_doc = json.loads(st_err_json.stdout)
            self.assertIsNotNone(st_err_doc["loop_status_error"])
            self.assertNotIn("sk-secret1234567890", st_err_doc["loop_status_error"])
            self.assertIn("[REDACTED]", st_err_doc["loop_status_error"])
            st_err_txt = run_pilot(["status"], p3)
            self.assertEqual(st_err_txt.returncode, 0, st_err_txt.stderr)
            self.assertIn("loop_status_error=", st_err_txt.stdout)
            self.assertNotIn("sk-secret1234567890", st_err_txt.stdout)

            # Version alignment, ANSI/quoted/PGP redaction, systemd escaping & injection block,
            # non-dict/corrupt JSON guards, and read-only status
            self.assertEqual(dp.VERSION, "1.3.1")
            self.assertEqual(dp._redact_text("Bearer \x1b[31msecret1234567890abcdef\x1b[0m"), "[REDACTED]")
            self.assertEqual(dp._redact_text('API_KEY="tok part2 secret"'), "[REDACTED]")
            self.assertEqual(
                dp._redact_text("-----BEGIN PGP PRIVATE KEY BLOCK-----\nabc\n-----END PGP PRIVATE KEY BLOCK-----"),
                "[REDACTED]",
            )
            self.assertEqual(dp._systemd_quote("/tmp/x$HOME"), '"/tmp/x$$HOME"')
            bad_sysd = run_pilot(["render-service", "--target", "systemd"], "/tmp/bad\nExecStartPost=/bin/sh -c id")
            self.assertEqual(bad_sysd.returncode, 2)
            self.assertNotIn("ExecStartPost=", bad_sysd.stdout)

            missing_pilot = os.path.join(self.tmp, "missing_status_pilot")
            miss_st = run_pilot(["status", "--json"], missing_pilot)
            self.assertEqual(miss_st.returncode, 0, miss_st.stderr)
            self.assertFalse(json.loads(miss_st.stdout)["initialized"])
            self.assertFalse(os.path.exists(missing_pilot))

            badjson_dots = os.path.join(self.tmp, "dots_badjson.py")
            badjson_src = "import sys\nprint('[]')\nsys.exit(0)\n"
            pathlib.Path(badjson_dots).write_text(badjson_src, encoding="utf-8")
            p3_cfg["dots_py"] = badjson_dots
            p3_cfg["dots_sha256"] = hashlib.sha256(badjson_src.encode("utf-8")).hexdigest()
            _write_json(p3_cfg_path, p3_cfg)

            once_badjson = run_pilot(["once"], p3)
            self.assertEqual(once_badjson.returncode, 0, once_badjson.stdout + once_badjson.stderr)
            self.assertIn("PILOT_LOOP_STATUS_WARN exit=0", once_badjson.stdout)

            st_badjson = run_pilot(["status", "--json"], p3)
            self.assertEqual(st_badjson.returncode, 0, st_badjson.stderr)
            st_badjson_doc = json.loads(st_badjson.stdout)
            self.assertIsNotNone(st_badjson_doc["loop_status_error"])

            pathlib.Path(p3_cfg_path).write_text("null\n", encoding="utf-8")
            pathlib.Path(os.path.join(p3, "ledger.json")).write_text("[]\n", encoding="utf-8")
            pathlib.Path(os.path.join(p3, "heartbeat.json")).write_text("[]\n", encoding="utf-8")
            null_cfg_init = run_pilot(["init", "--ops-base", ops3], p3)
            self.assertEqual(null_cfg_init.returncode, 2)
            self.assertIn("not a JSON object", null_cfg_init.stderr)
            null_status = run_pilot(["status", "--json"], p3)
            self.assertEqual(null_status.returncode, 0, null_status.stderr)
            null_doc = json.loads(null_status.stdout)
            self.assertFalse(null_doc["initialized"])
            self.assertIsNone(null_doc["heartbeat"])
            null_launchd = run_pilot(["render-service", "--target", "launchd"], p3)
            self.assertEqual(null_launchd.returncode, 0, null_launchd.stderr)
            self.assertIn("<plist", null_launchd.stdout)
            pathlib.Path(p3_cfg_path).write_text("[1]\n", encoding="utf-8")
            list_status = run_pilot(["status", "--json"], p3)
            self.assertEqual(list_status.returncode, 0, list_status.stderr)
            self.assertFalse(json.loads(list_status.stdout)["initialized"])
            list_systemd = run_pilot(["render-service", "--target", "systemd"], p3)
            self.assertEqual(list_systemd.returncode, 0, list_systemd.stderr)
            self.assertIn("[Service]", list_systemd.stdout)

            for raw_bad in ("123\n", "{bad\n"):
                pathlib.Path(p3_cfg_path).write_text(raw_bad, encoding="utf-8")
                bad_st = run_pilot(["status", "--json"], p3)
                self.assertEqual(bad_st.returncode, 0, bad_st.stderr)
                bad_doc = json.loads(bad_st.stdout)
                self.assertFalse(bad_doc["initialized"])
                self.assertIsNone(bad_doc["heartbeat"])
                bad_rs = run_pilot(["render-service", "--target", "systemd"], p3)
                self.assertEqual(bad_rs.returncode, 0, bad_rs.stderr)

            pathlib.Path(p3_cfg_path).write_text('{"dot": 123}\n', encoding="utf-8")
            num_dot_st = run_pilot(["status", "--json"], p3)
            self.assertEqual(num_dot_st.returncode, 0, num_dot_st.stderr)
            self.assertIsNone(json.loads(num_dot_st.stdout)["loop_status_error"])
            num_dot_rs = run_pilot(["render-service", "--target", "systemd"], p3)
            self.assertEqual(num_dot_rs.returncode, 0, num_dot_rs.stderr)
            self.assertIn("DOT-A", num_dot_rs.stdout)

            pathlib.Path(p3_cfg_path).write_text('{"dot": "\\u001b[31m \\u001b[0m"}\n', encoding="utf-8")
            ansi_dot_rs = run_pilot(["render-service", "--target", "launchd"], p3)
            self.assertEqual(ansi_dot_rs.returncode, 0, ansi_dot_rs.stderr)
            self.assertIn("com.dotomation.pilot.dot-a", ansi_dot_rs.stdout)

            _write_json(p3_cfg_path, {
                "dot": "REM17",
                "ops_base": 123,
                "dots_py": badjson_dots,
                "dots_sha256": hashlib.sha256(badjson_src.encode("utf-8")).hexdigest(),
            })
            bad_ops_st = run_pilot(["status", "--json"], p3)
            self.assertEqual(bad_ops_st.returncode, 0, bad_ops_st.stderr)
            self.assertTrue(json.loads(bad_ops_st.stdout)["dots_pin_ok"])

            # 8) CDP tab exact/prefix disambiguation (/dots/1 must not match /dots/10) + IPv6 [::1] Host bracket check
            fake_tabs = [{"type": "page", "url": "https://chatgpt.com/dots/10", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/10"}]
            with mock.patch("urllib.request.build_opener") as mock_opener:
                class _TabResp:
                    def __enter__(self): return self
                    def __exit__(self, *a): return False
                    def read(self, n=-1): return json.dumps(fake_tabs).encode("utf-8")
                mock_opener.return_value.open.return_value = _TabResp()
                with self.assertRaises(RuntimeError) as ctx_tab:
                    dp._cdp_send_aside("http://[::1]:9222", "https://chatgpt.com/dots/1", "hello", timeout=2)
                self.assertIn("no open browser tab", str(ctx_tab.exception))
                req_passed = mock_opener.return_value.open.call_args[0][0]
                self.assertEqual(req_passed.full_url, "http://[::1]:9222/json/list")
        finally:
            if tmp_pilot_ctx is not None:
                tmp_pilot_ctx.cleanup()

    def test_astra_brief_require_ack_propagation_and_pin_type_guards(self):
        res = run_pilot([
            "init", "--dot", "DOT-P", "--project", "kice",
            "--ops-base", self.ops_base, "--preset", "research-3track",
            "--require-astra-ack",
        ], self.pilot_dir)
        self.assertEqual(res.returncode, 0, res.stderr)

        cfg = _read_json(os.path.join(self.pilot_dir, "config.json"))
        self.assertTrue(cfg["require_astra_ack"])

        for tr, expected_role in (("research", "worker"), ("ontology", "worker"), ("authoring", "worker")):
            cpath = os.path.join(self.ops_base, "loops", "DOT-P", f"{tr}.contract.json")
            cdoc = _read_json(cpath)
            self.assertTrue(cdoc["require_astra_ack"], tr)
            self.assertEqual(cdoc["astra_role"], expected_role, tr)
            self.assertTrue(cdoc.get("astra_profile_sha"), tr)

        br_txt = run_pilot(["brief"], self.pilot_dir)
        self.assertEqual(br_txt.returncode, 0, br_txt.stderr)
        self.assertIn("Track `research` (DOT-P/kice)", br_txt.stdout)
        self.assertIn("Ack required:", br_txt.stdout)
        self.assertIn("[ASTRA_HARNESS_PROFILE v2", br_txt.stdout)

        br_json = run_pilot(["brief", "--track", "research", "--json"], self.pilot_dir)
        self.assertEqual(br_json.returncode, 0, br_json.stderr)
        bdoc = json.loads(br_json.stdout)
        self.assertEqual(bdoc["dot"], "DOT-P")
        self.assertEqual(len(bdoc["tracks"]), 1)
        t0 = bdoc["tracks"][0]
        self.assertEqual(t0["track"], "research")
        self.assertTrue(t0["require_astra_ack"])
        self.assertIsNotNone(t0["ack_cmd"])
        self.assertIn("--profile-sha", t0["ack_cmd"])
        self.assertEqual(t0["sha256"], t0["profile"]["sha256"])
        self.assertEqual(t0["profile"]["version"], "astra-v2")

        bad_tr = run_pilot(["brief", "--track", "missing"], self.pilot_dir)
        self.assertEqual(bad_tr.returncode, 2)

        ok1, _, _, err1 = dp._verify_dots_pin({"dots_py": 123, "dots_sha256": "0" * 64})
        self.assertFalse(ok1)
        self.assertIn("string", err1)
        ok2, _, _, err2 = dp._verify_dots_pin({
            "dots_py": os.path.join(TOOLS_DIR, "dots.py"),
            "dots_sha256": {"not": "str"},
        })
        self.assertFalse(ok2)
        self.assertIn("dots_sha256", err2)

        ok3, _, _, err3 = dp._verify_monitor_pin({"dots_monitor_py": ["bad"], "monitor_sha256": "0" * 64})
        self.assertFalse(ok3)
        self.assertIn("string", err3)
        ok4, _, _, err4 = dp._verify_monitor_pin({
            "dots_monitor_py": os.path.join(TOOLS_DIR, "dots_monitor.py"),
            "monitor_sha256": 999,
        })
        self.assertFalse(ok4)
        self.assertIn("monitor_sha256", err4)

        # JSON secret, 48-hex Hub URL, and PGP block redaction + reviewer astra_role brief
        red_json = dp._redact_text('{"api_key": "mytopsecret123", "session_token": "tok999"}')
        self.assertNotIn("mytopsecret123", red_json)
        self.assertNotIn("tok999", red_json)
        red_url = dp._redact_text("http://127.0.0.1:9000/a/" + ("ab" * 24) + "/mcp")
        self.assertNotIn("ab" * 24, red_url)
        self.assertTrue(red_url.endswith("/mcp"))

        cfg_rev = dict(cfg)
        cfg_rev["tracks"] = [
            {"track": "audit-t", "astra_role": "reviewer", "role": "reviewer", "objective": "Audit cleanly", "continuous": True}
        ]
        _write_json(os.path.join(self.pilot_dir, "config.json"), cfg_rev)
        os.chmod(os.path.join(self.pilot_dir, "config.json"), 0o600)
        br_rev = run_pilot(["brief", "--track", "audit-t", "--json"], self.pilot_dir)
        self.assertEqual(br_rev.returncode, 0, br_rev.stderr)
        t_rev = json.loads(br_rev.stdout)["tracks"][0]
        self.assertEqual(t_rev["role"], "reviewer")
        self.assertTrue(t_rev["profile"]["read_only"])

        # Custom track omitting both role and astra_role aligns loop init contract with brief SHA
        esc_json = dp._redact_text('{"api_key": "part1\\"part2secret"}')
        self.assertNotIn("part2secret", esc_json)
        large_pem = "-----BEGIN PRIVATE KEY-----\n" + (("B" * 64 + "\n") * 1100) + "ABCD\n-----END PRIVATE KEY-----\nAFTER_PEM"
        self.assertIn("AFTER_PEM", dp._redact_text(large_pem))
        self.assertNotIn("BBBBBBBB", dp._redact_text(large_pem))
        self.assertNotIn("\nABCD\n", dp._redact_text(large_pem))
        aq_pilot = dp._redact_text("ABCDEFGH12345678\nAQ==\n-----END PRIVATE KEY-----\nKEEP_PILOT")
        self.assertNotIn("AQ==", aq_pilot)
        self.assertNotIn("ABCDEFGH12345678", aq_pilot)
        self.assertIn("KEEP_PILOT", aq_pilot)
        front_pgp = "PRE\n" + ("ABCDEFGH12345678\n" * 2) + "=AbCd\n-----END PGP PRIVATE KEY BLOCK-----\nPOST"
        red_fpgp = dp._redact_text(front_pgp)
        self.assertNotIn("ABCDEFGH12345678", red_fpgp)
        self.assertNotIn("=AbCd", red_fpgp)
        self.assertIn("POST", red_fpgp)
        front_unp = "PRE\n" + ("ABCDEFGH12345678\n" * 2) + "ABCD\n-----END PRIVATE KEY-----\nPOST"
        red_funp = dp._redact_text(front_unp)
        self.assertNotIn("ABCDEFGH12345678", red_funp)
        self.assertNotIn("\nABCD\n", red_funp)
        self.assertIn("POST", red_funp)
        over64_pgp = "-----BEGIN PGP PRIVATE KEY BLOCK-----\n" + (("B" * 64 + "\n") * 1100) + "ABCD\n=AbCd\n-----END PGP PRIVATE KEY BLOCK-----\nTAIL_64P"
        red_64p = dp._redact_text(over64_pgp)
        self.assertNotIn("BBBBBBBB", red_64p)
        self.assertNotIn("\nABCD\n", red_64p)
        self.assertNotIn("=AbCd", red_64p)
        self.assertIn("TAIL_64P", red_64p)
        front_pgp_pad = "PRE_PGP\n" + ("MIIEowIBAAKCAQEA\n" * 2) + "AQ==\n=AbCd\n-----END PGP PRIVATE KEY BLOCK-----\nPOST_PGP"
        red_fpp = dp._redact_text(front_pgp_pad)
        self.assertNotIn("MIIEowIBAAKCAQEA", red_fpp)
        self.assertNotIn("AQ==", red_fpp)
        self.assertNotIn("=AbCd", red_fpp)
        self.assertIn("POST_PGP", red_fpp)
        over64_pgp_pad = "-----BEGIN PGP PRIVATE KEY BLOCK-----\n" + (("B" * 64 + "\n") * 1100) + "AQ==\n=AbCd\n-----END PGP PRIVATE KEY BLOCK-----\nTAIL_64PAD"
        red_64pad = dp._redact_text(over64_pgp_pad)
        self.assertNotIn("BBBBBBBB", red_64pad)
        self.assertNotIn("AQ==", red_64pad)
        self.assertNotIn("=AbCd", red_64pad)
        self.assertIn("TAIL_64PAD", red_64pad)

        custom_cfg = dict(cfg)
        custom_cfg["require_astra_ack"] = True
        custom_cfg["tracks"] = [{"track": "review", "objective": "Review custom track"}]
        _write_json(os.path.join(self.pilot_dir, "config.json"), custom_cfg)
        os.chmod(os.path.join(self.pilot_dir, "config.json"), 0o600)
        dp._apply_tracks_to_ops(custom_cfg, os.path.join(TOOLS_DIR, "dots.py"), force=True)
        cdoc_rev = json.loads(pathlib.Path(os.path.join(self.ops_base, "loops", cfg["dot"], "review.contract.json")).read_text(encoding="utf-8"))
        self.assertEqual(cdoc_rev["astra_role"], "reviewer")
        br_cust = json.loads(run_pilot(["brief", "--track", "review", "--json"], self.pilot_dir).stdout)["tracks"][0]
        self.assertEqual(br_cust["role"], "reviewer")
        ack_res = subprocess.run(
            [
                sys.executable, "-B", os.path.join(TOOLS_DIR, "dots.py"),
                "loop", "ack", cfg["dot"], "review", "--agent", "r-cust", "--profile-sha", br_cust["profile"]["sha256"],
            ],
            env=dict(os.environ, DOTS_OPS_BASE=self.ops_base, DOTS_AUTO_UPDATE="0"),
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(ack_res.returncode, 0, ack_res.stderr)


if __name__ == "__main__":
    unittest.main()
