#!/usr/bin/env python3
"""Tests for dots_monitor: classifications, legacy lines, watch dedup, redaction, evidence.

Run with: python3 tools/test_dots_monitor.py
No network, no ssh and no gh: a fake transport serves synthetic ledger and daemon
log dumps, and a fake gh answers the evidence lookups.
"""

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True  # leave no __pycache__ next to the tool
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import dots_monitor as dm  # noqa: E402

NOW = datetime(2026, 11, 22, 21, 40, 0, tzinfo=timezone.utc)


def stamp(minutes_ago=0, seconds_ago=0):
    return dm.iso(NOW - timedelta(minutes=minutes_ago, seconds=seconds_ago))


def record(agent="writer", minutes_ago=5, state="step", evidence="MATCH-CHECK",
           note="did the step", task="task-1/1", role="구현", project="example",
           host="local", **extra):
    body = {
        "t": stamp(minutes_ago),
        "agent": agent,
        "role": role,
        "model": "requested gpt-6.2-astra xhigh; runtime does not display effective",
        "project": project,
        "task": task,
        "state": state,
        "host": host,
        "note": note,
        "evidence": evidence,
    }
    body.update(extra)
    return json.dumps(body, ensure_ascii=False)


TEST_NONCE = "0123456789abcdef"
NONCE_RE = re.compile(r"==(?:DOT|LOG) ([0-9a-f]{16}) ")


def tag(name, nonce, *rest):
    """A dump sentinel; ``nonce=None`` gives the pre-nonce framing of 50e0272."""
    return " ".join([name] + ([nonce] if nonce else []) + [str(r) for r in rest])


def ledger_dump(files_by_dot, rosters=None, sizes=None, nonce=TEST_NONCE, nodirs=()):
    """Build the framed dump that ledger_script() produces."""
    rosters = rosters or {}
    sizes = sizes or {}
    lines = []
    for dot in list(files_by_dot) + [d for d in nodirs if d not in files_by_dot]:
        lines.append(tag("==DOT", nonce, dot))
        if dot in nodirs:
            lines.append(tag("==NODIR", nonce))
            continue
        payload = dict(files_by_dot[dot])
        if dot in rosters:
            payload["roster.json"] = json.dumps(rosters[dot], ensure_ascii=False)
        for name, text in payload.items():
            size = sizes.get((dot, name), len(text.encode("utf-8")))
            lines.append(tag("==FILE", nonce, name, size))
            lines.append(text)
            lines.append(tag("==END", nonce))
    return "\n".join(lines) + "\n"


def activity_dump(paths_text, sizes=None, nonce=TEST_NONCE):
    """Build the framed dump that activity_script() produces."""
    sizes = sizes or {}
    lines = []
    for index, text in enumerate(paths_text):
        lines.append(tag("==LOG", nonce, index))
        if text is None:
            lines.append(tag("==MISSING", nonce))
            continue
        lines.append(tag("==SIZE", nonce, sizes.get(index, len(text.encode("utf-8")))))
        lines.append(text)
        lines.append(tag("==END", nonce))
    return "\n".join(lines) + "\n"


def log_line(minutes_ago, path="mcp/core", status="200", extra=""):
    return "%s  info   request POST %s \u2192 %s in 120ms%s" % (
        stamp(minutes_ago), path, status, extra)


class FakeTransport:
    """Answers the generated scripts with synthetic dumps.

    The dumps are framed with the nonce read from the script itself, so a
    mismatch between ledger_script()/activity_script() and their parsers fails
    here instead of only on a real host.
    """

    def __init__(self, files=None, rosters=None, activity_texts=None, gh=None,
                 failures=None, sizes=None, nodirs=()):
        self.files = files or {}
        self.rosters = rosters or {}
        self.activity_texts = activity_texts or []
        self.gh_map = gh or {}
        self.failures = failures or {}
        self.sizes = sizes or {}
        self.nodirs = tuple(nodirs)
        self.gh_calls = []
        self.scripts = []

    def run(self, host, script):
        self.scripts.append(script)
        if host in self.failures:
            return None, self.failures[host]
        found = NONCE_RE.search(script)
        nonce = found.group(1) if found else None
        if "==DOT" in script:
            return ledger_dump(self.files, self.rosters, self.sizes, nonce, self.nodirs), None
        return activity_dump(self.activity_texts, None, nonce), None

    def gh(self, endpoint, jq):
        self.gh_calls.append(endpoint)
        if endpoint in self.gh_map:
            return self.gh_map[endpoint]
        return None, "gh: Not Found (HTTP 404)"


def base_config(dots=("DOT-A",), activity=None, repos=("owner/repo",), enabled=True):
    return {
        "ledger": {"host": "local", "root": "~/live", "dots": list(dots)},
        "dots": {dot: {"account": 17} for dot in dots},
        "activity": activity if activity is not None else [
            {"dot": "DOT-A", "host": "local", "label": "local", "path": "~/wgpt/app.log"}],
        "github": {"enabled": enabled, "repos": list(repos), "cache_seconds": 0,
                   "max_lookups": 40},
        "thresholds": {"stall_minutes": 30, "active_window_minutes": 15,
                       "long_window_minutes": 60},
        "limits": {"tail_bytes": 262144, "command_timeout_seconds": 5,
                   "gh_timeout_seconds": 5},
    }


def snapshot_for(files, activity_text=None, roster=None, gh=None, config=None,
                 failures=None, sizes=None, nodirs=()):
    config = config or base_config()
    transport = FakeTransport(
        files=files,
        rosters={config["ledger"]["dots"][0]: roster} if roster else None,
        activity_texts=[activity_text] if activity_text is not None else [],
        gh=gh, failures=failures, sizes=sizes, nodirs=nodirs)
    errors = []
    snapshot = dm.build_snapshot(config, transport, NOW, errors)
    return snapshot, transport, errors


def rows(snapshot, dot="DOT-A"):
    return {row["agent"]: row for row in snapshot["dots"][dot]["agents"]}


# ---------------------------------------------------------------------------


class TestRedaction(unittest.TestCase):
    def test_github_and_sk_tokens_are_removed(self):
        for text in ("ghp_ABCDEFGHIJKLMNOPQRSTUVWX", "github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrst",
                     "sk-abcdefghijklmnop1234"):
            self.assertIn("[REDACTED]", dm.redact(text), text)

    def test_key_value_secrets_are_removed(self):
        self.assertNotIn("hunter2", dm.redact("token=hunter2"))
        self.assertNotIn("abc123", dm.redact("api_key: abc123"))
        self.assertNotIn("deadbeef", dm.redact("?access_token=deadbeef&x=1"))

    def test_prefixed_credentials_and_bearer_headers_are_redacted(self):
        sha = "9f610ad13a6cbaeadcf33d80b6a0cb2de3c8d1db"
        for label, secret in (
            ("DATABASE_PASSWORD=", "hunter2"),
            ("MY_API_KEY=", "abcd1234efgh5678"),
            ("OPENAI_API_KEY=", "feedface"),
            ("SERVICE_KEY=", "shortkey"),
            ("Authorization: Bearer ", "0123456789abcdef0123456789abcdef"),
        ):
            with self.subTest(label=label):
                text = dm.redact(label + secret + " task=task-1/1 base=" + sha)
                self.assertNotIn(secret, text)
                self.assertIn("[REDACTED]", text)
                self.assertIn("task=task-1/1 base=" + sha, text)

    def test_commit_sha_survives(self):
        sha = "9f610ad13a6cbaeadcf33d80b6a0cb2de3c8d1db"
        self.assertEqual(dm.redact("base %s" % sha), "base %s" % sha)

    def test_hyphenated_task_id_survives(self):
        task = "alpha-intake-20261122T2100Z/1"
        self.assertEqual(dm.redact(task), task)

    def test_high_entropy_token_is_removed(self):
        token = "aB3dE5gH7jK9mN1pQ2sT4vW6yZ8bC0dF"
        self.assertEqual(dm.redact(token), "[REDACTED]")

    def test_mcp_path_secret_is_removed(self):
        text = "http://127.0.0.1:54859/mcp/core/pH2N0yfbENMEyXEhWwI1sd51z3XJhpG1Mg"
        self.assertNotIn("pH2N0yfb", dm.redact(text))
        self.assertIn("/mcp/core/[REDACTED]", dm.redact(text))


class TestRecordParsing(unittest.TestCase):
    def test_valid_record(self):
        rec, reason = dm.parse_record_line(record())
        self.assertIsNone(reason)
        self.assertEqual(rec["agent"], "writer")
        self.assertEqual(rec["_t"], dm.parse_ts(stamp(5)))

    def test_fractional_seconds_and_offset(self):
        line = record().replace(stamp(5), "2026-11-22T21:35:00.123456+00:00")
        rec, reason = dm.parse_record_line(line)
        self.assertIsNone(reason)
        self.assertEqual(rec["_t"].second, 0)
        self.assertEqual(rec["_t"].microsecond, 123456)

    def test_timestamp_alias_is_legacy(self):
        body = json.loads(record())
        body["timestamp"] = body.pop("t")
        _, reason = dm.parse_record_line(json.dumps(body))
        self.assertIn("timestamp", reason)

    def test_missing_and_extra_fields_are_legacy(self):
        body = json.loads(record())
        body.pop("host")
        self.assertIn("missing host", dm.parse_record_line(json.dumps(body))[1])
        body = json.loads(record())
        body["extra"] = "x"
        self.assertIn("extra extra", dm.parse_record_line(json.dumps(body))[1])

    def test_unknown_state_is_legacy(self):
        _, reason = dm.parse_record_line(record(state="running"))
        self.assertIn("state 'running'", reason)

    def test_multiline_and_empty_values_are_legacy(self):
        self.assertIn("spans lines", dm.parse_record_line(record(note="a\nb"))[1])
        self.assertIn("empty note", dm.parse_record_line(record(note=""))[1])

    def test_not_json_is_legacy(self):
        self.assertIn("not JSON", dm.parse_record_line("{oops")[1])
        self.assertIn("not a JSON object", dm.parse_record_line("[1,2]")[1])

    def test_bad_timestamp_is_legacy(self):
        self.assertIn("not RFC 3339", dm.parse_record_line(record(t="yesterday"))[1])


class TestLedgerFraming(unittest.TestCase):
    def test_parse_dump(self):
        dump = dm.parse_ledger_dump(ledger_dump({"DOT-A": {"writer.jsonl": record()}},
                                                {"DOT-A": {"dot": "DOT-A", "slots": []}}),
                                    TEST_NONCE)
        self.assertEqual(sorted(dump["DOT-A"]["files"]), ["roster.json", "writer.jsonl"])
        self.assertIn('"agent": "writer"', dump["DOT-A"]["files"]["writer.jsonl"]["text"])

    def test_split_tail_drops_partial_first_line(self):
        text = '{"half": true}\n{"full": true}'
        self.assertEqual(dm.split_tail(text, 40, 20), '{"full": true}')
        self.assertEqual(dm.split_tail(text, 10, 20), text)

    def test_truncated_file_keeps_complete_lines(self):
        first = record(agent="writer", minutes_ago=60)
        body = first + "\n" + record(agent="writer", minutes_ago=5)
        dump = dm.parse_ledger_dump(ledger_dump({"DOT-A": {"writer.jsonl": body}},
                                                sizes={("DOT-A", "writer.jsonl"): 9999}),
                                    TEST_NONCE)
        data = dm.read_dot("DOT-A", dump, 100)
        self.assertEqual(len(data["agents"]["writer"]["records"]), 1)
        self.assertEqual(len(data["agents"]["writer"]["legacy"]), 0)

    def test_content_cannot_forge_sentinels(self):
        # Bare or wrong-nonce sentinels inside a file are file content, not framing.
        body = "\n".join([record(minutes_ago=9), "==END", "==DOT DOT-B",
                          "==END ffffffffffffffff", record(minutes_ago=2, state="done")])
        dump = dm.parse_ledger_dump(ledger_dump({"DOT-A": {"writer.jsonl": body}},
                                                nonce=TEST_NONCE), TEST_NONCE)
        self.assertEqual(list(dump), ["DOT-A"])
        data = dm.read_dot("DOT-A", dump, 262144)
        records = data["agents"]["writer"]["records"]
        self.assertEqual([r["state"] for r in records], ["step", "done"])
        self.assertEqual(len(data["agents"]["writer"]["legacy"]), 3)

    def test_activity_content_cannot_end_a_section(self):
        text = log_line(2) + "\n==END\n" + log_line(1)
        dump = dm.parse_activity_dump(activity_dump([text]), TEST_NONCE)
        self.assertEqual(dump["0"]["text"].count("request POST"), 2)

    def test_generated_scripts_parse_on_a_real_shell(self):
        with tempfile.TemporaryDirectory() as root:
            os.mkdir(os.path.join(root, "DOT-A"))
            with open(os.path.join(root, "DOT-A", "writer.jsonl"), "w", encoding="utf-8") as fh:
                fh.write(record() + "\n==END\n==DOT DOT-B\n")
            log = os.path.join(root, "app.log")
            with open(log, "w", encoding="utf-8") as fh:
                fh.write(log_line(1) + "\n==END\n")
            transport = dm.Transport(10, 10)
            nonce = dm.new_nonce()
            out, error = transport.run("local", dm.ledger_script(root, ["DOT-A", "DOT-B"],
                                                                 262144, nonce))
            self.assertIsNone(error)
            dump = dm.parse_ledger_dump(out, nonce)
            self.assertFalse(dump["DOT-A"]["nodir"])
            self.assertTrue(dump["DOT-B"]["nodir"])
            self.assertIn("==DOT DOT-B", dump["DOT-A"]["files"]["writer.jsonl"]["text"])
            out, error = transport.run("local", dm.activity_script([log, root + "/none"],
                                                                   262144, nonce))
            self.assertIsNone(error)
            dump = dm.parse_activity_dump(out, nonce)
            self.assertIn("==END", dump["0"]["text"])
            self.assertIsNone(dump["1"]["text"])

    def test_unreadable_live_files_and_activity_logs_are_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            dot = Path(tmp) / "DOT-A"
            dot.mkdir()
            log = Path(tmp) / "app.log"
            for path, text in ((dot / "writer.jsonl", record()),
                               (dot / "roster.json", '{"slots": []}'),
                               (log, log_line(1))):
                path.write_text(text, encoding="utf-8")
                path.chmod(0)
                if os.access(path, os.R_OK):
                    self.skipTest("the test user can read files with mode 000")
            config = base_config(activity=[{"dot": "DOT-A", "host": "local", "path": str(log)}])
            config["ledger"]["root"] = tmp
            snapshot = dm.build_snapshot(config, dm.Transport(10, 10), NOW)

        table = dm.render_table(snapshot)
        self.assertIn("DOTS_ERROR ledger DOT-A: writer.jsonl unreadable", table)
        self.assertIn("DOTS_ERROR ledger DOT-A: roster.json unreadable", table)
        self.assertIn("DOTS_ERROR activity " + str(log) + " unreadable", table)
        self.assertEqual(snapshot["dots"]["DOT-A"]["activity"], {})

    def test_ledger_caps_live_files_per_dot_and_reports_overflow(self):
        with tempfile.TemporaryDirectory() as tmp:
            dot = Path(tmp) / "DOT-A"
            dot.mkdir()
            for index in range(1, 203):
                agent = "writer-%03d" % index
                (dot / (agent + ".jsonl")).write_text(record(agent=agent), encoding="utf-8")
            (dot / "roster.json").write_text('{"coordinator": "lead"}', encoding="utf-8")
            other = Path(tmp) / "DOT-B"
            other.mkdir()
            (other / "writer.jsonl").write_text(record(), encoding="utf-8")
            (other / "roster.json").write_text('{"coordinator": "second"}', encoding="utf-8")
            config = base_config(dots=("DOT-A", "DOT-B"), activity=[])
            config["ledger"]["root"] = tmp
            snapshot = dm.build_snapshot(config, dm.Transport(10, 10), NOW)

        self.assertEqual(set(rows(snapshot)), {"writer-%03d" % i for i in range(1, 201)})
        self.assertEqual(list(rows(snapshot, "DOT-B")), ["writer"])
        self.assertEqual(snapshot["dots"]["DOT-A"]["roster"]["coordinator"], "lead")
        self.assertEqual(snapshot["dots"]["DOT-B"]["roster"]["coordinator"], "second")
        self.assertEqual(len(snapshot["errors"]), 1)
        self.assertIn("DOTS_ERROR ledger DOT-A: more than 200 live files", dm.render_table(snapshot))


class TestMissingLiveDirectory(unittest.TestCase):
    def test_missing_live_directory_is_a_ledger_error(self):
        snapshot, _, errors = snapshot_for({}, activity_text=log_line(1), nodirs=("DOT-A",))
        self.assertEqual(snapshot["dots"]["DOT-A"]["agents"], [])
        self.assertTrue(any(e["source"] == "ledger" and "DOT-A" in e["detail"]
                            and "missing" in e["detail"] for e in errors), errors)
        self.assertIn("DOTS_ERROR ledger DOT-A: live directory missing", dm.render_table(snapshot))

    def test_unreachable_host_is_reported_once(self):
        _, _, errors = snapshot_for({}, failures={"local": "exit 255: connection refused"})
        self.assertEqual(len([e for e in errors if e["source"] == "ledger"]), 1)

    def test_snapshot_exits_two_when_a_live_directory_is_missing(self):
        config = base_config(activity=[], enabled=False)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "config.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(config, fh)
            fake = FakeTransport(files={}, nodirs=("DOT-A",))
            out = io.StringIO()
            with mock.patch.object(dm, "Transport", lambda *a, **k: fake), \
                    contextlib.redirect_stdout(out):
                code = dm.main(["--config", path, "snapshot"])
        self.assertEqual(code, 2)
        self.assertIn("DOTS_ERROR ledger", out.getvalue())


class TestActivity(unittest.TestCase):
    def test_counts_exclude_probes_and_other_paths(self):
        text = "\n".join([
            log_line(2),
            log_line(3, extra=" calls=2 ingress_ms=1"),
            log_line(4, extra=" (tunnel probe)"),
            log_line(5, path="mcp/desktop"),
            log_line(20),
            log_line(50),
            log_line(90),
            "2026-11-22T21:00:00.000Z  info   session catalog ready",
        ])
        windows = {"short": timedelta(minutes=15), "long": timedelta(minutes=60)}
        counted = dm.count_calls(text, len(text), 262144, NOW, windows)
        self.assertEqual(counted["calls"]["short"], 2)
        self.assertEqual(counted["calls"]["long"], 4)
        self.assertEqual(counted["last_call_at"], dm.parse_ts(stamp(2)))
        self.assertFalse(counted["partial"])

    def test_partial_window_when_log_starts_late(self):
        text = log_line(1) + "\n" + log_line(2)
        windows = {"short": timedelta(minutes=15), "long": timedelta(minutes=60)}
        self.assertTrue(dm.count_calls(text, len(text), 262144, NOW, windows)["partial"])

    def test_activity_dump_indexes(self):
        dump = dm.parse_activity_dump(activity_dump(["a", None]), TEST_NONCE)
        self.assertEqual(dump["0"]["text"], "a")
        self.assertIsNone(dump["1"]["text"])

    def test_missing_log_reports_error(self):
        snapshot, _, errors = snapshot_for({"DOT-A": {"writer.jsonl": record()}}, activity_text=None)
        self.assertTrue(any(e["source"] == "activity" for e in errors))
        self.assertIn("DOTS_ERROR", dm.render_table(snapshot))


class TestClassification(unittest.TestCase):
    def test_working_when_tool_calls_follow_the_record(self):
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                      activity_text=log_line(2))
        self.assertEqual(rows(snapshot)["writer"]["classification"], "WORKING")

    def test_quiet_when_record_is_newer_than_every_call(self):
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                      activity_text=log_line(9))
        row = rows(snapshot)["writer"]
        self.assertEqual(row["classification"], "QUIET")
        self.assertEqual(row["activity_since_record"], 0)

    def test_stalled_after_thirty_minutes(self):
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=45)}},
                                      activity_text=log_line(1))
        self.assertEqual(rows(snapshot)["writer"]["classification"], "STALLED")

    def test_blocked_wins_over_age(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(minutes_ago=90, state="blocked",
                                         evidence="local-token")}})
        self.assertEqual(rows(snapshot)["writer"]["classification"], "BLOCKED")

    def test_done_with_resolved_evidence(self):
        gh = {"repos/owner/repo/pulls/7": ({"state": "open", "draft": False,
                                            "merged": False, "head": "a" * 40,
                                            "merge": None}, None)}
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(minutes_ago=5, state="done",
                                         evidence="https://github.com/owner/repo/pull/7")}},
            gh=gh)
        row = rows(snapshot)["writer"]
        self.assertEqual(row["classification"], "DONE")
        self.assertEqual(row["evidence_status"], "resolved")

    def test_done_on_local_only_evidence_is_unproven(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(minutes_ago=5, state="done",
                                         evidence="MATCH-REDTEAM-CHECK")}})
        row = rows(snapshot)["writer"]
        self.assertEqual(row["classification"], "UNPROVEN")
        self.assertEqual(row["evidence_status"], "local")

    def test_done_on_missing_pull_request_is_unproven(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(minutes_ago=5, state="done",
                                         evidence="https://github.com/owner/repo/pull/404")}})
        row = rows(snapshot)["writer"]
        self.assertEqual(row["classification"], "UNPROVEN")
        self.assertEqual(row["evidence_status"], "unresolved")

    def test_unresolved_evidence_on_a_step_is_unproven(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(minutes_ago=5,
                                         evidence="https://github.com/owner/repo/pull/404")}})
        self.assertEqual(rows(snapshot)["writer"]["classification"], "UNPROVEN")

    def test_unknown_resolver_keeps_done_but_flags_it(self):
        snapshot, _, errors = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(minutes_ago=5, state="done",
                                         evidence="https://github.com/owner/repo/pull/7")}},
            gh={"repos/owner/repo/pulls/7": (None, "error connecting to api.github.com")})
        row = rows(snapshot)["writer"]
        self.assertEqual(row["evidence_status"], "unknown")
        self.assertEqual(row["classification"], "DONE")
        self.assertTrue(any(e["source"] == "github" for e in errors))

    def test_legacy_only_agent(self):
        legacy = json.dumps({"timestamp": stamp(1), "agent": "old", "state": "step"})
        snapshot, _, _ = snapshot_for({"DOT-A": {"old.jsonl": legacy}})
        row = rows(snapshot)["old"]
        self.assertEqual(row["classification"], "LEGACY")
        self.assertEqual(row["state"], "unconfirmed")
        self.assertEqual(row["legacy_lines"], 1)
        self.assertIn("timestamp", row["legacy_reasons"][0])

    def test_legacy_lines_are_not_dropped_when_v1_lines_exist(self):
        body = json.dumps({"timestamp": stamp(30)}) + "\n" + record(minutes_ago=2)
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": body}},
                                      activity_text=log_line(1))
        row = rows(snapshot)["writer"]
        self.assertEqual(row["classification"], "WORKING")
        self.assertEqual(row["legacy_lines"], 1)

    def test_roster_slot_without_file(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record()}},
            roster={"dot": "DOT-A", "slots": [{"agent": "writer", "role": "구현"},
                                              {"agent": "ghost", "role": "검토"}]})
        row = rows(snapshot)["ghost"]
        self.assertEqual(row["classification"], "NO_RECORD")
        self.assertFalse(row["file_present"])
        self.assertIn("roster slot without live file", dm.render_table(snapshot))

    def test_non_list_roster_entries_are_dropped_and_reported(self):
        for entries in ({"slots": 3, "agents": 1.5, "actors": True},
                        {"slots": {}, "agents": "x", "actors": None}):
            with self.subTest(entries=entries):
                snapshot, _, _ = snapshot_for(
                    {"DOT-A": {"writer.jsonl": record()}}, roster=entries)
                self.assertEqual(list(rows(snapshot)), ["writer"])
                self.assertIn("DOTS_ERROR roster DOT-A: 3 roster slot(s) dropped",
                              dm.render_table(snapshot))

    def test_non_object_roster_json_is_reported(self):
        for roster in ([1, 2], "x", 5):
            with self.subTest(roster=roster):
                snapshot, _, _ = snapshot_for(
                    {"DOT-A": {"writer.jsonl": record()}}, roster=roster)
                self.assertIn("DOTS_ERROR roster DOT-A: roster.json is not an object",
                              dm.render_table(snapshot))

    def test_file_without_roster_slot_is_reported(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"extra-agent.jsonl": record(agent="extra-agent")}},
            roster={"dot": "DOT-A", "slots": [{"agent": "writer"}]})
        self.assertEqual(snapshot["dots"]["DOT-A"]["unrostered"], ["extra-agent"])
        self.assertIn("without roster slot", dm.render_table(snapshot))

    def test_json_snapshot_shape(self):
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record()}},
                                      activity_text=log_line(1))
        row = rows(snapshot)["writer"]
        for key in ("role", "task", "state", "age_seconds", "classification", "note",
                    "evidence_status", "evidence"):
            self.assertIn(key, row)
        parsed = json.loads(dm.render_json(snapshot))
        self.assertEqual(parsed["dots"]["DOT-A"]["agents"][0]["classification"], "WORKING")
        self.assertEqual(parsed["dots"]["DOT-A"]["activity"]["local"]["calls_15m"], 1)

    def test_ledger_failure_is_reported_and_not_fatal(self):
        snapshot, _, errors = snapshot_for({"DOT-A": {"writer.jsonl": record()}},
                                           failures={"local": "timed out after 5s"})
        self.assertEqual(snapshot["dots"]["DOT-A"]["agents"], [])
        self.assertTrue(any(e["source"] == "ledger" for e in errors))

    def test_done_on_stale_pull_request_head_is_unproven(self):
        gh = {"repos/owner/repo/pulls/7": ({"state": "open", "draft": False,
                                            "merged": False, "head": "1" * 40,
                                            "merge": None}, None)}
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(
                minutes_ago=5, state="done",
                evidence="https://github.com/owner/repo/pull/7 @" + "2" * 40)}},
            gh=gh)
        row = rows(snapshot)["writer"]
        self.assertEqual(row["evidence_status"], "stale")
        self.assertEqual(row["classification"], "UNPROVEN")

    def test_legacy_only_agent_past_the_threshold_is_stalled(self):
        old = json.dumps({"timestamp": stamp(90), "agent": "old", "state": "step"})
        undated = json.dumps({"agent": "undated", "state": "step"})
        snapshot, _, _ = snapshot_for({"DOT-A": {"old.jsonl": old, "undated.jsonl": undated}},
                                      activity_text=log_line(1))
        found = rows(snapshot)
        self.assertEqual(found["old"]["classification"], "STALLED")
        self.assertEqual(found["old"]["legacy_last_at"], stamp(90))
        self.assertIsNone(found["old"]["last_record_at"])
        self.assertEqual(found["undated"]["classification"], "STALLED")

    def test_legend_uses_configured_stall_threshold(self):
        config = base_config()
        config["thresholds"]["stall_minutes"] = 5
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=7)}},
                                      config=config)
        self.assertEqual(rows(snapshot)["writer"]["classification"], "STALLED")
        table = dm.render_table(snapshot)
        self.assertIn("older than 5m", table)
        self.assertNotIn("30m", table)

    def test_file_name_that_is_not_a_token_is_skipped_and_reported(self):
        snapshot, _, errors = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(), "bad name.jsonl": record(agent="bad")}})
        self.assertEqual(sorted(rows(snapshot)), ["writer"])
        self.assertTrue(any(e["source"] == "ledger" and "1 live file(s) skipped" in e["detail"]
                            for e in errors), errors)


class TestReviewRegressions(unittest.TestCase):
    """Blocking findings from the review of 50e0272; each test fails on that commit."""

    def test_json_output_redacts_every_field(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record(task="api_key=SECRETTASK2",
                                              role="password=SECRETROLE3",
                                              project="ghp_" + "A" * 36)}},
            activity_text=log_line(1),
            roster={"coordinator": "ghp_" + "D" * 36, "slots": [{"agent": "writer"}]})
        text = dm.render_json(snapshot)
        for secret in ("SECRETTASK2", "SECRETROLE3", "ghp_AAAA", "ghp_DDDD"):
            self.assertNotIn(secret, text)
        self.assertEqual(json.loads(text)["dots"]["DOT-A"]["agents"][0]["agent"], "writer")

    def test_watch_json_events_are_redacted(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            dm.emit_event({"event": "DOTS_BLOCKED", "dot": "DOT-A", "agent": "writer",
                           "detail": {"role": "ghp_" + "B" * 36, "task": "api_key=SECRET9"}},
                          True)
        self.assertNotIn("SECRET9", out.getvalue())
        self.assertNotIn("ghp_BBBB", out.getvalue())
        self.assertEqual(json.loads(out.getvalue())["agent"], "writer")

    def test_table_redacts_and_flattens_roster_coordinator(self):
        snapshot, _, _ = snapshot_for(
            {"DOT-A": {"writer.jsonl": record()}},
            roster={"coordinator": "ghp_" + "C" * 36 + "\nDOTS_ERROR ledger forged",
                    "updated_at": "2026\nDOTS_STALLED forged", "slots": [{"agent": "writer"}]})
        table = dm.render_table(snapshot)
        self.assertNotIn("ghp_CCCC", table)
        lines = table.splitlines()
        self.assertFalse([l for l in lines if l.startswith("DOTS_ERROR ledger forged")])
        self.assertFalse([l for l in lines if l.startswith("DOTS_STALLED forged")])

    def test_forged_roster_agent_id_is_dropped_and_reported(self):
        forged = "bad\nDOTS_ERROR ledger fake line"
        snapshot, _, errors = snapshot_for(
            {"DOT-A": {"writer.jsonl": record()}},
            roster={"slots": [{"agent": "writer"}, {"agent": forged}, {"agent": 7}, "x"]})
        table = dm.render_table(snapshot)
        self.assertFalse([l for l in table.splitlines()
                          if l.startswith("DOTS_ERROR ledger fake line")], table)
        self.assertEqual(sorted(rows(snapshot)), ["writer"])
        self.assertTrue(any(e["source"] == "roster" and "3 roster slot(s) dropped" in e["detail"]
                            for e in errors), errors)
        self.assertNotIn("fake line", json.dumps(errors))


class TestRelease103(unittest.TestCase):
    def test_conversation_permalink_resolves_only_the_exact_comment_and_parent(self):
        endpoint = "repos/o/r/issues/comments/123"
        for route in ("issues", "pull"):
            url = f"https://github.com/o/r/{route}/24#issuecomment-123"
            for data, error, expected in (({"id": 123, "html_url": url}, None, "resolved"),
                                          ({"id": 124, "html_url": url}, None, "unresolved"),
                                          ({"id": 123, "html_url": url.replace("/24#", "/25#")}, None, "unresolved"),
                                          ({"id": 123}, None, "unresolved"), (None, "HTTP 404", "unresolved")):
                with self.subTest(route=route, data=data, error=error):
                    transport = FakeTransport(gh={endpoint: (data, error)})
                    resolver = dm.EvidenceResolver(base_config(), transport, NOW, [])
                    self.assertEqual(dm.evidence_status(url, resolver)[0], expected)
                    self.assertEqual(transport.gh_calls, [endpoint])

    def test_post_permission_io_failure_discards_incomplete_samples(self):
        # Ordinary count/read errors in a local shell; every fixture path is readable.
        for failed_tool, result in (("wc", "printf partial; return 1"), ("tail", "printf partial; return 1"),
                                    ("tail", "printf fixture-diagnostic >&2; return 0")):
            with self.subTest(tool=failed_tool, result=result), tempfile.TemporaryDirectory() as root:
                dot = Path(root) / "DOT-A"
                dot.mkdir()
                (dot / "writer.jsonl").write_text(record(), encoding="utf-8")
                (dot / "roster.json").write_text("{}", encoding="utf-8")
                log = Path(root) / "activity.log"
                log.write_text(log_line(1), encoding="utf-8")
                config = base_config(activity=[{"dot": "DOT-A", "host": "local", "path": str(log)}])
                config["ledger"]["root"] = root
                class FailedRead(dm.Transport):
                    def run(self, host, script):
                        stub = failed_tool + "() { " + result + "; }\n"
                        return super().run(host, stub + script)
                snap = dm.build_snapshot(config, FailedRead(), NOW)
                table = dm.render_table(snap)
                self.assertIn("DOTS_ERROR ledger DOT-A: writer.jsonl unreadable", table)
                self.assertIn("DOTS_ERROR ledger DOT-A: roster.json unreadable", table)
                self.assertIn("DOTS_ERROR activity " + str(log) + " unreadable", table)
                self.assertNotIn("partial", json.dumps(snap["errors"]))
                self.assertNotIn("fixture-diagnostic", json.dumps(snap))
                self.assertEqual(rows(snap), {})
                self.assertEqual(snap["dots"]["DOT-A"]["activity"], {})

    def test_stderr_with_zero_status_is_visible_but_not_echoed(self):
        class Diagnostic(dm.Transport):
            def run(self, host, script):
                return super().run(host, "printf fixture-diagnostic >&2; :")
        snap = dm.build_snapshot(base_config(), Diagnostic(), NOW)
        table = dm.render_table(snap)
        for source in ("ledger", "activity"):
            self.assertIn("DOTS_ERROR " + source + " probe exit 0 (stderr)", table)
        self.assertNotIn("fixture-diagnostic", json.dumps(snap))


class TestEvidence(unittest.TestCase):
    def test_reference_shapes(self):
        refs, bare = dm.evidence_refs(
            "https://github.com/o/r/pull/17 @930b042b642946f8d3c5875c5df6d25905fe8288; "
            "https://github.com/o/r/issues/24#issuecomment-7086145047; "
            "https://github.com/o/r/commit/" + "b" * 40 + "; "
            "https://github.com/o/r/actions/runs/45068432009; "
            "https://github.com/o/r/blob/" + "c" * 40 + "/a/b.swift#L1-L2")
        kinds = [r["kind"] for r in refs]
        self.assertEqual(kinds, ["pull", "comment", "commit", "run", "commit"])
        self.assertEqual(len(bare), 1)

    def test_issue_comment_marker_must_be_in_the_fragment(self):
        gh = {"repos/o/r": ({"full_name": "o/r"}, None),
              "repos/o/r/issues/comments/7086145047": ({"id": 7086145047,
                  "html_url": "https://github.com/o/r/issues/24#issuecomment-7086145047"}, None)}
        resolver = dm.EvidenceResolver(base_config(), FakeTransport(gh=gh), NOW, [])
        for suffix, expected in (("issuecomment-1#2", "unresolved"),
                                 ("24#issuecomment-7086145047", "resolved")):
            with self.subTest(suffix=suffix):
                status, _, _ = dm.evidence_status("https://github.com/o/r/issues/" + suffix, resolver)
                self.assertEqual(status, expected)

    def test_search_url_is_a_repo_reference(self):
        refs, _ = dm.evidence_refs(
            "https://github.com/o/r/issues?q=is%3Aissue+is%3Aopen+label%3Arev-task")
        self.assertEqual(refs[0]["kind"], "repo")
        self.assertEqual(refs[0]["endpoint"], "repos/o/r")

    def test_pull_request_head_sha_is_proven_by_the_pull_request(self):
        gh = {"repos/o/r/pulls/17": ({"state": "open", "draft": False, "merged": False,
                                      "head": "9" * 40, "merge": None}, None)}
        resolver = dm.EvidenceResolver(base_config(), FakeTransport(gh=gh), NOW, [])
        status, detail, _ = dm.evidence_status(
            "https://github.com/o/r/pull/17 @%s" % ("9" * 40), resolver)
        self.assertEqual(status, "resolved", detail)

    def test_stale_head_is_detected(self):
        gh = {"repos/o/r/pulls/17": ({"state": "open", "draft": False, "merged": False,
                                      "head": "1" * 40, "merge": None}, None)}
        resolver = dm.EvidenceResolver(base_config(), FakeTransport(gh=gh), NOW, [])
        status, detail, _ = dm.evidence_status(
            "https://github.com/o/r/pull/17 @%s" % ("2" * 40), resolver)
        self.assertEqual(status, "stale")
        self.assertIn("head is", detail)

    def test_bare_sha_found_in_a_later_repository(self):
        sha = "d" * 40
        gh = {"repos/owner/repo/commits/%s" % sha: (None, "gh: No commit found for SHA (HTTP 422)"),
              "repos/other/repo/commits/%s" % sha: ({"sha": sha}, None)}
        resolver = dm.EvidenceResolver(base_config(repos=("owner/repo", "other/repo")),
                                       FakeTransport(gh=gh), NOW, [])
        status, detail, _ = dm.evidence_status("merged at %s" % sha, resolver)
        self.assertEqual(status, "resolved", detail)

    def test_unknown_sha_is_unresolved(self):
        resolver = dm.EvidenceResolver(base_config(), FakeTransport(), NOW, [])
        status, _, _ = dm.evidence_status("at %s" % ("e" * 40), resolver)
        self.assertEqual(status, "unresolved")

    def test_missing_evidence(self):
        resolver = dm.EvidenceResolver(base_config(), FakeTransport(), NOW, [])
        self.assertEqual(dm.evidence_status("", resolver)[0], "missing")

    def test_lookup_budget(self):
        config = base_config()
        config["github"]["max_lookups"] = 1
        resolver = dm.EvidenceResolver(config, FakeTransport(), NOW, [])
        dm.evidence_status("https://github.com/o/r/pull/1", resolver)
        status, detail, _ = dm.evidence_status("https://github.com/o/r/pull/2", resolver)
        self.assertEqual(status, "unknown")
        self.assertIn("budget", detail)


class TestWatchDedup(unittest.TestCase):
    def test_first_scan_reports_new_agent_then_nothing(self):
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                      activity_text=log_line(2))
        previous = {"agents": {}, "activity": {}, "errors": set()}
        events, state = dm.diff_events(previous, snapshot)
        self.assertEqual([e["event"] for e in events], ["DOTS_NEW_AGENT"])
        events, _ = dm.diff_events(state, snapshot)
        self.assertEqual(events, [])

    def test_classification_change_emits_once(self):
        working, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                     activity_text=log_line(2))
        stalled, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=45)}},
                                     activity_text=log_line(2))
        previous = {"agents": {}, "activity": {}, "errors": set()}
        _, state = dm.diff_events(previous, working)
        events, state2 = dm.diff_events(state, stalled)
        self.assertEqual([e["event"] for e in events], ["DOTS_STALLED"])
        self.assertEqual(events[0]["detail"]["from"], "WORKING")
        self.assertEqual(events[0]["dot"], "DOT-A")
        self.assertEqual(events[0]["agent"], "writer")
        again, _ = dm.diff_events(state2, stalled)
        self.assertEqual(again, [])

    def test_activity_transitions(self):
        idle, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                  activity_text=log_line(40))
        active, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                    activity_text=log_line(1))
        previous = {"agents": {}, "activity": {}, "errors": set()}
        _, state = dm.diff_events(previous, idle)
        events, state = dm.diff_events(state, active)
        self.assertIn("DOTS_ACTIVE", [e["event"] for e in events])
        events, _ = dm.diff_events(state, idle)
        self.assertIn("DOTS_IDLE", [e["event"] for e in events])

    def test_failed_activity_never_reports_quiet_or_idle(self):
        config = base_config(dots=("DOT-A", "DOT-B"), activity=[
            {"dot": "DOT-A", "host": "other-host", "path": "/app.log"}])
        files = {"DOT-A": {"writer.jsonl": record()}, "DOT-B": {}}
        working, _, _ = snapshot_for(files, activity_text=log_line(1), config=config)
        previous = {"agents": {}, "activity": {}, "errors": set()}
        _, prior_state = dm.diff_events(previous, working)
        for failure in ("exit 255: connection refused", "timed out after 5s", None):
            with self.subTest(failure=failure):
                transport = FakeTransport(files=files, activity_texts=[None],
                                          failures={"other-host": failure} if failure else {})
                failed = dm.build_snapshot(config, transport, NOW)
                table = dm.render_table(failed)
                self.assertIn("DOTS_ERROR activity", table)
                self.assertIn("DOT-A (account 17)  roster=?@? slots=0\n  activity unknown", table)
                self.assertIn("DOT-B (account 17)  roster=?@? slots=0\n  activity none configured", table)
                self.assertEqual(rows(failed)["writer"]["classification"], "ACTIVITY_UNKNOWN")
                self.assertIsNone(rows(failed)["writer"]["activity_since_record"])
                events, state = dm.diff_events(prior_state, failed)
                self.assertIn("DOTS_ERROR - - source=activity", "\n".join(map(dm.format_event, events)))
                self.assertFalse({"DOTS_IDLE", "DOTS_QUIET"} & {e["event"] for e in events})
                quiet, _, _ = snapshot_for(files, activity_text="", config=config)
                recovered, _ = dm.diff_events(state, quiet)
                self.assertIn("DOTS_IDLE", {e["event"] for e in recovered})

    def test_error_and_recovery_are_emitted_once(self):
        broken, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record()}},
                                    failures={"local": "exit 255: connection refused"})
        healthy, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record()}},
                                     activity_text=log_line(1))
        previous = {"agents": {}, "activity": {}, "errors": set()}
        events, state = dm.diff_events(previous, broken)
        self.assertIn("DOTS_ERROR", [e["event"] for e in events])
        again, state = dm.diff_events(state, broken)
        self.assertEqual([e["event"] for e in again], [])
        recovered, _ = dm.diff_events(state, healthy)
        self.assertIn("DOTS_RECOVERED", [e["event"] for e in recovered])

    def test_event_line_format(self):
        line = dm.format_event({"event": "DOTS_BLOCKED", "dot": "DOT-B",
                                "agent": "example-23-engine",
                                "detail": {"state": "blocked", "age": "16m",
                                           "task": "issue #23"}})
        self.assertTrue(line.startswith("DOTS_BLOCKED DOT-B example-23-engine "))
        self.assertIn('task="issue #23"', line)
        self.assertIn("age=16m", line)

    def test_table_contains_every_classification(self):
        snapshot, _, _ = snapshot_for({"DOT-A": {"writer.jsonl": record(minutes_ago=5)}},
                                      activity_text=log_line(1))
        table = dm.render_table(snapshot)
        self.assertIn("WORKING", table)
        self.assertIn("activity local", table)
        self.assertIn("writer", table)


if __name__ == "__main__":
    unittest.main(verbosity=2)
