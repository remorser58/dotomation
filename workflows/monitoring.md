# Monitoring: is the dot actually working?

The [live records](live-status.md) are self-reports. `tools/dots_monitor.py` reads them and compares them with something the agent does not write about itself: the Web GPT Agent daemon log of the account that dot runs on, which records every MCP request the browser actually made. It also opens the GitHub reference in each record's `evidence` field and says whether that pull request, commit, issue, comment or workflow run exists.

It is one read-only file. No service, no install, no hook, no credential, no telemetry, and nothing is ever written to a host it reads.

## Run it

```bash
python3 tools/dots_monitor.py --config <instance config> snapshot            # human table
python3 tools/dots_monitor.py --config <instance config> snapshot --json     # same data as JSON
python3 tools/dots_monitor.py --config <instance config> watch --interval 60 # one line per change
```

Python 3.9 or newer, standard library only. `ssh` is used for a remote ledger or remote daemon log; `gh` is used to resolve evidence. A missing `gh` degrades evidence to `unknown` and is reported as `DOTS_ERROR`, not silently ignored. `snapshot` exits `0` when every source answered and `2` when at least one source failed, including a live directory missing for a configured dot. `watch` prints the failure as a `DOTS_ERROR` line, keeps running, and exits `0` on Ctrl-C.

## What it reads

| Source | Where | What it establishes |
|---|---|---|
| Live records | `<work root>/.dots-ops/live/<DOT>/*.jsonl` and `roster.json` on `local` | What each agent says it is doing, and when it last said it |
| Daemon requests | The Web GPT Agent daemon `app.log` of each account in the config's `activity` list, on `local` or on a host reached with `ssh` | How many `POST mcp/core` requests that account actually made in the last 15 and 60 minutes |
| Evidence | GitHub through `gh api` | Whether the cited pull request, commit, issue, comment or run exists and is in the state the note claims |

Only the last 256 KiB of each file is read, so a multi-gigabyte daemon log costs the same as a small one. A byte tail that starts mid-line drops that line instead of inventing one. Every `ssh` and `gh` call has a timeout, and a failed source becomes a `DOTS_ERROR` line rather than a wrong answer.

Each dot's scan considers only the first 200 `.jsonl` files in filename order and still reads `roster.json` separately. The monitor reports overflow and unreadable files as `DOTS_ERROR`. The limit is per dot, not per probe: two dots can supply 400 live files plus two rosters. If the byte count or tail fails after the permission check, the monitor discards the incomplete file; unexpected probe stderr invalidates the source without displaying its contents.

Tunnel probes (`mcp/core` requests marked `tunnel probe`), `mcp/desktop` and `mcp/plugins` requests are not counted as work. A daemon log that starts inside the 60-minute window is reported with `partial_window` so a low count is not mistaken for idleness.

## Classes

| Class | Meaning |
|---|---|
| `WORKING` | Newest v1 record is at most 30 minutes old and the dot's account made at least one `mcp/core` request at or after that record |
| `QUIET` | Newest record is at most 30 minutes old and the activity sources that answered show no tool request since it |
| `ACTIVITY_UNKNOWN` | Newest record would otherwise be `WORKING` or `QUIET`, but no activity source answered |
| `STALLED` | Newest record is `start` or `step` and older than 30 minutes, per [live-status](live-status.md#when-to-append); also an agent with only off-format lines whose newest readable timestamp is older than 30 minutes, or that has no readable timestamp at all |
| `BLOCKED` | Newest record is `blocked`; reported whatever its age |
| `DONE` | Newest record is `done` and its evidence resolves, or GitHub could not be asked (`unknown`, also printed as `DOTS_ERROR github`). **Without `gh`, or with `--no-github`, every `done` that cites a SHA or URL shows as `DONE`; a `done` whose evidence is only a local label stays `UNPROVEN`**: read the `EVID` column, not the class |
| `UNPROVEN` | A `done` whose evidence is missing, only a local label, `stale`, or cites something that does not exist; a `start` or `step` whose evidence is missing or does not exist. A local label on a `start` or `step` is not checked |
| `LEGACY` | The agent has only off-format lines and the newest readable timestamp among them is at most 30 minutes old. Shown as unconfirmed, never rewritten, never counted as a step |
| `NO_RECORD` | A roster slot with no live file |

The 30 minutes are `thresholds.stall_minutes` (or `--stall-minutes`); the legend under the table prints the value in use.

`EVIDENCE` is one of `resolved` (the reference exists), `stale` (a cited pull-request head SHA is no longer that pull request's head or merge commit; a `done` on it is `UNPROVEN`), `local` (a bare label such as `MATCH-CHECK`, not checkable), `unresolved` (the reference is malformed or GitHub says it does not exist), `unknown` (not checked: no `gh`, network failure, or the lookup budget ran out), or `missing`.

On a [direct mail](direct-mail.md) task, a `done` or `step` that cites a SHA not yet published to GitHub is `UNPROVEN` by design until the main session publishes it. For that window, read the agent's newest `result` and the main session's `review` or `answer` in `mail/<agentId>/` to tell a pending handover from bad evidence.

## Daemon Health (`--daemon-health`) & Large Rosters

- `limits.roster_max_bytes` defaults to `1048576` (1 MiB), separate from `limits.tail_bytes` (`262144`), so large coordinator `roster.json` files are read intact without truncation; rosters exceeding `roster_max_bytes` report an explicit size error instead of `"not JSON"`.
- Passing `--daemon-health` on `snapshot` or `watch` (or setting `"daemon_health": {"enabled": true}`) extends the read-only scan to `<data_dir>/sessions/*/events.jsonl` (or an explicit `daemon_health.sessions_dir`) modified within `window_minutes` (default 30, bounded by `max_session_files=20` and `tail_bytes=131072`):
  - **Retained terminal session exhaustion:** matches the exact suffix signature emitted when a Web GPT Agent daemon hits its retained terminal session cap (`too many retained terminal sessions`), without false-positiving on commands that merely grep or mention the string (`DOTS_DAEMON_LIMIT` in `watch`, `ALERT daemon` in `snapshot`).
  - **Chat errors:** counts recent `chat_error` events (`DOTS_CHAT_ERROR` in `watch`), and emits `DOTS_DAEMON_RECOVERED` when cleared. Note: `build_snapshot` does not auto-restart or modify daemon processes; opt-in cooldown-guarded restarts belong strictly to [`tools/dots_pilot.py`](../tools/dots_pilot.py) when explicitly enabled by the owner.
  - **`START-CHURN`:** `snapshot` flags consecutive `start`-only thrash without `step`/`done`, and `watch` emits `DOTS_START_CHURN`.
- **Retained-session self-heal pattern:** If an external operator daemon monitor restarts a unit on retained-session exhaustion, guard it with a non-blocking single-instance file lock (`flock -n`) and a per-unit cooldown window (e.g. >=10 minutes) so dropped SSH connections never leave duplicate killers racing the daemon.

## What the activity counts do and do not prove

They are per **account and host**, not per agent. A dot whose account is calling tools is really working; the count cannot say which sub-agent made the call. That is why the class needs both halves: the record names the agent and the task, the request count shows the account was alive at that time. Two dots on the same account share the count; the config keeps them separate by log path.

A request count is not a result. It does not show what was run, whether a gate passed, or that a PR is correct — the handoff checks and the reviewer do that. `WORKING` means "this dot is not idle and its newest claim is recent", nothing more.

The table says `activity unknown` when sources are configured but none can be read, and `activity none configured` when the dot has no activity sources. In both cases the JSON snapshot reports `activity_since_record` as `null` rather than zero.

## Config

`tools/dots-monitor.example.json` is the template, and an instance keeps its own filled-in copy outside this repository: host names, paths, account numbers and repository names only, no secrets. Pass your copy with `--config PATH`, before the subcommand as in [Run it](#run-it). Without `--config`, the monitor looks beside the script for `dots-monitor.owner.json` first (ignored by Git in this checkout), then the example template. A missing or unreadable config produces `config not found: <path>`. `--no-github` skips evidence resolution; `--stall-minutes N` changes the stall threshold.

```json
{
  "ledger": {"host": "local", "root": "<work root>/.dots-ops/live", "dots": ["DOT-A", "DOT-B"]},
  "activity": [{"dot": "DOT-A", "host": "local", "label": "local", "path": "~/.local/state/web-gpt-agent/app.log"}],
  "github": {"enabled": true, "repos": ["owner/repo"], "max_lookups": 60}
}
```

`"host": "local"` reads the file on the machine running the tool; any other value is used as an `ssh` destination.

## Watch

`watch` prints one line per change and nothing when nothing changed, so a line-filter monitor can key on the prefix:

```text
DOTS_STALLED DOT-A example-23-engine from=WORKING state=step age=34m evidence=resolved task="issue #23" note="..."
DOTS_QUIET DOT-A example-ultra from=WORKING state=step age=6m evidence=resolved task=DOT-A-RESEARCH+DRAFTS/r1 note="..."
DOTS_WORKING DOT-A example_item_owner from=UNPROVEN state=start age=4s evidence=resolved task=EXAMPLE-62438-Q29-MATCH/r2 note="..."
DOTS_BLOCKED, DOTS_DONE, DOTS_UNPROVEN, DOTS_ACTIVITY_UNKNOWN, DOTS_LEGACY, DOTS_NO_RECORD: same fields
DOTS_NEW_AGENT DOT-A example_drafts class=WORKING evidence=local role=구현 task=DOT-A-DRAFTS/r1
DOTS_ACTIVE DOT-B - calls_15m=15          # dot went from no tool request in 15 minutes to calling tools
DOTS_IDLE DOT-B - calls_15m=0             # no tool request in the last 15 minutes any more
DOTS_ERROR - - source=activity detail="exit 255: ssh: connect to host example-host port 22: Connection refused"
DOTS_RECOVERED - - source=activity
DOTS_DOT DOT-A - calls_15m=16             # first scan only, one per dot
```

`DOTS_MONITOR started ...` opens the stream. `--json` prints one JSON object per line with the same `event` name.

An unread activity source cannot trigger `DOTS_QUIET` or `DOTS_IDLE` if no other source for that dot answered. Watch compares the next successful count with the last observed count's active/idle state. The initial `DOTS_DOT` omits `calls_15m` if no activity source answered.

## Boundaries

- Read-only: the remote side runs only `wc -c` and `tail -c` over the configured files. The tool never appends, edits, renames or deletes a record, and writes nothing on any host it reads; output goes to the reader's terminal. The `ssh` host is passed after `--`, so a config value cannot become an `ssh` option.
- Framing: every section marker in the remote dump carries a random nonce chosen for that run, so a record, note or log line that contains `==END` or `==DOT DOT-A` stays file content and cannot move lines into another file or dot.
- Agent ids: a roster slot's `agent` and a live file's name must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`, like a record's `agent`. Anything else is dropped and counted in a `DOTS_ERROR roster` or `DOTS_ERROR ledger` line; the dropped text itself is never printed.
- A configured dot whose live directory is missing on a reachable host is `DOTS_ERROR ledger`, not an empty table.
- Not resident: `watch` is a foreground command the reader starts and stops. Nothing installs, schedules or restarts it.
- It is not a coordinator. It reports; a coordinator still decides to reassign a stalled slot or to close a task.
- It does not contact a dot. Nothing here sends a message to a dot's chat, and nothing assigns work.
- It prints no secret. Values are redacted before display, in the table, in `--json` output and in `watch` lines and JSON events; the JSON paths redact every string of the document recursively. Covered: GitHub tokens, `sk-` keys, `token=`/`api_key=` pairs (including prefixed names such as `DATABASE_PASSWORD=` and `OPENAI_API_KEY=`), `Authorization: Bearer` tokens, MCP path tokens, and any long mixed-case alphanumeric run. Commit SHAs and hyphenated task ids survive.
- Legacy lines are shown, counted and left exactly as written, because the log is append-only.
- Tests: `python3 tools/test_dots_monitor.py` — stdlib `unittest`, synthetic fixtures, a fake transport and a fake `gh`; no network. Some tests run the generated dump scripts with the local `/bin/sh` against temporary files.
