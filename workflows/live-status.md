# Live records: seeing Dots internal work as it happens

The owner opens one folder on `local` and sees which internal agent is doing what, on which host, with which model requested, and how long ago. It is an append-only log written by the agents themselves — not a service, dashboard, broker, watcher or telemetry. The one read-only tool that reads it is [monitoring](monitoring.md); it writes nothing here.

## Where the record lives

`local`, in the shared folder `<work root>/.dots-ops/`:

- `handoff/` — cross-dot handoffs, human-readable.
- `live/<DOT>/` — the live records, one folder per dot.
- `tasks/` — approved task records.

Every root, coordinator, worker and reviewer appends to `live/<DOT>/<agentId>.jsonl`, using the same `agentId` it uses in messages and in the roster. Lines are append-only: no agent edits another agent's file or rewrites an earlier line; a correction is a new line. Only the coordinator writes `roster.json`.

Creating the folder and appending a line are ordinary filesystem operations on the approved `local` account (`mkdir -p live/<DOT>`, `>>`). The kit installs nothing: no service, watcher, scheduler, hook, dashboard or collector.

## One line, ten fields

```json
{"t":"2026-11-22T20:37:55Z","agent":"example-lead","role":"총괄","model":"requested gpt-6.2-astra ultra; runtime does not display effective","project":"example-project","task":"task intake","state":"step","host":"local","note":"Open task issues: 0. No assignment made.","evidence":"https://github.com/example/app/issues"}
```

[Schema](../protocol/live-record-v1.schema.json). Exactly these ten field names, all present, nothing extra; `t` is required. `state` is the only enumerated field. `role`, `model`, `project` and `host` accept any non-empty single line so that records already in the folder stay readable, and the table below gives the wording to use from now on. A synthetic start-to-done sequence is in [examples/live-records.json](../examples/live-records.json).

| Field | Required shape | Wording to use |
|---|---|---|
| `t` | RFC 3339 date-time, e.g. `2026-11-22T20:37:55Z`. Fractional seconds and `+00:00` are accepted. | UTC. A line that uses `timestamp`, `time` or `at` instead of `t` is not a live record. |
| `agent` | Stable id token. | The same id used in messages and in the roster. |
| `role` | One non-empty line. | One of 총괄 (lead/coordinator), 구현 (implementation), 연구 (research), 디자인 (design), 디버깅 (debugging), 검토 (review). One record names one role. |
| `model` | One non-empty line. | `confirmed gpt-6.2-astra xhigh` only when the runtime displays the model and effort; otherwise `requested gpt-6.2-astra ultra; runtime does not display effective`, naming what the summon asked for. A role name, a prior run or a coordinator's statement is never the value. |
| `project` | One non-empty line. | The project the lane belongs to, one id per project. A lane that spans two projects appends one record per project rather than a combined value. |
| `task` | One non-empty line. | The approved task or its `work_id/revision`. Keep it stable for the whole task so the lines group. |
| `state` | `start`, `step`, `blocked` or `done`. | — |
| `host` | One non-empty line. | `vm` (Dots VM) or `local` (the local computer). The host actually used, confirmed by hostname — not where the agent was launched from. |
| `note` | One non-empty line. | What changed, what was observed, what is missing. No secrets, credentials, tokens, personal identifiers, raw logs or unrelated source text. |
| `evidence` | One non-empty line. | A path or link to the exact artifact: commit and path, PR, message id, file path. "It worked" is not evidence. |

## Legacy lines

Lines already in the folder may use `timestamp` instead of `t`, miss a field, or spell a role, host, project or model in the wording of the day. They stay exactly as written: never rewritten, reformatted, migrated or deleted, because the log is append-only. Read them as unconfirmed — they still show what that agent said at that time, but they are not v1 records, they do not prove a step, and they cannot be cited as a confirmed setting. A stall clock runs from that agent's newest v1 line. An agent with a live file but no v1 line is checked as stalled once its newest off-format line with a readable time (`t`, `timestamp`, `time`, `at` or `datetime`) is over 30 minutes old, and at once when no line has one. The correction is a new v1 line, not an edit of the old one.

## When to append

- `start` when taking up the brief, before the first workspace mutation.
- `step` at each material step, and at least every 15 minutes while working. A long tool run gets a `step` before it and a `step` after it.
- Each time it appends a line, the agent also rewrites its own [session memory](session-memory.md) with the same `t`, `task`, `state` and `host`. The helper [`dots.py`](../tools/dots.py) `tick` does both in one call and prints unread [direct mail](direct-mail.md#steering-send-wait-reply) first, before the live line, so a new instruction steers the agent before its next step.
- An agent with no assigned work does not end its turn: it holds `dots.py wait` open and appends a `step` each time that wait times out, until the coordinator releases it ([direct mail](direct-mail.md#steering-send-wait-reply)). A wait that is blocking is not a stall; the stall clock still runs from the newest line, so the agent's own timeout keeps it fresh. `wait` exits 3 on that timeout and 4 when a signal ends it, and in both cases the agent starts another wait; the lines it printed are already marked read, so nothing is handled twice. An agent that hands its work back through `dots.py reply` stays in the same loop until the main session answers, and `dots.py tick ... done` is refused while an inbox line still owes an answer, so a `done` cannot be the last thing an agent does while a review is unanswered.
- `blocked` when the agent cannot proceed: missing input, denied route, owner pause, or a suspected VM reset (see below).
- `done` when the assigned output is submitted or nothing further can be done. `done` does not close the task; only the coordinator closes, under the handoff rules. A later `review` that reopens the work is answered with a new `step`; on [direct mail](direct-mail.md) an accepting `answer` gets one more `done`.

A working agent with no new line for 30 minutes is **stalled**. The coordinator checks that agent directly — alive, waiting on a peer, host gone — and reassigns the slot. A stall is not evidence of a VM reset, and a reset suspicion is not a stall.

## Roster

`live/<DOT>/roster.json` is the coordinator's single-writer summary, rewritten after reading the new lines each round:

```json
{
  "dot": "DOT-A",
  "updated_at": "2026-11-22T20:37:25Z",
  "coordinator": "example-ultra",
  "slots": [
    {
      "agent": "example-root",
      "role": "총괄",
      "model": "requested gpt-6.2-astra xhigh; runtime does not display effective",
      "task": "root approvals and verified handoffs",
      "state": "running",
      "last_record_at": "2026-11-22T20:34:39Z"
    }
  ],
  "unassigned": 1,
  "method": "optional one line on the improvements this coordinator adopted"
}
```

- `slots` carries agent id, role, model, task, state and the `t` of that agent's latest line (`null` when there is none). `unassigned` counts free slots.
- `method` is optional and describes the method the coordinator adopted; it is not a second log.
- No secrets, and no per-agent history — that is the jsonl.
- A slot without a live file, or a live file without a slot, is a discrepancy to reconcile before reporting status, not something to hide by deleting either.

## Reading it

The owner, or any authorised reader, reads `live/<DOT>/` read-only through the approved route for that account ([hosts](hosts.md#approved-routes)). Show the roster, then the newest line of each agent, then the stalls. Do not edit, truncate or reorder another dot's files.

A self-report alone does not show that an agent is working. [Monitoring](monitoring.md) reads these files and compares them with the Web GPT Agent daemon request log of the dot's account, and resolves each `evidence` reference against GitHub: `python3 tools/dots_monitor.py snapshot`. It is read-only, and a `WORKING` there still means only "recent claim and real tool calls", not a verified result.

What the record establishes: the last confirmed step, the host actually used, and the model and effort that were requested. What it does not establish: the effective model or effort when Dots does not display them, the content of a tool run, that a gate passed (the handoff check does that), or that an agent is idle — a missing line means the agent has not recorded.

## The VM reset note

When a reset is suspected, the affected agent writes `state: "blocked"` with `note: "vm reset suspected"` and the observation as `evidence`, keeps the job on HOLD, and follows the [VM watchdog](vm-watchdog.md). The coordinator does not move that work to another host on the strength of the note alone.
