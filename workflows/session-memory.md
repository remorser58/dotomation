# Session memory: what each agent is doing, readable without the dot

The dot's sub-agent threads cannot be read from outside the dot, so each agent keeps a short memory file on `local` next to its live records, and readers see its current state the way a project main session sees its own session. The main session, the coordinator and the owner's lead read it directly, and talk to the agent through [direct mail](direct-mail.md) where that is set up. The root does not relay any of it. The owner approves this per dot.

## Where it lives

`<work root>/.dots-ops/memory/<DOT>/<agentId>.json` on `local`, one file per agent, beside `live/<DOT>/<agentId>.jsonl`. `<agentId>` is the same id the agent uses in its live records, roster slot and mail folder.

- **One writer.** Only that agent writes its file. It rewrites the whole file each time; nobody else edits, truncates or deletes it.
- **Same moments as the live record.** The agent rewrites its memory whenever it appends a live line: `start`, each material step and at least every 15 minutes while working, `blocked` and `done`. The memory's `t`, `task`, `state` and `host` equal the line written with it.
- **History stays in the log.** The memory holds the current state only. The live log keeps every step; the memory keeps the newest 20 decisions of the current task. A new task starts a fresh memory.

## One file: memory-v1

[Schema](../protocol/memory-v1.schema.json). Thirteen fields, all present, nothing extra. A synthetic file is in [examples/session-memory.json](../examples/session-memory.json).

| Field | Meaning |
|---|---|
| `v`, `t` | Always `1`; RFC 3339 UTC time of the rewrite. |
| `dot`, `agent`, `project`, `task`, `state`, `host` | Copied from the live line written at the same time. |
| `goal` | What the assignment is for, inside the coordinator's brief. At most 500 characters. |
| `doing` | What the agent is doing now. At most 500 characters. |
| `next` | The next concrete step. At most 500 characters. |
| `decisions` | `{t, text}` choices the agent made on this task, oldest first. A choice reserved for the owner or the coordinator is asked for, not recorded here as made. |
| `blockers` | Open blockers, each naming the missing fact or capability. Empty when none; never empty while `state` is `blocked`. |

No secrets, credentials, tokens, personal identifiers, raw logs or unrelated source text, as in every other record in the shared folder.

## One call per step

The helper ships with this kit as `tools/dots.py`, the [second exception](../AGENTS.md#the-second-exception-toolsdotspy) to the instruction-only rule; run it from a checkout of the kit. A hand-written line and file that match the schemas have the same effect, and an instance that already keeps its own copy of the helper can keep using it.

- **Agent:** `dots.py tick <DOT> <agentId> <state> --task T --host H --note N --evidence E [--role R --model M --project P] [--goal G --doing D --next X] [--decision D]... [--blocker B]... [--unblock] [--took <mail id>]`. It checks every value first and writes nothing if one fails. Then it appends the live line, rewrites the memory, moves the agent's own mail read marker when `--took` names a message, and prints the agent's unread mail and the mail it has read but not answered, from every project mailbox under that agent id. `role`, `model` and `project` are required on the agent's first tick and otherwise default to its newest v1 line. `goal`, `doing` and `next` are required on the first tick of a task and otherwise carry over. Ticks in one dot run one at a time, so two ticks never lose each other's decisions. Answering mail is still `mail.py reply`; a tick never sends a message.
- **Readers:** `dots.py board [DOT...] [--json]` shows each agent's newest line and its age, host, roster state, `doing`, `next`, open blockers and mail counts, flagged `STALLED`, `BLOCKED`, `NO-LIVE`, `NO-MEMO`, `MEMO-BEHIND`, `MAIL-UNANSWERED`, `MAIL_WAITING` or `CLAIM-STALE`. An agent with a live file but no v1 line is `NO-LIVE` and also `STALLED` unless an off-format line is under 30 minutes old, as in [live status](live-status.md#legacy-lines). A roster slot whose agent id is not a token is skipped and counted, and a dot without a live folder is reported; `--json` prints `{"t", "agents", "errors"}` so those reports never mix with agent rows. The roster value prints as `roster=<state> (the root's claim)`, and `CLAIM-STALE` with its `claim:` line names the contradiction between that claim and the agent's own newest live line. `dots.py memo <DOT> <agentId>` prints one memory and the last three live lines. Both only read.
- **Steering:** `dots.py send`, `wait`, `reply`, `inbox` and `read` carry [direct mail](direct-mail.md#steering-send-wait-reply) and hold the wait open for a new instruction; `wait` moves the same `.agent-read` marker `tick --took` moves, so the lines it printed are not printed again. `reply` for a `result`, `question` or `block` continues into that same loop and prints the answer as its own output (`--no-wait` opts out), and `tick done` is refused while an inbox line still owes an answer. `init` writes the folder's `dots.json` once per machine, and `watch` is the owner's fallback for owed mail whose agent holds no wait ([if the dot root is not trusted](direct-mail.md#if-the-dot-root-is-not-trusted)). None of them changes what a record looks like.

One tick replaces three separate calls (append the line, rewrite the memory, read the inbox), so an agent spends fewer tool calls per step and cannot record a step without seeing its mail.

## How readers use it

- **Main session.** Before sending a `task` or `review`, read the agent's memory. A question the memory already answers does not need a message. If `doing` shows the agent is on something else, say which work comes first in the message itself; the agent answers in its outbox.
- **Coordinator.** Read the board each round with the roster. A `MEMO-BEHIND` or `NO-MEMO` flag on a working agent is a missing record, handled like a missing live line. A stall is still judged from the live line, not from the memory.
- **Replacement after a stall or VM reset.** The coordinator briefs the replacement with the previous agent's memory file path and its last live lines. The replacement reads them, then reads the actual target (branch, SHA, file) before any mutation and continues from the last confirmed step only. It starts its own memory under its own id; it never writes the previous agent's file.
- **Lead and owner.** `dots.py board` through the approved route to `local` is the one-call status view.

## What it is not

- It is the agent's own summary, like the live record. It is not a `submit`, a `check`, gate evidence or a closure, and it does not prove the agent did what `doing` says. [Monitoring](monitoring.md) still compares claims with real tool activity.
- When the memory and the live log disagree, the newer live line is the record; the board shows `MEMO-BEHIND`.
- It is not a message route. A `next` that mentions another agent does not reach that agent; mail or the native exchange does.
- It is files in the shared folder on `local`: no service, watcher, scheduler, hook, dashboard or telemetry. The route grants nothing; the owner's approval for that dot is the only grant.
