# Session memory: what each agent is doing, readable without the dot

On 2026-11-23 (KST) the owner asked to see each dot sub-agent's session the way a project main session sees its own. The dot's sub-agent threads cannot be read from outside the dot, so each agent keeps a short memory file on Osaka next to its live records. The main session, the coordinator and the owner's lead read it directly, and talk to the agent through [direct mail](direct-mail.md) where that is set up. The root does not relay any of it. The owner approved this for REM17 and REV18.

## Where it lives

`~/CodeProjects/.dots-ops/memory/<DOT>/<agentId>.json` on Osaka, one file per agent, beside `live/<DOT>/<agentId>.jsonl`. `<agentId>` is the same id the agent uses in its live records, roster slot and mail folder.

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

No secrets, credentials, tokens, personal identifiers, raw logs or unrelated source text, as in every other Osaka record.

## One call per step

The instance keeps a helper, `dots.py`, at `~/CodeProjects/.dots-ops/dots.py`. Like `mail.py`, it is instance setup the owner asked for, not part of this repository, and a hand-written line and file that match the schemas have the same effect.

- **Agent:** `dots.py tick <DOT> <agentId> <state> --task T --host H --note N --evidence E [--doing D] [--next X] [--decision D]... [--blocker B]... [--unblock] [--took <mail id>]`. It checks every value first and writes nothing if one fails. Then it appends the live line, rewrites the memory, moves the agent's own mail read marker when `--took` names a message, and prints the agent's unread mail and the mail it has read but not answered, from every project mailbox under that agent id. `role`, `model` and `project` default to the agent's newest v1 line; a new task also needs `--goal`, `--doing` and `--next`. Answering mail is still `mail.py reply`; a tick never sends a message.
- **Readers:** `dots.py board [DOT...] [--json]` shows each agent's newest line and its age, host, roster state, `doing`, `next`, open blockers and mail counts, flagged `STALLED`, `BLOCKED`, `NO-LIVE`, `NO-MEMO`, `MEMO-BEHIND` or `MAIL-UNANSWERED`. `dots.py memo <DOT> <agentId>` prints one memory and the last three live lines. Both only read.

One tick replaces three separate calls (append the line, rewrite the memory, read the inbox), so an agent spends fewer tool calls per step and cannot record a step without seeing its mail.

## How readers use it

- **Main session.** Before sending a `task` or `review`, read the agent's memory. A question the memory already answers does not need a message. If `doing` shows the agent is on something else, say which work comes first in the message itself; the agent answers in its outbox.
- **Coordinator.** Read the board each round with the roster. A `MEMO-BEHIND` or `NO-MEMO` flag on a working agent is a missing record, handled like a missing live line. A stall is still judged from the live line, not from the memory.
- **Replacement after a stall or VM reset.** The coordinator briefs the replacement with the previous agent's memory file path and its last live lines. The replacement reads them, then reads the actual target (branch, SHA, file) before any mutation and continues from the last confirmed step only. It starts its own memory under its own id; it never writes the previous agent's file.
- **Lead and owner.** `dots.py board` over `ssh osaka` is the one-call status view.

## What it is not

- It is the agent's own summary, like the live record. It is not a `submit`, a `check`, gate evidence or a closure, and it does not prove the agent did what `doing` says. [Monitoring](monitoring.md) still compares claims with real tool activity.
- When the memory and the live log disagree, the newer live line is the record; the board shows `MEMO-BEHIND`.
- It is not a message route. A `next` that mentions another agent does not reach that agent; mail or the native exchange does.
- It is files on the shared Osaka folder: no service, watcher, scheduler, hook, dashboard or telemetry. The route grants nothing; the owner's 2026-11-23 approval above is the only grant.
