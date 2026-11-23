# Direct mail: a project's main session and a dot's sub-agents

A project's main session talks with a dot's sub-agents directly, not through the dot root, and sub-agents hand code over without GitHub. The main session is an agent session that runs outside Dots, on the user's own machine, such as a project's own OMP session. The root does not relay this traffic. It still summons agents, and the coordinator still keeps the roster and closes tasks. On the mail path the root does exactly two things: it wakes an idle agent when the main session rings the doorbell, and it reads the mail folder read-only for status and closure. The main session's own GitHub push and pull request rest on the user's instruction to that session, not on this file.

Instance setup: project `<project>`, dot `<DOT>`, main session `<project>-main`, and the project's outgoing `rev-out` repository on `local`. The kit names these generically; an instance fills in its own project, dot, session name and repository, and keeps that mapping in its own notes rather than in this file.

## Where it lives

All on `local`, under `<work root>/.dots-ops/<project>/`:

| Path | Written by | Content |
|---|---|---|
| `mail/<agentId>/in.jsonl` | the main session only | messages to that agent |
| `mail/<agentId>/out.jsonl` | that agent only | messages to the main session |
| `mail/<agentId>/.agent-read` | that agent only | the `id` of the last `in.jsonl` line it has read |
| `mail/<agentId>/.main-read` | the main session only | the `id` of the last `out.jsonl` line it has read |
| `mail/README.md` | the instance owner | short guide for that instance |

`<agentId>` is the same id the agent uses in its live records and in the roster. Lines are append-only: no one edits or deletes a line, and a correction is a new line. A read marker names a line, not a point in time. The unread messages are the lines after the line it names, in file order; ids are never compared with each other.

A Dots agent reads and appends these files through the approved route to `local` for its account ([hosts](hosts.md#approved-routes)), whatever workspace the agent's own work runs in. The main session runs outside Dots and uses its own access to `local`. An instance may keep a helper, `mail.py`, next to the folder. It appends a valid line, lists unread messages (`mail.py new <agentId>`) and moves the agent's read marker (`mail.py done <agentId> <message id>`). It is instance setup, not part of this repository. A hand-written line that matches the schema has the same effect.

## One line: mail-v1

[Schema](../protocol/mail-v1.schema.json). Ten fields, all present, nothing extra: `v` (always `1`), `id`, `t`, `from`, `to`, `task`, `kind`, `re`, `text`, `refs`. A synthetic exchange is in [examples/direct-mail.json](../examples/direct-mail.json).

| Field | Meaning |
|---|---|
| `id` | Opaque and unique, for example `m-20261123T001500Z-4f2a`. It is not an ordering. |
| `t` | RFC 3339 UTC time of the append. |
| `from`, `to` | The main session's name and the agent id. One real recipient per message. |
| `task` | The approved task, the same value the agent's live records use. |
| `kind` | Main session to agent: `task`, `question`, `answer`, `review`, `note`. Agent to main session: `ack`, `answer`, `question`, `result`, `block`, `note`. |
| `re` | The `id` of the message this answers, or `null`. Every message an agent sends carries `re`. A new question from the agent uses the last message it received. |
| `text` | Plain text. No secrets, credentials, tokens, personal identifiers or raw logs. |
| `refs` | Exact references: a `rev/*` branch, a full commit SHA, `local:<workspace path>`, a path, a link, `gate:<ID>`, or `supersedes:<message id>`. |

## How it maps to the handoff rules

Direct mail is the addressed message and reply route that [direct exchanges](../.agents/skills/dots-superkit/SKILL.md#direct-exchanges-not-another-queue) allows for an approved worker surface. For the tasks it carries, the main session is the named verifier. The handoff event rules apply as follows:

- **Gates.** The coordinator's brief to the agent holds the mandatory gate list. The agent's first message on a task is an `ack` that copies each gate ID and its pass condition, one `gate:<ID>` per gate in `refs`, so the main session checks against the same list. A main-session `task` that adds work inside the assignment names any new gate the same way. A message never removes a gate.
- `re` is `reply_to`. A message is never authorization to widen scope. A decision that belongs to the owner goes to the owner through the main session.
- `task` from the main session is a follow-up inside the agent's current assignment. New work still starts as an issue the dot's coordinator assigns. After the assignment, the main session and the agent talk directly.
- `result` is a **submit**. It names the exact target: the `rev/*` branch and full commit SHA, plus `local:<workspace path>` when the commit stays in a workspace instead of being pushed. It also lists the checks the agent ran with their observed results, and what it did not check. A replacement is a new `result` with `supersedes:<previous result id>` in `refs`. The agent's own tests are writer self-tests, not independent review.
- `review` from the main session is a **check**. It answers one `result` id and SHA. It gives one verdict per gate, `gate:<ID> PASS|FAIL|NOT_RUN`, with the observed output or its location, and the reason for each FAIL or NOT_RUN. For a FAIL it tells the agent what to fix.
- `answer` that accepts a `result` is the final check. It gives PASS for every gate on that SHA, names the SHA the main session published, and names the pull request. A rejection is a `review` with at least one FAIL. A rejection never closes anything.
- **Coordinator receipt.** The coordinator reads `mail/<agentId>/` read-only. That copy covers the submit chain and the verdicts, so the agent does not have to send the coordinator a second copy. The direct agent-to-main-session leg is still the one that counts.
- **Close.** Only the coordinator closes, under the [close rule](../.agents/skills/dots-superkit/SKILL.md#events-and-evidence). It closes only after an accepting `answer` on the newest `result`. Before closing, it reads that answer's per-gate verdicts and the published target, compares the gate IDs with the mandatory list in its own brief (a missing gate is NOT_RUN, never PASS), and confirms there is no open `block`, pause or newer owner direction. After a rejection the task stays open, and the coordinator records which gate failed.
- `block` names the missing fact or capability and the next safe step. A message is never a resume after an owner pause.

## Delivering code

- **Workspace with an approved push route:** commit on `rev/<topic>-<date>` and push only that branch to the project's outgoing repository on `local` with `git push <rev-out path> rev/<topic>-<date>`. The repository accepts `refs/heads/rev/*` only and refuses `loop/*`, `main`, `release/*` and tags. It borrows objects from the read-only mirror, so a push adds only the new objects.
- **No approved push route from that workspace:** commit on `rev/<topic>-<date>` inside the approved work root and push nowhere. The `result` names `local:<absolute workspace path>`, the branch and the full SHA. The main session fetches that exact commit from that workspace. Do not route around the missing push route with a remote login, another account or a copy of the canonical tree.
- Then send a `result`.
- Sub-agents on this route do not push to GitHub or open pull requests for the project, and that includes the GitHub connector. The main session fetches the exact SHA, verifies it in its own environment, pushes that same SHA to GitHub `rev/<topic>-<date>` and opens the pull request.
- **Approval.** Assigned sub-agents may commit `rev/*` branches in their approved workspaces and push them to the project's outgoing repository only where the owner approved that route for that project. Every other commit, push, merge or external write still needs its own approval.

## Steering: send, wait, reply

An agent's conversation cannot be read or written from outside the dot, so a new instruction from the main session is a file the agent reaches through its own approved route, plus a command the agent runs that returns as soon as that file changes. `tools/dots.py` in this kit does both ([session memory](session-memory.md#one-call-per-step)); run it from a checkout, or copy it beside the shared folder. Its `init` command writes `<base>/dots.json` and creates `live/<DOT>/`, `memory/<DOT>/` and `<project>/mail/`, then prints the brief to paste to each dot root ([quick start](../README.md#빠른-시작-로컬-한-대)).

| Side | Command |
|---|---|
| Main session | `dots.py send <project> <agentId> <kind> <task> [--re ID] [--ref R]...` — text on stdin; prints the message id, then the agent's listening status |
| Main session | `dots.py inbox [project] [agentId]` — agent replies not yet marked read; `dots.py read <project> <agentId> <id>` marks them read |
| Agent | `dots.py wait <DOT> <agentId> [--timeout SECONDS]` — default 3300 seconds (55 minutes) |
| Agent | `dots.py reply <project> <agentId> <kind> <task> --re ID [--ref R]...` — text on stdin |

`send` writes one [mail-v1](../protocol/mail-v1.schema.json) line into `in.jsonl` and `reply` one into `out.jsonl`, exactly as a hand-written line does, so `mail.py` or any reader of the schema reads them unchanged. `from` is the project's main-session id from `dots.json` (`<project>-main` when the config names none). Delivery then depends on what the agent is doing:

1. **The agent is in `wait`.** The line is printed the moment it lands — typically within a second — and `wait` exits 0. This is the only near-real-time path, and it is what steering means here: the agent is holding one tool call open, so an instruction reaches it mid-task instead of at the end of a step.
2. **The agent is working.** It sees the line at the top of its next `tick`, printed before the live line under a `MAIL_FIRST ... handle these before your next step` header. A working agent ticks at each material step and at least every 15 minutes, so that is the worst case while it is working.
3. **The agent is idle.** Nothing reads the file until it runs a command again. `send` prints `LISTENING` with the waiting process, or `NOT_LISTENING last_tick=<age>`. When it is not listening and `dots.json` holds a wake command, `send` runs that command once, prints its exit code, and does not retry. Otherwise it prints the manual doorbell below.

The wake command is whatever the instance already has to post one line into that dot's chat: an argv list, no shell, `{project}`, `{agent}`, `{id}` and `{dot}` substituted. It is not part of this kit and the kit cannot check that it exists.

While `wait` blocks it holds `<base>/listen/<agentId>.json` — `{pid, dot, since, until, host}` — and removes it on any exit, including `SIGTERM`, `SIGHUP` and `SIGINT`. A marker whose process is gone, or whose `until` has passed, counts as not listening, so a waiter that died never looks alive. `wait` prints unread mail at once and exits 0 if anything is already unread, and exits 3 with one line on timeout.

**An agent with no assigned work does not end its turn.** It runs `wait` with a long timeout and, if its own tool returns early with the session still running, keeps polling that session; on exit 3 it ticks `step` and waits again, until the coordinator releases it. This is the loop that lets a main session steer an agent the root never wakes.

**What it costs.** The wait is one blocking command and spends nothing by itself, but the agent's session polls it: each poll is one model call, at most one every five minutes. An agent waiting for an hour therefore makes about a dozen calls while it has nothing to do. Keep the timeout long enough to be useful and let the coordinator release an agent that is no longer needed; an agent left waiting for a day spends a day of calls.

### `dots.json`: the optional config for one machine

`dots.py init` writes `<base>/dots.json` once per machine; the shape is in [examples/dots.json](../examples/dots.json). Nothing needs it: without the file every command still works, the dot comes from `live/<DOT>/`, the project from `<project>/mail/`, and the main session's name is `<project>-main`. With it, `send` uses the project's own main-session id and can run a wake command.

| Key | Meaning |
|---|---|
| `v` | Always `1`. |
| `dots` | The dot ids this machine keeps `live/` and `memory/` folders for. |
| `projects` | `id`, plus `main`, the session id that appears as `from` in that project's `in.jsonl`. |
| `hosts` | `label`, the value a live record's `host` uses, plus what that host is for. `vm` and `local` are always there; `init --host` adds the user's own ([hosts](hosts.md#placement-the-vm-first-then-the-one-local-machine)). |
| `wake` | Optional argv list, no shell. `{project}`, `{agent}`, `{id}` and `{dot}` are substituted; a command that names `{dot}` is not run when the dot is unknown. One run per `send`, 60-second timeout, never retried. |

`init` adds only what is missing and says what it kept: a second run never rewrites an entry, a record, a memory file or a mail line. `DOTS_OPS_BASE` chooses the folder; there is no other environment variable.

## Reading cadence and the doorbell

- An agent reads its inbox whenever it appends a live record, which is at least every 15 minutes while working, and again before `done`; `dots.py tick` prints the unread and the read-but-unanswered messages in the same call, unread first. It answers every message it receives with a message carrying `re`, even if the answer is only an `ack`. Before sending a `task` or `review`, the main session reads the agent's session memory, so it does not ask what the memory already answers.
- An agent that is neither in `wait` nor ticking cannot read. The main session checks its sent messages whenever it reads its own inbox. No watcher does this. A message needs a doorbell when all four hold: no `out.jsonl` line answers it with `re`, the agent's `.agent-read` marker has not reached it, the agent's newest live line is older than the message, and 20 minutes have passed since `t`. A message the agent has marked read but not yet answered gets no doorbell; the agent is already working from it. If that agent then goes quiet, the 30-minute stall rule in [live status](live-status.md#when-to-append) applies, and the coordinator checks it directly.
- **Doorbell:** the main session posts one line in the dot's own chat, which is the root's conversation. The line names the agent and the message id and asks the root to wake that agent to read its inbox, for example `[<project>-main → <DOT>] <agentId>: mail m-20261123T001500Z-4f2a unread for 20+ min; wake it to read its inbox (dots.py wait <DOT> <agentId>).` The main session sends one doorbell per message. If the agent still has not read it, the main session takes it to the owner.
- The root wakes that agent and nothing more. It does not read the mail aloud, summarize it or answer for the agent.

## Live records and the monitor

- A `step` that concerns mail cites the message as evidence, for example `mail:<agentId>/m-20261123T001500Z-4f2a`. The [monitor](monitoring.md) treats that as `local`.
- The agent appends `done` when it sends its `result`, as [live status](live-status.md#when-to-append) defines: the output is submitted. That `done` cites the `result` id as `mail:<agentId>/<message id>`, not a bare SHA. The agent then waits; the main session's `review` or `answer` reaches it at its next inbox read or through the doorbell. A `review` with a FAIL reopens the work: the agent appends `step` and later a new `result` and `done`. An accepting `answer` gets one more `done` that cites the `answer` id and the published SHA.
- A SHA that exists only in the outgoing repository or a workspace does not resolve on GitHub, so the monitor shows `UNPROVEN` for a line that cites it bare until the main session publishes it. That is intended: nothing counts as proven before the main session has checked and published it. A `done` that cites only the mail id reads as `local`, which the monitor also shows as `UNPROVEN`; read `mail/<agentId>/` to tell a pending handover from bad evidence. When `gh` is unavailable or the monitor runs with `--no-github`, an unchecked SHA reads as `unknown` and a `done` shows `DONE`; see [monitoring](monitoring.md).

## What this is not

It is files and appends in the shared folder on `local`, plus one foreground command an agent or the user starts and that ends by itself. It is not a broker, queue service, watcher, scheduler, hook, daemon or telemetry, and this repository installs nothing for it. The route itself grants nothing. The only grant is the owner's approval for that project.
