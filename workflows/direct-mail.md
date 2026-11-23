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
| `re` | The `id` of the message this answers, or `null`. Every `ack`, `answer` and `note` from an agent carries `re`, because it answers something; a first `result`, `question` or `block` may carry `null` when its `refs` say what it is about. A new question from the agent uses the last message it received. |
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

An agent's conversation cannot be read or written from outside the dot, so a new instruction from the main session is a file the agent reaches through its own approved route, plus a command the agent runs that returns as soon as that file changes. `tools/dots.py` in this kit does both ([session memory](session-memory.md#one-call-per-step)); run it from a checkout, or copy it beside the shared folder — a copy there drops `sys.path[0]` before importing the standard library, so a `json.py` beside it cannot be imported in its place. Its `init` command writes `<base>/dots.json` and creates `live/<DOT>/`, `memory/<DOT>/` and `<project>/mail/`, then prints the brief to paste to each dot root ([quick start](../README.md#빠른-시작-로컬-한-대)).

| Side | Command |
|---|---|
| Main session | `dots.py send <project> <agentId> <kind> <task> [--re ID] [--ref R]...` — text on stdin; `--re` names a message in `in.jsonl` or an agent reply in `out.jsonl`; prints the message id, then the agent's listening status |
| Main session | `dots.py inbox [project] [agentId]` — agent replies not yet marked read; `dots.py read <project> <agentId> <id>` marks them read |
| Agent | `dots.py wait <DOT> <agentId> [--timeout SECONDS]` — default 3300 seconds (55 minutes) |
| Agent | `dots.py reply <project> <agentId> <kind> <task> [--re ID] [--ref R]... [--dot DOT] [--timeout S] [--no-wait]` — text on stdin. A `result`, `question` or `block` stays open and prints the answer; `--no-wait` returns as soon as the line is written |

`send` writes one [mail-v1](../protocol/mail-v1.schema.json) line into `in.jsonl` and `reply` one into `out.jsonl`, exactly as a hand-written line does, so `mail.py` or any reader of the schema reads them unchanged. `from` is the project's main-session id from `dots.json` (`<project>-main` when the config names none). `--re` accepts an id from either file, because the review and answer legs of this route answer the agent's own `result`, which is a line in `out.jsonl`. Delivery then depends on what the agent is doing:

A `result`, `question` or `block` is the last thing an agent says before it has nothing to do, so
`reply` does not return after writing the line: it continues into the same blocking loop `wait`
runs, and the main session's `answer` or `review` is printed as that command's own output. The
agent therefore never ends its turn with an answer sitting unread — the case that once left a
review waiting hours — and the answer costs no extra call, because the agent's own session was
already polling that command. `--no-wait` writes the line and returns at once, for an agent that
must hand over and stop. `ack`, `answer` and `note` never wait: they answer something and the agent
goes on working.

Only the answer to that line ends the wait: it stays open until a message whose `re` is the reply's
own id arrives, so a note, a question or mail that was already unread when the command started is
printed and marked delivered on the way but does not end it. On exit 3 the reply has not arrived —
the message itself is already written, so the agent ticks `step` and runs `wait` again instead of
writing the result twice.

A first `result` often answers work the dot root gave, which is not a mail line, so `re` is `null`
there and at least one `--ref` says what the result is about. `--re` stays required for `ack`,
`answer` and `note`, and every line the tool writes is checked against
[mail-v1](../protocol/mail-v1.schema.json) before it is appended.

1. **The agent is in `wait`.** The line is printed the moment it lands — typically within a second — and `wait` exits 0. This is the only near-real-time path, and it is what steering means here: the agent is holding one tool call open, so an instruction reaches it mid-task instead of at the end of a step.
2. **The agent is working.** It sees the line at the top of its next `tick`, printed before the live line under a `MAIL_FIRST ... handle these before your next step` header. A working agent ticks at each material step and at least every 15 minutes, so that is the worst case while it is working.
3. **The agent is idle.** Nothing reads the file until it runs a command again. `send` prints `LISTENING` with the waiting process, or `NOT_LISTENING last_tick=<age>`. When it is not listening and `dots.json` holds a wake command, `send` runs that command once, prints its exit code, and does not retry. Otherwise it prints the manual doorbell below.

The main session does not have to guess which agents are stuck: `dots.py board` flags
`MAIL_WAITING` on an agent whose inbox holds a message that still owes an answer while no `wait`
marker of that agent is alive, and prints the count and the age of the oldest. `MAIL-UNANSWERED` is
the same mail without that second condition. Both are read from the files; see
[If the dot root is not trusted](#if-the-dot-root-is-not-trusted) for what the board does with the
roster.

The wake command is whatever the instance already has to post one line into that dot's chat: an argv list, no shell, `{project}`, `{agent}`, `{id}` and `{dot}` substituted. It is not part of this kit and the kit cannot check that it exists.

While `wait` blocks it holds one marker per waiting process, `<base>/listen/<agentId>.<pid>.json` — `{pid, dot, since, until, host}` — and removes only its own on any exit, including `SIGTERM`, `SIGHUP` and `SIGINT`. A marker counts as listening only with a `until` still in the future and no more than 24 hours out, judged with one hour of slack because the waiter's clock may run ahead of the reader's, and with its process still alive here; a marker whose process is gone, whose `until` has passed or which names a date a day or more away counts as not listening, so a waiter that died — or one that was never there — never looks alive. A marker written on another host is judged by `until` alone: a pid there says nothing here, so without a usable `until` it proves nothing. `wait` refuses a `--timeout` over a day for that reason. `wait` prints unread mail at once and exits 0 if anything is already unread; otherwise it blocks. It exits 3 with one line on timeout and 4 when a signal ends it, so an interrupted call is never mistaken for a delivery.

**An agent with no assigned work does not end its turn.** It runs `wait` with a long timeout and, if its own tool returns early with the session still running, keeps polling that session. `wait` moves the same `.agent-read` marker `tick --took` moves, so the lines it printed count as delivered: the loop is wait, handle, wait, and the same instruction is never printed twice. A line whose `id` cannot be marked delivered — a hand-written one with a bad id — prints once and stays quiet for the rest of that command, so it cannot flood the poll. On exit 3 it ticks `step` and waits again; on exit 4 a signal ended it, so it waits again. This is the loop that lets a main session steer an agent the root never wakes.

**What it costs.** The wait is one blocking command and spends nothing by itself, but the agent's session polls it: each poll is one model call, at most one every five minutes. An agent waiting for an hour therefore makes about a dozen calls while it has nothing to do. Each poll also re-reads that agent's inboxes from disk, which is local CPU and disk rather than model calls. Keep the timeout long enough to be useful and let the coordinator release an agent that is no longer needed; an agent left waiting for a day spends a day of calls.

### `dots.json`: the optional config for one machine

`dots.py init` writes `<base>/dots.json` once per machine; the shape is in [examples/dots.json](../examples/dots.json). Nothing needs it: without the file every command still works, the dot comes from `live/<DOT>/`, the project from `<project>/mail/`, and the main session's name is `<project>-main`. With it, `send` uses the project's own main-session id and can run a wake command.

| Key | Meaning |
|---|---|
| `v` | Always `1`. |
| `dots` | The dot ids this machine keeps `live/` and `memory/` folders for. |
| `projects` | `id`, plus `main`, the session id that appears as `from` in that project's `in.jsonl`. |
| `hosts` | `label`, the value a live record's `host` uses, plus what that host is for. `vm` and `local` are always there; `init --host` adds the user's own ([hosts](hosts.md#placement-the-vm-first-then-the-one-local-machine)). |
| `wake` | Optional argv list, no shell. `{project}`, `{agent}`, `{id}` and `{dot}` are substituted; a command that names `{dot}` is not run when the dot is unknown. One run per `send`, 25-second timeout (under the connector's 30-second yield, so the result is not lost), never retried. |
| `notify` | Optional argv list, same placeholders plus `{age}`. Run by `watch`, never by `send`, when a wake ran and the message is still unanswered. |
| `watch` | `after_min` (default 10) and `notify_after_min` (default 20), the two delays `watch` uses. Integers, 0 or more; anything else is ignored and the default stays. |

Every value here is a suggestion, not a setting: `dots.json` sits in the folder the dot root can
write, so `watch` takes the same four from its own command line and the command line wins —
`--wake-cmd`, `--notify-cmd`, `--after-min`, `--notify-after-min`, plus `--state` for the ledger
path. Each run prints one `WATCH_CONFIG` line naming where every value came from (`argv`,
`dots.json` or `default`), so a config that changed under the owner is visible as a changed line.

`init` adds only what is missing and says what it kept: a second run never rewrites an entry, a record, a memory file or a mail line. `DOTS_OPS_BASE` chooses the folder, else `~/.dots-ops`; there is no other environment variable, and `init` resolves the same folder as every other command. `init --base DIR` writes the tree elsewhere and prints the `DOTS_OPS_BASE` line to set, because later commands still resolve the environment first. An instance that already keeps its folder at `<work root>/.dots-ops` rather than `~/.dots-ops` exports `DOTS_OPS_BASE` to it; nothing is moved or copied.

## Reading cadence and the doorbell

- An agent reads its inbox whenever it appends a live record, which is at least every 15 minutes while working, and again before `done`; `dots.py tick` prints the unread and the read-but-unanswered messages in the same call, unread first. It answers every message it receives with a message carrying `re`, even if the answer is only an `ack`. Before sending a `task` or `review`, the main session reads the agent's session memory, so it does not ask what the memory already answers.
- `dots.py tick <DOT> <agentId> done` is refused with exit 2 and nothing written while a `task`,
  `question` or `review` in the inbox has no `out.jsonl` line answering it, and the refusal names
  each id and prints the exact `reply` command. An agent that has just submitted a review it never
  answered cannot leave the turn quietly.
- An agent that is neither in `wait` nor ticking cannot read. The main session checks its sent messages whenever it reads its own inbox. No watcher does this. A message needs a doorbell when all four hold: no `out.jsonl` line answers it with `re`, the agent's `.agent-read` marker has not reached it, the agent's newest live line is older than the message, and 20 minutes have passed since `t`. A message the agent has marked read but not yet answered gets no doorbell; the agent is already working from it. That includes a message `wait` printed: `wait` marks it read the moment it prints it, before the agent acts, so if that agent's session then dies the main session rings no doorbell for it. The 30-minute stall rule in [live status](live-status.md#when-to-append) is what finds that agent, the coordinator checks it directly, and `tick` prints the message again as `MAIL_UNANSWERED` when the agent comes back.
- The board is the other half of that rule: `dots.py board` flags `MAIL_WAITING` on an agent that
  owes an answer while no `wait` marker is alive, which is exactly the case the doorbell rule leaves
  alone — a message a `wait` printed and marked read, whose process then died. Read the flag as "a
  person has to look", not as "a doorbell is owed": the doorbell goes to a root that may not act,
  and `dots.py watch` is the fallback that runs a wake and then a notify without waiting for the
  root to notice.
- **Doorbell:** the main session posts one line in the dot's own chat, which is the root's conversation. The line names the agent and the message id and asks the root to wake that agent to read its inbox, for example `[<project>-main → <DOT>] <agentId>: mail m-20261123T001500Z-4f2a unread for 20+ min; wake it to read its inbox (dots.py wait <DOT> <agentId>).` The main session sends one doorbell per message. If the agent still has not read it, the main session takes it to the owner.
- The root wakes that agent and nothing more. It does not read the mail aloud, summarize it or answer for the agent.

## Live records and the monitor

- A `step` that concerns mail cites the message as evidence, for example `mail:<agentId>/m-20261123T001500Z-4f2a`. The [monitor](monitoring.md) treats that as `local`.
- The agent appends `done` when it sends its `result`, as [live status](live-status.md#when-to-append) defines: the output is submitted. That `done` cites the `result` id as `mail:<agentId>/<message id>`, not a bare SHA. The agent then waits; the main session's `review` or `answer` reaches it at its next inbox read or through the doorbell. A `review` with a FAIL reopens the work: the agent appends `step` and later a new `result` and `done`. An accepting `answer` gets one more `done` that cites the `answer` id and the published SHA.
- A SHA that exists only in the outgoing repository or a workspace does not resolve on GitHub, so the monitor shows `UNPROVEN` for a line that cites it bare until the main session publishes it. That is intended: nothing counts as proven before the main session has checked and published it. A `done` that cites only the mail id reads as `local`, which the monitor also shows as `UNPROVEN`; read `mail/<agentId>/` to tell a pending handover from bad evidence. When `gh` is unavailable or the monitor runs with `--no-github`, an unchecked SHA reads as `unknown` and a `done` shows `DONE`; see [monitoring](monitoring.md).

## If the dot root is not trusted

The dot root is an agent too: it can forget a rule, never paste the brief, misreport the roster, fail
to wake an agent, or edit the shared folder. Nothing here assumes otherwise. What the tool enforces
from the files alone:

- **The rules reach the agent without the root.** The first `dots.py` command an agent runs while it
  has no memory file of its own — `tick`, `reply` or `wait` — prints the agent rules ahead of that
  command's own output, and `init` prints the same block in the brief. A file that merely exists is
  not enough: the banner is skipped only for a memory the tool itself would have written (`v` 1, that
  agent's own name, and the task, goal, doing and next a first task requires), so a stub in the
  root-writable memory folder cannot swallow it. A root that never pastes the brief costs the agent
  nothing.
- **An agent cannot go quiet while it owes an answer.** `dots.py tick <DOT> <agentId> done` is
  refused with exit 2 and nothing written while an inbox line of kind `task`, `question` or `review`
  has no `out.jsonl` line answering it with `re`. The refusal names each id and prints the exact
  `reply` command, and that reply stays open and prints the answer, so the way out of the refusal is
  also the way to keep the turn alive.
- **Flags come from evidence, not from the roster.** `board` derives `MAIL_WAITING`,
  `MAIL-UNANSWERED` and `CLAIM-STALE` from the mail folders, the live lines and the listen markers.
  The roster value is printed as `roster=<state> (the root's claim)`, and `CLAIM-STALE` with its
  `claim:` line says why that claim contradicts what the agent itself wrote: a `done` or `blocked`
  line with a message at or after it still unanswered, or a roster `done`/`idle` while the agent's
  own newest line is `start`/`step`.
- **The owner can run the watchdog.** `dots.py watch [--loop MINUTES] [--expect-sha256 HEX]
  [--wake-cmd ARGV] [--notify-cmd ARGV] [--after-min N] [--notify-after-min N] [--state PATH]` trusts
  five things, four of them in the folder the root writes: the mail folders (what owes an answer),
  the `listen/` markers (whether a wake runs), the `live/` lines (for `{dot}`), `dots.json` (every
  value no flag overrides) and its own ledger. `--state PATH` moves the ledger out of that folder,
  and the four flags pin the values `dots.json` would otherwise set. For every message that owes an
  answer and whose agent holds no live wait marker, it runs the wake argv once after `after_min`
  minutes; if the message is still unanswered `notify_after_min` minutes after that wake, it runs the
  notify argv once. A message counts as owed until an `out.jsonl` line answers it, read or not: a
  `.agent-read` marker that reached it prints `WATCH_STUCK … read=yes unanswered=yes` once and the
  notify runs anyway, because that marker lives in the same folder as the mail. An owed line always
  has an age — its own `t`, else the moment the ledger first saw it — so an unparseable `t` cannot
  hide a message; a time still in the future is not an age either, and if both are ahead the run
  prints `WATCH_ERROR … no usable time t=… seen_t=…` instead of staying silent. The ledger records
  that first sight, so a line deleted before its first ring still ends as `WATCH_RESOLVED`; the cost
  is one `WATCH_RESOLVED` per answered message. Its ledger defaults to `<base>/watch/state.json`, so
  a restart never rings twice, and `--state PATH` keeps it outside the folder the root writes. The
  ledger may only add a line: every step it suppresses prints
  `WATCH_SKIP … reason=wake_already|notify_already`, and the rest are `WATCH_CONFIG`, `WATCH_WAKE`,
  `WATCH_NOTIFY`, `WATCH_STUCK`, `WATCH_HELD`, `WATCH_RESOLVED`, `WATCH_TOOL_CHANGED` and
  `WATCH_ERROR`. The flags win over `dots.json` for the reason above, and `WATCH_CONFIG` names the
  source of each value on every run. `--loop 0` (the default) runs once; a loop sleeps between runs
  and ends on `SIGTERM`, `SIGHUP` or `SIGINT`. It makes no network and no model call: the two
  commands are argv lists the owner configures, for example `dots.py watch
  --loop 5 --wake-cmd '["ssh","local","dots-post","{project}","{agent}","{id}"]'
  --notify-cmd '["notify-send","{agent}","{id}","{age}"]'`.
- **A wait marker holds the wake, and `WATCH_HELD` says so.** `listen/<agent>.<pid>.json` needs a
  `until` still in the future and no more than 24 hours out, judged with one hour of slack because
  the waiter's clock may run ahead of the reader's; a marker without one, with a past one or with a
  date years away is not a waiter, and a marker written on another host is judged by `until` alone.
  A live marker suppresses the wake but not the report: a real waiter prints unread mail and marks
  it read within a second, so owed mail still unread past `after_min` contradicts it, and
  `WATCH_HELD {project} {agent} {id} age=… pid=… host=… until=… read=…` prints on every run. That
  line is the contradiction signal, and both halves of it are the root's to write: `read=` is
  forgeable exactly like the marker. Nothing else fires while a marker holds the wake — no notify —
  so this stdout line is the owner's only warning, and the one case the tool cannot settle by
  itself: check `ps -p <pid>` and the agent's newest live line before believing the marker.

**What it only detects.** The tool cannot make the root honest. A line the root writes looks exactly
like one an agent wrote, because `from` is a string and nothing signs it. A roster entry is a claim,
so `board` reports it next to the evidence instead of trusting it. A message that never gets an
answer stays visible as `MAIL_WAITING`, but nothing in this kit can answer it for the agent.

The `done` refusal and the flags are built for a root that forgets, not for one that lies. A forged
`out.jsonl` line answering the id clears the refusal and `MAIL_WAITING` with it, because an answer is
an answer whichever session wrote it; a forged `in.jsonl` line, or a deleted answer, re-arms both on
the next read. Read `refs` and the live-line history when it matters, and treat a clean board as "no
file says work is owed", not as "the work was done".

**The hard limit.** Only the root can restart a sub-agent whose turn has ended. A sub-agent's
conversation cannot be reopened from outside Dots: the app-server lists no conversation for one, and
the root's own conversation answers 404. So the last step is always a message to the root, or the
owner. `watch` says so plainly — `WATCH_ERROR no wake command; an ended agent cannot be restarted`
when nothing is configured, `WATCH_NOTIFY … read=no|yes` when the wake ran and the message is still
unanswered — and hands the owner the ids to act on. `watch` never claims an agent was woken when no
wake command ran.

**Residual risks.**

- **A forged line.** The root can append a plausible `result` or a `done` on an agent's behalf. Read
  `refs`: a `rev/*` branch and a full SHA the main session can resolve, or a `local:` path it can
  open, are the only things that make a `result` worth checking, and the live-line history shows
  whether the steps that lead to it happened.
- **A swapped tool.** The shared folder is writable by the root, so a `dots.py` kept inside it can be
  replaced. Keep the tool outside the shared folder and pass `--expect-sha256` on the owner's own
  `watch` command line — not in the root's brief, which the root writes — with the hash of the file
  the owner reviewed; `WATCH_TOOL_CHANGED expected=… actual=… path=…` prints as soon as the bytes
  differ, and with `actual=unreadable` when the file cannot be read at all — a copy replaced by a
  FIFO would otherwise block that read. A copy that does sit in a root-writable folder drops
  `sys.path[0]` before importing the standard library, so a `json.py` or `socket.py` beside it cannot
  be imported in its place, but that only protects the imports: the copy's own bytes still need the
  hash check.
- **A forged ledger record.** `watch/state.json` is in the shared folder too. A record that says a
  wake or a notify already ran suppresses that step, and the tool prints `WATCH_SKIP … wake_already`
  or `notify_already` so the suppression is visible; `--state PATH` keeps the ledger where the root
  cannot write it, which is the only way to make the record trustworthy.
- **A path replaced by something that is not a file.** Any name in that folder can be a directory, a
  FIFO or a socket instead of a file: `in.jsonl`, `out.jsonl`, `.agent-read`, a `listen/` marker, the
  ledger. Each one then reads as empty — no lines, no waiter, no record — and `watch`, `wait` and
  `reply` keep running rather than dying or blocking on it, but not silently: a path that exists and
  cannot be read prints `dots: cannot read <path> (…)` once on stderr (`treated as no lines`, and
  `(not a regular file)` for a FIFO), while a missing file is the normal case and stays quiet. A
  stat before the open is not enough on its own, because the root can rename a FIFO onto the path
  between the two, so every read also opens with `O_NONBLOCK` — a FIFO open returns at once instead
  of waiting for a writer — and then asks the descriptor what it holds: only a regular file is read,
  and a FIFO that wins that race is reported as unreadable rather than read as empty. Appending has
  the same hole, so a mail line goes through a descriptor opened the same way: a FIFO that wins that
  race is refused with `dots: cannot append to <path>: it is not a regular file` and exit 2 instead
  of taking the line, and the temp file `write_atomic` writes is opened non-blocking for the same
  reason. A read marker that cannot be written prints
  `dots: cannot mark … read (…)` once and the mail stays unread; a ledger that cannot be written
  prints `dots: cannot write the ledger …` once and says `every run rings again until it is
  writable`, which is the safe direction. A ledger record that is not an object is dropped, so that
  run rings again instead of dying. Nothing is repaired, and an unreadable mailbox hides its mail
  from `watch` the same way a deleted line does.
- **A line whose bytes are not UTF-8.** A mailbox is bytes the root appends to, so one damaged line is
  expected. It costs that line and nothing else: each line is decoded on its own, a line that does not
  decode is skipped, and the run prints `dots: skipped N lines in <path>, not UTF-8; every other line
  was read` once per change in N, so the size of the damage is visible without a once-a-second poll
  repeating itself. Decoding the file as one stream would drop every line from the bad byte to the
  next buffer boundary — a message written after the damage would never be rung — so the per-line
  read is the rule, not an optimisation. `.agent-read` and `.main-read` are single ids and read as one
  line; a marker with damage in it reads as no marker, which the missing-id case below covers.
- **A line with no usable id.** An owed line whose `id` is missing or not a token can never be
  woken or answered, and `board` still counts it as `MAIL_WAITING`; `watch` prints
  `WATCH_ERROR {project} {agent} {id!r} unusable id` on every run instead of skipping it in silence.
  Fix or delete the line.
- **A forged listen marker.** A `listen/<agent>.<pid>.json` with a plausible `until` and a live local
  pid looks like a waiter and holds the wake back. It needs `until` inside the next 24 hours; compare
  `ps -p <pid>` with the agent's newest live line when a wait looks too convenient.
- **A forged `.agent-read`.** Marking a message read adds `WATCH_STUCK … read=yes unanswered=yes` once
  and the notify still runs: the read marker can make the watchdog noisier, never quieter.
- **A deleted message reads as answered.** The ledger knows an id, not the line: a message the root
  deletes from `in.jsonl` disappears from `watch`'s list and is reported `WATCH_RESOLVED`. Compare the
  count with `inbox` before believing a resolution you did not cause.
- **Printed rules are text.** An agent's model can still ignore them. The enforcement with teeth is
  the `done` refusal and the flags, not the paragraph.

## What this is not

It is files and appends in the shared folder on `local`, plus one foreground command an agent or the
user starts and that ends by itself; `watch` is that command too, and its `--loop` is a delay the
owner chooses, not a service. It is not a broker, queue service, watcher, scheduler, hook, daemon or
telemetry, and this repository installs nothing for it. The route itself grants nothing. The only
grant is the owner's approval for that project.
