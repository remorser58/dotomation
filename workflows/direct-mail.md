# Direct mail: a project's main session and a dot's sub-agents

On 2026-11-23 (KST) the owner asked that a project's main session talk with a dot's sub-agents directly, not through the dot root, and that sub-agents hand code over without GitHub. The main session is an agent session that runs outside Dots, such as the Yomanchi OMP session on the MacBook. The root no longer relays this traffic. It still summons agents, and the coordinator still keeps the roster and closes tasks. On the mail path the root does exactly two things: it wakes an idle agent when the main session rings the doorbell, and it reads the mail folder read-only for status and closure. The main session's own GitHub push and pull request rest on the owner's instruction to that session (for Yomanchi, the 2026-11-23 setup brief), not on this file.

Current instance: project `yomanchi`, dot `REV18`, main session `yomanchi-main`, Osaka outgoing repository `/home/ubuntu/repos/Yomanchi-rev-out.git`, Mac mini root `/Volumes/SanDisk2TB/m4mini/CodeProjects/yomanchi-rev18`.

## Where it lives

All on Osaka, under `~/CodeProjects/.dots-ops/<project>/`:

| Path | Written by | Content |
|---|---|---|
| `mail/<agentId>/in.jsonl` | the main session only | messages to that agent |
| `mail/<agentId>/out.jsonl` | that agent only | messages to the main session |
| `mail/<agentId>/.agent-read` | that agent only | the `id` of the last `in.jsonl` line it has read |
| `mail/<agentId>/.main-read` | the main session only | the `id` of the last `out.jsonl` line it has read |
| `mail/README.md` | the instance owner | short guide for that instance |

`<agentId>` is the same id the agent uses in its live records and in the roster. Lines are append-only: no one edits or deletes a line, and a correction is a new line. A read marker names a line, not a point in time. The unread messages are the lines after the line it names, in file order; ids are never compared with each other.

A Dots agent reads and appends these files through the approved route to Osaka for its account ([hosts](hosts.md#approved-routes)); for REV18 that is the `webgptagent_OCI_Osaka` connector, including when the agent's workspace is on the Mac mini. The main session runs outside Dots and uses its own access to Osaka. The instance keeps a helper, `mail.py`, next to the folder. It appends a valid line, lists unread messages (`mail.py new <agentId>`) and moves the agent's read marker (`mail.py done <agentId> <message id>`). It is instance setup the owner asked for, not part of this repository. A hand-written line that matches the schema has the same effect.

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
| `refs` | Exact references: a `rev/*` branch, a full commit SHA, `mini:<workspace path>`, a path, a link, `gate:<ID>`, or `supersedes:<message id>`. |

## How it maps to the handoff rules

Direct mail is the addressed message and reply route that [direct exchanges](../.agents/skills/dots-superkit/SKILL.md#direct-exchanges-not-another-queue) allows for an approved worker surface. For the tasks it carries, the main session is the named verifier. The handoff event rules apply as follows:

- **Gates.** The coordinator's brief to the agent holds the mandatory gate list. The agent's first message on a task is an `ack` that copies each gate ID and its pass condition, one `gate:<ID>` per gate in `refs`, so the main session checks against the same list. A main-session `task` that adds work inside the assignment names any new gate the same way. A message never removes a gate.
- `re` is `reply_to`. A message is never authorization to widen scope. A decision that belongs to the owner goes to the owner through the main session.
- `task` from the main session is a follow-up inside the agent's current assignment. New work still starts as an issue the dot's coordinator assigns. After the assignment, the main session and the agent talk directly.
- `result` is a **submit**. It names the exact target: the `rev/*` branch and full commit SHA, plus `mini:<workspace path>` when the commit stays on the Mac mini. It also lists the checks the agent ran with their observed results, and what it did not check. A replacement is a new `result` with `supersedes:<previous result id>` in `refs`. The agent's own tests are writer self-tests, not independent review.
- `review` from the main session is a **check**. It answers one `result` id and SHA. It gives one verdict per gate, `gate:<ID> PASS|FAIL|NOT_RUN`, with the observed output or its location, and the reason for each FAIL or NOT_RUN. For a FAIL it tells the agent what to fix.
- `answer` that accepts a `result` is the final check. It gives PASS for every gate on that SHA, names the SHA the main session published, and names the pull request. A rejection is a `review` with at least one FAIL. A rejection never closes anything.
- **Coordinator receipt.** The coordinator reads `mail/<agentId>/` read-only. That copy covers the submit chain and the verdicts, so the agent does not have to send the coordinator a second copy. The direct agent-to-main-session leg is still the one that counts.
- **Close.** Only the coordinator closes, under the [close rule](../.agents/skills/dots-superkit/SKILL.md#events-and-evidence). It closes only after an accepting `answer` on the newest `result`. Before closing, it reads that answer's per-gate verdicts and the published target, compares the gate IDs with the mandatory list in its own brief (a missing gate is NOT_RUN, never PASS), and confirms there is no open `block`, pause or newer owner direction. After a rejection the task stays open, and the coordinator records which gate failed.
- `block` names the missing fact or capability and the next safe step. A message is never a resume after an owner pause.

## Delivering code

- **Workspace on Osaka:** commit on `rev/<topic>-<date>` and push only that branch to the project's rev-out repository with `git push ~/repos/<Repo>-rev-out.git rev/<topic>-<date>`. The repository accepts `refs/heads/rev/*` only and refuses `loop/*`, `main`, `release/*` and tags. It borrows objects from the read-only mirror, so a push adds only the new objects.
- **Workspace on the Mac mini:** commit on `rev/<topic>-<date>` inside the approved Mac mini root and push nowhere. The `result` names `mini:<absolute workspace path>`, the branch and the full SHA. The main session fetches that exact commit from the workspace on the Mac mini. The Mac mini has no approved write route to Osaka for code, so do not use `ssh osaka` or `git push osaka:` from there.
- Then send a `result`.
- Sub-agents on this route do not push to GitHub or open pull requests for the project, and that includes the GitHub connector. The main session fetches the exact SHA, verifies it in its own environment, pushes that same SHA to GitHub `rev/<topic>-<date>` and opens the pull request.
- **Approval.** On 2026-11-23 the owner approved this for Yomanchi. Assigned REV sub-agents may commit `rev/*` branches in their approved workspaces and push them, from Osaka, to `Yomanchi-rev-out.git` on Osaka. Every other commit, push, merge or external write still needs its own approval. Another project uses this route only after the owner approves it for that project.

## Reading cadence and the doorbell

- An agent reads its inbox whenever it appends a live record, which is at least every 15 minutes while working, and again before `done`; `dots.py tick` ([session memory](session-memory.md#one-call-per-step)) prints the unread and the read-but-unanswered messages in the same call. It answers every message it receives with a message carrying `re`, even if the answer is only an `ack`. Before sending a `task` or `review`, the main session reads the agent's session memory, so it does not ask what the memory already answers.
- An idle agent cannot read. The main session checks its sent messages whenever it reads its own inbox. No watcher does this. A message needs a doorbell when all four hold: no `out.jsonl` line answers it with `re`, the agent's `.agent-read` marker has not reached it, the agent's newest live line is older than the message, and 20 minutes have passed since `t`. A message the agent has marked read but not yet answered gets no doorbell; the agent is already working from it. If that agent then goes quiet, the 30-minute stall rule in [live status](live-status.md#when-to-append) applies, and the coordinator checks it directly.
- **Doorbell:** the main session posts one line in the dot's own chat, which is the root's conversation. The line names the agent and the message id and asks the root to wake that agent to read its inbox, for example `[yomanchi-main → REV] yomanchi-26-android: mail m-20261123T001500Z-4f2a unread for 20+ min; wake it to read its inbox (mail.py new).` The main session sends one doorbell per message. If the agent still has not read it, the main session takes it to the owner.
- The root wakes that agent and nothing more. It does not read the mail aloud, summarize it or answer for the agent.

## Live records and the monitor

- A `step` that concerns mail cites the message as evidence, for example `mail:yomanchi-26-android/m-20261123T001500Z-4f2a`. The [monitor](monitoring.md) treats that as `local`.
- The agent appends `done` when it sends its `result`, as [live status](live-status.md#when-to-append) defines: the output is submitted. That `done` cites the `result` id as `mail:<agentId>/<message id>`, not a bare SHA. The agent then waits; the main session's `review` or `answer` reaches it at its next inbox read or through the doorbell. A `review` with a FAIL reopens the work: the agent appends `step` and later a new `result` and `done`. An accepting `answer` gets one more `done` that cites the `answer` id and the published SHA.
- A SHA that exists only in the rev-out repository or a Mac mini workspace does not resolve on GitHub, so the monitor shows `UNPROVEN` for a line that cites it bare until the main session publishes it. That is intended: nothing counts as proven before the main session has checked and published it. A `done` that cites only the mail id reads as `local`, which the monitor also shows as `UNPROVEN`; read `mail/<agentId>/` to tell a pending handover from bad evidence. When `gh` is unavailable or the monitor runs with `--no-github`, an unchecked SHA reads as `unknown` and a `done` shows `DONE`; see [monitoring](monitoring.md).

## What this is not

It is files and appends in the shared Osaka folder. It is not a broker, queue service, watcher, scheduler, hook or telemetry, and this repository installs nothing for it. The route itself grants nothing. The only grant is the owner's 2026-11-23 approval above.
