# Direct mail: a project's main session and a dot's sub-agents

On 2026-11-23 (KST) the owner asked that a project's main session talk with a dot's sub-agents directly, not through the dot root, and that sub-agents hand code over through an Osaka repository instead of GitHub. The main session is an agent session that lives outside Dots, such as the Yomanchi OMP session on the MacBook. The root stops relaying this traffic. It still summons agents and keeps the roster, and the only thing it does here is wake an idle agent when asked.

Current instance: project `yomanchi`, dot `REV18`, main session `yomanchi-main`, outgoing repository `/home/ubuntu/repos/Yomanchi-rev-out.git`.

## Where it lives

All on Osaka, under `~/CodeProjects/.dots-ops/<project>/`:

| Path | Written by | Content |
|---|---|---|
| `mail/<agentId>/in.jsonl` | the main session only | messages to that agent |
| `mail/<agentId>/out.jsonl` | that agent only | messages to the main session |
| `mail/<agentId>/.agent-read` | that agent only | id of the last `in.jsonl` message it has read |
| `mail/<agentId>/.main-read` | the main session only | id of the last `out.jsonl` message it has read |
| `mail/README.md` | the instance owner | short guide for that instance |

`<agentId>` is the same id the agent uses in its live records and in the roster. Each line is append-only: no one edits or deletes a line, and a correction is a new line. The instance may keep a helper next to the folder, such as `mail.py`, that appends a valid line, lists unread messages and moves a read marker. A hand-written append of a line that matches the schema has the same effect. The helper belongs to the instance and is not part of this kit.

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
| `refs` | Exact references: a branch, a full commit SHA, a path, a link, or `supersedes:<id>`. |

## How it maps to the handoff rules

Direct mail is the addressed message and reply route that [direct exchanges](../.agents/skills/dots-superkit/SKILL.md#direct-exchanges-not-another-queue) allows for an approved worker surface. All the event rules still apply:

- `re` is `reply_to`. A message is never authorization to widen scope, and a decision that belongs to the owner goes to the owner through the main session.
- `task` from the main session is a follow-up inside the agent's current assignment. New work still starts as an issue the dot's coordinator assigns. After the assignment, the main session and the agent talk directly.
- `result` is a **submit**: the exact `rev/*` branch and full commit SHA in the rev-out repository, the checks the agent ran with their observed results, and what it did not check. A replacement is a new `result` with `supersedes:<previous result id>` in `refs`. The agent's own tests are writer self-tests, not independent review.
- `review` from the main session is a **check**: it is tied to one result id and SHA, and it says what to fix when something fails.
- `block` names the missing fact or capability and the next safe step. A message is never a resume after an owner pause.
- The main session decides acceptance for the project. The dot's coordinator closes the dot-internal task once the main session's `answer` accepts or rejects a named result. That `answer` names the published SHA and the pull request, or gives the reason for rejection.

## Delivering code: the rev-out repository

- Commit on `rev/<topic>-<date>` and push only that branch to the project's rev-out repository on Osaka. The repository accepts `refs/heads/rev/*` only and refuses `loop/*`, `main`, `release/*` and tags. It borrows objects from the read-only mirror, so a push adds only the new objects.
  - From an Osaka workspace: `git push ~/repos/<Repo>-rev-out.git rev/<topic>-<date>`.
  - From a Mac mini workspace: `git push osaka:/home/ubuntu/repos/<Repo>-rev-out.git rev/<topic>-<date>`.
- Then send a `result`.
- Do not push to GitHub and do not open a pull request for this project. That includes the GitHub connector route. The main session fetches the exact SHA, verifies it in its own environment, pushes that same SHA to GitHub `rev/<topic>-<date>` and opens the pull request.
- Owner approval: on 2026-11-23 the owner approved this route for Yomanchi. Assigned REV sub-agents may commit and push `rev/*` branches to `Yomanchi-rev-out.git` on Osaka. Every other commit, push, merge or external write still needs its own approval.

## Reading cadence and the doorbell

- An agent reads its inbox whenever it appends a live record, which is at least every 15 minutes while working, and again before `done`. It answers every message it receives with a message carrying `re`, even if the answer is only an `ack`.
- An idle agent cannot read. If a message stays unread for 20 minutes and the agent has no newer live line, the main session sends the dot root one line asking it to wake that agent to read its inbox. The main session sends one doorbell per message.
- The root wakes the agent and nothing more. It does not read the mail aloud, summarize it or answer for the agent. The coordinator may read the mail folder for status, read-only.

## Live records and the monitor

- A `step` that concerns mail cites the message as evidence, for example `mail:yomanchi-26-android/m-20261123T001500Z-4f2a`. The [monitor](monitoring.md) treats that as `local`.
- A `done` cites the `result` id and the full SHA. A SHA that exists only in the rev-out repository does not resolve on GitHub, so the monitor shows `UNPROVEN` until the main session has published that SHA. That is intended: nothing counts as proven before the main session has checked and published it. Do not cite a bare SHA in a `step` line before publication, because the monitor would mark it `UNPROVEN` too.

## What this is not

It is files and appends in the shared Osaka folder. It is not a broker, queue service, watcher, scheduler, hook or telemetry. It installs nothing and grants no permission.
